//! How Cloud commands find workspaces, projects, and databases when they are
//! not passed explicitly: helix.toml link, sole candidate, or an error that
//! lists the candidates (tests never have a TTY, so no picker).

mod support;

use assert_cmd::assert::Assert;
use serde_json::{json, Value};
use std::fs;
use support::CliFixture;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: &Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).unwrap()
}

fn stderr(assert: &Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).unwrap()
}

async fn get(server: &MockServer, at: &str, body: Value) {
    Mock::given(method("GET"))
        .and(path(at))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn cloud() -> (MockServer, CliFixture) {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("owner@example.com", "session-access");
    (server, fixture)
}

/// Two workspaces: Acme (ws-1: projects Graph and Search) and Beta (ws-2: Web).
async fn mount_two_workspaces(server: &MockServer) {
    get(
        server,
        "/v1/workspaces",
        json!({"workspaces":[
            {"id":"ws-1","slug":"acme","displayName":"Acme"},
            {"id":"ws-2","slug":"beta","displayName":"Beta"}
        ]}),
    )
    .await;
    for (workspace, projects) in [
        (
            "ws-1",
            json!([
                {"id":"p-graph","slug":"graph","displayName":"Graph"},
                {"id":"p-search","slug":"search","displayName":"Search"}
            ]),
        ),
        (
            "ws-2",
            json!([{"id":"p-web","slug":"web","displayName":"Web"}]),
        ),
    ] {
        Mock::given(method("GET"))
            .and(path("/v1/projects"))
            .and(query_param("workspace_id", workspace))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"projects": projects})))
            .mount(server)
            .await;
    }
}

fn write_project(fixture: &CliFixture, name: &str, toml: &str) -> std::path::PathBuf {
    let dir = fixture.root().join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("helix.toml"), toml).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn several_candidates_without_a_terminal_list_them_instead_of_guessing() {
    let (server, fixture) = cloud().await;
    mount_two_workspaces(&server).await;

    let human = fixture
        .command()
        .args(["project", "list"])
        .assert()
        .failure();
    let error = stderr(&human);
    assert!(error.contains("2 workspaces found; choose one"), "{error}");
    assert!(error.contains("--workspace"), "{error}");
    assert!(error.contains("Acme") && error.contains("ws-2"), "{error}");
    assert!(stdout(&human).is_empty());

    let machine = fixture
        .command()
        .args(["project", "list", "--json"])
        .assert()
        .code(1);
    assert!(stdout(&machine).is_empty());
    let error: Value = serde_json::from_str(stderr(&machine).trim()).unwrap();
    assert_eq!(error["error"]["message"], "2 workspaces found; choose one");
    assert_eq!(
        error["error"]["candidates"],
        json!([{"id":"ws-1","name":"Acme"},{"id":"ws-2","name":"Beta"}])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projects_resolve_by_id_slug_or_name_across_workspaces() {
    let (server, fixture) = cloud().await;
    mount_two_workspaces(&server).await;
    // IDs resolve directly; anything else 404s and falls back to a search.
    get(
        &server,
        "/v1/projects/p-web",
        json!({"id":"p-web","workspaceId":"ws-2","displayName":"Web"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/search"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"message":"not found"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/GRAPH"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"message":"not found"})))
        .mount(&server)
        .await;

    for (query, id, workspace) in [
        ("p-web", "p-web", "ws-2"),
        ("search", "p-search", "ws-1"),
        ("GRAPH", "p-graph", "ws-1"),
    ] {
        let assert = fixture
            .command()
            .args(["project", "get", query, "--json"])
            .assert()
            .success();
        let project: Value = serde_json::from_str(&stdout(&assert)).unwrap();
        assert_eq!(project["id"], id, "{query}");
        // --json is verbatim: only the direct fetch returned a workspaceId.
        assert_eq!(
            project.get("workspaceId").is_some(),
            query == "p-web",
            "{query}: {project}"
        );
        // The human view shows the workspace the project was found in.
        let assert = fixture
            .command()
            .args(["project", "get", query])
            .assert()
            .success();
        let details = stdout(&assert);
        let row = details
            .lines()
            .find(|line| line.starts_with("Workspace"))
            .unwrap();
        assert!(row.ends_with(workspace), "{query}: {details}");
    }

    // With --workspace, only that workspace is searched.
    let assert = fixture
        .command()
        .args(["project", "get", "web", "--workspace", "acme"])
        .assert()
        .failure();
    let error = stderr(&assert);
    assert!(error.contains("no project matches 'web'"), "{error}");
    assert!(
        error.contains("Graph") && error.contains("Search"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_names_list_only_the_matches() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/workspaces",
        json!({"workspaces":[
            {"id":"ws-1","displayName":"Team"},
            {"id":"ws-2","displayName":"team"},
            {"id":"ws-3","displayName":"Other"}
        ]}),
    )
    .await;
    let assert = fixture
        .command()
        .args(["workspace", "get", "team", "--json"])
        .assert()
        .failure();
    let error: Value = serde_json::from_str(stderr(&assert).trim()).unwrap();
    assert_eq!(error["error"]["message"], "'team' matches 2 workspaces");
    assert_eq!(error["error"]["candidates"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lists_follow_every_page() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/workspaces",
        json!({"workspaces":[{"id":"ws-1","displayName":"Acme"}]}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .and(query_param("page.size", "200"))
        .and(query_param_is_missing("page.token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "projects":[{"id":"p-1"}], "nextPageToken":"cursor-2"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .and(query_param("page.token", "cursor-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "projects":[{"id":"p-2"}], "nextPageToken":""
        })))
        .expect(1)
        .mount(&server)
        .await;
    let assert = fixture
        .command()
        .args(["project", "list", "--json"])
        .assert()
        .success();
    let projects: Value = serde_json::from_str(&stdout(&assert)).unwrap();
    let ids: Vec<_> = projects
        .as_array()
        .unwrap()
        .iter()
        .map(|project| project["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["p-1", "p-2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_repeats_a_page_token_is_an_error_not_a_hang() {
    let (server, fixture) = cloud().await;
    Mock::given(method("GET"))
        .and(path("/v1/workspaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "workspaces":[{"id":"ws-1"}], "nextPageToken":"same"
        })))
        .mount(&server)
        .await;
    let assert = fixture
        .command()
        .args(["workspace", "list"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("repeated a page token"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_output_is_pure_and_carries_no_chrome() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/workspaces",
        json!({"workspaces":[{"id":"ws-1","displayName":"Acme","iconUrl":"https://x"}]}),
    )
    .await;
    let assert = fixture
        .command()
        .args(["workspace", "list", "--json"])
        .assert()
        .success();
    assert_eq!(
        stderr(&assert),
        "",
        "no chrome, not even the auto-pick remark"
    );
    let workspaces: Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(
        workspaces[0]["iconUrl"], "https://x",
        "unmodelled fields survive"
    );

    // The human rendering auto-picks the only workspace and says so on stderr.
    get(
        &server,
        "/v1/workspaces/ws-1/service-credentials",
        json!({"credentials":[]}),
    )
    .await;
    let assert = fixture
        .command()
        .args(["service-credential", "list"])
        .assert()
        .success();
    assert!(stderr(&assert).contains("Using workspace Acme"));
    assert!(stderr(&assert).contains("no service credentials"));
    assert_eq!(stdout(&assert), "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_linked_directory_needs_no_flags() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/projects/p-graph",
        json!({"id":"p-graph","workspaceId":"ws-1","displayName":"Graph"}),
    )
    .await;
    get(
        &server,
        "/v1/tenants/t-1",
        json!({"id":"t-1","name":"App","projectId":"p-graph"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/clusters"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"clusters":[]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/tenants"))
        .and(query_param("project_id", "p-graph"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tenants":[{"id":"t-1","name":"App"},{"id":"t-2","name":"Staging"}]
        })))
        .mount(&server)
        .await;

    // Only an [enterprise] entry: its owner is the link.
    let dir = write_project(
        &fixture,
        "enterprise-only",
        "[project]\nname = \"enterprise-only\"\n\n[enterprise.production]\ndatabase = \"tenant:t-1\"\nproject_id = \"p-graph\"\nworkspace_id = \"ws-1\"\n",
    );
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["database", "list"])
        .assert()
        .success();
    let table = stdout(&assert);
    assert!(
        table.contains("App") && table.contains("Staging"),
        "{table}"
    );
    let linked_row = table.lines().find(|line| line.contains("t-1")).unwrap();
    assert!(
        linked_row.starts_with('●'),
        "the linked database is marked: {table}"
    );

    // With one linked database, database commands default to it.
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["database", "get", "--json"])
        .assert()
        .success();
    let database: Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(database["id"], "t-1");

    // --project overrides the linked database default.
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["database", "get", "--project", "p-graph", "--json"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("2 databases found"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_links_explain_how_to_fix_them() {
    let (server, fixture) = cloud().await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/gone"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({"message":"project not found"})),
        )
        .mount(&server)
        .await;
    let dir = write_project(
        &fixture,
        "stale",
        "[project]\nname = \"stale\"\nid = \"gone\"\n\n[local.dev]\n",
    );
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["project", "get"])
        .assert()
        .failure();
    let error = stderr(&assert);
    assert!(
        error.contains("the project linked in helix.toml (gone)"),
        "{error}"
    );
    assert!(error.contains("project not found"), "{error}");
    assert!(error.contains("helix project link"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_cloud_without_flags_links_the_project_so_later_commands_work() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/workspaces",
        json!({"workspaces":[{"id":"ws-1","displayName":"Acme"}]}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .and(query_param("workspace_id", "ws-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "projects":[{"id":"p-graph","displayName":"Graph"}]
        })))
        .mount(&server)
        .await;
    get(
        &server,
        "/v1/projects/p-graph",
        json!({"id":"p-graph","workspaceId":"ws-1","displayName":"Graph"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/clusters"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"clusters":[]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/tenants"))
        .and(query_param("project_id", "p-graph"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tenants":[{"id":"t-1","name":"App"}]
        })))
        .mount(&server)
        .await;

    let dir = fixture.root().join("cloud-app");
    let assert = fixture
        .command()
        .args(["init", "--path"])
        .arg(&dir)
        .args(["cloud", "--no-skills"])
        .assert()
        .success();
    let chrome = stderr(&assert);
    assert!(chrome.contains("Using workspace Acme"), "{chrome}");
    assert!(chrome.contains("Using project Graph"), "{chrome}");
    assert!(chrome.contains("Linked App (tenant:t-1)"), "{chrome}");
    let config = fs::read_to_string(dir.join("helix.toml")).unwrap();
    assert!(config.contains("id = \"p-graph\""), "{config}");
    assert!(config.contains("database = \"tenant:t-1\""), "{config}");

    // Previously failed with "Pass --project": the project is linked now.
    fixture
        .command()
        .current_dir(&dir)
        .args(["database", "list"])
        .assert()
        .success();

    // `add cloud` defaults to the linked project but never re-adds a database.
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["add", "cloud", "--name", "second"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("every database in Graph is already in helix.toml"));
    // Naming it explicitly still adds it under another name.
    fixture
        .command()
        .current_dir(&dir)
        .args(["add", "cloud", "--name", "second", "--database", "App"])
        .assert()
        .success();
    let config = fs::read_to_string(dir.join("helix.toml")).unwrap();
    assert!(config.contains("[enterprise.second]"), "{config}");

    // A database from another project is refused rather than mislinked.
    get(
        &server,
        "/v1/tenants/t-9",
        json!({"id":"t-9","projectId":"p-other","workspaceId":"ws-1"}),
    )
    .await;
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args([
            "add",
            "cloud",
            "--name",
            "third",
            "--database",
            "tenant:t-9",
        ])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("but helix.toml is linked to p-graph"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_clusters_are_not_databases() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/clusters/shared-1",
        json!({"id":"shared-1","access":"shared"}),
    )
    .await;
    let assert = fixture
        .command()
        .args(["database", "get", "cluster:shared-1"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("is shared, so it is not a database"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_an_unreachable_cloud_database_without_failing() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/tenants/t-1",
        json!({"id":"t-1","name":"App","status":"RESOURCE_STATUS_ACTIVE"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/tenants/t-2"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({"message":"backend down"})))
        .mount(&server)
        .await;
    let dir = write_project(
        &fixture,
        "status",
        "[project]\nname = \"status\"\n\n[enterprise.production]\ndatabase = \"tenant:t-1\"\n\n[enterprise.staging]\ndatabase = \"tenant:t-2\"\n",
    );

    let assert = fixture
        .command()
        .current_dir(&dir)
        .arg("status")
        .assert()
        .success();
    let table = stdout(&assert);
    let production = table
        .lines()
        .find(|line| line.starts_with("production"))
        .unwrap();
    assert!(
        production.contains("active") && production.contains("App"),
        "{table}"
    );
    let staging = table
        .lines()
        .find(|line| line.starts_with("staging"))
        .unwrap();
    assert!(staging.contains("unreachable"), "{table}");
    assert!(
        stderr(&assert).contains("staging:"),
        "the cause is reported as a warning"
    );

    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["status", "--json"])
        .assert()
        .success();
    let report: Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(report["project"], "status");
    let instances = report["instances"].as_array().unwrap();
    assert_eq!(instances[0]["kind"], "cloud");
    assert_eq!(instances[0]["state"], "active");
    assert_eq!(instances[1]["state"], "unreachable");
    assert!(instances[1]["error"].as_str().unwrap().contains("503"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_logs_say_when_there_is_nothing_to_show() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/tenants/t-1/query-errors",
        json!({"errors":[]}),
    )
    .await;
    let dir = write_project(
        &fixture,
        "quiet-logs",
        "[project]\nname = \"quiet-logs\"\n\n[enterprise.production]\ndatabase = \"tenant:t-1\"\n",
    );
    let assert = fixture
        .command()
        .current_dir(&dir)
        .arg("logs")
        .assert()
        .success();
    assert_eq!(stdout(&assert), "");
    assert!(stderr(&assert).contains("No query errors on production"));

    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["logs", "--json"])
        .assert()
        .success();
    assert_eq!(stdout(&assert), "[]\n");

    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["logs", "--start", "yesterday"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("RFC 3339"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_never_takes_the_only_candidate_silently() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/workspaces",
        json!({"workspaces":[{"id":"ws-1","displayName":"Acme"}]}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "projects":[{"id":"p-only","displayName":"Only"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;
    let assert = fixture
        .command()
        .args(["project", "delete", "-y"])
        .assert()
        .failure();
    let error = stderr(&assert);
    assert!(error.contains("name the project to remove"), "{error}");
    assert!(error.contains("p-only"), "the candidate is listed: {error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_create_with_link_checks_for_helix_toml_first() {
    let (server, fixture) = cloud().await;
    let dir = fixture.root().join("not-a-project");
    fs::create_dir_all(&dir).unwrap();
    let assert = fixture
        .command()
        .current_dir(&dir)
        .args(["project", "create", "App", "--link"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("no helix.toml found"));
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "nothing was created"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_errors_are_not_mistaken_for_unknown_names() {
    let (server, fixture) = cloud().await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/p-1"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({"message":"backend down"})))
        .mount(&server)
        .await;
    let assert = fixture
        .command()
        .args(["project", "get", "p-1"])
        .assert()
        .failure();
    let error = stderr(&assert);
    assert!(
        error.contains("503") && error.contains("backend down"),
        "{error}"
    );
    assert!(!error.contains("no project matches"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_create_prints_the_key_even_when_the_tenant_is_missing() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/projects/p-1",
        json!({"id":"p-1","workspaceId":"ws-1","displayName":"Graph"}),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/v1/tenants"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"token":"only-once"})))
        .mount(&server)
        .await;
    let assert = fixture
        .command()
        .args([
            "database",
            "create",
            "App",
            "--project",
            "p-1",
            "--plan",
            "starter",
        ])
        .assert()
        .success();
    assert_eq!(stdout(&assert), "only-once\n");
}

/// Refreshing a session holds a file lock across an HTTP request. Concurrent
/// lookups must not block runtime threads on that lock, or the refresh never
/// completes. Every refresh here returns a token that is already inside the
/// refresh window, so each of the concurrent requests refreshes in turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_lookups_survive_session_refreshes() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    let soon = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 30;
    fs::write(
        fixture.helix_home().join("credentials"),
        json!({"access_token":"a","refresh_token":"r","expires_at":soon,"email":"owner@example.com"})
            .to_string(),
    )
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/v1/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accessToken":"a2","refreshToken":"r2","expiresAt": soon.to_string()
        })))
        .mount(&server)
        .await;
    let workspaces: Vec<Value> = (0..24)
        .map(|index| json!({"id": format!("ws-{index}"), "displayName": format!("W{index}")}))
        .collect();
    get(&server, "/v1/workspaces", json!({"workspaces": workspaces})).await;
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"projects":[]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/nowhere"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"message":"not found"})))
        .mount(&server)
        .await;
    let assert = fixture
        .command()
        .args(["project", "get", "nowhere"])
        .timeout(std::time::Duration::from_secs(30))
        .assert()
        .failure();
    assert!(stderr(&assert).contains("no project matches 'nowhere'"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_flags_constrain_typed_references_and_direct_lookups() {
    let (server, fixture) = cloud().await;
    get(
        &server,
        "/v1/projects/p-a",
        json!({"id":"p-a","workspaceId":"ws-1","displayName":"A"}),
    )
    .await;
    get(
        &server,
        "/v1/tenants/t-b",
        json!({"id":"t-b","name":"B","projectId":"p-b","workspaceId":"ws-1"}),
    )
    .await;
    get(
        &server,
        "/v1/clusters/c-b",
        json!({"id":"c-b","access":"dedicated","projectId":"p-b","workspaceId":"ws-1"}),
    )
    .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    for args in [
        vec![
            "database",
            "delete",
            "tenant:t-b",
            "--project",
            "p-a",
            "--yes",
        ],
        vec!["database", "get", "tenant:t-b", "--project", "p-a"],
        vec!["cluster", "indexes", "c-b", "--project", "p-a"],
    ] {
        let assert = fixture.command().args(&args).assert().failure();
        let error = stderr(&assert);
        assert!(error.contains("is not in project A"), "{args:?}: {error}");
    }

    // The same reference inside its own project is fine.
    get(
        &server,
        "/v1/projects/p-b",
        json!({"id":"p-b","workspaceId":"ws-1","displayName":"B"}),
    )
    .await;
    fixture
        .command()
        .args(["database", "get", "tenant:t-b", "--project", "p-b"])
        .assert()
        .success();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_missing_its_collection_is_an_error_not_empty() {
    let (server, fixture) = cloud().await;
    get(&server, "/v1/tenants/t-1/query-errors", json!({})).await;
    let dir = write_project(
        &fixture,
        "malformed-logs",
        "[project]\nname = \"malformed-logs\"\n\n[enterprise.production]\ndatabase = \"tenant:t-1\"\n",
    );
    let assert = fixture
        .command()
        .current_dir(&dir)
        .arg("logs")
        .assert()
        .failure();
    let error = stderr(&assert);
    assert!(error.contains("has no `errors` list"), "{error}");
    assert!(!error.contains("No query errors"), "{error}");
}

#[test]
fn chef_refuses_json_mode() {
    let fixture = CliFixture::new();
    let assert = fixture
        .command()
        .args(["chef", "--json"])
        .assert()
        .failure();
    assert!(stdout(&assert).is_empty());
    let error: Value = serde_json::from_str(stderr(&assert).trim()).unwrap();
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("no JSON result"));
}
