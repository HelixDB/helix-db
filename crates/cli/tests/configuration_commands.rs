mod support;

use assert_cmd::assert::Assert;
use serde_json::{json, Value};
use support::CliFixture;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).unwrap()
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).unwrap()
}

fn json_stdout(assert: Assert) -> Value {
    let out = stdout(assert);
    serde_json::from_str(&out).unwrap_or_else(|error| panic!("not JSON ({error}): {out}"))
}

async fn get(server: &MockServer, at: &str, body: Value) {
    Mock::given(method("GET"))
        .and(path(at))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// One workspace (`ws-1`, "Acme") with one project (`project-1`, "Graph").
async fn mount_acme(server: &MockServer) {
    get(
        server,
        "/v1/workspaces",
        json!({"workspaces":[{"id":"ws-1","slug":"acme","displayName":"Acme"}]}),
    )
    .await;
    get(
        server,
        "/v1/projects/project-1",
        json!({"id":"project-1","workspaceId":"ws-1","slug":"graph","displayName":"Graph"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .and(query_param("workspace_id", "ws-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "projects":[{"id":"project-1","slug":"graph","displayName":"Graph"}]
        })))
        .mount(server)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_and_service_credentials_are_displayed_once_and_never_stored() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("owner@example.com", "session-access");
    mount_acme(&server).await;

    Mock::given(method("POST"))
        .and(path("/v1/tenants"))
        .and(header("authorization", "Bearer session-access"))
        .and(body_json(json!({
            "projectId":"project-1", "clusterId":"", "name":"App", "slug":"app",
            "planCode":"starter"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tenant":{"id":"tenant-1","projectId":"project-1","name":"App"},
            "token":"default-database-secret", "key":{"id":"default-key-1"}
        })))
        .expect(1)
        .mount(&server)
        .await;
    // The slug is derived from the name, and the project resolves by name.
    let created = json_stdout(
        fixture
            .command()
            .args(["database", "create", "App", "--plan", "starter"])
            .args(["--project", "Graph", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(created["tenant"]["id"], "tenant-1");
    assert_eq!(created["token"], "default-database-secret");

    get(
        &server,
        "/v1/tenants/tenant-1",
        json!({"id":"tenant-1","name":"App","projectId":"project-1","workspaceId":"ws-1"}),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/v1/tenants/tenant-1/keys"))
        .and(body_json(json!({
            "name":"application", "access":"DATABASE_KEY_ACCESS_READ_WRITE"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token":"database-secret", "key":{"id":"key-1"}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let key = fixture
        .command()
        .args(["database", "key", "create", "--database", "tenant:tenant-1"])
        .args(["--name", "application", "--access", "read-write"])
        .assert()
        .success();
    // Only the token reaches stdout, so it can be piped; the warning is chrome.
    let output = key.get_output();
    assert!(String::from_utf8_lossy(&output.stderr).contains("shown once"));
    assert_eq!(String::from_utf8_lossy(&output.stdout), "database-secret\n");

    Mock::given(method("POST"))
        .and(path("/v1/workspaces/ws-1/service-credentials"))
        .and(body_json(json!({
            "workspaceId":"ws-1",
            "name":"automation",
            "grants":[{"projectId":"project-1","permissions":["SERVICE_CREDENTIAL_PERMISSION_DATABASE_QUERY_READ"]}],
            "expiresAt":null
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "credential":{"id":"svc-1"}, "token":"service-secret"
        })))
        .expect(1)
        .mount(&server)
        .await;
    // The only workspace is picked without --workspace.
    let credential = stdout(
        fixture
            .command()
            .args(["service-credential", "create", "--name", "automation"])
            .args(["--grant", "project-1=query-read"])
            .assert()
            .success(),
    );
    assert_eq!(credential, "service-secret\n");

    let stored = std::fs::read_to_string(fixture.helix_home().join("credentials")).unwrap();
    assert!(!stored.contains("default-database-secret"));
    assert!(!stored.contains("database-secret"));
    assert!(!stored.contains("service-secret"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_project_cluster_and_auth_commands_cover_crud_contracts() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("owner@example.com", "session-access");
    mount_acme(&server).await;

    let status = stdout(
        fixture
            .command()
            .args(["auth", "status"])
            .assert()
            .success(),
    );
    assert!(status.contains("owner@example.com"), "{status}");
    assert!(status.contains("Acme"), "{status}");
    let status = json_stdout(
        fixture
            .command()
            .args(["auth", "status", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(status["email"], "owner@example.com");
    assert_eq!(status["workspaces"][0], json!({"id":"ws-1","name":"Acme"}));

    let workspaces = stdout(
        fixture
            .command()
            .args(["workspace", "list"])
            .assert()
            .success(),
    );
    assert!(
        workspaces.lines().next().unwrap().contains("NAME"),
        "{workspaces}"
    );
    assert!(
        workspaces.contains("Acme") && workspaces.contains("ws-1"),
        "{workspaces}"
    );
    // A bare group lists, and get resolves by slug.
    assert_eq!(
        stdout(fixture.command().arg("workspace").assert().success()),
        workspaces
    );
    let workspace = json_stdout(
        fixture
            .command()
            .args(["workspace", "get", "acme", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(workspace["id"], "ws-1");

    let project = stdout(
        fixture
            .command()
            .args(["project", "get", "project-1"])
            .assert()
            .success(),
    );
    assert!(
        project.contains("Graph") && project.contains("ws-1"),
        "{project}"
    );
    let projects = json_stdout(
        fixture
            .command()
            .args(["project", "list", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(projects[0]["id"], "project-1");
    assert!(
        projects[0].get("workspaceId").is_none(),
        "--json is verbatim; the listing omitted workspaceId"
    );

    Mock::given(method("POST"))
        .and(path("/v1/projects"))
        .and(body_json(json!({
            "workspaceId":"ws-1","slug":"new-graph","displayName":"New Graph"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"project-2","workspaceId":"ws-1","displayName":"New Graph"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let created = json_stdout(
        fixture
            .command()
            .args(["project", "create", "New Graph", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(created["id"], "project-2");

    Mock::given(method("DELETE"))
        .and(path("/v1/projects/project-1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let deleted = json_stdout(
        fixture
            .command()
            .args(["project", "delete", "project-1", "--yes", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(
        deleted,
        json!({"deleted":{"kind":"project","id":"project-1"}})
    );

    let project_dir = fixture.root().join("linked-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project_dir)
        .args(["local", "--no-skills"])
        .assert()
        .success();
    let before = std::fs::read_to_string(project_dir.join("helix.toml")).unwrap();
    // No project argument: the only project in the only workspace is linked.
    fixture
        .command()
        .current_dir(&project_dir)
        .args(["project", "link"])
        .assert()
        .success();
    let linked = std::fs::read_to_string(project_dir.join("helix.toml")).unwrap();
    assert!(linked.contains("id = \"project-1\""), "{linked}");
    assert!(linked.contains("workspace_id = \"ws-1\""), "{linked}");
    assert!(
        linked.contains("name = \"linked-project\""),
        "linking keeps the local project name, which names containers: {before}\n{linked}"
    );

    Mock::given(method("GET"))
        .and(path("/v1/clusters"))
        .and(query_param("workspace_id", "ws-1"))
        .and(query_param("project_id", "project-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "clusters":[{"id":"cluster-1","displayName":"Primary","access":"CLUSTER_ACCESS_DEDICATED","status":"RESOURCE_STATUS_ACTIVE"}]
        })))
        .mount(&server)
        .await;
    // The linked project scopes the list without any flags.
    let clusters = stdout(
        fixture
            .command()
            .current_dir(&project_dir)
            .args(["cluster", "list"])
            .assert()
            .success(),
    );
    assert!(clusters.contains("Primary"), "{clusters}");
    assert!(
        clusters.contains("dedicated") && clusters.contains("active"),
        "{clusters}"
    );

    get(
        &server,
        "/v1/clusters/cluster-1",
        json!({"id":"cluster-1","displayName":"Primary","access":"dedicated","projectId":"project-1","workspaceId":"ws-1"}),
    )
    .await;
    let cluster = json_stdout(
        fixture
            .command()
            .args(["cluster", "get", "cluster-1", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(cluster["access"], "dedicated", "passed through verbatim");

    get(
        &server,
        "/v1/clusters/cluster-1/indexes",
        json!({"indexes":[{"name":"by_email"}]}),
    )
    .await;
    let indexes = json_stdout(
        fixture
            .command()
            .args(["cluster", "indexes", "cluster-1", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(indexes["indexes"][0]["name"], "by_email");

    Mock::given(method("POST"))
        .and(path("/v1/auth/logout"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .args(["auth", "logout"])
        .assert()
        .success();
    assert!(!fixture.helix_home().join("credentials").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_commands_cover_discovery_lifecycle_indexes_and_keys() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("owner@example.com", "session-access");
    mount_acme(&server).await;

    Mock::given(method("GET"))
        .and(path("/v1/clusters"))
        .and(query_param("workspace_id", "ws-1"))
        .and(query_param("project_id", "project-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "clusters":[
                {"id":"cluster-1","displayName":"Dedicated","access":"dedicated"},
                {"id":"shared-1","access":"shared"}
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/tenants"))
        .and(query_param("project_id", "project-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tenants":[{"id":"tenant-1","name":"App","status":"RESOURCE_STATUS_ACTIVE"}]
        })))
        .mount(&server)
        .await;
    let databases = json_stdout(
        fixture
            .command()
            .args(["database", "list", "--project", "project-1", "--json"])
            .assert()
            .success(),
    );
    let databases = databases.as_array().unwrap();
    assert_eq!(databases.len(), 2, "shared clusters are not databases");
    assert_eq!(databases[0]["kind"], "dedicated");
    assert_eq!(databases[1]["kind"], "tenant");
    assert!(
        databases[1].get("projectId").is_none(),
        "--json is verbatim; the listing omitted projectId"
    );

    // Without --project, the only project of the only workspace is used.
    let table = stdout(
        fixture
            .command()
            .args(["database", "list"])
            .assert()
            .success(),
    );
    assert!(
        table.contains("tenant:tenant-1") && table.contains("active"),
        "{table}"
    );

    get(
        &server,
        "/v1/tenants/tenant-1",
        json!({"id":"tenant-1","name":"App","projectId":"project-1","workspaceId":"ws-1"}),
    )
    .await;
    get(
        &server,
        "/v1/clusters/cluster-1",
        json!({"id":"cluster-1","displayName":"Dedicated","access":"dedicated","projectId":"project-1","workspaceId":"ws-1"}),
    )
    .await;
    for (target, kind) in [
        ("tenant:tenant-1", "tenant"),
        ("cluster:cluster-1", "dedicated"),
    ] {
        let database = json_stdout(
            fixture
                .command()
                .args(["database", "get", target, "--json"])
                .assert()
                .success(),
        );
        assert_eq!(database["kind"], kind);
    }
    // Names resolve within the project.
    let database = json_stdout(
        fixture
            .command()
            .args(["database", "get", "app", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(database["id"], "tenant-1");

    for endpoint in [
        "/v1/tenants/tenant-1/indexes",
        "/v1/clusters/cluster-1/indexes",
    ] {
        get(&server, endpoint, json!({"indexes":[{"name":"by_email"}]})).await;
    }
    for target in ["tenant:tenant-1", "cluster:cluster-1"] {
        let indexes = json_stdout(
            fixture
                .command()
                .args(["database", "indexes", target, "--json"])
                .assert()
                .success(),
        );
        assert_eq!(indexes["indexes"][0]["name"], "by_email");
    }

    Mock::given(method("POST"))
        .and(path("/v1/tenants"))
        .and(body_json(json!({
            "projectId":"project-1","clusterId":"cluster-1","name":"Dedicated DB",
            "slug":"dedicated-db","planCode":""
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tenant":{"id":"tenant-2"},"token":"dedicated-secret"
        })))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .args([
            "database",
            "create",
            "Dedicated DB",
            "--cluster",
            "cluster-1",
        ])
        .args(["--project", "project-1", "--json"])
        .assert()
        .success();

    get(
        &server,
        "/v1/tenants/tenant-1/keys",
        json!({"keys":[{"id":"key-1","name":"application","access":"DATABASE_KEY_ACCESS_READ_ONLY"}]}),
    )
    .await;
    let keys = stdout(
        fixture
            .command()
            .args(["database", "key", "list", "--database", "tenant:tenant-1"])
            .assert()
            .success(),
    );
    assert!(
        keys.contains("application") && keys.contains("key-1"),
        "{keys}"
    );
    assert!(keys.contains("read-only"), "{keys}");

    Mock::given(method("DELETE"))
        .and(path("/v1/tenants/tenant-1/keys/key-1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&server)
        .await;
    // A key revokes by ID or by name.
    for key in ["key-1", "application"] {
        fixture
            .command()
            .args([
                "database",
                "key",
                "revoke",
                key,
                "--database",
                "tenant:tenant-1",
                "--yes",
            ])
            .assert()
            .success();
    }

    Mock::given(method("DELETE"))
        .and(path("/v1/tenants/tenant-1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .args(["database", "delete", "tenant:tenant-1", "--yes"])
        .assert()
        .success();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_credential_and_generic_api_commands_cover_all_methods() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("owner@example.com", "session-access");
    mount_acme(&server).await;

    get(
        &server,
        "/v1/workspaces/ws-1/service-credentials",
        json!({"credentials":[{"id":"svc-1","name":"automation"}]}),
    )
    .await;
    let credentials = json_stdout(
        fixture
            .command()
            .args([
                "service-credential",
                "list",
                "--workspace",
                "Acme",
                "--json",
            ])
            .assert()
            .success(),
    );
    assert_eq!(credentials[0]["id"], "svc-1");

    let credential = stdout(
        fixture
            .command()
            .args(["service-credential", "get", "automation"])
            .assert()
            .success(),
    );
    assert!(credential.contains("svc-1"), "{credential}");

    Mock::given(method("PATCH"))
        .and(path("/v1/workspaces/ws-1/service-credentials/svc-1"))
        .and(body_json(json!({
            "workspaceId":"ws-1",
            "id":"svc-1",
            "replaceGrants":true,
            "grants":[{"projectId":"project-1","permissions":[
                "SERVICE_CREDENTIAL_PERMISSION_PROJECT_READ",
                "SERVICE_CREDENTIAL_PERMISSION_PROJECT_WRITE"
            ]}],
            "name":"renamed",
            "replaceExpiry":true,
            "expiresAt":"2030-01-01T00:00:00Z"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"svc-1","name":"renamed"
        })))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .args(["service-credential", "update", "svc-1", "--name", "renamed"])
        .args(["--grant", "project-1=project-read,project-write"])
        .args(["--expires-at", "2030-01-01T00:00:00Z"])
        .assert()
        .success();

    Mock::given(method("DELETE"))
        .and(path("/v1/workspaces/ws-1/service-credentials/svc-1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .args(["service-credential", "revoke", "automation", "--yes"])
        .assert()
        .success();

    for (verb, endpoint) in [
        ("GET", "/v1/test-get"),
        ("POST", "/v1/test-post"),
        ("PATCH", "/v1/test-patch"),
        ("DELETE", "/v1/test-delete"),
    ] {
        Mock::given(method(verb))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "method":verb
            })))
            .expect(1)
            .mount(&server)
            .await;
    }
    let pretty = stdout(
        fixture
            .command()
            .args(["api", "get", "/v1/test-get"])
            .assert()
            .success(),
    );
    assert_eq!(pretty, "{\n  \"method\": \"GET\"\n}\n");
    let compact = stdout(
        fixture
            .command()
            .args([
                "api",
                "post",
                "/v1/test-post",
                "--body",
                r#"{"value":1}"#,
                "--json",
            ])
            .assert()
            .success(),
    );
    assert_eq!(compact, "{\"method\":\"POST\"}\n");
    fixture
        .command()
        .args(["api", "patch", "/v1/test-patch", "--body", r#"{"value":2}"#])
        .assert()
        .success();
    fixture
        .command()
        .args(["api", "delete", "/v1/test-delete"])
        .assert()
        .success();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_command_validation_rejects_bad_requests_before_dispatch() {
    // Every route is unmounted, so any request would 404; each case must fail
    // with its own message instead.
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("owner@example.com", "session-access");
    for (args, expected) in [
        (
            vec!["database", "create", "App", "--project", "project-1"],
            "--plan is required",
        ),
        (
            vec![
                "database",
                "create",
                "App",
                "--cluster",
                "c",
                "--plan",
                "starter",
            ],
            "--plan only applies",
        ),
        (
            vec!["database", "delete", "cluster:cluster-1", "--yes"],
            "only deletes tenant databases",
        ),
        (
            vec![
                "database",
                "key",
                "revoke",
                "key-1",
                "--database",
                "tenant:tenant-1",
            ],
            "needs confirmation",
        ),
        (vec!["project", "delete", "project-1"], "needs confirmation"),
        (
            vec!["service-credential", "update", "svc-1"],
            "nothing to update",
        ),
        (
            vec![
                "service-credential",
                "create",
                "--name",
                "svc",
                "--grant",
                "p=query-write",
            ],
            "query-write requires query-read",
        ),
        (
            vec!["api", "get", "https://example.com/v1/projects"],
            "/v1/",
        ),
        (vec!["api", "get", "/v2/query"], "/v1/"),
        (
            vec!["api", "post", "/v1/projects", "--body", "not-json"],
            "invalid JSON body",
        ),
    ] {
        let error = stderr(fixture.command().args(&args).assert().failure());
        assert!(error.contains(expected), "{args:?}: {error}");
    }
    assert!(server.received_requests().await.unwrap().is_empty());

    // clap rejects these before the CLI runs at all.
    for args in [
        vec!["service-credential", "create", "--name", "svc"],
        vec![
            "service-credential",
            "update",
            "svc-1",
            "--expires-at",
            "2030-01-01T00:00:00Z",
            "--clear-expiry",
        ],
    ] {
        fixture.command().args(args).assert().failure();
    }
}
