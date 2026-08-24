mod support;

use assert_cmd::assert::Assert;
use helix_db_testkit::transport_corpus::{expected_transport_observations, transport_query_corpus};
use serde_json::Value;
use std::fs;
use support::CliFixture;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_login_and_key_rotation_cover_success_and_error_responses() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    let sse_body = concat!(
        "data: {\"user_verification\":{\"user_code\":\"ABCD-1234\",",
        "\"verification_uri\":\"https://github.com/login/device\"}}\n\n",
        "data: {\"success\":{\"key\":\"admin-key\",\"user_id\":\"user-1\"}}\n\n"
    );
    Mock::given(method("POST"))
        .and(path("/github-login"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse_body, "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let login = stdout(fixture.command().args(["auth", "login"]).assert().success());
    assert!(login.contains("Logged in successfully"));
    let credentials = fs::read_to_string(fixture.helix_home().join("credentials")).unwrap();
    assert!(credentials.contains("helix_user_id=user-1"));
    assert!(credentials.contains("helix_user_key=admin-key"));

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/key"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "key": "cluster-key",
            "warning": "redeploy required"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let rotate = stdout(
        fixture
            .command()
            .args(["auth", "create-key", "cluster-1"])
            .assert()
            .success(),
    );
    assert!(rotate.contains("cluster-key"));
    assert!(rotate.contains("redeploy required"));

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/key"))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(serde_json::json!({"error": "expired"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let error = stderr(
        fixture
            .command()
            .args(["auth", "create-key", "cluster-1"])
            .assert()
            .failure(),
    );
    assert!(error.contains("Authentication failed"));

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/key"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let empty_error = stderr(
        fixture
            .command()
            .args(["auth", "create-key", "cluster-1"])
            .assert()
            .failure(),
    );
    assert!(empty_error.contains("request failed with status 500"));

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "key": "second-key"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let no_warning = stdout(
        fixture
            .command()
            .args(["auth", "create-key", "cluster-1"])
            .assert()
            .success(),
    );
    assert!(no_warning.contains("Previous cluster keys were revoked"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_login_surfaces_timeout_error_and_incomplete_sse_results() {
    for (body, expected) in [
        (
            "data: {\"device_code_timeout\":{\"message\":\"expired\"}}\n\n",
            "Login timeout: expired",
        ),
        (
            "data: {\"error\":{\"error\":\"device denied\"}}\n\n",
            "Login error: device denied",
        ),
        (
            "data: {\"success\":{\"key\":\"only-key\"}}\n\n",
            "credentials were not received",
        ),
    ] {
        let server = MockServer::start().await;
        let fixture = CliFixture::new().with_http_base(server.uri());
        Mock::given(method("POST"))
            .and(path("/github-login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(body, "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let error = stderr(fixture.command().args(["auth", "login"]).assert().failure());
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_configuration_commands_validate_requests_and_failures() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("user-1", "admin-key");

    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "id": "ws-1",
                "name": "Acme",
                "url_slug": "acme"
            }])),
        )
        .expect(1)
        .mount(&server)
        .await;
    let workspaces = stdout(
        fixture
            .command()
            .args(["workspace", "list", "--format", "json"])
            .assert()
            .success(),
    );
    let workspaces: Value = serde_json::from_str(&workspaces).unwrap();
    assert_eq!(workspaces[0]["id"], "ws-1");

    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces/ws-1/projects"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "id": "project-1",
                "name": "Graph App"
            }])),
        )
        .expect(1)
        .mount(&server)
        .await;
    let projects = stdout(
        fixture
            .command()
            .args([
                "project",
                "list",
                "--workspace-id",
                "ws-1",
                "--format",
                "json",
            ])
            .assert()
            .success(),
    );
    let projects: Value = serde_json::from_str(&projects).unwrap();
    assert_eq!(projects[0]["id"], "project-1");

    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces/ws-1/clusters"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "enterprise": [{"cluster_id": "cluster-1", "name": "Primary"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let clusters = stdout(
        fixture
            .command()
            .args([
                "cluster",
                "list",
                "--workspace-id",
                "ws-1",
                "--format",
                "json",
            ])
            .assert()
            .success(),
    );
    let clusters: Value = serde_json::from_str(&clusters).unwrap();
    assert_eq!(clusters[0]["cluster_id"], "cluster-1");

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces"))
        .respond_with(ResponseTemplate::new(503).set_body_string("maintenance"))
        .expect(1)
        .mount(&server)
        .await;
    let error = stderr(
        fixture
            .command()
            .args(["workspace", "list", "--format", "json"])
            .assert()
            .failure(),
    );
    assert!(error.contains("HTTP 503"));
    assert!(error.contains("maintenance"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_command_sends_body_headers_and_surfaces_http_errors() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new();
    let project = fixture.root().join("query-api-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();
    let request = serde_json::json!({
        "request_type": "read",
        "query": {"queries": [], "returns": []},
        "parameters": {}
    });
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .and(header("content-type", "application/json"))
        .and(header("x-helix-warm", "true"))
        .and(body_json(&request))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 3
        })))
        .expect(1)
        .mount(&server)
        .await;

    let output = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--json"])
            .arg(request.to_string())
            .args([
                "--host",
                &server.address().ip().to_string(),
                "--port",
                &server.address().port().to_string(),
                "--warm",
                "--compact",
            ])
            .assert()
            .success(),
    );
    assert_eq!(output.trim(), r#"{"count":3}"#);

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .respond_with(ResponseTemplate::new(422).set_body_string("invalid traversal"))
        .expect(1)
        .mount(&server)
        .await;
    let error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--json"])
            .arg(request.to_string())
            .args([
                "--host",
                &server.address().ip().to_string(),
                "--port",
                &server.address().port().to_string(),
            ])
            .assert()
            .failure(),
    );
    assert!(error.contains("HTTP 422"));
    assert!(error.contains("invalid traversal"));
}

fn write_enterprise_query_project(
    project: &std::path::Path,
    gateway_url: &str,
    auth_header: &str,
    auth_scheme: Option<&str>,
) {
    let auth_scheme = auth_scheme.map_or_else(String::new, |scheme| {
        format!("query_auth_scheme = \"{scheme}\"\n")
    });
    fs::create_dir_all(project).unwrap();
    fs::write(
        project.join("helix.toml"),
        format!(
            r#"[project]
name = "query-auth-project"

[enterprise.production]
cluster_id = "cluster-1"
gateway_url = "{gateway_url}"
query_auth_header = "{auth_header}"
query_auth_env = "HELIX_API_KEY"
{auth_scheme}
"#
        ),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enterprise_query_auth_formats_bearer_and_raw_headers_without_leaking_secrets() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new();
    let project = fixture.root().join("enterprise-query-auth-project");
    let request = serde_json::json!({
        "request_type": "read",
        "query": {"queries": [], "returns": []},
        "parameters": {}
    });

    write_enterprise_query_project(&project, &server.uri(), "Authorization", Some("bearer"));
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .and(header("authorization", "Bearer cluster-key"))
        .and(body_json(&request))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok":true})))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "production", "--json"])
        .arg(request.to_string())
        .env("HELIX_API_KEY", "cluster-key")
        .assert()
        .success();

    server.reset().await;
    write_enterprise_query_project(&project, &server.uri(), "x-api-key", None);
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .and(header("x-api-key", "cluster-key"))
        .and(body_json(&request))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok":true})))
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "production", "--json"])
        .arg(request.to_string())
        .env("HELIX_API_KEY", "cluster-key")
        .assert()
        .success();

    server.reset().await;
    let redirect_target = MockServer::start().await;
    write_enterprise_query_project(&project, &server.uri(), "x-api-key", None);
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .and(header("x-api-key", "cluster-key"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/redirected", redirect_target.uri())),
        )
        .expect(1)
        .mount(&server)
        .await;
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "production", "--json"])
        .arg(request.to_string())
        .env("HELIX_API_KEY", "cluster-key")
        .assert()
        .failure();
    assert!(redirect_target
        .received_requests()
        .await
        .unwrap()
        .is_empty());

    let missing = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "production", "--json"])
            .arg(request.to_string())
            .env_remove("HELIX_API_KEY")
            .assert()
            .failure(),
    );
    assert!(missing.contains("HELIX_API_KEY"));

    server.reset().await;
    write_enterprise_query_project(&project, &server.uri(), "Authorization", Some("bearer"));
    let secret = "Bearer\r\nsecret-value";
    let invalid_value = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "production", "--json"])
            .arg(request.to_string())
            .env("HELIX_API_KEY", secret)
            .assert()
            .failure(),
    );
    assert!(!invalid_value.contains("secret-value"));
    assert!(server.received_requests().await.unwrap().is_empty());

    write_enterprise_query_project(&project, &server.uri(), "Authorization", Some("token"));
    let invalid_config = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "production", "--json"])
            .arg(request.to_string())
            .env("HELIX_API_KEY", "cluster-key")
            .assert()
            .failure(),
    );
    assert!(invalid_config.contains("token"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_command_preserves_the_shared_transport_corpus() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new();
    let project = fixture.root().join("query-transport-corpus-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    let corpus = transport_query_corpus();
    let expected_observations = expected_transport_observations();
    assert_eq!(corpus.len(), expected_observations.len());
    for (step, expected) in corpus.into_iter().zip(expected_observations) {
        server.reset().await;
        let request = step.request();
        Mock::given(method("POST"))
            .and(path("/v2/query"))
            .and(header("content-type", "application/json"))
            .and(body_json(&request))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected.clone()))
            .expect(1)
            .mount(&server)
            .await;

        let output = stdout(
            fixture
                .command()
                .current_dir(&project)
                .args(["query", "dev", "--json"])
                .arg(serde_json::to_string(&request).unwrap())
                .args([
                    "--host",
                    &server.address().ip().to_string(),
                    "--port",
                    &server.address().port().to_string(),
                    "--compact",
                ])
                .assert()
                .success(),
        );
        let observed: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(observed, expected, "{} corpus response", step.name());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enterprise_logs_send_authenticated_range_and_surface_errors() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("user-1", "admin-key");
    let project = fixture.root().join("logs-api-project");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join("helix.toml"),
        r#"[project]
name = "logs-api-project"

[enterprise.production]
cluster_id = "cluster-1"
gateway_url = "https://gateway.example.com"
"#,
    )
    .unwrap();

    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/logs/range"))
        .and(query_param("start_time", "1767225600"))
        .and(query_param("end_time", "1767229200"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "logs": [{"message": "first"}, {"message": "second"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let logs = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args([
                "logs",
                "production",
                "--range",
                "--start",
                "2026-01-01T00:00:00Z",
                "--end",
                "2026-01-01T01:00:00Z",
            ])
            .assert()
            .success(),
    );
    assert!(logs.contains("first"));
    assert!(logs.contains("second"));

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/logs/range"))
        .respond_with(ResponseTemplate::new(500).set_body_string("log backend failed"))
        .expect(1)
        .mount(&server)
        .await;
    let error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["logs", "production", "--range"])
            .assert()
            .failure(),
    );
    assert!(error.contains("log backend failed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_check_uses_mocked_service() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    Mock::given(method("GET"))
        .and(path("/__helix_test/github/releases/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": "v99.0.0",
            "name": "Future",
            "html_url": "https://example.com/release"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let welcome = stdout(
        fixture
            .command()
            .env_remove("HELIX_NO_UPDATE_CHECK")
            .env_remove("HELIX_DISABLE_UPDATE_CHECK")
            .assert()
            .success(),
    );
    assert!(welcome.contains("Update available"));
    assert!(welcome.contains("99.0.0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_update_check_baselines_caches_refreshes_and_throttles_errors() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new()
        .with_fake_tools()
        .with_http_base(server.uri());
    let lockfile = fixture.skills_lockfile();
    fs::create_dir_all(lockfile.parent().unwrap()).unwrap();
    fs::write(&lockfile, r#"{"sources":["HelixDB/skills"]}"#).unwrap();

    Mock::given(method("GET"))
        .and(path("/__helix_test/github/releases/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": format!("v{}", env!("CARGO_PKG_VERSION")),
            "name": "Current",
            "html_url": "https://example.com/current"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/__helix_test/github/skills/commits"))
        .and(query_param("per_page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"sha":"new-sha"}
        ])))
        .expect(2)
        .mount(&server)
        .await;

    let baseline = stdout(
        fixture
            .command()
            .env_remove("HELIX_NO_UPDATE_CHECK")
            .env_remove("HELIX_DISABLE_UPDATE_CHECK")
            .assert()
            .success(),
    );
    assert!(!baseline.contains("Helix skills update available"));
    let cache_path = fixture.helix_home().join("skills_cache.toml");
    let cache = fs::read_to_string(&cache_path).unwrap();
    assert!(cache.contains("applied_sha = \"new-sha\""));
    assert!(cache.contains("update_available = false"));

    fs::write(
        &cache_path,
        "last_check = 0\napplied_sha = \"old-sha\"\nupdate_available = false\n",
    )
    .unwrap();
    let stale = stdout(
        fixture
            .command()
            .env_remove("HELIX_NO_UPDATE_CHECK")
            .env_remove("HELIX_DISABLE_UPDATE_CHECK")
            .assert()
            .success(),
    );
    assert!(stale.contains("Helix skills update available"));

    let cached = stdout(
        fixture
            .command()
            .env_remove("HELIX_NO_UPDATE_CHECK")
            .env_remove("HELIX_DISABLE_UPDATE_CHECK")
            .assert()
            .success(),
    );
    assert!(cached.contains("Helix skills update available"));

    fixture
        .command()
        .args(["skills", "update"])
        .assert()
        .success();
    assert!(!cache_path.exists());

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/__helix_test/github/skills/commits"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let unavailable = stdout(
        fixture
            .command()
            .env_remove("HELIX_NO_UPDATE_CHECK")
            .env_remove("HELIX_DISABLE_UPDATE_CHECK")
            .assert()
            .success(),
    );
    assert!(!unavailable.contains("Helix skills update available"));
    assert!(fs::read_to_string(cache_path)
        .unwrap()
        .contains("update_available = false"));
}
