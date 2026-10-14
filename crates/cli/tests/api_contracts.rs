mod support;

use assert_cmd::assert::Assert;
use base64::Engine as _;
use helix_db_testkit::transport_corpus::{expected_transport_observations, transport_query_corpus};
use serde_json::Value;
use std::fs;
use support::CliFixture;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).unwrap()
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_query_derives_the_sole_linked_database_and_uses_the_session_broker() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("user@example.com", "session-access");
    let project = fixture.root().join("cloud-query");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join("helix.toml"),
        r#"[project]
name = "cloud-query"

[enterprise.production]
database = "tenant:tenant-1"
"#,
    )
    .unwrap();
    let request = serde_json::json!({
        "request_type":"read",
        "query":{"queries":[],"returns":[]},
        "parameters":{},
        "parameter_types":{}
    });
    let encoded_request =
        base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&request).unwrap());
    let encoded_response = base64::engine::general_purpose::STANDARD.encode(br#"{"count":3}"#);
    Mock::given(method("POST"))
        .and(path("/v1/databases:query-read"))
        .and(header("authorization", "Bearer session-access"))
        .and(body_json(serde_json::json!({
            "database":{"tenantId":"tenant-1"},
            "queryJson":encoded_request,
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "responseJson":encoded_response,
            "statusCode":200,
            "contentType":"application/json"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let output = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "--body"])
            .arg(request.to_string())
            .arg("--json")
            .assert()
            .success(),
    );
    assert_eq!(output.trim(), r#"{"count":3}"#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_query_accepts_an_explicit_typed_database_target() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("user@example.com", "session-access");
    // A typed database target needs no helix.toml at all.
    let project = fixture.root().join("no-project-here");
    fs::create_dir_all(&project).unwrap();
    let request = serde_json::json!({
        "request_type":"read",
        "query":{"queries":[],"returns":[]}
    });
    Mock::given(method("POST"))
        .and(path("/v1/databases:query-read"))
        .and(header("authorization", "Bearer session-access"))
        .and(body_json(serde_json::json!({
            "database":{"tenantId":"tenant-2"},
            "queryJson":base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&request).unwrap()),
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "responseJson":base64::engine::general_purpose::STANDARD.encode(br#"{"ok":true}"#),
            "statusCode":200,
            "contentType":"application/json"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let output = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "tenant:tenant-2", "--body"])
            .arg(request.to_string())
            .arg("--json")
            .assert()
            .success(),
    );
    assert_eq!(output.trim(), r#"{"ok":true}"#);
}

#[test]
fn query_requires_an_explicit_target_when_the_project_is_ambiguous() {
    let fixture = CliFixture::new();
    let project = fixture.root().join("ambiguous-query-target");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join("helix.toml"),
        r#"[project]
name = "ambiguous-query-target"

[local.preview]

[enterprise.production]
database = "tenant:tenant-1"
"#,
    )
    .unwrap();

    let error = stderr(
        fixture
            .command()
            .current_dir(project)
            .args([
                "query",
                "--body",
                r#"{"request_type":"read","query":{"queries":[],"returns":[]}}"#,
            ])
            .assert()
            .failure(),
    );
    assert!(error.contains("no default query target"), "{error}");
    assert!(error.contains("candidates:"), "{error}");
    assert!(error.contains("preview"), "{error}");
    assert!(error.contains("production"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_typed_pre_dispatch_rejection_refreshes_and_retries() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("user@example.com", "old-access");

    Mock::given(method("GET"))
        .and(path("/v1/workspaces"))
        .and(header("authorization", "Bearer old-access"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "details":[{"reason":"SESSION_REJECTED_PRE_DISPATCH"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/auth/refresh"))
        .and(body_json(
            serde_json::json!({"refreshToken":"refresh-token"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken":"new-access",
            "refreshToken":"new-refresh",
            "expiresAt":i64::MAX
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/workspaces"))
        .and(header("authorization", "Bearer new-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "workspaces":[]
        })))
        .expect(1)
        .mount(&server)
        .await;

    fixture
        .command()
        .args(["workspace", "list", "--json"])
        .assert()
        .success();

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/project-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":"project-1","workspaceId":"ws-1"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/tenants"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "message":"ordinary handler rejection"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let error = stderr(
        fixture
            .command()
            .args(["database", "create", "db", "--project", "project-1"])
            .args(["--plan", "starter"])
            .assert()
            .failure(),
    );
    assert!(error.contains("ordinary handler rejection"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_and_query_error_paths_use_bearer_session() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    fixture.write_credentials("user@example.com", "session-access");
    Mock::given(method("GET"))
        .and(path("/v1/workspaces"))
        .and(header("authorization", "Bearer session-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "workspaces":[{"id":"ws-1","displayName":"Acme"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let output = stdout(
        fixture
            .command()
            .args(["workspace", "list", "--json"])
            .assert()
            .success(),
    );
    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value[0]["id"], "ws-1");

    let project = fixture.root().join("cloud-logs");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join("helix.toml"),
        r#"[project]
name = "cloud-logs"
[enterprise.production]
database = "cluster:cluster-1"
"#,
    )
    .unwrap();
    Mock::given(method("GET"))
        .and(path("/v1/clusters/cluster-1/query-errors"))
        .and(query_param("startTime", "1767225600"))
        .and(query_param("endTime", "1767229200"))
        .and(header("authorization", "Bearer session-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "errors":[{"timestamp":"2026-01-01T00:30:00Z","queryName":"read","output":"failed"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let logs = stdout(
        fixture
            .command()
            .current_dir(project)
            .args([
                "logs",
                "production",
                "--start",
                "2026-01-01T00:00:00Z",
                "--end",
                "2026-01-01T01:00:00Z",
            ])
            .assert()
            .success(),
    );
    let row = logs
        .lines()
        .find(|line| line.contains("read"))
        .unwrap_or_default();
    assert!(
        row.contains("2026-01-01T00:30:00Z") && row.contains("failed"),
        "{logs}"
    );
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
                .args(["query", "dev", "--body"])
                .arg(serde_json::to_string(&request).unwrap())
                .args([
                    "--host",
                    &server.address().ip().to_string(),
                    "--port",
                    &server.address().port().to_string(),
                    "--json",
                ])
                .assert()
                .success(),
        );
        let observed: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(observed, expected, "{} corpus response", step.name());
    }
}
