mod support;

use assert_cmd::assert::Assert;
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: &Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: &Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

/// A server whose health and diagnostics answer with `checks`.
async fn server_reporting(checks: serde_json::Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"ready": true, "mode": "writer"})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/diagnostics"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"uptime_secs": 7200, "checks": checks})),
        )
        .mount(&server)
        .await;
    server
}

fn missing_index_warning() -> serde_json::Value {
    serde_json::json!([{
        "id": "queries.missing_indexes",
        "category": "queries",
        "status": "warn",
        "summary": "1 filter scans without an index",
        "detail": "User.email (node equality): 3 queries, last seen 2 s ago",
        "fix": "Create each index; it builds in the background and applies once active:\nhelix query <instance> -e 'writeBatch()'"
    }])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doctor_diagnoses_a_server_by_url_without_a_project() {
    let server = server_reporting(missing_index_warning()).await;
    let fixture = CliFixture::new();

    let human = fixture
        .command()
        .current_dir(fixture.root())
        .args(["doctor", "--url", &format!("{}/", server.uri())])
        .assert()
        .success();
    let output = stdout(&human);
    assert!(
        output.starts_with(&format!("Helix doctor {} · up 2h\n", server.uri())),
        "{output}"
    );
    assert!(output.contains("✓ The server answers at"), "{output}");
    assert!(
        output.contains("▲ 1 filter scans without an index"),
        "{output}"
    );
    assert!(
        output.contains("helix query <instance> -e 'writeBatch()'"),
        "a URL target keeps the placeholder: {output}"
    );
    assert!(
        output.contains("Setup works, with room to improve"),
        "{output}"
    );

    let json = fixture
        .command()
        .current_dir(fixture.root())
        .args(["doctor", "--url", &server.uri(), "--json"])
        .assert()
        .success();
    let report: serde_json::Value = serde_json::from_str(&stdout(&json)).unwrap();
    assert_eq!(report["target"]["kind"], "server");
    assert_eq!(report["target"]["url"], server.uri());
    assert_eq!(report["uptime_secs"], 7200);
    let ids = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| check["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        ["cli.version", "server.reachable", "queries.missing_indexes"]
    );
    assert_eq!(
        report["checks"][0]["status"], "skip",
        "update checks are off in tests"
    );
    assert_eq!(
        report["summary"],
        serde_json::json!({"pass": 1, "warn": 1, "fail": 0, "skip": 1})
    );
    assert!(stderr(&json).is_empty(), "--json prints no chrome");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_check_exits_nonzero_after_printing_the_report() {
    let server = server_reporting(serde_json::json!([{
        "id": "storage.data_device",
        "category": "storage",
        "status": "fail",
        "summary": "HELIX_DATA_DIR is on ephemeral instance storage",
        "detail": "/var/lib/helix is on nvme1n1 (Amazon EC2 NVMe Instance Storage).",
        "fix": "Keep data on an EBS volume or in S3."
    }]))
    .await;
    let fixture = CliFixture::new();

    let human = fixture
        .command()
        .args(["doctor", "--url", &server.uri()])
        .assert()
        .failure()
        .code(1);
    assert!(stdout(&human).contains("✗ HELIX_DATA_DIR is on ephemeral instance storage"));
    assert!(stdout(&human).contains("Problems need fixing · 1 failure"));
    assert!(
        stderr(&human).contains("1 check failed"),
        "{}",
        stderr(&human)
    );

    let json = fixture
        .command()
        .args(["doctor", "--url", &server.uri(), "--json"])
        .assert()
        .failure()
        .code(1);
    let report: serde_json::Value = serde_json::from_str(&stdout(&json)).unwrap();
    assert_eq!(report["summary"]["fail"], 1);
    let error: serde_json::Value = serde_json::from_str(stderr(&json).trim()).unwrap();
    assert_eq!(error["error"]["message"], "1 check failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doctor_checks_a_running_local_instance_and_names_it_in_fixes() {
    let server = server_reporting(missing_index_warning()).await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("doctor-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .arg("--no-skills")
        .assert()
        .success();

    let ps_output = format!(
        "helix-doctor-project-dev\tUp 5 minutes\tlocalhost:{}",
        server.address().port()
    );
    let assert = fixture
        .command()
        .current_dir(&project)
        .args(["doctor", "--json"])
        .env("HELIX_TEST_RUNTIME_PS_OUTPUT", &ps_output)
        .assert()
        .success();
    let report: serde_json::Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(report["target"]["kind"], "local");
    assert_eq!(report["target"]["name"], "dev");
    let statuses = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| {
            (
                check["id"].as_str().unwrap().to_owned(),
                check["status"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let expected = [
        ("cli.version", "skip"),
        ("runtime.daemon", "pass"),
        ("runtime.container", "pass"),
        ("runtime.image", "pass"),
        ("server.reachable", "pass"),
        ("queries.missing_indexes", "warn"),
    ]
    .map(|(id, status)| (id.to_owned(), status.to_owned()));
    assert_eq!(statuses, expected);
    assert!(
        report["checks"][5]["fix"]
            .as_str()
            .unwrap()
            .ends_with("helix query dev -e 'writeBatch()'"),
        "{}",
        report["checks"][5]
    );
}

#[test]
fn a_stopped_local_instance_fails_without_contacting_the_server() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("stopped-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    let assert = fixture
        .command()
        .current_dir(&project)
        .args(["doctor", "dev", "--json"])
        .assert()
        .failure()
        .code(1);
    let report: serde_json::Value = serde_json::from_str(&stdout(&assert)).unwrap();
    let container = &report["checks"][2];
    assert_eq!(container["id"], "runtime.container");
    assert_eq!(container["status"], "fail");
    assert_eq!(container["fix"], "Run `helix start dev`.");
    let server = &report["checks"][4];
    assert_eq!(server["id"], "server.reachable");
    assert_eq!(server["status"], "skip");

    // A runtime that does not answer `info` fails on its own; the container
    // is not inspected.
    let assert = fixture
        .command()
        .current_dir(&project)
        .args(["doctor", "--json"])
        .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "info")
        .assert()
        .failure();
    let report: serde_json::Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(report["checks"][1]["id"], "runtime.daemon");
    assert_eq!(report["checks"][1]["status"], "fail");
    assert_eq!(report["checks"][2]["status"], "skip");
}

#[test]
fn a_cloud_database_is_reported_as_managed_without_any_request() {
    let fixture = CliFixture::new();
    let assert = fixture
        .command()
        .args(["doctor", "tenant:t1", "--json"])
        .assert()
        .success();
    let report: serde_json::Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(
        report["target"],
        serde_json::json!({"kind": "cloud", "name": "tenant:t1", "database": "tenant:t1"})
    );
    assert_eq!(report["checks"][1]["id"], "cloud.managed");
    assert_eq!(report["checks"][1]["status"], "skip");
}

#[test]
fn doctor_rejects_a_url_without_a_scheme() {
    let fixture = CliFixture::new();
    let assert = fixture
        .command()
        .args(["doctor", "--url", "10.0.1.5:8080"])
        .assert()
        .failure();
    assert!(stderr(&assert).contains("is not a server URL"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cli_version_check_reads_the_release_feed() {
    let server = server_reporting(serde_json::json!([])).await;
    Mock::given(method("GET"))
        .and(path("/__helix_test/github/releases/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": "v999.0.0",
            "name": "v999.0.0",
            "html_url": "https://example.com/release",
        })))
        .mount(&server)
        .await;
    let fixture = CliFixture::new().with_http_base(server.uri());
    let assert = fixture
        .command()
        .env_remove("HELIX_NO_UPDATE_CHECK")
        .env_remove("HELIX_DISABLE_UPDATE_CHECK")
        .args(["doctor", "--url", &server.uri(), "--json"])
        .assert()
        .success();
    let report: serde_json::Value = serde_json::from_str(&stdout(&assert)).unwrap();
    assert_eq!(report["checks"][0]["id"], "cli.version");
    assert_eq!(report["checks"][0]["status"], "warn");
    assert!(report["checks"][0]["summary"]
        .as_str()
        .unwrap()
        .ends_with("is behind v999.0.0"));
}
