mod support;

use assert_cmd::assert::Assert;
use std::fs;
use std::path::{Path, PathBuf};

use support::{free_port, CliFixture};

const WRITE_E2E_USER: &str = r#"{
  "request_type": "write",
  "query_name": null,
  "query": {
    "queries": [{
      "Query": {
        "name": "created",
        "steps": [{
          "AddN": {
            "label": "E2EUser",
            "properties": [
              ["externalId", {"Value": {"String": "cli-e2e"}}],
              ["name", {"Value": {"String": "CI User"}}]
            ]
          }
        }],
        "condition": null
      }
    }],
    "returns": ["created"]
  },
  "parameters": {}
}
"#;

const READ_E2E_USERS: &str = r#"{
  "request_type": "read",
  "query_name": null,
  "query": {
    "queries": [{
      "Query": {
        "name": "e2e_count",
        "steps": [
          {"NWhere": {"Eq": ["$label", {"String": "E2EUser"}]}},
          "Count"
        ],
        "condition": null
      }
    }],
    "returns": ["e2e_count"]
  },
  "parameters": {}
}
"#;

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

struct RuntimeCleanup<'a> {
    fixture: &'a CliFixture,
    project: PathBuf,
}

impl Drop for RuntimeCleanup<'_> {
    fn drop(&mut self) {
        cleanup_runtime(self.fixture, &self.project);
    }
}

fn cleanup_runtime(fixture: &CliFixture, project: &Path) {
    let _ = fixture
        .command()
        .current_dir(project)
        .args(["stop", "dev"])
        .output();
    let _ = fixture
        .command()
        .current_dir(project)
        .args(["prune", "dev", "--yes"])
        .output();
}

fn assert_e2e_count_is_one(output: &str) {
    let count_idx = output
        .find("e2e_count")
        .unwrap_or_else(|| panic!("expected e2e_count in output: {output}"));
    let count_region = &output[count_idx..];
    assert!(
        count_region.contains('1'),
        "expected a count of 1 for e2e_count in output: {output}"
    );
}

#[test]
#[ignore = "requires Docker and pulls ghcr.io/helixdb/enterprise-dev"]
fn local_runtime_lifecycle_and_query_smoke() {
    let fixture = CliFixture::new();
    let port = free_port();
    let project = fixture
        .root()
        .join(format!("runtime-project-{}-{port}", std::process::id()));

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--name", "dev", "--port"])
        .arg(port.to_string())
        .arg("--no-skills")
        .assert()
        .success();

    cleanup_runtime(&fixture, &project);
    let _cleanup = RuntimeCleanup {
        fixture: &fixture,
        project: project.clone(),
    };

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();

    let status = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["status", "dev"])
            .assert()
            .success(),
    );
    assert!(status.contains("dev (local)"));
    assert!(status.contains(&format!("localhost:{port}")));

    let initial_query = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args([
                "query",
                "dev",
                "--file",
                "examples/request.json",
                "--compact",
            ])
            .assert()
            .success(),
    );
    assert!(initial_query.contains("node_count"));

    let write_request = project.join("examples/write-e2e-user.json");
    fs::write(&write_request, WRITE_E2E_USER).unwrap();

    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "--file"])
        .arg(&write_request)
        .arg("--compact")
        .assert()
        .success();

    let read_request = project.join("examples/read-e2e-users.json");
    fs::write(&read_request, READ_E2E_USERS).unwrap();

    let read_output = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--file"])
            .arg(&read_request)
            .arg("--compact")
            .assert()
            .success(),
    );
    assert_e2e_count_is_one(&read_output);

    fixture
        .command()
        .current_dir(&project)
        .args(["logs", "dev"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["restart", "dev"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["status", "dev"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "dev", "--yes"])
        .assert()
        .success();
}

#[test]
#[ignore = "requires Docker and pulls ghcr.io/helixdb/enterprise-dev plus MinIO"]
fn disk_runtime_persists_data_across_stop_and_start() {
    let fixture = CliFixture::new();
    let port = free_port();
    let project = fixture.root().join(format!(
        "disk-runtime-project-{}-{port}",
        std::process::id()
    ));

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--name", "dev", "--port"])
        .arg(port.to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    cleanup_runtime(&fixture, &project);
    let _cleanup = RuntimeCleanup {
        fixture: &fixture,
        project: project.clone(),
    };

    let write_request = project.join("examples/write-e2e-user.json");
    let read_request = project.join("examples/read-e2e-users.json");
    fs::write(&write_request, WRITE_E2E_USER).unwrap();
    fs::write(&read_request, READ_E2E_USERS).unwrap();

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "--file"])
        .arg(&write_request)
        .arg("--compact")
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();
    let read_output = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--file"])
            .arg(&read_request)
            .arg("--compact")
            .assert()
            .success(),
    );
    assert_e2e_count_is_one(&read_output);

    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "dev", "--yes"])
        .assert()
        .success();
}
