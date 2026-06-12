mod support;

use assert_cmd::assert::Assert;
use std::fs;
use std::path::{Path, PathBuf};

use support::{CliFixture, free_port};

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
    fs::write(
        &write_request,
        r#"{
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
"#,
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "--file"])
        .arg(&write_request)
        .arg("--compact")
        .assert()
        .success();

    let read_request = project.join("examples/read-e2e-users.json");
    fs::write(
        &read_request,
        r#"{
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
"#,
    )
    .unwrap();

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
    assert!(read_output.contains("e2e_count"));
    assert!(read_output.contains('1'));

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
