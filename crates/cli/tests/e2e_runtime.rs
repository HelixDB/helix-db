mod support;

use assert_cmd::assert::Assert;
use std::fs;
use std::path::{Path, PathBuf};

use support::{free_port, CliFixture};

const WRITE_E2E_USER: &str = r#"{
  "request_type": "write",
  "query": {
    "write": {
      "entries": [{
        "query": {
          "name": "created",
          "root": {
            "add_n": {
              "label": "E2EUser",
              "properties": [
                ["externalId", {"value": {"string": "cli-e2e"}}],
                ["name", {"value": {"string": "CI User"}}]
              ]
            }
          }
        }
      }],
      "returns": ["created"]
    }
  }
}
"#;

const READ_E2E_USERS: &str = r#"{
  "request_type": "read",
  "query": {
    "read": {
      "entries": [{
        "query": {
          "name": "e2e_count",
          "root": {
            "count": {
              "input": {
                "nodes_where": {
                  "predicate": {
                    "eq": {
                      "left": {"property": "$label"},
                      "right": {"constant": {"string": "E2EUser"}}
                    }
                  }
                }
              }
            }
          }
        }
      }],
      "returns": ["e2e_count"]
    }
  }
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

// Allows the same lifecycle suite to validate a freshly built, unpublished image.
fn select_test_image(project: &Path) {
    let Ok(reference) = std::env::var("HELIX_E2E_IMAGE") else {
        return;
    };
    let (repository, version) = reference
        .split_once('@')
        .or_else(|| reference.rsplit_once(':'))
        .expect("HELIX_E2E_IMAGE must include an explicit tag or digest");
    let path = project.join("helix.toml");
    let mut config: helix_cli::config::HelixConfig =
        toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let instance = config.local.get_mut("dev").unwrap();
    instance.image = repository.to_owned();
    instance.tag = version.parse().unwrap();
    instance.pull = Some(helix_cli::image::PullPolicy::Never);
    config.save_to_file(&path).unwrap();
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
#[ignore = "requires Docker and pulls ghcr.io/helixdb/helixdb:v0.0.6"]
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

    select_test_image(&project);
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
#[ignore = "requires Docker and pulls ghcr.io/helixdb/helixdb:v0.0.6 plus MinIO"]
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

    select_test_image(&project);
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
    // A restart must retain the image even when project settings now name an
    // unavailable version. It must also retain persistent data.
    let config_path = project.join("helix.toml");
    let original = fs::read_to_string(&config_path).unwrap();
    let mut changed: helix_cli::config::HelixConfig = toml::from_str(&original).unwrap();
    changed.local.get_mut("dev").unwrap().tag = "unpublished-restart-test".parse().unwrap();
    changed.save_to_file(&config_path).unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["restart", "dev"])
        .assert()
        .success();
    let after_restart = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--file"])
            .arg(&read_request)
            .arg("--compact")
            .assert()
            .success(),
    );
    assert_e2e_count_is_one(&after_restart);
    fs::write(&config_path, original).unwrap();

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

#[test]
#[ignore = "requires Docker and a Helix server image"]
fn restart_keeps_non_persisted_port_override() {
    let fixture = CliFixture::new();
    let port = free_port();
    let project = fixture
        .root()
        .join(format!("restart-port-{}-{port}", std::process::id()));
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();
    select_test_image(&project);
    let _cleanup = RuntimeCleanup {
        fixture: &fixture,
        project: project.clone(),
    };
    let original = fs::read_to_string(project.join("helix.toml")).unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev", "--port"])
        .arg(port.to_string())
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["restart", "dev"])
        .assert()
        .success();
    assert_eq!(
        fs::read_to_string(project.join("helix.toml")).unwrap(),
        original
    );
}
