mod support;

use assert_cmd::assert::Assert;
use std::fs;
use std::path::{Path, PathBuf};

use support::{free_port, CliFixture};

/// Mirrors docs/database/helix-db/start-here/quickstart.mdx's write-users example
/// exactly (`examples/write-users.json` step). If the documented request shape or
/// its expected response changes, update both the doc and this constant together.
const WRITE_USERS_REQUEST: &str = r#"{
  "request_type": "write",
  "query_name": "write_users",
  "query": {
    "write": {
      "entries": [
        {
          "query": {
            "name": "alice",
            "root": { "add_n": { "label": "User", "properties": [["name", { "value": { "string": "Alice" } }]] } }
          }
        },
        {
          "query": {
            "name": "bob",
            "root": { "add_n": { "label": "User", "properties": [["name", { "value": { "string": "Bob" } }]] } }
          }
        },
        {
          "query": {
            "name": "follow",
            "root": {
              "add_e": {
                "input": { "nodes": { "reference": { "var": "alice" } } },
                "label": "FOLLOWS",
                "to": { "var": "bob" },
                "properties": [["since", { "value": { "string": "2026-07-24" } }]]
              }
            }
          }
        },
        {
          "query": {
            "name": "friends",
            "root": {
              "value_map": {
                "input": { "out": { "input": { "nodes": { "reference": { "var": "alice" } } }, "label": "FOLLOWS" } },
                "properties": ["$id", "name"]
              }
            }
          }
        }
      ],
      "returns": ["alice", "bob", "friends"]
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

/// Runs the exact `helix init` / `helix start` / `helix query` / `helix stop`
/// journey documented in the database quickstart
/// (docs/database/helix-db/start-here/quickstart.mdx), against an isolated
/// fixture project, so a docs edit that drifts from real CLI behavior fails CI
/// instead of silently shipping.
#[test]
#[ignore = "requires Docker and pulls ghcr.io/helixdb/helixdb:v0.0.4"]
fn quickstart_documented_cli_journey_works() {
    let fixture = CliFixture::new();
    let port = free_port();
    let project = fixture
        .root()
        .join(format!("quickstart-smoke-{}-{port}", std::process::id()));

    // Step: "Create a project" — `helix init local` (docs use the bare form; the
    // fixture pins name/port so the test can run concurrently with other suites).
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--name", "dev", "--port"])
        .arg(port.to_string())
        .arg("--no-skills")
        .assert()
        .success();

    assert!(
        project.join("helix.toml").exists(),
        "helix init local must create helix.toml as documented"
    );
    assert!(
        project.join(".helix").is_dir(),
        "helix init local must create the .helix/ workspace directory as documented"
    );
    assert!(
        project.join("examples/request.json").exists(),
        "helix init local must create examples/request.json as documented"
    );

    cleanup_runtime(&fixture, &project);
    let _cleanup = RuntimeCleanup {
        fixture: &fixture,
        project: project.clone(),
    };

    // Step: "Start the local database" — `helix start dev`.
    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();

    // Step: "Write the graph" — save examples/write-users.json, then
    // `helix query dev --file examples/write-users.json --compact`.
    let write_request = project.join("examples/write-users.json");
    fs::write(&write_request, WRITE_USERS_REQUEST).unwrap();

    let query_output = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--file"])
            .arg(&write_request)
            .arg("--compact")
            .assert()
            .success(),
    );

    // Step: "Read the result" — the response documented in the quickstart.
    assert!(
        query_output.contains("\"alice\""),
        "expected alice in response: {query_output}"
    );
    assert!(
        query_output.contains("\"bob\""),
        "expected bob in response: {query_output}"
    );
    assert!(
        query_output.contains("\"friends\""),
        "expected friends in response: {query_output}"
    );
    assert!(
        query_output.contains("\"name\":\"Bob\"") || query_output.contains("\"name\": \"Bob\""),
        "expected friends to resolve Bob's name across the FOLLOWS edge: {query_output}"
    );

    // Step: "Clean up" — `helix stop dev`.
    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .assert()
        .success();

    // `helix stop` is documented as idempotent.
    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .assert()
        .success();
}
