//! Exercise the production executable, including its real process entry point.

use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_helix-opencypher-tck"));
    command
        .env("HELIX_TELEMETRY_LEVEL", "off")
        .env("HELIX_NO_UPDATE_CHECK", "1")
        .kill_on_drop(true);
    command
}

#[tokio::test]
async fn one_worker_uses_fresh_databases_and_reports_production_progress_and_errors() {
    let mut child = command()
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        for (id, setup, query, assertion, planned, passes) in [
            ("created", "Given having executed:\n      \"\"\"\n      CREATE (:N)\n      \"\"\"\n    ", "MATCH (n) RETURN count(n) AS count", "the result should be, in any order:\n      | count |\n      | 1 |", true, true),
            ("fresh", "", "MATCH (n) RETURN count(n) AS count", "the result should be, in any order:\n      | count |\n      | 0 |", true, true),
            ("compile", "", "RETURN missing", "a SyntaxError should be raised at compile time: UndefinedVariable", false, true),
            ("failure", "", "RETURN 1 AS count", "the result should be, in any order:\n      | count |\n      | 2 |", true, false),
        ] {
            let feature = gherkin::Feature::parse(format!("Feature: Worker\n  Scenario: {id}\n    {setup}When executing query:\n      \"\"\"\n      {query}\n      \"\"\"\n    Then {assertion}\n"), gherkin::GherkinEnv::default()).unwrap();
            let request = serde_json::json!({"id":id,"steps":feature.scenarios[0].steps});
            let bytes = serde_json::to_vec(&request).unwrap();
            input.write_u32(bytes.len().try_into().unwrap()).await.unwrap();
            input.write_all(&bytes).await.unwrap();
            input.flush().await.unwrap();
            let mut observed_progress = false;
            loop {
                let length = output.read_u32().await.unwrap() as usize;
                assert!(length <= 8 * 1024 * 1024);
                let mut bytes = vec![0; length];
                output.read_exact(&mut bytes).await.unwrap();
                let reply: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                match reply.get("Planned") {
                    Some(progress) => {
                        assert_eq!(progress["id"],id);
                        assert!(!observed_progress);
                        observed_progress = true;
                        continue;
                    },
                    None => {},
                }
                let finished = &reply["Finished"];
                assert_eq!(finished["id"],id);
                assert_eq!(finished["planned"],planned);
                assert!(!observed_progress || planned);
                assert_eq!(finished["result"].get("Ok").is_some(),passes,"{reply}");
                if !passes {
                    assert_eq!(finished["result"]["Err"]["status"],"failed");
                }
                break;
            }
        }
        drop(input);
        assert!(child.wait().await.unwrap().success());
    }).await.expect("production worker must complete and close its databases");
}

#[tokio::test]
async fn cli_runs_bounded_workers_and_short_lifetime_limits_stay_visible() {
    let directory = tempfile::tempdir().unwrap();
    for timeout in [60_000, 1] {
        let path = directory.path().join(format!("report-{timeout}.json"));
        let output = tokio::time::timeout(
            Duration::from_secs(45),
            command()
                .args([
                    "--filter",
                    "Literals1.feature",
                    "--parallelism",
                    "2",
                    "--scenario-timeout-ms",
                    &timeout.to_string(),
                    "--output",
                ])
                .arg(&path)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(report["scenarios"].as_u64().unwrap() > 0);
        assert!(report["counts"].get("harness_error").is_none(), "{report}");
        let category = if timeout == 1 { "timed_out" } else { "passed" };
        assert!(report["counts"][category].as_u64().unwrap() > 0, "{report}");
    }
}

#[tokio::test]
async fn worker_rejects_oversized_protocol_input_before_a_database_opens() {
    let mut child = command()
        .arg("--worker")
        .stdin(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&u32::MAX.to_be_bytes())
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
}
