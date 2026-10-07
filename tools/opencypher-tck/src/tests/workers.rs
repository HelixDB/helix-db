use super::*;

#[tokio::test]
async fn invalid_configuration_and_missing_executables_never_produce_passes() {
    let missing = Path::new("/missing/tck-worker");
    for parallelism in [0, 33] {
        assert!(run(&[], parallelism, missing, Duration::from_secs(60))
            .await
            .is_err());
    }
    for timeout in [Duration::ZERO, Duration::from_secs(61)] {
        assert!(run(&[], 1, missing, timeout).await.is_err());
    }
    assert_eq!(
        run(&[], 32, missing, Duration::from_secs(60))
            .await
            .unwrap()
            .scenarios,
        0
    );
    let scenario = crate::test_support::scenario("missing-worker", "    Given an empty graph");
    let report = run(&[scenario], 1, missing, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(report.counts["harness_error"], 1);
}

#[cfg(unix)]
mod processes {
    use super::*;
    fn fixture(script: &str) -> tempfile::TempDir {
        let directory = tempfile::Builder::new()
            .prefix("tck worker ")
            .tempdir()
            .unwrap();
        std::fs::write(
            directory.path().join("worker"),
            format!("{script}\nexec cat >/dev/null\n"),
        )
        .unwrap();
        directory
    }

    async fn run(
        corpus: &[corpus::Scenario],
        parallelism: usize,
        script: &Path,
        timeout: Duration,
    ) -> corpus::Result<report::Report> {
        super::super::run_with(
            corpus,
            parallelism,
            || {
                // Interpret fixture data with the platform shell. Production
                // workers still launch their binary directly through run().
                let mut command = Command::new("/bin/sh");
                command.arg(script).arg("--worker");
                command
            },
            timeout,
        )
        .await
    }

    async fn output(replies: &[Reply]) -> String {
        let mut bytes = Vec::new();
        for reply in replies {
            frames::write(&mut bytes, reply).await.unwrap();
        }
        format!(
            "printf '{}'",
            bytes
                .iter()
                .map(|byte| format!("\\{byte:03o}"))
                .collect::<String>()
        )
    }

    #[tokio::test]
    async fn interpreted_fixtures_preserve_arguments_and_start_only_when_requested() {
        use std::os::unix::fs::PermissionsExt;
        let scenario =
            crate::test_support::scenario("fixture-contract", "    Given an empty graph");
        let completed = output(&[Reply::Finished {
            id: scenario.id.clone(),
            planned: false,
            result: Ok(()),
        }])
        .await;
        let directory = fixture(&format!("if [ \"$1\" != --worker ] || [ \"$#\" != 1 ]; then exit 9; fi\n: >\"$0.marker\"\n{completed}"));
        let script = directory.path().join("worker");
        assert_eq!(
            std::fs::metadata(&script).unwrap().permissions().mode() & 0o111,
            0
        );
        assert!(!script.with_extension("marker").exists());
        let report = run(&[scenario], 1, &script, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(report.counts.get("passed"), Some(&1));
        assert!(script.with_extension("marker").exists());
    }

    #[tokio::test]
    async fn progress_failure_categories_and_protocol_faults_are_independently_checked() {
        let scenario = crate::test_support::scenario("protocol", "    Given an empty graph");
        let id = scenario.id.clone();
        let good = output(&[
            Reply::Planned { id: id.clone() },
            Reply::Finished {
                id: id.clone(),
                planned: true,
                result: Ok(()),
            },
        ])
        .await;
        let directory = fixture(&good);
        let report = run(
            std::slice::from_ref(&scenario),
            1,
            &directory.path().join("worker"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(
            report.counts.get("passed"),
            Some(&1),
            "{}",
            serde_json::to_string(&report).unwrap()
        );
        assert!(report.outcomes[0].planned);

        let variants = [
            vec![Reply::Finished {
                id: "wrong".into(),
                planned: false,
                result: Ok(()),
            }],
            vec![
                Reply::Planned { id: id.clone() },
                Reply::Planned { id: id.clone() },
            ],
            vec![
                Reply::Planned { id: id.clone() },
                Reply::Finished {
                    id: id.clone(),
                    planned: false,
                    result: Ok(()),
                },
            ],
            vec![Reply::Finished {
                id: id.clone(),
                planned: false,
                result: Err(runner::Failure {
                    status: report::Status::Passed,
                    reason: "invalid".into(),
                }),
            }],
        ];
        for replies in variants {
            let directory = fixture(&output(&replies).await);
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(
                report.counts.get("harness_error"),
                Some(&1),
                "{}",
                serde_json::to_string(&report).unwrap()
            );
        }
        for status in [
            report::Status::Failed,
            report::Status::SetupBlocked,
            report::Status::Unsupported,
            report::Status::TimedOut,
            report::Status::HarnessError,
        ] {
            let expected = serde_json::to_value(&status)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            let directory = fixture(
                &output(&[Reply::Finished {
                    id: id.clone(),
                    planned: false,
                    result: Err(runner::Failure {
                        status,
                        reason: "visible".into(),
                    }),
                }])
                .await,
            );
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(
                report.counts.get(&expected),
                Some(&1),
                "{}",
                serde_json::to_string(&report).unwrap()
            );
            assert_eq!(report.outcomes[0].reason.as_deref(), Some("visible"));
        }
        for script in [
            "exit 0",
            "printf '\\000\\000\\000\\002{'",
            "printf '\\377\\377\\377\\377'",
        ] {
            let directory = fixture(script);
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(
                report.counts.get("harness_error"),
                Some(&1),
                "{}",
                serde_json::to_string(&report).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn deadlines_kill_reap_and_replace_workers_without_losing_confirmed_progress() {
        let scenarios = ["first", "second"]
            .map(|name| crate::test_support::scenario(name, "    Given an empty graph"));
        let progress = output(&[Reply::Planned {
            id: scenarios[0].id.clone(),
        }])
        .await;
        let completed = output(&[Reply::Finished {
            id: scenarios[1].id.clone(),
            planned: false,
            result: Ok(()),
        }])
        .await;
        let directory = fixture(&format!("if [ ! -f \"$0.marker\" ]; then\n: >\"$0.marker\"\n{progress}\nexec sleep 60\nfi\n{completed}"));
        let report = run(
            &scenarios,
            1,
            &directory.path().join("worker"),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(
            report.counts.get("timed_out"),
            Some(&1),
            "{}",
            serde_json::to_string(&report).unwrap()
        );
        assert_eq!(
            report.counts.get("passed"),
            Some(&1),
            "{}",
            serde_json::to_string(&report).unwrap()
        );
        assert!(report.outcomes[0].planned);
        assert_eq!(
            report.outcomes[0].reason.as_deref(),
            Some("ScenarioLifetimeTimeout")
        );
    }

    #[tokio::test]
    async fn failed_or_stuck_worker_shutdown_fails_the_gate() {
        let scenario = crate::test_support::scenario("shutdown", "    Given an empty graph");
        let completed = output(&[Reply::Finished {
            id: scenario.id.clone(),
            planned: false,
            result: Ok(()),
        }])
        .await;
        for suffix in ["read -r discard || :\nexit 9", "exec sleep 60"] {
            let directory = fixture(&format!("{completed}\n{suffix}"));
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(
                report.counts.get("harness_error"),
                Some(&1),
                "{}",
                serde_json::to_string(&report).unwrap()
            );
            assert!(report.outcomes[0]
                .reason
                .as_ref()
                .unwrap()
                .contains("WorkerShutdown"));
        }
    }
}
