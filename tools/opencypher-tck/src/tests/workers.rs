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
    use std::os::unix::fs::PermissionsExt;

    fn executable(script: &str) -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("worker");
        std::fs::write(&path, format!("#!/bin/sh\n{script}\nexec cat >/dev/null\n")).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
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
        let directory = executable(&good);
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
            let directory = executable(&output(&replies).await);
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(report.counts["harness_error"], 1);
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
            let directory = executable(
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
            assert_eq!(report.counts[&expected], 1);
            assert_eq!(report.outcomes[0].reason.as_deref(), Some("visible"));
        }
        for script in [
            "exit 0",
            "printf '\\000\\000\\000\\002{'",
            "printf '\\377\\377\\377\\377'",
        ] {
            let directory = executable(script);
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(report.counts["harness_error"], 1);
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
        let directory = executable(&format!("if [ ! -f \"$0.marker\" ]; then\n: >\"$0.marker\"\n{progress}\nexec sleep 60\nfi\n{completed}"));
        let report = run(
            &scenarios,
            1,
            &directory.path().join("worker"),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(report.counts["timed_out"], 1);
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
            let directory = executable(&format!("{completed}\n{suffix}"));
            let report = run(
                std::slice::from_ref(&scenario),
                1,
                &directory.path().join("worker"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(report.counts["harness_error"], 1);
            assert!(report.outcomes[0]
                .reason
                .as_ref()
                .unwrap()
                .contains("WorkerShutdown"));
        }
    }
}
