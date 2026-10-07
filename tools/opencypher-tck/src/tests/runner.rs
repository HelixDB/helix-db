use super::*;

async fn execute(
    scenario: &Scenario,
    planned: &mut bool,
    timeout: Duration,
) -> Result<(), Failure> {
    let (progress, updates) = watch::channel(*planned);
    let result = execute_tracked(scenario, &progress, timeout).await;
    *planned = *updates.borrow();
    result
}

async fn run(corpus: &[Scenario], parallelism: usize) -> corpus::Result<crate::report::Report> {
    crate::workers::run(
        corpus,
        parallelism,
        Path::new("/missing/tck-worker"),
        Duration::from_secs(60),
    )
    .await
}

#[cfg(test)]
mod scenarios {
    use super::*;
    use crate::test_support;

    #[tokio::test]
    async fn assertion_failures_and_unsupported_setup_remain_distinct() {
        let cases = [
            ("When executing query:\n      \"\"\"\n      RETURN 1 AS x\n      \"\"\"\n    Then the result should be, in any order:\n      | x |\n      | 2 |", "failed"),
            ("When executing query:\n      \"\"\"\n      RETURN 1 AS x\n      \"\"\"\n    Then the result should be, in any order:\n      | y |\n      | 1 |", "failed"),
            ("When executing query:\n      \"\"\"\n      RETURN 1 AS x\n      \"\"\"\n    Then a SyntaxError should be raised at compile time: UndefinedVariable", "failed"),
            ("When executing query:\n      \"\"\"\n      RETURN missing\n      \"\"\"\n    Then a SyntaxError should be raised at runtime: UndefinedVariable", "failed"),
            ("When executing query:\n      \"\"\"\n      MERGE (n:N)\n      \"\"\"\n    Then a SyntaxError should be raised at compile time: UndefinedVariable", "unsupported"),
            ("Given having executed:\n      \"\"\"\n      CREATE ()\n      \"\"\"", "setup_blocked"),
            ("Given there exists a procedure foo()", "setup_blocked"),
            ("Given the missing graph", "harness_error"),
            ("When executing query:", "harness_error"),
            ("Then the result should be, in any order:\n      | x |\n      | 1 |", "harness_error"),
            ("Given an unknown fixture", "harness_error"),
            ("Given an empty graph", "harness_error"),
            ("Given parameters are:\n      | x | unknown_value |", "harness_error"),
            ("Given parameters are:\n      | x | 1 | extra |", "harness_error"),
            ("When executing query:\n      \"\"\"\n      CREATE (:N)\n      \"\"\"\n    Then the result should be empty\n    And no side effects", "failed"),
        ];
        for (body, status) in cases {
            let scenario = test_support::scenario("assertions", &format!("    {body}"));
            let mut planned = false;
            let error = execute(&scenario, &mut planned, Duration::from_secs(60))
                .await
                .unwrap_err();
            assert_eq!(
                serde_json::to_value(error.status).unwrap(),
                status,
                "{body}: {}",
                error.reason
            );
        }
    }

    #[tokio::test]
    async fn parameters_control_queries_and_observable_side_effects() {
        let scenario = test_support::scenario(
            "parameters",
            r#"    Given parameters are:
      | value | [1, 1, 2] |
    When executing query:
      """
      UNWIND $value AS x CREATE (:N {x:x}) RETURN x ORDER BY x
      """
    Then the result should be, in order:
      | x |
      | 1 |
      | 1 |
      | 2 |
    And the side effects should be:
      | +nodes | 3 |
      | +labels | 1 |
      | +properties | 3 |
    When executing control query:
      """
      MATCH (n:N) RETURN count(n) AS n
      """
    Then the result should be, in any order:
      | n |
      | 3 |
    And no side effects
"#,
        );
        let mut planned = false;
        execute(&scenario, &mut planned, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(planned);
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use crate::test_support;

    #[tokio::test]
    async fn malformed_assertions_never_become_passes() {
        let prefix = "    When executing query:\n      \"\"\"\n      RETURN 1 AS x\n      \"\"\"\n";
        let base = test_support::scenario(
            "protocol",
            &format!(
                "{prefix}    Then the result should be, in any order:\n      | x |\n      | 1 |"
            ),
        );
        let mut bad_width = base.clone();
        bad_width
            .steps
            .last_mut()
            .unwrap()
            .table
            .as_mut()
            .unwrap()
            .rows[1]
            .push("2".into());
        let mut no_columns = base.clone();
        no_columns
            .steps
            .last_mut()
            .unwrap()
            .table
            .as_mut()
            .unwrap()
            .rows
            .clear();
        let mut no_table = base.clone();
        no_table.steps.last_mut().unwrap().table = None;
        let mut bad_value = base.clone();
        bad_value
            .steps
            .last_mut()
            .unwrap()
            .table
            .as_mut()
            .unwrap()
            .rows[1][0] = "{".into();
        for scenario in [bad_width, no_columns, no_table, bad_value] {
            let error = execute(&scenario, &mut false, Duration::from_secs(60))
                .await
                .unwrap_err();
            assert!(matches!(error.status, Status::HarnessError), "{error:?}");
        }
        let scenario = test_support::scenario(
            "nonempty",
            &format!("{prefix}    Then the result should be empty"),
        );
        assert_eq!(
            execute(&scenario, &mut false, Duration::from_secs(60))
                .await
                .unwrap_err()
                .reason,
            "NonemptyResult"
        );
        let duplicate = test_support::scenario("unasserted",&format!("{prefix}{prefix}    Then the result should be, in any order:\n      | x |\n      | 1 |"));
        assert!(execute(&duplicate, &mut false, Duration::from_secs(60))
            .await
            .unwrap_err()
            .reason
            .contains("previous subject"));
        for assertion in [
            "a SyntaxError should be raised at phase",
            "a SyntaxError should be raised at unknown phase: UndefinedVariable",
        ] {
            let scenario = test_support::scenario("phase",&format!("    When executing query:\n      \"\"\"\n      RETURN missing\n      \"\"\"\n    Then {assertion}"));
            assert!(matches!(
                execute(&scenario, &mut false, Duration::from_secs(60))
                    .await
                    .unwrap_err()
                    .status,
                Status::HarnessError
            ));
        }
        for body in [
            "    Then no side effects",
            "    Given parameters are:",
            "    Given parameters are:\n      | x | (:N) |",
            "    When executing query:\n      \"\"\"\n      CREATE (:N)\n      \"\"\"\n    Then the result should be empty\n    And the side effects should be:\n      | bogus | 1 |",
            "    When executing query:\n      \"\"\"\n      CREATE (:N)\n      \"\"\"\n    Then the result should be empty\n    And the side effects should be:\n      | +nodes | x |",
        ] {
            let scenario = test_support::scenario("malformed",body);
            assert!(matches!(execute(&scenario,&mut false,Duration::from_secs(60)).await.unwrap_err().status,Status::HarnessError),"{body}");
        }
        assert!(run(&[], 0).await.is_err());
        assert!(run(&[], 33).await.is_err());
        assert!(run(
            &[test_support::scenario(
                "missing",
                "    When executing query:"
            )],
            1
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn timeout_and_storage_failures_stay_visible() {
        let scenario = test_support::scenario(
            "timeout",
            r#"    When executing query:
      """
      UNWIND range(1,100000) AS i CREATE (:N {i:i})
      """
    Then the result should be empty
"#,
        );
        let error = execute(&scenario, &mut false, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(matches!(error.status, Status::TimedOut), "{error:?}");
        let error = Failure::from(db::error::HelixDbError::TransactionConflict(
            "fixture storage failure".into(),
        ));
        assert!(matches!(error.status, Status::HarnessError));
        let subject = Some(Err(cypher::Error::Storage(
            db::error::HelixDbError::TransactionConflict("subject storage failure".into()),
        )));
        assert!(result(&subject)
            .unwrap_err()
            .reason
            .contains("subject storage failure"));
    }
}

#[tokio::test]
async fn missing_or_unsupported_named_fixture_and_storage_errors_cannot_pass() {
    let local = tempfile::tempdir().unwrap();
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "harness-io".into(),
    })
    .await
    .unwrap();
    let error = setup_graph(&db, "binary-tree-1", local.path())
        .await
        .unwrap_err();
    assert!(matches!(error.status, Status::HarnessError));
    std::fs::create_dir(local.path().join("binary-tree-1")).unwrap();
    std::fs::write(
        local.path().join("binary-tree-1/binary-tree-1.cypher"),
        "CREATE ()",
    )
    .unwrap();
    let error = setup_graph(&db, "binary-tree-1", local.path())
        .await
        .unwrap_err();
    assert!(matches!(error.status, Status::SetupBlocked));
    let subject = Some(Err(cypher::Error::Storage(
        db::error::HelixDbError::TransactionConflict("test storage conflict".into()),
    )));
    let error = assert_error(&subject, "SyntaxError", "compile time", "*").unwrap_err();
    assert!(matches!(error.status, Status::Failed));
    assert!(error.reason.contains("ExpectedCypherError"));
    db.close().await.unwrap();
    let scenario = crate::test_support::scenario("closed","    When executing query:\n      \"\"\"\n      RETURN 1\n      \"\"\"\n    Then the result should be empty");
    assert!(matches!(
        steps(&db, &scenario, &watch::channel(false).0)
            .await
            .unwrap_err()
            .status,
        Status::HarnessError
    ));
}

#[test]
fn production_deadlines_are_timeouts_in_setup_success_and_error_assertions() {
    let deadline = || cypher::Error::Storage(db::error::HelixDbError::QueryDeadlineExceeded);
    assert!(matches!(blocked(deadline()).status, Status::TimedOut));
    let subject = Some(Err(deadline()));
    assert!(matches!(
        result(&subject).unwrap_err().status,
        Status::TimedOut
    ));
    assert!(matches!(
        assert_error(&subject, "SyntaxError", "any time", "*")
            .unwrap_err()
            .status,
        Status::TimedOut
    ));
}
