use super::*;
use crate::{report, test_support};

#[test]
fn ratchet_rejects_missing_duplicate_unclassified_and_regressed_results() {
    let root = tempfile::tempdir().unwrap();
    let corpus = vec![
        test_support::scenario("required", "    Given an empty graph"),
        test_support::scenario("excluded", "    Given an empty graph"),
    ];
    let revision = "007895aff5f33097d67b2e48a0a2babd6bd18590";
    let mut required = Required {
        revision: revision.into(),
        required: vec!["required".into()],
        exclusions: BTreeMap::from([("excluded".into(), "Merge".into())]),
        unclassified: vec![],
    };
    let path = root.path().join("required-mvp.json");
    let baseline = root.path().join("previously-passing.json");
    std::fs::write(&path, serde_json::to_vec(&required).unwrap()).unwrap();
    std::fs::write(
        &baseline,
        serde_json::to_vec(&serde_json::json!({"revision":revision,"scenarios":["required"]}))
            .unwrap(),
    )
    .unwrap();
    let mut report = report::summarize(vec![
        report::Outcome {
            id: "required".into(),
            status: Status::Passed,
            parsed: true,
            resolved: true,
            planned: true,
            reason: None,
        },
        report::Outcome {
            id: "excluded".into(),
            status: Status::Unsupported,
            parsed: true,
            resolved: false,
            planned: false,
            reason: Some("Merge".into()),
        },
    ]);
    verify(root.path(), &corpus, &report).unwrap();
    for status in [
        Status::Failed,
        Status::Unsupported,
        Status::SetupBlocked,
        Status::TimedOut,
        Status::HarnessError,
        Status::NotExecuted,
    ] {
        report.outcomes[0].status = status;
        assert!(verify(root.path(), &corpus, &report).is_err());
    }
    report.outcomes[0].status = Status::Passed;
    report.outcomes[1].id = "required".into();
    assert!(verify(root.path(), &corpus, &report).is_err());
    report.outcomes[1].id = "excluded".into();
    report.scenarios = 1;
    assert!(verify(root.path(), &corpus, &report).is_err());
    report.scenarios = 2;
    required.exclusions.insert("excluded".into(), String::new());
    std::fs::write(&path, serde_json::to_vec(&required).unwrap()).unwrap();
    assert!(verify(root.path(), &corpus, &report).is_err());
    required.exclusions.clear();
    required.unclassified.push("excluded".into());
    std::fs::write(&path, serde_json::to_vec(&required).unwrap()).unwrap();
    assert!(verify(root.path(), &corpus, &report).is_err());
    required.required.push("required".into());
    std::fs::write(&path, serde_json::to_vec(&required).unwrap()).unwrap();
    assert!(verify(root.path(), &corpus, &report).is_err());
    required.required.pop();
    required.unclassified.clear();
    required
        .exclusions
        .insert("excluded".into(), "Merge".into());
    std::fs::write(&path, serde_json::to_vec(&required).unwrap()).unwrap();
    for (revision, scenarios) in [
        ("wrong", vec!["required"]),
        (revision, vec!["required", "required"]),
        (revision, vec!["absent"]),
    ] {
        std::fs::write(
            &baseline,
            serde_json::to_vec(&serde_json::json!({"revision":revision,"scenarios":scenarios}))
                .unwrap(),
        )
        .unwrap();
        assert!(verify(root.path(), &corpus, &report).is_err());
    }
}

#[test]
fn capability_draft_uses_syntax_and_setup_never_pass_counts() {
    let corpus = vec![
            test_support::scenario("read","    When executing query:\n      \"\"\"\n      RETURN 1\n      \"\"\""),
            test_support::scenario("deferred","    When executing query:\n      \"\"\"\n      MERGE (n:N)\n      \"\"\""),
            test_support::scenario("storage","    When executing query:\n      \"\"\"\n      CREATE ()\n      \"\"\""),
            test_support::scenario("unknown","    When executing query:\n      \"\"\"\n      RETURN @\n      \"\"\""),
            test_support::scenario("negative","    When executing query:\n      \"\"\"\n      RETURN @\n      \"\"\"\n    Then a SyntaxError should be raised at compile time: UnexpectedSyntax"),
            test_support::scenario("missing","    When executing query:"),
            test_support::scenario("procedure","    Given there exists a procedure test()"),
            test_support::scenario("fixture","    Given the binary-tree-1 graph\n    When executing query:\n      \"\"\"\n      RETURN 1\n      \"\"\""),
        ];
    let draft = draft(&corpus);
    assert_eq!(draft.required, vec!["read", "negative", "fixture"]);
    assert_eq!(draft.unclassified, vec!["unknown", "missing"]);
    assert_eq!(draft.exclusions.len(), 3);
}
