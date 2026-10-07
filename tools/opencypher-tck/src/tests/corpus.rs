use super::*;
#[test]
fn pinned_corpus_is_complete_and_deterministic() {
    let corpus = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor")).unwrap();
    assert_eq!(corpus.len(), 3897);
    assert!(corpus
        .iter()
        .all(|s| s.steps.iter().any(|step| step.value == "executing query:")));
}
#[test]
fn pinned_named_fixtures_use_supported_storage_labels() {
    for name in ["binary-tree-1", "binary-tree-2"] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("vendor/opencypher/tck/graphs/{name}/{name}.cypher"));
        let source = std::fs::read_to_string(path).unwrap();
        for query in source.split(';').filter(|s| !s.trim().is_empty()) {
            helix_cypher::compile(query).unwrap();
        }
    }
}

#[test]
fn outlines_expand_both_backgrounds_docstrings_and_tables() {
    let text = r#"Feature: Expansion
  Background:
    Given an empty graph
  Rule: scoped
    Background:
      Given having executed:
        """
        CREATE (:N)
        """
    Scenario Outline: values
      When executing query:
        """
        RETURN <value> AS v
        """
      Then the result should be, in any order:
        | v |
        | <value> |
      Examples:
        | value |
        | 1 |
        | 2 |
"#;
    let scenarios = parse_feature("expanded.feature", text).unwrap();
    assert_eq!(scenarios.len(), 2);
    assert_eq!(scenarios[0].steps.len(), 4);
    assert!(scenarios[0].id.ends_with("example 0:0"));
    assert!(scenarios[1].steps[2]
        .docstring
        .as_ref()
        .unwrap()
        .contains("RETURN 2 AS v"));
    assert_eq!(
        scenarios[1].steps[3].table.as_ref().unwrap().rows[1][0],
        "2"
    );
    let mut ast = gherkin::Feature::parse(text, gherkin::GherkinEnv::default()).unwrap();
    let scenario = &mut ast.rules[0].scenarios[0];
    scenario.examples[0].table.as_mut().unwrap().rows[1].push("extra".into());
    assert!(expand("invalid", &[], scenario.clone(), &mut Vec::new()).is_err());
    scenario.examples[0].table.as_mut().unwrap().rows.clear();
    assert!(expand("invalid", &[], scenario.clone(), &mut Vec::new()).is_err());
    scenario.examples[0].table = None;
    assert!(expand("invalid", &[], scenario.clone(), &mut Vec::new()).is_err());
}

#[test]
fn corpus_rejects_changed_manifest_and_missing_resources() {
    let root = tempfile::tempdir().unwrap();
    let vendor = Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor");
    std::fs::write(root.path().join("manifest.json"), "{}").unwrap();
    assert!(load(root.path())
        .unwrap_err()
        .to_string()
        .contains("manifest checksum"));
    std::fs::copy(
        vendor.join("manifest.json"),
        root.path().join("manifest.json"),
    )
    .unwrap();
    std::fs::create_dir(root.path().join("opencypher")).unwrap();
    assert!(load(root.path())
        .unwrap_err()
        .to_string()
        .contains("inventory differs"));
    for source in files(&vendor.join("opencypher")).unwrap() {
        let target = root
            .path()
            .join("opencypher")
            .join(source.strip_prefix(vendor.join("opencypher")).unwrap());
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(source, target).unwrap();
    }
    std::fs::write(root.path().join("opencypher/LICENSE"), "modified").unwrap();
    assert!(load(root.path())
        .unwrap_err()
        .to_string()
        .contains("checksum mismatch"));
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("LICENSE", root.path().join("opencypher/extra-link")).unwrap();
        assert!(load(root.path())
            .unwrap_err()
            .to_string()
            .contains("symlinks"));
    }
}

#[test]
fn table_escape_adaptation_preserves_docstrings_and_unknown_escapes() {
    let input = "  | \\q | \\n | \\| | \\\\ |\n  \"\"\"\n  | \\q |\n  \"\"\"\n";
    let output = table_escapes(input);
    assert!(output.starts_with("  | \\\\q |"));
    assert!(output.contains("\n  | \\q |\n"));
}
