//! Strict, review-required plan regressions across both query frontends.
mod support;

#[test]
fn complete_query_plans_match_the_reviewed_manifest() {
    let capture = support::Capture::plan(support::inputs());
    let actual = capture.manifest();
    let expected: serde_json::Value = serde_json::from_str(include_str!("reviewed.json")).unwrap();
    if actual != expected {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/planner-regression");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("candidate.json"),
            serde_json::to_vec_pretty(&capture).unwrap(),
        )
        .unwrap();
        std::fs::write(
            directory.join("candidate-manifest.json"),
            serde_json::to_vec_pretty(&actual).unwrap(),
        )
        .unwrap();
        let old = expected["cases"]
            .as_object()
            .expect("reviewed case manifest");
        let new = actual["cases"].as_object().unwrap();
        let changes: Vec<_> = old
            .keys()
            .chain(new.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|name| old.get(*name) != new.get(*name))
            .collect();
        panic!("{} plan cases changed: {changes:?}. Full candidate at {}. Replay the previous capture and inspect complete plan diffs and runtime reads before manually reviewing any baseline change.", changes.len(), directory.display());
    }
}
