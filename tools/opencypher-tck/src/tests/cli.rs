use super::*;

async fn run(arguments: Vec<String>) -> corpus::Result<()> {
    super::run(arguments, std::path::Path::new("/missing/tck-worker")).await
}
#[tokio::test]
async fn cli_validates_options_and_keeps_drafts_and_reports_local() {
    for args in [
        vec!["--unknown"],
        vec!["--output"],
        vec!["--parallelism"],
        vec!["--parallelism", "0"],
        vec!["--parallelism", "33"],
        vec!["--parallelism", "bad"],
        vec!["--scenario-timeout-ms"],
        vec!["--scenario-timeout-ms", "0"],
        vec!["--scenario-timeout-ms", "60001"],
        vec!["--scenario-timeout-ms", "bad"],
        vec!["--filter"],
        vec!["--profile-draft"],
        vec!["--profile-draft", "unused", "--gate"],
    ] {
        assert!(
            run(args.iter().map(|s| s.to_string()).collect())
                .await
                .is_err(),
            "{args:?}"
        );
    }
    let local = tempfile::tempdir().unwrap();
    let draft = local
        .path()
        .join("review.json")
        .to_string_lossy()
        .to_string();
    run(["--profile-draft".into(), draft.clone()]
        .into_iter()
        .collect())
    .await
    .unwrap();
    assert!(run(["--profile-draft".into(), draft].into_iter().collect())
        .await
        .is_err());
    let output = local
        .path()
        .join("inventory.json")
        .to_string_lossy()
        .to_string();
    run([
        "--inventory".into(),
        "--filter".into(),
        "no-such-scenario".into(),
        "--output".into(),
        output.clone(),
    ]
    .into_iter()
    .collect())
    .await
    .unwrap();
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    assert_eq!(report["scenarios"], 0);
    assert!(run(["--filter", "Literals1.feature", "--parallelism", "2"]
        .into_iter()
        .map(str::to_owned)
        .collect())
    .await
    .is_err());
    assert!(run(["--inventory", "--gate"]
        .into_iter()
        .map(str::to_owned)
        .collect())
    .await
    .is_err());
}
