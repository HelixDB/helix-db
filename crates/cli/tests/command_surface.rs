mod support;

use support::CliFixture;

fn stdout(assert: assert_cmd::assert::Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).unwrap()
}

fn stderr(assert: assert_cmd::assert::Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).unwrap()
}

#[test]
fn every_retained_command_renders_help() {
    const CASES: &[&[&str]] = &[
        &["init", "local", "--help"],
        &["init", "cloud", "--help"],
        &["chef", "--help"],
        &["add", "local", "--help"],
        &["add", "cloud", "--help"],
        &["start", "--help"],
        &["stop", "--help"],
        &["restart", "--help"],
        &["status", "--help"],
        &["doctor", "--help"],
        &["logs", "--help"],
        &["query", "--help"],
        &["shell", "--help"],
        &["auth", "login", "--help"],
        &["auth", "status", "--help"],
        &["auth", "logout", "--help"],
        &["workspace", "list", "--help"],
        &["workspace", "get", "--help"],
        &["project", "list", "--help"],
        &["project", "get", "--help"],
        &["project", "create", "--help"],
        &["project", "delete", "--help"],
        &["project", "link", "--help"],
        &["cluster", "list", "--help"],
        &["cluster", "get", "--help"],
        &["cluster", "indexes", "--help"],
        &["database", "list", "--help"],
        &["database", "get", "--help"],
        &["database", "create", "--help"],
        &["database", "delete", "--help"],
        &["database", "indexes", "--help"],
        &["database", "key", "create", "--help"],
        &["database", "key", "list", "--help"],
        &["database", "key", "revoke", "--help"],
        &["service-credential", "create", "--help"],
        &["service-credential", "list", "--help"],
        &["service-credential", "get", "--help"],
        &["service-credential", "update", "--help"],
        &["service-credential", "revoke", "--help"],
        &["api", "get", "--help"],
        &["api", "post", "--help"],
        &["api", "patch", "--help"],
        &["api", "delete", "--help"],
        &["prune", "--help"],
        &["delete", "--help"],
        &["skills", "install", "--help"],
        &["skills", "update", "--help"],
        &["skills", "list", "--help"],
        &["metrics", "status", "--help"],
        &["update", "--help"],
        &["feedback", "--help"],
    ];
    let fixture = CliFixture::new();
    for args in CASES {
        let output = stdout(
            fixture
                .command()
                .args(args.iter().copied())
                .assert()
                .success(),
        );
        assert!(output.contains("Usage:"), "missing usage for {args:?}");
    }
}

#[test]
fn obsolete_cloud_commands_and_auth_paths_are_absent() {
    let fixture = CliFixture::new();
    for args in [
        &["push"][..],
        &["sync"][..],
        &["auth", "create-key"][..],
        &["workspace", "switch"][..],
        &["project", "switch"][..],
        &["project", "update"][..],
    ] {
        assert!(stderr(fixture.command().args(args).assert().failure())
            .contains("unrecognized subcommand"));
    }
}

#[test]
fn local_commands_do_not_require_cloud_credentials() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("local-only");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["status", "dev"])
        .assert()
        .success();
    assert!(!fixture.helix_home().join("credentials").exists());
}
