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
        &["explorer", "--help"],
        &["auth", "login", "--help"],
        &["auth", "login", "github", "--help"],
        &["auth", "login", "google", "--help"],
        &["auth", "login", "microsoft", "--help"],
        &["auth", "login", "sso", "--help"],
        &["auth", "login", "password", "--help"],
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
fn login_flags_only_exist_where_they_apply() {
    let fixture = CliFixture::new();
    for args in [
        &["auth", "login", "--email", "person@test.dev"][..],
        &["auth", "login", "github", "--email", "person@test.dev"][..],
        &["auth", "login", "google", "--email", "person@test.dev"][..],
        &["auth", "login", "microsoft", "--email", "person@test.dev"][..],
        // Passwords only come from the hidden prompt, never from argv.
        &["auth", "login", "password", "--password", "secret"][..],
        &["auth", "login", "--password", "secret"][..],
    ] {
        assert!(
            stderr(fixture.command().args(args).assert().failure()).contains("unexpected argument"),
            "{args:?}"
        );
    }
    // A typo or an unset shell variable must not get as far as a prompt or a
    // request.
    for method in ["sso", "password"] {
        for email in ["", " ", "person@", "not-an-email"] {
            assert!(
                stderr(
                    fixture
                        .command()
                        .args(["auth", "login", method, "--email", email])
                        .assert()
                        .failure()
                )
                .contains("valid email address"),
                "{method} {email:?}"
            );
        }
    }
    assert!(stderr(
        fixture
            .command()
            .args(["auth", "login", "--provider", "carrier-pigeon"])
            .assert()
            .failure()
    )
    .contains("invalid value"));
    assert!(stderr(
        fixture
            .command()
            .args(["auth", "login", "--provider", "google", "github"])
            .assert()
            .failure()
    )
    .contains("cannot use --provider"));
    assert!(stderr(
        fixture
            .command()
            .args(["auth", "login", "carrier-pigeon"])
            .assert()
            .failure()
    )
    .contains("unrecognized subcommand"));
}

#[test]
fn login_needs_a_terminal_for_every_method() {
    let fixture = CliFixture::new();
    for args in [
        &["auth", "login", "--json"][..],
        &["auth", "login", "--provider", "google", "--json"][..],
        &["auth", "login", "--provider", "github", "--json"][..],
        &["auth", "login", "--provider", "microsoft", "--json"][..],
        &["auth", "login", "--provider", "sso", "--json"][..],
        &["auth", "login", "--provider", "Google", "--json"][..],
        &["auth", "login", "github", "--json"][..],
        &["auth", "login", "google", "--json"][..],
        &["auth", "login", "microsoft", "--json"][..],
        &[
            "auth",
            "login",
            "sso",
            "--email",
            "person@test.dev",
            "--json",
        ][..],
        &[
            "auth",
            "login",
            "password",
            "--email",
            "person@test.dev",
            "--json",
        ][..],
    ] {
        let error: serde_json::Value =
            serde_json::from_str(stderr(fixture.command().args(args).assert().failure()).trim())
                .unwrap();
        assert_eq!(
            error["error"]["message"], "logging in needs an interactive terminal",
            "{args:?}"
        );
    }
    assert!(!fixture.helix_home().join("credentials").exists());
}
