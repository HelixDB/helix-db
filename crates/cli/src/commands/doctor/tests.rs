use super::*;
use crate::local_runtime::LocalStatus;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn check(id: &str, category: &str, status: Status) -> Check {
    Check {
        id: id.to_owned(),
        category: category.to_owned(),
        status,
        summary: format!("{id} summary"),
        detail: Some(format!("{id} detail")),
        fix: matches!(status, Status::Warn | Status::Fail).then(|| format!("{id} fix")),
    }
}

fn local_config(image: &str, tag: &str) -> LocalInstanceConfig {
    toml::from_str(&format!("image = \"{image}\"\ntag = \"{tag}\"\n")).unwrap()
}

fn plain(rendered: &str) -> String {
    console::strip_ansi_codes(rendered).to_string()
}

#[test]
fn statuses_round_trip_and_unknown_ones_read_as_skip() {
    for (json, status) in [
        ("\"pass\"", Status::Pass),
        ("\"warn\"", Status::Warn),
        ("\"fail\"", Status::Fail),
        ("\"skip\"", Status::Skip),
        ("\"degraded\"", Status::Skip),
    ] {
        assert_eq!(
            serde_json::from_str::<Status>(json).unwrap(),
            status,
            "{json}"
        );
    }
    assert_eq!(serde_json::to_string(&Status::Warn).unwrap(), "\"warn\"");

    let server: Check = serde_json::from_value(serde_json::json!({
        "id": "cache.device",
        "category": "cache",
        "status": "pass",
        "summary": "The disk cache is on a local SSD",
        "extra": "ignored",
    }))
    .unwrap();
    assert_eq!(server.detail, None);
    assert_eq!(server.fix, None);
}

#[test]
fn cli_checks_take_their_category_from_the_id() {
    let warn = Check::new(
        "runtime.image",
        "summary",
        Verdict::Warn {
            detail: "detail".into(),
            fix: "fix".into(),
        },
    );
    assert_eq!(warn.category, "runtime");
    assert_eq!(warn.status, Status::Warn);
    assert_eq!(warn.fix.as_deref(), Some("fix"));

    let fail = Check::new(
        "solitary",
        "summary",
        Verdict::Fail {
            detail: None,
            fix: "fix".into(),
        },
    );
    assert_eq!(fail.category, "solitary");
    assert_eq!(fail.detail, None);

    let skip = Check::new("cli.version", "summary", Verdict::Skip("why".into()));
    assert_eq!(
        serde_json::to_value(&skip).unwrap(),
        serde_json::json!({
            "id": "cli.version",
            "category": "cli",
            "status": "skip",
            "summary": "summary",
            "detail": "why",
        })
    );
}

#[test]
fn the_cli_version_check_follows_the_release_feed() {
    let current = cli_version("3.3.0", &LatestRelease::Current);
    assert_eq!(current.status, Status::Pass);
    assert_eq!(current.summary, "Helix CLI v3.3.0 is up to date");

    let behind = cli_version("3.3.0", &LatestRelease::Newer("3.4.0".into()));
    assert_eq!(behind.status, Status::Warn);
    assert_eq!(behind.summary, "Helix CLI v3.3.0 is behind v3.4.0");
    assert_eq!(behind.fix.as_deref(), Some("Run `helix update`."));

    let disabled = cli_version("3.3.0", &LatestRelease::Disabled);
    assert_eq!(disabled.status, Status::Skip);
    assert!(disabled.detail.unwrap().contains("HELIX_NO_UPDATE_CHECK"));

    let unknown = cli_version("3.3.0", &LatestRelease::Unknown("timed out".into()));
    assert_eq!(unknown.status, Status::Skip);
    assert!(unknown.detail.unwrap().ends_with("timed out"));
}

#[test]
fn server_urls_must_be_http_and_lose_their_trailing_slash() {
    assert_eq!(
        server_url(" http://10.0.1.5:8080/ ").unwrap(),
        "http://10.0.1.5:8080"
    );
    assert_eq!(
        server_url("https://helix.internal").unwrap(),
        "https://helix.internal"
    );
    let error = server_url("10.0.1.5:8080").unwrap_err();
    let error = CliError::from_report(&error);
    assert_eq!(error.message, "`10.0.1.5:8080` is not a server URL");
    assert!(error.hint.unwrap().contains("--url http://"));
}

#[test]
fn container_states_map_to_pass_fail_or_skip() {
    let status = |status: &str| {
        Ok(Some(LocalStatus {
            instance_name: "dev".into(),
            container_name: "helix-app-dev".into(),
            status: status.into(),
            ports: "0.0.0.0:6969->8080/tcp".into(),
        }))
    };
    let (running, up) = container_check("dev", status("Up 3 hours"));
    assert!(up);
    assert_eq!(running.status, Status::Pass);
    assert_eq!(running.summary, "Container helix-app-dev is running");
    assert_eq!(running.detail.as_deref(), Some("Up 3 hours"));

    let (stopped, up) = container_check("dev", status("Exited (0) 2 minutes ago"));
    assert!(!up);
    assert_eq!(stopped.status, Status::Fail);
    assert_eq!(stopped.summary, "dev is stopped");
    assert_eq!(stopped.fix.as_deref(), Some("Run `helix start dev`."));

    let (missing, up) = container_check("dev", Ok(None));
    assert!(!up);
    assert_eq!(missing.status, Status::Fail);
    assert_eq!(missing.summary, "dev has not been started");

    let (unreadable, up) = container_check("dev", Err(eyre::eyre!("ps exploded")));
    assert!(!up);
    assert_eq!(unreadable.status, Status::Skip);
    assert!(unreadable.detail.unwrap().contains("ps exploded"));
}

#[test]
fn image_tags_are_compared_with_the_release_this_cli_ships() {
    let default = crate::config::DEFAULT_LOCAL_IMAGE_TAG;
    let image = crate::config::DEFAULT_LOCAL_IMAGE;
    let check = |image: &str, tag: &str| image_check("dev", &local_config(image, tag));

    let shipped = check(image, default);
    assert_eq!(shipped.status, Status::Pass);
    assert_eq!(shipped.detail, Some(format!("{image}:{default}")));

    assert_eq!(check(image, "latest").status, Status::Pass);

    let older = check(image, "v0.0.1");
    assert_eq!(older.status, Status::Warn);
    assert_eq!(older.summary, "dev pins an older server image");
    assert!(older.fix.unwrap().contains(&format!(
        "helix start dev --image-version {default} --persist"
    )));

    let newer = check(image, "v999.0.0");
    assert_eq!(newer.status, Status::Pass);
    assert_eq!(newer.summary, "dev pins server image v999.0.0");

    for tag in [
        "nightly",
        "v1.2-rc1",
        &format!("sha256:{}", "a1".repeat(32)),
    ] {
        assert_eq!(check(image, tag).status, Status::Skip, "{tag}");
    }

    let custom = check("registry.example.com/helix", default);
    assert_eq!(custom.status, Status::Skip);
    assert_eq!(custom.summary, "dev runs a custom image");
}

fn report(target: Diagnosed, checks: Vec<Check>) -> Report {
    let summary = checks
        .iter()
        .fold(Summary::default(), |mut summary, check| {
            match check.status {
                Status::Pass => summary.pass += 1,
                Status::Warn => summary.warn += 1,
                Status::Fail => summary.fail += 1,
                Status::Skip => summary.skip += 1,
            }
            summary
        });
    Report {
        target,
        uptime_secs: Some(7_200),
        checks,
        summary,
    }
}

#[test]
fn the_report_groups_checks_and_explains_only_what_is_not_passing() {
    let mut multi_line = check("queries.missing_indexes", "queries", Status::Warn);
    multi_line.detail = Some("User.email: 12 queries\nPost.slug: 1 query".into());
    multi_line.fix =
        Some("Create each index:\nhelix query dev -e 'a'\nhelix query dev -e 'b'".into());
    let report = report(
        Diagnosed::Local {
            name: "dev".into(),
            url: "http://localhost:6969".into(),
        },
        vec![
            check("cli.version", "cli", Status::Pass),
            check("storage.durability", "storage", Status::Warn),
            check("cache.device", "cache", Status::Skip),
            check("storage.data_device", "storage", Status::Fail),
            check("custom.check", "custom", Status::Pass),
            multi_line,
        ],
    );
    let normal = plain(&render(&report, Verbosity::Normal));
    assert_eq!(
        normal,
        "Helix doctor dev (local) · http://localhost:6969 · up 2h

CLI
  ✓ cli.version summary

Storage
  ▲ storage.durability summary
    storage.durability detail
    → storage.durability fix
  ✗ storage.data_device summary
    storage.data_device detail
    → storage.data_device fix

Disk cache
  ○ cache.device summary
    cache.device detail

custom
  ✓ custom.check summary

Queries
  ▲ queries.missing_indexes summary
    User.email: 12 queries
    Post.slug: 1 query
    → Create each index:
      helix query dev -e 'a'
      helix query dev -e 'b'

Problems need fixing · 1 failure · 2 warnings · 2 passed · 1 skipped
"
    );

    let verbose = plain(&render(&report, Verbosity::Verbose));
    assert!(verbose.contains("  ✓ cli.version summary\n    cli.version detail\n"));

    let quiet = plain(&render(&report, Verbosity::Quiet));
    assert!(!quiet.contains("CLI"), "{quiet}");
    assert!(!quiet.contains("Disk cache"), "{quiet}");
    assert!(quiet.contains("✗ storage.data_device summary"));
    assert!(
        quiet.ends_with("Problems need fixing · 1 failure · 2 warnings · 2 passed · 1 skipped\n")
    );
}

#[test]
fn headers_and_verdicts_name_the_target_and_the_outcome() {
    let healthy = report(
        Diagnosed::Server {
            url: "http://10.0.1.5:8080".into(),
        },
        vec![check("server.reachable", "server", Status::Pass)],
    );
    let rendered = plain(&render(&healthy, Verbosity::Normal));
    assert!(rendered.starts_with("Helix doctor http://10.0.1.5:8080 · up 2h\n"));
    assert!(rendered.ends_with("No problems found · 1 passed\n"));

    let mut improvable = report(
        Diagnosed::Cloud {
            name: "production".into(),
            database: "tenant:t1".into(),
        },
        vec![check("cli.version", "cli", Status::Warn)],
    );
    improvable.uptime_secs = None;
    let rendered = plain(&render(&improvable, Verbosity::Normal));
    assert!(rendered.starts_with("Helix doctor production (cloud) · tenant:t1\n"));
    assert!(rendered.ends_with("Setup works, with room to improve · 1 warning\n"));

    let typed = report(
        Diagnosed::Cloud {
            name: "tenant:t1".into(),
            database: "tenant:t1".into(),
        },
        vec![],
    );
    assert!(plain(&render(&typed, Verbosity::Normal))
        .starts_with("Helix doctor tenant:t1 (cloud) · up 2h\n"));
}

#[test]
fn names_a_query_client_chose_cannot_drive_the_terminal() {
    let mut hostile = check("queries.missing_indexes", "queries\u{7}", Status::Warn);
    hostile.summary = "clip\u{1b}]52;c;cGF5bG9hZA==\u{7}".into();
    hostile.detail = Some("User\u{1b}[2J.email".into());
    hostile.fix = Some("helix query <instance> -e 'x\u{9b}31m'".into());
    let rendered = render(
        &report(
            Diagnosed::Server {
                url: "http://10.0.1.5:8080".into(),
            },
            vec![hostile],
        ),
        Verbosity::Normal,
    );
    for raw in ["\u{1b}]52", "\u{1b}[2J", "\u{9b}31m", "queries\u{7}"] {
        assert!(!rendered.contains(raw), "{raw:?} reached the terminal");
    }
    assert!(
        rendered.contains(r"clip\u{1b}]52;c;cGF5bG9hZA==\u{7}"),
        "{rendered}"
    );
    assert!(rendered.contains(r"User\u{1b}[2J.email"));
    assert!(rendered.contains(r"queries\u{7}"));
    assert_eq!(printable("plain 'quotes' stay"), "plain 'quotes' stay");
}

#[test]
fn uptimes_categories_and_plurals_read_naturally() {
    assert_eq!(uptime(59), "59s");
    assert_eq!(uptime(60), "1m");
    assert_eq!(uptime(3_599), "59m");
    assert_eq!(uptime(3_600), "1h");
    assert_eq!(uptime(86_400 * 2), "2d");
    for (category, title) in [
        ("cli", "CLI"),
        ("runtime", "Local runtime"),
        ("server", "Server"),
        ("cloud", "Helix Cloud"),
        ("storage", "Storage"),
        ("cache", "Disk cache"),
        ("resources", "Resources"),
        ("indexes", "Indexes"),
        ("queries", "Queries"),
        ("future", "future"),
    ] {
        assert_eq!(category_title(category), title);
    }
    assert_eq!(plural(1, "check", "checks"), "check");
    assert_eq!(plural(2, "check", "checks"), "checks");
}

async fn health(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn server_checks_append_the_servers_report_and_name_the_instance_in_fixes() {
    let server = MockServer::start().await;
    health(
        &server,
        serde_json::json!({"ready": true, "mode": "writer"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v2/diagnostics"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "uptime_secs": 42,
            "checks": [{
                "id": "queries.missing_indexes",
                "category": "queries",
                "status": "warn",
                "summary": "1 filter scans without an index",
                "detail": "User.email (node equality): 3 queries, last seen 2 s ago",
                "fix": "Create each index:\nhelix query <instance> -e 'x'"
            }]
        })))
        .mount(&server)
        .await;

    let mut checks = Vec::new();
    let uptime = server_checks(&server.uri(), Some("dev"), &mut checks).await;
    assert_eq!(uptime, Some(42));
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].id, "server.reachable");
    assert_eq!(checks[0].status, Status::Pass);
    assert_eq!(checks[0].detail.as_deref(), Some("Running as a writer."));
    assert_eq!(
        checks[1].fix.as_deref(),
        Some("Create each index:\nhelix query dev -e 'x'")
    );

    // A URL target has no instance name to substitute.
    let mut checks = Vec::new();
    server_checks(&server.uri(), None, &mut checks).await;
    assert!(checks[1].fix.as_deref().unwrap().contains("<instance>"));
}

#[tokio::test]
async fn an_unready_server_warns_and_an_old_one_skips_its_checks() {
    let server = MockServer::start().await;
    health(
        &server,
        serde_json::json!({"ready": false, "mode": "reader", "index_runtime": "loading"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v2/diagnostics"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let mut checks = Vec::new();
    assert_eq!(server_checks(&server.uri(), None, &mut checks).await, None);
    assert_eq!(checks[0].status, Status::Warn);
    assert_eq!(checks[0].detail.as_deref(), Some("Index runtime: loading."));
    assert_eq!(checks[1].id, "server.diagnostics");
    assert_eq!(checks[1].status, Status::Skip);
    assert!(checks[1].detail.as_deref().unwrap().contains("predates"));
}

#[tokio::test]
async fn diagnostics_errors_and_bad_reports_are_skipped_with_their_cause() {
    for (response, expected) in [
        (ResponseTemplate::new(500), "answered HTTP 500"),
        (
            ResponseTemplate::new(200).set_body_string("not json"),
            "Could not read the server's report",
        ),
    ] {
        let server = MockServer::start().await;
        health(&server, serde_json::json!({})).await;
        Mock::given(method("GET"))
            .and(path("/v2/diagnostics"))
            .respond_with(response)
            .mount(&server)
            .await;
        let mut checks = Vec::new();
        assert_eq!(server_checks(&server.uri(), None, &mut checks).await, None);
        assert_eq!(
            checks[0].status,
            Status::Pass,
            "missing health fields still pass"
        );
        assert_eq!(
            checks[0].detail, None,
            "a health body without a mode names none"
        );
        assert!(
            checks[1].detail.as_deref().unwrap().contains(expected),
            "{:?}",
            checks[1].detail
        );
    }
}

#[tokio::test]
async fn unreachable_or_failing_servers_fail_the_reachability_check() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let mut checks = Vec::new();
    assert_eq!(
        server_checks(&server.uri(), Some("dev"), &mut checks).await,
        None
    );
    let [failing] = checks.as_slice() else {
        panic!("only the reachability check: {checks:?}");
    };
    assert_eq!(failing.status, Status::Fail);
    assert!(failing
        .summary
        .ends_with("answered HTTP 503 Service Unavailable"));
    assert_eq!(
        failing.fix.as_deref(),
        Some("Check `helix logs dev`, then `helix restart dev`.")
    );

    // Nothing listens on a port that was just released.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    };
    let mut checks = Vec::new();
    assert_eq!(server_checks(&closed, None, &mut checks).await, None);
    let [unreachable] = checks.as_slice() else {
        panic!("only the reachability check: {checks:?}");
    };
    assert_eq!(unreachable.status, Status::Fail);
    assert!(unreachable
        .summary
        .starts_with("Cannot reach the server at"));
    assert!(unreachable
        .fix
        .as_deref()
        .unwrap()
        .starts_with("Check the URL"));
}
