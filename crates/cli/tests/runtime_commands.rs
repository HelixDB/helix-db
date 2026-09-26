mod support;

use assert_cmd::assert::Assert;
use std::fs;
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The disk-mode dependency pin, mirrored by the fake runtime in `support`.
const SEAWEEDFS_IMAGE: &str = "ghcr.io/chrislusf/seaweedfs:4.47@sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882";

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_runtime_commands_cover_resource_reuse_status_cleanup_and_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("disk-command-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();

    let container = "helix-disk-command-project-dev";
    let ps_output = format!(
        "{container}\tUp 1 minute\tlocalhost:{}",
        server.address().port()
    );
    let status = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["status", "dev"])
            .env("HELIX_TEST_RUNTIME_PS_OUTPUT", &ps_output)
            .assert()
            .success(),
    );
    assert!(status.contains("Up 1 minute"));
    assert!(status.contains("storage: disk"));
    assert!(status.contains(&server.address().port().to_string()));

    fixture
        .command()
        .current_dir(&project)
        .args(["logs", "dev", "--follow"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["restart", "dev"])
        .env(
            "HELIX_TEST_RUNTIME_PORT_OUTPUT",
            format!("127.0.0.1:{}", server.address().port()),
        )
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();

    let stopped = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["stop", "dev"])
            .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
            .assert()
            .success(),
    );
    assert!(stopped.contains("Stopped 'dev' successfully"));
    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "--all", "--yes"])
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();

    let status_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["status", "dev"])
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "ps")
            .assert()
            .failure(),
    );
    assert!(status_error.contains("simulated runtime failure"));

    let log = fixture.runtime_log();
    assert!(log.contains("network create"));
    assert!(log.contains(&format!(
        "volume create --label helixdb.identity=20:disk-command-project/dev {container}-seaweedfs-data"
    )));
    assert!(log.contains(&format!(
        "run -d --restart unless-stopped --name {container}-seaweedfs"
    )));
    assert!(log.contains(&format!("exec {container}-seaweedfs sh -c")));
    assert!(log.contains(&format!("AWS_ENDPOINT=http://{container}-seaweedfs:8333")));
    assert!(!log.contains("quay.io/minio"), "{log}");
    assert!(log.contains("logs -f"));
    assert!(log.contains("network inspect"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_minio_sidecars_are_removed_and_their_data_is_kept_until_prune() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("upgraded-disk");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    // Every resource inspection succeeds, so the MinIO-era volume is present.
    let started = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["start", "dev"])
            .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
            .assert()
            .success(),
    );
    let base = "helix-upgraded-disk-dev";
    assert!(
        started.contains(&format!(
            "Volume {base}-minio-data holds data from a MinIO-based Helix CLI"
        )),
        "{started}"
    );
    assert!(started.contains(&format!("now stores data in {base}-seaweedfs-data")));
    assert!(started.contains("#migrate-minio-disk-data"));

    let log = fixture.runtime_log().replace('\r', "");
    let lines: Vec<_> = log.lines().collect();
    let legacy_removal = lines
        .iter()
        .position(|line| *line == format!("rm -f {base}-minio"))
        .expect("start must remove the MinIO sidecar");
    let network_inspect = lines
        .iter()
        .position(|line| *line == format!("network inspect {base}-net"))
        .expect("start must reuse the instance network");
    assert!(
        legacy_removal < network_inspect,
        "the MinIO sidecar must leave the shared network first: {log}"
    );
    assert!(!log.contains(&format!("-v {base}-minio-data")), "{log}");

    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();
    let log = fixture.runtime_log().replace('\r', "");
    assert_eq!(
        log.matches(&format!("rm -f {base}-minio\n")).count(),
        2,
        "{log}"
    );
    assert!(
        !log.contains("volume rm"),
        "stop must keep both volumes: {log}"
    );

    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "dev", "--yes"])
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();
    let log = fixture.runtime_log().replace('\r', "");
    assert!(
        log.contains(&format!("volume rm {base}-seaweedfs-data\n")),
        "{log}"
    );
    assert!(
        log.contains(&format!("volume rm {base}-minio-data\n")),
        "{log}"
    );
}

#[test]
fn seaweedfs_readiness_failure_stops_before_starting_helix() {
    for foreground in [false, true] {
        let fixture = CliFixture::new_with_fake_runtime();
        let project = fixture.root().join("unready-disk");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(
            project.join("helix.toml"),
            "[project]\nname = \"unready-disk\"\n[local.dev]\nstorage = \"disk\"\n",
        )
        .unwrap();
        let mut command = fixture.command();
        command
            .current_dir(&project)
            .args(["start", "dev"])
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "exec");
        if foreground {
            command.arg("--foreground");
        }
        let error = stderr(command.assert().failure());
        assert!(
            error.contains("local SeaweedFS bucket helix-db did not become ready"),
            "{error}"
        );
        assert!(error.contains("simulated runtime failure"), "{error}");
        assert!(
            error.contains("docker logs helix-unready-disk-dev-seaweedfs"),
            "{error}"
        );
        let log = fixture.runtime_log();
        let runs: Vec<_> = log
            .lines()
            .filter(|line| line.starts_with("run "))
            .collect();
        assert_eq!(runs.len(), 1, "only the sidecar may start: {log}");
        assert!(
            runs[0].contains("--name helix-unready-disk-dev-seaweedfs"),
            "{log}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_suffixed_legacy_resources_are_adopted_on_upgrade() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("upgrade-corner-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = project.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    let started = stdout(
        fixture
            .command()
            .current_dir(&project)
            .env("HELIX_TEST_RUNTIME_VOLUME_MODE", "existing")
            .args(["start", instance])
            .assert()
            .success(),
    );

    let legacy = "helix-upgrade-corner-project-14527b3cbdf37376ceb9eda41d2afac4";
    let log = fixture.runtime_log();
    assert!(
        log.contains(&format!("volume inspect {legacy}-seaweedfs-data")),
        "expected the legacy volume to be adopted, got: {log}"
    );
    assert!(
        started.contains(&format!("Volume {legacy}-minio-data holds data")),
        "the adopted name must also find its MinIO-era volume, got: {started}"
    );
    assert!(
        !log.contains("volume create"),
        "an adopted volume must be reopened, not recreated, got: {log}"
    );
    assert!(
        log.contains(&format!("--name {legacy} -p")),
        "expected the legacy container name, got: {log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_hash_suffixed_names_get_their_own_digest() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("upgrade-corner-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = project.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&project)
        .env("HELIX_TEST_RUNTIME_LABEL_PROBE", "missing")
        .args(["start", instance])
        .assert()
        .success();

    let suffixed =
        "helix-upgrade-corner-project-14527b3cbdf37376ceb9eda41d2afac4-4d82fafccc46ce0a61a48599cc612258";
    let log = fixture.runtime_log();
    assert!(
        log.contains(&format!(
            "volume create --label helixdb.identity=22:upgrade-corner-project/14527b3cbdf37376ceb9eda41d2afac4 {suffixed}-seaweedfs-data"
        )),
        "expected a fresh labeled suffixed volume, got: {log}"
    );
    assert!(
        log.contains(&format!("--name {suffixed} -p")),
        "expected a fresh suffixed container, got: {log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_labeled_resources_are_not_adopted_or_removed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let first = fixture.root().join("a b");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&first)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--no-skills"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&first)
        .args(["start", "dev"])
        .assert()
        .success();

    let second = fixture.root().join("a-b-dev");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&second)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = second.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&second)
        .env("HELIX_TEST_RUNTIME_LABEL_PROBE", "3:a b/dev")
        .args(["start", instance])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&second)
        .env("HELIX_TEST_RUNTIME_LABEL_PROBE", "3:a b/dev")
        .args(["stop", instance])
        .assert()
        .success();

    let legacy = "helix-a-b-dev-14527b3cbdf37376ceb9eda41d2afac4";
    let log = fixture.runtime_log().replace('\r', "");
    assert!(
        log.contains(&format!(
            "--name {legacy}-13e3b00b2c8ffd87792b25c1d1cf2aea -p"
        )),
        "the second identity must use its own suffixed name, got: {log}"
    );
    assert_eq!(
        log.matches(&format!("--name {legacy} -p")).count(),
        1,
        "the legacy run line must belong to the first identity only, got: {log}"
    );
    assert_eq!(
        log.matches(&format!("rm -f {legacy}\n")).count(),
        1,
        "the first identity's resources must not be removed again, got: {log}"
    );
    assert!(
        log.contains(&format!(
            "rm -f {legacy}-13e3b00b2c8ffd87792b25c1d1cf2aea\n"
        )),
        "stopping the second identity must only remove its own resources, got: {log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_ownership_is_not_adopted() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("a-b-dev");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = project.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&project)
        .env("HELIX_TEST_RUNTIME_CONTAINER_LABEL", "unlabeled")
        .env("HELIX_TEST_RUNTIME_NETWORK_LABEL", "unlabeled")
        .env("HELIX_TEST_RUNTIME_VOLUME_LABEL", "3:a b/dev")
        .args(["start", instance])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .env("HELIX_TEST_RUNTIME_CONTAINER_LABEL", "unlabeled")
        .env("HELIX_TEST_RUNTIME_NETWORK_LABEL", "unlabeled")
        .env("HELIX_TEST_RUNTIME_VOLUME_LABEL", "3:a b/dev")
        .args(["prune", instance, "--yes"])
        .assert()
        .success();

    let legacy = "helix-a-b-dev-14527b3cbdf37376ceb9eda41d2afac4";
    let suffixed = format!("{legacy}-13e3b00b2c8ffd87792b25c1d1cf2aea");
    let log = fixture.runtime_log().replace('\r', "");
    assert!(
        log.contains(&format!("--name {suffixed} -p")),
        "mixed ownership must use the suffixed name, got: {log}"
    );
    assert!(
        !log.contains(&format!("--name {legacy} -p")),
        "mixed ownership must not adopt the legacy container, got: {log}"
    );
    assert!(
        log.contains(&format!(
            "volume create --label helixdb.identity=7:a-b-dev/{instance} {suffixed}-seaweedfs-data"
        )),
        "mixed ownership must create its own volume, got: {log}"
    );
    assert!(
        !log.contains(&format!(
            "volume create --label helixdb.identity=7:a-b-dev/{instance} {legacy}-seaweedfs-data"
        )),
        "mixed ownership must not reuse the foreign volume, got: {log}"
    );
    assert!(
        log.contains(&format!("rm -f {suffixed}\n")),
        "prune must remove the suffixed container, got: {log}"
    );
    assert!(
        !log.contains(&format!("rm -f {legacy}\n")),
        "prune must not remove the legacy container, got: {log}"
    );
    for volume in ["seaweedfs-data", "minio-data"] {
        assert!(
            log.contains(&format!("volume rm {suffixed}-{volume}")),
            "prune must remove the suffixed {volume} volume, got: {log}"
        );
        assert!(
            !log.contains(&format!("volume rm {legacy}-{volume}")),
            "prune must not remove the foreign {volume} volume, got: {log}"
        );
    }
    assert!(
        !log.contains(&format!("rm -f {legacy}-minio\n")),
        "prune must not remove the foreign MinIO sidecar, got: {log}"
    );
    assert!(
        !log.contains(&format!("network rm {legacy}-net\n")),
        "prune must not remove the legacy network, got: {log}"
    );
}

#[test]
fn logs_and_status_report_a_missing_runtime_like_stop_does() {
    let fixture = CliFixture::new().with_missing_runtime();
    let project = fixture.root().join("missing-runtime-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    for command in [["logs", "dev"], ["status", "dev"], ["stop", "dev"]] {
        let message = stderr(
            fixture
                .command()
                .current_dir(&project)
                .args(command)
                .assert()
                .failure(),
        );
        assert!(
            message.contains("Docker is not installed"),
            "`helix {}` should name the missing runtime, got: {message}",
            command.join(" ")
        );
        // The number is platform-specific: ENOENT is 2, while Windows reports 3
        // (ERROR_PATH_NOT_FOUND) when the parent directory is absent too. What
        // matters is that the originating error survives as the cause.
        assert!(
            message.contains("os error"),
            "`helix {}` should keep the underlying cause, got: {message}",
            command.join(" ")
        );
    }
}

/// A runtime that is present but refuses to launch is a different failure from
/// one that is not installed, and must not be reported as a missing install.
#[cfg(unix)]
#[test]
fn a_present_but_unspawnable_runtime_keeps_its_command_error() {
    let fixture = CliFixture::new().with_unspawnable_runtime();
    let project = fixture.root().join("unspawnable-runtime-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    for command in [["logs", "dev"], ["status", "dev"]] {
        let message = stderr(
            fixture
                .command()
                .current_dir(&project)
                .args(command)
                .assert()
                .failure(),
        );
        assert!(
            !message.contains("is not installed"),
            "`helix {}` should not blame the install for a permission failure, got: {message}",
            command.join(" ")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_selection_and_pull_policy_matrix() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // Each case gets a fresh cache. Both runtime choices use the same contract.
    for runtime in ["docker", "podman"] {
        for (tag, policy, cached, fail_pull, succeeds, pulls) in [
            ("v1.2.3", None, true, false, true, false),
            ("v1.2.3", None, false, false, true, true),
            ("latest", None, true, false, true, true),
            ("latest", None, true, true, false, true),
            ("v1.2.3", Some("always"), true, true, false, true),
            ("v1.2.3", Some("always"), true, false, true, true),
            ("latest", Some("missing"), true, false, true, false),
            ("latest", Some("never"), true, false, true, false),
            ("v1.2.3", Some("never"), false, false, false, false),
            ("v1.2.3", Some("missing"), false, true, false, true),
        ] {
            let fixture = CliFixture::new_with_fake_runtime();
            let project = fixture.root().join("image-project");
            std::fs::create_dir(&project).unwrap();
            std::fs::write(project.join("helix.toml"), format!(
                "[project]\nname = \"image-project\"\ncontainer_runtime = \"{runtime}\"\n[local.dev]\nport = {}\ntag = \"{tag}\"\n",
                server.address().port(),
            )).unwrap();
            let mut command = fixture.command();
            command.current_dir(&project).args(["start", "dev"]);
            command.args(policy.into_iter().flat_map(|policy| ["--pull", policy]));
            if !cached {
                command.env("HELIX_TEST_RUNTIME_IMAGE_MISSING", "1");
            }
            if fail_pull {
                command.env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "pull");
            }
            let result = command.assert();
            if succeeds {
                let output = stdout(result.success());
                assert!(output.contains(&format!("ghcr.io/helixdb/helixdb:{tag}")));
                assert!(output.contains("sha256:"));
            } else {
                result.failure();
            }
            let log = fixture.runtime_log();
            assert_eq!(
                log.lines().any(|line| line.starts_with("pull ")),
                pulls,
                "{tag} {policy:?}: {log}"
            );
            assert_eq!(
                log.lines().any(|line| line.starts_with("run ")),
                succeeds,
                "{log}"
            );
            if !succeeds {
                assert!(
                    !log.lines().any(|line| line.starts_with("rm ")),
                    "failed pull must preserve existing containers: {log}"
                );
            } else {
                assert!(
                    log.lines()
                        .find(|line| line.starts_with("run "))
                        .unwrap()
                        .contains("sha256:"),
                    "must run resolved image ID: {log}"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_overrides_persist_only_when_requested_and_support_digests() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    for persist in [false, true] {
        let fixture = CliFixture::new_with_fake_runtime();
        let project = fixture.root().join("image-project");
        std::fs::create_dir(&project).unwrap();
        let original = format!("[project]\nname = \"image-project\"\n[local.dev]\nport = {}\ntag = \"latest\"\npull = \"always\"\n", server.address().port());
        std::fs::write(project.join("helix.toml"), &original).unwrap();
        let digest = format!("sha256:{}", "b".repeat(64));
        let mut command = fixture.command();
        command.current_dir(&project).args([
            "start",
            "dev",
            "--image-version",
            &digest,
            "--pull",
            "never",
        ]);
        if persist {
            command.arg("--persist");
        }
        command.assert().success();
        let saved = std::fs::read_to_string(project.join("helix.toml")).unwrap();
        if persist {
            let config: helix_cli::config::HelixConfig = toml::from_str(&saved).unwrap();
            assert_eq!(config.local["dev"].tag.to_string(), digest);
            assert_eq!(
                config.local["dev"].pull,
                Some(helix_cli::image::PullPolicy::Never)
            );
        } else {
            assert_eq!(saved, original);
        }
        let log = fixture.runtime_log();
        assert!(log.contains(&format!("ghcr.io/helixdb/helixdb@{digest}")));
        assert!(!log.lines().any(|line| line.starts_with("pull ")));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_keeps_existing_image_for_all_storage_modes_and_never_falls_back() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    for storage in ["memory", "disk", "s3"] {
        for fails in [false, true] {
            let fixture = CliFixture::new_with_fake_runtime();
            let project = fixture.root().join("restart-project");
            std::fs::create_dir(&project).unwrap();
            let s3 = if storage == "s3" {
                "[local.dev.s3]\nbucket = \"test-bucket\"\n"
            } else {
                ""
            };
            std::fs::write(project.join("helix.toml"), format!(
                "[project]\nname = \"restart-project\"\n[local.dev]\nport = {}\ntag = \"latest\"\nstorage = \"{storage}\"\n{s3}", server.address().port()
            )).unwrap();
            let mut command = fixture.command();
            command.current_dir(&project).args(["restart", "dev"]).env(
                "HELIX_TEST_RUNTIME_PORT_OUTPUT",
                format!("127.0.0.1:{}", server.address().port()),
            );
            if fails {
                command.env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "restart");
            }
            let result = command.assert();
            if fails {
                assert!(stderr(result.failure()).contains("helix start dev"));
            } else {
                result.success();
            }
            let log = fixture.runtime_log();
            assert!(log.contains("restart helix-restart-project-dev"));
            for forbidden in ["pull ", "run ", "rm ", "image "] {
                assert!(
                    !log.lines().any(|line| line.starts_with(forbidden)),
                    "{log}"
                );
            }
        }
    }
}

#[test]
fn invalid_image_flags_and_configuration_fail_before_runtime_access() {
    for args in [
        vec!["--image-version", "bad/tag"],
        vec!["--pull", "sometimes"],
    ] {
        let fixture = CliFixture::new_with_fake_runtime();
        fixture.command().arg("start").args(args).assert().failure();
        assert!(fixture.runtime_log().is_empty());
    }
    for field in ["tag = \"bad tag\"", "pull = \"sometimes\""] {
        let fixture = CliFixture::new_with_fake_runtime();
        let project = fixture.root().join("invalid-project");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(
            project.join("helix.toml"),
            format!("[project]\nname = \"invalid-project\"\n[local.dev]\n{field}\n"),
        )
        .unwrap();
        fixture
            .command()
            .current_dir(&project)
            .arg("start")
            .assert()
            .failure();
        assert!(fixture.runtime_log().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_policy_applies_to_foreground_and_dependency_failures_preserve_containers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    for foreground in [false, true] {
        for failed_image in ["", SEAWEEDFS_IMAGE] {
            let fixture = CliFixture::new_with_fake_runtime();
            let project = fixture.root().join("image-project");
            std::fs::create_dir(&project).unwrap();
            std::fs::write(project.join("helix.toml"), format!(
                "[project]\nname = \"image-project\"\n[local.dev]\nport = {}\npull = \"always\"\nstorage = \"disk\"\n", server.address().port()
            )).unwrap();
            let mut command = fixture.command();
            command.current_dir(&project).args(["start", "dev"]);
            if foreground {
                command.arg("--foreground");
            }
            if !failed_image.is_empty() {
                command.env("HELIX_TEST_RUNTIME_FAIL_IMAGE", failed_image);
            }
            let result = command.assert();
            if failed_image.is_empty() {
                result.success();
            } else {
                result.failure();
            }
            let log = fixture.runtime_log();
            assert!(log.contains("pull ghcr.io/helixdb/helixdb:v0.0.6"));
            assert!(log.contains(&format!("pull {SEAWEEDFS_IMAGE}")));
            if !failed_image.is_empty() {
                assert!(
                    !log.lines()
                        .any(|line| line.starts_with("rm ") || line.starts_with("run ")),
                    "{log}"
                );
            }
        }
    }
}

#[test]
fn unresolved_image_after_pull_never_replaces_a_container() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("image-project");
    std::fs::create_dir(&project).unwrap();
    std::fs::write(
        project.join("helix.toml"),
        "[project]\nname = \"image-project\"\n[local.dev]\npull = \"always\"\n",
    )
    .unwrap();
    let error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .arg("start")
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "image")
            .assert()
            .failure(),
    );
    assert!(error.contains("Cannot inspect local image"));
    let log = fixture.runtime_log();
    assert!(log.contains("pull "));
    assert!(!log
        .lines()
        .any(|line| line.starts_with("rm ") || line.starts_with("run ")));
}

#[test]
fn failed_image_resolution_with_persist_preserves_config_and_containers() {
    for foreground in [false, true] {
        for (policy, missing, failed_command, failed_image) in [
            ("always", false, "pull", ""),
            ("never", true, "", ""),
            ("missing", true, "pull", ""),
            ("always", false, "image", ""),
            ("always", false, "", SEAWEEDFS_IMAGE),
        ] {
            let fixture = CliFixture::new_with_fake_runtime();
            let project = fixture.root().join("persist-failure");
            std::fs::create_dir(&project).unwrap();
            let original =
                "# preserve comments too\n[project]\nname = \"persist-failure\"\n[local.dev]\n";
            let path = project.join("helix.toml");
            std::fs::write(&path, original).unwrap();
            let mut command = fixture.command();
            command
                .current_dir(&project)
                .args([
                    "start",
                    "dev",
                    "--persist",
                    "--image-version",
                    "unavailable",
                    "--pull",
                    policy,
                    "--disk",
                    "--port",
                    "12345",
                ])
                .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", failed_command)
                .env("HELIX_TEST_RUNTIME_FAIL_IMAGE", failed_image);
            if missing {
                command.env("HELIX_TEST_RUNTIME_IMAGE_MISSING", "1");
            }
            if foreground {
                command.arg("--foreground");
            }
            command.assert().failure();
            assert_eq!(std::fs::read_to_string(path).unwrap(), original);
            assert!(!fixture
                .runtime_log()
                .lines()
                .any(|line| line.starts_with("run ") || line.starts_with("rm ")));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_disk_container_uses_its_resolved_image_without_resolving_again() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    for foreground in [false, true] {
        let fixture = CliFixture::new_with_fake_runtime();
        let project = fixture.root().join("resolved-images");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(project.join("helix.toml"), format!("[project]\nname = \"resolved-images\"\n[local.dev]\nport = {}\nstorage = \"disk\"\n", server.address().port())).unwrap();
        let mut command = fixture.command();
        command
            .current_dir(&project)
            .args(["start", "dev", "--persist"]);
        if foreground {
            command.arg("--foreground");
        }
        command.assert().success();
        let log = fixture.runtime_log();
        let lines: Vec<_> = log.lines().collect();
        let first_removal = lines
            .iter()
            .position(|line| line.starts_with("rm "))
            .unwrap();
        let inspections: Vec<_> = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.starts_with("image inspect "))
            .collect();
        assert_eq!(inspections.len(), 2);
        assert!(inspections.iter().all(|(index, _)| *index < first_removal));
        let runs: Vec<_> = lines
            .iter()
            .filter(|line| line.starts_with("run "))
            .collect();
        assert_eq!(runs.len(), 2);
        for (run, byte) in runs.iter().zip(['c', 'a']) {
            assert!(
                run.contains(&format!("sha256:{}", byte.to_string().repeat(64))),
                "{run}"
            );
            assert!(
                !run.contains(":latest"),
                "mutable tags must not be used: {run}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_probes_the_retained_port_instead_of_current_config() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;
    for address in ["127.0.0.1", "[::]"] {
        let fixture = CliFixture::new_with_fake_runtime();
        let project = fixture.root().join("retained-port");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(
            project.join("helix.toml"),
            "[project]\nname = \"retained-port\"\n[local.dev]\nport = 1\n",
        )
        .unwrap();
        fixture
            .command()
            .current_dir(&project)
            .args(["restart", "dev"])
            .env(
                "HELIX_TEST_RUNTIME_PORT_OUTPUT",
                format!("{address}:{}", server.address().port()),
            )
            .assert()
            .success();
        assert!(fixture
            .runtime_log()
            .contains("port helix-retained-port-dev 8080/tcp"));
    }
}

#[test]
fn restart_rejects_unavailable_or_invalid_published_ports() {
    for (published, fail) in [
        ("", false),
        ("garbage", false),
        ("127.0.0.1:0", false),
        ("127.0.0.1:65536", false),
        ("127.0.0.1:abc", false),
        ("", true),
    ] {
        let fixture = CliFixture::new_with_fake_runtime();
        let project = fixture.root().join("invalid-port");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(
            project.join("helix.toml"),
            "[project]\nname = \"invalid-port\"\n[local.dev]\n",
        )
        .unwrap();
        let mut command = fixture.command();
        command
            .current_dir(&project)
            .args(["restart", "dev"])
            .env("HELIX_TEST_RUNTIME_PORT_OUTPUT", published);
        if fail {
            command.env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "port");
        }
        assert!(stderr(command.assert().failure()).contains("published port"));
        assert!(!fixture.runtime_log().contains("run "));
    }
}
