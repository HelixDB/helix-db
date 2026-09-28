mod support;

use assert_cmd::assert::Assert;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SDK_VERSION: &str = "3.1.0";

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

fn init_project(fixture: &CliFixture, server: &MockServer, name: &str) -> PathBuf {
    let project = fixture.root().join(name);
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .arg("--no-skills")
        .assert()
        .success();
    project
}

fn generated_request(request_type: &str) -> String {
    json!({
        "request_type": request_type,
        "query": {"queries": [], "returns": []},
        "parameters": {}
    })
    .to_string()
}

fn npm_install_count(fixture: &CliFixture) -> usize {
    fixture
        .tool_log()
        .lines()
        .filter(|line| line.starts_with("npm install "))
        .count()
}

fn assert_no_runtime_debris(cache: &Path) {
    let entries = fs::read_dir(cache)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        entries.iter().all(|name| {
            !name.starts_with("ts-runtime-prepare-")
                && !name.starts_with("ts-runtime-backup-")
                && name != ".ts-runtime-install-lock"
        }),
        "unexpected TypeScript runtime debris: {entries:?}"
    );
    if let Ok(entries) = fs::read_dir(cache.join("ts-runtime")) {
        let wrappers = entries
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("__helix_query_"))
            .collect::<Vec<_>>();
        assert!(
            wrappers.is_empty(),
            "query wrappers were not cleaned: {wrappers:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_runtime_covers_cache_install_failure_and_cleanup_states() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime().with_fake_tools();
    let project = init_project(&fixture, &server, "typescript-install-project");
    let runtime = fixture.cache().join("ts-runtime");
    let sdk = runtime.join("node_modules/@helix-db/helix-db");
    let read = generated_request("read");

    // Cold install.
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    assert_eq!(npm_install_count(&fixture), 1);
    assert_eq!(
        fs::read_to_string(runtime.join(".sdk-version")).unwrap(),
        SDK_VERSION
    );
    let runtime_package: serde_json::Value =
        serde_json::from_slice(&fs::read(runtime.join("package.json")).unwrap()).unwrap();
    assert_eq!(
        runtime_package["dependencies"]["@helix-db/helix-db"],
        SDK_VERSION
    );
    assert_no_runtime_debris(fixture.cache());

    // Warm cache.
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    assert_eq!(npm_install_count(&fixture), 1);

    // Missing package, corrupt manifest, and wrong version all rebuild.
    fs::remove_dir_all(&sdk).unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    assert_eq!(npm_install_count(&fixture), 2);

    fs::write(sdk.join("package.json"), "{").unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    assert_eq!(npm_install_count(&fixture), 3);

    fs::write(
        sdk.join("package.json"),
        r#"{"name":"@helix-db/helix-db","version":"2.0.5"}"#,
    )
    .unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    assert_eq!(npm_install_count(&fixture), 4);

    // An interrupted preparation is removed before the next atomic promotion.
    fs::remove_dir_all(&runtime).unwrap();
    let partial = fixture.cache().join("ts-runtime-prepare-orphan");
    fs::create_dir(&partial).unwrap();
    fs::write(partial.join("partial"), "incomplete").unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    assert_eq!(npm_install_count(&fixture), 5);
    assert!(!partial.exists());

    // A package that installs but cannot be imported is never promoted.
    fs::write(runtime.join(".sdk-version"), "wrong").unwrap();
    let verify_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_FAIL_COMMAND", "--input-type=module")
            .env("HELIX_TEST_TOOL_STDERR", "module import failed")
            .assert()
            .failure(),
    );
    assert!(verify_error.contains("installed TypeScript query runtime is unusable"));
    assert!(verify_error.contains("module import failed"));
    assert_eq!(
        fs::read_to_string(runtime.join(".sdk-version")).unwrap(),
        "wrong"
    );
    assert_no_runtime_debris(fixture.cache());

    // npm failure preserves the previous runtime and leaves no preparation state.
    let install_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_FAIL_COMMAND", "install")
            .env("HELIX_TEST_TOOL_STDERR", "registry unavailable")
            .assert()
            .failure(),
    );
    assert!(install_error.contains("failed to install the TypeScript query runtime"));
    assert!(install_error.contains("registry unavailable"));
    assert_eq!(
        fs::read_to_string(runtime.join(".sdk-version")).unwrap(),
        "wrong"
    );
    assert_no_runtime_debris(fixture.cache());

    let tools = fixture.root().join("tools");
    let npm = tools.join(if cfg!(windows) { "npm.cmd" } else { "npm" });
    let hidden_npm = tools.join("npm.disabled");
    fs::rename(&npm, &hidden_npm).unwrap();
    let missing_npm = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .assert()
            .failure(),
    );
    assert!(missing_npm.contains("npm is required"));
    fs::rename(hidden_npm, npm).unwrap();

    // Repair, then distinguish Node failure and invalid JSON output. Both clean
    // their unique wrapper file.
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &read)
        .assert()
        .success();
    let node_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_FAIL_NODE_QUERY", "1")
            .env("HELIX_TEST_TOOL_STDERR", "Node evaluation failed")
            .assert()
            .failure(),
    );
    assert!(node_error.contains("TypeScript query failed to evaluate"));
    assert!(node_error.contains("Node evaluation failed"));
    assert_no_runtime_debris(fixture.cache());

    let invalid_output = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_STDOUT", "not JSON")
            .assert()
            .failure(),
    );
    assert!(invalid_output.contains("did not produce valid JSON"));
    assert_no_runtime_debris(fixture.cache());

    let node = tools.join(if cfg!(windows) { "node.cmd" } else { "node" });
    let hidden_node = tools.join("node.disabled");
    fs::rename(&node, &hidden_node).unwrap();
    let missing_node = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .assert()
            .failure(),
    );
    assert!(missing_node.contains("Node.js is required"));
    fs::rename(hidden_node, node).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_cold_queries_share_one_atomic_install() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime().with_fake_tools();
    let project = init_project(&fixture, &server, "typescript-concurrent-project");
    let request = generated_request("read");

    let configured = |project: &Path| {
        let mut configured = fixture.command();
        configured
            .current_dir(project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_STDOUT", &request);
        let mut command = Command::new(configured.get_program());
        command.args(configured.get_args());
        if let Some(directory) = configured.get_current_dir() {
            command.current_dir(directory);
        }
        for (name, value) in configured.get_envs() {
            match value {
                Some(value) => {
                    command.env(name, value);
                }
                None => {
                    command.env_remove(name);
                }
            }
        }
        command
    };

    let mut first = configured(&project);
    let mut second = configured(&project);
    let first = thread::spawn(move || first.output().unwrap());
    let second = thread::spawn(move || second.output().unwrap());
    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert!(
        first.status.success(),
        "first query failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        second.status.success(),
        "second query failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(npm_install_count(&fixture), 1);
    assert_no_runtime_debris(fixture.cache());
}

fn build_checkout_tarball(output: &Path) -> PathBuf {
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdks/typescript");
    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };
    assert!(
        sdk.join("node_modules/typescript").is_dir(),
        "run `npm ci` in sdks/typescript before the CLI suite"
    );
    let build = Command::new(npm)
        .args(["run", "build"])
        .current_dir(&sdk)
        .status()
        .expect("npm must build the TypeScript SDK");
    assert!(build.success(), "npm run build failed");
    let packed = Command::new(npm)
        .args(["pack", "--ignore-scripts", "--pack-destination"])
        .arg(output)
        .current_dir(&sdk)
        .output()
        .expect("npm must pack the TypeScript SDK");
    assert!(
        packed.status.success(),
        "npm pack failed: {}",
        String::from_utf8_lossy(&packed.stderr)
    );
    let filename = String::from_utf8(packed.stdout)
        .unwrap()
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .expect("npm pack prints the tarball filename")
        .trim()
        .to_string();
    output.join(filename)
}

async fn exercise_real_sdk(source: Option<&Path>, test_name: &str) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = init_project(&fixture, &server, test_name);
    let mut read = fixture.command();
    read.current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("npm_config_cache", fixture.cache().join("npm-cache"));
    if let Some(source) = source {
        read.env("HELIX_TEST_TS_SDK_TARBALL", source)
            .env("npm_config_offline", "true");
    }
    read.assert().success();

    let mut write = fixture.command();
    write
        .current_dir(&project)
        .args([
            "query",
            "dev",
            "-e",
            r#"writeBatch().varAs("created", g().addN("CliUser", {name: "Ada"})).returning(["created"])"#,
        ])
        .env("npm_config_cache", fixture.cache().join("npm-cache"));
    if let Some(source) = source {
        write
            .env("HELIX_TEST_TS_SDK_TARBALL", source)
            .env("npm_config_offline", "true");
    }
    write.assert().success();

    let requests = server.received_requests().await.unwrap();
    let envelopes = requests
        .iter()
        .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(envelopes[0]["request_type"], "read");
    assert!(envelopes[0]["query"]["read"].is_object());
    assert_eq!(envelopes[1]["request_type"], "write");
    assert!(envelopes[1]["query"]["write"].is_object());

    let installed: serde_json::Value = serde_json::from_slice(
        &fs::read(
            fixture
                .cache()
                .join("ts-runtime/node_modules/@helix-db/helix-db/package.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(installed["version"], SDK_VERSION);
    assert_no_runtime_debris(fixture.cache());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_tarball_runtime_is_offline_and_executes_read_and_write() {
    let packed = tempfile::tempdir().unwrap();
    let tarball = build_checkout_tarball(packed.path());
    exercise_real_sdk(Some(&tarball), "typescript-local-tarball-project").await;
}

/// This is deliberately selected only by the scheduled/manual registry-smoke
/// workflow. It becomes the release-availability gate after npm 3.1.0 is
/// published; normal tests use the checkout tarball and never contact npm.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the separately published @helix-db/helix-db@3.1.0 registry release"]
async fn registry_smoke_executes_exact_sdk_read_and_write() {
    exercise_real_sdk(None, "typescript-registry-smoke-project").await;
}
