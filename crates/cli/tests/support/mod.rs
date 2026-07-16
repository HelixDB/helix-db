use assert_cmd::Command;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use tempfile::{Builder, TempDir};

pub struct CliFixture {
    root: PathBuf,
    _tempdir: Option<TempDir>,
    home: PathBuf,
    helix_home: PathBuf,
    cache: PathBuf,
    test_runtime_bin: Option<PathBuf>,
    test_tool_dir: Option<PathBuf>,
    http_base_url: Option<String>,
    host_action_log: PathBuf,
    tool_log: PathBuf,
    runtime_log: PathBuf,
}

impl CliFixture {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_inner(false)
    }

    #[allow(dead_code)]
    pub fn new_with_fake_runtime() -> Self {
        Self::new_inner(true)
    }

    fn new_inner(fake_runtime: bool) -> Self {
        let root = Builder::new()
            .prefix("helix-cli-e2e-")
            .tempdir()
            .expect("create e2e tempdir");
        let root_path = root.path().to_path_buf();
        let home = root_path.join("home");
        let helix_home = root_path.join("helix-home");
        let cache = root_path.join("helix-cache");
        fs::create_dir_all(&home).expect("create isolated home");
        fs::create_dir_all(&helix_home).expect("create isolated helix home");
        fs::create_dir_all(&cache).expect("create isolated cache");
        fs::write(
            helix_home.join("metrics.toml"),
            "level = \"off\"\nlast_updated = 0\ninstall_event_sent = true\n",
        )
        .expect("disable metrics for isolated CLI tests");
        let test_runtime_bin = fake_runtime.then(|| install_fake_docker(&root_path.join("bin")));
        let host_action_log = root_path.join("host-actions.jsonl");
        let tool_log = root_path.join("tools.log");
        let runtime_log = root_path.join("runtime.log");

        let tempdir = if std::env::var_os("HELIX_E2E_KEEP_TMP").is_some() {
            std::mem::forget(root);
            None
        } else {
            Some(root)
        };

        Self {
            root: root_path,
            _tempdir: tempdir,
            home,
            helix_home,
            cache,
            test_runtime_bin,
            test_tool_dir: None,
            http_base_url: None,
            host_action_log,
            tool_log,
            runtime_log,
        }
    }

    #[allow(dead_code)]
    pub fn with_http_base(mut self, base_url: impl Into<String>) -> Self {
        self.http_base_url = Some(base_url.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_fake_tools(mut self) -> Self {
        let directory = self.root.join("tools");
        install_fake_tools(&directory);
        self.test_tool_dir = Some(directory);
        self
    }

    #[allow(dead_code)]
    pub fn write_credentials(&self, user_id: &str, api_key: &str) {
        let directory = self.home.join(".helix");
        fs::create_dir_all(&directory).expect("create credentials directory");
        fs::write(
            directory.join("credentials"),
            format!("helix_user_id={user_id}\nhelix_user_key={api_key}"),
        )
        .expect("write credentials");
    }

    #[allow(dead_code)]
    pub fn host_actions(&self) -> String {
        fs::read_to_string(&self.host_action_log).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub fn tool_log(&self) -> String {
        fs::read_to_string(&self.tool_log).unwrap_or_default()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    #[allow(dead_code)]
    pub fn home(&self) -> &Path {
        &self.home
    }

    #[allow(dead_code)]
    pub fn helix_home(&self) -> &Path {
        &self.helix_home
    }

    #[allow(dead_code)]
    pub fn cache(&self) -> &Path {
        &self.cache
    }

    #[allow(dead_code)]
    pub fn runtime_log(&self) -> String {
        fs::read_to_string(&self.runtime_log).unwrap_or_default()
    }

    pub fn command(&self) -> Command {
        let mut command = Command::cargo_bin("helix").expect("helix binary should be built");
        command
            .env("HELIX_NO_UPDATE_CHECK", "1")
            .env("HELIX_DISABLE_UPDATE_CHECK", "1")
            .env("NO_COLOR", "1")
            .env("HELIX_HOME", &self.helix_home)
            .env("HELIX_CACHE_DIR", &self.cache)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("PATHEXT", ".COM;.EXE;.BAT;.CMD")
            .env("CLICOLOR", "0")
            .env("HELIX_TEST_HOST_ACTION_LOG", &self.host_action_log)
            .env("HELIX_TEST_TOOL_LOG", &self.tool_log)
            .env("HELIX_TEST_RUNTIME_LOG", &self.runtime_log);
        if let Some(test_runtime_bin) = &self.test_runtime_bin {
            command.env("HELIX_TEST_CONTAINER_RUNTIME_BIN", test_runtime_bin);
        }
        if let Some(test_tool_dir) = &self.test_tool_dir {
            command.env("HELIX_TEST_TOOL_DIR", test_tool_dir);
        }
        if let Some(http_base_url) = &self.http_base_url {
            command.env("HELIX_TEST_HTTP_BASE_URL", http_base_url);
        }
        command
    }
}

#[allow(dead_code)]
fn install_fake_tools(directory: &Path) {
    fs::create_dir_all(directory).expect("create fake tool directory");
    for tool in [
        "cargo", "node", "npm", "npx", "curl", "claude", "codex", "opencode",
    ] {
        install_fake_tool(directory, tool);
    }
}

#[cfg(windows)]
#[allow(dead_code)]
fn install_fake_tool(directory: &Path, tool: &str) {
    let script = directory.join(format!("{tool}.cmd"));
    fs::write(
        script,
        format!(
            r#"@echo off
if defined HELIX_TEST_TOOL_LOG echo {tool} %*>>"%HELIX_TEST_TOOL_LOG%"
if "%1"=="%HELIX_TEST_TOOL_FAIL_COMMAND%" (
  if defined HELIX_TEST_TOOL_STDERR echo %HELIX_TEST_TOOL_STDERR% 1>&2
  exit /b 42
)
if defined HELIX_TEST_TOOL_STDOUT echo %HELIX_TEST_TOOL_STDOUT%
if defined HELIX_TEST_TOOL_STDERR echo %HELIX_TEST_TOOL_STDERR% 1>&2
if defined HELIX_TEST_TOOL_EXIT_CODE exit /b %HELIX_TEST_TOOL_EXIT_CODE%
exit /b 0
"#
        ),
    )
    .expect("write fake Windows tool");
}

#[cfg(not(windows))]
#[allow(dead_code)]
fn install_fake_tool(directory: &Path, tool: &str) {
    use std::os::unix::fs::PermissionsExt;

    let script = directory.join(tool);
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
if [ -n "$HELIX_TEST_TOOL_LOG" ]; then
  printf '%s %s\n' '{tool}' "$*" >> "$HELIX_TEST_TOOL_LOG"
fi
if [ "$1" = "$HELIX_TEST_TOOL_FAIL_COMMAND" ]; then
  printf '%s\n' "${{HELIX_TEST_TOOL_STDERR:-simulated tool failure}}" >&2
  exit 42
fi
if [ -n "$HELIX_TEST_TOOL_STDOUT" ]; then
  printf '%s\n' "$HELIX_TEST_TOOL_STDOUT"
fi
if [ -n "$HELIX_TEST_TOOL_STDERR" ]; then
  printf '%s\n' "$HELIX_TEST_TOOL_STDERR" >&2
fi
exit "${{HELIX_TEST_TOOL_EXIT_CODE:-0}}"
"#
        ),
    )
    .expect("write fake Unix tool");
    let mut permissions = fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(script, permissions).unwrap();
}

fn install_fake_docker(bin: &Path) -> PathBuf {
    fs::create_dir_all(bin).expect("create fake docker bin");

    #[cfg(windows)]
    {
        let script = bin.join("docker.cmd");
        fs::write(
            &script,
            r#"@echo off
if defined HELIX_TEST_RUNTIME_LOG echo %*>>"%HELIX_TEST_RUNTIME_LOG%"
if /I "%1"=="%HELIX_TEST_RUNTIME_FAIL_COMMAND%" (
  echo simulated runtime failure 1>&2
  exit /b 42
)
if "%1"=="info" exit /b 0
if "%1"=="ps" (
  if defined HELIX_TEST_RUNTIME_PS_OUTPUT echo %HELIX_TEST_RUNTIME_PS_OUTPUT%
  exit /b 0
)
if "%1"=="logs" (
  echo fake logs
  exit /b 0
)
if "%1"=="rm" (
  if "%HELIX_TEST_RUNTIME_RESOURCES_EXIST%"=="1" exit /b 0
  echo No such container 1>&2
  exit /b 1
)
if "%1"=="network" (
  if "%2"=="create" exit /b 0
  if "%HELIX_TEST_RUNTIME_RESOURCES_EXIST%"=="1" exit /b 0
  echo not found 1>&2
  exit /b 1
)
if "%1"=="volume" (
  if "%2"=="create" exit /b 0
  if "%HELIX_TEST_RUNTIME_RESOURCES_EXIST%"=="1" exit /b 0
  echo not found 1>&2
  exit /b 1
)
exit /b 0
"#,
        )
        .expect("write fake docker cmd");
        script
    }

    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;

        let script = bin.join("docker");
        fs::write(
            &script,
            r#"#!/bin/sh
if [ -n "$HELIX_TEST_RUNTIME_LOG" ]; then
  printf '%s\n' "$*" >> "$HELIX_TEST_RUNTIME_LOG"
fi
if [ "$1" = "$HELIX_TEST_RUNTIME_FAIL_COMMAND" ]; then
  echo "simulated runtime failure" >&2
  exit 42
fi
case "$1" in
  info) exit 0 ;;
  ps)
    if [ -n "$HELIX_TEST_RUNTIME_PS_OUTPUT" ]; then
      printf '%s\n' "$HELIX_TEST_RUNTIME_PS_OUTPUT"
    fi
    exit 0
    ;;
  logs) echo "fake logs"; exit 0 ;;
  rm)
    if [ "$HELIX_TEST_RUNTIME_RESOURCES_EXIST" = "1" ]; then exit 0; fi
    echo "No such container" >&2
    exit 1
    ;;
  network|volume)
    if [ "$2" = "create" ] || [ "$HELIX_TEST_RUNTIME_RESOURCES_EXIST" = "1" ]; then
      exit 0
    fi
    echo "not found" >&2
    exit 1
    ;;
  *) exit 0 ;;
esac
"#,
        )
        .expect("write fake docker script");
        let mut permissions = fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).unwrap();
        script
    }
}

/// Pick a port the OS currently considers free.
///
/// There is an unavoidable TOCTOU window here: we release the listener before
/// the caller can bind, and the caller hands the port to a Docker container in
/// a child process, so we cannot keep the `fd` alive and pass it through (the
/// usual `from_std` / `SO_REUSEPORT` mitigations don't cross the container
/// boundary). This is acceptable for these e2e tests because they are
/// `#[ignore]`d behind Docker and are not run concurrently in CI; collisions
/// would surface as a loud `helix start` failure rather than silent corruption.
#[allow(dead_code)]
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free TCP port");
    let port = listener.local_addr().expect("read local addr").port();
    drop(listener);
    port
}
