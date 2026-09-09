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
    skills_state: PathBuf,
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
        let skills_state = root_path.join("agent-state");
        fs::create_dir_all(&home).expect("create isolated home");
        fs::create_dir_all(&helix_home).expect("create isolated helix home");
        fs::create_dir_all(&cache).expect("create isolated cache");
        fs::create_dir_all(&skills_state).expect("create isolated agent state");
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
            skills_state,
            test_runtime_bin,
            test_tool_dir: None,
            http_base_url: None,
            host_action_log,
            tool_log,
            runtime_log,
        }
    }

    /// Points the runtime seam at a path that does not exist, so every runtime
    /// command fails to spawn exactly as it does on a host without Docker.
    #[allow(dead_code)]
    pub fn with_missing_runtime(mut self) -> Self {
        self.test_runtime_bin = Some(self.root.join("bin").join("missing-runtime"));
        self
    }

    /// Points the runtime seam at a file that exists but cannot be executed, so
    /// spawning fails for a reason other than the runtime being absent.
    #[cfg(unix)]
    #[allow(dead_code)]
    pub fn with_unspawnable_runtime(mut self) -> Self {
        use std::os::unix::fs::PermissionsExt;

        let directory = self.root.join("bin");
        fs::create_dir_all(&directory).expect("create runtime fixture directory");
        let binary = directory.join("unspawnable-runtime");
        fs::write(&binary, "not an executable\n").expect("write runtime fixture");
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o644))
            .expect("drop execute permission on runtime fixture");
        self.test_runtime_bin = Some(binary);
        self
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
    pub fn with_fake_tools_named(mut self, tools: &[&str]) -> Self {
        let directory = self.root.join("tools");
        fs::create_dir_all(&directory).expect("create fake tool directory");
        for tool in tools {
            install_fake_tool(&directory, tool);
        }
        self.test_tool_dir = Some(directory);
        self
    }

    #[allow(dead_code)]
    pub fn write_credentials(&self, email: &str, access_token: &str) {
        let directory = &self.helix_home;
        fs::create_dir_all(directory).expect("create credentials directory");
        fs::write(
            directory.join("credentials"),
            serde_json::to_vec(&serde_json::json!({
                "access_token": access_token,
                "refresh_token": "refresh-token",
                "expires_at": i64::MAX,
                "email": email,
            }))
            .unwrap(),
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

    #[allow(dead_code)]
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
    pub fn skills_lockfile(&self) -> PathBuf {
        self.skills_state.join("skills").join(".skill-lock.json")
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
            .env("XDG_STATE_HOME", &self.skills_state)
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
    let log_command = if tool == "node" {
        r#"if "%~1"=="--input-type" if defined HELIX_TEST_TOOL_LOG echo node --input-type=module>>"%HELIX_TEST_TOOL_LOG%"
if not "%~1"=="--input-type" if defined HELIX_TEST_TOOL_LOG echo node %*>>"%HELIX_TEST_TOOL_LOG%""#
            .to_owned()
    } else {
        format!(r#"if defined HELIX_TEST_TOOL_LOG echo {tool} %*>>"%HELIX_TEST_TOOL_LOG%""#)
    };
    fs::write(
        script,
        format!(
            r#"@echo off
{log_command}
set "HELIX_TEST_TOOL_FIRST_ARGUMENT=%~1"
if "{tool}"=="node" if "%~1"=="--input-type" set "HELIX_TEST_TOOL_FIRST_ARGUMENT=--input-type=module"
if defined HELIX_TEST_TOOL_FAIL_COMMAND if "%HELIX_TEST_TOOL_FIRST_ARGUMENT%"=="%HELIX_TEST_TOOL_FAIL_COMMAND%" (
  if defined HELIX_TEST_TOOL_STDERR echo %HELIX_TEST_TOOL_STDERR% 1>&2
  exit /b 42
)
if "{tool}"=="npm" if "%1"=="install" (
  mkdir "node_modules\@helix-db\helix-db\dist" 2>nul
  >"node_modules\@helix-db\helix-db\package.json" echo {{"name":"@helix-db/helix-db","version":"3.0.4","type":"module","exports":"./dist/index.js"}}
  >"node_modules\@helix-db\helix-db\dist\index.js" echo export const g = ^(^) =^> ({{}}^); export const readBatch = ^(^) =^> ({{}}^); export const writeBatch = ^(^) =^> ({{}}^);
)
if "{tool}"=="node" if not "%~1"=="--input-type" if defined HELIX_TEST_TOOL_FAIL_NODE_QUERY (
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
if [ '{tool}' = 'npm' ] && [ "$1" = 'install' ]; then
  mkdir -p 'node_modules/@helix-db/helix-db/dist'
  printf '%s\n' '{{"name":"@helix-db/helix-db","version":"3.0.4","type":"module","exports":"./dist/index.js"}}' > 'node_modules/@helix-db/helix-db/package.json'
  printf '%s\n' 'export const g = () => ({{}}); export const readBatch = () => ({{}}); export const writeBatch = () => ({{}});' > 'node_modules/@helix-db/helix-db/dist/index.js'
fi
if [ '{tool}' = 'node' ] && [ "$1" != '--input-type=module' ] && [ -n "$HELIX_TEST_TOOL_FAIL_NODE_QUERY" ]; then
  printf '%s\n' "${{HELIX_TEST_TOOL_STDERR:-simulated Node failure}}" >&2
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
if "%1"=="container" (
  if "%2"=="inspect" (
    if "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="missing" (
      echo No such object 1>&2
      exit /b 1
    )
    if not "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="" if not "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="unlabeled" echo %HELIX_TEST_RUNTIME_LABEL_PROBE%
    exit /b 0
  )
  exit /b 0
)
if "%1"=="rm" (
  if "%HELIX_TEST_RUNTIME_RESOURCES_EXIST%"=="1" exit /b 0
  echo No such container 1>&2
  exit /b 1
)
if "%1"=="network" (
  if "%2"=="inspect" if "%3"=="--format" (
    if "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="missing" (
      echo not found 1>&2
      exit /b 1
    )
    if not "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="" if not "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="unlabeled" echo %HELIX_TEST_RUNTIME_LABEL_PROBE%
    exit /b 0
  )
  if "%2"=="create" exit /b 0
  if "%HELIX_TEST_RUNTIME_RESOURCES_EXIST%"=="1" exit /b 0
  echo not found 1>&2
  exit /b 1
)
if "%1"=="volume" (
  if "%2"=="inspect" if "%3"=="--format" (
    if "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="missing" (
      echo not found 1>&2
      exit /b 1
    )
    if not "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="" if not "%HELIX_TEST_RUNTIME_LABEL_PROBE%"=="unlabeled" echo %HELIX_TEST_RUNTIME_LABEL_PROBE%
    exit /b 0
  )
  if "%HELIX_TEST_RUNTIME_VOLUME_MODE%"=="existing" exit /b 0
  if "%HELIX_TEST_RUNTIME_VOLUME_MODE%"=="raced" (
    if "%2"=="create" (
      echo volume already exists 1>&2
      exit /b 1
    )
    echo not found 1>&2
    exit /b 1
  )
  if "%HELIX_TEST_RUNTIME_VOLUME_MODE%"=="denied" (
    if "%2"=="create" (
      echo permission denied 1>&2
      exit /b 1
    )
    echo not found 1>&2
    exit /b 1
  )
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
  container)
    if [ "$2" = "inspect" ]; then
      if [ "$HELIX_TEST_RUNTIME_LABEL_PROBE" = "missing" ]; then
        echo "No such object" >&2
        exit 1
      fi
      case "$HELIX_TEST_RUNTIME_LABEL_PROBE" in
        "" | unlabeled) ;;
        *) printf '%s\n' "$HELIX_TEST_RUNTIME_LABEL_PROBE" ;;
      esac
      exit 0
    fi
    exit 0
    ;;
  rm)
    if [ "$HELIX_TEST_RUNTIME_RESOURCES_EXIST" = "1" ]; then exit 0; fi
    echo "No such container" >&2
    exit 1
    ;;
  network)
    if [ "$2" = "inspect" ] && [ "$3" = "--format" ]; then
      if [ "$HELIX_TEST_RUNTIME_LABEL_PROBE" = "missing" ]; then
        echo "not found" >&2
        exit 1
      fi
      case "$HELIX_TEST_RUNTIME_LABEL_PROBE" in
        "" | unlabeled) ;;
        *) printf '%s\n' "$HELIX_TEST_RUNTIME_LABEL_PROBE" ;;
      esac
      exit 0
    fi
    if [ "$2" = "create" ] || [ "$HELIX_TEST_RUNTIME_RESOURCES_EXIST" = "1" ]; then
      exit 0
    fi
    echo "not found" >&2
    exit 1
    ;;
  volume)
    if [ "$2" = "inspect" ] && [ "$3" = "--format" ]; then
      if [ "$HELIX_TEST_RUNTIME_LABEL_PROBE" = "missing" ]; then
        echo "not found" >&2
        exit 1
      fi
      case "$HELIX_TEST_RUNTIME_LABEL_PROBE" in
        "" | unlabeled) ;;
        *) printf '%s\n' "$HELIX_TEST_RUNTIME_LABEL_PROBE" ;;
      esac
      exit 0
    fi
    case "${HELIX_TEST_RUNTIME_VOLUME_MODE:-fresh}" in
      existing)
        # inspect succeeds, so ensure_volume returns before ever creating
        exit 0
        ;;
      raced)
        # inspect misses, create loses the race to another process
        if [ "$2" = "create" ]; then
          echo "volume already exists" >&2
          exit 1
        fi
        echo "not found" >&2
        exit 1
        ;;
      denied)
        # inspect misses, create fails for an unrelated reason
        if [ "$2" = "create" ]; then
          echo "permission denied" >&2
          exit 1
        fi
        echo "not found" >&2
        exit 1
        ;;
      *)
        # fresh: inspect misses, create succeeds
        if [ "$2" = "create" ] || [ "$HELIX_TEST_RUNTIME_RESOURCES_EXIST" = "1" ]; then
          exit 0
        fi
        echo "not found" >&2
        exit 1
        ;;
    esac
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
