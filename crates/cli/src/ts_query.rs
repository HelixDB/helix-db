//! Translate raw TypeScript DSL snippets into the query JSON body that
//! `POST /v2/query` expects, so `helix query -e '<ts>'` works like `mysql -e`.
//!
//! The snippet is treated as a single expression that evaluates to a Helix
//! `readBatch()` / `writeBatch()` builder. We evaluate it in Node with the
//! published `@helix-db/helix-db` SDK in scope, call `.toQueryJson()` on the
//! result, and capture the JSON on stdout. The SDK is zero-dependency and its
//! builders are pure (no I/O), so this needs no running instance — just Node and
//! a one-time `npm install` cached under the Helix cache dir.

use crate::errors::CliError;
use crate::external_tools::{self, ExternalTool};
use crate::output::Step;
use crate::project::get_helix_cache_dir;
use eyre::{Report, Result};
use serde_json::{json, Value};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The TS SDK that exposes `g`, `readBatch`, `writeBatch`, `defineParams`, `param`
/// and `toQueryJson()`. Keep this exact version synchronized with the SDK's
/// `package.json`; exact pins prevent a warm cache and a cold install from
/// evaluating different query-envelope implementations.
const SDK_PACKAGE: &str = "@helix-db/helix-db";
const SDK_VERSION: &str = "3.1.0";
const VERSION_MARKER: &str = ".sdk-version";
const INSTALL_LOCK: &str = ".ts-runtime-install-lock";
const INSTALL_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const STALE_INSTALL_LOCK_AGE: Duration = Duration::from_secs(120);
const TEST_SDK_TARBALL_ENV: &str = "HELIX_TEST_TS_SDK_TARBALL";

/// Build a query request body from a raw TS DSL snippet (inline `-e` or a
/// `--ts-file`). Returns the parsed JSON envelope, ready for the normal send path.
pub fn build_request_from_ts(snippet: &str) -> Result<Value> {
    let snippet = snippet.trim();
    if snippet.is_empty() {
        return Err(CliError::new("the TypeScript query is empty")
            .with_hint("pass an expression, e.g. -e 'readBatch().varAs(\"c\", g().nWithLabel(\"User\").count()).returning([\"c\"])'")
            .into());
    }

    ensure_node()?;
    let runtime_dir = ensure_sdk()?;
    let wrapper = WrapperFile::new(write_wrapper(&runtime_dir, snippet)?);
    let json = run_node(&runtime_dir, wrapper.path())?;

    serde_json::from_str(&json).map_err(|e| {
        CliError::new("the TypeScript query did not produce valid JSON")
            .with_caused_by(e.to_string())
            .with_context(truncate(&json, 2000))
            .into()
    })
}

/// Confirm Node (and, for installs, npm) are on PATH; otherwise return a friendly
/// error pointing back at the JSON entry points.
fn ensure_node() -> Result<()> {
    if !external_tools::available(ExternalTool::Node) {
        return Err(
            CliError::new("Node.js is required to run TypeScript queries")
                .with_hint(
                    "install Node.js 20+ to use -e/--ts/--ts-file, or pass JSON with --json/--file",
                )
                .into(),
        );
    }
    Ok(())
}

/// Ensure the pinned SDK is installed in a cached runtime dir, returning that dir.
/// Installs only when missing or version-mismatched, so repeat queries are fast.
fn ensure_sdk() -> Result<PathBuf> {
    let cache_dir = get_helix_cache_dir()?;
    fs::create_dir_all(&cache_dir)?;
    ensure_sdk_in(&cache_dir, install_source()?)
}

#[derive(Debug)]
enum InstallSource {
    Registry,
    LocalTarball(PathBuf),
}

impl InstallSource {
    fn dependency_spec(&self) -> String {
        match self {
            Self::Registry => SDK_VERSION.to_string(),
            Self::LocalTarball(path) => {
                format!("file:{}", dunce::simplified(path).display())
            }
        }
    }
}

fn install_source() -> Result<InstallSource> {
    let Some(path) = std::env::var_os(TEST_SDK_TARBALL_ENV).map(PathBuf::from) else {
        return Ok(InstallSource::Registry);
    };
    let path = path.canonicalize().map_err(|error| {
        CliError::new("the local TypeScript SDK tarball is unavailable")
            .with_context(path.display().to_string())
            .with_caused_by(error.to_string())
    })?;
    if !path.is_file() {
        return Err(
            CliError::new("the local TypeScript SDK tarball is not a file")
                .with_context(path.display().to_string())
                .into(),
        );
    }
    Ok(InstallSource::LocalTarball(path))
}

fn ensure_sdk_in(cache_dir: &Path, source: InstallSource) -> Result<PathBuf> {
    let runtime_dir = cache_dir.join("ts-runtime");
    if sdk_ready(&runtime_dir) {
        return Ok(runtime_dir);
    }

    let _lock = InstallLock::acquire(cache_dir)?;
    recover_interrupted_install(cache_dir, &runtime_dir)?;
    if sdk_ready(&runtime_dir) {
        return Ok(runtime_dir);
    }
    if !external_tools::available(ExternalTool::Npm) {
        return Err(CliError::new(format!(
            "npm is required to install the TypeScript query runtime ({SDK_PACKAGE}@{SDK_VERSION})"
        ))
        .with_hint("install Node.js 20+ (which bundles npm), or pass JSON with --json/--file")
        .into());
    }

    let prepared_dir = cache_dir.join(unique_runtime_path("ts-runtime-prepare"));
    let prepared = PreparedRuntime::new(prepared_dir)?;
    let package_json = json!({
        "name": "helix-ts-runtime",
        "private": true,
        "type": "module",
        "dependencies": {(SDK_PACKAGE): source.dependency_spec()},
    });
    fs::write(
        prepared.path().join("package.json"),
        serde_json::to_vec(&package_json)?,
    )?;

    let mut step = Step::with_messages(
        "Preparing TypeScript query runtime",
        "TypeScript query runtime ready",
    );
    step.start();
    let output = external_tools::command(ExternalTool::Npm)
        .args(["install", "--no-audit", "--no-fund", "--ignore-scripts"])
        .current_dir(prepared.path())
        .output();

    match output {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            step.fail();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let details = match (stdout.trim(), stderr.trim()) {
                ("", "") => format!("npm exited with {}", output.status),
                ("", stderr) => stderr.to_owned(),
                (stdout, "") => stdout.to_owned(),
                (stdout, stderr) => format!("stdout:\n{stdout}\nstderr:\n{stderr}"),
            };
            return Err(CliError::new(format!(
                "failed to install the TypeScript query runtime ({SDK_PACKAGE}@{SDK_VERSION})"
            ))
            .with_caused_by(truncate(&details, 2000))
            .with_hint("check your network connection and that npm works")
            .into());
        }
        Err(error) => {
            step.fail();
            return Err(Report::from(error).wrap_err("failed to run npm install"));
        }
    }

    if let Err(error) = verify_installed_sdk(prepared.path()) {
        step.fail();
        return Err(CliError::new(format!(
            "installed TypeScript query runtime is unusable ({SDK_PACKAGE}@{SDK_VERSION})"
        ))
        .with_caused_by(error.to_string())
        .into());
    }
    if let Err(error) = fs::write(prepared.path().join(VERSION_MARKER), SDK_VERSION) {
        step.fail();
        return Err(error.into());
    }
    if let Err(error) = promote_runtime(prepared, &runtime_dir) {
        step.fail();
        return Err(error);
    }
    step.done();
    Ok(runtime_dir)
}

/// A warm runtime must have the exact package version and a loadable SDK
/// module. Missing, partial, corrupt, or stale caches all reinstall.
fn sdk_ready(runtime_dir: &Path) -> bool {
    fs::read_to_string(runtime_dir.join(VERSION_MARKER))
        .is_ok_and(|version| version.trim() == SDK_VERSION)
        && verify_installed_sdk(runtime_dir).is_ok()
}

fn verify_installed_sdk(runtime_dir: &Path) -> Result<()> {
    let package_json_path = runtime_dir
        .join("node_modules")
        .join("@helix-db")
        .join("helix-db")
        .join("package.json");
    let package: Value = serde_json::from_slice(&fs::read(&package_json_path)?)
        .map_err(|error| Report::from(error).wrap_err("invalid installed SDK package.json"))?;
    if package["version"] != SDK_VERSION {
        return Err(eyre::eyre!(
            "installed SDK version is {}, expected {SDK_VERSION}",
            package["version"]
        ));
    }

    let verification = format!(
        r#"import {{ g, readBatch, writeBatch }} from "{SDK_PACKAGE}";
if (typeof g !== "function" || typeof readBatch !== "function" || typeof writeBatch !== "function") {{
  throw new Error("required query builders are unavailable");
}}"#
    );
    let output = external_tools::command(ExternalTool::Node)
        .args(["--input-type=module", "--eval", &verification])
        .current_dir(runtime_dir)
        .output()
        .map_err(|error| {
            Report::from(error).wrap_err("failed to verify the installed SDK import")
        })?;
    if !output.status.success() {
        return Err(eyre::eyre!(
            "SDK import failed: {}",
            truncate(String::from_utf8_lossy(&output.stderr).trim(), 2000)
        ));
    }
    Ok(())
}

fn promote_runtime(prepared: PreparedRuntime, runtime_dir: &Path) -> Result<()> {
    let prepared_dir = prepared.into_path();
    let backup_dir = runtime_dir.with_file_name(unique_runtime_path("ts-runtime-backup"));
    let had_runtime = runtime_dir.exists();
    if had_runtime {
        fs::rename(runtime_dir, &backup_dir)?;
    }
    if let Err(error) = fs::rename(&prepared_dir, runtime_dir) {
        if had_runtime {
            let _ = fs::rename(&backup_dir, runtime_dir);
        }
        let _ = fs::remove_dir_all(&prepared_dir);
        return Err(Report::from(error).wrap_err("failed to promote TypeScript query runtime"));
    }
    if had_runtime {
        let _ = fs::remove_dir_all(backup_dir);
    }
    Ok(())
}

fn recover_interrupted_install(cache_dir: &Path, runtime_dir: &Path) -> Result<()> {
    let mut backups = Vec::new();
    let mut prepared = Vec::new();
    for entry in fs::read_dir(cache_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("ts-runtime-backup-") {
            backups.push(entry.path());
        } else if name.starts_with("ts-runtime-prepare-") {
            prepared.push(entry.path());
        }
    }
    backups.sort();
    if !runtime_dir.exists()
        && let Some(backup) = backups.pop()
    {
        fs::rename(backup, runtime_dir)?;
    }
    for path in backups.into_iter().chain(prepared) {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn unique_runtime_path(prefix: &str) -> String {
    format!("{prefix}-{}-{}", std::process::id(), timestamp_nanos())
}

fn timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

struct PreparedRuntime {
    path: Option<PathBuf>,
}

impl PreparedRuntime {
    fn new(path: PathBuf) -> Result<Self> {
        fs::create_dir(&path)?;
        Ok(Self { path: Some(path) })
    }

    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("prepared runtime owns its path")
    }

    fn into_path(mut self) -> PathBuf {
        self.path.take().expect("prepared runtime owns its path")
    }
}

impl Drop for PreparedRuntime {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

struct InstallLock {
    path: PathBuf,
}

impl InstallLock {
    fn acquire(cache_dir: &Path) -> Result<Self> {
        let path = cache_dir.join(INSTALL_LOCK);
        let started = Instant::now();
        loop {
            match fs::create_dir(&path) {
                Ok(()) => {
                    let _ = fs::write(
                        path.join("owner"),
                        format!(
                            "pid={}\ncreated={}\n",
                            std::process::id(),
                            timestamp_nanos()
                        ),
                    );
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    let stale = fs::metadata(&path)
                        .and_then(|metadata| metadata.modified())
                        .and_then(|modified| modified.elapsed().map_err(std::io::Error::other))
                        .is_ok_and(|age| age >= STALE_INSTALL_LOCK_AGE);
                    if stale {
                        let _ = fs::remove_dir_all(&path);
                        continue;
                    }
                    if started.elapsed() >= INSTALL_WAIT_TIMEOUT {
                        return Err(CliError::new(
                            "timed out waiting for another TypeScript runtime installation",
                        )
                        .with_context(path.display().to_string())
                        .into());
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl Drop for InstallLock {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct WrapperFile {
    path: PathBuf,
}

impl WrapperFile {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for WrapperFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Write the Node wrapper that injects the common DSL imports, evaluates the
/// snippet as an expression, and prints `toQueryJson()` to stdout.
fn write_wrapper(runtime_dir: &Path, snippet: &str) -> Result<PathBuf> {
    // Files conventionally end expressions with a semicolon. The snippet is
    // embedded inside parentheses, where that terminator is invalid syntax.
    let snippet = snippet.trim_end().trim_end_matches(';').trim_end();
    let wrapper = format!(
        r#"import {{ g, readBatch, writeBatch, defineParams, param }} from "{SDK_PACKAGE}";

const __query = (
{snippet}
);

if (__query == null || typeof __query.toQueryJson !== "function") {{
  console.error("The TypeScript query must evaluate to a readBatch()/writeBatch() builder.");
  console.error("Example: readBatch().varAs(\"c\", g().nWithLabel(\"User\").count()).returning([\"c\"])");
  process.exit(1);
}}

process.stdout.write(__query.toQueryJson());
"#
    );
    let wrapper_path = runtime_dir.join(unique_wrapper_file());
    fs::write(&wrapper_path, wrapper)?;
    Ok(wrapper_path)
}

fn unique_wrapper_file() -> String {
    format!(
        "__helix_query_{}_{}.mjs",
        std::process::id(),
        timestamp_nanos()
    )
}

/// Run the wrapper with Node from the runtime dir (so `node_modules` resolves) and
/// return its stdout, surfacing SDK/build errors from stderr.
fn run_node(runtime_dir: &Path, wrapper_path: &Path) -> Result<String> {
    let output = external_tools::command(ExternalTool::Node)
        .arg(wrapper_path)
        .current_dir(runtime_dir)
        .output()
        .map_err(|e| Report::from(e).wrap_err("failed to run node"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CliError::new("the TypeScript query failed to evaluate")
            .with_caused_by(truncate(stderr.trim(), 2000))
            .with_hint(
                "the snippet must be a single expression returning a builder; \
                 remove TypeScript type annotations for inline -e use",
            )
            .into());
    }

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let end = s
            .char_indices()
            .take_while(|(i, c)| i + c.len_utf8() <= max)
            .map(|(i, c)| i + c.len_utf8())
            .last()
            .unwrap_or(0);
        format!("{}…", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_snippet() {
        let error = build_request_from_ts("   ").unwrap_err().to_string();
        assert!(error.contains("empty"));
    }

    #[test]
    fn wrapper_injects_imports_and_snippet() {
        let dir = std::env::temp_dir().join(format!("helix-tsq-wrapper-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let snippet = r#"readBatch().varAs("c", g().nWithLabel("User").count()).returning(["c"])"#;
        let path = write_wrapper(&dir, snippet).unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();

        assert!(contents.contains(&format!("from \"{SDK_PACKAGE}\"")));
        assert!(contents.contains("readBatch, writeBatch"));
        assert!(contents.contains(snippet));
        assert!(contents.contains("toQueryJson()"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wrapper_accepts_a_trailing_expression_semicolon() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_wrapper(dir.path(), "readBatch();\n").unwrap();
        let contents = std::fs::read_to_string(path).unwrap();

        assert!(contents.contains("\nreadBatch()\n);"));
        assert!(!contents.contains("\nreadBatch();\n);"));
    }

    #[test]
    fn cli_sdk_pin_matches_the_checkout_package() {
        let package: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../sdks/typescript/package.json"
        )))
        .unwrap();
        assert_eq!(package["name"], SDK_PACKAGE);
        assert_eq!(package["version"], SDK_VERSION);
    }

    #[cfg(windows)]
    #[test]
    fn local_tarball_dependency_uses_a_non_verbatim_windows_path() {
        let source = InstallSource::LocalTarball(PathBuf::from(r"\\?\C:\temp\helix-sdk.tgz"));

        assert_eq!(source.dependency_spec(), r"file:C:\temp\helix-sdk.tgz");
    }

    #[test]
    fn interrupted_install_restores_backup_and_removes_partial_state() {
        let cache = tempfile::tempdir().unwrap();
        let runtime = cache.path().join("ts-runtime");
        let backup = cache.path().join("ts-runtime-backup-old");
        let partial = cache.path().join("ts-runtime-prepare-old");
        std::fs::create_dir(&backup).unwrap();
        std::fs::write(backup.join("sentinel"), "previous").unwrap();
        std::fs::create_dir(&partial).unwrap();
        std::fs::write(partial.join("partial"), "incomplete").unwrap();

        recover_interrupted_install(cache.path(), &runtime).unwrap();

        assert_eq!(
            std::fs::read_to_string(runtime.join("sentinel")).unwrap(),
            "previous"
        );
        assert!(!backup.exists());
        assert!(!partial.exists());
    }

    #[test]
    fn install_lock_has_single_owner_and_cleans_up_on_drop() {
        let cache = tempfile::tempdir().unwrap();
        let lock_path = cache.path().join(INSTALL_LOCK);
        {
            let _lock = InstallLock::acquire(cache.path()).unwrap();
            assert!(lock_path.is_dir());
            assert!(lock_path.join("owner").is_file());
        }
        assert!(!lock_path.exists());
    }

    #[test]
    fn wrapper_guard_cleans_up_on_every_exit_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wrapper.mjs");
        std::fs::write(&path, "invalid output").unwrap();
        {
            let wrapper = WrapperFile::new(path.clone());
            assert_eq!(wrapper.path(), path);
        }
        assert!(!path.exists());
    }

    #[test]
    fn truncate_is_char_boundary_safe() {
        let s = "é".repeat(10); // 2 bytes per char
        let out = truncate(&s, 5);
        assert!(out.ends_with('…'));
        // Must not panic and must stay under the byte budget (+ ellipsis).
        assert!(out.len() <= 5 + '…'.len_utf8());
        assert_eq!(truncate("hé_", 4), "hé_");
        assert_eq!(truncate("hé_x", 4), "hé_…");
    }
}
