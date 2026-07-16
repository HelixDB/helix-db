//! Translate raw TypeScript DSL snippets into the query JSON body that
//! `POST /v1/query` expects, so `helix query -e '<ts>'` works like `mysql -e`.
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
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// The TS SDK that exposes `g`, `readBatch`, `writeBatch`, `defineParams`, `param`
/// and `toQueryJson()`. `SDK_SPEC` is an npm semver range so we pick up the
/// latest compatible publish; bump it when the query envelope changes.
const SDK_PACKAGE: &str = "@helix-db/helix-db";
const SDK_SPEC: &str = "^3.0.0";
const SPEC_MARKER: &str = ".sdk-spec";

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
    let wrapper_path = write_wrapper(&runtime_dir, snippet)?;
    let json = match run_node(&runtime_dir, &wrapper_path) {
        Ok(json) => json,
        Err(err) => {
            let _ = fs::remove_file(&wrapper_path);
            return Err(err);
        }
    };
    let _ = fs::remove_file(&wrapper_path);

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
    let runtime_dir = get_helix_cache_dir()?.join("ts-runtime");
    fs::create_dir_all(&runtime_dir)?;

    if sdk_ready(&runtime_dir) {
        return Ok(runtime_dir);
    }

    if !external_tools::available(ExternalTool::Npm) {
        return Err(CliError::new(format!(
            "npm is required to install the TypeScript query runtime ({SDK_PACKAGE}@{SDK_SPEC})"
        ))
        .with_hint("install Node.js 20+ (which bundles npm), or pass JSON with --json/--file")
        .into());
    }

    let package_json = format!(
        r#"{{"name":"helix-ts-runtime","private":true,"type":"module","dependencies":{{"{SDK_PACKAGE}":"{SDK_SPEC}"}}}}"#
    );
    fs::write(runtime_dir.join("package.json"), package_json)?;

    let mut step = Step::with_messages(
        "Preparing TypeScript query runtime",
        "TypeScript query runtime ready",
    );
    step.start();
    let output = external_tools::command(ExternalTool::Npm)
        .args(["install", "--silent", "--no-audit", "--no-fund"])
        .current_dir(&runtime_dir)
        .output();

    match output {
        Ok(out) if out.status.success() => {
            // Record the satisfied spec so repeat runs skip install until it changes.
            let _ = fs::write(runtime_dir.join(SPEC_MARKER), SDK_SPEC);
            step.done();
            Ok(runtime_dir)
        }
        Ok(out) => {
            step.fail();
            Err(CliError::new(format!(
                "failed to install the TypeScript query runtime ({SDK_PACKAGE}@{SDK_SPEC})"
            ))
            .with_caused_by(truncate(&String::from_utf8_lossy(&out.stderr), 2000))
            .with_hint("check your network connection and that npm works")
            .into())
        }
        Err(e) => {
            step.fail();
            Err(Report::from(e).wrap_err("failed to run npm install"))
        }
    }
}

/// True when the SDK is installed for the current spec, so we can skip `npm install`.
/// We reinstall when the package is missing or `SDK_SPEC` was bumped since last time.
fn sdk_ready(runtime_dir: &Path) -> bool {
    let installed = runtime_dir
        .join("node_modules")
        .join("@helix-db")
        .join("helix-db")
        .join("package.json")
        .exists();
    let spec_ok = fs::read_to_string(runtime_dir.join(SPEC_MARKER))
        .map(|spec| spec.trim() == SDK_SPEC)
        .unwrap_or(false);
    installed && spec_ok
}

/// Write the Node wrapper that injects the common DSL imports, evaluates the
/// snippet as an expression, and prints `toQueryJson()` to stdout.
fn write_wrapper(runtime_dir: &Path, snippet: &str) -> Result<PathBuf> {
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
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("__helix_query_{}_{}.mjs", std::process::id(), nanos)
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
    fn truncate_is_char_boundary_safe() {
        let s = "é".repeat(10); // 2 bytes per char
        let out = truncate(&s, 5);
        assert!(out.ends_with('…'));
        // Must not panic and must stay under the byte budget (+ ellipsis).
        assert!(out.len() <= 5 + '…'.len_utf8());
        assert_eq!(truncate("hé_", 4), "hé_");
        assert_eq!(truncate("hé_x", 4), "hé_…");
    }

    /// End-to-end: requires Node + npm + network (real `npm install`). Run with
    /// `cargo test -p helix-cli -- --ignored ts_query`.
    #[test]
    #[ignore]
    fn builds_read_envelope_from_ts() {
        let request = build_request_from_ts(
            r#"readBatch().varAs("c", g().nWithLabel("User").count()).returning(["c"])"#,
        )
        .expect("snippet should evaluate");

        assert_eq!(request["request_type"], "read");
        assert!(request["query"]["queries"].is_array());
        assert_eq!(request["query"]["returns"][0], "c");
    }
}
