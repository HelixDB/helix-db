//! Build-time compilation utilities for HelixDB.
//!
//! This module provides functions to compile .hx schema and query files
//! into Rust code during the build process.
//!

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use helix_db::helixc::{
    analyzer::analyze,
    generator::generate,
    parser::{
        HelixParser,
        types::{Content, HxFile, Source},
    },
};

pub use crate::errors::{HelixError, Result};

/// Compiles .hx files from the default queries directory to src/queries.rs
///
/// This is the recommended function for most use cases. It:
/// - Looks for .hx files in `./queries/` directory
/// - Outputs to `./src/queries.rs`
/// - Tells cargo to rerun if queries/ changes
///

pub fn compile_queries_default() -> Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map_err(|_| HelixError::StorageError("CARGO_MANIFEST_DIR not set".to_string()))?;

    let queries_dir = PathBuf::from(&manifest_dir).join("queries");
    let output_dir = PathBuf::from(&manifest_dir).join("src");

    compile_queries(&queries_dir, &output_dir)?;

    // Tell cargo to rerun if any .hx file changes
    println!("cargo:rerun-if-changed=queries/");

    Ok(())
}

/// Compiles .hx files from a custom directory
///
/// Use this if your .hx files are not in the default `./queries/` directory.
///
/// # Arguments
/// * `queries_dir` - Directory containing .hx schema and query files
/// * `output_dir` - Directory where queries.rs will be created
///
/// # Example
///
/// ```no_run
/// use std::path::PathBuf;
///
/// // build.rs
/// fn main() {
///     let queries = PathBuf::from("./my_schemas");
///     let output = PathBuf::from("./src");
///     
///     helix_lib::build::compile_queries(&queries, &output)
///         .expect("Failed to compile Helix queries");
///     
///     println!("cargo:rerun-if-changed=my_schemas/");
/// }
/// ```
pub fn compile_queries(queries_dir: &Path, output_dir: &Path) -> Result<()> {
    // 1. Collect all .hx files
    let hx_files = collect_hx_files(queries_dir)?;

    if hx_files.is_empty() {
        // No .hx files found - create default queries.rs with no schema
        create_default_queries_rs(output_dir)?;
        return Ok(());
    }

    // 2. Read files into HxFile structs
    let hx_files_content: Vec<HxFile> = hx_files
        .iter()
        .filter_map(|path| {
            let name = path.to_string_lossy().into_owned();
            fs::read_to_string(path)
                .ok()
                .map(|content| HxFile { name, content })
        })
        .collect();

    if hx_files_content.is_empty() {
        return Err(HelixError::StorageError(
            "Failed to read any .hx files".to_string(),
        ));
    }

    // 3. Create Content for parser
    let content_str = hx_files_content
        .iter()
        .map(|f| f.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let content = Content {
        content: content_str,
        files: hx_files_content.clone(),
        source: Source::default(),
    };

    // 4. Parse using HelixParser
    let source = HelixParser::parse_source(&content)
        .map_err(|e| HelixError::StorageError(format!("Failed to parse .hx files: {}", e)))?;

    // 5. Analyze the parsed source
    let (diagnostics, generated_source) = analyze(&source)
        .map_err(|e| HelixError::StorageError(format!("Failed to analyze .hx files: {}", e)))?;

    // 6. Check for compilation errors
    if !diagnostics.is_empty() {
        let error_messages: Vec<String> = diagnostics.iter().map(|d| format!("{:?}", d)).collect();
        return Err(HelixError::StorageError(format!(
            "Helix compilation errors:\n{}",
            error_messages.join("\n")
        )));
    }

    // 7. Generate queries.rs
    fs::create_dir_all(output_dir)?;
    generate(generated_source, output_dir)
        .map_err(|e| HelixError::StorageError(format!("Failed to generate queries.rs: {}", e)))?;

    Ok(())
}

/// Recursively collect all .hx files from a directory
fn collect_hx_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut hx_files = Vec::new();

    if !dir.exists() {
        // No queries directory - that's ok, we'll create default queries.rs
        return Ok(hx_files);
    }

    if !dir.is_dir() {
        return Err(HelixError::InvalidPath(format!(
            "{} is not a directory",
            dir.display()
        )));
    }

    collect_recursive(dir, &mut hx_files)?;
    Ok(hx_files)
}

fn collect_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        // Skip .helix directory
        if path.file_name().and_then(|n| n.to_str()) == Some(".helix") {
            continue;
        }

        if path.is_file() {
            if path.extension().and_then(|s| s.to_str()) == Some("hx") {
                files.push(path);
            }
        } else if path.is_dir() {
            collect_recursive(&path, files)?;
        }
    }
    Ok(())
}

/// Create a default queries.rs with no schema
fn create_default_queries_rs(output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    let default_content = r#"// Generated by helix-lib build script
// No .hx files found, using default configuration

use helix_db::helix_engine::traversal_core::config::Config;

pub fn config() -> Option<Config> {
    None
}
"#;

    let output_file = output_dir.join("queries.rs");
    let mut file = fs::File::create(output_file)?;
    file.write_all(default_content.as_bytes())?;

    Ok(())
}
