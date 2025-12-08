use crate::errors::CliError;
use color_eyre::owo_colors::OwoColorize;
use eyre::{Result, eyre};
use helix_db::helixc::parser::types::HxFile;
use std::io::IsTerminal;
use std::{borrow::Cow, fs, path::Path};
use tokio::sync::oneshot;
use tokio::time::Duration;

const IGNORES: [&str; 3] = ["target", ".git", ".helix"];

/// Resolve the source text to use when rendering diagnostics.
pub fn diagnostic_source<'a>(
    filepath: &str,
    files: &'a [HxFile],
    fallback: &'a str,
) -> Cow<'a, str> {
    if let Some(src) = files.iter().find(|file| file.name == filepath) {
        Cow::Borrowed(src.content.as_str())
    } else if let Ok(contents) = fs::read_to_string(filepath) {
        Cow::Owned(contents)
    } else {
        Cow::Borrowed(fallback)
    }
}

/// Copy a directory recursively without any exclusions
pub fn copy_dir_recursively(src: &Path, dst: &Path) -> Result<()> {
    if !src.is_dir() {
        return Err(eyre::eyre!("Source is not a directory: {}", src.display()));
    }

    // Create destination directory
    fs::create_dir_all(dst)?;

    // Read the source directory
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursively(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }

    Ok(())
}

/// Copy a directory recursively
pub fn copy_dir_recursive_excluding(src: &Path, dst: &Path) -> Result<()> {
    if !src.is_dir() {
        return Err(eyre::eyre!("Source is not a directory: {}", src.display()));
    }

    // Create destination directory
    fs::create_dir_all(dst)?;

    // Read the source directory
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if IGNORES.contains(
            &entry
                .file_name()
                .into_string()
                .map_err(|s| eyre!("cannot convert file name to string: {s:?}"))?
                .as_str(),
        ) {
            continue;
        }

        if src_path.is_dir() {
            copy_dir_recursive_excluding(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }

    Ok(())
}

/// Check if a command exists in PATH
#[allow(unused)]
pub fn command_exists(command: &str) -> bool {
    std::process::Command::new("which")
        .arg(command)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Print a status message with a prefix
pub fn print_status(prefix: &str, message: &str) {
    println!("{} {message}", format!("[{prefix}]").blue().bold());
}

/// Print an info message with consistent formatting
pub fn print_info(message: &str) {
    println!("{} {message}", "[INFO]".cyan().bold());
}

/// Print a formatted message with custom color
#[allow(unused)]
pub fn print_message(prefix: &str, message: &str) {
    println!("{} {message}", format!("[{prefix}]").white().bold());
}

/// Print a plain message (replaces direct println! usage)
pub fn print_line(message: &str) {
    println!("{message}");
}

/// Print an empty line
pub fn print_newline() {
    println!();
}

/// Print multiple lines with consistent indentation
pub fn print_lines(lines: &[&str]) {
    for line in lines {
        println!("  {line}");
    }
}

/// Print next steps or instructions
pub fn print_instructions(title: &str, steps: &[&str]) {
    print_newline();
    println!("{}", title.bold());
    for (i, step) in steps.iter().enumerate() {
        println!("  {}. {step}", (i + 1).to_string().bright_white().bold());
    }
}

/// Print a section header
pub fn print_header(title: &str) {
    println!("{}", title.bold().underline());
}

/// Print formatted key-value pairs
pub fn print_field(key: &str, value: &str) {
    println!("  {}: {value}", key.bright_white().bold());
}

/// Print an error message
pub fn print_error(message: &str) {
    let error = CliError::new(message);
    eprint!("{}", error.render());
}

/// Print an error with context
#[allow(unused)]
pub fn print_error_with_context(message: &str, context: &str) {
    let error = CliError::new(message).with_context(context);
    eprint!("{}", error.render());
}

/// Print an error with hint
pub fn print_error_with_hint(message: &str, hint: &str) {
    let error = CliError::new(message).with_hint(hint);
    eprint!("{}", error.render());
}

/// Print a success message
pub fn print_success(message: &str) {
    println!("{} {message}", "[SUCCESS]".green().bold());
}

/// Print a completion message with summary
#[allow(unused)]
pub fn print_completion(operation: &str, details: &str) {
    println!(
        "{} {} completed successfully",
        "[SUCCESS]".green().bold(),
        operation
    );
    if !details.is_empty() {
        println!("  {details}");
    }
}

/// Print a warning message
pub fn print_warning(message: &str) {
    let warning = CliError::warning(message);
    eprint!("{}", warning.render());
}

/// Print a warning with hint
#[allow(unused)]
pub fn print_warning_with_hint(message: &str, hint: &str) {
    let warning = CliError::warning(message).with_hint(hint);
    eprint!("{}", warning.render());
}

/// Print a formatted CLI error
#[allow(unused)]
pub fn print_cli_error(error: &CliError) {
    eprint!("{}", error.render());
}

/// Print a confirmation prompt and read user input
pub fn print_prompt(message: &str) -> std::io::Result<String> {
    use std::io::{self, Write};
    print!("{} ", message.yellow().bold());
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

/// Print a yes/no confirmation prompt
pub fn print_confirm(message: &str) -> std::io::Result<bool> {
    let response = print_prompt(&format!("{message} (y/N):"))?;
    Ok(response.to_lowercase() == "y" || response.to_lowercase() == "yes")
}

/// Add or update an environment variable in a .env file
pub fn add_env_var_to_file(file_path: &Path, key: &str, value: &str) -> std::io::Result<()> {
    add_env_var_with_comment(file_path, key, value, None)
}

/// Add or update an environment variable in a .env file with an optional comment
pub fn add_env_var_with_comment(
    file_path: &Path,
    key: &str,
    value: &str,
    comment: Option<&str>,
) -> std::io::Result<()> {
    let mut content = if file_path.exists() {
        fs::read_to_string(file_path)?
    } else {
        String::new()
    };

    let key_prefix = format!("{}=", key);
    if content.lines().any(|line| line.starts_with(&key_prefix)) {
        // Replace existing key (preserve any existing comment above it)
        content = content
            .lines()
            .map(|line| {
                if line.starts_with(&key_prefix) {
                    format!("{}={}", key, value)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        if !content.ends_with('\n') {
            content.push('\n');
        }
    } else {
        // Append new key with optional comment
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        if let Some(cmt) = comment {
            content.push_str(&format!("{}\n", cmt));
        }
        content.push_str(&format!("{}={}\n", key, value));
    }

    fs::write(file_path, content)
}

#[derive(Default)]
#[allow(unused)]
pub enum Template {
    Typescript,
    Python,
    Rust,
    Go,
    #[default]
    Empty,
}
impl Template {
    #[allow(unused)]
    pub fn from(value: &str) -> Result<Self> {
        let template = match value {
            "ts" | "typescript" => Template::Typescript,
            "py" | "python" => Template::Python,
            "rs" | "rust" => Template::Rust,
            "go" => Template::Go,
            _ => return Err(eyre::eyre!("Invalid template: {value}")),
        };
        Ok(template)
    }
}

pub mod helixc_utils {
    use eyre::Result;
    use helix_db::helixc::{
        analyzer::analyze,
        generator::{Source as GeneratedSource, generate},
        parser::{
            HelixParser,
            types::{Content, HxFile, Source},
        },
    };
    use std::{fs, path::Path};

    /// Collect all .hx files from queries directory and subdirectories
    pub fn collect_hx_files(root: &Path, queries_dir: &Path) -> Result<Vec<std::fs::DirEntry>> {
        let mut files = Vec::new();
        let queries_path = root.join(queries_dir);

        fn collect_from_dir(dir: &Path, files: &mut Vec<std::fs::DirEntry>) -> Result<()> {
            if dir.file_name().unwrap_or_default() == ".helix" {
                return Ok(());
            }
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().map(|s| s == "hx").unwrap_or(false) {
                    files.push(entry);
                } else if path.is_dir() {
                    collect_from_dir(&path, files)?;
                }
            }
            Ok(())
        }

        collect_from_dir(&queries_path, &mut files)?;

        if files.is_empty() {
            return Err(eyre::eyre!(
                "No .hx files found in {}",
                queries_path.display()
            ));
        }

        Ok(files)
    }

    /// Generate content from .hx files (similar to build.rs)
    pub fn generate_content(files: &[std::fs::DirEntry]) -> Result<Content> {
        let hx_files: Vec<HxFile> = files
            .iter()
            .map(|file| {
                let name = file.path().to_string_lossy().into_owned();
                let content = fs::read_to_string(file.path())
                    .map_err(|e| eyre::eyre!("Failed to read file {name}: {e}"))?;
                Ok(HxFile { name, content })
            })
            .collect::<Result<Vec<_>>>()?;

        let content_str = hx_files
            .iter()
            .map(|file| file.content.clone())
            .collect::<Vec<String>>()
            .join("\n");

        Ok(Content {
            content: content_str,
            files: hx_files,
            source: Source::default(),
        })
    }

    /// Parse content (similar to build.rs)
    pub fn parse_content(content: &Content) -> Result<Source> {
        let source =
            HelixParser::parse_source(content).map_err(|e| eyre::eyre!("Parse error: {}", e))?;
        Ok(source)
    }

    /// Analyze source for validation (similar to build.rs)
    pub fn analyze_source(source: Source, files: &[HxFile]) -> Result<GeneratedSource> {
        let (diagnostics, generated_source) =
            analyze(&source).map_err(|e| eyre::eyre!("Analysis error: {}", e))?;

        if !diagnostics.is_empty() {
            // Format diagnostics properly using the helix-db pretty printer
            let formatted_diagnostics =
                format_diagnostics(&diagnostics, &generated_source.src, files);
            return Err(eyre::eyre!(
                "Compilation failed with {} error(s):\n\n{}",
                diagnostics.len(),
                formatted_diagnostics
            ));
        }

        Ok(generated_source)
    }

    /// Format diagnostics using the helix-db diagnostic renderer
    fn format_diagnostics(
        diagnostics: &[helix_db::helixc::analyzer::diagnostic::Diagnostic],
        src: &str,
        files: &[HxFile],
    ) -> String {
        let mut output = String::new();
        for diagnostic in diagnostics {
            // Use the render method with empty source for now
            let filepath = diagnostic
                .filepath
                .clone()
                .unwrap_or("queries.hx".to_string());

            let snippet_src = super::diagnostic_source(&filepath, files, src);
            output.push_str(&diagnostic.render(snippet_src.as_ref(), &filepath));
            output.push('\n');
        }
        output
    }

    pub fn generate_rust_code(source: GeneratedSource, path: &Path) -> Result<()> {
        generate(source, path)?;
        Ok(())
    }

    /// Collect all .hx file contents as a single string with file path headers.
    /// Used for GitHub issue reporting.
    /// Filters out files that only contain comments (no actual schema or query definitions).
    pub fn collect_hx_contents(root: &Path, queries_dir: &Path) -> Result<String> {
        let files = collect_hx_files(root, queries_dir)?;
        let mut combined = String::new();

        for file in files {
            let path = file.path();
            let content = fs::read_to_string(&path)
                .map_err(|e| eyre::eyre!("Failed to read file {}: {e}", path.display()))?;

            // Skip files that only contain comments
            if !has_actual_content(&content) {
                continue;
            }

            // Get relative path for cleaner display
            let relative_path = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();

            combined.push_str(&format!("// File: {}\n", relative_path));
            combined.push_str(&content);
            combined.push_str("\n\n");
        }

        Ok(combined.trim().to_string())
    }

    /// Check if a .hx file has actual content (not just comments and whitespace).
    /// Returns true if the file contains any non-comment, non-whitespace content.
    fn has_actual_content(content: &str) -> bool {
        for line in content.lines() {
            let trimmed = line.trim();
            // Skip empty lines and comment lines
            if trimmed.is_empty() || trimmed.starts_with("//") {
                continue;
            }
            // Found actual content
            return true;
        }
        false
    }
}

pub struct Spinner {
    message: std::sync::Arc<std::sync::Mutex<String>>,
    prefix: String,
    stop_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Spinner {
    pub fn new(prefix: &str, message: &str) -> Self {
        Self {
            message: std::sync::Arc::new(std::sync::Mutex::new(message.to_string())),
            prefix: prefix.to_string(),
            stop_tx: None,
            handle: None,
        }
    }
    // function that starts the spinner
    pub fn start(&mut self) {
        if !std::io::stdout().is_terminal() {
            return; // skip animation for non-interactive terminals
        }
        let message = self.message.clone();
        let prefix = self.prefix.clone();
        let (tx, mut rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            let frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
            let mut frame_idx = 0;
            loop {
                if rx.try_recv().is_ok() {
                    break;
                }
                let frame = frames[frame_idx % frames.len()];
                let msg = message.lock().unwrap().clone();
                print!("\r{} {frame} {msg}", format!("[{prefix}]").blue().bold());
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
                frame_idx += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        self.handle = Some(handle);
        self.stop_tx = Some(tx);
    }
    // function that Stops the spinner
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        // Clear the line completely
        print!("\r\x1b[K");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
    }
    /// function that updates the message
    pub fn update(&mut self, message: &str) {
        if let Ok(mut msg) = self.message.lock() {
            *msg = message.to_string();
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_add_env_var_creates_new_file() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        add_env_var_to_file(&env_path, "HELIX_API_KEY", "test-key-123").unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(content, "HELIX_API_KEY=test-key-123\n");
    }

    #[test]
    fn test_add_env_var_appends_to_existing_file() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create existing .env file
        fs::write(&env_path, "EXISTING_VAR=value\n").unwrap();

        add_env_var_to_file(&env_path, "HELIX_API_KEY", "test-key-123").unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(content, "EXISTING_VAR=value\nHELIX_API_KEY=test-key-123\n");
    }

    #[test]
    fn test_add_env_var_appends_newline_if_missing() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create existing .env file without trailing newline
        fs::write(&env_path, "EXISTING_VAR=value").unwrap();

        add_env_var_to_file(&env_path, "HELIX_API_KEY", "test-key-123").unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(content, "EXISTING_VAR=value\nHELIX_API_KEY=test-key-123\n");
    }

    #[test]
    fn test_add_env_var_updates_existing_key() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create existing .env file with the key already present
        fs::write(&env_path, "OTHER_VAR=foo\nHELIX_API_KEY=old-key\nANOTHER_VAR=bar\n").unwrap();

        add_env_var_to_file(&env_path, "HELIX_API_KEY", "new-key-456").unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(content, "OTHER_VAR=foo\nHELIX_API_KEY=new-key-456\nANOTHER_VAR=bar\n");
    }

    #[test]
    fn test_add_env_var_handles_empty_file() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create empty .env file
        fs::write(&env_path, "").unwrap();

        add_env_var_to_file(&env_path, "HELIX_API_KEY", "test-key-123").unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(content, "HELIX_API_KEY=test-key-123\n");
    }

    #[test]
    fn test_add_env_var_preserves_other_variables() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create .env with multiple variables
        fs::write(&env_path, "VAR1=value1\nVAR2=value2\nVAR3=value3\n").unwrap();

        add_env_var_to_file(&env_path, "HELIX_API_KEY", "my-key").unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert!(content.contains("VAR1=value1"));
        assert!(content.contains("VAR2=value2"));
        assert!(content.contains("VAR3=value3"));
        assert!(content.contains("HELIX_API_KEY=my-key"));
    }

    #[test]
    fn test_add_env_var_with_comment_creates_file_with_comment() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        add_env_var_with_comment(
            &env_path,
            "HELIX_CLOUD_URL",
            "https://example.com",
            Some("# HelixDB Cloud URL for instance: test"),
        )
        .unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(
            content,
            "# HelixDB Cloud URL for instance: test\nHELIX_CLOUD_URL=https://example.com\n"
        );
    }

    #[test]
    fn test_add_env_var_with_comment_appends_with_comment() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create existing .env file
        fs::write(&env_path, "EXISTING_VAR=value\n").unwrap();

        add_env_var_with_comment(
            &env_path,
            "HELIX_CLOUD_URL",
            "https://example.com",
            Some("# HelixDB Cloud URL for instance: test"),
        )
        .unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(
            content,
            "EXISTING_VAR=value\n# HelixDB Cloud URL for instance: test\nHELIX_CLOUD_URL=https://example.com\n"
        );
    }

    #[test]
    fn test_add_env_var_with_comment_updates_without_duplicate_comment() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        // Create existing .env file with key and comment
        fs::write(
            &env_path,
            "# HelixDB Cloud URL for instance: old\nHELIX_CLOUD_URL=https://old.com\n",
        )
        .unwrap();

        add_env_var_with_comment(
            &env_path,
            "HELIX_CLOUD_URL",
            "https://new.com",
            Some("# HelixDB Cloud URL for instance: new"),
        )
        .unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        // Should update value but preserve existing comment (not add duplicate)
        assert_eq!(
            content,
            "# HelixDB Cloud URL for instance: old\nHELIX_CLOUD_URL=https://new.com\n"
        );
    }

    #[test]
    fn test_add_env_var_with_no_comment() {
        let dir = tempdir().unwrap();
        let env_path = dir.path().join(".env");

        add_env_var_with_comment(&env_path, "HELIX_API_KEY", "test-key", None).unwrap();

        let content = fs::read_to_string(&env_path).unwrap();
        assert_eq!(content, "HELIX_API_KEY=test-key\n");
    }
}
