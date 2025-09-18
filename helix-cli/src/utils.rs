use crate::errors::CliError;
use color_eyre::owo_colors::OwoColorize;
use eyre::{Result, eyre};
use std::fs;
use std::path::Path;

/// Copy a directory recursively
pub fn copy_dir_recursive_excluding(src: &Path, dst: &Path, ignores: &[&str]) -> Result<()> {
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

        if ignores.contains(
            &entry
                .file_name()
                .into_string()
                .map_err(|s| eyre!("cannot convert file name to string: {s:?}"))?
                .as_str(),
        ) {
            continue;
        }

        if src_path.is_dir() {
            copy_dir_recursive_excluding(&src_path, &dst_path, ignores)?;
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
    use std::fs;
    use std::path::Path;

    /// Collect all .hx files from project root
    pub fn collect_hx_files(root: &Path) -> Result<Vec<std::fs::DirEntry>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().map(|s| s == "hx").unwrap_or(false) {
                files.push(entry);
            }
        }

        let has_queries = files.iter().any(|file| file.file_name() != "schema.hx");
        if !has_queries {
            return Err(eyre::eyre!("No query files (.hx) found"));
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
    pub fn analyze_source(source: Source) -> Result<GeneratedSource> {
        let (diagnostics, generated_source) = analyze(&source);

        if !diagnostics.is_empty() {
            let error_msg = diagnostics
                .iter()
                .map(|e| format!("{e:?}"))
                .collect::<Vec<_>>()
                .join("\n");
            return Err(eyre::eyre!("Validation failed:\n{error_msg}"));
        }

        Ok(generated_source)
    }

    pub fn generate_rust_code(source: GeneratedSource, path: &Path) -> Result<()> {
        generate(source, path)?;
        Ok(())
    }
}
