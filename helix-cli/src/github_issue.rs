//! GitHub issue creation helper for reporting cargo check failures.

use eyre::Result;
use regex::Regex;
use std::collections::HashSet;

const GITHUB_ISSUE_URL: &str = "https://github.com/helixdb/helix-db/issues/new";
const MAX_URL_LENGTH: usize = 8000;
const CONTEXT_LINES: usize = 5;

/// Builder for creating GitHub issue URLs with diagnostic information.
pub struct GitHubIssueBuilder {
    cargo_errors: String,
    hx_content: Option<String>,
    generated_rust: Option<String>,
    error_line_refs: Vec<usize>,
    first_error: Option<String>,
}

impl GitHubIssueBuilder {
    /// Create a new issue builder with cargo error output.
    pub fn new(cargo_errors: String) -> Self {
        let error_line_refs = parse_error_line_numbers(&cargo_errors);
        let first_error = extract_first_error(&cargo_errors);
        Self {
            cargo_errors,
            hx_content: None,
            generated_rust: None,
            error_line_refs,
            first_error,
        }
    }

    /// Add the full .hx file contents.
    pub fn with_hx_content(mut self, content: String) -> Self {
        self.hx_content = Some(content);
        self
    }

    /// Add the generated Rust code.
    pub fn with_generated_rust(mut self, rust_code: String) -> Self {
        self.generated_rust = Some(rust_code);
        self
    }

    /// Build the GitHub issue URL with query parameters.
    pub fn build_url(&self) -> String {
        let title = match &self.first_error {
            Some(error) => format!("bug (hql): rust generation failure - {}", error),
            None => "bug (hql): rust generation failure".to_string(),
        };
        let body = self.build_body();

        // URL encode the parameters
        let encoded_title = urlencoding::encode(&title);
        let encoded_body = urlencoding::encode(&body);
        let encoded_labels = urlencoding::encode("bug,cli");
        let encoded_type = urlencoding::encode("Bug");

        let url = format!(
            "{}?type={}&title={}&body={}&labels={}",
            GITHUB_ISSUE_URL, encoded_type, encoded_title, encoded_body, encoded_labels
        );

        // Truncate if too long
        if url.len() > MAX_URL_LENGTH {
            let truncated_body = self.build_truncated_body();
            let encoded_body = urlencoding::encode(&truncated_body);
            format!(
                "{}?type={}&title={}&body={}&labels={}",
                GITHUB_ISSUE_URL, encoded_type, encoded_title, encoded_body, encoded_labels
            )
        } else {
            url
        }
    }

    /// Open the issue URL in the default browser.
    pub fn open_in_browser(&self) -> Result<()> {
        let url = self.build_url();
        open::that(&url).map_err(|e| eyre::eyre!("Failed to open browser: {}", e))
    }

    /// Build the full issue body.
    fn build_body(&self) -> String {
        let mut body = String::new();

        // Environment section
        body.push_str("## Environment\n");
        body.push_str(&format!(
            "- Helix CLI version: {}\n",
            env!("CARGO_PKG_VERSION")
        ));
        body.push_str(&format!("- OS: {}\n\n", std::env::consts::OS));

        // Error output section
        body.push_str("## Error Output\n");
        body.push_str("```\n");
        body.push_str(&self.cargo_errors);
        body.push_str("\n```\n\n");

        // Schema/Queries section
        if let Some(hx_content) = &self.hx_content {
            body.push_str("## Schema/Queries (.hx files)\n");
            body.push_str("```helix\n");
            body.push_str(hx_content);
            body.push_str("\n```\n\n");
        }

        // Relevant Generated Rust Code section
        if let Some(rust_code) = &self.generated_rust {
            let relevant_rust = self.extract_relevant_rust_lines(rust_code);
            if !relevant_rust.is_empty() {
                body.push_str("## Relevant Generated Rust Code\n");
                body.push_str("<details>\n<summary>Click to expand</summary>\n\n");
                body.push_str("```rust\n");
                body.push_str(&relevant_rust);
                body.push_str("\n```\n</details>\n");
            }
        }

        body
    }

    /// Build a truncated body for when the URL would be too long.
    fn build_truncated_body(&self) -> String {
        let mut body = String::new();

        // Environment section (always include)
        body.push_str("## Environment\n");
        body.push_str(&format!(
            "- Helix CLI version: {}\n",
            env!("CARGO_PKG_VERSION")
        ));
        body.push_str(&format!("- OS: {}\n\n", std::env::consts::OS));

        // Error output section (truncated)
        body.push_str("## Error Output\n");
        body.push_str("```\n");
        let truncated_errors: String = self.cargo_errors.chars().take(2000).collect();
        body.push_str(&truncated_errors);
        if self.cargo_errors.len() > 2000 {
            body.push_str("\n... [truncated]");
        }
        body.push_str("\n```\n\n");

        // Schema/Queries section (truncated)
        if let Some(hx_content) = &self.hx_content {
            body.push_str("## Schema/Queries (.hx files)\n");
            body.push_str("```helix\n");
            let truncated_hx: String = hx_content.chars().take(3000).collect();
            body.push_str(&truncated_hx);
            if hx_content.len() > 3000 {
                body.push_str("\n... [truncated - please add full content manually]");
            }
            body.push_str("\n```\n\n");
        }

        body.push_str("\n_Note: Content was truncated due to URL length limits. Please add additional details if needed._\n");

        body
    }

    /// Extract only the Rust lines referenced in error messages, plus context.
    fn extract_relevant_rust_lines(&self, rust_code: &str) -> String {
        if self.error_line_refs.is_empty() {
            // If no line references found, return first 100 lines
            return rust_code
                .lines()
                .take(100)
                .enumerate()
                .map(|(i, line)| format!("{:4} | {}", i + 1, line))
                .collect::<Vec<_>>()
                .join("\n");
        }

        let lines: Vec<&str> = rust_code.lines().collect();
        let total_lines = lines.len();

        // Collect all line numbers we want to include (with context)
        let mut included_lines: HashSet<usize> = HashSet::new();
        for &error_line in &self.error_line_refs {
            let start = error_line.saturating_sub(CONTEXT_LINES);
            let end = (error_line + CONTEXT_LINES).min(total_lines);
            for line_num in start..=end {
                if line_num > 0 && line_num <= total_lines {
                    included_lines.insert(line_num);
                }
            }
        }

        // Sort and output with line numbers
        let mut sorted_lines: Vec<usize> = included_lines.into_iter().collect();
        sorted_lines.sort();

        let mut result = String::new();
        let mut last_line: Option<usize> = None;

        for line_num in sorted_lines {
            // Add separator if there's a gap
            if let Some(last) = last_line
                && line_num > last + 1
            {
                result.push_str("     ...\n");
            }

            // Line numbers are 1-indexed, array is 0-indexed
            if let Some(line_content) = lines.get(line_num - 1) {
                let marker = if self.error_line_refs.contains(&line_num) {
                    ">>>"
                } else {
                    "   "
                };
                result.push_str(&format!("{} {:4} | {}\n", marker, line_num, line_content));
            }

            last_line = Some(line_num);
        }

        result
    }
}

/// Parse cargo error output to extract line numbers from queries.rs errors.
fn parse_error_line_numbers(cargo_output: &str) -> Vec<usize> {
    // Match patterns like:
    // --> src/queries.rs:42:5
    // --> src/queries.rs:123:10
    let re = Regex::new(r"-->\s+[^:]+/queries\.rs:(\d+):\d+").unwrap();

    let mut line_numbers: Vec<usize> = re
        .captures_iter(cargo_output)
        .filter_map(|cap| cap.get(1).and_then(|m| m.as_str().parse().ok()))
        .collect();

    line_numbers.sort();
    line_numbers.dedup();
    line_numbers
}

/// Extract the first error code and message from cargo output.
/// Returns something like "error[E0308]: mismatched types"
fn extract_first_error(cargo_output: &str) -> Option<String> {
    // Match patterns like:
    // error[E0308]: mismatched types
    // error[E0425]: cannot find value `foo` in this scope
    let re = Regex::new(r"(error\[E\d+\]: [^\n]+)").unwrap();

    re.captures(cargo_output)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str().to_string())
}

/// Filter cargo output to include only errors (not warnings).
/// Preserves full error context including code snippets and line numbers.
pub fn filter_errors_only(cargo_output: &str) -> String {
    let mut result = String::new();
    let mut in_error_block = false;
    let lines: Vec<&str> = cargo_output.lines().collect();

    for (i, line) in lines.iter().enumerate() {
        // Check if this is the start of an error block
        if line.starts_with("error[") || line.starts_with("error:") {
            in_error_block = true;
            result.push_str(line);
            result.push('\n');
        } else if line.starts_with("warning[") || line.starts_with("warning:") {
            // Start of warning block - skip
            in_error_block = false;
        } else if line.trim().starts_with("= note:") && in_error_block {
            // Include notes that are part of error blocks
            result.push_str(line);
            result.push('\n');
        } else if line.trim().starts_with("= help:") && in_error_block {
            // Include help messages that are part of error blocks
            result.push_str(line);
            result.push('\n');
        } else if in_error_block {
            // Check if this line ends the error block
            // Error blocks end at blank lines followed by another error/warning, or at EOF
            let is_blank = line.trim().is_empty();
            let next_starts_new_block = lines
                .get(i + 1)
                .map(|next| {
                    next.starts_with("error[")
                        || next.starts_with("error:")
                        || next.starts_with("warning[")
                        || next.starts_with("warning:")
                        || next.starts_with("For more information")
                })
                .unwrap_or(true);

            if is_blank && next_starts_new_block {
                // End of error block
                in_error_block = false;
                result.push('\n');
            } else {
                // Continue error block
                result.push_str(line);
                result.push('\n');
            }
        }
        // Skip warning blocks entirely
    }

    // If result is empty or only has summary lines, return the full output
    // (better to have too much info than too little)
    let trimmed = result.trim();
    if trimmed.is_empty()
        || (trimmed.starts_with("error: could not compile") && !trimmed.contains("-->"))
    {
        // Filter out just warning lines from full output
        return cargo_output
            .lines()
            .filter(|line| !line.starts_with("warning"))
            .filter(|line| !line.trim().starts_with("= note: `#[warn"))
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string();
    }

    result.trim().to_string()
}

/// Simple URL encoding for the issue URL.
mod urlencoding {
    pub fn encode(input: &str) -> String {
        let mut encoded = String::new();
        for byte in input.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    encoded.push(byte as char);
                }
                b' ' => encoded.push_str("%20"),
                _ => {
                    encoded.push_str(&format!("%{:02X}", byte));
                }
            }
        }
        encoded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error_line_numbers() {
        let cargo_output = r#"
error[E0433]: failed to resolve: use of undeclared type `Foo`
 --> src/queries.rs:42:5
  |
42 |     let x: Foo = Foo::new();
  |            ^^^ not found in this scope

error[E0425]: cannot find value `bar` in this scope
 --> src/queries.rs:100:10
   |
100 |     bar.do_something();
   |     ^^^ not found in this scope
"#;

        let line_numbers = parse_error_line_numbers(cargo_output);
        assert_eq!(line_numbers, vec![42, 100]);
    }

    #[test]
    fn test_filter_errors_only() {
        let cargo_output = r#"warning: unused variable: `x`
 --> src/queries.rs:10:5
  |
10 |     let x = 5;
  |         ^ help: if this is intentional, prefix it with an underscore: `_x`

error[E0433]: failed to resolve: use of undeclared type `Foo`
 --> src/queries.rs:42:5
  |
42 |     let x: Foo = Foo::new();
  |            ^^^ not found in this scope

warning: unused import
 --> src/queries.rs:1:5

error: aborting due to 1 previous error
"#;

        let errors_only = filter_errors_only(cargo_output);
        assert!(errors_only.contains("error[E0433]"));
        assert!(errors_only.contains("--> src/queries.rs:42:5"));
        assert!(errors_only.contains("Foo::new()"));
        assert!(!errors_only.contains("warning: unused variable"));
        assert!(!errors_only.contains("warning: unused import"));
    }

    #[test]
    fn test_filter_errors_preserves_context() {
        let cargo_output = r#"error[E0425]: cannot find value `undefined_var` in this scope
  --> src/queries.rs:100:5
   |
100 |     undefined_var.do_something();
   |     ^^^^^^^^^^^^^ not found in this scope

For more information about this error, try `rustc --explain E0425`.
error: could not compile `helix-container` due to 1 previous error
"#;

        let errors_only = filter_errors_only(cargo_output);
        assert!(errors_only.contains("error[E0425]"));
        assert!(errors_only.contains("--> src/queries.rs:100:5"));
        assert!(errors_only.contains("undefined_var"));
        assert!(errors_only.contains("not found in this scope"));
    }

    #[test]
    fn test_extract_first_error() {
        let cargo_output = r#"error[E0308]: mismatched types
   --> helix-container/src/queries.rs:192:43
    |
192 | .insert_v::<fn(&HVector, &RoTxn) -> bool>(&data.vec, "File8Vec", Some(...
    |  ---------------------------------------- ^^^^^^^^^ expected `&[f64]`, found `&Vec<f32>`

error[E0308]: mismatched types
   --> helix-container/src/queries.rs:194:43

error: could not compile `helix-container` due to 2 previous errors
"#;

        let first_error = extract_first_error(cargo_output);
        assert_eq!(
            first_error,
            Some("error[E0308]: mismatched types".to_string())
        );
    }

    #[test]
    fn test_extract_first_error_none() {
        let cargo_output = "error: could not compile `helix-container`";
        let first_error = extract_first_error(cargo_output);
        assert_eq!(first_error, None);
    }
}
