//! GitHub issue creation helpers for reporting issues via URL.
//!
//! This module provides shared utilities for creating GitHub issue URLs
//! with proper truncation to stay within URL length limits.

use eyre::Result;
use regex::Regex;
use std::collections::HashSet;

/// The base URL for creating new GitHub issues.
pub const GITHUB_ISSUE_URL: &str = "https://github.com/helixdb/helix-db/issues/new";

/// Maximum URL length to stay within browser limits.
pub const MAX_URL_LENGTH: usize = 8000;

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

        // URL encode the fixed parameters
        let encoded_title = urlencoding::encode(&title);
        let encoded_labels = urlencoding::encode("bug,cli");
        let encoded_type = urlencoding::encode("Bug");

        // Calculate the URL overhead (everything except the body)
        // Format: {base}?type={type}&title={title}&body={body}&labels={labels}
        let url_overhead = GITHUB_ISSUE_URL.len()
            + "?type=&title=&body=&labels=".len()
            + encoded_type.len()
            + encoded_title.len()
            + encoded_labels.len();

        let max_body_encoded_len = MAX_URL_LENGTH.saturating_sub(url_overhead);

        // First try the full body
        let body = self.build_body();
        let body_encoded_len = urlencoding::encoded_len(&body);

        let final_body = if body_encoded_len <= max_body_encoded_len {
            body
        } else {
            // Need to truncate - use adaptive truncation based on available space
            self.build_truncated_body_to_fit(max_body_encoded_len)
        };

        let encoded_body = urlencoding::encode(&final_body);

        format!(
            "{}?type={}&title={}&body={}&labels={}",
            GITHUB_ISSUE_URL, encoded_type, encoded_title, encoded_body, encoded_labels
        )
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

    /// Build a truncated body that fits within the specified encoded length limit.
    /// Uses adaptive truncation to maximize content while staying under the limit.
    fn build_truncated_body_to_fit(&self, max_encoded_len: usize) -> String {
        // Start with aggressive truncation limits and adjust if needed
        // We use conservative estimates: assume ~2x expansion for URL encoding on average
        // (actual expansion varies from 1x for alphanumeric to 3x for special chars)

        // Reserve space for the fixed template parts (markdown headers, code fences, etc.)
        // These are mostly alphanumeric so they encode ~1:1
        let template_overhead = 300; // Conservative estimate for markdown structure
        let available_for_content = max_encoded_len.saturating_sub(template_overhead);

        // Divide available space among sections (errors get priority, then schema)
        // Use ~2.5x divisor to account for URL encoding expansion
        let max_error_chars = (available_for_content / 3).min(1500);
        let max_hx_chars = (available_for_content / 4).min(1000);

        let mut body = String::new();

        // Environment section (always include - small and important)
        body.push_str("## Environment\n");
        body.push_str(&format!(
            "- Helix CLI version: {}\n",
            env!("CARGO_PKG_VERSION")
        ));
        body.push_str(&format!("- OS: {}\n\n", std::env::consts::OS));

        // Error output section (truncated)
        body.push_str("## Error Output\n");
        body.push_str("```\n");
        let truncated_errors: String = self.cargo_errors.chars().take(max_error_chars).collect();
        body.push_str(&truncated_errors);
        if self.cargo_errors.chars().count() > max_error_chars {
            body.push_str("\n... [truncated]");
        }
        body.push_str("\n```\n\n");

        // Check if we still have room for schema content
        let current_encoded_len = urlencoding::encoded_len(&body);
        if current_encoded_len < max_encoded_len {
            let remaining = max_encoded_len.saturating_sub(current_encoded_len);
            let actual_max_hx = (remaining / 3).min(max_hx_chars);

            // Schema/Queries section (truncated)
            if let Some(hx_content) = &self.hx_content
                && actual_max_hx > 100
            {
                // Only include if we have reasonable space
                body.push_str("## Schema/Queries (.hx files)\n");
                body.push_str("```helix\n");
                let truncated_hx: String = hx_content.chars().take(actual_max_hx).collect();
                body.push_str(&truncated_hx);
                if hx_content.chars().count() > actual_max_hx {
                    body.push_str("\n... [truncated]");
                }
                body.push_str("\n```\n\n");
            }
        }

        body.push_str("_Note: Content truncated due to URL length limits. Please add full details manually._\n");

        // Final safety check - if still too long, do emergency truncation
        let final_encoded_len = urlencoding::encoded_len(&body);
        if final_encoded_len > max_encoded_len {
            // Emergency: just return minimal body
            let mut minimal = String::new();
            minimal.push_str("## Environment\n");
            minimal.push_str(&format!(
                "- Helix CLI version: {}\n",
                env!("CARGO_PKG_VERSION")
            ));
            minimal.push_str(&format!("- OS: {}\n\n", std::env::consts::OS));
            minimal.push_str("## Error\n");
            minimal.push_str("Content too large for URL. Please describe the issue manually.\n");
            return minimal;
        }

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

/// Generic builder for creating GitHub issue URLs with any content.
///
/// This is a simpler builder for general-purpose issue creation (like feedback).
/// For compile error issues, use `GitHubIssueBuilder` instead.
pub struct GitHubIssueUrlBuilder {
    title: String,
    body: String,
    labels: String,
    issue_type: String,
}

impl GitHubIssueUrlBuilder {
    /// Create a new URL builder with the given title.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            body: String::new(),
            labels: String::new(),
            issue_type: String::new(),
        }
    }

    /// Set the issue body content.
    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = body.into();
        self
    }

    /// Set the issue labels (comma-separated).
    pub fn labels(mut self, labels: impl Into<String>) -> Self {
        self.labels = labels.into();
        self
    }

    /// Set the issue type (Bug, Feature, etc.).
    pub fn issue_type(mut self, issue_type: impl Into<String>) -> Self {
        self.issue_type = issue_type.into();
        self
    }

    /// Build the GitHub issue URL with proper truncation.
    pub fn build_url(&self) -> String {
        let encoded_title = urlencoding::encode(&self.title);
        let encoded_labels = urlencoding::encode(&self.labels);
        let encoded_type = urlencoding::encode(&self.issue_type);

        // Calculate the URL overhead (everything except the body)
        let url_overhead = GITHUB_ISSUE_URL.len()
            + "?type=&title=&body=&labels=".len()
            + encoded_type.len()
            + encoded_title.len()
            + encoded_labels.len();

        let max_body_encoded_len = MAX_URL_LENGTH.saturating_sub(url_overhead);

        // Check if body fits, truncate if needed
        let body_encoded_len = urlencoding::encoded_len(&self.body);
        let final_body = if body_encoded_len <= max_body_encoded_len {
            self.body.clone()
        } else {
            self.truncate_body_to_fit(max_body_encoded_len)
        };

        let encoded_body = urlencoding::encode(&final_body);

        format!(
            "{}?type={}&title={}&body={}&labels={}",
            GITHUB_ISSUE_URL, encoded_type, encoded_title, encoded_body, encoded_labels
        )
    }

    /// Truncate the body to fit within the encoded length limit.
    fn truncate_body_to_fit(&self, max_encoded_len: usize) -> String {
        // Use conservative estimate: ~2.5x expansion for URL encoding
        let approx_max_chars = max_encoded_len / 3;

        let mut truncated: String = self.body.chars().take(approx_max_chars).collect();

        // Verify and adjust if still too long
        while urlencoding::encoded_len(&truncated) > max_encoded_len && !truncated.is_empty() {
            // Remove 10% of remaining chars
            let new_len = (truncated.chars().count() * 9) / 10;
            truncated = truncated.chars().take(new_len).collect();
        }

        if truncated.len() < self.body.len() {
            truncated.push_str("\n\n_[Content truncated due to URL length limits]_");

            // Final check - if the note itself pushed us over, truncate more aggressively
            while urlencoding::encoded_len(&truncated) > max_encoded_len && truncated.len() > 100 {
                let new_len = (truncated.chars().count() * 9) / 10;
                truncated = truncated.chars().take(new_len).collect();
            }
        }

        truncated
    }

    /// Open the issue URL in the default browser.
    pub fn open_in_browser(&self) -> Result<()> {
        let url = self.build_url();
        open::that(&url).map_err(|e| eyre::eyre!("Failed to open browser: {}", e))
    }
}

/// Simple URL encoding for the issue URL.
pub mod urlencoding {
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

    /// Calculate the length of a string after URL encoding without allocating.
    pub fn encoded_len(input: &str) -> usize {
        input
            .bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => 1,
                _ => 3,
            })
            .sum()
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

    #[test]
    fn test_encoded_len() {
        // Alphanumeric stays same length
        assert_eq!(urlencoding::encoded_len("abc123"), 6);
        // Spaces become %20 (3 chars each)
        assert_eq!(urlencoding::encoded_len("a b c"), 9); // a(1) + %20(3) + b(1) + %20(3) + c(1) = 9
        // Special chars triple
        assert_eq!(urlencoding::encoded_len("{}"), 6); // %7B(3) + %7D(3) = 6
        // Mix: "error: test" = e(1)+r(1)+r(1)+o(1)+r(1)+:(3)+space(3)+t(1)+e(1)+s(1)+t(1) = 15
        assert_eq!(urlencoding::encoded_len("error: test"), 15);
    }

    #[test]
    fn test_url_length_with_long_content_stays_under_limit() {
        // Create very long error message with lots of special characters
        let long_error = "error[E0308]: mismatched types\n".repeat(500);

        let builder = GitHubIssueBuilder::new(long_error.clone())
            .with_hx_content("N {}\nE {}\nQ test() {}".repeat(200));

        let url = builder.build_url();

        assert!(
            url.len() <= MAX_URL_LENGTH,
            "URL length {} exceeds limit {}",
            url.len(),
            MAX_URL_LENGTH
        );
    }

    #[test]
    fn test_url_includes_content_when_short() {
        let short_error = "error[E0308]: mismatched types";
        let builder = GitHubIssueBuilder::new(short_error.to_string())
            .with_hx_content("N User {}".to_string());

        let url = builder.build_url();

        // URL should contain the error
        assert!(url.contains("mismatched"));
        // URL should be well under the limit
        assert!(url.len() < MAX_URL_LENGTH / 2);
    }

    #[test]
    fn test_generic_url_builder_truncates_long_content() {
        // Create very long content with special characters
        let long_body = "This is feedback with special chars: {} [] <> \n".repeat(500);

        let url = GitHubIssueUrlBuilder::new("Test Issue")
            .body(long_body)
            .labels("feedback")
            .issue_type("Feature")
            .build_url();

        assert!(
            url.len() <= MAX_URL_LENGTH,
            "URL length {} exceeds limit {}",
            url.len(),
            MAX_URL_LENGTH
        );
    }

    #[test]
    fn test_generic_url_builder_preserves_short_content() {
        let short_body = "This is a short feedback message.";

        let url = GitHubIssueUrlBuilder::new("Short Test")
            .body(short_body)
            .labels("feedback")
            .issue_type("Feature")
            .build_url();

        // URL should contain the message
        assert!(url.contains("short%20feedback"));
        // URL should be well under the limit
        assert!(url.len() < MAX_URL_LENGTH / 2);
    }
}
