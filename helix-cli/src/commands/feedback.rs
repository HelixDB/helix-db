//! Send feedback to the Helix team via GitHub issues.

use crate::github_issue::GitHubIssueUrlBuilder;
use crate::output;
use crate::prompts;
use eyre::{Result, eyre};

/// Type of feedback being submitted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackType {
    Bug,
    FeatureRequest,
    General,
}

impl FeedbackType {
    /// Get the GitHub issue type parameter
    fn issue_type(&self) -> &'static str {
        match self {
            FeedbackType::Bug => "Bug",
            FeedbackType::FeatureRequest => "Feature",
            FeedbackType::General => "Feature",
        }
    }

    /// Get the GitHub labels for this feedback type
    fn labels(&self) -> &'static str {
        match self {
            FeedbackType::Bug => "bug,cli",
            FeedbackType::FeatureRequest => "enhancement",
            FeedbackType::General => "feedback",
        }
    }

    /// Get a title prefix for the issue
    fn title_prefix(&self) -> &'static str {
        match self {
            FeedbackType::Bug => "bug: ",
            FeedbackType::FeatureRequest => "feature: ",
            FeedbackType::General => "feedback: ",
        }
    }
}

/// Run the feedback command
pub async fn run(message: Option<String>) -> Result<()> {
    let (feedback_type, feedback_message) = if let Some(msg) = message {
        // Inline message provided - default to General feedback
        (FeedbackType::General, msg)
    } else {
        // Interactive mode
        if !prompts::is_interactive() {
            return Err(eyre!(
                "No feedback message provided. Run 'helix feedback \"your message\"' or run in an interactive terminal."
            ));
        }

        prompts::intro("helix feedback", Some("Submit feedback for Helix"))?;
        let feedback_type = prompts::select_feedback_type()?;
        let feedback_message = prompts::input_feedback_message()?;

        if !prompts::confirm("Open browser to submit feedback?")? {
            output::info("Feedback cancelled.");
            return Ok(());
        }

        (feedback_type, feedback_message)
    };

    // Build and open the GitHub issue URL
    output::info("Opening browser to submit feedback...");
    build_issue_builder(feedback_type, &feedback_message).open_in_browser()?;

    output::success("Browser opened! Complete your feedback submission on GitHub.");
    Ok(())
}

/// Build the GitHub issue URL builder with pre-filled content
fn build_issue_builder(feedback_type: FeedbackType, message: &str) -> GitHubIssueUrlBuilder {
    let title = format!(
        "{}{}",
        feedback_type.title_prefix(),
        truncate_for_title(message)
    );

    let body = build_issue_body(message);

    GitHubIssueUrlBuilder::new(title)
        .body(body)
        .labels(feedback_type.labels())
        .issue_type(feedback_type.issue_type())
}

/// Build the full issue body
fn build_issue_body(message: &str) -> String {
    let mut body = String::new();

    // Environment section
    body.push_str("## Environment\n");
    body.push_str(&format!(
        "- Helix CLI version: {}\n",
        env!("CARGO_PKG_VERSION")
    ));
    body.push_str(&format!("- OS: {}\n\n", std::env::consts::OS));

    // Feedback section
    body.push_str("## Feedback\n");
    body.push_str(message);

    body
}

/// Truncate message to create a reasonable issue title
fn truncate_for_title(message: &str) -> String {
    let first_line = message.lines().next().unwrap_or(message);
    if first_line.chars().count() > 50 {
        let truncated: String = first_line.chars().take(47).collect();
        format!("{}...", truncated)
    } else {
        first_line.to_string()
    }
}
