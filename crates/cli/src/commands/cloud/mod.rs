//! Helix Cloud resource commands.
//!
//! Every resource argument is optional and accepts an ID, slug, or name; see
//! [`crate::cloud::resolve`] for how an omitted one is found. Each command
//! prints a table or detail view by default and its JSON result under
//! `--json`. Running a group with no subcommand lists it.

pub mod api;
pub mod cluster;
pub mod database;
pub mod project;
pub mod service_credential;
pub mod workspace;

use crate::errors::CliError;
use crate::prompts;
use console::style;
use eyre::Result;

/// Fail fast, before any request, when a destructive action could never be
/// confirmed: no `--yes` and no terminal to prompt on (or `--json`).
fn ensure_confirmable(yes: bool) -> Result<()> {
    if yes || prompts::is_interactive() {
        return Ok(());
    }
    Err(CliError::new("this action needs confirmation")
        .with_hint("re-run with --yes to confirm")
        .into())
}

/// Ask before a destructive action; `--yes` skips the prompt. Call
/// [`ensure_confirmable`] first so a non-interactive run fails early.
fn confirm(yes: bool, question: &str) -> Result<bool> {
    ensure_confirmable(yes)?;
    if yes {
        return Ok(true);
    }
    prompts::confirm(question)
}

/// The one-time token in a create response. It is shown once and never
/// stored.
fn one_time_token(response: &serde_json::Value, what: &str) -> Result<String> {
    response
        .get("token")
        .and_then(serde_json::Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| eyre::eyre!("the {what} response omitted its one-time token"))
}

/// Derive a URL-safe slug from a display name.
///
/// ```text
/// "My App 2" → "my-app-2"
/// ```
fn slugify(name: &str) -> Result<String> {
    let slug = name
        .to_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        return Err(CliError::new(format!("cannot derive a slug from '{name}'"))
            .with_hint("pass --slug")
            .into());
    }
    Ok(slug)
}

/// Prefix for a table's name column marking the row this directory is
/// linked to with an orange dot. Rows (and the header, passed `linked =
/// false`) get the same width so columns align; with no link at all there is
/// no prefix.
fn linked_prefix(link_present: bool, linked: bool) -> String {
    match (link_present, linked) {
        (false, _) => String::new(),
        (true, true) => format!("{} ", style("●").color256(208)),
        (true, false) => "  ".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_lowercase_ascii_runs_joined_by_dashes() {
        assert_eq!(slugify("My App 2").unwrap(), "my-app-2");
        assert_eq!(slugify("  --Graph__DB--  ").unwrap(), "graph-db");
        assert_eq!(slugify("Ünïcode Name").unwrap(), "n-code-name");
        assert!(slugify("!!!").is_err());
    }

    #[test]
    fn confirmation_is_skipped_by_yes_and_required_without_a_terminal() {
        assert!(confirm(true, "Delete?").unwrap());
        ensure_confirmable(true).unwrap();
        assert!(ensure_confirmable(false).is_err());
        // Tests never run with an interactive stdin and stderr.
        let error = confirm(false, "Delete?").unwrap_err();
        assert_eq!(
            error.downcast_ref::<CliError>().unwrap().hint.as_deref(),
            Some("re-run with --yes to confirm")
        );
    }

    #[test]
    fn tokens_must_be_present_and_non_empty() {
        use serde_json::json;
        assert_eq!(one_time_token(&json!({"token": "s"}), "key").unwrap(), "s");
        assert!(one_time_token(&json!({"token": ""}), "key").is_err());
        assert!(one_time_token(&json!({}), "key").is_err());
    }

    #[test]
    fn linked_prefixes_keep_columns_aligned() {
        assert_eq!(
            console::measure_text_width(&linked_prefix(true, true)),
            console::measure_text_width(&linked_prefix(true, false))
        );
        assert_eq!(linked_prefix(false, true), "");
    }
}
