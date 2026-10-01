use std::{
    io::{self, IsTerminal as _},
    sync::LazyLock,
};

use crate::errors::CliError;
use crate::output::{self, table};
use crate::{
    metrics_sender::{load_metrics_config, save_metrics_config, MetricsLevel},
    prompts, MetricsAction,
};
use eyre::{eyre, Result};
use regex::Regex;
use serde_json::json;

pub async fn run(action: MetricsAction) -> Result<()> {
    let level = match action {
        MetricsAction::Full => MetricsLevel::Full,
        MetricsAction::Basic => MetricsLevel::Basic,
        MetricsAction::Off => MetricsLevel::Off,
        MetricsAction::Status => return show_metrics_status(),
    };
    let mut config = load_metrics_config().unwrap_or_default();
    if level == MetricsLevel::Full {
        config.email = Some(ask_for_email()?);
    }
    config.level = level;
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    save_metrics_config(&config)?;

    match level {
        MetricsLevel::Full => {
            output::success("Full metrics enabled");
            output::remark("Thank you for helping improve Helix!");
        }
        MetricsLevel::Basic => {
            output::success("Basic metrics enabled");
            output::remark("Only anonymous usage data is collected.");
        }
        MetricsLevel::Off => output::success("Metrics disabled"),
    }
    output::emit(&json!({"level": level_name(level)}), |_| Ok(()))
}

fn show_metrics_status() -> Result<()> {
    let config = load_metrics_config().unwrap_or_default();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let status = json!({
        "level": level_name(config.level),
        "userId": config.user_id,
        "lastUpdated": config.last_updated,
    });
    output::emit(&status, |_| {
        print!(
            "{}",
            table::key_values(&[
                ("Level", level_name(config.level).to_owned()),
                ("User ID", config.user_id.clone().unwrap_or_default()),
                (
                    "Last updated",
                    format_age(now.saturating_sub(config.last_updated))
                ),
            ])
        );
        Ok(())
    })
}

fn level_name(level: MetricsLevel) -> &'static str {
    match level {
        MetricsLevel::Full => "full",
        MetricsLevel::Basic => "basic",
        MetricsLevel::Off => "off",
    }
}

fn format_age(seconds: u64) -> String {
    match seconds {
        0..=4 => "just now".to_string(),
        5..=59 => format!("{seconds}s ago"),
        60..=3_599 => format!("{}m ago", seconds / 60),
        3_600..=86_399 => format!("{}h ago", seconds / 3_600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

static EMAIL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap());

fn is_valid_email(email: &str) -> bool {
    EMAIL_REGEX.is_match(email)
}

/// Prompt for an email on a terminal; read one line from piped stdin otherwise.
fn ask_for_email() -> Result<String> {
    if !prompts::is_interactive() {
        // Under --json nothing can prompt, so a terminal on stdin would wait
        // silently; only piped input is read.
        if io::stdin().is_terminal() {
            return Err(CliError::new("`helix metrics full` needs an email address")
                .with_hint(
                    "pipe it on stdin, e.g. `echo you@example.com | helix metrics full --json`",
                )
                .into());
        }
        return read_email_from(&mut io::stdin().lock());
    }
    let email: String = cliclack::input("Email address")
        .placeholder("you@example.com")
        .validate(|input: &String| {
            if is_valid_email(input.trim()) {
                Ok(())
            } else {
                Err("enter a valid email address")
            }
        })
        .interact()?;
    Ok(email.trim().to_owned())
}

fn read_email_from<R: io::BufRead>(reader: &mut R) -> Result<String> {
    loop {
        let mut email = String::new();
        let bytes = reader.read_line(&mut email)?;
        if bytes == 0 {
            return Err(eyre!(
                "email input ended before an address was provided; run `helix metrics full` from an interactive terminal"
            ));
        }
        let email = email.trim();
        if email.is_empty() || !is_valid_email(email) {
            output::warning("Invalid email address; enter another");
            continue;
        }
        return Ok(email.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{format_age, is_valid_email, read_email_from};

    #[test]
    fn formats_age_for_status_output() {
        assert_eq!(format_age(0), "just now");
        assert_eq!(format_age(42), "42s ago");
        assert_eq!(format_age(60), "1m ago");
        assert_eq!(format_age(3_600), "1h ago");
        assert_eq!(format_age(172_800), "2d ago");
    }

    #[test]
    fn validates_email_addresses() {
        assert!(is_valid_email("user@example.com"));
        assert!(!is_valid_email(""));
        assert!(!is_valid_email("not-an-email"));
        assert!(!is_valid_email("@example.com"));
    }

    #[test]
    fn read_email_from_rejects_eof_before_valid_input() {
        let mut reader = Cursor::new(Vec::new());
        let error = read_email_from(&mut reader).unwrap_err().to_string();
        assert!(error.contains("email input ended"));
    }

    #[test]
    fn read_email_from_skips_invalid_lines_before_accepting_valid_email() {
        let mut reader = Cursor::new(b"not-an-email\nuser@example.com\n");
        assert_eq!(
            read_email_from(&mut reader).unwrap(),
            "user@example.com".to_string()
        );
    }
}
