use std::{io, sync::LazyLock};

use crate::{
    metrics_sender::{load_metrics_config, save_metrics_config, MetricsLevel},
    output, MetricsAction,
};
use color_eyre::owo_colors::OwoColorize;
use eyre::{eyre, Result};
use regex::Regex;

pub async fn run(action: MetricsAction) -> Result<()> {
    match action {
        MetricsAction::Full => enable_full_metrics().await,
        MetricsAction::Basic => enable_basic_metrics().await,
        MetricsAction::Off => disable_metrics().await,
        MetricsAction::Status => show_metrics_status().await,
    }
}

async fn enable_full_metrics() -> Result<()> {
    output::info("Enabling metrics collection");

    let email = ask_for_email()?;
    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Full;
    config.email = Some(email);
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    save_metrics_config(&config)?;

    output::success("Metrics collection enabled");
    println!("  Thank you for helping us improve Helix!");

    Ok(())
}

async fn enable_basic_metrics() -> Result<()> {
    output::info("Enabling metrics collection");

    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Basic;
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    save_metrics_config(&config)?;

    output::success("Metrics collection enabled");
    println!("  Anonymous usage data will help improve Helix!");

    Ok(())
}

async fn disable_metrics() -> Result<()> {
    output::info("Disabling metrics collection");

    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Off;
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    save_metrics_config(&config)?;

    output::success("Metrics collection disabled");

    Ok(())
}

async fn show_metrics_status() -> Result<()> {
    let config = load_metrics_config().unwrap_or_default();

    println!("\n{}", "Metrics Status".bold().underline());
    println!(
        "  {}: {:?}",
        "Metrics Level".bright_white().bold(),
        config.level
    );

    if let Some(user_id) = &config.user_id {
        println!("  {}: {user_id}", "User ID".bright_white().bold());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let age = now.saturating_sub(config.last_updated);
    println!(
        "  {}: {}",
        "Last updated".bright_white().bold(),
        format_age(age)
    );

    Ok(())
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

fn ask_for_email() -> Result<String> {
    read_email_from(&mut io::stdin().lock())
}

fn read_email_from<R: io::BufRead>(reader: &mut R) -> Result<String> {
    loop {
        println!("Please enter your email address:");
        let mut email = String::new();
        let bytes = reader.read_line(&mut email)?;
        if bytes == 0 {
            return Err(eyre!(
                "email input ended before an address was provided; run `helix metrics full` from an interactive terminal"
            ));
        }
        let email = email.trim();
        if email.is_empty() || !is_valid_email(email) {
            println!("Invalid email address");
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
        let error = read_email_from(&mut reader)
            .unwrap_err()
            .to_string();
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
