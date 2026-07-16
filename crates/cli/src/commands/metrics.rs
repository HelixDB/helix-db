use std::{io, sync::LazyLock};

use crate::{
    metrics_sender::{load_metrics_config, save_metrics_config, MetricsLevel},
    output, MetricsAction,
};
use color_eyre::owo_colors::OwoColorize;
use eyre::Result;
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

    let email = ask_for_email();
    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Full;
    config.email = Some(email.leak());
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

fn ask_for_email() -> String {
    println!("Please enter your email address:");
    let mut email = String::new();
    io::stdin().read_line(&mut email).unwrap();
    let email = email.trim().to_string();
    // validate email
    if !EMAIL_REGEX.is_match(&email) {
        println!("Invalid email address");
        return ask_for_email();
    }
    email
}

#[cfg(test)]
mod tests {
    use super::format_age;

    #[test]
    fn formats_age_for_status_output() {
        assert_eq!(format_age(0), "just now");
        assert_eq!(format_age(42), "42s ago");
        assert_eq!(format_age(60), "1m ago");
        assert_eq!(format_age(3_600), "1h ago");
        assert_eq!(format_age(172_800), "2d ago");
    }
}
