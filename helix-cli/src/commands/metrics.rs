use std::{io, sync::LazyLock};

use crate::{
    MetricsAction,
    metrics_sender::{MetricsLevel, load_metrics_config, save_metrics_config},
    output,
    utils::print_field,
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

    if let Some(device_id) = &config.device_id {
        print_field("Device ID", device_id);
    }

    let last_updated = std::time::UNIX_EPOCH + std::time::Duration::from_secs(config.last_updated);
    if let Ok(datetime) = last_updated.duration_since(std::time::UNIX_EPOCH) {
        println!(
            "  {}: {} seconds ago",
            "Last updated".bright_white().bold(),
            datetime.as_secs()
        );
    }

    Ok(())
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
