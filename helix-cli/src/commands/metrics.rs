use std::{io, sync::LazyLock};

use crate::{
    MetricsAction,
    metrics_sender::{MetricsLevel, load_metrics_config, save_metrics_config},
    utils::{print_field, print_header, print_line, print_status, print_success},
};
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
    print_status("METRICS", "Enabling metrics collection");

    let email = ask_for_email();
    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Full;
    config.email = Some(email);
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    save_metrics_config(&config)?;

    print_success("Metrics collection enabled");
    print_line("  Thank you for helping us improve Helix!");

    Ok(())
}

async fn enable_basic_metrics() -> Result<()> {
    print_status("METRICS", "Enabling metrics collection");

    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Basic;
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    save_metrics_config(&config)?;

    print_success("Metrics collection enabled");
    print_line("  Anonymous usage data will help improve Helix!");

    Ok(())
}

async fn disable_metrics() -> Result<()> {
    print_status("METRICS", "Disabling metrics collection");

    let mut config = load_metrics_config().unwrap_or_default();
    config.level = MetricsLevel::Off;
    config.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    save_metrics_config(&config)?;

    print_success("Metrics collection disabled");

    Ok(())
}

async fn show_metrics_status() -> Result<()> {
    let config = load_metrics_config().unwrap_or_default();

    print_header("Metrics Status");
    print_field("Metrics Level", &format!("{:?}", config.level));

    if let Some(user_id) = &config.user_id {
        print_field("User ID", user_id);
    }

    let last_updated = std::time::UNIX_EPOCH + std::time::Duration::from_secs(config.last_updated);
    if let Ok(datetime) = last_updated.duration_since(std::time::UNIX_EPOCH) {
        print_field(
            "Last updated",
            &format!("{} seconds ago", datetime.as_secs()),
        );
    }

    Ok(())
}

static EMAIL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap());

fn ask_for_email() -> String {
    print_line("Please enter your email address:");
    let mut email = String::new();
    io::stdin().read_line(&mut email).unwrap();
    let email = email.trim().to_string();
    // validate email
    if !EMAIL_REGEX.is_match(&email) {
        print_line("Invalid email address");
        return ask_for_email();
    }
    email
}
