//! CLI mode log handlers for non-interactive log viewing.

use super::log_source::LogSource;
use chrono::{DateTime, Duration, Utc};
use eyre::{Result, eyre};

/// Stream live logs to stdout until interrupted.
pub async fn stream_live(log_source: &LogSource) -> Result<()> {
    println!("Streaming logs (Ctrl+C to stop)...\n");

    log_source
        .stream_live(|line| {
            println!("{}", line);
        })
        .await
}

/// Query and print historical logs within a time range.
pub async fn query_range(
    log_source: &LogSource,
    start: Option<String>,
    end: Option<String>,
) -> Result<()> {
    let (start_time, end_time) = parse_time_range(start, end)?;

    // Validate time range (max 1 hour)
    let duration = end_time.signed_duration_since(start_time);
    if duration > Duration::hours(1) {
        return Err(eyre!(
            "Time range cannot exceed 1 hour. Requested range: {} minutes",
            duration.num_minutes()
        ));
    }

    if start_time >= end_time {
        return Err(eyre!("Start time must be before end time"));
    }

    println!(
        "Fetching logs from {} to {}...\n",
        start_time.format("%Y-%m-%d %H:%M:%S UTC"),
        end_time.format("%Y-%m-%d %H:%M:%S UTC")
    );

    let logs = log_source.query_range(start_time, end_time).await?;

    if logs.is_empty() {
        println!("No logs found in the specified time range.");
    } else {
        for line in logs {
            println!("{}", line);
        }
    }

    Ok(())
}

/// Parse start and end time strings into DateTime<Utc>.
/// If not provided, defaults to last 15 minutes.
fn parse_time_range(
    start: Option<String>,
    end: Option<String>,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let now = Utc::now();

    let end_time = match end {
        Some(s) => parse_datetime(&s)?,
        None => now,
    };

    let start_time = match start {
        Some(s) => parse_datetime(&s)?,
        None => end_time - Duration::minutes(15),
    };

    Ok((start_time, end_time))
}

/// Parse a datetime string in ISO 8601 format.
fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    // Try parsing with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing ISO 8601 with Z suffix
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Ok(dt);
    }

    // Try parsing without timezone (assume UTC)
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(naive.and_utc());
    }

    Err(eyre!(
        "Invalid datetime format: '{}'. Use ISO 8601 format (e.g., 2024-01-15T10:00:00Z)",
        s
    ))
}
