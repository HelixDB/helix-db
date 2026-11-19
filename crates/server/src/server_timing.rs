//! The `Server-Timing` value both transports report for a query request,
//! executed or explained.
//!
//! HTTP sends it as the `Server-Timing` response header and gRPC as
//! `server-timing` response metadata. It carries durations only.

use std::time::Duration;

use db::query_service::CommitTimings;

/// Header and metadata name of the value.
pub(crate) const NAME: &str = "server-timing";

/// Format where the server time of one request went, in milliseconds.
///
/// `handled` runs from the fully received request to its response. `exec` is
/// that time less the two commit phases, so the metrics never overlap:
/// `commit` covers applying a write, and `durable` the wait for object
/// storage. A phase the request did not go through is left out.
pub(crate) fn value(handled: Duration, commit: CommitTimings) -> String {
    let exec = handled
        .saturating_sub(commit.commit.unwrap_or_default())
        .saturating_sub(commit.durable.unwrap_or_default());
    [
        ("exec", Some(exec)),
        ("commit", commit.commit),
        ("durable", commit.durable),
    ]
    .into_iter()
    .filter_map(|(name, duration)| {
        duration.map(|duration| format!("{name};dur={:.3}", duration.as_secs_f64() * 1000.0))
    })
    .collect::<Vec<_>>()
    .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_reports_only_execution() {
        assert_eq!(
            value(Duration::from_micros(1_250), CommitTimings::default()),
            "exec;dur=1.250"
        );
    }

    #[test]
    fn a_durable_write_reports_three_disjoint_phases() {
        assert_eq!(
            value(
                Duration::from_millis(50),
                CommitTimings {
                    commit: Some(Duration::from_millis(3)),
                    durable: Some(Duration::from_micros(40_100)),
                },
            ),
            "exec;dur=6.900, commit;dur=3.000, durable;dur=40.100"
        );
    }

    #[test]
    fn a_write_that_does_not_wait_omits_durable() {
        assert_eq!(
            value(
                Duration::from_millis(5),
                CommitTimings {
                    commit: Some(Duration::from_millis(2)),
                    durable: None,
                },
            ),
            "exec;dur=3.000, commit;dur=2.000"
        );
    }

    #[test]
    fn index_ddl_reports_its_durable_wait_without_a_commit() {
        assert_eq!(
            value(
                Duration::from_millis(12),
                CommitTimings {
                    commit: None,
                    durable: Some(Duration::from_millis(10)),
                },
            ),
            "exec;dur=2.000, durable;dur=10.000"
        );
    }

    /// The phases are measured inside the handled span, but separate clock
    /// reads can still put their sum a hair past it.
    #[test]
    fn execution_never_goes_negative() {
        assert_eq!(
            value(
                Duration::from_millis(1),
                CommitTimings {
                    commit: Some(Duration::from_millis(1)),
                    durable: Some(Duration::from_millis(1)),
                },
            ),
            "exec;dur=0.000, commit;dur=1.000, durable;dur=1.000"
        );
    }

    #[test]
    fn the_value_is_a_valid_header_and_metadata_value() {
        let value = value(
            Duration::from_secs(90),
            CommitTimings {
                commit: Some(Duration::from_secs(1)),
                durable: Some(Duration::from_secs(60)),
            },
        );
        assert!(axum::http::HeaderValue::from_str(&value).is_ok());
        assert!(value.parse::<tonic::metadata::AsciiMetadataValue>().is_ok());
    }
}
