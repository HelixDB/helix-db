//! Non-blocking anonymous query telemetry transport.
//!
//! Every recorded query is counted, whether or not its `query.execute` event
//! is delivered. While counts change, a `query.summary` event with the
//! session's cumulative counts is sent every [`SUMMARY_INTERVAL`], and once
//! more at shutdown. A session's query count is therefore the `queries` of its
//! last summary, even when individual events are dropped by the bounded queue
//! or rate-limited by the ingest service.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use thiserror::Error;

use crate::attribution;
use crate::cli;
use crate::telemetry::{self, Source};

use super::{ClusterId, QueryEvent};

/// How often a changed session summary is sent.
pub const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum TransportConfigError {
    #[error("query telemetry source must be helix-server or helix-embedded")]
    InvalidSource,
    #[error(transparent)]
    LocalConfiguration(#[from] cli::CliMetricsError),
    #[error(transparent)]
    Telemetry(#[from] telemetry::TelemetryError),
    #[error(transparent)]
    Query(#[from] super::ValidationError),
    #[error("HELIX_CLUSTER_ID must be valid UTF-8")]
    InvalidClusterIdEncoding,
}

/// Cumulative counts of one session. Each count only grows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetricsCounterSnapshot {
    pub emitted_events: u64,
    pub dropped_invalid_events: u64,
    pub dropped_ingress_full: u64,
}

impl MetricsCounterSnapshot {
    /// Every query recorded in the session, whether its event was queued or
    /// dropped.
    ///
    /// ```
    /// use helix_metrics::query::transport::MetricsCounterSnapshot;
    ///
    /// let snapshot = MetricsCounterSnapshot {
    ///     emitted_events: 5,
    ///     dropped_invalid_events: 1,
    ///     dropped_ingress_full: 2,
    /// };
    /// assert_eq!(snapshot.queries(), 8);
    /// ```
    #[must_use]
    pub const fn queries(&self) -> u64 {
        self.emitted_events
            .saturating_add(self.dropped_invalid_events)
            .saturating_add(self.dropped_ingress_full)
    }

    fn summary_event(self) -> Result<telemetry::Event, telemetry::TelemetryError> {
        #[derive(Serialize)]
        struct Properties {
            queries: u64,
            dropped_invalid: u64,
            dropped_queue_full: u64,
        }

        telemetry::Event::new(
            "query.summary",
            serde_json::to_value(Properties {
                queries: self.queries(),
                dropped_invalid: self.dropped_invalid_events,
                dropped_queue_full: self.dropped_ingress_full,
            })?,
        )
    }
}

#[derive(Default)]
struct MetricsCounters {
    emitted_events: AtomicU64,
    dropped_invalid_events: AtomicU64,
    dropped_ingress_full: AtomicU64,
}

impl MetricsCounters {
    fn snapshot(&self) -> MetricsCounterSnapshot {
        MetricsCounterSnapshot {
            emitted_events: self.emitted_events.load(Ordering::Relaxed),
            dropped_invalid_events: self.dropped_invalid_events.load(Ordering::Relaxed),
            dropped_ingress_full: self.dropped_ingress_full.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct OssQueryMetrics {
    recorder: telemetry::Recorder,
    cluster_id: Option<ClusterId>,
    counters: Arc<MetricsCounters>,
}

impl OssQueryMetrics {
    /// Records one event without waiting for delivery.
    #[must_use]
    pub fn record(&self, event: QueryEvent) -> bool {
        let event = match event.into_telemetry_with_cluster(self.cluster_id.as_ref()) {
            Ok(event) => event,
            Err(_) => {
                self.counters
                    .dropped_invalid_events
                    .fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };
        if self.recorder.record(event) {
            self.counters.emitted_events.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.counters
                .dropped_ingress_full
                .fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    #[must_use]
    pub fn counters(&self) -> MetricsCounterSnapshot {
        self.counters.snapshot()
    }
}

/// Owns delivery and periodic summaries of one session.
pub struct QueryMetricsRuntime {
    telemetry: Option<telemetry::Runtime>,
    summaries: tokio::task::JoinHandle<()>,
    counters: Arc<MetricsCounters>,
}

impl QueryMetricsRuntime {
    /// Flushes queued events and the session's final summary for a bounded
    /// amount of time.
    pub async fn shutdown(mut self) {
        self.summaries.abort();
        let snapshot = self.counters.snapshot();
        let final_summary = (snapshot.queries() > 0)
            .then(|| snapshot.summary_event().ok())
            .flatten();
        let Some(telemetry) = self.telemetry.take() else {
            return;
        };
        telemetry
            .shutdown(final_summary.into_iter().collect())
            .await;
    }
}

impl Drop for QueryMetricsRuntime {
    fn drop(&mut self) {
        self.summaries.abort();
    }
}

pub struct StartedQueryMetrics {
    pub recorder: OssQueryMetrics,
    pub runtime: QueryMetricsRuntime,
}

/// Starts query telemetry for `identity`, detecting attribution from this
/// process. `version` is the sending product's version.
pub fn start(
    source: Source,
    version: &str,
    identity: &super::OssIdentity,
    endpoint: impl Into<String>,
) -> Result<StartedQueryMetrics, TransportConfigError> {
    start_with(
        source,
        identity.client_info(version, &attribution::Attribution::detect(None))?,
        endpoint,
        None,
        SUMMARY_INTERVAL,
    )
}

fn start_with(
    source: Source,
    client: telemetry::ClientInfo,
    endpoint: impl Into<String>,
    cluster_id: Option<ClusterId>,
    summary_interval: Duration,
) -> Result<StartedQueryMetrics, TransportConfigError> {
    if !matches!(source, Source::Server | Source::Embedded) {
        return Err(TransportConfigError::InvalidSource);
    }
    let started = telemetry::start(source, client, endpoint)?;
    let counters = Arc::new(MetricsCounters::default());
    let summaries = tokio::spawn(report_summaries(
        started.recorder.clone(),
        Arc::clone(&counters),
        summary_interval,
    ));
    Ok(StartedQueryMetrics {
        recorder: OssQueryMetrics {
            recorder: started.recorder,
            cluster_id,
            counters: Arc::clone(&counters),
        },
        runtime: QueryMetricsRuntime {
            telemetry: Some(started.runtime),
            summaries,
            counters,
        },
    })
}

/// Sends the session's counts whenever they changed since the last summary
/// that was queued. A summary dropped by a full queue is retried next tick.
async fn report_summaries(
    recorder: telemetry::Recorder,
    counters: Arc<MetricsCounters>,
    summary_interval: Duration,
) {
    let mut interval = tokio::time::interval(summary_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    let mut reported = MetricsCounterSnapshot::default();
    loop {
        interval.tick().await;
        let snapshot = counters.snapshot();
        if snapshot != reported
            && snapshot
                .summary_event()
                .is_ok_and(|event| recorder.record(event))
        {
            reported = snapshot;
        }
    }
}

/// Starts telemetry from persisted privacy settings and environment overrides.
///
/// `version` is the sending product's version. `storage_deployment` is the
/// deployment ID derived from the database's storage, if any;
/// `HELIX_DEPLOYMENT_ID` replaces it.
pub fn start_oss_from_env(
    source: Source,
    version: &str,
    storage_deployment: Option<attribution::DeploymentId>,
) -> Result<Option<StartedQueryMetrics>, TransportConfigError> {
    let Some(settings) = cli::load_query_metrics_settings()? else {
        return Ok(None);
    };
    let cluster_id = cluster_id_from_env()?;
    let client = settings.identity.client_info(
        version,
        &attribution::Attribution::detect(storage_deployment),
    )?;
    start_with(
        source,
        client,
        settings.endpoint,
        cluster_id,
        SUMMARY_INTERVAL,
    )
    .map(Some)
}

fn cluster_id_from_env() -> Result<Option<ClusterId>, TransportConfigError> {
    cluster_id_from_env_value(std::env::var("HELIX_CLUSTER_ID"))
}

fn cluster_id_from_env_value(
    value: Result<String, std::env::VarError>,
) -> Result<Option<ClusterId>, TransportConfigError> {
    match value {
        Ok(cluster_id) => ClusterId::new(cluster_id).map(Some).map_err(Into::into),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(TransportConfigError::InvalidClusterIdEncoding)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::*;
    use crate::query::{
        CanonicalQuery, InstallationId, OssIdentity, QueryName, QueryOutcome, QueryType,
    };

    /// Accepts telemetry over HTTP/1.1 keep-alive and forwards each decoded
    /// envelope before answering `202`.
    async fn capture() -> (String, mpsc::UnboundedReceiver<telemetry::Envelope>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!(
            "http://{}/v1/events",
            listener.local_addr().expect("address")
        );
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let sender = sender.clone();
                tokio::spawn(async move {
                    let mut stream = tokio::io::BufReader::new(stream);
                    loop {
                        let mut content_length = 0;
                        loop {
                            let mut line = String::new();
                            if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                                return;
                            }
                            let line = line.trim_end();
                            if line.is_empty() {
                                break;
                            }
                            let Some((name, value)) = line.split_once(':') else {
                                continue;
                            };
                            if name.eq_ignore_ascii_case("content-length") {
                                content_length = value.trim().parse().expect("content length");
                            }
                        }
                        let mut body = vec![0; content_length];
                        stream.read_exact(&mut body).await.expect("body");
                        let _ = sender
                            .send(telemetry::Envelope::from_slice(&body).expect("valid envelope"));
                        stream
                            .get_mut()
                            .write_all(b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\n\r\n")
                            .await
                            .expect("respond");
                    }
                });
            }
        });
        (endpoint, receiver)
    }

    fn client() -> telemetry::ClientInfo {
        OssIdentity::new(InstallationId::now(), None)
            .client_info("9.9.9", &attribution::Attribution::detect(None))
            .expect("client")
    }

    fn query(name: &str) -> QueryEvent {
        QueryEvent::now(
            Some(QueryName::new(name).expect("query name")),
            CanonicalQuery::new(r#"{"queries":[],"returns":[]}"#).expect("query"),
            QueryType::Read,
            1,
            crate::query::RequestContext::default(),
            QueryOutcome::Succeeded {
                warnings: Vec::new(),
            },
            None,
        )
    }

    fn drain(
        received: &mut mpsc::UnboundedReceiver<telemetry::Envelope>,
    ) -> Vec<telemetry::Envelope> {
        std::iter::from_fn(|| received.try_recv().ok()).collect()
    }

    fn summaries(envelopes: &[telemetry::Envelope]) -> Vec<Value> {
        envelopes
            .iter()
            .flat_map(|envelope| &envelope.events)
            .filter(|event| event.name == "query.summary")
            .map(|event| event.properties.clone())
            .collect()
    }

    fn executions(envelopes: &[telemetry::Envelope]) -> usize {
        envelopes
            .iter()
            .flat_map(|envelope| &envelope.events)
            .filter(|event| event.name == "query.execute")
            .count()
    }

    #[tokio::test]
    async fn summaries_are_cumulative_skip_idle_ticks_and_end_with_a_final_summary() {
        let (endpoint, mut received) = capture().await;
        let client = client();
        let started = start_with(
            Source::Server,
            client.clone(),
            endpoint,
            Some(ClusterId::new("cluster-1").expect("cluster ID")),
            Duration::from_millis(20),
        )
        .expect("loopback endpoint");
        for _ in 0..3 {
            assert!(started.recorder.record(query("find_users")));
        }

        let mut envelopes = Vec::new();
        while summaries(&envelopes).is_empty() {
            envelopes.push(received.recv().await.expect("envelope"));
        }
        assert_eq!(
            summaries(&envelopes),
            [json!({"queries": 3, "dropped_invalid": 0, "dropped_queue_full": 0})]
        );
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        envelopes.extend(drain(&mut received));
        assert_eq!(summaries(&envelopes).len(), 1, "idle ticks send nothing");
        assert_eq!(executions(&envelopes), 3);

        assert!(started.recorder.record(query("find_users")));
        assert!(!started.recorder.record(query(&"n".repeat(20_000))));
        started.runtime.shutdown().await;
        envelopes.extend(drain(&mut received));
        assert_eq!(
            summaries(&envelopes).last(),
            Some(&json!({"queries": 5, "dropped_invalid": 1, "dropped_queue_full": 0}))
        );
        assert_eq!(executions(&envelopes), 4);
        assert!(envelopes.iter().all(|envelope| envelope.client == client));
        assert!(envelopes
            .iter()
            .flat_map(|envelope| &envelope.events)
            .filter(|event| event.name == "query.execute")
            .all(|event| event.properties["cluster_id"] == "cluster-1"));
    }

    #[tokio::test]
    async fn final_summary_counts_events_a_full_queue_dropped() {
        let (endpoint, mut received) = capture().await;
        let started = start_with(Source::Embedded, client(), endpoint, None, SUMMARY_INTERVAL)
            .expect("loopback endpoint");
        // The current-thread runtime cannot drain the queue until this loop
        // yields, so exactly the queue capacity is accepted.
        let accepted = (0..1_500)
            .filter(|_| started.recorder.record(query("bulk")))
            .count();
        assert_eq!(accepted, 1_000);
        assert_eq!(started.recorder.counters().dropped_ingress_full, 500);

        started.runtime.shutdown().await;
        let envelopes = drain(&mut received);
        assert_eq!(executions(&envelopes), 1_000);
        assert_eq!(
            summaries(&envelopes),
            [json!({"queries": 1_500, "dropped_invalid": 0, "dropped_queue_full": 500})]
        );
    }

    #[tokio::test]
    async fn sessions_without_queries_send_nothing() {
        let (endpoint, mut received) = capture().await;
        let started = start_with(
            Source::Embedded,
            client(),
            endpoint,
            None,
            Duration::from_millis(10),
        )
        .expect("loopback endpoint");
        tokio::time::sleep(Duration::from_millis(100)).await;
        started.runtime.shutdown().await;
        assert!(drain(&mut received).is_empty());
    }

    #[tokio::test]
    async fn dropping_the_runtime_stops_summaries() {
        let identity = OssIdentity::new(InstallationId::now(), None);
        let started = start(Source::Embedded, "9.9.9", &identity, "http://127.0.0.1:9")
            .expect("loopback endpoint");
        let summaries = started.runtime.summaries.abort_handle();
        drop(started.runtime);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !summaries.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("summary task stops with its runtime");
    }

    #[test]
    fn rejects_sources_without_query_events() {
        assert!(matches!(
            start_with(
                Source::Cli,
                client(),
                "http://127.0.0.1:9",
                None,
                SUMMARY_INTERVAL
            ),
            Err(TransportConfigError::InvalidSource)
        ));
        assert!(matches!(
            start_with(
                Source::Server,
                client(),
                "http://example.com",
                None,
                SUMMARY_INTERVAL
            ),
            Err(TransportConfigError::Telemetry(
                telemetry::TelemetryError::InsecureEndpoint
            ))
        ));
    }

    #[test]
    fn summary_counts_saturate() {
        let snapshot = MetricsCounterSnapshot {
            emitted_events: u64::MAX,
            dropped_invalid_events: 1,
            dropped_ingress_full: 1,
        };
        assert_eq!(snapshot.queries(), u64::MAX);
        assert_eq!(
            snapshot.summary_event().expect("summary").properties,
            json!({"queries": u64::MAX, "dropped_invalid": 1, "dropped_queue_full": 1})
        );
    }

    #[test]
    fn cluster_identity_uses_only_the_environment_contract() {
        assert_eq!(
            cluster_id_from_env_value(Ok("cluster-1".to_owned()))
                .expect("valid cluster environment"),
            Some(ClusterId::new("cluster-1").expect("cluster ID"))
        );
        assert_eq!(
            cluster_id_from_env_value(Err(std::env::VarError::NotPresent))
                .expect("missing cluster environment"),
            None
        );
        assert!(matches!(
            cluster_id_from_env_value(Ok(" cluster-1".to_owned())),
            Err(TransportConfigError::Query(
                super::super::ValidationError::InvalidContextId("cluster_id")
            ))
        ));
        assert!(matches!(
            cluster_id_from_env_value(Err(std::env::VarError::NotUnicode(
                std::ffi::OsString::new()
            ))),
            Err(TransportConfigError::InvalidClusterIdEncoding)
        ));
    }
}
