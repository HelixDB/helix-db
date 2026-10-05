//! Benchmark-only server entry (feature `async-index-benchmark`).
//!
//! Runs the normal transports as an explicit writer or reader and writes one
//! JSON sample per interval to stdout. Each sample carries cumulative
//! counters, so a measurement window is the difference of two samples:
//! index-operation backlog and publication, exact-operation publication lag,
//! queue merge cost, object-store connector attempts and body bytes, and
//! SlateDB's own storage metrics (flushes, compaction, per-attempt object
//! store requests). Connector byte counts are application body bytes, not
//! network-wire bytes.

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use db::config::{IndexOperationQueueTuning, QueueLayout};
use db::HelixDB;

use crate::config::{ServerConfig, StorageConfig};
use crate::{ServerDatabase, ServerResult};

/// Marker key identifying sample lines among other process output.
pub const SAMPLE_MARKER: &str = "helix_benchmark_sample";

/// Database role this benchmark process serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Opens the writer and runs index publication.
    Writer,
    /// Opens a read-only handle over shared storage.
    Reader,
}

impl Role {
    /// Parses `HELIX_BENCHMARK_ROLE`; absence selects the writer.
    ///
    /// ```
    /// use server::benchmark::Role;
    /// assert_eq!(Role::parse(None), Ok(Role::Writer));
    /// assert_eq!(Role::parse(Some("reader")), Ok(Role::Reader));
    /// assert!(Role::parse(Some("both")).is_err());
    /// ```
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("writer") => Ok(Self::Writer),
            Some("reader") => Ok(Self::Reader),
            Some(other) => Err(format!(
                "invalid HELIX_BENCHMARK_ROLE `{other}`: expected `writer` or `reader`"
            )),
        }
    }
}

/// Parses `HELIX_BENCHMARK_SAMPLE_MS`; absence selects one second.
///
/// ```
/// use std::time::Duration;
/// assert_eq!(server::benchmark::sample_interval(None), Ok(Duration::from_secs(1)));
/// assert_eq!(server::benchmark::sample_interval(Some("250")), Ok(Duration::from_millis(250)));
/// assert!(server::benchmark::sample_interval(Some("0")).is_err());
/// ```
pub fn sample_interval(value: Option<&str>) -> Result<Duration, String> {
    let Some(value) = value else {
        return Ok(Duration::from_secs(1));
    };
    match value.parse::<u64>() {
        Ok(millis) if millis > 0 => Ok(Duration::from_millis(millis)),
        Ok(_) | Err(_) => Err(format!(
            "invalid HELIX_BENCHMARK_SAMPLE_MS `{value}`: expected a positive integer"
        )),
    }
}

/// Parses `HELIX_INDEX_QUEUE_LAYOUT`; absence selects the product map layout.
///
/// Only benchmark builds read it, so product servers always run `map`. A
/// database must reopen with the layout that wrote its queues.
///
/// ```
/// use db::config::QueueLayout;
/// use server::benchmark::queue_layout;
/// assert_eq!(queue_layout(None), Ok(QueueLayout::Map));
/// assert_eq!(queue_layout(Some("rows")), Ok(QueueLayout::Rows));
/// assert!(queue_layout(Some("buckets")).is_err());
/// ```
pub fn queue_layout(value: Option<&str>) -> Result<QueueLayout, String> {
    match value {
        None | Some("map") => Ok(QueueLayout::Map),
        Some("rows") => Ok(QueueLayout::Rows),
        Some(other) => Err(format!(
            "invalid HELIX_INDEX_QUEUE_LAYOUT `{other}`: expected `map` or `rows`"
        )),
    }
}

/// Opens `role` with queue `layout` over the configured storage and caches,
/// holding a hybrid disk cache's directory lock like the product runner.
pub(crate) async fn open_database(
    role: Role,
    layout: QueueLayout,
    config: &ServerConfig,
) -> ServerResult<ServerDatabase> {
    if role == Role::Reader && matches!(config.storage, StorageConfig::Memory) {
        return Err("a benchmark reader requires shared disk or object storage".into());
    }
    let db_config = config.db_config().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default().with_layout(layout),
    );
    let cache_lock = crate::claim_disk_cache(config)?;
    let db = match role {
        Role::Writer => HelixDB::open_for_server(config.db_source(), db_config).await?,
        Role::Reader => HelixDB::open_reader_for_server(config.db_source(), db_config).await?,
    };
    Ok(ServerDatabase {
        db: Arc::new(db),
        cache_lock,
    })
}

/// Builds one cumulative sample.
pub fn sample(db: &HelixDB, role: Role, sequence: u64, started: Instant) -> serde_json::Value {
    serde_json::json!({
        SAMPLE_MARKER: 1,
        "role": match role {
            Role::Writer => "writer",
            Role::Reader => "reader",
        },
        "sample": sequence,
        "elapsed_ns": u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        "unix_ms": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX)),
        "queue": db.index_operation_queue_stats(),
        "lag": db.index_operation_publication_lag(),
        "merge": db::operation_queue_merge_stats(),
        "io": db::benchmark::connector_counters(),
        "storage": db::benchmark::storage_metrics(),
    })
}

/// Runs the transports for the configured role until shutdown, sampling
/// every interval and once more after the database closes.
pub async fn run_from_env() -> ServerResult<()> {
    crate::init_tracing_from_env();
    let role = Role::parse(env::var("HELIX_BENCHMARK_ROLE").ok().as_deref())?;
    let interval = sample_interval(env::var("HELIX_BENCHMARK_SAMPLE_MS").ok().as_deref())?;
    let layout = queue_layout(env::var("HELIX_INDEX_QUEUE_LAYOUT").ok().as_deref())?;
    let config = ServerConfig::from_env()?;
    tracing::info!(
        ?role,
        ?interval,
        ?layout,
        "starting async index benchmark server"
    );
    let started = Instant::now();
    let database = open_database(role, layout, &config).await?;
    let sampled = Arc::clone(&database.db);
    let runtime = crate::run_open_database_until_shutdown(config, database, async {
        crate::shutdown_signal().await;
        Ok(())
    });
    tokio::pin!(runtime);
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0_u64;
    let result = loop {
        tokio::select! {
            result = &mut runtime => break result,
            _ = ticks.tick() => {
                println!("{}", sample(&sampled, role, sequence, started));
                sequence += 1;
            }
        }
    };
    println!("{}", sample(&sampled, role, sequence, started));
    result
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use helix_ast::{batch, query, traversal, value};

    use super::*;

    #[tokio::test]
    async fn samples_carry_every_counter_group_and_readers_need_shared_storage() {
        let config = ServerConfig {
            http_addr: "127.0.0.1:0".parse().unwrap(),
            grpc_addr: "127.0.0.1:0".parse().unwrap(),
            db_path: "benchmark-sample".to_string(),
            storage: StorageConfig::Memory,
        };
        let Err(error) = open_database(Role::Reader, QueueLayout::Map, &config).await else {
            panic!("a memory reader cannot share the writer's storage");
        };
        assert!(error.to_string().contains("shared"), "{error}");

        let db = open_database(Role::Writer, QueueLayout::Map, &config)
            .await
            .unwrap()
            .db;
        let started = Instant::now();
        let first = sample(&db, Role::Writer, 0, started);
        let second = sample(&db, Role::Writer, 1, started);
        for key in ["queue", "lag", "merge", "io", "storage"] {
            assert!(first.get(key).is_some(), "sample lacks {key}: {first}");
        }
        assert_eq!(first[SAMPLE_MARKER], 1);
        assert_eq!(first["role"], "writer");
        assert_eq!(second["sample"], 1);
        assert!(second["elapsed_ns"].as_u64() >= first["elapsed_ns"].as_u64());
        assert_eq!(first["queue"]["pending_operations"], 0);
        assert_eq!(first["lag"]["count"], 0);
        let storage = first["storage"].as_array().unwrap();
        assert!(
            storage.iter().any(|metric| metric["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("slatedb."))),
            "the writer reports SlateDB metrics: {storage:?}"
        );
        db.close().await.unwrap();
    }

    /// Set only in the child processes
    /// [`benchmark_handles_leave_query_metrics_to_the_server`] launches; names
    /// how the child opens its reader.
    const METRICS_PROBE: &str = "HELIX_BENCHMARK_METRICS_PROBE";

    /// The server records query metrics itself, so a benchmark handle of
    /// either role must not also start the embedded query-metrics transport,
    /// which would report every query a second time under the embedded
    /// source. Telemetry settings are process-global, so each case re-runs
    /// this binary as a child whose environment enables telemetry toward a
    /// loopback collector. A plain embedded reader is the control: it must
    /// post to the collector, or the benchmark case proves nothing.
    #[test]
    fn benchmark_handles_leave_query_metrics_to_the_server() {
        for (case, expect_posts) in [("embedded", true), ("benchmark", false)] {
            let collector = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", collector.local_addr().unwrap());
            let posts = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&posts);
            std::thread::spawn(move || {
                for mut stream in collector.incoming().map_while(Result::ok) {
                    // Counted before the reply, so the child's close, which
                    // waits for its post, cannot finish before the count.
                    counted.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.read(&mut [0; 65_536]);
                    let _ = stream.write_all(
                        b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                    );
                }
            });
            let home = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(env::current_exe().unwrap())
                .args(["--exact", "benchmark::tests::metrics_probe", "--nocapture"])
                .env(METRICS_PROBE, case)
                .env("HELIX_HOME", home.path())
                .env("HELIX_TELEMETRY_LEVEL", "basic")
                .env(
                    "HELIX_TELEMETRY_INSTALLATION_ID",
                    "00000000-0000-4000-8000-000000000000",
                )
                .env("HELIX_TELEMETRY_ENDPOINT", endpoint)
                .env_remove("HELIX_TELEMETRY_USER_ID")
                .env_remove("HELIX_CLUSTER_ID")
                .status()
                .unwrap();
            assert!(status.success(), "the {case} metrics probe succeeds");
            assert_eq!(
                posts.load(Ordering::SeqCst) > 0,
                expect_posts,
                "whether the {case} reader posted embedded query metrics"
            );
        }
    }

    #[tokio::test]
    async fn metrics_probe() {
        let Ok(case) = env::var(METRICS_PROBE) else {
            return;
        };
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig {
            http_addr: "127.0.0.1:0".parse().unwrap(),
            grpc_addr: "127.0.0.1:0".parse().unwrap(),
            db_path: "benchmark-metrics".to_string(),
            storage: StorageConfig::Disk {
                root: root.path().to_path_buf(),
                cache: crate::config::CacheConfig::Memory,
            },
        };
        let writer = open_database(Role::Writer, QueueLayout::Map, &config)
            .await
            .unwrap()
            .db;
        writer
            .query(query::QueryRequest::write(batch::write_batch().var_as(
                "probe",
                traversal::g().add_n("Probe", vec![("n", value::PropertyInput::from(1_i64))]),
            )))
            .await
            .unwrap();
        writer.flush_writer().await.unwrap();
        let reader = match case.as_str() {
            "embedded" => Arc::new(
                HelixDB::open_reader_with_config(config.db_source(), config.db_config())
                    .await
                    .unwrap(),
            ),
            "benchmark" => {
                open_database(Role::Reader, QueueLayout::Map, &config)
                    .await
                    .unwrap()
                    .db
            }
            other => panic!("unknown metrics probe case {other}"),
        };
        let probes = reader
            .query(query::QueryRequest::read(
                batch::read_batch()
                    .var_as("probes", traversal::g().n_with_label("Probe").count())
                    .returning(["probes"]),
            ))
            .await
            .unwrap();
        assert_eq!(probes["probes"], 1);
        // Closing flushes any embedded transport's queued events.
        reader.close().await.unwrap();
        writer.close().await.unwrap();
    }
}
