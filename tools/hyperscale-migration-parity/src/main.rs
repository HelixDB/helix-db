#![recursion_limit = "256"]

mod external_sort;
mod hash_contract;
mod logical_oracle;
mod object_store_metrics;
mod report;
mod secondary_oracle;
#[cfg(test)]
mod secondary_oracle_regressions;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use db::config::{MigrationBatchRows, MigrationTuning, MigrationWorkerMode};
use db::migration_parity::migration_parity_index_outbox_failpoints;
use db::migrations::{MigrationFailpoint, MigrationParityId, MigrationParityState};
use db::{DbConfig, HelixDB};
use futures::TryStreamExt;
use helix::db::{
    DynamicIndexDefinition as HDynamicIndexDefinition,
    SecondaryIndexDefinition as HSecondaryIndexDefinition,
    VectorIndexDefinition as HVectorIndexDefinition,
};
use helix::{
    graph, HelixDb as HyperscaleDb, HelixDbConfig as HyperscaleConfig, Property as HProperty,
    PropertyValue as HPropertyValue, TextIndexDefinition as HTextIndexDefinition,
    EDGE_UPDATE_ADAPTIVE, EDGE_UPDATE_EAGER, EDGE_UPDATE_LAZY,
};
use hyperscale_slatedb::config::{
    FlushOptions as SourceFlushOptions, FlushType as SourceFlushType,
    ScanOptions as SourceScanOptions,
};
use hyperscale_slatedb::IsolationLevel;
use object_store::local::LocalFileSystem;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use target_slatedb_common::clock::{SystemClock, SystemClockTicker};
use tracing::{field, info, warn, Event, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

use crate::object_store_metrics::{
    FaultKind, FaultPolicy, InstrumentedStore012, InstrumentedStore014, ObjectStoreRecorder,
    Operation,
};

const FIRST_COMPACTION_ERROR_LIMIT: usize = 100;
static COMPACTION_ERRORS: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[derive(Debug)]
struct ShiftedSystemClock {
    advance: Duration,
}

impl SystemClock for ShiftedSystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
            + chrono::Duration::from_std(self.advance)
                .expect("checkpoint clock advance fits chrono duration")
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, duration)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointExpiryMode {
    RealTime,
    AdvancePastLatest,
}

struct GarbageCollectionCheckpointEvidence {
    clock_advance_millis: u64,
    checkpoints: u64,
    expiring_checkpoints: u64,
    permanent_checkpoints: u64,
}

struct CompactionErrorLayer;

impl<S> Layer<S> for CompactionErrorLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: LayerContext<'_, S>) {
        if event.metadata().target() != "slatedb::compactor"
            || *event.metadata().level() != tracing::Level::ERROR
        {
            return;
        }
        let mut visitor = CompactionErrorVisitor::default();
        event.record(&mut visitor);
        let message = format!("{}: {}", event.metadata().name(), visitor.fields.join(", "));
        let Ok(mut errors) = COMPACTION_ERRORS.lock() else {
            return;
        };
        errors.push(message);
    }
}

#[derive(Default)]
struct CompactionErrorVisitor {
    fields: Vec<String>,
}

impl field::Visit for CompactionErrorVisitor {
    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        self.fields.push(format!("{}={value:?}", field.name()));
    }
}

const DATABASE: &str = "graph";
const VECTOR_QUERY: [f32; 3] = [0.1, 0.0, 0.0];
const EXPECTED_VECTOR_HITS: [(u64, u32); 3] =
    [(1, 1_008_981_771), (2, 1_062_165_544), (3, 1_082_151_404)];
const SOURCE_HYPERSCALE_REVISION: &str = "e5bac15b020c9acac1649c44b58a2cf16dd1f874";
const TARGET_SLATEDB_VERSION: &str = "0.15.0";
const TARGET_SLATEDB_GIT_SOURCE: &str = "git+https://github.com/HelixDB/slatedb.git?rev=";
const FIRST_SCALE_NODE_ID: u64 = 1_000_000;
const SCALE_SEED_PROGRESS_INTERVAL: u64 = 100_000;
const SCALE_EDGE_LABEL: &str = "SCALE_EDGE";
const SCALE_NODE_LABEL: &str = "SCALE_NODE";
const SCALE_FIXTURE_VERSION: u32 = 2;
const SCALE_EDGE_CHECKPOINT_NAME: &[u8] = b"migration_parity_scale_edge_checkpoint_v1";
const SCALE_EDGE_CHECKPOINT_VERSION: u8 = 1;
const MAXIMUM_STAGED_WRITE_BYTES: usize = 64 * 1024 * 1024;

struct ProgressHeartbeat {
    processed: Arc<AtomicU64>,
    phase: Arc<Mutex<String>>,
    object_stores: Arc<Mutex<Vec<Arc<ObjectStoreRecorder>>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ProgressHeartbeat {
    fn start(
        phase: &str,
        total_rows: u64,
        scratch_root: PathBuf,
        object_store: Arc<ObjectStoreRecorder>,
    ) -> Self {
        let processed = Arc::new(AtomicU64::new(0));
        let observed = Arc::clone(&processed);
        let phase = Arc::new(Mutex::new(phase.to_string()));
        let observed_phase = Arc::clone(&phase);
        let object_stores = Arc::new(Mutex::new(vec![object_store]));
        let observed_stores = Arc::clone(&object_stores);
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let processed_rows = observed.load(Ordering::Relaxed);
                        let elapsed = started.elapsed().as_secs_f64().max(0.001);
                        let rows_per_second = processed_rows as f64 / elapsed;
                        let eta_seconds = if rows_per_second > 0.0 {
                            (total_rows.saturating_sub(processed_rows) as f64 / rows_per_second).ceil() as u64
                        } else {
                            u64::MAX
                        };
                        let (requests, transferred_bytes) = observed_stores
                            .lock()
                            .map(|stores| {
                                stores.iter().fold((0_u64, 0_u64), |totals, store| {
                                    let metrics = store.snapshot();
                                    (
                                        totals.0
                                            .saturating_add(metrics.get_requests)
                                            .saturating_add(metrics.head_requests)
                                            .saturating_add(metrics.put_requests)
                                            .saturating_add(metrics.multipart_requests)
                                            .saturating_add(metrics.list_requests)
                                            .saturating_add(metrics.delete_requests)
                                            .saturating_add(metrics.copy_requests),
                                        totals.1
                                            .saturating_add(metrics.bytes_read)
                                            .saturating_add(metrics.bytes_written),
                                    )
                                })
                            })
                            .unwrap_or_default();
                        let phase = observed_phase
                            .lock()
                            .map(|phase| phase.clone())
                            .unwrap_or_else(|_| "unknown".to_string());
                        let current_scratch_bytes = report::record_scratch_bytes(&scratch_root);
                        info!(
                            phase,
                            processed_rows,
                            total_rows,
                            rows_per_second,
                            eta_seconds,
                            rss_bytes = report::peak_rss_bytes(),
                            peak_scratch_bytes = report::peak_scratch_bytes(),
                            current_scratch_bytes,
                            object_store_requests = requests,
                            object_store_transferred_bytes = transferred_bytes,
                            "migration parity heartbeat"
                        );
                    }
                    _ = &mut stopped => break,
                }
            }
        });
        Self {
            processed,
            phase,
            object_stores,
            stop: Some(stop),
        }
    }

    fn set_processed(&self, rows: u64) {
        self.processed.store(rows, Ordering::Relaxed);
    }

    fn set_phase(&self, phase: &str) {
        if let Ok(mut current) = self.phase.lock() {
            *current = phase.to_string();
        }
    }

    fn add_object_store(&self, object_store: Arc<ObjectStoreRecorder>) {
        if let Ok(mut stores) = self.object_stores.lock() {
            stores.push(object_store);
        }
    }
}

impl Drop for ProgressHeartbeat {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[derive(Debug)]
struct Args {
    hyperscale: PathBuf,
    store_root: PathBuf,
    batch_rows: usize,
    oracle_buffer_bytes: NonZeroUsize,
    preserve_store: bool,
    report: PathBuf,
    object_store_latency: Duration,
    storage: Storage,
    target_fault: Option<TargetFault>,
    maximum_open_attempts: NonZeroUsize,
    scale_nodes: u64,
    scale_edges: u64,
    seed_batch_rows: NonZeroUsize,
    distribution: GraphDistribution,
    scenario_filter: Option<&'static str>,
    maximum_scenario_duration: Duration,
    maximum_suite_duration: Duration,
    profile: String,
    scale_baseline_reports: Vec<PathBuf>,
    project_next_rows: Option<u64>,
    resume_verification: bool,
    resume_source_seed: bool,
    compaction_drain_timeout: Duration,
    maximum_steady_l0_ssts: usize,
    migration_failpoint: Option<MigrationFailpoint>,
    crash_recovery_matrix: bool,
}

#[derive(Debug, Clone, Copy)]
struct TargetFault {
    kind: FaultKind,
    operation: Operation,
    every: NonZeroU64,
}

#[derive(Debug, Clone, Copy)]
enum GraphDistribution {
    Uniform,
    PowerLaw,
    Star,
    Dense,
    SelfLoop,
    HotPair,
}

impl GraphDistribution {
    const fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::PowerLaw => "power-law",
            Self::Star => "star",
            Self::Dense => "dense",
            Self::SelfLoop => "self-loop",
            Self::HotPair => "hot-pair",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "uniform" => Some(Self::Uniform),
            "power-law" | "zipf" => Some(Self::PowerLaw),
            "star" => Some(Self::Star),
            "dense" => Some(Self::Dense),
            "self-loop" => Some(Self::SelfLoop),
            "hot-pair" => Some(Self::HotPair),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum Storage {
    Local,
    Minio(MinioConfig),
}

#[derive(Debug, Clone)]
struct MinioConfig {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    run_prefix: String,
}

impl Storage {
    fn report_name(&self) -> String {
        match self {
            Self::Local => "local_filesystem".to_string(),
            Self::Minio(config) => format!(
                "minio(endpoint={},bucket={},prefix={})",
                config.endpoint, config.bucket, config.run_prefix
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Scenario {
    name: &'static str,
    edge_policy: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SourceSemanticEvidence {
    node_text_hits: Vec<report::TextHitEvidence>,
    edge_text_hits: Vec<report::TextHitEvidence>,
    node_vector_metadata: report::SourceVectorMetadataEvidence,
    edge_vector_metadata: report::SourceVectorMetadataEvidence,
    node_vector_hits: Vec<report::VectorHitEvidence>,
    edge_vector_hits: Vec<report::VectorHitEvidence>,
    vector_non_metadata_namespace_digests: BTreeMap<u64, String>,
}

#[derive(Debug, Clone, PartialEq)]
struct TargetSemanticEvidence {
    node_text: Vec<db::migration_parity::MigrationParityTextSearch>,
    edge_text: Vec<db::migration_parity::MigrationParityTextSearch>,
    node_vector_metadata: db::migration_parity::MigrationParityVectorMetadata,
    edge_vector_metadata: db::migration_parity::MigrationParityVectorMetadata,
    node_vector_hits: Vec<db::migration_parity::MigrationParityVectorHit>,
    edge_vector_hits: Vec<db::migration_parity::MigrationParityVectorHit>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    if std::env::args().len() == 2
        && std::env::args().nth(1).as_deref() == Some("--emit-hash-contract-golden")
    {
        println!("{}", hash_contract::emit_golden_json()?);
        return Ok(());
    }
    let hash_contract = hash_contract::verify()?;
    let args = parse_args()?;
    if args.crash_recovery_matrix {
        return run_crash_recovery_matrix(&args);
    }
    let revisions = revision_evidence(&args.hyperscale)?;

    if args.store_root.exists() && !args.preserve_store {
        std::fs::remove_dir_all(&args.store_root).with_context(|| {
            format!(
                "failed to remove existing store root {}",
                args.store_root.display()
            )
        })?;
    }
    std::fs::create_dir_all(&args.store_root)
        .with_context(|| format!("failed to create store root {}", args.store_root.display()))?;

    let scenarios = [
        Scenario {
            name: "eager-legacy-rewrite",
            edge_policy: EDGE_UPDATE_EAGER,
        },
        Scenario {
            name: "lazy-merge-operands",
            edge_policy: EDGE_UPDATE_LAZY,
        },
        Scenario {
            name: "adaptive-mixed-state",
            edge_policy: EDGE_UPDATE_ADAPTIVE,
        },
    ];

    let selected = scenarios
        .into_iter()
        .filter(|scenario| {
            args.scenario_filter
                .is_none_or(|filter| scenario.name.starts_with(filter))
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!("scenario filter did not select a migration mode");
    }
    if args.resume_verification {
        let mut resumed = Vec::with_capacity(selected.len());
        for scenario in selected.iter().copied() {
            resumed.push(
                tokio::time::timeout(
                    args.maximum_scenario_duration,
                    resume_scenario_verification(&args, scenario),
                )
                .await
                .with_context(|| {
                    format!(
                        "resumed scenario {} exceeded its {:?} runtime limit",
                        scenario.name, args.maximum_scenario_duration
                    )
                })??,
            );
        }
        report::write_resume_report(
            &args.report,
            &revisions,
            args.storage.report_name(),
            resumed,
        )?;
        return Ok(());
    }
    let mut evidence = Vec::with_capacity(selected.len());
    let suite_started = Instant::now();
    for scenario in selected.iter().copied() {
        let remaining =
            remaining_suite_duration(args.maximum_suite_duration, suite_started.elapsed())
                .with_context(|| {
                    format!(
                        "profile {} exceeded its {:?} aggregate runtime limit before scenario {}",
                        args.profile, args.maximum_suite_duration, scenario.name
                    )
                })?;
        let scenario_limit = remaining.min(args.maximum_scenario_duration);
        let outcome = tokio::time::timeout(scenario_limit, run_scenario(&args, scenario)).await;
        match outcome {
            Ok(Ok(result)) => {
                evidence.push(result);
                report::record_scratch_bytes(&args.store_root);
                if let Err(error) = cleanup_successful_scenario(&args, scenario).await {
                    report::write_failure_report(
                        &args.report,
                        &revisions,
                        harness_config(&args)?,
                        &error.to_string(),
                        &evidence,
                    )?;
                    return Err(error);
                }
            }
            Ok(Err(error)) => {
                report::write_failure_report(
                    &args.report,
                    &revisions,
                    harness_config(&args)?,
                    &error.to_string(),
                    &evidence,
                )?;
                return Err(error);
            }
            Err(error) => {
                let message = format!(
                    "scenario {} exceeded its {:?} runtime limit: {error}",
                    scenario.name, scenario_limit
                );
                report::write_failure_report(
                    &args.report,
                    &revisions,
                    harness_config(&args)?,
                    &message,
                    &evidence,
                )?;
                bail!(message);
            }
        }
    }

    report::record_scratch_bytes(&args.store_root);
    let host = report::host_evidence(&args.store_root);
    let mut release_blockers = Vec::new();
    if revisions.target_helix_dirty {
        release_blockers.push("target checkout has uncommitted changes".to_string());
    }
    if revisions.source_hyperscale_dirty {
        release_blockers.push("source hyperscale checkout has uncommitted changes".to_string());
    }
    if evidence
        .iter()
        .any(|scenario| scenario.failed_compactions > 0 || scenario.compaction_errors.count > 0)
    {
        release_blockers.push(
            "SlateDB recorded one or more failed compactions during migration verification"
                .to_string(),
        );
    }
    let scale_analysis = report::analyze_scale(
        &args.scale_baseline_reports,
        args.scale_nodes.saturating_add(args.scale_edges),
        &evidence,
        args.project_next_rows,
        host.total_memory_bytes,
        host.scratch_available_bytes,
        args.maximum_scenario_duration.as_secs(),
    )?;
    if scale_analysis
        .as_ref()
        .is_some_and(|analysis| !analysis.passed)
    {
        release_blockers.push(
            "scale exponent, per-row amplification, or next-rung projection exceeded its gate"
                .to_string(),
        );
    }
    let report = report::VerificationReport {
        schema_version: 3,
        status: if release_blockers.is_empty() {
            "passed".to_string()
        } else {
            "smoke_passed_release_blocked".to_string()
        },
        revisions,
        hash_contract,
        config: harness_config(&args)?,
        host,
        peak_rss_bytes: report::peak_rss_bytes(),
        peak_scratch_bytes: report::peak_scratch_bytes(),
        scenarios: evidence,
        unavailable_metrics: Vec::new(),
        release_blockers,
        scale_analysis,
    };
    report.write(&args.report)?;
    cleanup_successful_run(&args, &selected).await?;

    info!(report = %args.report.display(), "all hyperscale migration parity scenarios passed");
    Ok(())
}

fn remaining_suite_duration(limit: Duration, elapsed: Duration) -> Result<Duration> {
    limit
        .checked_sub(elapsed)
        .filter(|remaining| !remaining.is_zero())
        .context("aggregate suite timeout expired")
}

async fn cleanup_successful_scenario(args: &Args, scenario: Scenario) -> Result<()> {
    if args.preserve_store {
        return Ok(());
    }
    if matches!(args.storage, Storage::Minio(_)) {
        let store = build_source_store(args, &args.store_root)?;
        let (source_database, target_database) = scenario_databases(args, scenario);
        let rollback_database = scenario_rollback_database(args, scenario);
        clear_object_prefix(&store, &source_database).await?;
        clear_object_prefix(&store, &target_database).await?;
        clear_object_prefix(&store, &rollback_database).await?;
    }
    let root = args.store_root.join(scenario.name);
    if root.exists() {
        std::fs::remove_dir_all(&root).with_context(|| {
            format!(
                "failed to clean successful scenario root {}",
                root.display()
            )
        })?;
    }
    Ok(())
}

async fn cleanup_successful_run(args: &Args, scenarios: &[Scenario]) -> Result<()> {
    if args.preserve_store {
        return Ok(());
    }
    if matches!(args.storage, Storage::Minio(_)) {
        let store = build_source_store(args, &args.store_root)?;
        for scenario in scenarios {
            let (source_database, target_database) = scenario_databases(args, *scenario);
            let rollback_database = scenario_rollback_database(args, *scenario);
            clear_object_prefix(&store, &source_database).await?;
            clear_object_prefix(&store, &target_database).await?;
            clear_object_prefix(&store, &rollback_database).await?;
        }
    }
    if args.store_root.exists() {
        std::fs::remove_dir_all(&args.store_root).with_context(|| {
            format!(
                "failed to clean successful store root {}",
                args.store_root.display()
            )
        })?;
    }
    Ok(())
}

#[derive(Serialize)]
struct CrashRecoveryMatrixReport {
    schema_version: u32,
    status: &'static str,
    revisions: report::RevisionEvidence,
    config: report::HarnessConfig,
    entries: Vec<CrashRecoveryEntry>,
    total_millis: u64,
}

#[derive(Serialize)]
struct CrashRecoveryEntry {
    kind: &'static str,
    failpoint: &'static str,
    crash_millis: u64,
    recovery_millis: u64,
    crash_exit_code: Option<i32>,
    crash_signal: Option<i32>,
    recovery_report: serde_json::Value,
}

#[derive(Debug, Clone, Copy)]
struct CrashBoundary {
    kind: &'static str,
    failpoint: &'static str,
    failpoint_environment: &'static str,
    action_environment: &'static str,
}

fn run_crash_recovery_matrix(args: &Args) -> Result<()> {
    if args.target_fault.is_some() {
        bail!("--crash-recovery-matrix cannot be combined with --target-fault");
    }
    if args.resume_verification {
        bail!("--crash-recovery-matrix cannot be combined with --resume-verification");
    }
    let revisions = revision_evidence(&args.hyperscale)?;
    let started = Instant::now();
    let executable = std::env::current_exe().context("failed to locate parity executable")?;
    let child_arguments = crash_matrix_child_arguments();
    let boundaries = MigrationFailpoint::ALL
        .map(|failpoint| CrashBoundary {
            kind: "graph_migration",
            failpoint: failpoint.as_str(),
            failpoint_environment: "HELIX_MIGRATION_FAILPOINT",
            action_environment: "HELIX_MIGRATION_FAIL_ACTION",
        })
        .into_iter()
        .chain(
            migration_parity_index_outbox_failpoints().map(|failpoint| CrashBoundary {
                kind: "index_v2_outbox",
                failpoint,
                failpoint_environment: "HELIX_INDEX_OUTBOX_FAILPOINT",
                action_environment: "HELIX_INDEX_OUTBOX_FAIL_ACTION",
            }),
        )
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(boundaries.len());

    for boundary in boundaries {
        info!(
            kind = boundary.kind,
            failpoint = boundary.failpoint,
            "starting subprocess-abort recovery case"
        );
        let mut crash = Command::new(&executable);
        crash
            .args(&child_arguments)
            .env_remove("HELIX_MIGRATION_FAILPOINT")
            .env_remove("HELIX_MIGRATION_FAIL_ACTION")
            .env_remove("HELIX_INDEX_OUTBOX_FAILPOINT")
            .env_remove("HELIX_INDEX_OUTBOX_FAIL_ACTION")
            .env(boundary.failpoint_environment, boundary.failpoint)
            .env(boundary.action_environment, "abort");
        let (crash_status, crash_millis) = run_child_with_deadline(
            crash,
            args.maximum_scenario_duration,
            boundary.failpoint,
            "crash",
        )?;
        if !status_is_abort(&crash_status) {
            bail!(
                "failpoint {} exited with {crash_status} instead of SIGABRT",
                boundary.failpoint
            );
        }

        let mut recovery = Command::new(&executable);
        recovery
            .args(&child_arguments)
            .args(["--resume-verification", "--preserve-store"])
            .env_remove("HELIX_MIGRATION_FAILPOINT")
            .env_remove("HELIX_MIGRATION_FAIL_ACTION")
            .env_remove("HELIX_INDEX_OUTBOX_FAILPOINT")
            .env_remove("HELIX_INDEX_OUTBOX_FAIL_ACTION");
        let (recovery_status, recovery_millis) = run_child_with_deadline(
            recovery,
            args.maximum_scenario_duration,
            boundary.failpoint,
            "recovery",
        )?;
        if !recovery_status.success() {
            bail!(
                "recovery after failpoint {} exited with {recovery_status}",
                boundary.failpoint
            );
        }
        let recovery_report_bytes = std::fs::read(&args.report).with_context(|| {
            format!(
                "failed to read recovery report after failpoint {} from {}",
                boundary.failpoint,
                args.report.display()
            )
        })?;
        let recovery_report: serde_json::Value = serde_json::from_slice(&recovery_report_bytes)
            .with_context(|| {
                format!(
                    "recovery report after failpoint {} was not valid JSON",
                    boundary.failpoint
                )
            })?;
        if recovery_report["status"] != "resumed_verification_passed" {
            bail!(
                "recovery report after failpoint {} did not pass",
                boundary.failpoint
            );
        }
        entries.push(CrashRecoveryEntry {
            kind: boundary.kind,
            failpoint: boundary.failpoint,
            crash_millis,
            recovery_millis,
            crash_exit_code: crash_status.code(),
            crash_signal: status_signal(&crash_status),
            recovery_report,
        });
    }

    let matrix = CrashRecoveryMatrixReport {
        schema_version: 2,
        status: "crash_recovery_matrix_passed",
        revisions,
        config: harness_config(args)?,
        entries,
        total_millis: elapsed_millis(started),
    };
    if let Some(parent) = args.report.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create crash-matrix report directory {}",
                parent.display()
            )
        })?;
    }
    let temporary = args.report.with_extension("crash-matrix.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(&matrix)?)
        .with_context(|| format!("failed to write crash matrix {}", temporary.display()))?;
    std::fs::rename(&temporary, &args.report).with_context(|| {
        format!(
            "failed to publish crash matrix {} as {}",
            temporary.display(),
            args.report.display()
        )
    })?;
    info!(report = %args.report.display(), "subprocess-abort recovery matrix passed");
    Ok(())
}

fn crash_matrix_child_arguments() -> Vec<OsString> {
    let original = std::env::args_os().skip(1).collect::<Vec<_>>();
    let mut filtered = Vec::with_capacity(original.len() + 2);
    let mut index = 0;
    while index < original.len() {
        let argument = &original[index];
        if argument == "--crash-recovery-matrix"
            || argument == "--resume-verification"
            || argument == "--preserve-store"
        {
            index += 1;
            continue;
        }
        if argument == "--scenario" || argument == "--migration-failpoint" {
            index = index.saturating_add(2);
            continue;
        }
        filtered.push(argument.clone());
        index += 1;
    }
    filtered.push(OsString::from("--scenario"));
    filtered.push(OsString::from("eager"));
    filtered
}

fn run_child_with_deadline(
    mut command: Command,
    deadline: Duration,
    failpoint: &str,
    phase: &str,
) -> Result<(ExitStatus, u64)> {
    let started = Instant::now();
    let mut child = command.spawn().with_context(|| {
        format!("failed to spawn {phase} child for migration failpoint {failpoint}")
    })?;
    loop {
        if let Some(status) = child.try_wait().with_context(|| {
            format!("failed to poll {phase} child for migration failpoint {failpoint}")
        })? {
            return Ok((status, elapsed_millis(started)));
        }
        if started.elapsed() >= deadline {
            child.kill().with_context(|| {
                format!("failed to stop timed-out {phase} child for failpoint {failpoint}")
            })?;
            let _ = child.wait();
            bail!(
                "{phase} child for failpoint {failpoint} exceeded its {deadline:?} deadline and was stopped"
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(unix)]
fn status_is_abort(status: &ExitStatus) -> bool {
    status.signal() == Some(libc::SIGABRT)
}

#[cfg(not(unix))]
fn status_is_abort(status: &ExitStatus) -> bool {
    !status.success()
}

#[cfg(unix)]
fn status_signal(status: &ExitStatus) -> Option<i32> {
    status.signal()
}

#[cfg(not(unix))]
fn status_signal(_status: &ExitStatus) -> Option<i32> {
    None
}

fn harness_config(args: &Args) -> Result<report::HarnessConfig> {
    Ok(report::HarnessConfig {
        profile: args.profile.clone(),
        fixture_version: SCALE_FIXTURE_VERSION,
        database: DATABASE.to_string(),
        batch_rows: u64::try_from(args.batch_rows)?,
        batch_source_bytes: u64::try_from(MigrationTuning::DEFAULT_BATCH_BYTES)?,
        oracle_buffer_bytes_per_stream: u64::try_from(args.oracle_buffer_bytes.get())?,
        object_storage: args.storage.report_name(),
        added_latency_millis: u64::try_from(args.object_store_latency.as_millis())
            .unwrap_or(u64::MAX),
        target_fault: args
            .target_fault
            .map(|fault| format!("{:?}:{:?}:{}", fault.kind, fault.operation, fault.every)),
        migration_failpoint: args
            .migration_failpoint
            .map(|failpoint| failpoint.as_str().to_string()),
        maximum_open_attempts: u64::try_from(args.maximum_open_attempts.get())?,
        scale_nodes: args.scale_nodes,
        scale_edges: args.scale_edges,
        seed_batch_rows: u64::try_from(args.seed_batch_rows.get())?,
        distribution: args.distribution.name().to_string(),
        resume_source_seed: args.resume_source_seed,
        maximum_scenario_seconds: args.maximum_scenario_duration.as_secs(),
        maximum_suite_seconds: args.maximum_suite_duration.as_secs(),
        compaction_drain_seconds: args.compaction_drain_timeout.as_secs(),
        maximum_steady_l0_ssts: u64::try_from(args.maximum_steady_l0_ssts)?,
        definition_migration_batch_rows: u64::try_from(args.batch_rows)?,
    })
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        "hyperscale_migration_parity=info,db::migrations=info,db=warn,helix=warn,slatedb=warn"
            .into()
    });
    let subscriber = Registry::default()
        .with(filter)
        .with(CompactionErrorLayer)
        .with(tracing_subscriber::fmt::layer());
    let _ = tracing::subscriber::set_global_default(subscriber);
}

fn compaction_error_count() -> usize {
    COMPACTION_ERRORS.lock().map_or(0, |errors| errors.len())
}

fn compaction_errors_since(start: usize) -> report::CompactionErrorEvidence {
    let Ok(errors) = COMPACTION_ERRORS.lock() else {
        return report::CompactionErrorEvidence {
            count: 1,
            first_errors: vec!["compaction error recorder mutex was poisoned".to_string()],
        };
    };
    let observed = errors.len().saturating_sub(start);
    report::CompactionErrorEvidence {
        count: u64::try_from(observed).unwrap_or(u64::MAX),
        first_errors: errors
            .iter()
            .skip(start)
            .take(FIRST_COMPACTION_ERROR_LIMIT)
            .cloned()
            .collect(),
    }
}

fn parse_args() -> Result<Args> {
    let mut hyperscale = PathBuf::from("../helix-hyperscale-xav");
    let mut store_root = PathBuf::from("/tmp/helix-migration-parity");
    let mut batch_rows = 2_usize;
    let mut preserve_store = false;
    let mut oracle_buffer_mib = 64_usize;
    let mut report = None;
    let mut object_store_latency_millis = 0_u64;
    let mut minio_endpoint = None;
    let mut minio_bucket = "helix-migration-parity".to_string();
    let mut minio_run_prefix = "release-rehearsal".to_string();
    let mut target_fault = None;
    let mut maximum_open_attempts = NonZeroUsize::new(10).expect("ten is nonzero");
    let mut scale_nodes = 0_u64;
    let mut scale_edges = 0_u64;
    let mut seed_batch_rows = NonZeroUsize::new(10_000).expect("ten thousand is nonzero");
    let mut distribution = GraphDistribution::Uniform;
    let mut scenario_filter = None;
    let mut maximum_scenario_seconds = 8_u64 * 60 * 60;
    let mut maximum_suite_seconds = 24_u64 * 60 * 60;
    let mut profile = "custom".to_string();
    let mut scale_baseline_reports = Vec::new();
    let mut project_next_rows = None;
    let mut resume_verification = false;
    let mut resume_source_seed = false;
    let mut compaction_drain_seconds = 0_u64;
    let mut maximum_steady_l0_ssts = 4_usize;
    let mut migration_failpoint = None;
    let mut crash_recovery_matrix = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--hyperscale" => {
                let Some(value) = args.next() else {
                    bail!("--hyperscale requires a path");
                };
                hyperscale = PathBuf::from(value);
            }
            "--store-root" => {
                let Some(value) = args.next() else {
                    bail!("--store-root requires a path");
                };
                store_root = PathBuf::from(value);
            }
            "--batch-rows" => {
                let Some(value) = args.next() else {
                    bail!("--batch-rows requires a positive integer");
                };
                batch_rows = value
                    .parse::<usize>()
                    .context("--batch-rows must be a positive integer")?;
                if batch_rows == 0 {
                    bail!("--batch-rows must be positive");
                }
            }
            "--preserve-store" => preserve_store = true,
            "--report" => {
                let Some(value) = args.next() else {
                    bail!("--report requires a path");
                };
                report = Some(PathBuf::from(value));
            }
            "--oracle-buffer-mib" => {
                let Some(value) = args.next() else {
                    bail!("--oracle-buffer-mib requires a positive integer");
                };
                oracle_buffer_mib = value
                    .parse::<usize>()
                    .context("--oracle-buffer-mib must be a positive integer")?;
                if oracle_buffer_mib == 0 {
                    bail!("--oracle-buffer-mib must be positive");
                }
            }
            "--object-store-latency-ms" => {
                let Some(value) = args.next() else {
                    bail!("--object-store-latency-ms requires a non-negative integer");
                };
                object_store_latency_millis = value
                    .parse::<u64>()
                    .context("--object-store-latency-ms must be a non-negative integer")?;
            }
            "--minio-endpoint" => {
                let Some(value) = args.next() else {
                    bail!("--minio-endpoint requires a URL");
                };
                minio_endpoint = Some(value);
            }
            "--minio-bucket" => {
                let Some(value) = args.next() else {
                    bail!("--minio-bucket requires a bucket name");
                };
                minio_bucket = value;
            }
            "--minio-run-prefix" => {
                let Some(value) = args.next() else {
                    bail!("--minio-run-prefix requires an object prefix");
                };
                minio_run_prefix = value;
            }
            "--target-fault" => {
                let Some(value) = args.next() else {
                    bail!("--target-fault requires [KIND:]OPERATION:EVERY");
                };
                let fields = value.split(':').collect::<Vec<_>>();
                let (kind, operation, every) = match fields.as_slice() {
                    [operation, every] => (FaultKind::Transient, *operation, *every),
                    [kind, operation, every] => {
                        let Some(kind) = FaultKind::parse(kind) else {
                            bail!(
                                "unknown target fault kind {kind}; expected transient, timeout, throttled, or connection-loss"
                            );
                        };
                        (kind, *operation, *every)
                    }
                    _ => bail!("--target-fault requires [KIND:]OPERATION:EVERY"),
                };
                let Some(operation) = Operation::parse(operation) else {
                    bail!(
                        "unknown target fault operation {operation}; expected get, head, put, multipart, list, delete, or copy"
                    );
                };
                let every = every
                    .parse::<u64>()
                    .context("target fault interval must be a positive integer")?;
                target_fault = Some(TargetFault {
                    kind,
                    operation,
                    every: NonZeroU64::new(every)
                        .context("target fault interval must be positive")?,
                });
            }
            "--maximum-open-attempts" => {
                let Some(value) = args.next() else {
                    bail!("--maximum-open-attempts requires a positive integer");
                };
                maximum_open_attempts = NonZeroUsize::new(
                    value
                        .parse::<usize>()
                        .context("--maximum-open-attempts must be a positive integer")?,
                )
                .context("--maximum-open-attempts must be positive")?;
            }
            "--scale-nodes" => {
                let Some(value) = args.next() else {
                    bail!("--scale-nodes requires a non-negative integer");
                };
                scale_nodes = value.parse().context("--scale-nodes must be an integer")?;
            }
            "--scale-edges" => {
                let Some(value) = args.next() else {
                    bail!("--scale-edges requires a non-negative integer");
                };
                scale_edges = value.parse().context("--scale-edges must be an integer")?;
            }
            "--seed-batch-rows" => {
                let Some(value) = args.next() else {
                    bail!("--seed-batch-rows requires a positive integer");
                };
                seed_batch_rows = NonZeroUsize::new(
                    value
                        .parse()
                        .context("--seed-batch-rows must be an integer")?,
                )
                .context("--seed-batch-rows must be positive")?;
            }
            "--distribution" => {
                let Some(value) = args.next() else {
                    bail!("--distribution requires a name");
                };
                let Some(parsed) = GraphDistribution::parse(&value) else {
                    bail!(
                        "unknown distribution {value}; expected uniform, power-law, star, dense, self-loop, or hot-pair"
                    );
                };
                distribution = parsed;
            }
            "--scenario" => {
                let Some(value) = args.next() else {
                    bail!("--scenario requires eager, lazy, adaptive, or all");
                };
                scenario_filter = match value.as_str() {
                    "all" => None,
                    "eager" => Some("eager"),
                    "lazy" => Some("lazy"),
                    "adaptive" => Some("adaptive"),
                    _ => bail!("--scenario requires eager, lazy, adaptive, or all"),
                };
            }
            "--maximum-scenario-seconds" => {
                let Some(value) = args.next() else {
                    bail!("--maximum-scenario-seconds requires a positive integer");
                };
                maximum_scenario_seconds = value
                    .parse()
                    .context("--maximum-scenario-seconds must be an integer")?;
                if maximum_scenario_seconds == 0 {
                    bail!("--maximum-scenario-seconds must be positive");
                }
            }
            "--maximum-suite-seconds" => {
                let Some(value) = args.next() else {
                    bail!("--maximum-suite-seconds requires a positive integer");
                };
                maximum_suite_seconds = value
                    .parse()
                    .context("--maximum-suite-seconds must be an integer")?;
                if maximum_suite_seconds == 0 {
                    bail!("--maximum-suite-seconds must be positive");
                }
            }
            "--profile" => {
                profile = args.next().context("--profile requires a name")?;
                if profile.is_empty() {
                    bail!("--profile requires a non-empty name");
                }
            }
            "--scale-baseline-report" => {
                let Some(value) = args.next() else {
                    bail!("--scale-baseline-report requires a report path");
                };
                scale_baseline_reports.push(PathBuf::from(value));
            }
            "--project-next-rows" => {
                let value = args
                    .next()
                    .context("--project-next-rows requires an integer")?;
                let rows = value
                    .parse::<u64>()
                    .context("--project-next-rows must be an integer")?;
                if rows == 0 {
                    bail!("--project-next-rows must be positive");
                }
                project_next_rows = Some(rows);
            }
            "--resume-verification" => resume_verification = true,
            "--resume-source-seed" => resume_source_seed = true,
            "--compaction-drain-seconds" => {
                let Some(value) = args.next() else {
                    bail!("--compaction-drain-seconds requires a non-negative integer");
                };
                compaction_drain_seconds = value
                    .parse()
                    .context("--compaction-drain-seconds must be an integer")?;
            }
            "--maximum-steady-l0-ssts" => {
                let Some(value) = args.next() else {
                    bail!("--maximum-steady-l0-ssts requires a non-negative integer");
                };
                maximum_steady_l0_ssts = value
                    .parse()
                    .context("--maximum-steady-l0-ssts must be an integer")?;
            }
            "--migration-failpoint" => {
                let Some(value) = args.next() else {
                    bail!("--migration-failpoint requires a failpoint name");
                };
                let Some(failpoint) = MigrationFailpoint::parse(&value) else {
                    bail!(
                        "unknown migration failpoint {value}; expected one of {}",
                        MigrationFailpoint::ALL
                            .iter()
                            .map(|failpoint| failpoint.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                };
                migration_failpoint = Some(failpoint);
            }
            "--crash-recovery-matrix" => crash_recovery_matrix = true,
            "--help" | "-h" => {
                println!(
                    "usage: cargo run --manifest-path tools/hyperscale-migration-parity/Cargo.toml -- [--hyperscale PATH] [--store-root PATH] [--batch-rows N] [--oracle-buffer-mib N] [--object-store-latency-ms N] [--minio-endpoint URL] [--minio-bucket NAME] [--minio-run-prefix PREFIX] [--target-fault [KIND:]OPERATION:EVERY] [--migration-failpoint NAME] [--crash-recovery-matrix] [--maximum-open-attempts N] [--scale-nodes N] [--scale-edges N] [--seed-batch-rows N] [--distribution NAME] [--scenario MODE] [--maximum-scenario-seconds N] [--scale-baseline-report PATH] [--resume-verification] [--resume-source-seed] [--compaction-drain-seconds N] [--maximum-steady-l0-ssts N] [--report PATH] [--preserve-store]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let report = report.unwrap_or_else(|| store_root.join("migration-report.json"));
    let storage = match minio_endpoint {
        None => Storage::Local,
        Some(endpoint) => Storage::Minio(MinioConfig {
            endpoint,
            bucket: minio_bucket,
            access_key: std::env::var("MINIO_ROOT_USER")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            secret_key: std::env::var("MINIO_ROOT_PASSWORD")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            run_prefix: minio_run_prefix.trim_matches('/').to_string(),
        }),
    };
    if scale_edges > 0 && scale_nodes == 0 {
        bail!("--scale-edges requires --scale-nodes to be positive");
    }
    if resume_source_seed && !preserve_store {
        bail!("--resume-source-seed requires --preserve-store");
    }
    if resume_source_seed && resume_verification {
        bail!("--resume-source-seed and --resume-verification are mutually exclusive");
    }
    Ok(Args {
        hyperscale,
        store_root,
        batch_rows,
        oracle_buffer_bytes: NonZeroUsize::new(
            oracle_buffer_mib
                .checked_mul(1024 * 1024)
                .context("--oracle-buffer-mib overflows usize")?,
        )
        .expect("positive MiB count produces a positive byte count"),
        preserve_store,
        report,
        object_store_latency: Duration::from_millis(object_store_latency_millis),
        storage,
        target_fault,
        maximum_open_attempts,
        scale_nodes,
        scale_edges,
        seed_batch_rows,
        distribution,
        scenario_filter,
        maximum_scenario_duration: Duration::from_secs(maximum_scenario_seconds),
        maximum_suite_duration: Duration::from_secs(maximum_suite_seconds),
        profile,
        scale_baseline_reports,
        project_next_rows,
        resume_verification,
        resume_source_seed,
        compaction_drain_timeout: Duration::from_secs(compaction_drain_seconds),
        maximum_steady_l0_ssts,
        migration_failpoint,
        crash_recovery_matrix,
    })
}

fn revision_evidence(path: &Path) -> Result<report::RevisionEvidence> {
    let source_hyperscale = git_value(path, &["rev-parse", "HEAD"])?;
    if source_hyperscale != SOURCE_HYPERSCALE_REVISION {
        bail!("expected hyperscale revision {SOURCE_HYPERSCALE_REVISION}, got {source_hyperscale}");
    }
    let target_helix = git_value(Path::new("."), &["rev-parse", "HEAD"])?;
    let source_hyperscale_dirty = !git_value(path, &["status", "--porcelain"])?.is_empty();
    let target_helix_dirty = !git_value(Path::new("."), &["status", "--porcelain"])?.is_empty();
    info!(
        hyperscale = %path.display(),
        source_revision = source_hyperscale,
        target_revision = target_helix,
        source_dirty = source_hyperscale_dirty,
        target_dirty = target_helix_dirty,
        "verified exact migration revisions"
    );
    Ok(report::RevisionEvidence {
        target_helix,
        target_helix_dirty,
        source_slatedb: format!("hyperscale-subtree@{source_hyperscale}"),
        source_hyperscale,
        source_hyperscale_dirty,
        target_slatedb: target_slatedb_revision_from_lock(include_str!("../Cargo.lock"))?,
    })
}

fn target_slatedb_revision_from_lock(lock: &str) -> Result<String> {
    let lock: toml::Value = lock
        .parse()
        .context("parity Cargo.lock is not valid TOML")?;
    let packages = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .context("parity Cargo.lock has no package array")?;
    let candidates = packages
        .iter()
        .filter(|package| {
            package.get("name").and_then(toml::Value::as_str) == Some("slatedb")
                && package.get("version").and_then(toml::Value::as_str)
                    == Some(TARGET_SLATEDB_VERSION)
        })
        .collect::<Vec<_>>();
    let [package] = candidates.as_slice() else {
        bail!(
            "expected exactly one locked slatedb {TARGET_SLATEDB_VERSION} package, found {}",
            candidates.len()
        );
    };
    let source = package
        .get("source")
        .and_then(toml::Value::as_str)
        .context("locked target SlateDB package has no Git source")?;
    let pinned = source
        .strip_prefix(TARGET_SLATEDB_GIT_SOURCE)
        .with_context(|| {
            format!("locked target SlateDB package has an unexpected non-Git source: {source}")
        })?;
    let (requested, resolved) = pinned
        .split_once('#')
        .context("locked target SlateDB Git source has no resolved revision")?;
    let valid_revision = |revision: &str| {
        revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    };
    if !valid_revision(requested) || !valid_revision(resolved) || requested != resolved {
        bail!("locked target SlateDB Git source is not pinned to one exact 40-hex revision");
    }
    Ok(resolved.to_string())
}

fn git_value(path: &Path, arguments: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(arguments)
        .output()
        .with_context(|| format!("failed to inspect hyperscale checkout {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "failed to inspect hyperscale checkout {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout)
        .context("git output was not utf-8")
        .map(|value| value.trim().to_string())
}

async fn run_scenario(args: &Args, scenario: Scenario) -> Result<report::ScenarioEvidence> {
    let total_started = Instant::now();
    let compaction_error_start = compaction_error_count();
    let root = args.store_root.join(scenario.name);
    let source_root = root.join("source");
    let target_root = root.join("target");
    let rollback_root = root.join("rollback");
    let oracle_root = root.join("oracle");
    if root.exists() && !args.preserve_store {
        std::fs::remove_dir_all(&root)
            .with_context(|| format!("failed to remove scenario root {}", root.display()))?;
    }
    std::fs::create_dir_all(&root)
        .with_context(|| format!("failed to create scenario root {}", root.display()))?;
    std::fs::create_dir_all(&source_root).with_context(|| {
        format!(
            "failed to create immutable source root {}",
            source_root.display()
        )
    })?;
    std::fs::create_dir_all(&rollback_root).with_context(|| {
        format!(
            "failed to create rollback restore root {}",
            rollback_root.display()
        )
    })?;
    std::fs::create_dir_all(&oracle_root)
        .with_context(|| format!("failed to create oracle root {}", oracle_root.display()))?;

    info!(
        scenario = scenario.name,
        root = %root.display(),
        batch_rows = args.batch_rows,
        oracle_buffer_bytes = args.oracle_buffer_bytes.get(),
        "starting parity scenario"
    );

    let (source_database, target_database) = scenario_databases(args, scenario);
    let rollback_database = scenario_rollback_database(args, scenario);
    let source_raw_store = build_source_store(args, &source_root)?;
    if matches!(args.storage, Storage::Minio(_)) && !args.preserve_store {
        clear_object_prefix(&source_raw_store, &source_database).await?;
        clear_object_prefix(&source_raw_store, &target_database).await?;
        clear_object_prefix(&source_raw_store, &rollback_database).await?;
    }
    let source_store_metrics =
        ObjectStoreRecorder::new(FaultPolicy::latency(args.object_store_latency));
    let source_store: Arc<dyn object_store::ObjectStore> = Arc::new(InstrumentedStore012::new(
        source_raw_store,
        Arc::clone(&source_store_metrics),
    ));
    let heartbeat = ProgressHeartbeat::start(
        "source_seed",
        args.scale_nodes.saturating_add(args.scale_edges),
        args.store_root.clone(),
        Arc::clone(&source_store_metrics),
    );
    let source_initial_metrics = source_store_metrics.snapshot();
    let hyperscale = open_hyperscale(
        &source_database,
        scenario.edge_policy,
        Arc::clone(&source_store),
    )
    .await?;
    let seed_started = Instant::now();
    seed_legacy_index_definitions(&hyperscale).await?;
    let source_seed_resume = seed_hyperscale(&hyperscale, scenario, args, &heartbeat).await?;
    seed_blob_fixture(&source_store, &source_database).await?;
    let source_semantics = source_semantic_evidence(&hyperscale).await?;
    write_source_semantic_evidence(&root, &source_semantics)?;
    let seed_millis = elapsed_millis(seed_started);
    let source_after_seed_metrics = source_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "source_seed_complete",
        total_started,
        serde_json::json!({
            "source_object_store": source_after_seed_metrics,
            "source_seed_resume": source_seed_resume,
        }),
    )?;
    let oracle_paths = logical_oracle::OraclePaths::new(&oracle_root);
    let source_oracle_started = Instant::now();
    heartbeat.set_phase("source_oracle");
    let source_oracle = logical_oracle::build_source(
        &hyperscale,
        &oracle_root,
        &oracle_paths,
        args.oracle_buffer_bytes,
    )
    .await
    .context("failed to build bounded source oracle")?;
    let source_oracle_millis = elapsed_millis(source_oracle_started);
    let source_after_oracle_metrics = source_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "source_oracle_complete",
        total_started,
        serde_json::json!({
            "source_oracle": source_oracle,
            "source_object_store": source_after_oracle_metrics,
        }),
    )?;
    hyperscale
        .close()
        .await
        .context("failed to close hyperscale db")?;
    let source_durable_reopen_started = Instant::now();
    heartbeat.set_phase("source_durable_reopen");
    let reopened_source_oracle_root = oracle_root.join("reopened-source");
    std::fs::create_dir_all(&reopened_source_oracle_root).with_context(|| {
        format!(
            "failed to create reopened source oracle root {}",
            reopened_source_oracle_root.display()
        )
    })?;
    let reopened_source_paths = logical_oracle::OraclePaths::new(&reopened_source_oracle_root);
    let reopened_source = open_hyperscale(
        &source_database,
        scenario.edge_policy,
        Arc::clone(&source_store),
    )
    .await
    .context("failed to reopen the source from durable object storage")?;
    let reopened_source_oracle = logical_oracle::build_source(
        &reopened_source,
        &reopened_source_oracle_root,
        &reopened_source_paths,
        args.oracle_buffer_bytes,
    )
    .await
    .context("failed to build the reopened durable source oracle")?;
    let source_durability =
        logical_oracle::compare_source_durability(&oracle_paths, &reopened_source_paths)?;
    if !source_durability.is_equal() {
        bail!(
            "{}: source changed after close and durable reopen: {}",
            scenario.name,
            serde_json::to_string_pretty(&source_durability)?
        );
    }
    let reopened_source_semantics = source_semantic_evidence(&reopened_source).await?;
    if reopened_source_semantics != source_semantics {
        bail!(
            "{}: source semantic queries changed after durable reopen: live={source_semantics:?}, reopened={reopened_source_semantics:?}",
            scenario.name,
        );
    }
    reopened_source
        .close()
        .await
        .context("failed to close durable source reader")?;
    let source_durable_reopen_millis = elapsed_millis(source_durable_reopen_started);
    let source_after_durable_reopen_metrics = source_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "source_durable_reopen_complete",
        total_started,
        serde_json::json!({
            "reopened_source_oracle": reopened_source_oracle,
            "source_durability": source_durability,
            "source_object_store": source_after_durable_reopen_metrics,
        }),
    )?;

    let source_garbage_collection_started = Instant::now();
    heartbeat.set_phase("source_garbage_collection");
    let source_gc_storage_before =
        source_object_prefix_size(&source_store, &source_database).await?;
    let source_gc_checkpoints = run_source_garbage_collection(&source_database, &source_store)
        .await
        .context("source foreground SlateDB garbage collection failed")?;
    let source_gc_storage_after =
        source_object_prefix_size(&source_store, &source_database).await?;
    let post_gc_source_oracle_root = oracle_root.join("post-gc-source");
    std::fs::create_dir_all(&post_gc_source_oracle_root).with_context(|| {
        format!(
            "failed to create post-GC source oracle root {}",
            post_gc_source_oracle_root.display()
        )
    })?;
    let post_gc_source_paths = logical_oracle::OraclePaths::new(&post_gc_source_oracle_root);
    let post_gc_source = open_hyperscale(
        &source_database,
        scenario.edge_policy,
        Arc::clone(&source_store),
    )
    .await
    .context("failed to cold-reopen the source after garbage collection")?;
    let post_gc_source_oracle = logical_oracle::build_source(
        &post_gc_source,
        &post_gc_source_oracle_root,
        &post_gc_source_paths,
        args.oracle_buffer_bytes,
    )
    .await
    .context("failed to build the post-GC source oracle")?;
    let source_gc_durability =
        logical_oracle::compare_source_durability(&oracle_paths, &post_gc_source_paths)?;
    if !source_gc_durability.is_equal() {
        bail!(
            "{}: source changed after foreground garbage collection: {}",
            scenario.name,
            serde_json::to_string_pretty(&source_gc_durability)?
        );
    }
    let post_gc_source_semantics = source_semantic_evidence(&post_gc_source).await?;
    if post_gc_source_semantics != source_semantics {
        bail!(
            "{}: source semantic queries changed after foreground garbage collection: before={source_semantics:?}, after={post_gc_source_semantics:?}",
            scenario.name,
        );
    }
    post_gc_source
        .close()
        .await
        .context("failed to close the post-GC source reader")?;
    let source_compaction_jobs =
        capture_source_compaction_statuses(&source_database, &source_store).await?;
    let inherited_compaction_ids = source_compaction_jobs
        .iter()
        .map(|compaction| compaction.id.as_str())
        .collect::<BTreeSet<_>>();
    let source_garbage_collection_millis = elapsed_millis(source_garbage_collection_started);
    let source_garbage_collection = report::GarbageCollectionPassEvidence {
        phase: "source_before_immutable_copy",
        elapsed_millis: source_garbage_collection_millis,
        checkpoint_clock_advance_millis: source_gc_checkpoints.clock_advance_millis,
        checkpoints_before: source_gc_checkpoints.checkpoints,
        expiring_checkpoints_before: source_gc_checkpoints.expiring_checkpoints,
        permanent_checkpoints_before: source_gc_checkpoints.permanent_checkpoints,
        storage_before_bytes: source_gc_storage_before,
        storage_after_bytes: source_gc_storage_after,
        reclaimed_bytes: source_gc_storage_before.saturating_sub(source_gc_storage_after),
        cold_reopen_passed: true,
    };
    let source_after_garbage_collection_metrics = source_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "source_garbage_collection_complete",
        total_started,
        serde_json::json!({
            "garbage_collection": source_garbage_collection,
            "post_gc_source_oracle": post_gc_source_oracle,
            "source_gc_durability": source_gc_durability,
            "source_object_store": source_after_garbage_collection_metrics,
        }),
    )?;
    if !args.preserve_store {
        std::fs::remove_dir_all(&reopened_source_oracle_root).with_context(|| {
            format!(
                "failed to remove verified reopened source oracle {}",
                reopened_source_oracle_root.display()
            )
        })?;
        std::fs::remove_dir_all(&post_gc_source_oracle_root).with_context(|| {
            format!(
                "failed to remove verified post-GC source oracle {}",
                post_gc_source_oracle_root.display()
            )
        })?;
    }
    let copy_started = Instant::now();
    match &args.storage {
        Storage::Local => {
            copy_directory(&source_root, &target_root)
                .context("failed to copy immutable source store")?;
        }
        Storage::Minio(_) => {
            copy_object_prefix(&source_store, &source_database, &target_database)
                .await
                .context("failed to copy immutable source object prefix")?;
        }
    }
    let copy_millis = elapsed_millis(copy_started);
    let source_after_copy_metrics = source_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "immutable_copy_complete",
        total_started,
        serde_json::json!({"source_object_store": source_after_copy_metrics}),
    )?;

    let target_policy = args.target_fault.map_or_else(
        || FaultPolicy::latency(args.object_store_latency),
        |fault| {
            FaultPolicy::failing(
                args.object_store_latency,
                fault.kind,
                fault.operation,
                fault.every,
            )
        },
    );
    let target_store_metrics = ObjectStoreRecorder::new(target_policy);
    heartbeat.add_object_store(Arc::clone(&target_store_metrics));
    heartbeat.set_phase("target_rewrite");
    let target_store: Arc<dyn object_store_014::ObjectStore> = Arc::new(InstrumentedStore014::new(
        build_target_store(args, &target_root)?,
        Arc::clone(&target_store_metrics),
    ));
    let target_initial_metrics = target_store_metrics.snapshot();
    let tuning = MigrationTuning::default()
        .with_worker_mode(MigrationWorkerMode::Disabled)
        .with_batch_rows(
            MigrationBatchRows::new(args.batch_rows)
                .expect("batch rows were validated as positive"),
        );
    if let Some(failpoint) = args.migration_failpoint {
        db::migrations::inject_migration_failpoint_once(failpoint)?;
    }
    let rewrite_started = Instant::now();
    let mut open_attempts = 0_u64;
    let mut migration_failpoint_retries = 0_u64;
    let migrated = loop {
        open_attempts = open_attempts.saturating_add(1);
        let result = HelixDB::open_with_object_store_for_migration_parity(
            &target_database,
            Arc::clone(&target_store),
            DbConfig::new().with_migration_tuning(tuning),
        )
        .await;
        match result {
            Ok(database) => break database,
            Err(error)
                if (args.target_fault.is_some() || args.migration_failpoint.is_some())
                    && open_attempts
                        < u64::try_from(args.maximum_open_attempts.get()).unwrap_or(u64::MAX) =>
            {
                if args.migration_failpoint.is_some()
                    && db::migrations::migration_failpoint_was_triggered()
                {
                    migration_failpoint_retries = migration_failpoint_retries.saturating_add(1);
                }
                warn!(
                    scenario = scenario.name,
                    open_attempts,
                    error = %error,
                    "retrying migration target open after injected object-store failure"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error).context("failed to open migrated db"),
        }
    };
    let rewrite_millis = elapsed_millis(rewrite_started);
    let target_after_rewrite_metrics = target_store_metrics.snapshot();

    let rewrite_jobs = migrated
        .migration_parity_job_statuses()
        .await
        .context("failed to read post-rewrite migration jobs")?;
    assert_rewrite_job_completed(scenario.name, &rewrite_jobs)?;
    let lsm_after_rewrite = capture_slatedb_lsm(&migrated)?;
    write_phase_checkpoint(
        &root,
        "blocking_rewrite_complete",
        total_started,
        serde_json::json!({
            "migration_jobs": rewrite_jobs,
            "slatedb_lsm": lsm_after_rewrite,
            "target_object_store": target_after_rewrite_metrics,
        }),
    )?;

    let cleanup_started = Instant::now();
    let mut cleanup_steps = 0_u64;
    let maximum_cleanup_steps = 10_000_u64.max(
        args.scale_edges
            .div_ceil(u64::try_from(args.batch_rows)?)
            .saturating_add(100),
    );
    loop {
        let worked = match migrated.process_migration_once().await {
            Ok(worked) => worked,
            Err(error)
                if args.migration_failpoint.is_some()
                    && migration_failpoint_retries == 0
                    && db::migrations::migration_failpoint_was_triggered() =>
            {
                migration_failpoint_retries = migration_failpoint_retries.saturating_add(1);
                warn!(
                    scenario = scenario.name,
                    error = %error,
                    "retrying cleanup from its durable checkpoint after injected migration failure"
                );
                continue;
            }
            Err(error) => return Err(error).context("failed to step cleanup migration"),
        };
        if !worked {
            break;
        }
        cleanup_steps = cleanup_steps.saturating_add(1);
        if cleanup_steps > maximum_cleanup_steps {
            bail!("cleanup did not converge for scenario {}", scenario.name);
        }
    }
    let cleanup_millis = elapsed_millis(cleanup_started);
    let target_after_cleanup_metrics = target_store_metrics.snapshot();

    let migration_jobs = migrated
        .migration_parity_job_statuses()
        .await
        .context("failed to read post-cleanup migration jobs")?;
    assert_job_statuses_completed(scenario.name, &migration_jobs)?;
    let lsm_after_cleanup = capture_slatedb_lsm(&migrated)?;
    write_phase_checkpoint(
        &root,
        "cleanup_complete",
        total_started,
        serde_json::json!({
            "cleanup_steps": cleanup_steps,
            "migration_jobs": migration_jobs,
            "slatedb_lsm": lsm_after_cleanup,
            "target_object_store": target_after_cleanup_metrics,
        }),
    )?;
    let definition_migration_steps = 0_u64;
    let definition_migration_millis = 0_u64;
    let definition_migration_active = migrated.migration_parity_definition_migration_active();
    if !definition_migration_active {
        bail!(
            "{}: automatic legacy definition migration did not publish User.tier",
            scenario.name
        );
    }
    let target_after_definition_migration_metrics = target_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "definition_migration_complete",
        total_started,
        serde_json::json!({
            "steps": definition_migration_steps,
            "active": definition_migration_active,
            "target_object_store": target_after_definition_migration_metrics,
        }),
    )?;
    let reopen_started = Instant::now();
    migrated
        .close()
        .await
        .context("failed to close migrated db before cold verification")?;
    let migrated = HelixDB::open_with_object_store_for_migration_parity(
        &target_database,
        Arc::clone(&target_store),
        DbConfig::new().with_migration_tuning(tuning),
    )
    .await
    .context("failed to reopen migrated db for cold verification")?;
    if migrated.process_migration_once().await? {
        bail!(
            "{}: repeated open found unexpected migration work",
            scenario.name
        );
    }
    assert_job_statuses_completed(
        scenario.name,
        &migrated.migration_parity_job_statuses().await?,
    )?;
    if !migrated.migration_parity_definition_migration_active() {
        bail!(
            "{}: automatic User.tier definition migration was not durable across reopen",
            scenario.name
        );
    }
    let lsm_after_reopen = capture_slatedb_lsm(&migrated)?;
    let reopen_millis = elapsed_millis(reopen_started);
    let target_after_reopen_metrics = target_store_metrics.snapshot();
    let initial_garbage_collection_started = Instant::now();
    let initial_gc_storage_before = object_prefix_size(&target_store, &target_database).await?;
    let initial_gc_checkpoints = run_target_garbage_collection(
        &target_database,
        &target_store,
        CheckpointExpiryMode::RealTime,
    )
    .await
    .context("initial foreground SlateDB garbage collection failed")?;
    let initial_gc_storage_after = object_prefix_size(&target_store, &target_database).await?;
    migrated
        .close()
        .await
        .context("failed to close migrated db after initial garbage collection")?;
    let migrated = HelixDB::open_with_object_store_for_migration_parity(
        &target_database,
        Arc::clone(&target_store),
        DbConfig::new().with_migration_tuning(tuning),
    )
    .await
    .context("failed to reopen migrated db after initial garbage collection")?;
    if migrated.process_migration_once().await? {
        bail!(
            "{}: post-garbage-collection reopen found unexpected migration work",
            scenario.name
        );
    }
    assert_job_statuses_completed(
        scenario.name,
        &migrated.migration_parity_job_statuses().await?,
    )?;
    if !migrated.migration_parity_definition_migration_active() {
        bail!(
            "{}: User.tier definition migration did not survive initial garbage collection",
            scenario.name
        );
    }
    let initial_garbage_collection_millis = elapsed_millis(initial_garbage_collection_started);
    let initial_garbage_collection = report::GarbageCollectionPassEvidence {
        phase: "before_parity_oracle",
        elapsed_millis: initial_garbage_collection_millis,
        checkpoint_clock_advance_millis: initial_gc_checkpoints.clock_advance_millis,
        checkpoints_before: initial_gc_checkpoints.checkpoints,
        expiring_checkpoints_before: initial_gc_checkpoints.expiring_checkpoints,
        permanent_checkpoints_before: initial_gc_checkpoints.permanent_checkpoints,
        storage_before_bytes: initial_gc_storage_before,
        storage_after_bytes: initial_gc_storage_after,
        reclaimed_bytes: initial_gc_storage_before.saturating_sub(initial_gc_storage_after),
        cold_reopen_passed: true,
    };
    let target_after_initial_gc_metrics = target_store_metrics.snapshot();
    let lsm_after_initial_gc = capture_slatedb_lsm(&migrated)?;
    let adoption_snapshot = migrated.migration_parity_index_state().await?;
    assert_vector_adoption_evidence(scenario.name, &source_semantics, &adoption_snapshot)?;
    write_phase_checkpoint(
        &root,
        "initial_garbage_collection_complete",
        total_started,
        serde_json::json!({
            "garbage_collection": initial_garbage_collection,
            "slatedb_lsm": lsm_after_initial_gc,
            "target_object_store": target_after_initial_gc_metrics,
        }),
    )?;
    let target_oracle_started = Instant::now();
    heartbeat.set_phase("target_oracle");
    let target_oracle = logical_oracle::build_target(
        &migrated,
        &oracle_root,
        &oracle_paths,
        args.oracle_buffer_bytes,
    )
    .await
    .context("failed to build bounded target oracle")?;
    let target_oracle_millis = elapsed_millis(target_oracle_started);
    let oracle_comparison = logical_oracle::compare(&oracle_paths)?;
    if !oracle_comparison.is_equal() {
        bail!(
            "{}: streaming parity oracle found differences: {}",
            scenario.name,
            serde_json::to_string_pretty(&oracle_comparison)?
        );
    }
    let (text_query, vector_query) =
        verify_semantic_queries(&migrated, scenario.name, source_semantics.clone()).await?;
    verify_blob_fixture(&target_store, &target_database).await?;
    let target_after_oracle_metrics = target_store_metrics.snapshot();
    let lsm_after_oracle = capture_slatedb_lsm(&migrated)?;
    write_phase_checkpoint(
        &root,
        "target_oracle_complete",
        total_started,
        serde_json::json!({
            "target_oracle": target_oracle,
            "comparison": oracle_comparison,
            "slatedb_lsm": lsm_after_oracle,
            "target_object_store": target_after_oracle_metrics,
        }),
    )?;
    let post_migration_crud_started = Instant::now();
    heartbeat.set_phase("post_migration_crud");
    let post_migration_crud = migrated
        .migration_parity_run_crud_rehearsal()
        .await
        .context("post-migration CRUD rehearsal failed")?;
    let post_migration_crud_millis = elapsed_millis(post_migration_crud_started);
    let target_after_crud_metrics = target_store_metrics.snapshot();
    let lsm_after_crud = capture_slatedb_lsm(&migrated)?;
    write_phase_checkpoint(
        &root,
        "post_migration_crud_complete",
        total_started,
        serde_json::json!({
            "post_migration_crud": post_migration_crud,
            "slatedb_lsm": lsm_after_crud,
            "target_object_store": target_after_crud_metrics,
        }),
    )?;

    migrated
        .close()
        .await
        .context("failed to close migrated db after CRUD rehearsal")?;
    let post_crud_reopen_started = Instant::now();
    let migrated = HelixDB::open_with_object_store_for_migration_parity(
        &target_database,
        Arc::clone(&target_store),
        DbConfig::new().with_migration_tuning(tuning),
    )
    .await
    .context("failed to reopen migrated db after CRUD rehearsal")?;
    if migrated.process_migration_once().await? {
        bail!(
            "{}: post-CRUD reopen found unexpected migration work",
            scenario.name
        );
    }
    let post_crud_reopen = migrated
        .migration_parity_crud_query_corpus(&post_migration_crud.state)
        .await
        .context("post-CRUD cold-reopen query corpus failed")?;
    if post_crud_reopen != post_migration_crud.after_delete {
        bail!(
            "{}: post-CRUD query corpus changed across cold reopen: before={:?}, after={post_crud_reopen:?}",
            scenario.name,
            post_migration_crud.after_delete
        );
    }
    let post_crud_reopen_millis = elapsed_millis(post_crud_reopen_started);
    let target_after_crud_reopen_metrics = target_store_metrics.snapshot();
    let lsm_after_crud_reopen = capture_slatedb_lsm(&migrated)?;
    write_phase_checkpoint(
        &root,
        "post_crud_reopen_complete",
        total_started,
        serde_json::json!({
            "query_corpus": post_crud_reopen,
            "slatedb_lsm": lsm_after_crud_reopen,
            "target_object_store": target_after_crud_reopen_metrics,
        }),
    )?;
    let compaction_started = Instant::now();
    heartbeat.set_phase("compaction_drain");
    let compaction_drain = observe_compaction_drain(
        &migrated,
        args.compaction_drain_timeout,
        args.maximum_steady_l0_ssts,
    )
    .await?;
    let compaction_drain_millis = elapsed_millis(compaction_started);
    let target_after_compaction_metrics = target_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "compaction_drain_complete",
        total_started,
        serde_json::json!({
            "compaction_drain": compaction_drain,
            "target_object_store": target_after_compaction_metrics,
        }),
    )?;
    if compaction_drain.passed == Some(false) {
        bail!(
            "{}: SlateDB L0 debt did not drain to at most {} SSTs within {:?}",
            scenario.name,
            args.maximum_steady_l0_ssts,
            args.compaction_drain_timeout
        );
    }
    match args.migration_failpoint {
        Some(failpoint) if !db::migrations::migration_failpoint_was_triggered() => {
            bail!(
                "{}: requested migration failpoint {} was not reached",
                scenario.name,
                failpoint.as_str()
            );
        }
        _ => {}
    }
    let final_garbage_collection_started = Instant::now();
    let final_gc_storage_before = object_prefix_size(&target_store, &target_database).await?;
    migrated
        .close()
        .await
        .context("failed to quiesce migrated db before final garbage collection")?;
    let final_gc_checkpoints = run_target_garbage_collection(
        &target_database,
        &target_store,
        CheckpointExpiryMode::AdvancePastLatest,
    )
    .await
    .context("final quiescent SlateDB garbage collection failed")?;
    let final_gc_storage_after = object_prefix_size(&target_store, &target_database).await?;
    let migrated = HelixDB::open_with_object_store_for_migration_parity(
        &target_database,
        Arc::clone(&target_store),
        DbConfig::new().with_migration_tuning(tuning),
    )
    .await
    .context("failed to reopen migrated db after final garbage collection")?;
    if migrated.process_migration_once().await? {
        bail!(
            "{}: final post-garbage-collection reopen found unexpected migration work",
            scenario.name
        );
    }
    assert_job_statuses_completed(
        scenario.name,
        &migrated.migration_parity_job_statuses().await?,
    )?;
    if !migrated.migration_parity_definition_migration_active() {
        bail!(
            "{}: User.tier definition migration did not survive final garbage collection",
            scenario.name
        );
    }
    let post_final_gc_corpus = migrated
        .migration_parity_crud_query_corpus(&post_migration_crud.state)
        .await
        .context("post-final-GC cold-reopen query corpus failed")?;
    if post_final_gc_corpus != post_crud_reopen {
        bail!(
            "{}: CRUD query corpus changed across final garbage collection: before={post_crud_reopen:?}, after={post_final_gc_corpus:?}",
            scenario.name
        );
    }
    let final_garbage_collection_millis = elapsed_millis(final_garbage_collection_started);
    let final_garbage_collection = report::GarbageCollectionPassEvidence {
        phase: "after_crud_and_compaction",
        elapsed_millis: final_garbage_collection_millis,
        checkpoint_clock_advance_millis: final_gc_checkpoints.clock_advance_millis,
        checkpoints_before: final_gc_checkpoints.checkpoints,
        expiring_checkpoints_before: final_gc_checkpoints.expiring_checkpoints,
        permanent_checkpoints_before: final_gc_checkpoints.permanent_checkpoints,
        storage_before_bytes: final_gc_storage_before,
        storage_after_bytes: final_gc_storage_after,
        reclaimed_bytes: final_gc_storage_before.saturating_sub(final_gc_storage_after),
        cold_reopen_passed: true,
    };
    let target_after_final_gc_metrics = target_store_metrics.snapshot();
    let lsm_after_final_gc = capture_slatedb_lsm(&migrated)?;
    write_phase_checkpoint(
        &root,
        "final_garbage_collection_complete",
        total_started,
        serde_json::json!({
            "garbage_collection": final_garbage_collection,
            "query_corpus": post_final_gc_corpus,
            "slatedb_lsm": lsm_after_final_gc,
            "target_object_store": target_after_final_gc_metrics,
        }),
    )?;
    let compaction_jobs = migrated.migration_parity_compaction_statuses().await?;
    let migration_snapshot = migrated.migration_parity_index_state().await?;
    if migration_snapshot.v2.legacy_definition_rows != 0
        || migration_snapshot.v2.pending_operation_pointers != 0
        || migration_snapshot
            .v2
            .canonical_records
            .iter()
            .any(|record| record.state != "active")
    {
        bail!(
            "{}: target retained legacy definitions, pending V2 work, or a non-Active canonical record: {}",
            scenario.name,
            serde_json::to_string_pretty(&migration_snapshot.v2)?
        );
    }
    let failed_compactions = u64::try_from(
        compaction_jobs
            .iter()
            .filter(|compaction| {
                compaction.status == "failed"
                    && !inherited_compaction_ids.contains(compaction.id.as_str())
            })
            .count(),
    )?;
    let target_storage_bytes = object_prefix_size(&target_store, &target_database).await?;
    let target_after_storage_metrics = target_store_metrics.snapshot();
    migrated
        .close()
        .await
        .context("failed to close migrated db")?;
    let target_after_close_metrics = target_store_metrics.snapshot();
    let post_crud_fault_baseline =
        if args.target_fault.is_some() && target_after_close_metrics.injected_errors == 0 {
            let baseline_root = oracle_root.join("pre-fault-target");
            std::fs::create_dir_all(&baseline_root).with_context(|| {
                format!(
                    "failed to create pre-fault target oracle root {}",
                    baseline_root.display()
                )
            })?;
            let baseline_paths = logical_oracle::OraclePaths::new(&baseline_root);
            let baseline = HelixDB::open_with_object_store_for_migration_parity(
                &target_database,
                Arc::clone(&target_store),
                DbConfig::new().with_migration_tuning(tuning),
            )
            .await
            .context("failed to cold-reopen migrated db before the object-store fault probe")?;
            if baseline.process_migration_once().await? {
                bail!(
                    "{}: pre-fault cold reopen found unexpected migration work",
                    scenario.name
                );
            }
            logical_oracle::build_target(
                &baseline,
                &baseline_root,
                &baseline_paths,
                args.oracle_buffer_bytes,
            )
            .await
            .context("failed to build the pre-fault target oracle")?;
            let baseline_semantic = target_semantic_evidence(&baseline).await?;
            baseline
                .close()
                .await
                .context("failed to close the pre-fault migrated db")?;
            Some((baseline_paths, baseline_semantic))
        } else {
            None
        };
    let object_store_fault_probe_attempts = if let Some(fault) = args.target_fault
        && target_after_close_metrics.injected_errors == 0
    {
        exercise_target_fault(
            &target_store,
            &target_database,
            &target_store_metrics,
            fault,
            args.maximum_open_attempts,
        )
        .await
        .with_context(|| {
            format!(
                "{}: failed to exercise otherwise-unused target object-store operation {:?}",
                scenario.name, fault.operation
            )
        })?
    } else {
        0
    };
    if object_store_fault_probe_attempts > 0 {
        let post_fault = HelixDB::open_with_object_store_for_migration_parity(
            &target_database,
            Arc::clone(&target_store),
            DbConfig::new().with_migration_tuning(tuning),
        )
        .await
        .context("failed to cold-reopen migrated db after the object-store fault probe")?;
        if post_fault.process_migration_once().await? {
            bail!(
                "{}: post-fault cold reopen found unexpected migration work",
                scenario.name
            );
        }
        let post_fault_oracle_root = oracle_root.join("post-fault-target");
        std::fs::create_dir_all(&post_fault_oracle_root).with_context(|| {
            format!(
                "failed to create post-fault target oracle root {}",
                post_fault_oracle_root.display()
            )
        })?;
        let post_fault_oracle_paths = logical_oracle::OraclePaths::new(&post_fault_oracle_root);
        logical_oracle::build_target(
            &post_fault,
            &post_fault_oracle_root,
            &post_fault_oracle_paths,
            args.oracle_buffer_bytes,
        )
        .await
        .context("failed to rebuild the target oracle after the object-store fault probe")?;
        let (baseline_paths, baseline_semantic) = post_crud_fault_baseline
            .as_ref()
            .expect("a fault probe requires its pre-fault target oracle");
        let post_fault_comparison =
            logical_oracle::compare_target_durability(baseline_paths, &post_fault_oracle_paths)?;
        if !post_fault_comparison.is_equal() {
            bail!(
                "{}: post-fault parity oracle found differences: {}",
                scenario.name,
                serde_json::to_string_pretty(&post_fault_comparison)?
            );
        }
        let post_fault_semantic = target_semantic_evidence(&post_fault).await?;
        if &post_fault_semantic != baseline_semantic {
            bail!(
                "{}: text or vector query evidence changed after object-store fault recovery: before={baseline_semantic:?}, after={post_fault_semantic:?}",
                scenario.name
            );
        }
        verify_blob_fixture(&target_store, &target_database).await?;
        let post_fault_corpus = post_fault
            .migration_parity_crud_query_corpus(&post_migration_crud.state)
            .await
            .context("post-fault CRUD query corpus failed")?;
        if post_fault_corpus != post_crud_reopen {
            bail!(
                "{}: CRUD query corpus changed after object-store fault recovery: before={post_crud_reopen:?}, after={post_fault_corpus:?}",
                scenario.name
            );
        }
        let post_fault_jobs = post_fault.migration_parity_job_statuses().await?;
        assert_job_statuses_completed(scenario.name, &post_fault_jobs)?;
        let post_fault_snapshot = post_fault.migration_parity_snapshot().await?;
        if post_fault_snapshot.v2.legacy_definition_rows != 0
            || post_fault_snapshot.v2.pending_operation_pointers != 0
            || post_fault_snapshot
                .v2
                .canonical_records
                .iter()
                .any(|record| record.state != "active")
        {
            bail!(
                "{}: post-fault target retained legacy definitions, pending V2 work, or a non-Active canonical record: {}",
                scenario.name,
                serde_json::to_string_pretty(&post_fault_snapshot.v2)?
            );
        }
        post_fault
            .close()
            .await
            .context("failed to close post-fault migrated db")?;
    }
    let target_after_fault_probe_metrics = target_store_metrics.snapshot();
    if args.target_fault.is_some() && target_after_fault_probe_metrics.injected_errors == 0 {
        bail!(
            "{}: requested target object-store fault was never exercised",
            scenario.name
        );
    }
    let maximum_transaction_bytes =
        u64::try_from(MigrationTuning::DEFAULT_BATCH_BYTES).unwrap_or(u64::MAX);
    if target_after_fault_probe_metrics.maximum_wal_object_bytes == 0 {
        bail!(
            "{}: target object-store instrumentation observed no durable WAL transaction",
            scenario.name
        );
    }
    if target_after_fault_probe_metrics.maximum_wal_object_bytes > maximum_transaction_bytes {
        bail!(
            "{}: maximum serialized WAL transaction was {} bytes, exceeding the {} byte migration transaction limit",
            scenario.name,
            target_after_fault_probe_metrics.maximum_wal_object_bytes,
            maximum_transaction_bytes
        );
    }

    if !args.preserve_store {
        match &args.storage {
            Storage::Local => {
                std::fs::remove_dir_all(&target_root).with_context(|| {
                    format!(
                        "failed to remove verified target store {}",
                        target_root.display()
                    )
                })?;
            }
            Storage::Minio(_) => {
                let cleanup_store = build_target_store(args, &target_root)?;
                clear_target_object_prefix(&cleanup_store, &target_database).await?;
            }
        }
        remove_interrupted_target_oracle_files(&oracle_root)?;
    }

    let snapshot_restore_started = Instant::now();
    match &args.storage {
        Storage::Local if args.preserve_store => {
            copy_directory(&source_root, &rollback_root)
                .context("failed to restore the immutable source directory snapshot")?;
        }
        Storage::Local => {
            std::fs::remove_dir_all(&rollback_root).with_context(|| {
                format!(
                    "failed to remove empty rollback root {}",
                    rollback_root.display()
                )
            })?;
            std::fs::rename(&source_root, &rollback_root).with_context(|| {
                format!(
                    "failed to transfer immutable source snapshot {} to {}",
                    source_root.display(),
                    rollback_root.display()
                )
            })?;
        }
        Storage::Minio(_) => {
            copy_object_prefix(&source_store, &source_database, &rollback_database)
                .await
                .context("failed to restore the immutable source object prefix")?;
            if !args.preserve_store {
                let cleanup_store = build_source_store(args, &source_root)?;
                clear_object_prefix(&cleanup_store, &source_database).await?;
            }
        }
    }
    let rollback_store: Arc<dyn object_store::ObjectStore> = Arc::new(InstrumentedStore012::new(
        build_source_store(args, &rollback_root)?,
        Arc::clone(&source_store_metrics),
    ));
    let restored = open_hyperscale(
        &rollback_database,
        scenario.edge_policy,
        Arc::clone(&rollback_store),
    )
    .await
    .context("failed to open the restored source snapshot")?;
    let rollback_oracle_root = oracle_root.join("rollback-restore");
    std::fs::create_dir_all(&rollback_oracle_root).with_context(|| {
        format!(
            "failed to create rollback oracle root {}",
            rollback_oracle_root.display()
        )
    })?;
    let rollback_paths = logical_oracle::OraclePaths::new(&rollback_oracle_root);
    let rollback_oracle = logical_oracle::build_source(
        &restored,
        &rollback_oracle_root,
        &rollback_paths,
        args.oracle_buffer_bytes,
    )
    .await
    .context("failed to build the restored snapshot oracle")?;
    let rollback_comparison =
        logical_oracle::compare_source_durability(&oracle_paths, &rollback_paths)?;
    if !rollback_comparison.is_equal() {
        bail!(
            "{}: restored source snapshot differs from its immutable oracle: {}",
            scenario.name,
            serde_json::to_string_pretty(&rollback_comparison)?
        );
    }
    let rollback_semantics = source_semantic_evidence(&restored).await?;
    if rollback_semantics != source_semantics {
        bail!(
            "{}: restored source semantic queries differ: source={source_semantics:?}, restored={rollback_semantics:?}",
            scenario.name,
        );
    }
    restored
        .close()
        .await
        .context("failed to close the restored source snapshot")?;
    let rollback_storage_bytes =
        source_object_prefix_size(&rollback_store, &rollback_database).await?;
    let snapshot_restore = report::SnapshotRestoreEvidence {
        database: rollback_database,
        oracle: rollback_oracle,
        comparison: rollback_comparison,
        node_text_hits: rollback_semantics.node_text_hits,
        edge_text_hits: rollback_semantics.edge_text_hits,
        node_vector_hits: rollback_semantics.node_vector_hits,
        edge_vector_hits: rollback_semantics.edge_vector_hits,
        storage_bytes: rollback_storage_bytes,
    };
    let snapshot_restore_millis = elapsed_millis(snapshot_restore_started);
    let source_after_snapshot_restore_metrics = source_store_metrics.snapshot();
    write_phase_checkpoint(
        &root,
        "snapshot_restore_complete",
        total_started,
        serde_json::json!({
            "snapshot_restore": snapshot_restore,
            "source_object_store": source_after_snapshot_restore_metrics,
        }),
    )?;

    info!(
        scenario = scenario.name,
        cleanup_steps,
        definition_migration_steps,
        definition_migration_active,
        source_nodes = source_oracle.nodes.records,
        source_current_edges = source_oracle.current_edges.records,
        source_legacy_edges = source_oracle
            .legacy_edges
            .as_ref()
            .map(|stats| stats.records)
            .unwrap_or_default(),
        target_edges = target_oracle.current_edges.records,
        source_oracle_records = source_oracle
            .nodes
            .records
            .saturating_add(source_oracle.current_edges.records)
            .saturating_add(
                source_oracle
                    .legacy_edges
                    .as_ref()
                    .map(|stats| stats.records)
                    .unwrap_or_default()
            )
            .saturating_add(source_oracle.exact_keys.records),
        target_oracle_records = target_oracle
            .nodes
            .records
            .saturating_add(target_oracle.current_edges.records)
            .saturating_add(target_oracle.exact_keys.records),
        "parity scenario passed"
    );
    Ok(report::ScenarioEvidence {
        name: scenario.name.to_string(),
        edge_policy: scenario.edge_policy,
        source_seed_resume,
        counts: report::LogicalCounts {
            source_nodes: source_oracle.nodes.records,
            source_current_edges: source_oracle.current_edges.records,
            source_legacy_edges: source_oracle
                .legacy_edges
                .as_ref()
                .map(|stats| stats.records)
                .unwrap_or_default(),
            expected_target_edges: source_oracle
                .expected_edges
                .as_ref()
                .map(|stats| stats.records)
                .unwrap_or_default(),
            target_nodes: target_oracle.nodes.records,
            target_edges: target_oracle.current_edges.records,
        },
        cleanup_steps,
        definition_migration_steps,
        definition_migration_active,
        migration_jobs,
        adoption_snapshot,
        migration_snapshot,
        source_vector_non_metadata_namespace_digests: source_semantics
            .vector_non_metadata_namespace_digests
            .clone(),
        text_query,
        vector_query,
        post_migration_crud,
        post_crud_reopen,
        source_garbage_collection,
        garbage_collection: report::GarbageCollectionEvidence {
            before_parity_oracle: initial_garbage_collection,
            after_crud_and_compaction: final_garbage_collection,
        },
        snapshot_restore,
        source_oracle,
        reopened_source_oracle,
        source_durability,
        post_gc_source_oracle,
        source_gc_durability,
        target_oracle,
        comparison: oracle_comparison,
        source_object_store: source_after_snapshot_restore_metrics.clone(),
        target_object_store: target_after_fault_probe_metrics.clone(),
        target_storage_bytes,
        open_attempts,
        object_store_fault_probe_attempts,
        migration_failpoint_retries,
        object_store_phases: report::ObjectStorePhaseEvidence {
            source_seed: source_after_seed_metrics.delta_since(&source_initial_metrics),
            source_oracle: source_after_oracle_metrics.delta_since(&source_after_seed_metrics),
            source_durable_reopen: source_after_durable_reopen_metrics
                .delta_since(&source_after_oracle_metrics),
            source_garbage_collection: source_after_garbage_collection_metrics
                .delta_since(&source_after_durable_reopen_metrics),
            source_close_and_copy: source_after_copy_metrics
                .delta_since(&source_after_garbage_collection_metrics),
            snapshot_restore: source_after_snapshot_restore_metrics
                .delta_since(&source_after_copy_metrics),
            target_rewrite: target_after_rewrite_metrics.delta_since(&target_initial_metrics),
            cleanup: target_after_cleanup_metrics.delta_since(&target_after_rewrite_metrics),
            definition_migration: target_after_definition_migration_metrics
                .delta_since(&target_after_cleanup_metrics),
            reopen: target_after_reopen_metrics
                .delta_since(&target_after_definition_migration_metrics),
            initial_garbage_collection: target_after_initial_gc_metrics
                .delta_since(&target_after_reopen_metrics),
            target_oracle: target_after_oracle_metrics
                .delta_since(&target_after_initial_gc_metrics),
            post_migration_crud: target_after_crud_metrics
                .delta_since(&target_after_oracle_metrics),
            post_crud_reopen: target_after_crud_reopen_metrics
                .delta_since(&target_after_crud_metrics),
            compaction_drain: target_after_compaction_metrics
                .delta_since(&target_after_crud_reopen_metrics),
            final_garbage_collection: target_after_final_gc_metrics
                .delta_since(&target_after_compaction_metrics),
            storage_measurement: target_after_storage_metrics
                .delta_since(&target_after_final_gc_metrics),
            target_close: target_after_close_metrics.delta_since(&target_after_storage_metrics),
            fault_probe: target_after_fault_probe_metrics.delta_since(&target_after_close_metrics),
        },
        slatedb_lsm_phases: report::SlateDbLsmPhaseEvidence {
            after_rewrite: lsm_after_rewrite,
            after_cleanup: lsm_after_cleanup,
            after_reopen: lsm_after_reopen,
            after_oracle: lsm_after_oracle,
            after_initial_garbage_collection: lsm_after_initial_gc,
            after_crud: lsm_after_crud,
            after_crud_reopen: lsm_after_crud_reopen,
            after_final_garbage_collection: lsm_after_final_gc,
        },
        compaction_drain,
        source_compaction_jobs,
        compaction_jobs,
        failed_compactions,
        compaction_errors: compaction_errors_since(compaction_error_start),
        timings_millis: report::TimingsMillis {
            seed: seed_millis,
            source_oracle: source_oracle_millis,
            source_durable_reopen: source_durable_reopen_millis,
            source_garbage_collection: source_garbage_collection_millis,
            immutable_copy: copy_millis,
            blocking_rewrite_open: rewrite_millis,
            cleanup: cleanup_millis,
            definition_migration: definition_migration_millis,
            reopen: reopen_millis,
            target_oracle: target_oracle_millis,
            post_migration_crud: post_migration_crud_millis,
            post_crud_reopen: post_crud_reopen_millis,
            garbage_collection: initial_garbage_collection_millis
                .saturating_add(final_garbage_collection_millis),
            snapshot_restore: snapshot_restore_millis,
            compaction_drain: compaction_drain_millis,
            total: elapsed_millis(total_started),
        },
    })
}

async fn resume_scenario_verification(
    args: &Args,
    scenario: Scenario,
) -> Result<report::ResumeScenarioEvidence> {
    let started = Instant::now();
    let compaction_error_start = compaction_error_count();
    let root = args.store_root.join(scenario.name);
    let target_root = root.join("target");
    let oracle_root = root.join("oracle");
    let source_expected = oracle_root.join("source-expected-edges.sorted");
    if !source_expected.is_file() {
        bail!(
            "cannot resume {}: source oracle {} is missing",
            scenario.name,
            source_expected.display()
        );
    }
    let source_semantic = read_source_semantic_evidence(&root)?;
    remove_interrupted_target_oracle_files(&oracle_root)?;
    let (_, target_database) = scenario_databases(args, scenario);
    let recorder = ObjectStoreRecorder::new(FaultPolicy::latency(args.object_store_latency));
    let target_store: Arc<dyn object_store_014::ObjectStore> = Arc::new(InstrumentedStore014::new(
        build_target_store(args, &target_root)?,
        Arc::clone(&recorder),
    ));
    let tuning = MigrationTuning::default()
        .with_worker_mode(MigrationWorkerMode::Disabled)
        .with_batch_rows(
            MigrationBatchRows::new(args.batch_rows)
                .expect("batch rows were validated as positive"),
        );
    let reopen_started = Instant::now();
    let migrated = HelixDB::open_with_object_store_for_migration_parity(
        &target_database,
        Arc::clone(&target_store),
        DbConfig::new().with_migration_tuning(tuning),
    )
    .await
    .context("failed to reopen migrated target for resumed verification")?;
    let reopen_millis = elapsed_millis(reopen_started);
    let migration_started = Instant::now();
    let maximum_migration_steps = 10_000_u64.max(
        args.scale_edges
            .div_ceil(u64::try_from(args.batch_rows)?)
            .saturating_add(100),
    );
    let mut migration_steps = 0_u64;
    while migrated.process_migration_once().await? {
        migration_steps = migration_steps.saturating_add(1);
        if migration_steps > maximum_migration_steps {
            bail!(
                "{}: resumed migration did not converge after {} steps",
                scenario.name,
                maximum_migration_steps
            );
        }
    }
    let migration_millis = elapsed_millis(migration_started);
    let migration_jobs = migrated.migration_parity_job_statuses().await?;
    assert_job_statuses_completed(scenario.name, &migration_jobs)?;

    let definition_migration_steps = 0_u64;
    let definition_migration_millis = 0_u64;
    let definition_migration_active = migrated.migration_parity_definition_migration_active();
    if !definition_migration_active {
        bail!(
            "{}: resumed verification found no migrated User.tier definition",
            scenario.name
        );
    }
    let oracle_paths = logical_oracle::OraclePaths::new(&oracle_root);
    let oracle_started = Instant::now();
    let target_oracle = logical_oracle::build_target(
        &migrated,
        &oracle_root,
        &oracle_paths,
        args.oracle_buffer_bytes,
    )
    .await?;
    let comparison = logical_oracle::compare(&oracle_paths)?;
    if !comparison.is_equal() {
        bail!(
            "{}: resumed streaming parity oracle found differences: {}",
            scenario.name,
            serde_json::to_string_pretty(&comparison)?
        );
    }
    let (text_query, vector_query) =
        verify_semantic_queries(&migrated, scenario.name, source_semantic.clone()).await?;
    verify_blob_fixture(&target_store, &target_database).await?;
    let oracle_millis = elapsed_millis(oracle_started);
    let slatedb_lsm = capture_slatedb_lsm(&migrated)?;
    let compaction_jobs = migrated.migration_parity_compaction_statuses().await?;
    let migration_snapshot = migrated.migration_parity_index_state().await?;
    assert_vector_adoption_evidence(scenario.name, &source_semantic, &migration_snapshot)?;
    if migration_snapshot.v2.legacy_definition_rows != 0
        || migration_snapshot.v2.pending_operation_pointers != 0
        || migration_snapshot
            .v2
            .canonical_records
            .iter()
            .any(|record| record.state != "active")
    {
        bail!(
            "{}: resumed target retained legacy definitions, pending V2 work, or a non-Active canonical record: {}",
            scenario.name,
            serde_json::to_string_pretty(&migration_snapshot.v2)?
        );
    }
    let target_storage_bytes = object_prefix_size(&target_store, &target_database).await?;
    migrated.close().await?;

    Ok(report::ResumeScenarioEvidence {
        name: scenario.name.to_string(),
        migration_jobs,
        migration_snapshot,
        source_vector_non_metadata_namespace_digests: source_semantic
            .vector_non_metadata_namespace_digests,
        migration_steps,
        migration_millis,
        text_query,
        vector_query,
        target_oracle,
        comparison,
        object_store: recorder.snapshot(),
        target_storage_bytes,
        slatedb_lsm,
        compaction_jobs,
        compaction_errors: compaction_errors_since(compaction_error_start),
        definition_migration_steps,
        definition_migration_active,
        definition_migration_millis,
        reopen_millis,
        oracle_millis,
        total_millis: elapsed_millis(started),
    })
}

async fn capture_source_compaction_statuses(
    database: &str,
    store: &Arc<dyn object_store::ObjectStore>,
) -> Result<Vec<report::SourceCompactionStatus>> {
    let admin = hyperscale_slatedb::admin::AdminBuilder::new(database, Arc::clone(store)).build();
    let encoded = admin
        .read_compactions(None)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let Some(encoded) = encoded else {
        return Ok(Vec::new());
    };
    let value: serde_json::Value = serde_json::from_str(&encoded)?;
    let state = value
        .as_array()
        .and_then(|tuple| tuple.get(1))
        .context("source compactions JSON is not an [id, state] tuple")?;
    let recent = state
        .get("recent_compactions")
        .and_then(serde_json::Value::as_object)
        .context("source compactions JSON has no recent_compactions object")?;
    let mut latest = BTreeMap::new();
    for (map_id, compaction) in recent {
        let id = compaction
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(map_id)
            .to_string();
        let status = compaction
            .get("status")
            .and_then(serde_json::Value::as_str)
            .context("source compaction has no string status")?
            .to_lowercase();
        let spec = serde_json::to_string(
            compaction
                .get("spec")
                .context("source compaction has no spec")?,
        )?;
        let bytes_processed = compaction
            .get("bytes_processed")
            .and_then(serde_json::Value::as_u64)
            .context("source compaction has no u64 bytes_processed")?;
        latest.insert(
            id.clone(),
            report::SourceCompactionStatus {
                id,
                status,
                spec,
                bytes_processed,
            },
        );
    }
    Ok(latest.into_values().collect())
}

async fn run_source_garbage_collection(
    database: &str,
    store: &Arc<dyn object_store::ObjectStore>,
) -> Result<GarbageCollectionCheckpointEvidence> {
    let inspection_admin =
        hyperscale_slatedb::admin::AdminBuilder::new(database, Arc::clone(store)).build();
    let checkpoints = inspection_admin
        .list_checkpoints(None)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let expiring_checkpoints = checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.expire_time.is_some())
        .count();
    let permanent_checkpoints = checkpoints.len().saturating_sub(expiring_checkpoints);
    let directory = hyperscale_slatedb::config::GarbageCollectorDirectoryOptions {
        interval: None,
        min_age: Duration::ZERO,
    };
    let options = hyperscale_slatedb::config::GarbageCollectorOptions {
        manifest_options: Some(directory),
        wal_options: Some(directory),
        compacted_options: Some(directory),
        compactions_options: Some(directory),
    };
    inspection_admin
        .run_gc_once(options)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(GarbageCollectionCheckpointEvidence {
        clock_advance_millis: 0,
        checkpoints: u64::try_from(checkpoints.len()).unwrap_or(u64::MAX),
        expiring_checkpoints: u64::try_from(expiring_checkpoints).unwrap_or(u64::MAX),
        permanent_checkpoints: u64::try_from(permanent_checkpoints).unwrap_or(u64::MAX),
    })
}

fn capture_slatedb_lsm(database: &HelixDB) -> Result<report::SlateDbLsmSnapshot> {
    let raw = database.migration_parity_inner_db()?;
    let manifest = raw.manifest();
    let mut l0_ssts = u64::try_from(manifest.l0().len())?;
    let mut l0_bytes = manifest
        .l0()
        .iter()
        .map(|sst| sst.estimate_size())
        .sum::<u64>();
    let mut compacted_runs = u64::try_from(manifest.compacted().len())?;
    let mut compacted_ssts = manifest.compacted().iter().try_fold(0_u64, |total, run| {
        Ok::<u64, anyhow::Error>(total.saturating_add(u64::try_from(run.sst_views.len())?))
    })?;
    let mut compacted_bytes = manifest
        .compacted()
        .iter()
        .map(|run| run.estimate_size())
        .sum::<u64>();
    let mut maximum_tree_l0_ssts = l0_ssts;
    for segment in manifest.segments() {
        let segment_l0_ssts = u64::try_from(segment.l0().len())?;
        l0_ssts = l0_ssts.saturating_add(segment_l0_ssts);
        l0_bytes = l0_bytes.saturating_add(
            segment
                .l0()
                .iter()
                .map(|sst| sst.estimate_size())
                .sum::<u64>(),
        );
        compacted_runs = compacted_runs.saturating_add(u64::try_from(segment.compacted().len())?);
        compacted_ssts = segment
            .compacted()
            .iter()
            .try_fold(compacted_ssts, |total, run| {
                Ok::<u64, anyhow::Error>(total.saturating_add(u64::try_from(run.sst_views.len())?))
            })?;
        compacted_bytes = compacted_bytes.saturating_add(
            segment
                .compacted()
                .iter()
                .map(|run| run.estimate_size())
                .sum::<u64>(),
        );
        maximum_tree_l0_ssts = maximum_tree_l0_ssts.max(segment_l0_ssts);
    }
    Ok(report::SlateDbLsmSnapshot {
        manifest_version: manifest.id(),
        writer_epoch: manifest.writer_epoch(),
        compactor_epoch: manifest.compactor_epoch(),
        l0_ssts,
        l0_bytes,
        compacted_runs,
        compacted_ssts,
        compacted_bytes,
        segments: u64::try_from(manifest.segments().len())?,
        maximum_tree_l0_ssts,
    })
}

async fn observe_compaction_drain(
    database: &HelixDB,
    timeout: Duration,
    maximum_steady_l0_ssts: usize,
) -> Result<report::CompactionDrainEvidence> {
    const REQUIRED_STEADY_SAMPLES: u64 = 3;
    const POLL_INTERVAL: Duration = Duration::from_secs(1);

    let started = Instant::now();
    let initial = capture_slatedb_lsm(database)?;
    if timeout.is_zero() {
        return Ok(report::CompactionDrainEvidence {
            enabled: false,
            maximum_wait_millis: 0,
            elapsed_millis: 0,
            maximum_steady_l0_ssts: u64::try_from(maximum_steady_l0_ssts)?,
            peak_l0_ssts: initial.l0_ssts,
            samples: 1,
            passed: None,
            initial: initial.clone(),
            final_snapshot: initial,
        });
    }

    let maximum_steady_l0_ssts = u64::try_from(maximum_steady_l0_ssts)?;
    let mut final_snapshot = initial.clone();
    let mut peak_l0_ssts = initial.l0_ssts;
    let mut samples = 1_u64;
    let mut steady_samples = u64::from(initial.maximum_tree_l0_ssts <= maximum_steady_l0_ssts);
    while steady_samples < REQUIRED_STEADY_SAMPLES && started.elapsed() < timeout {
        tokio::time::sleep(POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed()))).await;
        final_snapshot = capture_slatedb_lsm(database)?;
        samples = samples.saturating_add(1);
        peak_l0_ssts = peak_l0_ssts.max(final_snapshot.l0_ssts);
        if final_snapshot.maximum_tree_l0_ssts <= maximum_steady_l0_ssts {
            steady_samples = steady_samples.saturating_add(1);
        } else {
            steady_samples = 0;
        }
    }
    Ok(report::CompactionDrainEvidence {
        enabled: true,
        maximum_wait_millis: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        elapsed_millis: elapsed_millis(started),
        maximum_steady_l0_ssts,
        peak_l0_ssts,
        samples,
        passed: Some(steady_samples >= REQUIRED_STEADY_SAMPLES),
        initial,
        final_snapshot,
    })
}

fn remove_interrupted_target_oracle_files(directory: &Path) -> Result<()> {
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("failed to list oracle directory {}", directory.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        if name.as_encoded_bytes().starts_with(b"target-") && entry.file_type()?.is_file() {
            std::fs::remove_file(entry.path()).with_context(|| {
                format!(
                    "failed to remove interrupted target oracle file {}",
                    entry.path().display()
                )
            })?;
        }
    }
    Ok(())
}

fn write_source_semantic_evidence(root: &Path, evidence: &SourceSemanticEvidence) -> Result<()> {
    let path = root.join("source-semantic-evidence.json");
    let temporary = root.join("source-semantic-evidence.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(evidence)?)
        .with_context(|| format!("failed to write source semantics {}", temporary.display()))?;
    std::fs::rename(&temporary, &path).with_context(|| {
        format!(
            "failed to publish source semantics {} as {}",
            temporary.display(),
            path.display()
        )
    })
}

fn read_source_semantic_evidence(root: &Path) -> Result<SourceSemanticEvidence> {
    let path = root.join("source-semantic-evidence.json");
    let bytes = std::fs::read(&path)
        .with_context(|| format!("failed to read source semantics {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to decode source semantics {}", path.display()))
}

fn write_phase_checkpoint(
    root: &Path,
    phase: &str,
    started: Instant,
    evidence: serde_json::Value,
) -> Result<()> {
    let path = root.join("phase-checkpoint.json");
    let temporary = root.join("phase-checkpoint.json.tmp");
    let checkpoint = serde_json::json!({
        "schema_version": 1,
        "status": "running",
        "phase": phase,
        "elapsed_millis": elapsed_millis(started),
        "peak_rss_bytes": report::peak_rss_bytes(),
        "compaction_errors": compaction_errors_since(0),
        "evidence": evidence,
    });
    std::fs::write(&temporary, serde_json::to_vec_pretty(&checkpoint)?)
        .with_context(|| format!("failed to write phase checkpoint {}", temporary.display()))?;
    std::fs::rename(&temporary, &path)
        .with_context(|| format!("failed to publish phase checkpoint {}", path.display()))
}

fn migration_index_definitions() -> Vec<HDynamicIndexDefinition> {
    use helix::db::index::hnsw::VectorDistanceMetric;

    vec![
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_equality(
            "User", "tier",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_unique_equality(
            "User",
            "external_id",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_range("User", "rank")),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_range_desc(
            "Account", "rank",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::edge_equality(
            "FOLLOWS", "kind",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::edge_range(
            "FOLLOWS", "since",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::edge_range_desc(
            "KNOWS", "since",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_equality(
            SCALE_NODE_LABEL,
            "bucket",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_unique_equality(
            SCALE_NODE_LABEL,
            "external_id",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_range(
            SCALE_NODE_LABEL,
            "ordinal",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::node_range_desc(
            SCALE_NODE_LABEL,
            "reverse_ordinal",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::edge_equality(
            SCALE_EDGE_LABEL,
            "kind",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::edge_range(
            SCALE_EDGE_LABEL,
            "weight",
        )),
        HDynamicIndexDefinition::Secondary(HSecondaryIndexDefinition::edge_range_desc(
            SCALE_EDGE_LABEL,
            "reverse_weight",
        )),
        HDynamicIndexDefinition::Text(HTextIndexDefinition::new_node("User", "bio")),
        HDynamicIndexDefinition::Text(HTextIndexDefinition::new_edge("FOLLOWS", "body")),
        HDynamicIndexDefinition::Vector(HVectorIndexDefinition::new_node(
            "User",
            "embedding",
            3,
            VectorDistanceMetric::Euclidean,
        )),
        HDynamicIndexDefinition::Vector(HVectorIndexDefinition::new_edge(
            "FOLLOWS",
            "embedding",
            3,
            VectorDistanceMetric::Euclidean,
        )),
    ]
}

async fn seed_legacy_index_definitions(database: &HyperscaleDb) -> Result<()> {
    let mut transaction = database.write_tx().await?;
    for definition in migration_index_definitions() {
        match definition {
            HDynamicIndexDefinition::Secondary(definition) => {
                transaction
                    .create_secondary_index_if_not_exists(definition)
                    .await?;
            }
            HDynamicIndexDefinition::Vector(definition) => {
                transaction
                    .create_vector_index_if_not_exists(definition)
                    .await?;
            }
            HDynamicIndexDefinition::Text(definition) => {
                transaction
                    .create_text_index_if_not_exists(definition)
                    .await?;
            }
        }
    }
    transaction.commit().await?;
    database.diagnostic_index_snapshot().await?;
    Ok(())
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn copy_directory(source: &Path, target: &Path) -> Result<()> {
    if target.exists() {
        std::fs::remove_dir_all(target)
            .with_context(|| format!("failed to remove target copy {}", target.display()))?;
    }
    std::fs::create_dir_all(target)
        .with_context(|| format!("failed to create target copy {}", target.display()))?;
    for entry in std::fs::read_dir(source)
        .with_context(|| format!("failed to list source copy {}", source.display()))?
    {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&source_path, &target_path)?;
        } else {
            std::fs::copy(&source_path, &target_path).with_context(|| {
                format!(
                    "failed to copy {} to {}",
                    source_path.display(),
                    target_path.display()
                )
            })?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScaleSeedProgress {
    nodes: u64,
    edges: u64,
}

impl ScaleSeedProgress {
    const fn fresh() -> Self {
        Self { nodes: 0, edges: 0 }
    }

    fn recovered(
        nodes: u64,
        edges: u64,
        target_nodes: u64,
        target_edges: u64,
        batch_rows: NonZeroUsize,
    ) -> Result<Self> {
        if nodes > target_nodes || edges > target_edges {
            bail!(
                "recovered scale seed progress exceeds configured totals: nodes={nodes}/{target_nodes}, edges={edges}/{target_edges}"
            );
        }
        if edges > 0 && nodes != target_nodes {
            bail!(
                "recovered scale edges require every scale node: nodes={nodes}/{target_nodes}, edges={edges}"
            );
        }
        let batch_rows = u64::try_from(batch_rows.get())?;
        if nodes < target_nodes && !nodes.is_multiple_of(batch_rows) {
            bail!("recovered node progress {nodes} is not aligned to seed batch size {batch_rows}");
        }
        if edges < target_edges && !edges.is_multiple_of(batch_rows) {
            bail!("recovered edge progress {edges} is not aligned to seed batch size {batch_rows}");
        }
        Ok(Self { nodes, edges })
    }

    const fn evidence(self, enabled: bool) -> report::SourceSeedResumeEvidence {
        report::SourceSeedResumeEvidence {
            enabled,
            recovered_nodes: self.nodes,
            recovered_edges: self.edges,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScaleEdgeSeedCheckpoint {
    rows_completed: u64,
    global_label_completed: u64,
}

impl ScaleEdgeSeedCheckpoint {
    const ENCODED_LEN: usize = core::mem::size_of::<u8>() + 2 * core::mem::size_of::<u64>();

    const fn initial(completed: u64) -> Self {
        Self {
            rows_completed: completed,
            global_label_completed: completed,
        }
    }

    fn decode(encoded: &[u8]) -> Result<Self> {
        const VERSION_LEN: usize = core::mem::size_of::<u8>();
        const ROWS_LEN: usize = core::mem::size_of::<u64>();
        const GLOBAL_LEN: usize = core::mem::size_of::<u64>();
        if encoded.len() != Self::ENCODED_LEN {
            bail!(
                "scale edge checkpoint has invalid length {}; expected {}",
                encoded.len(),
                Self::ENCODED_LEN
            );
        }
        if encoded[0] != SCALE_EDGE_CHECKPOINT_VERSION {
            bail!(
                "scale edge checkpoint has unsupported version {}; expected {}",
                encoded[0],
                SCALE_EDGE_CHECKPOINT_VERSION
            );
        }
        let rows_completed = u64::from_be_bytes(
            encoded[VERSION_LEN..VERSION_LEN + ROWS_LEN]
                .try_into()
                .expect("validated scale edge checkpoint contains the row count"),
        );
        let global_label_completed = u64::from_be_bytes(
            encoded[VERSION_LEN + ROWS_LEN..VERSION_LEN + ROWS_LEN + GLOBAL_LEN]
                .try_into()
                .expect("validated scale edge checkpoint contains the global label count"),
        );
        if global_label_completed > rows_completed {
            bail!(
                "scale edge checkpoint is invalid: global label progress {global_label_completed} exceeds row progress {rows_completed}"
            );
        }
        Ok(Self {
            rows_completed,
            global_label_completed,
        })
    }

    fn encode(self) -> Bytes {
        let mut encoded = Vec::with_capacity(Self::ENCODED_LEN);
        encoded.push(SCALE_EDGE_CHECKPOINT_VERSION);
        encoded.extend_from_slice(&self.rows_completed.to_be_bytes());
        encoded.extend_from_slice(&self.global_label_completed.to_be_bytes());
        Bytes::from(encoded)
    }

    fn advance_rows(self, completed: u64) -> Result<Self> {
        if completed < self.rows_completed {
            bail!(
                "scale edge row checkpoint cannot move backwards from {} to {completed}",
                self.rows_completed
            );
        }
        Ok(Self {
            rows_completed: completed,
            global_label_completed: self.global_label_completed,
        })
    }

    fn advance_global_label(self, completed: u64) -> Result<Self> {
        if completed < self.global_label_completed {
            bail!(
                "scale edge global-label checkpoint cannot move backwards from {} to {completed}",
                self.global_label_completed
            );
        }
        if completed > self.rows_completed {
            bail!(
                "scale edge global-label checkpoint {completed} exceeds completed rows {}",
                self.rows_completed
            );
        }
        Ok(Self {
            rows_completed: self.rows_completed,
            global_label_completed: completed,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeededScaleEdge {
    id: u64,
    ordinal: u64,
    from: u64,
    to: u64,
}

fn scale_node_properties(offset: u64, total_nodes: u64) -> Vec<HProperty> {
    vec![
        HProperty::new("$label", SCALE_NODE_LABEL),
        HProperty::new(
            "external_id",
            format!("scale-v{SCALE_FIXTURE_VERSION}-node-{offset:020}"),
        ),
        HProperty::new("bucket", format!("bucket-{:04}", offset % 1_024)),
        HProperty::i64("ordinal", i64::try_from(offset).unwrap_or(i64::MAX)),
        HProperty::i64(
            "reverse_ordinal",
            i64::try_from(total_nodes.saturating_sub(1).saturating_sub(offset)).unwrap_or(i64::MAX),
        ),
    ]
}

fn scale_edge_properties(
    distribution: GraphDistribution,
    ordinal: u64,
    total_edges: u64,
) -> Vec<HProperty> {
    vec![
        HProperty::new("$label", SCALE_EDGE_LABEL),
        HProperty::new("distribution", distribution.name()),
        HProperty::new("kind", format!("kind-{:03}", ordinal % 256)),
        HProperty::i64("weight", i64::try_from(ordinal).unwrap_or(i64::MAX)),
        HProperty::i64(
            "reverse_weight",
            i64::try_from(total_edges.saturating_sub(1).saturating_sub(ordinal))
                .unwrap_or(i64::MAX),
        ),
    ]
}

#[derive(Debug, Default)]
struct StagedWriteBytes {
    bytes: usize,
}

impl StagedWriteBytes {
    fn include(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .context("scale seed staged-write byte count overflows usize")?;
        if self.bytes > MAXIMUM_STAGED_WRITE_BYTES {
            bail!(
                "scale seed transaction stages {} bytes, exceeding the {} byte limit",
                self.bytes,
                MAXIMUM_STAGED_WRITE_BYTES
            );
        }
        Ok(())
    }
}

fn scale_edge_checkpoint_key() -> Bytes {
    graph::metadata_keys::make_metadata_key(SCALE_EDGE_CHECKPOINT_NAME)
}

async fn load_scale_edge_checkpoint(db: &HyperscaleDb) -> Result<Option<ScaleEdgeSeedCheckpoint>> {
    let raw = db.inner_db();
    let transaction = raw.begin(IsolationLevel::Snapshot).await?;
    transaction
        .get(scale_edge_checkpoint_key())
        .await?
        .map(|encoded| ScaleEdgeSeedCheckpoint::decode(&encoded))
        .transpose()
}

async fn persist_initial_scale_edge_checkpoint(
    db: &HyperscaleDb,
    completed: u64,
) -> Result<ScaleEdgeSeedCheckpoint> {
    let checkpoint = ScaleEdgeSeedCheckpoint::initial(completed);
    let key = scale_edge_checkpoint_key();
    let value = checkpoint.encode();
    let raw = db.inner_db();
    let transaction = raw.begin(IsolationLevel::Snapshot).await?;
    transaction.put(&key, &value)?;
    transaction.commit().await?;
    Ok(checkpoint)
}

async fn load_scale_global_edge_label(db: &HyperscaleDb) -> Result<RoaringTreemap> {
    let key = graph::make_global_edge_label_index_key(SCALE_EDGE_LABEL);
    let raw = db.inner_db();
    let transaction = raw.begin(IsolationLevel::Snapshot).await?;
    let Some(encoded) = transaction.get(&key).await? else {
        return Ok(RoaringTreemap::new());
    };
    helix::db::index::decode_roaring_treemap(&encoded).map_err(Into::into)
}

async fn commit_scale_edge_rows(
    db: &HyperscaleDb,
    distribution: GraphDistribution,
    total_edges: u64,
    edges: &[SeededScaleEdge],
    completed: u64,
    checkpoint: ScaleEdgeSeedCheckpoint,
) -> Result<ScaleEdgeSeedCheckpoint> {
    let expected = completed
        .checked_sub(checkpoint.rows_completed)
        .context("scale edge row completion moved backwards")?;
    if u64::try_from(edges.len())? != expected {
        bail!(
            "scale edge row batch has {} entries but checkpoint requires {expected}",
            edges.len()
        );
    }

    let mut legacy_pairs = BTreeMap::new();
    let mut neighbor_deltas = BTreeMap::<Bytes, RoaringTreemap>::new();
    for edge in edges {
        legacy_pairs.insert(
            (edge.from, edge.to),
            graph::encode_properties(&scale_edge_properties(
                distribution,
                edge.ordinal,
                total_edges,
            )),
        );
        neighbor_deltas
            .entry(graph::make_edge_label_out_key(edge.from, SCALE_EDGE_LABEL))
            .or_default()
            .insert(edge.to);
        neighbor_deltas
            .entry(graph::make_edge_label_in_key(edge.to, SCALE_EDGE_LABEL))
            .or_default()
            .insert(edge.from);
    }

    let raw = db.inner_db();
    let transaction = raw.begin(IsolationLevel::Snapshot).await?;
    let mut staged = StagedWriteBytes::default();
    for edge in edges {
        let properties = graph::encode_properties(&scale_edge_properties(
            distribution,
            edge.ordinal,
            total_edges,
        ));
        let key = graph::make_edge_property_key_by_id(edge.id);
        staged.include(&key, &properties)?;
        transaction.put_bytes(key, properties)?;
    }
    for ((from, to), properties) in legacy_pairs {
        let key = graph::make_edge_property_key(from, to);
        staged.include(&key, &properties)?;
        transaction.put_bytes(key, properties.clone())?;
    }
    let neighbor_keys = neighbor_deltas.keys().cloned().collect::<Vec<_>>();
    let neighbor_values = transaction.multi_get(&neighbor_keys).await?;
    for ((key, delta), existing) in neighbor_deltas.into_iter().zip(neighbor_values) {
        let mut neighbors = match existing {
            Some(encoded) => helix::db::index::decode_roaring_treemap(&encoded)?,
            None => RoaringTreemap::new(),
        };
        neighbors |= &delta;
        let value = helix::db::index::encode_roaring_treemap(&neighbors);
        staged.include(&key, &value)?;
        transaction.put_bytes(key, value)?;
    }

    let checkpoint = checkpoint.advance_rows(completed)?;
    let checkpoint_key = scale_edge_checkpoint_key();
    let checkpoint_value = checkpoint.encode();
    staged.include(&checkpoint_key, &checkpoint_value)?;
    transaction.put_bytes(checkpoint_key, checkpoint_value)?;
    transaction.commit().await?;
    Ok(checkpoint)
}

async fn commit_scale_global_edge_label(
    db: &HyperscaleDb,
    pending_edge_ids: &RoaringTreemap,
    completed: u64,
    checkpoint: ScaleEdgeSeedCheckpoint,
) -> Result<ScaleEdgeSeedCheckpoint> {
    let expected = completed
        .checked_sub(checkpoint.global_label_completed)
        .context("scale global edge-label completion moved backwards")?;
    if pending_edge_ids.len() != expected {
        bail!(
            "scale global edge-label delta has {} IDs but checkpoint requires {expected}",
            pending_edge_ids.len()
        );
    }

    let key = graph::make_global_edge_label_index_key(SCALE_EDGE_LABEL);
    let raw = db.inner_db();
    let transaction = raw.begin(IsolationLevel::Snapshot).await?;
    let edge_ids = match transaction.get(&key).await? {
        Some(encoded) => helix::db::index::decode_roaring_treemap(&encoded)?,
        None => RoaringTreemap::new(),
    };
    if edge_ids.len() != completed {
        bail!(
            "scale global edge-label index has {} IDs after legacy writes; expected {completed}",
            edge_ids.len(),
        );
    }
    if pending_edge_ids
        .iter()
        .any(|edge_id| !edge_ids.contains(edge_id))
    {
        bail!(
            "scale global edge-label index is missing one or more IDs from the durable checkpoint delta"
        );
    }

    let mut staged = StagedWriteBytes::default();
    let checkpoint = checkpoint.advance_global_label(completed)?;
    let checkpoint_key = scale_edge_checkpoint_key();
    let checkpoint_value = checkpoint.encode();
    staged.include(&checkpoint_key, &checkpoint_value)?;
    transaction.put_bytes(checkpoint_key, checkpoint_value)?;
    transaction.commit().await?;
    Ok(checkpoint)
}

async fn recover_scale_seed_progress(
    db: &HyperscaleDb,
    scenario: Scenario,
    args: &Args,
) -> Result<ScaleSeedProgress> {
    if args.scale_nodes == 0 {
        bail!("--resume-source-seed requires --scale-nodes to be positive");
    }
    let scale_node_end = FIRST_SCALE_NODE_ID
        .checked_add(args.scale_nodes)
        .context("scale node recovery bound overflows u64")?;
    let read = db
        .read_tx()
        .await
        .context("failed to begin source seed recovery read")?;
    for node_id in (1..=8_u64).chain(30..=34_u64) {
        if !read.node_exists(node_id).await? {
            bail!("cannot resume source seed: base fixture node {node_id} is missing");
        }
    }

    let base_edges = if scenario.edge_policy == EDGE_UPDATE_EAGER {
        5_u64
    } else {
        10_u64
    };
    for edge_id in 0..base_edges {
        if read.get_edge_endpoints(edge_id).await?.is_none() {
            bail!("cannot resume source seed: base fixture edge {edge_id} is missing");
        }
    }
    source_semantic_evidence(db)
        .await
        .context("cannot resume source seed: durable base semantic fixture is incomplete")?;

    let mut first_missing_node = 0_u64;
    let mut node_upper_bound = args.scale_nodes;
    while first_missing_node < node_upper_bound {
        let candidate = first_missing_node + (node_upper_bound - first_missing_node) / 2;
        if read
            .node_exists(
                FIRST_SCALE_NODE_ID
                    .checked_add(candidate)
                    .expect("candidate is below validated scale node end"),
            )
            .await?
        {
            first_missing_node = candidate.saturating_add(1);
        } else {
            node_upper_bound = candidate;
        }
    }
    if read.node_exists(scale_node_end).await? {
        bail!("cannot resume source seed: scale nodes exceed the configured total");
    }

    let options = SourceScanOptions::default()
        .with_read_ahead_bytes(2 * 1024 * 1024)
        .with_cache_blocks(false)
        .with_max_fetch_tasks(16);
    let stored_checkpoint = load_scale_edge_checkpoint(db).await?;
    let mut endpoint_rows = db
        .inner_db()
        .scan_prefix_with_options(
            graph::key_space_prefix(graph::KeySpace::EdgeEndpoints),
            &options,
        )
        .await
        .context("failed to scan source edge endpoints during seed recovery")?;
    const PREFIX_LEN: usize = core::mem::size_of::<u8>();
    const EDGE_ID_LEN: usize = core::mem::size_of::<u64>();
    let mut current_edges = 0_u64;
    let mut maximum_edge_id = None;
    let mut all_scale_edge_ids = RoaringTreemap::new();
    let mut pending_global_edge_ids = RoaringTreemap::new();
    let mut pending_row_edges = Vec::new();
    while let Some(endpoint) = endpoint_rows.next().await? {
        if endpoint.key.len() != PREFIX_LEN + EDGE_ID_LEN {
            bail!(
                "cannot resume source seed: edge endpoint key has invalid length {}; expected {}",
                endpoint.key.len(),
                PREFIX_LEN + EDGE_ID_LEN
            );
        }
        let edge_id = u64::from_be_bytes(
            endpoint.key[PREFIX_LEN..PREFIX_LEN + EDGE_ID_LEN]
                .try_into()
                .expect("validated edge endpoint key contains an edge ID"),
        );
        if maximum_edge_id.is_some_and(|previous| edge_id <= previous) {
            bail!("cannot resume source seed: edge endpoint keys are not strictly ordered");
        }
        maximum_edge_id = Some(edge_id);
        if current_edges >= base_edges {
            let ordinal = current_edges - base_edges;
            if ordinal >= args.scale_edges {
                bail!(
                    "cannot resume source seed: scale edge ordinal {ordinal} exceeds configured total {}",
                    args.scale_edges
                );
            }
            let (from_offset, to_offset) =
                scale_endpoints(args.distribution, ordinal, args.scale_nodes);
            let expected = (
                FIRST_SCALE_NODE_ID
                    .checked_add(from_offset)
                    .expect("generated source endpoint is below the validated scale node end"),
                FIRST_SCALE_NODE_ID
                    .checked_add(to_offset)
                    .expect("generated target endpoint is below the validated scale node end"),
            );
            let actual = graph::decode_endpoints(&endpoint.value).with_context(|| {
                format!("cannot resume source seed: edge {edge_id} has malformed endpoint bytes")
            })?;
            if actual != expected {
                bail!(
                    "cannot resume source seed: recovered edge distribution mismatch at ordinal {ordinal}: expected {expected:?}, found {actual:?}"
                );
            }
            all_scale_edge_ids.insert(edge_id);
            if let Some(checkpoint) = stored_checkpoint {
                if ordinal >= checkpoint.global_label_completed {
                    pending_global_edge_ids.insert(edge_id);
                }
                if ordinal >= checkpoint.rows_completed {
                    pending_row_edges.push(SeededScaleEdge {
                        id: edge_id,
                        ordinal,
                        from: actual.0,
                        to: actual.1,
                    });
                }
            }
        }
        current_edges = current_edges
            .checked_add(1)
            .context("source edge count overflows u64")?;
    }
    let first_missing_edge = current_edges.checked_sub(base_edges).with_context(|| {
        format!(
            "cannot resume source seed: found {current_edges} current edges but expected at least {base_edges} base edges"
        )
    })?;

    let progress = ScaleSeedProgress::recovered(
        first_missing_node,
        first_missing_edge,
        args.scale_nodes,
        args.scale_edges,
        args.seed_batch_rows,
    )?;
    drop(read);

    let actual_global_edge_ids = load_scale_global_edge_label(db).await?;
    let mut checkpoint = match stored_checkpoint {
        Some(checkpoint) => {
            if checkpoint.rows_completed > progress.edges {
                bail!(
                    "cannot resume source seed: row checkpoint {} exceeds recovered scale edges {}",
                    checkpoint.rows_completed,
                    progress.edges
                );
            }
            if checkpoint.global_label_completed > progress.edges {
                bail!(
                    "cannot resume source seed: global-label checkpoint {} exceeds recovered scale edges {}",
                    checkpoint.global_label_completed,
                    progress.edges
                );
            }
            if actual_global_edge_ids != all_scale_edge_ids {
                bail!(
                    "cannot resume source seed: global edge-label bitmap differs from the exact endpoint stream: actual_ids={}, expected_ids={}",
                    actual_global_edge_ids.len(),
                    all_scale_edge_ids.len()
                );
            }
            checkpoint
        }
        None => {
            if actual_global_edge_ids != all_scale_edge_ids {
                bail!(
                    "cannot bootstrap scale seed checkpoint: existing global edge-label bitmap differs from the exact endpoint stream: actual_ids={}, expected_ids={}",
                    actual_global_edge_ids.len(),
                    all_scale_edge_ids.len()
                );
            }
            persist_initial_scale_edge_checkpoint(db, progress.edges).await?
        }
    };

    if !pending_row_edges.is_empty() {
        let maximum_repair_rows = u64::try_from(args.seed_batch_rows.get())?;
        if u64::try_from(pending_row_edges.len())? > maximum_repair_rows {
            bail!(
                "cannot resume source seed: {} scale edge rows are incomplete, exceeding one seed batch of {maximum_repair_rows}",
                pending_row_edges.len()
            );
        }
        checkpoint = commit_scale_edge_rows(
            db,
            args.distribution,
            args.scale_edges,
            &pending_row_edges,
            progress.edges,
            checkpoint,
        )
        .await
        .context("failed to repair interrupted scale edge row batch")?;
    }
    if !pending_global_edge_ids.is_empty() {
        checkpoint = commit_scale_global_edge_label(
            db,
            &pending_global_edge_ids,
            progress.edges,
            checkpoint,
        )
        .await
        .context("failed to repair interrupted scale global edge-label checkpoint")?;
    }
    if checkpoint != ScaleEdgeSeedCheckpoint::initial(progress.edges) {
        bail!(
            "scale edge checkpoint repair is incomplete: rows={}, global={}, recovered={}",
            checkpoint.rows_completed,
            checkpoint.global_label_completed,
            progress.edges
        );
    }
    info!(
        recovered_nodes = progress.nodes,
        target_nodes = args.scale_nodes,
        recovered_edges = progress.edges,
        target_edges = args.scale_edges,
        "validated resumable source seed progress"
    );
    Ok(progress)
}

async fn open_hyperscale(
    database: &str,
    edge_policy: u8,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<HyperscaleDb> {
    HyperscaleDb::open(hyperscale_config(database, edge_policy, store))
        .await
        .context("failed to open hyperscale db")
}

fn hyperscale_config(
    database: &str,
    edge_policy: u8,
    store: Arc<dyn object_store::ObjectStore>,
) -> HyperscaleConfig {
    HyperscaleConfig::new(database, store)
        .with_edge_update_policy(edge_policy)
        .with_high_degree_threshold(2)
        .with_skip_startup_warm(true)
}

fn scenario_databases(args: &Args, scenario: Scenario) -> (String, String) {
    match &args.storage {
        Storage::Local => (DATABASE.to_string(), DATABASE.to_string()),
        Storage::Minio(config) => (
            format!("{}/{}/source/{DATABASE}", config.run_prefix, scenario.name),
            format!("{}/{}/target/{DATABASE}", config.run_prefix, scenario.name),
        ),
    }
}

fn scenario_rollback_database(args: &Args, scenario: Scenario) -> String {
    match &args.storage {
        Storage::Local => DATABASE.to_string(),
        Storage::Minio(config) => format!(
            "{}/{}/rollback/{DATABASE}",
            config.run_prefix, scenario.name
        ),
    }
}

fn build_source_store(
    args: &Args,
    local_root: &Path,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    match &args.storage {
        Storage::Local => Ok(Arc::new(
            LocalFileSystem::new_with_prefix(local_root).with_context(|| {
                format!(
                    "failed to create local object store {}",
                    local_root.display()
                )
            })?,
        )),
        Storage::Minio(config) => Ok(Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(&config.bucket)
                .with_endpoint(&config.endpoint)
                .with_access_key_id(&config.access_key)
                .with_secret_access_key(&config.secret_key)
                .with_allow_http(config.endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false)
                .build()
                .context("failed to build source MinIO object store")?,
        )),
    }
}

fn build_target_store(
    args: &Args,
    local_root: &Path,
) -> Result<Arc<dyn object_store_014::ObjectStore>> {
    match &args.storage {
        Storage::Local => Ok(Arc::new(
            object_store_014::local::LocalFileSystem::new_with_prefix(local_root).with_context(
                || {
                    format!(
                        "failed to create target object store {}",
                        local_root.display()
                    )
                },
            )?,
        )),
        Storage::Minio(config) => Ok(Arc::new(
            object_store_014::aws::AmazonS3Builder::new()
                .with_bucket_name(&config.bucket)
                .with_endpoint(&config.endpoint)
                .with_access_key_id(&config.access_key)
                .with_secret_access_key(&config.secret_key)
                .with_allow_http(config.endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false)
                .build()
                .context("failed to build target MinIO object store")?,
        )),
    }
}

async fn clear_object_prefix(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<()> {
    let prefix = object_store::path::Path::from(prefix);
    let mut objects = store.list(Some(&prefix));
    while let Some(object) = objects.try_next().await? {
        store
            .delete(&object.location)
            .await
            .with_context(|| format!("failed to clear object {}", object.location))?;
    }
    let mut remaining = store.list(Some(&prefix));
    if let Some(object) = remaining.try_next().await? {
        bail!(
            "object-store cleanup verification found remaining object {} under {}",
            object.location,
            prefix
        );
    }
    Ok(())
}

async fn clear_target_object_prefix(
    store: &Arc<dyn object_store_014::ObjectStore>,
    prefix: &str,
) -> Result<()> {
    use object_store_014::ObjectStoreExt as _;

    let prefix = object_store_014::path::Path::from(prefix);
    let mut objects = store.list(Some(&prefix));
    while let Some(object) = objects.try_next().await? {
        store
            .delete(&object.location)
            .await
            .with_context(|| format!("failed to clear target object {}", object.location))?;
    }
    Ok(())
}

async fn copy_object_prefix(
    store: &Arc<dyn object_store::ObjectStore>,
    source_prefix: &str,
    target_prefix: &str,
) -> Result<()> {
    let source_path = object_store::path::Path::from(source_prefix);
    let mut objects = store.list(Some(&source_path));
    let mut copied = 0_u64;
    while let Some(object) = objects.try_next().await? {
        let source = object.location.as_ref();
        let Some(suffix) = source.strip_prefix(source_prefix) else {
            bail!("listed object {source} is outside immutable source prefix {source_prefix}");
        };
        let target = object_store::path::Path::from(format!("{target_prefix}{suffix}"));
        store
            .copy(&object.location, &target)
            .await
            .with_context(|| {
                format!(
                    "failed to copy immutable object {} to {target}",
                    object.location
                )
            })?;
        copied = copied.saturating_add(1);
    }
    if copied == 0 {
        bail!("immutable source prefix {source_prefix} contained no objects");
    }
    info!(
        copied,
        source_prefix, target_prefix, "copied immutable object prefix"
    );
    Ok(())
}

async fn seed_hyperscale(
    db: &HyperscaleDb,
    scenario: Scenario,
    args: &Args,
    heartbeat: &ProgressHeartbeat,
) -> Result<report::SourceSeedResumeEvidence> {
    let initial_progress = if args.resume_source_seed {
        recover_scale_seed_progress(db, scenario, args).await?
    } else {
        let mut tx = db.write_tx().await.context("failed to start seed tx")?;
        for node_id in 1..=8_u64 {
            let mut properties = vec![
                HProperty::new("$label", "User"),
                HProperty::i64("rank", node_id as i64),
                HProperty::new("tier", if node_id % 2 == 0 { "premium" } else { "free" }),
                HProperty::new(
                    "bio",
                    match node_id {
                        1 => "migration migration parity alpha",
                        2 => "migration parity beta",
                        3 => "migration gamma",
                        _ => "unrelated fixture text",
                    },
                ),
                HProperty::new(
                    "embedding",
                    HPropertyValue::F32Array(match node_id {
                        1 => vec![0.0, 0.0, 0.0],
                        2 => vec![1.0, 0.0, 0.0],
                        3 => vec![0.0, 2.0, 0.0],
                        _ => vec![node_id as f32 + 10.0, 10.0, 0.0],
                    }),
                ),
            ];
            if node_id == 8 {
                properties.extend(exhaustive_property_values());
            }
            tx.add_node(node_id, Some(properties))
                .await
                .with_context(|| format!("failed to seed node {node_id}"))?;
        }
        for node_id in 30..=34_u64 {
            tx.add_node(
                node_id,
                Some(vec![
                    HProperty::new("$label", "Account"),
                    HProperty::i64("rank", node_id as i64),
                ]),
            )
            .await
            .with_context(|| format!("failed to seed node {node_id}"))?;
        }

        tx.add_edge(
            1,
            2,
            Some(vec![
                HProperty::new("$label", "FOLLOWS"),
                HProperty::new("kind", scenario.name),
                HProperty::i64("since", 10),
                HProperty::new("body", "edgeparitytoken alpha"),
                HProperty::new("embedding", HPropertyValue::F32Array(vec![0.0, 0.0, 0.0])),
            ]),
        )
        .await
        .context("failed to seed edge 1->2")?;
        tx.add_edge(
            1,
            3,
            Some(vec![
                HProperty::new("$label", "FOLLOWS"),
                HProperty::new("kind", "direct"),
                HProperty::i64("since", 11),
                HProperty::new("body", "edgeparitytoken beta"),
                HProperty::new("embedding", HPropertyValue::F32Array(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .await
        .context("failed to seed edge 1->3")?;
        tx.add_edge(
            2,
            3,
            Some(vec![
                HProperty::new("$label", "KNOWS"),
                HProperty::new("kind", "work"),
                HProperty::i64("since", 20),
            ]),
        )
        .await
        .context("failed to seed edge 2->3")?;
        tx.add_edge(
            30,
            31,
            Some(vec![
                HProperty::new("$label", "FOLLOWS"),
                HProperty::new("kind", "parallel-a"),
                HProperty::i64("since", 30),
                HProperty::new("body", "edge fixture text"),
                HProperty::new("embedding", HPropertyValue::F32Array(vec![10.0, 0.0, 0.0])),
            ]),
        )
        .await
        .context("failed to seed edge 30->31 a")?;
        tx.add_edge(
            30,
            31,
            Some(vec![
                HProperty::new("$label", "FOLLOWS"),
                HProperty::new("kind", "parallel-b"),
                HProperty::i64("since", 31),
                HProperty::new("body", "edge fixture text"),
                HProperty::new("embedding", HPropertyValue::F32Array(vec![11.0, 0.0, 0.0])),
            ]),
        )
        .await
        .context("failed to seed edge 30->31 b")?;

        if scenario.edge_policy != EDGE_UPDATE_EAGER {
            for to in 4..=8_u64 {
                tx.add_edge(
                    1,
                    to,
                    Some(vec![
                        HProperty::new("$label", "LAZY_LINK"),
                        HProperty::new("kind", "lazy"),
                        HProperty::i64("since", to as i64),
                    ]),
                )
                .await
                .with_context(|| format!("failed to seed lazy edge 1->{to}"))?;
            }
        }

        tx.commit().await.context("failed to commit seed tx")?;

        put_legacy_pair_row(
            db,
            4,
            5,
            &[
                HProperty::new("$label", "LEGACY_ONLY"),
                HProperty::new("kind", "raw-legacy"),
                HProperty::i64("since", 45),
            ],
        )
        .await?;
        put_legacy_pair_row(
            db,
            30,
            31,
            &[
                HProperty::new("$label", "FOLLOWS"),
                HProperty::new("kind", "parallel-c-legacy"),
                HProperty::i64("since", 32),
            ],
        )
        .await?;
        put_legacy_pair_row(
            db,
            1,
            2,
            &[
                HProperty::new("$label", "FOLLOWS"),
                HProperty::new("kind", scenario.name),
                HProperty::i64("since", 10),
            ],
        )
        .await?;
        ScaleSeedProgress::fresh()
    };
    if !args.resume_source_seed {
        // The base semantic fixture lives in otherwise quiet vector keyspaces.
        // Flush it before the long scale seed so a process interruption cannot
        // leave those rows dependent on WAL replay while graph keyspaces have
        // already accumulated millions of durable rows.
        db.inner_db()
            .flush()
            .await
            .context("failed to flush the base hyperscale fixture WAL")?;
        db.inner_db()
            .flush_with_options(SourceFlushOptions {
                flush_type: SourceFlushType::MemTable,
            })
            .await
            .context("failed to flush every base fixture keyspace to durable SSTs")?;
        source_semantic_evidence(db)
            .await
            .context("base semantic fixture failed after its durable pre-scale flush")?;
    }
    seed_scale_graph(db, args, initial_progress, heartbeat).await?;
    seed_exact_passthrough_rows(db).await?;
    db.inner_db()
        .flush()
        .await
        .context("failed to flush hyperscale WAL")?;
    db.inner_db()
        .flush_with_options(SourceFlushOptions {
            flush_type: SourceFlushType::MemTable,
        })
        .await
        .context("failed to flush every hyperscale keyspace to durable SSTs")?;
    Ok(initial_progress.evidence(args.resume_source_seed))
}

async fn source_semantic_evidence(db: &HyperscaleDb) -> Result<SourceSemanticEvidence> {
    let transaction = db
        .read_tx()
        .await
        .context("failed to begin source semantic parity queries")?;
    let mut node_text_hits = transaction
        .text_search_nodes("User", "bio", "migration parity", 8, None)
        .await
        .context("failed to execute source node text parity query")
        .map(|hits| {
            hits.into_iter()
                .map(|hit| report::TextHitEvidence {
                    entity_id: hit.entity_id,
                    score_bits: hit.score.to_bits(),
                })
                .collect::<Vec<_>>()
        })?;
    node_text_hits.sort_by(|left, right| {
        f32::from_bits(right.score_bits)
            .partial_cmp(&f32::from_bits(left.score_bits))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    let mut edge_text_hits = transaction
        .text_search_edges("FOLLOWS", "body", "edgeparitytoken", 8, None)
        .await
        .context("failed to execute source edge text parity query")
        .map(|hits| {
            hits.into_iter()
                .map(|hit| report::TextHitEvidence {
                    entity_id: hit.entity_id,
                    score_bits: hit.score.to_bits(),
                })
                .collect::<Vec<_>>()
        })?;
    edge_text_hits.sort_by(|left, right| {
        f32::from_bits(right.score_bits)
            .partial_cmp(&f32::from_bits(left.score_bits))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    let parameters = helix::db::index::hnsw::SearchParams::new(3);
    let node_vector_hits = transaction
        .vector_search::<helix::Euclidean>(
            &semantic_vector_index_name(helix::db::VectorElementType::Node),
            &VECTOR_QUERY,
            &parameters,
        )
        .await
        .context("failed to execute source node vector parity query")?
        .into_iter()
        .map(|hit| report::VectorHitEvidence {
            node_id: hit.node_id,
            distance_bits: hit.distance.to_bits(),
        })
        .collect::<Vec<_>>();
    let edge_vector_hits = transaction
        .vector_search::<helix::Euclidean>(
            &semantic_vector_index_name(helix::db::VectorElementType::Edge),
            &VECTOR_QUERY,
            &parameters,
        )
        .await
        .context("failed to execute source edge vector parity query")?
        .into_iter()
        .map(|hit| report::VectorHitEvidence {
            node_id: hit.node_id,
            distance_bits: hit.distance.to_bits(),
        })
        .collect::<Vec<_>>();

    let expected_node = expected_vector_hits();
    let expected_edge = expected_edge_vector_hits();
    if node_vector_hits != expected_node || edge_vector_hits != expected_edge {
        bail!(
            "durable source vector fixtures differ from their independent contracts: expected_node={expected_node:?}, actual_node={node_vector_hits:?}, expected_edge={expected_edge:?}, actual_edge={edge_vector_hits:?}"
        );
    }
    Ok(SourceSemanticEvidence {
        node_text_hits,
        edge_text_hits,
        node_vector_metadata: source_vector_metadata(db, helix::db::VectorElementType::Node)
            .await?,
        edge_vector_metadata: source_vector_metadata(db, helix::db::VectorElementType::Edge)
            .await?,
        node_vector_hits,
        edge_vector_hits,
        vector_non_metadata_namespace_digests: source_vector_non_metadata_digests(db).await?,
    })
}

async fn source_vector_metadata(
    db: &HyperscaleDb,
    element_type: helix::db::VectorElementType,
) -> Result<report::SourceVectorMetadataEvidence> {
    let index_name = semantic_vector_index_name(element_type);
    let metadata = helix::db::index::hnsw::VectorIndex::<helix::Euclidean>::new(&index_name)
        .get_metadata(&db.inner_db())
        .await?
        .with_context(|| format!("source vector metadata `{index_name}` is missing"))?;
    Ok(report::SourceVectorMetadataEvidence {
        index_name: metadata.config.index_name,
        property_name: metadata.config.property_name,
        dimension: metadata.config.dimension,
        m: metadata.config.m,
        m0: metadata.config.m0,
        ef_construction: metadata.config.ef_construction,
        ml_bits: metadata.config.ml.to_bits(),
        simhash_threshold: metadata.config.simhash_threshold,
        sampling_ratio_bits: metadata.config.sampling_ratio.to_bits(),
        adaptive_enabled: metadata.config.adaptive_enabled,
        adaptive_failure_probability_bits: metadata.config.adaptive_failure_prob.to_bits(),
        entry_point: metadata.entry_point,
        max_layer: metadata.max_layer,
        count: metadata.count,
    })
}

async fn source_vector_non_metadata_digests(db: &HyperscaleDb) -> Result<BTreeMap<u64, String>> {
    const INDEX_ID_LEN: usize = core::mem::size_of::<u64>();
    const CORE_INDEX_ID_OFFSET: usize = 2;
    const CORE_KIND_OFFSET: usize = CORE_INDEX_ID_OFFSET + INDEX_ID_LEN;
    const VECTOR_METADATA_KIND: u8 = 0x01;
    const HOT_INDEX_ID_OFFSET: usize = 1;
    const LAYER0_INDEX_ID_OFFSET: usize = 1;
    const READ_AHEAD_BYTES: usize = 2 * 1024 * 1024;
    const MAXIMUM_FETCH_TASKS: usize = 16;

    let options = SourceScanOptions::default()
        .with_read_ahead_bytes(READ_AHEAD_BYTES)
        .with_cache_blocks(false)
        .with_max_fetch_tasks(MAXIMUM_FETCH_TASKS);
    let mut digests = BTreeMap::<u64, Sha256>::new();
    for (prefix, index_id_offset, metadata_kind_offset) in [
        (
            Bytes::from_static(&[0x03, 0x03]),
            CORE_INDEX_ID_OFFSET,
            Some(CORE_KIND_OFFSET),
        ),
        (Bytes::from_static(&[0xF0]), HOT_INDEX_ID_OFFSET, None),
        (Bytes::from_static(&[0xF1]), LAYER0_INDEX_ID_OFFSET, None),
    ] {
        let mut rows = db
            .inner_db()
            .scan_prefix_with_options(prefix, &options)
            .await?;
        while let Some(row) = rows.next().await? {
            if metadata_kind_offset.is_some() && !matches!(row.key.len(), 10 | 11) {
                continue;
            }
            if row.key.len() < index_id_offset + INDEX_ID_LEN {
                bail!("source vector row is shorter than its physical index ID");
            }
            if metadata_kind_offset
                .is_some_and(|offset| row.key.get(offset).copied() == Some(VECTOR_METADATA_KIND))
            {
                continue;
            }
            let physical_id = u64::from_be_bytes(
                row.key[index_id_offset..index_id_offset + INDEX_ID_LEN]
                    .try_into()
                    .expect("length-checked source vector index ID is eight bytes"),
            );
            let digest = digests.entry(physical_id).or_default();
            digest.update(u64::try_from(row.key.len())?.to_be_bytes());
            digest.update(&row.key);
            digest.update(u64::try_from(row.value.len())?.to_be_bytes());
            digest.update(&row.value);
        }
    }
    Ok(digests
        .into_iter()
        .map(|(physical_id, digest)| {
            (
                physical_id,
                digest
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            )
        })
        .collect())
}

fn assert_vector_adoption_evidence(
    scenario: &str,
    source: &SourceSemanticEvidence,
    target: &db::migration_parity::MigrationParityIndexState,
) -> Result<()> {
    let stats = &target.v2.vector_migration;
    if stats.adopted_indexes != 2
        || stats.rebuilt_indexes != 0
        || stats.reused_physical_ids.len() != 2
        || stats.validated_rows == 0
        || stats.validated_bytes == 0
        || stats.logical_output_operations != stats.adopted_indexes
        || stats.logical_output_bytes == 0
    {
        bail!("{scenario}: vector adoption counters violate the zero-rebuild contract: {stats:?}");
    }
    for physical_id in &stats.reused_physical_ids {
        let source_digest = source
            .vector_non_metadata_namespace_digests
            .get(physical_id)
            .with_context(|| {
                format!("{scenario}: adopted physical ID {physical_id} is absent from the source")
            })?;
        let target_digest = target
            .vector_non_metadata_namespace_digests
            .get(physical_id)
            .with_context(|| {
                format!("{scenario}: adopted physical ID {physical_id} is absent from the target")
            })?;
        if target_digest != source_digest {
            bail!(
                "{scenario}: adopted physical ID {physical_id} changed non-metadata bytes: source={source_digest}, target={target_digest}"
            );
        }
    }
    Ok(())
}

fn semantic_vector_index_name(element_type: helix::db::VectorElementType) -> String {
    let label = match element_type {
        helix::db::VectorElementType::Node => "User",
        helix::db::VectorElementType::Edge => "FOLLOWS",
    };
    helix::db::index::vector_index_name(element_type, label, "embedding")
}

fn expected_vector_hits() -> Vec<report::VectorHitEvidence> {
    EXPECTED_VECTOR_HITS
        .into_iter()
        .map(|(node_id, distance_bits)| report::VectorHitEvidence {
            node_id,
            distance_bits,
        })
        .collect()
}

fn expected_edge_vector_hits() -> Vec<report::VectorHitEvidence> {
    [
        (0_u64, [0.0, 0.0, 0.0]),
        (1, [1.0, 0.0, 0.0]),
        (3, [10.0, 0.0, 0.0]),
    ]
    .into_iter()
    .map(|(node_id, vector)| report::VectorHitEvidence {
        node_id,
        distance_bits: VECTOR_QUERY
            .iter()
            .zip(vector)
            .map(|(query, value)| {
                let delta = query - value;
                delta * delta
            })
            .sum::<f32>()
            .to_bits(),
    })
    .collect()
}

async fn verify_semantic_queries(
    migrated: &HelixDB,
    scenario: &str,
    source: SourceSemanticEvidence,
) -> Result<(report::TextQueryEvidence, report::VectorQueryEvidence)> {
    let mut text_cases = Vec::with_capacity(2);
    for (element_kind, query, source_hits) in [
        ("node", "migration parity", source.node_text_hits),
        ("edge", "edgeparitytoken", source.edge_text_hits),
    ] {
        let target_manifests = migrated
            .migration_parity_text_search(query, 8)
            .await
            .with_context(|| format!("failed to execute migrated {element_kind} text query"))?;
        let populated = target_manifests
            .iter()
            .filter(|manifest| !manifest.hits.is_empty())
            .collect::<Vec<_>>();
        if populated.len() != 1 {
            bail!(
                "{scenario}: expected one populated {element_kind} text manifest, found {}",
                populated.len()
            );
        }
        let target_hits = populated[0]
            .hits
            .iter()
            .map(|hit| report::TextHitEvidence {
                entity_id: hit.entity_id,
                score_bits: hit.score_bits,
            })
            .collect::<Vec<_>>();
        if target_hits != source_hits {
            bail!(
                "{scenario}: {element_kind} text search parity differs: source={source_hits:?}, target={target_hits:?}"
            );
        }
        text_cases.push(report::TextQueryCaseEvidence {
            element_kind,
            query: query.to_string(),
            source_hits,
            target_manifests,
        });
    }
    let text_query = report::TextQueryEvidence {
        cases: text_cases,
        passed: true,
    };

    let mut vector_cases = Vec::with_capacity(2);
    for (element_kind, element_type, source_metadata, source_hits, expected_hits) in [
        (
            "node",
            helix::db::VectorElementType::Node,
            source.node_vector_metadata,
            source.node_vector_hits,
            expected_vector_hits(),
        ),
        (
            "edge",
            helix::db::VectorElementType::Edge,
            source.edge_vector_metadata,
            source.edge_vector_hits,
            expected_edge_vector_hits(),
        ),
    ] {
        if source_hits != expected_hits {
            bail!(
                "{scenario}: saved {element_kind} vector evidence differs from its fixture contract: expected={expected_hits:?}, source={source_hits:?}"
            );
        }
        let index_name = semantic_vector_index_name(element_type);
        let target_metadata = migrated
            .migration_parity_vector_metadata(&index_name)
            .await
            .with_context(|| format!("failed to read migrated {element_kind} vector metadata"))?;
        if source_metadata.index_name != index_name
            || target_metadata.property_name != source_metadata.property_name
            || target_metadata.dimension != source_metadata.dimension
            || target_metadata.m != source_metadata.m
            || target_metadata.m0 != source_metadata.m0
            || target_metadata.ef_construction != source_metadata.ef_construction
            || target_metadata.ml.to_bits() != source_metadata.ml_bits
            || target_metadata.simhash_threshold != source_metadata.simhash_threshold
            || target_metadata.sampling_ratio.to_bits() != source_metadata.sampling_ratio_bits
            || target_metadata.adaptive_enabled != source_metadata.adaptive_enabled
            || target_metadata.adaptive_failure_prob.to_bits()
                != source_metadata.adaptive_failure_probability_bits
            || target_metadata.entry_point != source_metadata.entry_point
            || target_metadata.max_layer != source_metadata.max_layer
            || target_metadata.count != source_metadata.count
        {
            let v2 = migrated.migration_parity_v2_state().await?;
            bail!(
                "{scenario}: {element_kind} vector physical metadata differs: source={source_metadata:?}, target={target_metadata:?}; v2={v2:?}"
            );
        }
        let target_hits = migrated
            .migration_parity_vector_search(&index_name, &VECTOR_QUERY, 3)
            .await
            .with_context(|| format!("failed to execute migrated {element_kind} vector query"))?
            .into_iter()
            .map(|hit| report::VectorHitEvidence {
                node_id: hit.node_id,
                distance_bits: hit.distance_bits,
            })
            .collect::<Vec<_>>();
        if target_hits != expected_hits {
            bail!(
                "{scenario}: {element_kind} vector parity differs: expected={expected_hits:?}, source={source_hits:?}, target={target_hits:?}"
            );
        }
        vector_cases.push(report::VectorQueryCaseEvidence {
            element_kind,
            index_name,
            source_metadata,
            target_metadata,
            query_bits: VECTOR_QUERY.into_iter().map(f32::to_bits).collect(),
            source_hits,
            target_hits,
        });
    }
    let vector_query = report::VectorQueryEvidence {
        cases: vector_cases,
        passed: true,
    };
    Ok((text_query, vector_query))
}

async fn target_semantic_evidence(migrated: &HelixDB) -> Result<TargetSemanticEvidence> {
    let node_text = migrated
        .migration_parity_text_search("migration parity", 8)
        .await
        .context("failed to capture target node text evidence")?;
    let edge_text = migrated
        .migration_parity_text_search("edgeparitytoken", 8)
        .await
        .context("failed to capture target edge text evidence")?;
    let node_index_name = semantic_vector_index_name(helix::db::VectorElementType::Node);
    let edge_index_name = semantic_vector_index_name(helix::db::VectorElementType::Edge);
    let node_vector_metadata = migrated
        .migration_parity_vector_metadata(&node_index_name)
        .await
        .context("failed to capture target node vector metadata")?;
    let edge_vector_metadata = migrated
        .migration_parity_vector_metadata(&edge_index_name)
        .await
        .context("failed to capture target edge vector metadata")?;
    let node_vector_hits = migrated
        .migration_parity_vector_search(&node_index_name, &VECTOR_QUERY, 8)
        .await
        .context("failed to capture target node vector hits")?;
    let edge_vector_hits = migrated
        .migration_parity_vector_search(&edge_index_name, &VECTOR_QUERY, 8)
        .await
        .context("failed to capture target edge vector hits")?;
    Ok(TargetSemanticEvidence {
        node_text,
        edge_text,
        node_vector_metadata,
        edge_vector_metadata,
        node_vector_hits,
        edge_vector_hits,
    })
}

async fn seed_scale_graph(
    db: &HyperscaleDb,
    args: &Args,
    initial_progress: ScaleSeedProgress,
    heartbeat: &ProgressHeartbeat,
) -> Result<()> {
    if args.scale_nodes == 0 {
        return Ok(());
    }
    FIRST_SCALE_NODE_ID
        .checked_add(args.scale_nodes)
        .context("scale node IDs overflow u64")?;

    let seed_started = Instant::now();
    heartbeat.set_phase("source_scale_seed");
    let batch_rows = u64::try_from(args.seed_batch_rows.get())?;
    let mut created_nodes = initial_progress.nodes;
    while created_nodes < args.scale_nodes {
        let batch_end = created_nodes
            .saturating_add(batch_rows)
            .min(args.scale_nodes);
        let mut transaction = db
            .write_tx()
            .await
            .context("failed to begin scale node transaction")?;
        for offset in created_nodes..batch_end {
            let node_id = FIRST_SCALE_NODE_ID + offset;
            transaction
                .add_node(
                    node_id,
                    Some(scale_node_properties(offset, args.scale_nodes)),
                )
                .await
                .with_context(|| format!("failed to seed scale node {node_id}"))?;
        }
        transaction
            .commit()
            .await
            .context("failed to commit scale node transaction")?;
        created_nodes = batch_end;
        heartbeat.set_processed(created_nodes);
        if created_nodes == args.scale_nodes
            || created_nodes % SCALE_SEED_PROGRESS_INTERVAL < batch_rows
        {
            log_seed_progress(
                "nodes",
                initial_progress.nodes,
                created_nodes,
                args.scale_nodes,
                seed_started,
            );
        }
    }

    let edges_started = Instant::now();
    let mut created_edges = initial_progress.edges;
    let mut edge_checkpoint = match load_scale_edge_checkpoint(db).await? {
        Some(checkpoint) => checkpoint,
        None => persist_initial_scale_edge_checkpoint(db, created_edges).await?,
    };
    if edge_checkpoint != ScaleEdgeSeedCheckpoint::initial(created_edges) {
        bail!(
            "scale edge checkpoint does not match initial progress: rows={}, global={}, initial={created_edges}",
            edge_checkpoint.rows_completed,
            edge_checkpoint.global_label_completed
        );
    }
    let mut pending_global_edge_ids = RoaringTreemap::new();
    while created_edges < args.scale_edges {
        let batch_end = created_edges
            .saturating_add(batch_rows)
            .min(args.scale_edges);
        let mut transaction = db
            .write_tx()
            .await
            .context("failed to begin scale edge transaction")?;
        let batch_capacity = usize::try_from(batch_end - created_edges)?;
        let mut seeded_edges = Vec::with_capacity(batch_capacity);
        for ordinal in created_edges..batch_end {
            let (from_offset, to_offset) =
                scale_endpoints(args.distribution, ordinal, args.scale_nodes);
            let from = FIRST_SCALE_NODE_ID + from_offset;
            let to = FIRST_SCALE_NODE_ID + to_offset;
            let edge_id = transaction
                .add_edge(
                    from,
                    to,
                    Some(scale_edge_properties(
                        args.distribution,
                        ordinal,
                        args.scale_edges,
                    )),
                )
                .await
                .with_context(|| format!("failed to seed scale edge {ordinal}"))?;
            seeded_edges.push(SeededScaleEdge {
                id: edge_id,
                ordinal,
                from,
                to,
            });
        }
        transaction
            .commit()
            .await
            .context("failed to commit scale edge transaction")?;
        edge_checkpoint = commit_scale_edge_rows(
            db,
            args.distribution,
            args.scale_edges,
            &seeded_edges,
            batch_end,
            edge_checkpoint,
        )
        .await
        .context("failed to commit aggregated scale edge rows")?;
        pending_global_edge_ids.extend(seeded_edges.iter().map(|edge| edge.id));
        if should_commit_scale_global_edge_label(
            edge_checkpoint,
            batch_end,
            args.scale_edges,
            batch_rows,
        ) {
            edge_checkpoint = commit_scale_global_edge_label(
                db,
                &pending_global_edge_ids,
                batch_end,
                edge_checkpoint,
            )
            .await
            .context("failed to commit aggregated scale global edge-label index")?;
            pending_global_edge_ids.clear();
        }
        created_edges = batch_end;
        heartbeat.set_processed(args.scale_nodes.saturating_add(created_edges));
        if created_edges == args.scale_edges
            || created_edges % SCALE_SEED_PROGRESS_INTERVAL < batch_rows
        {
            log_seed_progress(
                "edges",
                initial_progress.edges,
                created_edges,
                args.scale_edges,
                edges_started,
            );
        }
    }
    if !pending_global_edge_ids.is_empty()
        || edge_checkpoint != ScaleEdgeSeedCheckpoint::initial(args.scale_edges)
    {
        bail!(
            "scale edge seed ended with incomplete checkpoints: rows={}, global={}, total={}, pending_global_ids={}",
            edge_checkpoint.rows_completed,
            edge_checkpoint.global_label_completed,
            args.scale_edges,
            pending_global_edge_ids.len()
        );
    }
    Ok(())
}

fn should_commit_scale_global_edge_label(
    checkpoint: ScaleEdgeSeedCheckpoint,
    completed: u64,
    total: u64,
    batch_rows: u64,
) -> bool {
    let next_checkpoint = if checkpoint.global_label_completed == 0 {
        batch_rows
    } else {
        checkpoint.global_label_completed.saturating_mul(2)
    };
    completed == total || completed >= next_checkpoint
}

fn scale_endpoints(distribution: GraphDistribution, ordinal: u64, nodes: u64) -> (u64, u64) {
    let uniform_to = ordinal
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1)
        % nodes;
    match distribution {
        GraphDistribution::Uniform => (ordinal % nodes, uniform_to),
        GraphDistribution::PowerLaw => {
            let ranks = nodes.min(10_000);
            let rank = ordinal % ranks + 1;
            ((nodes / rank).saturating_sub(1), uniform_to)
        }
        GraphDistribution::Star => (0, (ordinal % nodes.saturating_sub(1).max(1) + 1) % nodes),
        GraphDistribution::Dense => {
            let width = nodes.min(4_096);
            ((ordinal / width) % nodes, ordinal % width)
        }
        GraphDistribution::SelfLoop => {
            let node = ordinal % nodes;
            (node, node)
        }
        GraphDistribution::HotPair => (0, u64::from(nodes > 1)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SeedProgressMetrics {
    processed_this_run: u64,
    rows_per_second: f64,
    eta_seconds: f64,
}

fn seed_progress_metrics(
    initial_completed: u64,
    completed: u64,
    total: u64,
    elapsed: Duration,
) -> SeedProgressMetrics {
    assert!(initial_completed <= completed);
    assert!(completed <= total);
    let elapsed = elapsed.as_secs_f64().max(f64::EPSILON);
    let processed_this_run = completed - initial_completed;
    let rows_per_second = processed_this_run as f64 / elapsed;
    let eta_seconds = (total.saturating_sub(completed)) as f64 / rows_per_second;
    SeedProgressMetrics {
        processed_this_run,
        rows_per_second,
        eta_seconds,
    }
}

fn log_seed_progress(
    kind: &str,
    initial_completed: u64,
    completed: u64,
    total: u64,
    started: Instant,
) {
    let metrics = seed_progress_metrics(initial_completed, completed, total, started.elapsed());
    info!(
        kind,
        initial_completed,
        completed,
        processed_this_run = metrics.processed_this_run,
        total,
        rows_per_second = metrics.rows_per_second,
        eta_seconds = metrics.eta_seconds,
        "scale fixture progress"
    );
}

fn exhaustive_property_values() -> Vec<HProperty> {
    let nested_object = BTreeMap::from([
        ("empty".to_string(), HPropertyValue::String(String::new())),
        (
            "nested".to_string(),
            HPropertyValue::Array(vec![HPropertyValue::Bool(false), HPropertyValue::I64(-1)]),
        ),
    ]);
    vec![
        HProperty::new("duplicate", HPropertyValue::F64(f64::from_bits(1))),
        HProperty::new("duplicate", HPropertyValue::F64(f64::from_bits(2))),
        HProperty::new("null", HPropertyValue::Null),
        HProperty::new("bool_false", HPropertyValue::Bool(false)),
        HProperty::new("bool_true", HPropertyValue::Bool(true)),
        HProperty::new("i64_min", HPropertyValue::I64(i64::MIN)),
        HProperty::new("i64_max", HPropertyValue::I64(i64::MAX)),
        HProperty::new("datetime", HPropertyValue::DateTime(-1_234_567_890)),
        HProperty::new("f64_negative_zero", HPropertyValue::F64(-0.0)),
        HProperty::new(
            "f64_nan_payload",
            HPropertyValue::F64(f64::from_bits(0x7ff8_0000_0000_0042)),
        ),
        HProperty::new(
            "f32_storage_bits",
            HPropertyValue::F32(f64::from_bits(0x8000_0000_0000_0000)),
        ),
        HProperty::new("empty_string", HPropertyValue::String(String::new())),
        HProperty::new("unicode", HPropertyValue::String("nul\0é🦀𝄞".to_string())),
        HProperty::new("empty_bytes", HPropertyValue::Bytes(Vec::new())),
        HProperty::new("bytes", HPropertyValue::Bytes(vec![0x00, 0xFF, 0x7F, 0x80])),
        HProperty::new(
            "i64_array",
            HPropertyValue::I64Array(vec![i64::MIN, 0, i64::MAX]),
        ),
        HProperty::new(
            "f64_array",
            HPropertyValue::F64Array(vec![-0.0, f64::from_bits(0x7ff8_0000_0000_0043)]),
        ),
        HProperty::new(
            "f32_array",
            HPropertyValue::F32Array(vec![-0.0, f32::from_bits(0x7fc0_0042)]),
        ),
        HProperty::new(
            "string_array",
            HPropertyValue::StringArray(vec![String::new(), "é".to_string(), "é".to_string()]),
        ),
        HProperty::new(
            "array",
            HPropertyValue::Array(vec![
                HPropertyValue::Null,
                HPropertyValue::Bytes(vec![1, 2, 3]),
                HPropertyValue::Object(nested_object.clone()),
            ]),
        ),
        HProperty::new("object", HPropertyValue::Object(nested_object)),
        HProperty::new("empty_array", HPropertyValue::Array(Vec::new())),
        HProperty::new("empty_object", HPropertyValue::Object(BTreeMap::new())),
    ]
}

async fn seed_exact_passthrough_rows(db: &HyperscaleDb) -> Result<()> {
    let mut rows = vec![
        (b"\x77unknown-key".to_vec(), b"unknown-value\0\xff".to_vec()),
        (
            b"\xffparity_text_manifest:parity-fixture".to_vec(),
            b"{\"logical_version\":7,\"blob\":\"fixture\"}".to_vec(),
        ),
        (
            b"\xffparity_text_live:parity-fixture:00000000000000000008".to_vec(),
            b"{\"logical_version\":7,\"live\":true}".to_vec(),
        ),
        (
            b"\xffparity_text_version:parity-fixture".to_vec(),
            7_u64.to_be_bytes().to_vec(),
        ),
    ];
    for tenant_prefix in [[0x77_u8; 16], [0x88_u8; 16]] {
        for logical_prefix in [0x00_u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0xFF] {
            let mut key = tenant_prefix.to_vec();
            key.push(logical_prefix);
            key.extend_from_slice(&8_u64.to_be_bytes());
            rows.push((
                key,
                format!("tenant-isolated-{tenant_prefix:?}-{logical_prefix:02x}").into_bytes(),
            ));
        }
    }
    rows.extend(vector_passthrough_rows());

    let raw = db.inner_db();
    let transaction = raw.begin(IsolationLevel::Snapshot).await?;
    for (key, value) in rows {
        transaction.put(key, value)?;
    }
    transaction.commit().await?;
    Ok(())
}

fn vector_passthrough_rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    const NODE_ID: u64 = 8;
    const OTHER_NODE_ID: u64 = 7;
    let index_id = db::search::vector::index_id_from_name("parity-vector");
    let index = index_id.to_be_bytes();
    let node = NODE_ID.to_be_bytes();
    let other = OTHER_NODE_ID.to_be_bytes();
    let mut rows = vec![(
        [&[0x03, 0x03][..], &index, &[0x01]].concat(),
        db::migration_parity::migration_parity_empty_vector_metadata(
            "parity-vector",
            "embedding",
            3,
        ),
    )];
    let mut push = |key: Vec<u8>, kind: &str| rows.push((key, kind.as_bytes().to_vec()));
    push([&[0x03, 0x03][..], &index].concat(), "index-prefix");
    push([&[0x03, 0x03][..], &index, &[0x09]].concat(), "txn-guard");
    push([&[0xF0][..], &index].concat(), "memory-prefix");
    push([&[0xF1][..], &index].concat(), "l0-prefix");
    push(
        [&[0xF0][..], &index, &[0x16], &node].concat(),
        "layer0-neighbors",
    );
    push(
        [&[0xF1][..], &index, &[0x02], &99_u64.to_be_bytes(), &node].concat(),
        "vector",
    );
    push([&[0xF1][..], &index, &[0x04]].concat(), "candidate-prefix");
    push(
        [&[0xF1][..], &index, &[0x04], &3_u16.to_be_bytes(), &node].concat(),
        "candidate-sorted",
    );
    push(
        [&[0xF1][..], &index, &[0x05], &node].concat(),
        "candidate-node",
    );
    push(
        [&[0xF0][..], &index, &[0x11], &3_u16.to_be_bytes(), &node].concat(),
        "upper-neighbors",
    );
    push([&[0xF0][..], &index, &[0x12], &node].concat(), "simhash");
    push(
        [&[0xF0][..], &index, &[0x13], &node].concat(),
        "upper-vector",
    );
    push(
        [&[0xF1][..], &index, &[0x15], &node].concat(),
        "reverse-prefix",
    );
    push(
        [
            &[0xF1][..],
            &index,
            &[0x15],
            &node,
            &3_u16.to_be_bytes(),
            &other,
        ]
        .concat(),
        "reverse-edge",
    );
    rows
}

const BLOB_FIXTURE: &[u8] = b"migration parity text blob\0with exact bytes\xff";

async fn seed_blob_fixture(
    store: &Arc<dyn object_store::ObjectStore>,
    database: &str,
) -> Result<()> {
    use object_store::ObjectStore as _;
    use sha2::Digest as _;

    let digest = sha2::Sha256::digest(BLOB_FIXTURE);
    let path =
        object_store::path::Path::from(format!("{database}/text/blobs/{}", hex::encode(digest)));
    store
        .put(&path, Bytes::from_static(BLOB_FIXTURE).into())
        .await?;
    Ok(())
}

async fn verify_blob_fixture(
    store: &Arc<dyn object_store_014::ObjectStore>,
    database: &str,
) -> Result<()> {
    use object_store_014::ObjectStoreExt as _;
    use sha2::Digest as _;

    let expected_digest = sha2::Sha256::digest(BLOB_FIXTURE);
    let path = object_store_014::path::Path::from(format!(
        "{database}/text/blobs/{}",
        hex::encode(expected_digest)
    ));
    let bytes = store.get(&path).await?.bytes().await?;
    if bytes.as_ref() != BLOB_FIXTURE {
        bail!("referenced text blob bytes changed at {path}");
    }
    let actual_digest = sha2::Sha256::digest(&bytes);
    if actual_digest.as_slice() != expected_digest.as_slice() {
        bail!("referenced text blob SHA-256 changed at {path}");
    }
    Ok(())
}

async fn run_target_garbage_collection(
    database: &str,
    store: &Arc<dyn object_store_014::ObjectStore>,
    expiry_mode: CheckpointExpiryMode,
) -> Result<GarbageCollectionCheckpointEvidence> {
    let inspection_admin =
        target_slatedb::admin::AdminBuilder::new(database, Arc::clone(store)).build();
    let checkpoints = inspection_admin.list_checkpoints(None).await?;
    let expiring_checkpoints = checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.expire_time.is_some())
        .count();
    let permanent_checkpoints = checkpoints.len().saturating_sub(expiring_checkpoints);
    let now = Utc::now();
    let clock_advance = match expiry_mode {
        CheckpointExpiryMode::RealTime => Duration::ZERO,
        CheckpointExpiryMode::AdvancePastLatest => checkpoints
            .iter()
            .filter_map(|checkpoint| checkpoint.expire_time)
            .max()
            .and_then(|expiry| expiry.signed_duration_since(now).to_std().ok())
            .map(|until_expiry| until_expiry.saturating_add(Duration::from_secs(1)))
            .unwrap_or_default(),
    };
    let directory = target_slatedb::config::GarbageCollectorDirectoryOptions {
        interval: None,
        min_age: Duration::ZERO,
        dry_run: false,
    };
    let options = target_slatedb::config::GarbageCollectorOptions {
        manifest_options: Some(directory),
        wal_options: Some(directory),
        wal_fence_options: None,
        compacted_options: Some(directory),
        compactions_options: Some(directory),
        detach_options: None,
        metric_level: None,
        boundary_files_enabled: true,
        object_store_max_retries: None,
    };
    let clock: Arc<dyn SystemClock> = Arc::new(ShiftedSystemClock {
        advance: clock_advance,
    });
    target_slatedb::admin::AdminBuilder::new(database, Arc::clone(store))
        .with_system_clock(clock)
        .build()
        .run_gc_once(options)
        .await?;
    Ok(GarbageCollectionCheckpointEvidence {
        clock_advance_millis: u64::try_from(clock_advance.as_millis()).unwrap_or(u64::MAX),
        checkpoints: u64::try_from(checkpoints.len()).unwrap_or(u64::MAX),
        expiring_checkpoints: u64::try_from(expiring_checkpoints).unwrap_or(u64::MAX),
        permanent_checkpoints: u64::try_from(permanent_checkpoints).unwrap_or(u64::MAX),
    })
}

async fn object_prefix_size(
    store: &Arc<dyn object_store_014::ObjectStore>,
    database: &str,
) -> Result<u64> {
    let prefix = object_store_014::path::Path::from(database);
    let mut objects = store.list(Some(&prefix));
    let mut bytes = 0_u64;
    while let Some(object) = objects.try_next().await? {
        bytes = bytes.saturating_add(object.size);
    }
    Ok(bytes)
}

async fn exercise_target_fault(
    store: &Arc<dyn object_store_014::ObjectStore>,
    database: &str,
    recorder: &Arc<ObjectStoreRecorder>,
    fault: TargetFault,
    maximum_attempts: NonZeroUsize,
) -> Result<u64> {
    use object_store_014::{ObjectStore as _, ObjectStoreExt as _};

    let probe_prefix = format!("{database}/migration-parity-object-store-fault-probe");
    let prefix = object_store_014::path::Path::from(probe_prefix.clone());
    let source = object_store_014::path::Path::from(format!("{probe_prefix}/source"));
    let target = object_store_014::path::Path::from(format!("{probe_prefix}/target"));
    let payload = Bytes::from_static(b"migration parity object-store fault probe");
    let initial_injected_errors = recorder.snapshot().injected_errors;
    let mut attempts = 0_u64;
    let injection_attempt_limit = maximum_attempts
        .get()
        .checked_sub(1)
        .context("target object-store fault recovery requires at least two attempts")?;

    for _ in 0..injection_attempt_limit {
        attempts = attempts.saturating_add(1);
        let result =
            exercise_target_fault_once(store, fault.operation, &source, &target, &payload).await;
        if recorder.snapshot().injected_errors > initial_injected_errors {
            if result.is_ok() {
                bail!(
                    "target object-store operation {:?} recorded an injected error but returned success",
                    fault.operation
                );
            }
            attempts = attempts.saturating_add(1);
            exercise_target_fault_once(store, fault.operation, &source, &target, &payload)
                .await
                .with_context(|| {
                    format!(
                        "target object-store operation {:?} did not recover after its injected failure",
                        fault.operation
                    )
                })?;
            break;
        }
        result.with_context(|| {
            format!(
                "target object-store operation {:?} failed before the configured injection",
                fault.operation
            )
        })?;
    }

    if recorder.snapshot().injected_errors == initial_injected_errors {
        bail!(
            "target object-store operation {:?} did not inject within {} attempts",
            fault.operation,
            maximum_attempts
        );
    }
    if usize::try_from(attempts).unwrap_or(usize::MAX) > maximum_attempts.get() {
        bail!(
            "target object-store operation {:?} required {attempts} attempts, exceeding the configured limit {maximum_attempts}",
            fault.operation
        );
    }

    let mut objects = store.list(Some(&prefix));
    while let Some(object) = objects.try_next().await? {
        store.delete(&object.location).await.with_context(|| {
            format!(
                "failed to delete object-store fault probe {}",
                object.location
            )
        })?;
    }
    Ok(attempts)
}

async fn exercise_target_fault_once(
    store: &Arc<dyn object_store_014::ObjectStore>,
    operation: Operation,
    source: &object_store_014::path::Path,
    target: &object_store_014::path::Path,
    payload: &Bytes,
) -> object_store_014::Result<()> {
    use object_store_014::{ObjectStore as _, ObjectStoreExt as _};

    match operation {
        Operation::Get => {
            store.put(source, payload.clone().into()).await?;
            store.get(source).await?.bytes().await?;
        }
        Operation::Head => {
            store.put(source, payload.clone().into()).await?;
            store.head(source).await?;
        }
        Operation::Put => {
            store.put(source, payload.clone().into()).await?;
        }
        Operation::Multipart => {
            let mut upload = store.put_multipart(target).await?;
            if let Err(error) = upload.put_part(payload.clone().into()).await {
                let _ = upload.abort().await;
                return Err(error);
            }
            upload.complete().await?;
        }
        Operation::List => {
            store.put(source, payload.clone().into()).await?;
            store.list(Some(source)).try_collect::<Vec<_>>().await?;
        }
        Operation::Delete => {
            store.put(source, payload.clone().into()).await?;
            store.delete(source).await?;
        }
        Operation::Copy => {
            store.put(source, payload.clone().into()).await?;
            store.copy(source, target).await?;
        }
    }
    Ok(())
}

async fn source_object_prefix_size(
    store: &Arc<dyn object_store::ObjectStore>,
    database: &str,
) -> Result<u64> {
    let prefix = object_store::path::Path::from(database);
    let mut objects = store.list(Some(&prefix));
    let mut bytes = 0_u64;
    while let Some(object) = objects.try_next().await? {
        bytes = bytes.saturating_add(object.size);
    }
    Ok(bytes)
}

async fn put_legacy_pair_row(
    db: &HyperscaleDb,
    from: u64,
    to: u64,
    properties: &[HProperty],
) -> Result<()> {
    let raw = db.inner_db();
    let txn = raw
        .begin(IsolationLevel::Snapshot)
        .await
        .context("failed to begin raw legacy tx")?;
    txn.put(
        graph::make_edge_property_key(from, to),
        graph::encode_properties(properties),
    )
    .context("failed to put raw legacy row")?;
    txn.commit()
        .await
        .context("failed to commit raw legacy row")?;
    Ok(())
}

fn assert_rewrite_job_completed(
    scenario: &str,
    statuses: &[db::migrations::MigrationParityJobStatus],
) -> Result<()> {
    let Some(rewrite) = statuses
        .iter()
        .find(|job| job.id == MigrationParityId::GraphFormatV1Rewrite)
    else {
        bail!("{scenario}: missing graph_format_v1_rewrite migration job");
    };
    if !rewrite.state.is_completed() {
        bail!(
            "{scenario}: rewrite migration not completed after open: {:#?}",
            rewrite
        );
    }
    Ok(())
}

fn assert_job_statuses_completed(
    scenario: &str,
    statuses: &[db::migrations::MigrationParityJobStatus],
) -> Result<()> {
    for id in [
        MigrationParityId::GraphFormatV1Rewrite,
        MigrationParityId::LegacyVectorPropertyMaterialization,
        MigrationParityId::LegacyVectorPhysicalCleanup,
        MigrationParityId::GraphFormatV1Cleanup,
        MigrationParityId::VectorSimHashDirectoryV1,
    ] {
        let Some(job) = statuses.iter().find(|job| job.id == id) else {
            bail!("{scenario}: missing migration job {id:?}");
        };
        if !job.state.is_completed() {
            bail!("{scenario}: migration job {id:?} is not completed: {job:#?}");
        }
        if matches!(job.state, MigrationParityState::Failed { .. }) {
            bail!("{scenario}: migration job {id:?} has failed: {job:#?}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_timeout_never_grants_time_after_deadline() {
        assert_eq!(
            remaining_suite_duration(Duration::from_secs(300), Duration::from_secs(42))
                .expect("suite retains time"),
            Duration::from_secs(258)
        );
        assert!(
            remaining_suite_duration(Duration::from_secs(300), Duration::from_secs(300)).is_err()
        );
        assert!(
            remaining_suite_duration(Duration::from_secs(300), Duration::from_secs(301)).is_err()
        );
    }

    #[tokio::test]
    async fn target_fault_probe_exercises_and_cleans_every_operation_within_limit() {
        use object_store_014::ObjectStore as _;

        for operation in [
            Operation::Get,
            Operation::Head,
            Operation::Put,
            Operation::Multipart,
            Operation::List,
            Operation::Delete,
            Operation::Copy,
        ] {
            let fault = TargetFault {
                kind: FaultKind::Transient,
                operation,
                every: NonZeroU64::new(2).expect("two is nonzero"),
            };
            let recorder = ObjectStoreRecorder::new(FaultPolicy::failing(
                Duration::ZERO,
                fault.kind,
                fault.operation,
                fault.every,
            ));
            let store: Arc<dyn object_store_014::ObjectStore> =
                Arc::new(InstrumentedStore014::new(
                    Arc::new(object_store_014::memory::InMemory::new()),
                    Arc::clone(&recorder),
                ));
            let maximum_attempts = NonZeroUsize::new(10).expect("ten is nonzero");

            let attempts =
                exercise_target_fault(&store, "graph", &recorder, fault, maximum_attempts)
                    .await
                    .expect("fault probe recovers");

            assert!(attempts <= 10, "{operation:?}");
            assert_eq!(recorder.snapshot().injected_errors, 1, "{operation:?}");
            assert!(
                store
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("probe cleanup can be listed")
                    .is_empty(),
                "{operation:?}"
            );
        }
    }

    #[test]
    fn vector_fixture_contract_preserves_exact_order_and_distance_bits() {
        let squared_euclidean_bits = |vector: [f32; 3]| {
            VECTOR_QUERY
                .iter()
                .zip(vector)
                .map(|(query, value)| {
                    let delta = query - value;
                    delta * delta
                })
                .sum::<f32>()
                .to_bits()
        };
        assert_eq!(
            expected_vector_hits(),
            vec![
                report::VectorHitEvidence {
                    node_id: 1,
                    distance_bits: squared_euclidean_bits([0.0, 0.0, 0.0]),
                },
                report::VectorHitEvidence {
                    node_id: 2,
                    distance_bits: squared_euclidean_bits([1.0, 0.0, 0.0]),
                },
                report::VectorHitEvidence {
                    node_id: 3,
                    distance_bits: squared_euclidean_bits([0.0, 2.0, 0.0]),
                },
            ]
        );
    }

    #[test]
    fn recovered_scale_seed_progress_requires_committed_batch_boundaries() {
        let batch_rows = NonZeroUsize::new(10_000).expect("batch size is nonzero");
        let progress =
            ScaleSeedProgress::recovered(10_000_000, 5_490_000, 10_000_000, 10_000_000, batch_rows)
                .expect("durable progress is resumable");

        assert_eq!(progress.nodes, 10_000_000);
        assert_eq!(progress.edges, 5_490_000);
        assert!(ScaleSeedProgress::recovered(
            10_000_000, 5_490_001, 10_000_000, 10_000_000, batch_rows
        )
        .is_err());
    }

    #[test]
    fn recovered_scale_edges_require_all_nodes_and_configured_bounds() {
        let batch_rows = NonZeroUsize::new(10_000).expect("batch size is nonzero");

        assert!(ScaleSeedProgress::recovered(
            9_990_000, 10_000, 10_000_000, 10_000_000, batch_rows
        )
        .is_err());
        assert!(
            ScaleSeedProgress::recovered(10_010_000, 0, 10_000_000, 10_000_000, batch_rows)
                .is_err()
        );
        assert_eq!(
            ScaleSeedProgress::recovered(
                10_000_000, 10_000_000, 10_000_000, 10_000_000, batch_rows
            )
            .expect("completed seed is resumable"),
            ScaleSeedProgress {
                nodes: 10_000_000,
                edges: 10_000_000,
            }
        );
    }

    #[test]
    fn resumed_seed_progress_uses_only_rows_processed_this_run() {
        let metrics =
            seed_progress_metrics(5_490_000, 5_500_000, 10_000_000, Duration::from_secs(25));

        assert_eq!(metrics.processed_this_run, 10_000);
        assert_eq!(metrics.rows_per_second, 400.0);
        assert_eq!(metrics.eta_seconds, 11_250.0);
    }

    #[test]
    fn scale_edge_checkpoint_round_trips_every_stage() {
        let checkpoint = ScaleEdgeSeedCheckpoint::initial(40_000)
            .advance_rows(50_000)
            .expect("row progress is monotonic");
        assert_eq!(
            ScaleEdgeSeedCheckpoint::decode(&checkpoint.encode())
                .expect("checkpoint encoding is valid"),
            ScaleEdgeSeedCheckpoint {
                rows_completed: 50_000,
                global_label_completed: 40_000,
            }
        );
        assert_eq!(
            checkpoint
                .advance_global_label(50_000)
                .expect("global label catches up"),
            ScaleEdgeSeedCheckpoint::initial(50_000)
        );
    }

    #[test]
    fn scale_edge_checkpoint_rejects_invalid_encodings_and_transitions() {
        assert!(ScaleEdgeSeedCheckpoint::decode(&[]).is_err());
        let mut unsupported = ScaleEdgeSeedCheckpoint::initial(1).encode().to_vec();
        unsupported[0] = SCALE_EDGE_CHECKPOINT_VERSION + 1;
        assert!(ScaleEdgeSeedCheckpoint::decode(&unsupported).is_err());

        let mut inverted = ScaleEdgeSeedCheckpoint::initial(1).encode().to_vec();
        const VERSION_LEN: usize = core::mem::size_of::<u8>();
        const ROWS_LEN: usize = core::mem::size_of::<u64>();
        inverted[VERSION_LEN + ROWS_LEN..VERSION_LEN + ROWS_LEN + core::mem::size_of::<u64>()]
            .copy_from_slice(&2_u64.to_be_bytes());
        assert!(ScaleEdgeSeedCheckpoint::decode(&inverted).is_err());

        let checkpoint = ScaleEdgeSeedCheckpoint::initial(10);
        assert!(checkpoint.advance_rows(9).is_err());
        assert!(checkpoint.advance_global_label(9).is_err());
        assert!(checkpoint
            .advance_rows(20)
            .expect("rows can advance")
            .advance_global_label(21)
            .is_err());
    }

    #[test]
    fn global_label_checkpoints_grow_geometrically_and_finish_exactly() {
        let mut checkpoint = ScaleEdgeSeedCheckpoint::initial(0);
        let mut commits = Vec::new();
        for completed in (10_000..=100_000).step_by(10_000) {
            checkpoint = checkpoint
                .advance_rows(completed)
                .expect("rows advance by one batch");
            if should_commit_scale_global_edge_label(checkpoint, completed, 100_000, 10_000) {
                commits.push(completed);
                checkpoint = checkpoint
                    .advance_global_label(completed)
                    .expect("global label catches up at its checkpoint");
            }
        }
        assert_eq!(commits, [10_000, 20_000, 40_000, 80_000, 100_000]);

        let resumed = ScaleEdgeSeedCheckpoint::initial(7_800_000)
            .advance_rows(10_000_000)
            .expect("resumed rows finish");
        assert!(should_commit_scale_global_edge_label(
            resumed, 10_000_000, 10_000_000, 10_000
        ));
        assert!(!should_commit_scale_global_edge_label(
            resumed, 9_990_000, 10_000_000, 10_000
        ));
    }

    #[test]
    fn staged_write_budget_accepts_boundary_and_rejects_excess() {
        let mut staged = StagedWriteBytes {
            bytes: MAXIMUM_STAGED_WRITE_BYTES - 2,
        };
        staged
            .include(&[0], &[0])
            .expect("exact byte limit is accepted");
        assert_eq!(staged.bytes, MAXIMUM_STAGED_WRITE_BYTES);
        assert!(staged.include(&[0], &[]).is_err());
    }

    #[test]
    fn target_slatedb_revision_comes_from_one_exact_locked_git_source() {
        let revision = "e902085bb06da40a9f8a962a6a3956f1da01f476";
        let lock = format!(
            r#"[[package]]
name = "slatedb"
version = "0.15.0"
source = "git+https://github.com/HelixDB/slatedb.git?rev={revision}#{revision}"
"#
        );
        assert_eq!(
            target_slatedb_revision_from_lock(&lock).expect("exact Git revision resolves"),
            revision
        );
    }

    #[test]
    fn target_slatedb_revision_rejects_missing_ambiguous_and_unpinned_sources() {
        let revision = "e902085bb06da40a9f8a962a6a3956f1da01f476";
        assert!(target_slatedb_revision_from_lock("version = 4").is_err());

        let package = format!(
            r#"[[package]]
name = "slatedb"
version = "0.15.0"
source = "git+https://github.com/HelixDB/slatedb.git?rev={revision}#{revision}"
"#
        );
        assert!(target_slatedb_revision_from_lock(&format!("{package}{package}")).is_err());
        assert!(target_slatedb_revision_from_lock(
            r#"[[package]]
name = "slatedb"
version = "0.15.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#,
        )
        .is_err());
        assert!(target_slatedb_revision_from_lock(
            r#"[[package]]
name = "slatedb"
version = "0.15.0"
source = "git+https://github.com/HelixDB/slatedb.git?rev=main#not-a-revision"
"#,
        )
        .is_err());
    }
}
