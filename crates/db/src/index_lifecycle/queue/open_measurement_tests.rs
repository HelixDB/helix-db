//! Manual measurement of writer open over a large durable backlog.
//!
//! One process builds the fixture (`HELIX_BACKLOG_PHASE=build`); fresh
//! processes then reopen it, so each reports its own peak RSS (Linux
//! `VmHWM`): `open` opens a writer, `load` runs only the backlog load over a
//! raw SlateDB handle. Run in release on a disk-backed directory:
//!
//! ```text
//! HELIX_BACKLOG_DIR=/work/backlog HELIX_BACKLOG_PHASE=build \
//!   cargo test --release -p db --lib -- --ignored --exact \
//!   index_lifecycle::queue::open_measurement_tests::measure_writer_open_over_a_large_backlog \
//!   --nocapture
//! ```
//!
//! The fixture holds `HELIX_BACKLOG_INDEXES` (6) legacy-scope vector indexes
//! with a `HELIX_BACKLOG_QUEUE_MIB` (256) MiB queue each, two queues of
//! `HELIX_BACKLOG_TENANT_QUEUE_MIB` (64) MiB for dropped indexes in each of
//! `HELIX_BACKLOG_QUEUED_TENANTS` (4) tenant scopes, and
//! `HELIX_BACKLOG_EMPTY_TENANTS` (2000) tenant scopes holding one node and
//! no queue. Operations carry 1536-dimension vectors.

use std::sync::Arc;
use std::time::Instant;

use helix_ast::{batch, query::QueryRequest, traversal, value::PropertyInput};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use super::backlog::{BacklogLimits, IndexOperationBacklog};
use super::storage::QueueStore;
use super::tests::{open, queued};
use super::QueueTarget;
use crate::config::{
    CacheConfig, CacheMode, DbConfig, IndexOperationQueueTuning, QueueLayout, VectorIndexDefinition,
};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{IndexEntity, ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueOperand, QueuedOperation, QueuedOperationId, QueuedPayload, QueuedVectorPayload,
    QueuedVectorReplacement,
};
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, ValidatedDynamicIndexDefinition,
};
use crate::merge_operator::HelixMergeOperator;
use crate::search::vector::VectorDistanceMetric;

const NAME: &str = "backlog";
const DIMENSION: usize = 1536;
const MIB: u64 = 1024 * 1024;

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| value.parse().expect("numeric setting"))
}

fn config() -> DbConfig {
    queued(IndexOperationQueueTuning::default())
        .with_cache(CacheConfig::default().with_mode(CacheMode::VectorMemoryOnly))
}

/// `(VmRSS, VmHWM)` in MiB.
fn memory() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| {
                rest.trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .map_or(0, |kib| kib / 1024)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

fn vector_operation(entity: u64, seed: u64) -> QueuedOperation {
    let vector = (0..DIMENSION)
        .map(|component| ((seed.wrapping_mul(31) + component as u64) % 997) as f32 / 997.0)
        .collect::<Arc<[f32]>>();
    QueuedOperation::new(
        QueuedOperationId::generate(),
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(entity),
        },
        QueuedPayload::Vector(QueuedVectorPayload {
            previous: None,
            replacement: Some(
                QueuedVectorReplacement::try_new(TextPartition::Unpartitioned, vector).unwrap(),
            ),
        }),
    )
}

async fn build(store: Arc<dyn ObjectStore>) {
    let started = Instant::now();
    let db = open(NAME, store, config()).await;
    for index in 0..setting("HELIX_BACKLOG_INDEXES", 6) {
        db.install_index_for_tests(
            ValidatedDynamicIndexDefinition::try_from(
                VectorIndexDefinition::new_node(
                    "Doc",
                    format!("embedding{index}"),
                    DIMENSION,
                    VectorDistanceMetric::Euclidean,
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    let storage = db.inner_db();
    let mut targets = Vec::new();
    let prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::logical_prefix(RecordKind::IndexRecord),
    );
    let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let record = decode_index_record(&row.value).unwrap();
        targets.push((
            QueueTarget::new(
                DataScope::LegacyUnscoped,
                record.index_id(),
                record.state().generation(),
            ),
            setting("HELIX_BACKLOG_QUEUE_MIB", 256) * MIB,
        ));
    }
    let handle = &db;
    let node = |scope| async move {
        handle
            .query_scoped(
                QueryRequest::write(
                    batch::write_batch().var_as(
                        "created",
                        traversal::g()
                            .add_n("Doc", vec![("body", PropertyInput::from("x".to_string()))]),
                    ),
                ),
                scope,
            )
            .await
            .unwrap();
    };
    for tenant in 1..=setting("HELIX_BACKLOG_QUEUED_TENANTS", 4) {
        let scope = DataScope::Tenant(TenantId::from_u128(u128::from(tenant) * 1_000));
        node(scope).await;
        for index in 0..2 {
            targets.push((
                QueueTarget::new(
                    scope,
                    IndexId::new(10_000 + index).unwrap(),
                    IndexGenerationId::initial(),
                ),
                setting("HELIX_BACKLOG_TENANT_QUEUE_MIB", 64) * MIB,
            ));
        }
    }
    for tenant in 0..setting("HELIX_BACKLOG_EMPTY_TENANTS", 2_000) {
        node(DataScope::Tenant(TenantId::from_u128(
            1_000_000 + u128::from(tenant),
        )))
        .await;
    }
    let queues = QueueStore::new(QueueLayout::Map, db.index_operand_limit(), 0);
    let per_operand =
        usize::try_from((db.index_operand_limit() - 64) / vector_operation(1, 0).retained_bytes())
            .unwrap();
    let mut operations = 0_u64;
    for (target, bytes) in targets {
        let mut written = 0;
        let mut entity = 1;
        while written < bytes {
            let batch = (0..per_operand)
                .map(|offset| vector_operation(entity + offset as u64, operations))
                .collect::<Vec<_>>();
            entity += per_operand as u64;
            operations += batch.len() as u64;
            written += batch
                .iter()
                .map(QueuedOperation::retained_bytes)
                .sum::<u64>();
            let transaction = storage
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            queues
                .stage_enqueue(
                    &transaction,
                    target,
                    QueueOperand::enqueue(&batch).unwrap(),
                    &batch,
                )
                .unwrap();
            transaction.commit().await.unwrap();
        }
    }
    storage
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    db.close().await.unwrap();
    println!(
        "BUILD operations={operations} elapsed_ms={}",
        started.elapsed().as_millis()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual release measurement of writer open over a large backlog"]
async fn measure_writer_open_over_a_large_backlog() {
    let dir = std::env::var("HELIX_BACKLOG_DIR").expect("HELIX_BACKLOG_DIR is set");
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
    let phase = std::env::var("HELIX_BACKLOG_PHASE").expect("HELIX_BACKLOG_PHASE is set");
    let (rss_before, _) = memory();
    let started = Instant::now();
    let (queues, operations, retained_bytes) = match phase.as_str() {
        "build" => return build(store).await,
        "open" => {
            let db = open(NAME, store, config()).await;
            let elapsed = started.elapsed();
            let stats = db.index_operation_queue_stats();
            let (_, peak) = memory();
            println!(
                "OPEN elapsed_ms={} rss_before_mib={rss_before} peak_rss_mib={peak}",
                elapsed.as_millis()
            );
            db.close().await.unwrap();
            (0, stats.pending_operations, stats.retained_bytes)
        }
        "load" => {
            let db = slatedb::Db::builder(NAME, store)
                .with_merge_operator(Arc::new(HelixMergeOperator::new()))
                .build()
                .await
                .unwrap();
            let opened = started.elapsed();
            let (rss_opened, _) = memory();
            let backlog = IndexOperationBacklog::new(
                BacklogLimits {
                    max_retained_bytes: u64::MAX,
                    max_members: u64::MAX,
                },
                crate::index_lifecycle::worker::IndexWorkerWakeHandle::default(),
            );
            let store = QueueStore::new(QueueLayout::Map, 8 * MIB, 0);
            let loading = Instant::now();
            let summary = super::recovery::load_backlog(&db, &store, &backlog)
                .await
                .unwrap();
            let elapsed = loading.elapsed();
            let (_, peak) = memory();
            println!(
                "LOAD slate_open_ms={} load_ms={} rss_after_slate_open_mib={rss_opened} \
                 peak_rss_mib={peak}",
                opened.as_millis(),
                elapsed.as_millis()
            );
            let totals = backlog.totals();
            db.close().await.unwrap();
            (
                summary.queues,
                totals.usage.operations,
                totals.usage.retained_bytes,
            )
        }
        other => panic!("unknown phase {other}"),
    };
    println!("LOADED queues={queues} operations={operations} retained_bytes={retained_bytes}");
}
