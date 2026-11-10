//! Startup migration of hash-keyed label index rows to exact label keys.
//!
//! Before storage version 6 every label index row was keyed by the 64-bit
//! SipHash of its label, so two labels with one hash shared a bitmap. This
//! migration copies every node label, global edge label, and edge-label
//! neighbor row of every data scope to the key of its exact label (see
//! [`crate::encoding::indexes::label`]), publishes
//! [`IndexStorageVersion::LABEL_IDENTITY`], and only then deletes the hash rows.
//!
//! # Contract
//!
//! - It runs in a migration-authorized writer open, after the blocking startup
//!   migrations and before the writer serves any graph write, so the hash rows
//!   it copies do not change under it. It is a no-op on a store already at
//!   [`IndexStorageVersion::LABEL_IDENTITY`].
//! - Until it publishes, it only adds exact rows and leaves every hash row in
//!   place. Readers of the store, whether of this binary or one that supports at
//!   most version 5, keep reading complete hash rows, so reads stay correct for
//!   the whole copy. Version 6 is published in one commit after every scope is
//!   copied and flushed, so readers switch from hash rows to complete exact rows
//!   at once.
//! - A row's label is the `$label` of its lowest live member's record, checked
//!   against the row's hash; a mismatch, or a label too long for any exact key,
//!   fails closed. One member names the whole row: the migration relies on no
//!   two labels of a scope sharing a hash, which production stores were
//!   verified to satisfy before it shipped. A row with no live member indexes
//!   nothing and is not copied.
//! - Edge-label neighbor rows name opposite endpoints, not edges. Their labels
//!   come from the scope's edge label rows, copied first. A neighbor row whose
//!   hash no live edge label has is stale and is not copied.
//! - A failed or interrupted copy leaves a version-5 store with intact hash
//!   rows, which any version-5 binary still opens and keeps current. So before
//!   copying again, a retry clears every exact row an earlier attempt left,
//!   which [`LABEL_IDENTITY_COPY_STARTED`] records, and copies afresh. Batches
//!   are committed without awaiting durability, and the store is flushed before
//!   the version is published.
//! - The publishing commit also clears [`LABEL_IDENTITY_COPY_STARTED`]. Once
//!   version 6 is published nothing reads the hash rows:
//!   [`delete_hash_label_rows`] removes them from the background migration
//!   worker while the writer serves, and records [`LABEL_HASH_CLEANUP_READY`].
//!
//! # Cost
//!
//! The writer open blocks on the copy, which is linear in label rows: per
//! scope it scans each family of hash rows once, reads one live member's record
//! per node and edge row (after any dead members below it), and writes every
//! exact row. A scope with no live edge label skips its neighbor scan. Progress
//! is logged at most every [`PROGRESS_INTERVAL`]. The cleanup scans the hash
//! rows once more after the writer is serving.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::config::WriteOptions;
use slatedb::{Db, IsolationLevel, WriteBatch};

use crate::encoding::indexes::label::{
    EdgeLabelKey, EdgeLabelNeighborKey, LabelIdentity, LabelScanPrefix, NodeLabelKey, SharedLabel,
};
use crate::encoding::indexes::{hash_property_value, EdgeDirection, PropertyIndexKey, ValueHash};
use crate::encoding::keys::scope::DataScope;
use crate::encoding::keys::{EdgePropertyByIdKey, NodePropertyKey};
use crate::encoding::property::decode_properties;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::label_hash::{
    legacy_edge_label_neighbor_scan_prefix, legacy_edge_label_scan_prefix, LegacyNodeLabelKey,
};
use crate::encoding::v2::values::indexes::{SecondaryEqualityBitmapValue, SecondaryEqualityValue};
use crate::encoding::NodeId;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::IndexStorageVersion;
use crate::migrations::startup::{advance_storage_version, storage_version};
use crate::transaction::Mutation;

/// Metadata marker of a label identity copy that has started and not been
/// published: its exact rows may be incomplete or stale.
const LABEL_IDENTITY_COPY_STARTED: &[u8] = b"kv_migration_started:label_identity_copy";
/// Metadata marker of a published label identity migration whose hash rows are
/// all deleted.
const LABEL_HASH_CLEANUP_READY: &[u8] = b"kv_migration_ready:label_hash_cleanup";

/// Rows written or deleted per commit. Neighbor rows dominate: one per
/// endpoint, direction, and edge label.
const BATCH_ROWS: usize = 8192;
/// Stored row bytes written per commit, bounding one batch's memory.
const BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Shortest interval between two progress logs.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// One family of hash-keyed label rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyLabelRows {
    Node,
    Edge,
    Neighbor,
}

impl LegacyLabelRows {
    const ALL: [Self; 3] = [Self::Node, Self::Edge, Self::Neighbor];

    fn logical_prefix(self) -> Bytes {
        match self {
            Self::Node => LegacyNodeLabelKey::scan_prefix(),
            Self::Edge => legacy_edge_label_scan_prefix(),
            Self::Neighbor => legacy_edge_label_neighbor_scan_prefix(),
        }
    }
}

/// The exact row family one hash row is copied to, less the label.
#[derive(Debug, Clone, Copy)]
enum ExactRows {
    Node,
    Edge,
    Neighbor(EdgeDirection, NodeId),
}

impl ExactRows {
    fn key(self, scope: DataScope, label: LabelIdentity<'_>) -> Bytes {
        DataKey::Data {
            scope,
            kind: DataKeyKind::PropertyIndex(match self {
                Self::Node => PropertyIndexKey::NodeLabel(NodeLabelKey::new(label)),
                Self::Edge => PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(label)),
                Self::Neighbor(direction, node) => PropertyIndexKey::EdgeLabelNeighbor(
                    EdgeLabelNeighborKey::new(direction, node, label),
                ),
            }),
        }
        .to_bytes()
    }
}

/// Rows one migration copied, skipped, or cleared, for the startup log.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LabelIdentityMigration {
    pub(crate) scopes: usize,
    /// Hash rows copied to their exact key.
    pub(crate) copied: u64,
    /// Hash rows with no live member, or stale neighbor rows no live edge
    /// label explains; nothing reads them once the migration is published.
    pub(crate) skipped: u64,
    /// Exact rows an earlier, unpublished attempt left, deleted before copying.
    pub(crate) cleared: u64,
}

/// Running totals and the throttle of the progress log.
struct Progress {
    rows: LabelIdentityMigration,
    scope: usize,
    scopes: usize,
    started: Instant,
    reported: Instant,
}

impl Progress {
    fn new(scopes: usize) -> Self {
        Self {
            rows: LabelIdentityMigration::default(),
            scope: 0,
            scopes,
            started: Instant::now(),
            reported: Instant::now(),
        }
    }

    fn batch_written(&mut self) {
        if self.reported.elapsed() < PROGRESS_INTERVAL {
            return;
        }
        self.reported = Instant::now();
        tracing::info!(
            scope = self.scope + 1,
            scopes = self.scopes,
            copied = self.rows.copied,
            skipped = self.rows.skipped,
            cleared = self.rows.cleared,
            elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "label identity migration in progress"
        );
    }
}

/// Rows written to or deleted from one batch, flushed at the batch bounds.
struct Batched<'a> {
    db: &'a Db,
    batch: WriteBatch,
    rows: usize,
    bytes: usize,
}

impl<'a> Batched<'a> {
    fn new(db: &'a Db) -> Self {
        Self {
            db,
            batch: WriteBatch::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// Counts one staged row of `bytes` stored bytes, committing the batch
    /// when it reaches a bound. Returns whether it committed.
    async fn staged(&mut self, bytes: usize) -> Result<bool> {
        self.rows += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        if self.rows < BATCH_ROWS && self.bytes < BATCH_BYTES {
            return Ok(false);
        }
        self.commit().await?;
        Ok(true)
    }

    async fn commit(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        self.db
            .write_with_options(
                std::mem::take(&mut self.batch),
                &WriteOptions {
                    await_durable: false,
                    ..WriteOptions::default()
                },
            )
            .await?;
        self.rows = 0;
        self.bytes = 0;
        Ok(())
    }
}

/// Copies every hash-keyed label row to its exact key and publishes
/// [`IndexStorageVersion::LABEL_IDENTITY`]; see the module contract.
pub(crate) async fn migrate_label_identity(db: &Db) -> Result<LabelIdentityMigration> {
    let version = storage_version(db).await?;
    if version >= IndexStorageVersion::LABEL_IDENTITY {
        return Ok(LabelIdentityMigration::default());
    }
    if version != IndexStorageVersion::OPERATION_QUEUES {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "label identity migration requires storage version {}, found {}",
                IndexStorageVersion::OPERATION_QUEUES.get(),
                version.get()
            ),
        });
    }
    let copy_started =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_IDENTITY_COPY_STARTED);
    // Only an earlier attempt can have left exact rows behind.
    let retrying = db.get(&copy_started).await?.is_some();
    if !retrying {
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        Mutation::put(&transaction, copy_started.clone(), Bytes::from_static(b"1"))?;
        transaction.commit().await?;
    }

    let scopes = crate::index_lifecycle::queue::recovery::discover_scopes(db).await?;
    let mut progress = Progress::new(scopes.len());
    for (index, scope) in scopes.into_iter().enumerate() {
        progress.scope = index;
        if retrying {
            let mut batch = Batched::new(db);
            for prefix in [
                LabelScanPrefix::NodeLabels,
                LabelScanPrefix::EdgeLabels,
                LabelScanPrefix::EdgeLabelNeighbors,
            ] {
                let mut rows = db
                    .scan_prefix(DataKey::data_prefix(scope, prefix.to_bytes()), ..)
                    .await?;
                while let Some(row) = rows.next().await? {
                    batch.batch.delete(&row.key);
                    progress.rows.cleared += 1;
                    if batch.staged(row.key.len()).await? {
                        progress.batch_written();
                    }
                }
            }
            batch.commit().await?;
        }
        let mut edge_labels = BTreeMap::new();
        for rows in LegacyLabelRows::ALL {
            // Without a live edge label no neighbor row has a label to copy to.
            if rows == LegacyLabelRows::Neighbor && edge_labels.is_empty() {
                continue;
            }
            copy_rows(db, scope, rows, &mut edge_labels, &mut progress).await?;
        }
        progress.rows.scopes += 1;
    }

    db.flush().await?;
    // The published copy is complete, so its marker goes with the old version.
    advance_storage_version(
        db,
        IndexStorageVersion::OPERATION_QUEUES..IndexStorageVersion::LABEL_IDENTITY,
        IndexStorageVersion::LABEL_IDENTITY,
        |transaction| Ok(transaction.delete(copy_started)?),
    )
    .await?;
    let migration = progress.rows;
    tracing::info!(
        scopes = migration.scopes,
        copied = migration.copied,
        skipped = migration.skipped,
        cleared = migration.cleared,
        elapsed_ms = u64::try_from(progress.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "label identity migration published exact label rows"
    );
    Ok(migration)
}

/// Copies every `rows` row of `scope` to its exact key, in batches.
///
/// The edge pass records each exact edge label by the hash its neighbor rows
/// use in `edge_labels`; the neighbor pass names its rows from it.
async fn copy_rows(
    db: &Db,
    scope: DataScope,
    rows: LegacyLabelRows,
    edge_labels: &mut BTreeMap<ValueHash, SharedLabel>,
    progress: &mut Progress,
) -> Result<()> {
    let mut legacy = db
        .scan_prefix(DataKey::data_prefix(scope, rows.logical_prefix()), ..)
        .await?;
    let mut batch = Batched::new(db);
    while let Some(row) = legacy.next().await? {
        let members = SecondaryEqualityValue::decode(&row.value)?.into_ids();
        let DataKey::Data {
            kind: DataKeyKind::PropertyIndex(key),
            ..
        } = DataKey::parse_from_slice(scope, &row.key)?
        else {
            unreachable!("the label prefixes are property index prefixes");
        };
        let target = match (rows, key) {
            (LegacyLabelRows::Node, PropertyIndexKey::Equality(key)) => row_label(
                db,
                scope,
                &members,
                |id| DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
                &key.value_hash,
            )
            .await?
            .map(|label| ExactRows::Node.key(scope, label.identity())),
            (LegacyLabelRows::Edge, PropertyIndexKey::LegacyEdgeLabel(key)) => row_label(
                db,
                scope,
                &members,
                |id| DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(id)),
                key.label_hash(),
            )
            .await?
            .map(|label| {
                let exact = ExactRows::Edge.key(scope, label.identity());
                // One hash row per hash, so each hash names one edge label.
                edge_labels.insert(*key.label_hash(), label);
                exact
            }),
            (LegacyLabelRows::Neighbor, PropertyIndexKey::LegacyEdgeLabelNeighbor(key)) => {
                edge_labels.get(key.label_hash()).map(|label| {
                    ExactRows::Neighbor(key.direction(), key.node_id()).key(scope, label.identity())
                })
            }
            (rows, key) => {
                return Err(HelixDbError::MigrationRequired {
                    reason: format!("{scope:?} holds {key:?} under the {rows:?} hash label prefix"),
                });
            }
        };
        let Some(exact) = target else {
            progress.rows.skipped += 1;
            continue;
        };
        batch
            .batch
            .merge(exact, SecondaryEqualityBitmapValue::new(members).encode());
        progress.rows.copied += 1;
        if batch.staged(row.key.len() + row.value.len()).await? {
            progress.batch_written();
        }
    }
    batch.commit().await?;
    progress.batch_written();
    Ok(())
}

/// The label of one node or edge hash row: the `$label` of its lowest live
/// member, which must hash to the row and fit an exact key. `None` when no
/// member is live. `record` names a member's property record.
async fn row_label(
    db: &Db,
    scope: DataScope,
    members: &RoaringTreemap,
    record: impl Fn(u64) -> DataKeyKind<'static>,
    row_hash: &ValueHash,
) -> Result<Option<SharedLabel>> {
    for member in members {
        let record = record(member);
        let Some(stored) = db
            .get(
                DataKey::Data {
                    scope,
                    kind: record.clone(),
                }
                .to_bytes(),
            )
            .await?
        else {
            continue;
        };
        let Some(label) = decode_properties(&stored)?
            .into_iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str().map(str::to_owned))
        else {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "{scope:?} {record:?} is indexed by label but has no string $label"
                ),
            });
        };
        if hash_property_value(&label) != *row_hash {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "{scope:?} {record:?} has a $label that does not hash to the label row it is in"
                ),
            });
        }
        let Ok(identity) = LabelIdentity::new(&label) else {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "{scope:?} {record:?} has a {}-byte $label, longer than any label key holds",
                    label.len()
                ),
            });
        };
        return Ok(Some(SharedLabel::new(identity)));
    }
    Ok(None)
}

/// What one cleanup of retired hash label rows did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelHashCleanup {
    /// The migration is not published, so its hash rows are still read.
    Unpublished,
    /// An earlier cleanup already deleted every hash row.
    AlreadyDone,
    /// Shutdown was requested; the next migration-authorized open resumes.
    Stopped { deleted: u64 },
    /// Every hash row is deleted and the cleanup is recorded.
    Done { deleted: u64 },
}

/// Deletes every scope's retired hash label rows once the label identity
/// migration is published, then records [`LABEL_HASH_CLEANUP_READY`].
///
/// Nothing reads those rows by then, and graph writes never touch them, so it
/// runs beside the serving writer. `stop` is polled between batches.
pub(crate) async fn delete_hash_label_rows(
    db: &Db,
    stop: impl Fn() -> bool,
) -> Result<LabelHashCleanup> {
    let ready =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_HASH_CLEANUP_READY);
    if db.get(&ready).await?.is_some() {
        return Ok(LabelHashCleanup::AlreadyDone);
    }
    if storage_version(db).await? < IndexStorageVersion::LABEL_IDENTITY {
        return Ok(LabelHashCleanup::Unpublished);
    }
    let started = Instant::now();
    let mut deleted = 0u64;
    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(db).await? {
        for rows in LegacyLabelRows::ALL {
            let mut legacy = db
                .scan_prefix(DataKey::data_prefix(scope, rows.logical_prefix()), ..)
                .await?;
            let mut batch = Batched::new(db);
            while let Some(row) = legacy.next().await? {
                batch.batch.delete(&row.key);
                deleted += 1;
                if batch.staged(row.key.len()).await? && stop() {
                    return Ok(LabelHashCleanup::Stopped { deleted });
                }
            }
            batch.commit().await?;
        }
        if stop() {
            return Ok(LabelHashCleanup::Stopped { deleted });
        }
    }
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    Mutation::put(&transaction, ready, Bytes::from_static(b"1"))?;
    transaction.commit().await?;
    tracing::info!(
        deleted,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "label identity cleanup deleted the retired hash label rows"
    );
    Ok(LabelHashCleanup::Done { deleted })
}

/// Files every exact label row of every scope back under its hash key, rewinds
/// the marker to [`IndexStorageVersion::OPERATION_QUEUES`], and forgets every
/// copy and cleanup: the store a binary before label identity left. The caller
/// must quiesce every write.
#[cfg(test)]
pub(crate) async fn make_legacy_label_fixture(db: &Db) -> Result<()> {
    use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey};
    use crate::encoding::v2::legacy::label_hash::{LegacyEdgeLabelKey, LegacyEdgeLabelNeighborKey};
    use crate::encoding::v2::values::encode_metadata_value;
    use crate::index_lifecycle::IndexV2MetadataValue;

    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(db).await? {
        for prefix in [
            LabelScanPrefix::NodeLabels,
            LabelScanPrefix::EdgeLabels,
            LabelScanPrefix::EdgeLabelNeighbors,
        ] {
            let mut rows = db
                .scan_prefix(DataKey::data_prefix(scope, prefix.to_bytes()), ..)
                .await?;
            let mut batch = WriteBatch::new();
            while let Some(row) = rows.next().await? {
                let DataKey::Data {
                    kind: DataKeyKind::PropertyIndex(key),
                    ..
                } = DataKey::parse_from_slice(scope, &row.key)?
                else {
                    unreachable!("label prefixes hold property index rows");
                };
                let legacy = match key {
                    PropertyIndexKey::NodeLabel(key) => PropertyIndexKey::Equality(
                        LegacyNodeLabelKey::for_label(key.identity().label()),
                    ),
                    PropertyIndexKey::EdgeLabel(key) => PropertyIndexKey::LegacyEdgeLabel(
                        LegacyEdgeLabelKey::for_label(key.identity().label()),
                    ),
                    PropertyIndexKey::EdgeLabelNeighbor(key) => {
                        PropertyIndexKey::LegacyEdgeLabelNeighbor(
                            LegacyEdgeLabelNeighborKey::for_label(
                                key.direction(),
                                key.node_id(),
                                key.identity().label(),
                            ),
                        )
                    }
                    other @ (PropertyIndexKey::Equality(_)
                    | PropertyIndexKey::EdgeEquality(_)
                    | PropertyIndexKey::LegacyEdgeLabel(_)
                    | PropertyIndexKey::LegacyEdgeLabelNeighbor(_)
                    | PropertyIndexKey::GlobalEdgeEquality(_)
                    | PropertyIndexKey::Range(_)
                    | PropertyIndexKey::EdgeRange(_)
                    | PropertyIndexKey::GlobalEdgeRange(_)) => {
                        unreachable!("label prefixes hold only label rows, got {other:?}")
                    }
                };
                batch.merge(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::PropertyIndex(legacy),
                    }
                    .to_bytes(),
                    SecondaryEqualityBitmapValue::new(
                        SecondaryEqualityValue::decode(&row.value)?.into_ids(),
                    )
                    .encode(),
                );
                batch.delete(&row.key);
            }
            if !batch.is_empty() {
                db.write(batch).await?;
            }
        }
    }
    db.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::OPERATION_QUEUES,
        )),
    )
    .await?;
    for marker in [LABEL_IDENTITY_COPY_STARTED, LABEL_HASH_CLEANUP_READY] {
        db.delete(super::super::scoped_metadata_key(
            DataScope::LegacyUnscoped,
            marker,
        ))
        .await?;
    }
    db.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use helix_ast::batch::{read_batch, write_batch};
    use helix_ast::graph::NodeRef;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;
    use helix_ast::value::PropertyInput;
    use slatedb::object_store::{memory::InMemory, ObjectStore};
    use slatedb::DbReadOps;

    use super::*;
    use crate::encoding::keys::scope::TenantId;
    use crate::encoding::property::{encode_properties, Property};
    use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey};
    use crate::encoding::v2::legacy::label_hash::{LegacyEdgeLabelKey, LegacyEdgeLabelNeighborKey};
    use crate::encoding::v2::values::encode_metadata_value;
    use crate::index_lifecycle::repository::ReaderStorageCompatibility;
    use crate::index_lifecycle::IndexV2MetadataValue;

    type Rows = BTreeMap<Bytes, RoaringTreemap>;

    fn scopes() -> [DataScope; 3] {
        [
            DataScope::LegacyUnscoped,
            DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap()),
            DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000B").unwrap()),
        ]
    }

    fn marker_key() -> Bytes {
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes()
    }

    /// Two people who know each other and both work at one company.
    async fn write_graph(db: &crate::HelixDB, scope: DataScope) {
        let since = || vec![("since", PropertyInput::from(2020i64))];
        db.query_scoped(
            QueryRequest::write(
                write_batch()
                    .var_as(
                        "ada",
                        g().add_n("Person", vec![("name", PropertyInput::from("ada"))]),
                    )
                    .var_as(
                        "bob",
                        g().add_n("Person", vec![("name", PropertyInput::from("bob"))]),
                    )
                    .var_as(
                        "acme",
                        g().add_n("Company", vec![("name", PropertyInput::from("acme"))]),
                    )
                    .var_as(
                        "knows",
                        g().n(NodeRef::var("ada"))
                            .add_e("KNOWS", NodeRef::var("bob"), since()),
                    )
                    .var_as(
                        "ada_works",
                        g().n(NodeRef::var("ada"))
                            .add_e("WORKS_AT", NodeRef::var("acme"), since()),
                    )
                    .var_as(
                        "bob_works",
                        g().n(NodeRef::var("bob"))
                            .add_e("WORKS_AT", NodeRef::var("acme"), since()),
                    )
                    .returning(Vec::<String>::new()),
            ),
            scope,
        )
        .await
        .expect("fixture graph writes");
    }

    /// Every label-served read shape: node and edge label scans and labeled
    /// expansion in both directions.
    async fn label_reads(db: &crate::HelixDB, scope: DataScope) -> serde_json::Value {
        db.query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as("people", g().n_with_label("Person").count())
                    .var_as("companies", g().n_with_label("Company").count())
                    .var_as("knows", g().e_with_label("KNOWS").count())
                    .var_as("works_at", g().e_with_label("WORKS_AT").count())
                    .var_as(
                        "employers",
                        g().n_with_label("Person").out(Some("WORKS_AT")).count(),
                    )
                    .var_as(
                        "employees",
                        g().n_with_label("Company").in_(Some("WORKS_AT")).count(),
                    )
                    .returning([
                        "people",
                        "companies",
                        "knows",
                        "works_at",
                        "employers",
                        "employees",
                    ]),
            ),
            scope,
        )
        .await
        .expect("label reads execute")
    }

    async fn all_label_reads(db: &crate::HelixDB) -> Vec<serde_json::Value> {
        let mut reads = Vec::new();
        for scope in scopes() {
            reads.push(label_reads(db, scope).await);
        }
        reads
    }

    /// Exact and legacy label rows of every scope.
    async fn label_rows(reader: &(impl DbReadOps + Sync)) -> (Rows, Rows) {
        let mut exact = Rows::new();
        let mut legacy = Rows::new();
        for scope in crate::index_lifecycle::queue::recovery::discover_scopes(reader)
            .await
            .unwrap()
        {
            for (is_exact, prefix) in [
                (true, LabelScanPrefix::NodeLabels.to_bytes()),
                (true, LabelScanPrefix::EdgeLabels.to_bytes()),
                (true, LabelScanPrefix::EdgeLabelNeighbors.to_bytes()),
                (false, LegacyLabelRows::Node.logical_prefix()),
                (false, LegacyLabelRows::Edge.logical_prefix()),
                (false, LegacyLabelRows::Neighbor.logical_prefix()),
            ] {
                let rows = if is_exact { &mut exact } else { &mut legacy };
                let mut scan = reader
                    .scan_prefix(DataKey::data_prefix(scope, prefix), ..)
                    .await
                    .unwrap();
                while let Some(row) = scan.next().await.unwrap() {
                    rows.insert(
                        row.key,
                        SecondaryEqualityValue::decode(&row.value)
                            .unwrap()
                            .into_ids(),
                    );
                }
            }
        }
        (exact, legacy)
    }

    fn legacy_key(key: PropertyIndexKey<'_>) -> Bytes {
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(key),
        }
        .to_bytes()
    }

    async fn put_legacy_row(db: &Db, key: PropertyIndexKey<'_>, ids: &[u64]) -> Bytes {
        let key = legacy_key(key);
        db.put(
            &key,
            SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter(ids.iter().copied()))
                .encode(),
        )
        .await
        .unwrap();
        key
    }

    /// A writer without the background migration worker, so no cleanup races
    /// a test's hash rows.
    async fn open_writer(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
        crate::HelixDB::open_with_object_store_and_config(
            "label-identity",
            Arc::clone(store),
            crate::config::DbConfig::new().with_migration_tuning(
                crate::config::MigrationTuning::default()
                    .with_worker_mode(crate::config::MigrationWorkerMode::Disabled),
            ),
        )
        .await
        .expect("writer opens")
    }

    async fn open_reader(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
        crate::HelixDB::open_reader_with_object_store_for_tests("label-identity", Arc::clone(store))
            .await
            .expect("a store before or during label identity is reader-compatible")
    }

    /// The unscoped fixture graph's node IDs, from its legacy label rows.
    fn fixture_nodes(legacy: &Rows) -> (u64, u64, u64) {
        let people = &legacy[&legacy_key(PropertyIndexKey::Equality(
            LegacyNodeLabelKey::for_label("Person"),
        ))];
        let companies = &legacy[&legacy_key(PropertyIndexKey::Equality(
            LegacyNodeLabelKey::for_label("Company"),
        ))];
        let [ada, bob] = people.iter().collect::<Vec<_>>()[..] else {
            panic!("the fixture has two people");
        };
        (
            ada,
            bob,
            companies.min().expect("the fixture has a company"),
        )
    }

    /// A store with the fixture graph in every scope, filed under hash keys
    /// as a binary before label identity left it.
    async fn legacy_store() -> (
        Arc<dyn ObjectStore>,
        crate::HelixDB,
        Vec<serde_json::Value>,
        Rows,
    ) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        assert_eq!(
            storage_version(db.inner_db().as_ref()).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        for scope in scopes() {
            write_graph(&db, scope).await;
        }
        let reads = all_label_reads(&db).await;
        for scope_reads in &reads {
            for (read, expected) in [
                ("people", 2),
                ("companies", 1),
                ("knows", 1),
                ("works_at", 2),
                ("employers", 2),
                ("employees", 2),
            ] {
                assert_eq!(scope_reads[read], serde_json::json!(expected), "{read}");
            }
        }
        let (exact, legacy) = label_rows(db.inner_db().as_ref()).await;
        assert!(legacy.is_empty());
        // Two node labels, two edge labels, and five neighbor rows per scope.
        assert_eq!(exact.len(), 9 * scopes().len());
        make_legacy_label_fixture(&db.inner_db()).await.unwrap();
        (store, db, reads, exact)
    }

    async fn marker(db: &Db, name: &[u8]) -> bool {
        db.get(super::super::super::scoped_metadata_key(
            DataScope::LegacyUnscoped,
            name,
        ))
        .await
        .unwrap()
        .is_some()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn migration_copies_every_scope_and_reads_stay_exact_at_every_boundary() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert!(exact.is_empty());
        assert_eq!(legacy.len(), exact_before.len());
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );

        let reader = open_reader(&store).await;
        assert_eq!(
            reader.reader_storage_compatibility_for_tests(),
            ReaderStorageCompatibility::LegacyLabelHash
        );
        assert_eq!(all_label_reads(&reader).await, reads);

        // A crash after node rows were copied leaves every hash row in place.
        inner
            .put(
                super::super::super::scoped_metadata_key(
                    DataScope::LegacyUnscoped,
                    LABEL_IDENTITY_COPY_STARTED,
                ),
                Bytes::from_static(b"1"),
            )
            .await
            .unwrap();
        let mut partial = Progress::new(scopes().len());
        for scope in scopes() {
            copy_rows(
                &inner,
                scope,
                LegacyLabelRows::Node,
                &mut BTreeMap::new(),
                &mut partial,
            )
            .await
            .unwrap();
        }
        assert_eq!(partial.rows.copied, 2 * scopes().len() as u64);
        inner.flush().await.unwrap();
        // Read once the reader's own snapshot holds the partial copy.
        let crate::HelixStorage::Reader(storage) = reader.storage() else {
            unreachable!("the reader opened in reader mode");
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = storage.snapshot().await.unwrap();
                let (exact, legacy) = label_rows(snapshot.as_ref()).await;
                if exact.len() == 2 * scopes().len() {
                    assert_eq!(legacy.len(), exact_before.len());
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the reader observes the partial copy");
        assert_eq!(all_label_reads(&reader).await, reads);
        assert_eq!(
            reader.reader_storage_compatibility_for_tests(),
            ReaderStorageCompatibility::LegacyLabelHash
        );

        // The retry clears the partial copy and copies every row afresh.
        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.cleared, partial.rows.copied);
        assert_eq!(migration.copied, exact_before.len() as u64);
        assert_eq!(migration.skipped, 0);
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        // Published, but the hash rows stay until the cleanup.
        assert_eq!(legacy.len(), exact_before.len());
        assert!(!marker(&inner, LABEL_IDENTITY_COPY_STARTED).await);
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        assert_eq!(
            migrate_label_identity(&inner).await.unwrap(),
            LabelIdentityMigration::default()
        );
        assert_eq!(all_label_reads(&db).await, reads);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                assert_eq!(all_label_reads(&reader).await, reads);
                if reader.reader_storage_compatibility_for_tests()
                    == ReaderStorageCompatibility::Current
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the reader switches to exact label rows after publication");

        assert_eq!(
            delete_hash_label_rows(&inner, || false).await.unwrap(),
            LabelHashCleanup::Done {
                deleted: exact_before.len() as u64
            }
        );
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert!(marker(&inner, LABEL_HASH_CLEANUP_READY).await);
        assert_eq!(
            delete_hash_label_rows(&inner, || false).await.unwrap(),
            LabelHashCleanup::AlreadyDone
        );
        assert_eq!(all_label_reads(&reader).await, reads);

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writer_open_migrates_and_the_background_worker_cleans_up() {
        let (store, db, reads, exact_before) = legacy_store().await;
        db.close().await.unwrap();

        // The default configuration runs the background migration worker.
        let db =
            crate::HelixDB::open_with_object_store_for_tests("label-identity", Arc::clone(&store))
                .await
                .expect("writer opens");
        let inner = db.inner_db();
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        assert_eq!(all_label_reads(&db).await, reads);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !marker(&inner, LABEL_HASH_CLEANUP_READY).await {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the background worker deletes the hash rows");
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_copy_leaves_a_version_5_store_that_a_retry_rebuilds() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        // A malformed edge row fails the copy after node rows were copied.
        let mut malformed = DataKey::data_prefix(
            DataScope::LegacyUnscoped,
            LegacyLabelRows::Edge.logical_prefix(),
        )
        .to_vec();
        malformed.push(0x01);
        inner
            .put(
                &malformed,
                SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter([1])).encode(),
            )
            .await
            .unwrap();
        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::Encoding(_))
        ));
        // Still the store binaries before label identity open, hash rows intact.
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );
        assert!(marker(&inner, LABEL_IDENTITY_COPY_STARTED).await);
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert!(!exact.is_empty());
        // Every hash row, and the malformed one beside them.
        assert_eq!(legacy.len(), exact_before.len() + 1);
        let reader = open_reader(&store).await;
        assert_eq!(
            reader.reader_storage_compatibility_for_tests(),
            ReaderStorageCompatibility::LegacyLabelHash
        );
        assert_eq!(all_label_reads(&reader).await, reads);

        // A binary before label identity keeps the hash rows current, so the
        // partial copy goes stale: Bob leaves the Person row.
        let (ada, bob, _acme) = fixture_nodes(&legacy);
        let people = legacy_key(PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label(
            "Person",
        )));
        put_legacy_row(
            &inner,
            PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label("Person")),
            &[ada],
        )
        .await;
        inner.delete(&malformed).await.unwrap();

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.cleared, 2);
        assert_eq!(migration.copied, exact_before.len() as u64);
        let (exact, _legacy) = label_rows(inner.as_ref()).await;
        let exact_people = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::NodeLabel(NodeLabelKey::new(
                LabelIdentity::new("Person").unwrap(),
            ))),
        }
        .to_bytes();
        assert_eq!(exact[&exact_people], RoaringTreemap::from_iter([ada]));
        assert!(exact_before[&exact_people].contains(bob));
        assert_eq!(exact.len(), exact_before.len());
        assert!(label_rows(inner.as_ref()).await.1.contains_key(&people));
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rows_without_a_live_member_are_not_copied_and_are_cleaned_up() {
        let (_store, db, _reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        put_legacy_row(
            &inner,
            PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label("Ghost")),
            &[9_999],
        )
        .await;
        put_legacy_row(
            &inner,
            PropertyIndexKey::LegacyEdgeLabel(LegacyEdgeLabelKey::for_label("HAUNTS")),
            &[9_999],
        )
        .await;

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.copied, exact_before.len() as u64);
        assert_eq!(migration.skipped, 2);
        assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
        assert_eq!(
            delete_hash_label_rows(&inner, || false).await.unwrap(),
            LabelHashCleanup::Done {
                deleted: exact_before.len() as u64 + 2
            }
        );
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lowest_live_member_whose_label_disagrees_with_its_row_fails_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        let (ada, _bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
        // Ada, a Person, is the lowest live member of a Company row.
        put_legacy_row(
            &inner,
            PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label("Company")),
            &[ada, acme],
        )
        .await;

        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn neighbor_rows_no_live_edge_label_explains_are_not_copied() {
        let (_store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        let (ada, bob, _acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
        // Every HAUNTS edge is gone but its neighbor rows remain.
        put_legacy_row(
            &inner,
            PropertyIndexKey::LegacyEdgeLabel(LegacyEdgeLabelKey::for_label("HAUNTS")),
            &[9_999],
        )
        .await;
        for (direction, node, neighbor) in [
            (EdgeDirection::Out, ada, bob),
            (EdgeDirection::In, bob, ada),
        ] {
            put_legacy_row(
                &inner,
                PropertyIndexKey::LegacyEdgeLabelNeighbor(LegacyEdgeLabelNeighborKey::for_label(
                    direction, node, "HAUNTS",
                )),
                &[neighbor],
            )
            .await;
        }

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(
            (migration.copied, migration.skipped),
            (exact_before.len() as u64, 3)
        );
        assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lowest_live_member_whose_label_no_key_holds_fails_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        let long = "L".repeat(crate::encoding::indexes::label::MAX_LABEL_LEN + 1);
        inner
            .put(
                DataKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(9_999)),
                }
                .to_bytes(),
                encode_properties(&[Property::string("$label", &long)]),
            )
            .await
            .unwrap();
        put_legacy_row(
            &inner,
            PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label(&long)),
            &[9_999],
        )
        .await;

        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cleanup_waits_for_publication_and_resumes_after_a_stop() {
        let (_store, db, _reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        assert_eq!(
            delete_hash_label_rows(&inner, || false).await.unwrap(),
            LabelHashCleanup::Unpublished
        );
        assert_eq!(label_rows(inner.as_ref()).await.1.len(), exact_before.len());

        migrate_label_identity(&inner).await.unwrap();
        assert_eq!(
            delete_hash_label_rows(&inner, || true).await.unwrap(),
            LabelHashCleanup::Stopped {
                deleted: exact_before.len() as u64 / scopes().len() as u64
            }
        );
        assert!(!marker(&inner, LABEL_HASH_CLEANUP_READY).await);
        assert_eq!(
            delete_hash_label_rows(&inner, || false).await.unwrap(),
            LabelHashCleanup::Done {
                deleted: exact_before.len() as u64
                    - exact_before.len() as u64 / scopes().len() as u64
            }
        );
        assert!(label_rows(inner.as_ref()).await.1.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn migration_runs_only_from_the_operation_queue_version() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        let inner = db.inner_db();
        assert_eq!(
            migrate_label_identity(&inner).await.unwrap(),
            LabelIdentityMigration::default()
        );
        inner
            .put(
                marker_key(),
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::EQUALITY_BITMAPS,
                )),
            )
            .await
            .unwrap();
        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::EQUALITY_BITMAPS
        );
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn labels_longer_than_any_label_key_are_rejected_as_invalid_queries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        let label = "L".repeat(crate::encoding::indexes::label::MAX_LABEL_LEN + 1);
        let error = db
            .query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "node",
                        g().add_n(label.as_str(), vec![("name", PropertyInput::from("long"))]),
                    )
                    .returning(Vec::<String>::new()),
            ))
            .await
            .expect_err("an unkeyable label is rejected");
        assert!(
            error.to_string().contains("label index bound"),
            "unexpected error: {error}"
        );
        assert_eq!(
            error.error_code(),
            helix_ast::error_code::QueryErrorCode::InvalidQuery
        );
        db.close().await.unwrap();
    }
}
