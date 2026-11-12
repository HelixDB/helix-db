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
//! - A node or edge row's label is the `$label` of its lowest live member
//!   whose label hashes to the row. A live member whose label hashes elsewhere
//!   is a stale leftover of a label change: it is not copied. The highest live
//!   member whose label hashes to the row must have the same label, else two
//!   labels share the row's hash and the copy fails closed; so does a member
//!   with no string `$label`, or a label too long for any exact key. These two
//!   members name the whole row: the migration relies on no two labels of a
//!   scope sharing a hash, which production stores were verified to satisfy
//!   before it shipped, and the second member catches a pair that does unless
//!   one label holds both ends. A row with no such member indexes nothing and
//!   is not copied.
//! - Edge-label neighbor rows name opposite endpoints, not edges. Their labels
//!   come from the scope's edge label rows, copied first. A neighbor row whose
//!   hash no live edge label has is stale and is not copied.
//! - Each batch commits [`LABEL_IDENTITY_COPY_PROGRESS`] with its rows: the
//!   last hash row it copied and the open's SlateDB writer epoch. Every writer
//!   open fences the one before it with the next epoch, so progress recorded
//!   one epoch before this open's means no other writer opened since, and the
//!   copy resumes after it. Any other gap means another writer may have opened
//!   — a binary before label identity among them, which keeps the hash rows
//!   current but not the exact copy — so the copy clears every exact row and
//!   starts over. Resumed progress is re-recorded durably under this open's
//!   epoch before anything else, so the next open can resume from it too.
//! - The publishing commit also deletes [`LABEL_IDENTITY_COPY_PROGRESS`]. Once
//!   version 6 is published nothing reads the hash rows:
//!   [`delete_hash_label_rows_once`] removes them in background migration
//!   steps while the writer serves, recording [`LABEL_HASH_CLEANUP_PROGRESS`]
//!   as it goes and [`LABEL_HASH_CLEANUP_READY`] at the end. Like every
//!   background migration it runs only while the background migration worker
//!   is enabled; with [`crate::config::MigrationWorkerMode::Disabled`] the
//!   hash rows stay until a step is taken.
//!
//! # Cost
//!
//! The writer open blocks on the copy, which is linear in label rows: per
//! scope it scans each family of hash rows once, reads the records of the two
//! naming members of each node and edge row (and of any stale or dead members
//! before them), up to [`NAMING_CONCURRENCY`] at once, and writes every exact
//! row. Scans read ahead with the index lifecycle's bulk scan tuning, so a
//! scope's rows stream in a few object store reads; scopes are copied one at a
//! time, so a store of many small scopes is bound by each scope's handful of
//! sequential round trips rather than by its rows. A scope with no live edge
//! label skips its neighbor scan. An open that is stopped keeps every
//! committed batch for the next to resume from. Progress is logged at most
//! every [`PROGRESS_INTERVAL`]. The cleanup scans the hash rows once more after
//! the writer is serving, a batch per step.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::TryStreamExt;
use roaring::RoaringTreemap;
use slatedb::config::WriteOptions;
use slatedb::{Db, DbTransaction, IsolationLevel, KeyValue};

use crate::config::MigrationTuning;
use crate::encoding::indexes::prefix::exclusive_prefix_end_bound;
use crate::encoding::indexes::{
    hash_property_value, label, EdgeDirection, PropertyIndexKey, ValueHash,
};
use crate::encoding::keys::scope::DataScope;
use crate::encoding::keys::{EdgePropertyByIdKey, NodePropertyKey};
use crate::encoding::property::decode_properties;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::label_hash;
use crate::encoding::v2::values::indexes::{SecondaryEqualityBitmapValue, SecondaryEqualityValue};
use crate::encoding::v2::values::label_migration;
use crate::encoding::NodeId;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::IndexStorageVersion;
use crate::migrations::startup::{advance_storage_version, storage_version};
use crate::transaction::Mutation;

/// Metadata key of an unpublished copy's progress: its exact rows may be
/// incomplete or, if another writer opened since, stale.
const LABEL_IDENTITY_COPY_PROGRESS: &[u8] = b"kv_migration_progress:label_identity_copy";
/// Metadata marker of a published label identity migration whose hash rows are
/// all deleted.
const LABEL_HASH_CLEANUP_READY: &[u8] = b"kv_migration_ready:label_hash_cleanup";
/// Metadata key of an unfinished cleanup's progress: the last hash row it
/// deleted.
const LABEL_HASH_CLEANUP_PROGRESS: &[u8] = b"kv_migration_progress:label_hash_cleanup";

/// Rows copied or deleted per commit of the copy. Neighbor rows dominate: one
/// per endpoint, direction, and edge label.
const BATCH_ROWS: usize = 8192;
/// Stored row bytes written per commit of the copy, bounding one batch's
/// memory.
const BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Hash rows whose members' records are read at once while naming rows. Each
/// read may miss the cache and go to the object store, so they overlap.
const NAMING_CONCURRENCY: usize = 64;
/// Shortest interval between two progress logs.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// The exact row family one hash row is copied to, less the label.
#[derive(Debug, Clone, Copy)]
enum ExactRows {
    Node,
    Edge,
    Neighbor(EdgeDirection, NodeId),
}

impl ExactRows {
    fn key(self, scope: DataScope, label: label::LabelIdentity<'_>) -> Bytes {
        DataKey::Data {
            scope,
            kind: DataKeyKind::PropertyIndex(match self {
                Self::Node => PropertyIndexKey::NodeLabel(label::NodeLabelKey::new(label)),
                Self::Edge => PropertyIndexKey::EdgeLabel(label::EdgeLabelKey::new(label)),
                Self::Neighbor(direction, node) => PropertyIndexKey::EdgeLabelNeighbor(
                    label::EdgeLabelNeighborKey::new(direction, node, label),
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
    /// Whether it resumed after an earlier open's last committed batch.
    pub(crate) resumed: bool,
    /// Hash rows copied to their exact key.
    pub(crate) copied: u64,
    /// Hash rows no live member names, or stale neighbor rows no live edge
    /// label explains; nothing reads them once the migration is published.
    pub(crate) skipped: u64,
    /// Members left out of their copied row: live records whose label hashes
    /// to another row.
    pub(crate) stale_members: u64,
    /// Exact rows a stale earlier attempt left, deleted before copying.
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
            stale_members = self.rows.stale_members,
            cleared = self.rows.cleared,
            elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "label identity migration in progress"
        );
    }
}

/// One commit of rows, bounded by a row and a byte limit.
///
/// Each batch must commit before the next begins: consecutive batches all
/// write the progress key, and a transaction that began before another's
/// commit conflicts with it on that key.
struct Batch {
    transaction: DbTransaction,
    rows: usize,
    bytes: usize,
}

impl Batch {
    async fn begin(db: &Db) -> Result<Self> {
        Ok(Self {
            transaction: db.begin(IsolationLevel::Snapshot).await?,
            rows: 0,
            bytes: 0,
        })
    }

    /// Counts one row of `bytes` stored bytes; whether the batch reached
    /// `max_rows` rows or `max_bytes` bytes.
    fn staged(&mut self, bytes: usize, max_rows: usize, max_bytes: usize) -> bool {
        self.rows += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        self.rows >= max_rows || self.bytes >= max_bytes
    }

    async fn commit(self, await_durable: bool) -> Result<()> {
        self.transaction
            .commit_with_options(&WriteOptions {
                await_durable,
                ..WriteOptions::default()
            })
            .await?;
        Ok(())
    }
}

/// The scope and family of the hash label row `key`.
fn locate(key: &[u8]) -> Result<(DataScope, label_hash::LegacyLabelScanPrefix)> {
    let (scope, logical) = DataScope::strip_tenant_envelope(key)
        .map_or((DataScope::LegacyUnscoped, key), |(tenant, logical)| {
            (DataScope::Tenant(tenant), logical)
        });
    let family = label_hash::LegacyLabelScanPrefix::of(logical).ok_or_else(|| {
        HelixDbError::MigrationRequired {
            reason: "label identity progress names no hash label row".to_string(),
        }
    })?;
    Ok((scope, family))
}

/// Scans `scope`'s rows under the logical `prefix` after `after`, or all of
/// them.
async fn scan_family(
    db: &Db,
    scope: DataScope,
    prefix: Bytes,
    after: Option<Bytes>,
) -> Result<slatedb::DbIterator> {
    let prefix = DataKey::data_prefix(scope, prefix);
    let end = exclusive_prefix_end_bound(&prefix).map_or(Bound::Unbounded, Bound::Excluded);
    let start = after.map_or(Bound::Included(prefix), Bound::Excluded);
    // A bulk scan: it fetches blocks ahead, several at once, without filling
    // the block cache. One block per fetch makes every block a round trip.
    Ok(db
        .scan_with_options(
            (start, end),
            &crate::config::IndexLifecycleScanTuning::default().scan_options(),
        )
        .await?)
}

/// Whether the `b"1"` metadata marker `name` is set; a malformed marker
/// fails closed.
async fn marker_set(db: &Db, name: &[u8]) -> Result<bool> {
    match db
        .get(super::super::scoped_metadata_key(
            DataScope::LegacyUnscoped,
            name,
        ))
        .await?
        .as_deref()
    {
        None => Ok(false),
        Some(b"1") => Ok(true),
        Some(_) => Err(HelixDbError::MigrationRequired {
            reason: format!(
                "label identity marker {} is malformed",
                String::from_utf8_lossy(name)
            ),
        }),
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
    let progress_key =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_IDENTITY_COPY_PROGRESS);
    let writer_epoch = db.manifest().writer_epoch();
    let recorded = db
        .get(&progress_key)
        .await?
        .map(|value| {
            label_migration::LabelCopyProgressValue::decode(&value).map_err(|error| {
                HelixDbError::MigrationRequired {
                    reason: format!("label identity copy progress is malformed: {error}"),
                }
            })
        })
        .transpose()?;

    let scopes = crate::index_lifecycle::queue::recovery::discover_scopes(db).await?;
    let mut progress = Progress::new(scopes.len());
    let resume_after = match recorded {
        None => None,
        // The open before this one advanced it, and no writer opened between.
        Some(recorded) if recorded.writer_epoch().checked_add(1) == Some(writer_epoch) => {
            progress.rows.resumed = true;
            recorded.resume_after().cloned()
        }
        Some(_) => {
            // Another writer may have changed hash rows the copy had copied.
            for &scope in &scopes {
                for prefix in label::LabelScanPrefix::ALL {
                    let mut rows = scan_family(db, scope, prefix.to_bytes(), None).await?;
                    let mut batch = Batch::begin(db).await?;
                    while let Some(row) = rows.next().await? {
                        batch.transaction.delete(&row.key)?;
                        progress.rows.cleared += 1;
                        if batch.staged(row.key.len(), BATCH_ROWS, BATCH_BYTES) {
                            batch.commit(false).await?;
                            batch = Batch::begin(db).await?;
                            progress.batch_written();
                        }
                    }
                    batch.commit(false).await?;
                }
            }
            None
        }
    };
    // Durable before any copied row, so the next open resumes from this one.
    let claim = Batch::begin(db).await?;
    Mutation::put(
        &claim.transaction,
        &progress_key,
        label_migration::LabelCopyProgressValue::new(writer_epoch, resume_after.clone()).encode(),
    )?;
    claim.commit(true).await?;

    let start = resume_after
        .as_deref()
        .map(locate)
        .transpose()?
        .map(|(scope, family)| {
            scopes
                .iter()
                .position(|candidate| *candidate == scope)
                .map(|index| (index, family))
                .ok_or_else(|| HelixDbError::MigrationRequired {
                    reason: format!("label identity progress names {scope:?}, which holds no row"),
                })
        })
        .transpose()?;
    tracing::info!(
        scopes = scopes.len(),
        writer_epoch,
        resumed = progress.rows.resumed,
        cleared = progress.rows.cleared,
        "label identity migration copying hash label rows"
    );
    for (index, &scope) in scopes.iter().enumerate() {
        progress.scope = index;
        let resume_in_scope = match start {
            Some((start, _)) if index < start => continue,
            Some((start, family)) if index == start => Some(family),
            Some(_) | None => None,
        };
        // A resumed scope's copied edge labels name its neighbor rows.
        let mut edge_labels = BTreeMap::new();
        if resume_in_scope.is_some() {
            let mut rows = scan_family(
                db,
                scope,
                label::LabelScanPrefix::EdgeLabels.to_bytes(),
                None,
            )
            .await?;
            while let Some(row) = rows.next().await? {
                let DataKey::Data {
                    kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(key)),
                    ..
                } = DataKey::parse_from_slice(scope, &row.key)?
                else {
                    unreachable!("the edge label prefix holds edge label rows");
                };
                edge_labels.insert(
                    *label_hash::LegacyEdgeLabelKey::for_label(key.identity().label()).label_hash(),
                    label::SharedLabel::new(key.identity()),
                );
            }
        }
        for family in label_hash::LegacyLabelScanPrefix::ALL {
            let after = match resume_in_scope {
                Some(resumed) if family < resumed => continue,
                Some(resumed) if family == resumed => resume_after.clone(),
                Some(_) | None => None,
            };
            // Without a live edge label no neighbor row has a label to copy to.
            if family == label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors
                && edge_labels.is_empty()
            {
                continue;
            }
            copy_rows(
                db,
                writer_epoch,
                scope,
                family,
                after,
                &mut edge_labels,
                &mut progress,
            )
            .await?;
        }
        progress.rows.scopes += 1;
    }

    db.flush().await?;
    // The published copy is complete, so its progress goes with the old version.
    advance_storage_version(
        db,
        IndexStorageVersion::OPERATION_QUEUES..IndexStorageVersion::LABEL_IDENTITY,
        IndexStorageVersion::LABEL_IDENTITY,
        |transaction| Ok(transaction.delete(&progress_key)?),
    )
    .await?;
    let migration = progress.rows;
    tracing::info!(
        scopes = migration.scopes,
        resumed = migration.resumed,
        copied = migration.copied,
        skipped = migration.skipped,
        stale_members = migration.stale_members,
        cleared = migration.cleared,
        elapsed_ms = u64::try_from(progress.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "label identity migration published exact label rows"
    );
    Ok(migration)
}

/// One hash row, parsed and, for node and edge rows, named.
enum ParsedRow {
    /// A node or edge row: its hash, and its label unless no member names it.
    Named {
        hash: ValueHash,
        label: Option<RowLabel>,
    },
    /// A neighbor row, named from the scope's edge labels.
    Neighbor(label_hash::LegacyEdgeLabelNeighborKey),
}

/// Copies `scope`'s `family` rows after `after`, or all of them, to their
/// exact keys, committing each batch with the copy's progress.
///
/// The edge pass records each exact edge label by the hash its neighbor rows
/// use in `edge_labels`; the neighbor pass names its rows from it.
async fn copy_rows(
    db: &Db,
    writer_epoch: u64,
    scope: DataScope,
    family: label_hash::LegacyLabelScanPrefix,
    after: Option<Bytes>,
    edge_labels: &mut BTreeMap<ValueHash, label::SharedLabel>,
    progress: &mut Progress,
) -> Result<()> {
    let progress_key =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_IDENTITY_COPY_PROGRESS);
    let rows = scan_family(db, scope, family.to_bytes(), after).await?;
    // Naming reads overlap, but rows are staged in key order, so a batch's
    // progress is the last row it holds.
    let named = futures::stream::try_unfold(rows, |mut rows| async move {
        Ok::<_, HelixDbError>(rows.next().await?.map(|row| (row, rows)))
    })
    .map_ok(|row: KeyValue| async move {
        let Some(logical) = scope.strip_key(&row.key) else {
            return Err(HelixDbError::InvariantViolation(
                "label identity scan returned a key outside its scope".to_string(),
            ));
        };
        let members = SecondaryEqualityValue::decode(&row.value)?.into_ids();
        let parsed = match family {
            label_hash::LegacyLabelScanPrefix::NodeLabels => {
                let PropertyIndexKey::Equality(key) = PropertyIndexKey::parse_from_slice(logical)?
                else {
                    unreachable!("the legacy node label prefix holds equality rows");
                };
                ParsedRow::Named {
                    hash: key.value_hash,
                    label: row_label(
                        db,
                        scope,
                        &members,
                        |id| DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
                        |label| hash_property_value(label) == key.value_hash,
                    )
                    .await?,
                }
            }
            label_hash::LegacyLabelScanPrefix::EdgeLabels => {
                let key = label_hash::LegacyEdgeLabelKey::parse_from_slice(logical)?;
                ParsedRow::Named {
                    hash: *key.label_hash(),
                    label: row_label(
                        db,
                        scope,
                        &members,
                        |id| DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(id)),
                        |label| hash_property_value(label) == *key.label_hash(),
                    )
                    .await?,
                }
            }
            label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors => ParsedRow::Neighbor(
                label_hash::LegacyEdgeLabelNeighborKey::parse_from_slice(logical)?,
            ),
        };
        Ok((row, members, parsed))
    })
    .try_buffered(NAMING_CONCURRENCY);
    futures::pin_mut!(named);

    let mut batch = Batch::begin(db).await?;
    let mut last = None;
    while let Some((row, mut members, parsed)) = named.try_next().await? {
        let exact = match parsed {
            ParsedRow::Named { label: None, .. } => None,
            ParsedRow::Named {
                hash,
                label: Some(RowLabel { label, stale }),
            } => {
                progress.rows.stale_members += stale.len();
                members -= stale;
                Some(match family {
                    label_hash::LegacyLabelScanPrefix::NodeLabels => {
                        ExactRows::Node.key(scope, label.identity())
                    }
                    label_hash::LegacyLabelScanPrefix::EdgeLabels => {
                        let exact = ExactRows::Edge.key(scope, label.identity());
                        // One hash row per hash, so each hash names one edge label.
                        edge_labels.insert(hash, label);
                        exact
                    }
                    label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors => {
                        unreachable!("neighbor rows are named from edge labels")
                    }
                })
            }
            ParsedRow::Neighbor(key) => edge_labels.get(key.label_hash()).map(|label| {
                ExactRows::Neighbor(key.direction(), key.node_id()).key(scope, label.identity())
            }),
        };
        match exact {
            Some(exact) => {
                batch
                    .transaction
                    .merge(exact, SecondaryEqualityBitmapValue::new(members).encode())?;
                progress.rows.copied += 1;
            }
            None => progress.rows.skipped += 1,
        }
        let full = batch.staged(row.key.len() + row.value.len(), BATCH_ROWS, BATCH_BYTES);
        last = Some(row.key);
        if full {
            Mutation::put(
                &batch.transaction,
                &progress_key,
                label_migration::LabelCopyProgressValue::new(writer_epoch, last.take()).encode(),
            )?;
            batch.commit(false).await?;
            batch = Batch::begin(db).await?;
            progress.batch_written();
        }
    }
    if last.is_some() {
        Mutation::put(
            &batch.transaction,
            &progress_key,
            label_migration::LabelCopyProgressValue::new(writer_epoch, last).encode(),
        )?;
    }
    batch.commit(false).await?;
    progress.batch_written();
    Ok(())
}

/// The label naming one node or edge hash row, and the live members whose
/// labels hash to another row.
struct RowLabel {
    label: label::SharedLabel,
    stale: RoaringTreemap,
}

/// The `$label` of the live member `id`, or `None` when its record is gone.
/// `record` names a member's property record.
async fn member_label(
    db: &Db,
    scope: DataScope,
    record: &impl Fn(u64) -> DataKeyKind<'static>,
    id: u64,
) -> Result<Option<String>> {
    let record = record(id);
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
        return Ok(None);
    };
    decode_properties(&stored)?
        .into_iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str().map(str::to_owned))
        .map(Some)
        .ok_or_else(|| HelixDbError::MigrationRequired {
            reason: format!("{scope:?} {record:?} is indexed by label but has no string $label"),
        })
}

/// The label of one node or edge hash row; see the module contract. `None`
/// when no live member's label hashes to the row. `hashes_to_row` tells
/// whether a label hashes to the row.
async fn row_label(
    db: &Db,
    scope: DataScope,
    members: &RoaringTreemap,
    record: impl Fn(u64) -> DataKeyKind<'static>,
    hashes_to_row: impl Fn(&str) -> bool,
) -> Result<Option<RowLabel>> {
    let mut stale = RoaringTreemap::new();
    let mut lowest = None;
    for member in members {
        let Some(label) = member_label(db, scope, &record, member).await? else {
            continue;
        };
        if !hashes_to_row(&label) {
            stale.insert(member);
            continue;
        }
        lowest = Some((member, label));
        break;
    }
    let Some((lowest, label)) = lowest else {
        return Ok(None);
    };
    for member in members.iter().rev().take_while(|member| *member > lowest) {
        let Some(highest) = member_label(db, scope, &record, member).await? else {
            continue;
        };
        if !hashes_to_row(&highest) {
            stale.insert(member);
            continue;
        }
        if highest != label {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "{scope:?} labels {label:?} and {highest:?} share one label hash row; \
                     neither can be copied to its exact key"
                ),
            });
        }
        break;
    }
    let Ok(identity) = label::LabelIdentity::new(&label) else {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "{scope:?} {:?} has a {}-byte $label, longer than any label key holds",
                record(lowest),
                label.len()
            ),
        });
    };
    Ok(Some(RowLabel {
        label: label::SharedLabel::new(identity),
        stale,
    }))
}

/// What one cleanup step of retired hash label rows did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelHashCleanup {
    /// The migration is not published, so its hash rows are still read.
    Unpublished,
    /// An earlier step already deleted every hash row.
    AlreadyDone,
    /// One batch deleted; a later step resumes after it.
    Advanced { deleted: u64 },
    /// The last hash rows are deleted and the cleanup is recorded.
    Done { deleted: u64 },
}

impl LabelHashCleanup {
    /// Whether a later step has more to delete.
    pub(crate) const fn advanced(self) -> bool {
        matches!(self, Self::Advanced { .. })
    }
}

/// Deletes the next batch of retired hash label rows, at most `tuning`'s
/// batch rows and bytes, once the label identity migration is published, and
/// records [`LABEL_HASH_CLEANUP_READY`] with the last.
///
/// Nothing reads those rows by then, and graph writes never touch them, so it
/// runs beside the serving writer. Each batch commits
/// [`LABEL_HASH_CLEANUP_PROGRESS`] with its deletes, so the next step, of this
/// open or a later one, starts after it rather than rescanning deleted rows.
pub(crate) async fn delete_hash_label_rows_once(
    db: &Db,
    tuning: MigrationTuning,
) -> Result<LabelHashCleanup> {
    if marker_set(db, LABEL_HASH_CLEANUP_READY).await? {
        return Ok(LabelHashCleanup::AlreadyDone);
    }
    if crate::migrations::startup::stored_storage_version(db)
        .await?
        .is_none_or(|version| version < IndexStorageVersion::LABEL_IDENTITY)
    {
        return Ok(LabelHashCleanup::Unpublished);
    }
    let progress_key =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_HASH_CLEANUP_PROGRESS);
    let mut after = db
        .get(&progress_key)
        .await?
        .map(|value| {
            label_migration::LabelCleanupProgressValue::decode(&value)
                .map(|progress| progress.resume_after().clone())
                .map_err(|error| HelixDbError::MigrationRequired {
                    reason: format!("label hash cleanup progress is malformed: {error}"),
                })
        })
        .transpose()?;
    let (mut scope, mut family) = after.as_deref().map(locate).transpose()?.unwrap_or((
        DataScope::LegacyUnscoped,
        label_hash::LegacyLabelScanPrefix::NodeLabels,
    ));
    let mut batch = Batch::begin(db).await?;
    let mut deleted = 0u64;
    loop {
        let mut rows = scan_family(db, scope, family.to_bytes(), after.take()).await?;
        while let Some(row) = rows.next().await? {
            batch.transaction.delete(&row.key)?;
            deleted += 1;
            if batch.staged(
                row.key.len(),
                tuning.batch_rows().get(),
                tuning.batch_bytes().get(),
            ) {
                Mutation::put(
                    &batch.transaction,
                    &progress_key,
                    label_migration::LabelCleanupProgressValue::new(row.key).encode(),
                )?;
                batch.commit(false).await?;
                return Ok(LabelHashCleanup::Advanced { deleted });
            }
        }
        match label_hash::LegacyLabelScanPrefix::ALL
            .into_iter()
            .skip_while(|candidate| *candidate != family)
            .nth(1)
        {
            Some(next) => family = next,
            None => {
                let Some(next) =
                    crate::index_lifecycle::queue::recovery::next_scope(db, scope).await?
                else {
                    break;
                };
                (scope, family) = (next, label_hash::LegacyLabelScanPrefix::NodeLabels);
            }
        }
    }
    batch.transaction.delete(&progress_key)?;
    Mutation::put(
        &batch.transaction,
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_HASH_CLEANUP_READY),
        Bytes::from_static(b"1"),
    )?;
    batch.commit(false).await?;
    tracing::info!(
        deleted,
        "label identity cleanup deleted the last retired hash label rows"
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
    use crate::encoding::v2::values::encode_metadata_value;
    use crate::index_lifecycle::IndexV2MetadataValue;

    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(db).await? {
        for prefix in label::LabelScanPrefix::ALL {
            let mut rows = scan_family(db, scope, prefix.to_bytes(), None).await?;
            let batch = Batch::begin(db).await?;
            while let Some(row) = rows.next().await? {
                let DataKey::Data {
                    kind: DataKeyKind::PropertyIndex(key),
                    ..
                } = DataKey::parse_from_slice(scope, &row.key)?
                else {
                    unreachable!("label prefixes hold property index rows");
                };
                let legacy = match key {
                    PropertyIndexKey::NodeLabel(key) => DataKey::Data {
                        scope,
                        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
                            label_hash::LegacyNodeLabelKey::for_label(key.identity().label()),
                        )),
                    }
                    .to_bytes(),
                    PropertyIndexKey::EdgeLabel(key) => DataKey::data_prefix(
                        scope,
                        label_hash::LegacyEdgeLabelKey::for_label(key.identity().label())
                            .to_bytes(),
                    ),
                    PropertyIndexKey::EdgeLabelNeighbor(key) => DataKey::data_prefix(
                        scope,
                        label_hash::LegacyEdgeLabelNeighborKey::for_label(
                            key.direction(),
                            key.node_id(),
                            key.identity().label(),
                        )
                        .to_bytes(),
                    ),
                    other @ (PropertyIndexKey::Equality(_)
                    | PropertyIndexKey::EdgeEquality(_)
                    | PropertyIndexKey::GlobalEdgeEquality(_)
                    | PropertyIndexKey::Range(_)
                    | PropertyIndexKey::EdgeRange(_)
                    | PropertyIndexKey::GlobalEdgeRange(_)) => {
                        unreachable!("label prefixes hold only label rows, got {other:?}")
                    }
                };
                batch.transaction.merge(
                    legacy,
                    SecondaryEqualityBitmapValue::new(
                        SecondaryEqualityValue::decode(&row.value)?.into_ids(),
                    )
                    .encode(),
                )?;
                batch.transaction.delete(&row.key)?;
            }
            batch.commit(false).await?;
        }
    }
    let rewind = Batch::begin(db).await?;
    Mutation::put(
        &rewind.transaction,
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::OPERATION_QUEUES,
        )),
    )?;
    for name in [
        LABEL_IDENTITY_COPY_PROGRESS,
        LABEL_HASH_CLEANUP_READY,
        LABEL_HASH_CLEANUP_PROGRESS,
    ] {
        rewind
            .transaction
            .delete(super::super::scoped_metadata_key(
                DataScope::LegacyUnscoped,
                name,
            ))?;
    }
    rewind.commit(false).await?;
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
    use crate::encoding::v2::values::encode_metadata_value;
    use crate::index_lifecycle::repository::ReaderStorageCompatibility;
    use crate::index_lifecycle::IndexV2MetadataValue;

    type Rows = BTreeMap<Bytes, RoaringTreemap>;

    const DB_PATH: &str = "label-identity";

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

    fn metadata(name: &[u8]) -> Bytes {
        super::super::super::scoped_metadata_key(DataScope::LegacyUnscoped, name)
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
            let prefixes = label::LabelScanPrefix::ALL
                .map(|prefix| (true, prefix.to_bytes()))
                .into_iter()
                .chain(
                    label_hash::LegacyLabelScanPrefix::ALL.map(|prefix| (false, prefix.to_bytes())),
                );
            for (is_exact, prefix) in prefixes {
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

    fn node_row(label: &str) -> Bytes {
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
                label_hash::LegacyNodeLabelKey::for_label(label),
            )),
        }
        .to_bytes()
    }

    fn edge_row(label: &str) -> Bytes {
        label_hash::LegacyEdgeLabelKey::for_label(label).to_bytes()
    }

    fn neighbor_row(direction: EdgeDirection, node: NodeId, label: &str) -> Bytes {
        label_hash::LegacyEdgeLabelNeighborKey::for_label(direction, node, label).to_bytes()
    }

    fn exact_node_row(label: &str) -> Bytes {
        ExactRows::Node.key(
            DataScope::LegacyUnscoped,
            label::LabelIdentity::new(label).unwrap(),
        )
    }

    async fn put_legacy_row(db: &Db, key: &Bytes, ids: &[u64]) {
        crate::transaction::put_unbatched(
            db,
            key,
            &SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter(ids.iter().copied()))
                .encode(),
        )
        .await
        .unwrap();
    }

    /// Stores a node record of `label` under `id` in the unscoped scope.
    async fn put_node(db: &Db, id: u64, label: &str) {
        crate::transaction::put_unbatched(
            db,
            &DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
            }
            .to_bytes(),
            &encode_properties(&[Property::string("$label", label)]),
        )
        .await
        .unwrap();
    }

    /// A writer without the background migration worker, so no cleanup races
    /// a test's hash rows.
    async fn open_writer(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
        crate::HelixDB::open_with_object_store_and_config(
            DB_PATH,
            Arc::clone(store),
            crate::config::DbConfig::new().with_migration_tuning(
                MigrationTuning::default()
                    .with_worker_mode(crate::config::MigrationWorkerMode::Disabled),
            ),
        )
        .await
        .expect("writer opens")
    }

    /// A bare SlateDB writer of the store, which runs no startup migration:
    /// each open fences the last with the next writer epoch.
    async fn open_raw(store: &Arc<dyn ObjectStore>) -> Db {
        Db::builder(DB_PATH, Arc::clone(store))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("raw writer opens")
    }

    async fn open_reader(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
        crate::HelixDB::open_reader_with_object_store_for_tests(DB_PATH, Arc::clone(store))
            .await
            .expect("a store before or during label identity is reader-compatible")
    }

    /// The unscoped fixture graph's node IDs, from its legacy label rows.
    fn fixture_nodes(legacy: &Rows) -> (u64, u64, u64) {
        let [ada, bob] = legacy[&node_row("Person")].iter().collect::<Vec<_>>()[..] else {
            panic!("the fixture has two people");
        };
        let acme = legacy[&node_row("Company")]
            .min()
            .expect("the fixture has a company");
        (ada, bob, acme)
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

    async fn present(db: &Db, name: &[u8]) -> bool {
        db.get(metadata(name)).await.unwrap().is_some()
    }

    async fn copy_progress(db: &Db) -> Option<label_migration::LabelCopyProgressValue> {
        db.get(metadata(LABEL_IDENTITY_COPY_PROGRESS))
            .await
            .unwrap()
            .map(|value| label_migration::LabelCopyProgressValue::decode(&value).unwrap())
    }

    /// Copies the unscoped scope's node and edge rows as the open at
    /// `writer_epoch` would have before it was stopped.
    async fn copy_unscoped_nodes_and_edges(db: &Db, writer_epoch: u64) -> Progress {
        let mut partial = Progress::new(scopes().len());
        let mut edge_labels = BTreeMap::new();
        for family in [
            label_hash::LegacyLabelScanPrefix::NodeLabels,
            label_hash::LegacyLabelScanPrefix::EdgeLabels,
        ] {
            copy_rows(
                db,
                writer_epoch,
                DataScope::LegacyUnscoped,
                family,
                None,
                &mut edge_labels,
                &mut partial,
            )
            .await
            .unwrap();
        }
        assert_eq!(partial.rows.copied, 4);
        partial
    }

    /// Takes cleanup steps of `batch_rows` rows until the cleanup is done,
    /// returning each step's outcome.
    async fn clean_up(db: &Db, batch_rows: usize) -> Vec<LabelHashCleanup> {
        let tuning = MigrationTuning::default()
            .with_batch_rows(crate::config::MigrationBatchRows::new(batch_rows).unwrap());
        let mut steps = Vec::new();
        loop {
            let step = delete_hash_label_rows_once(db, tuning).await.unwrap();
            steps.push(step);
            if !step.advanced() {
                return steps;
            }
        }
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

        // The open before this one was stopped after the unscoped scope's node
        // and edge rows were copied; every hash row stays in place.
        let writer_epoch = inner.manifest().writer_epoch();
        let partial = copy_unscoped_nodes_and_edges(&inner, writer_epoch - 1).await;
        inner.flush().await.unwrap();
        // Read once the reader's own snapshot holds the partial copy.
        let crate::HelixStorage::Reader(storage) = reader.storage() else {
            unreachable!("the reader opened in reader mode");
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = storage.snapshot().await.unwrap();
                let (exact, legacy) = label_rows(snapshot.as_ref()).await;
                if exact.len() == 4 {
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

        // This open resumes after the last copied row instead of starting over.
        let migration = migrate_label_identity(&inner).await.unwrap();
        assert!(migration.resumed);
        assert_eq!(migration.cleared, 0);
        assert_eq!(
            migration.copied,
            exact_before.len() as u64 - partial.rows.copied
        );
        assert_eq!((migration.skipped, migration.stale_members), (0, 0));
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        // Published, but the hash rows stay until the cleanup.
        assert_eq!(legacy.len(), exact_before.len());
        assert!(!present(&inner, LABEL_IDENTITY_COPY_PROGRESS).await);
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
            clean_up(&inner, 1_024).await,
            [LabelHashCleanup::Done {
                deleted: exact_before.len() as u64
            }]
        );
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert!(marker_set(&inner, LABEL_HASH_CLEANUP_READY).await.unwrap());
        assert_eq!(
            clean_up(&inner, 1_024).await,
            [LabelHashCleanup::AlreadyDone]
        );
        assert_eq!(all_label_reads(&reader).await, reads);

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_open_resumes_the_copy_its_predecessor_was_stopped_in() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let stopped_epoch = db.inner_db().manifest().writer_epoch();
        copy_unscoped_nodes_and_edges(&db.inner_db(), stopped_epoch).await;
        db.close().await.unwrap();

        let raw = open_raw(&store).await;
        // Each writer open fences the one before it with the next epoch.
        assert_eq!(raw.manifest().writer_epoch(), stopped_epoch + 1);
        let migration = migrate_label_identity(&raw).await.unwrap();
        assert!(migration.resumed);
        assert_eq!(migration.cleared, 0);
        assert_eq!(migration.copied, exact_before.len() as u64 - 4);
        assert_eq!(label_rows(&raw).await.0, exact_before);
        raw.close().await.unwrap();

        let db = open_writer(&store).await;
        assert_eq!(db.inner_db().manifest().writer_epoch(), stopped_epoch + 2);
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    /// Writes a graph of `nodes` nodes with `degree` out-edges each straight
    /// into records and hash label rows of the unscoped scope, as a binary
    /// before label identity filed it, and returns its labels by hash.
    async fn write_raw_legacy_graph(
        db: &Db,
        nodes: u64,
        degree: u64,
    ) -> BTreeMap<ValueHash, String> {
        let node_label = |index: u64| format!("Label{}", index % 3);
        let edge_label = |index: u64| format!("TYPE_{}", index % 2);
        let mut rows = BTreeMap::<Bytes, RoaringTreemap>::new();
        let mut records = Vec::new();
        let mut edge = 0;
        for index in 0..nodes {
            let node = index + 1;
            records.push((
                DataKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node)),
                }
                .to_bytes(),
                encode_properties(&[Property::string("$label", node_label(index))]),
            ));
            rows.entry(node_row(&node_label(index)))
                .or_default()
                .insert(node);
            for offset in 0..degree {
                edge += 1;
                let target = (index * 7_919 + offset * 104_729 + 1) % nodes + 1;
                let label = edge_label(index + offset);
                records.push((
                    DataKey::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge)),
                    }
                    .to_bytes(),
                    encode_properties(&[Property::string("$label", &label)]),
                ));
                rows.entry(edge_row(&label)).or_default().insert(edge);
                rows.entry(neighbor_row(EdgeDirection::Out, node, &label))
                    .or_default()
                    .insert(target);
                rows.entry(neighbor_row(EdgeDirection::In, target, &label))
                    .or_default()
                    .insert(node);
            }
        }
        let staged = records.into_iter().chain(
            rows.into_iter()
                .map(|(key, ids)| (key, SecondaryEqualityBitmapValue::new(ids).encode())),
        );
        let mut batch = Batch::begin(db).await.unwrap();
        for (key, value) in staged {
            Mutation::put(&batch.transaction, key, value).unwrap();
            if batch.staged(0, 10_000, usize::MAX) {
                batch.commit(false).await.unwrap();
                batch = Batch::begin(db).await.unwrap();
            }
        }
        batch.commit(false).await.unwrap();
        (0..3)
            .map(node_label)
            .chain((0..2).map(edge_label))
            .map(|label| (hash_property_value(&label), label))
            .collect()
    }

    /// The exact key the unscoped hash row `key` is copied to.
    fn exact_key_of(key: &[u8], labels: &BTreeMap<ValueHash, String>) -> Bytes {
        let identity = |hash: &ValueHash| label::LabelIdentity::new(&labels[hash]).unwrap();
        match label_hash::LegacyLabelScanPrefix::of(key) {
            Some(label_hash::LegacyLabelScanPrefix::NodeLabels) => {
                let Ok(PropertyIndexKey::Equality(row)) = PropertyIndexKey::parse_from_slice(key)
                else {
                    panic!("a node label row is an equality row");
                };
                ExactRows::Node.key(DataScope::LegacyUnscoped, identity(&row.value_hash))
            }
            Some(label_hash::LegacyLabelScanPrefix::EdgeLabels) => {
                let row = label_hash::LegacyEdgeLabelKey::parse_from_slice(key).unwrap();
                ExactRows::Edge.key(DataScope::LegacyUnscoped, identity(row.label_hash()))
            }
            Some(label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors) => {
                let row = label_hash::LegacyEdgeLabelNeighborKey::parse_from_slice(key).unwrap();
                ExactRows::Neighbor(row.direction(), row.node_id())
                    .key(DataScope::LegacyUnscoped, identity(row.label_hash()))
            }
            None => panic!("not a hash label row"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_copy_stopped_mid_family_resumes_after_its_last_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        open_writer(&store).await.close().await.unwrap();
        let raw = open_raw(&store).await;
        let labels = write_raw_legacy_graph(&raw, 6_000, 3).await;
        crate::transaction::put_unbatched(
            &raw,
            &marker_key(),
            &encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::OPERATION_QUEUES,
            )),
        )
        .await
        .unwrap();
        let (exact, legacy) = label_rows(&raw).await;
        assert!(exact.is_empty());
        let neighbor_rows = legacy
            .keys()
            .filter(|key| {
                label_hash::LegacyLabelScanPrefix::of(key)
                    == Some(label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors)
            })
            .count();
        // More neighbor rows than two batches hold, so the family commits a
        // batch after another before it stops.
        assert!(neighbor_rows > 2 * BATCH_ROWS);
        // A malformed neighbor row sorts after every real one and stops the copy.
        let mut malformed = label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors
            .to_bytes()
            .to_vec();
        malformed.push(0x02);
        malformed.extend_from_slice(&[0; 16]);
        let malformed = Bytes::from(malformed);
        put_legacy_row(&raw, &malformed, &[1]).await;
        assert!(matches!(
            migrate_label_identity(&raw).await,
            Err(HelixDbError::Encoding(_))
        ));
        let progress = copy_progress(&raw)
            .await
            .expect("the copy recorded progress");
        let resume_after = progress.resume_after().expect("a batch was committed");
        assert_eq!(
            label_hash::LegacyLabelScanPrefix::of(resume_after),
            Some(label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors)
        );
        raw.delete(&malformed).await.unwrap();
        raw.close().await.unwrap();

        let raw = open_raw(&store).await;
        let migration = migrate_label_identity(&raw).await.unwrap();
        assert!(migration.resumed);
        assert_eq!((migration.cleared, migration.skipped), (0, 0));
        // Every full batch was kept; only the last, partial one is copied again.
        assert_eq!(migration.copied as usize, neighbor_rows % BATCH_ROWS);
        let (exact, legacy) = label_rows(&raw).await;
        let expected = legacy
            .iter()
            .map(|(key, ids)| (exact_key_of(key, &labels), ids.clone()))
            .collect::<Rows>();
        assert_eq!(exact.len(), legacy.len());
        assert_eq!(exact, expected);
        raw.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_copy_another_writer_opened_over_starts_over() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let stopped_epoch = db.inner_db().manifest().writer_epoch();
        copy_unscoped_nodes_and_edges(&db.inner_db(), stopped_epoch).await;
        let (ada, bob, _acme) = fixture_nodes(&label_rows(db.inner_db().as_ref()).await.1);
        db.close().await.unwrap();

        // A binary before label identity opens and keeps the hash rows
        // current, so the partial copy goes stale: Bob leaves the Person row.
        let old = open_raw(&store).await;
        put_legacy_row(&old, &node_row("Person"), &[ada]).await;
        old.close().await.unwrap();

        let raw = open_raw(&store).await;
        assert_eq!(raw.manifest().writer_epoch(), stopped_epoch + 2);
        let migration = migrate_label_identity(&raw).await.unwrap();
        assert!(!migration.resumed);
        assert_eq!(migration.cleared, 4);
        assert_eq!(migration.copied, exact_before.len() as u64);
        let (exact, _legacy) = label_rows(&raw).await;
        assert_eq!(
            exact[&exact_node_row("Person")],
            RoaringTreemap::from_iter([ada])
        );
        assert!(exact_before[&exact_node_row("Person")].contains(bob));
        assert_eq!(exact.len(), exact_before.len());
        raw.close().await.unwrap();

        let db = open_writer(&store).await;
        let mut people = reads[0].clone();
        people["people"] = serde_json::json!(1);
        people["employers"] = serde_json::json!(1);
        assert_eq!(label_reads(&db, DataScope::LegacyUnscoped).await, people);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn public_label_helpers_follow_the_store_layout() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
        let scope = DataScope::LegacyUnscoped;
        let lookups = || async {
            let read = inner.begin(IsolationLevel::Snapshot).await.unwrap();
            (
                crate::search::lookup_node_label_index_scoped(&read, "Person", scope)
                    .await
                    .unwrap(),
                crate::search::lookup_equality_index_set_scoped(&read, "$label", "Person", scope)
                    .await
                    .unwrap(),
                crate::search::lookup_global_edge_label_index_scoped(&read, "WORKS_AT", scope)
                    .await
                    .unwrap()
                    .len(),
                crate::search::lookup_in_neighbors_by_label_scoped(&read, acme, "WORKS_AT", scope)
                    .await
                    .unwrap(),
            )
        };
        let people = RoaringTreemap::from_iter([ada, bob]);
        let expected = (people.clone(), people.clone(), 2, people.clone());
        // Before publication they read the hash rows, the only complete ones.
        assert_eq!(lookups().await, expected);
        migrate_label_identity(&inner).await.unwrap();
        assert_eq!(lookups().await, expected);
        clean_up(&inner, 1_024).await;
        assert_eq!(lookups().await, expected);

        // `$label` equality writes maintain the exact node label row.
        let write = inner
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::search::add_to_equality_index_scoped(&write, "$label", "Person", 77, scope).unwrap();
        crate::search::remove_from_equality_index_scoped(&write, "$label", "Person", bob, scope)
            .unwrap();
        // Removing a label too long for any key removes nothing, as for graph
        // writes.
        let long = "L".repeat(label::MAX_LABEL_LEN + 1);
        crate::search::remove_from_equality_index_scoped(&write, "$label", &long, ada, scope)
            .unwrap();
        crate::search::remove_from_edge_label_index_scoped(&write, ada, acme, &long, scope)
            .unwrap();
        crate::search::remove_from_global_edge_label_index_scoped(&write, &long, 1, scope).unwrap();
        write.commit().await.unwrap();
        let people = RoaringTreemap::from_iter([ada, 77]);
        let (by_label, by_equality, _, _) = lookups().await;
        assert_eq!((by_label, by_equality), (people.clone(), people));
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writer_open_migrates_and_the_background_worker_cleans_up() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let epoch = db.inner_db().manifest().writer_epoch();
        db.close().await.unwrap();

        // The default configuration runs the background migration worker.
        let db = crate::HelixDB::open_with_object_store_for_tests(DB_PATH, Arc::clone(&store))
            .await
            .expect("writer opens");
        let inner = db.inner_db();
        // One writer open takes one writer epoch, so its successor can resume.
        assert_eq!(inner.manifest().writer_epoch(), epoch + 1);
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        assert_eq!(all_label_reads(&db).await, reads);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !present(&inner, LABEL_HASH_CLEANUP_READY).await {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the background worker deletes the hash rows");
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert!(!present(&inner, LABEL_HASH_CLEANUP_PROGRESS).await);
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_copy_leaves_a_version_5_store_that_a_retry_rebuilds() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        // A malformed edge row fails the copy after node rows were copied.
        let mut malformed = label_hash::LegacyLabelScanPrefix::EdgeLabels
            .to_bytes()
            .to_vec();
        malformed.push(0x01);
        put_legacy_row(&inner, &Bytes::from(malformed.clone()), &[1]).await;
        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::Encoding(_))
        ));
        // Still the store binaries before label identity open, hash rows intact.
        assert_eq!(
            storage_version(inner.as_ref()).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );
        let progress = copy_progress(&inner)
            .await
            .expect("the copy recorded progress");
        assert_eq!(progress.writer_epoch(), inner.manifest().writer_epoch());
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact.len(), 2);
        // Every hash row, and the malformed one beside them.
        assert_eq!(legacy.len(), exact_before.len() + 1);
        let reader = open_reader(&store).await;
        assert_eq!(
            reader.reader_storage_compatibility_for_tests(),
            ReaderStorageCompatibility::LegacyLabelHash
        );
        assert_eq!(all_label_reads(&reader).await, reads);

        // Progress of this very open is not its predecessor's, so a retry
        // in it does not trust its copy and starts over.
        inner.delete(&malformed).await.unwrap();
        let migration = migrate_label_identity(&inner).await.unwrap();
        assert!(!migration.resumed);
        assert_eq!(migration.cleared, 2);
        assert_eq!(migration.copied, exact_before.len() as u64);
        assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rows_without_a_live_member_are_not_copied_and_are_cleaned_up() {
        let (_store, db, _reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        put_legacy_row(&inner, &node_row("Ghost"), &[9_999]).await;
        put_legacy_row(&inner, &edge_row("HAUNTS"), &[9_999]).await;

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.copied, exact_before.len() as u64);
        assert_eq!(migration.skipped, 2);
        assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
        assert_eq!(
            clean_up(&inner, 1_024).await,
            [LabelHashCleanup::Done {
                deleted: exact_before.len() as u64 + 2
            }]
        );
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_members_are_left_out_of_the_row_they_no_longer_belong_to() {
        let (_store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
        // Ada and Bob, both Persons, stayed in the Company row after a label
        // change, below and above Acme, its one real member.
        assert!(ada < acme && bob < acme);
        put_node(&inner, 9_999, "Person").await;
        put_legacy_row(&inner, &node_row("Company"), &[ada, bob, acme, 9_999]).await;

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.stale_members, 3);
        assert_eq!(migration.copied, exact_before.len() as u64);
        let (exact, _legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert_eq!(
            exact[&exact_node_row("Company")],
            RoaringTreemap::from_iter([acme])
        );
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_labels_naming_one_row_fail_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
        let record = |id| DataKeyKind::NodeProperty(NodePropertyKey::new(id));
        let members = RoaringTreemap::from_iter([ada, bob, acme, 9_999]);

        // As if "Person" and "Company" shared a hash: the lowest and highest
        // live members disagree.
        assert!(matches!(
            row_label(&inner, DataScope::LegacyUnscoped, &members, record, |_| {
                true
            })
            .await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        // A dead highest member is passed over for the next live one.
        let RowLabel { label, stale } = row_label(
            &inner,
            DataScope::LegacyUnscoped,
            &RoaringTreemap::from_iter([ada, bob, 9_999]),
            record,
            |_| true,
        )
        .await
        .unwrap()
        .expect("the people name the row");
        assert_eq!(label.identity().label(), "Person");
        assert!(stale.is_empty());
        // No live member whose label hashes to the row names nothing.
        assert!(
            row_label(&inner, DataScope::LegacyUnscoped, &members, record, |_| {
                false
            })
            .await
            .unwrap()
            .is_none()
        );

        // A labeled record without a string $label fails closed.
        crate::transaction::put_unbatched(
            &inner,
            &DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: record(9_999),
            }
            .to_bytes(),
            &encode_properties(&[Property::string("name", "nameless")]),
        )
        .await
        .unwrap();
        assert!(matches!(
            row_label(
                &inner,
                DataScope::LegacyUnscoped,
                &RoaringTreemap::from_iter([9_999]),
                record,
                |_| true
            )
            .await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn neighbor_rows_no_live_edge_label_explains_are_not_copied() {
        let (_store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        let (ada, bob, _acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
        // Every HAUNTS edge is gone but its neighbor rows remain.
        put_legacy_row(&inner, &edge_row("HAUNTS"), &[9_999]).await;
        for (direction, node, neighbor) in [
            (EdgeDirection::Out, ada, bob),
            (EdgeDirection::In, bob, ada),
        ] {
            put_legacy_row(
                &inner,
                &neighbor_row(direction, node, "HAUNTS"),
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
    async fn a_naming_member_whose_label_no_key_holds_fails_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        let long = "L".repeat(label::MAX_LABEL_LEN + 1);
        put_node(&inner, 9_999, &long).await;
        put_legacy_row(&inner, &node_row(&long), &[9_999]).await;

        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cleanup_waits_for_publication_and_resumes_after_every_batch() {
        let (store, db, _reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        assert_eq!(clean_up(&inner, 4).await, [LabelHashCleanup::Unpublished]);
        assert_eq!(label_rows(inner.as_ref()).await.1.len(), exact_before.len());

        migrate_label_identity(&inner).await.unwrap();
        let tuning = MigrationTuning::default()
            .with_batch_rows(crate::config::MigrationBatchRows::new(4).unwrap());
        assert_eq!(
            delete_hash_label_rows_once(&inner, tuning).await.unwrap(),
            LabelHashCleanup::Advanced { deleted: 4 }
        );
        assert!(present(&inner, LABEL_HASH_CLEANUP_PROGRESS).await);
        assert!(!present(&inner, LABEL_HASH_CLEANUP_READY).await);
        assert_eq!(
            label_rows(inner.as_ref()).await.1.len(),
            exact_before.len() - 4
        );
        db.close().await.unwrap();

        // A later open resumes after the last deleted row, a batch per step.
        let db = open_writer(&store).await;
        let inner = db.inner_db();
        let steps = clean_up(&inner, 4).await;
        let remaining = exact_before.len() as u64 - 4;
        let (last, batches) = steps.split_last().unwrap();
        assert!(batches
            .iter()
            .all(|step| *step == LabelHashCleanup::Advanced { deleted: 4 }));
        assert_eq!(
            *last,
            LabelHashCleanup::Done {
                deleted: remaining % 4
            }
        );
        assert_eq!(batches.len() as u64, remaining / 4);
        assert!(label_rows(inner.as_ref()).await.1.is_empty());
        assert!(!present(&inner, LABEL_HASH_CLEANUP_PROGRESS).await);
        assert!(marker_set(&inner, LABEL_HASH_CLEANUP_READY).await.unwrap());
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn malformed_markers_and_progress_fail_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        for (name, value) in [
            (LABEL_IDENTITY_COPY_PROGRESS, Bytes::from_static(b"short")),
            (LABEL_HASH_CLEANUP_PROGRESS, Bytes::new()),
            (LABEL_HASH_CLEANUP_READY, Bytes::from_static(b"yes")),
        ] {
            crate::transaction::put_unbatched(&inner, &metadata(name), &value)
                .await
                .unwrap();
            let failed = if name == LABEL_IDENTITY_COPY_PROGRESS {
                migrate_label_identity(&inner).await.map(|_| ())
            } else {
                if name == LABEL_HASH_CLEANUP_PROGRESS {
                    migrate_label_identity(&inner).await.unwrap();
                }
                delete_hash_label_rows_once(&inner, MigrationTuning::default())
                    .await
                    .map(|_| ())
            };
            assert!(
                matches!(failed, Err(HelixDbError::MigrationRequired { .. })),
                "{}: {failed:?}",
                String::from_utf8_lossy(name)
            );
            inner.delete(metadata(name)).await.unwrap();
        }
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
        crate::transaction::put_unbatched(
            &inner,
            &marker_key(),
            &encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
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
        let label = "L".repeat(label::MAX_LABEL_LEN + 1);
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
