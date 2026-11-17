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
//!   hash no live edge label has is not copied. No graph write leaves one: an
//!   edge enters its neighbor rows and its global row in one commit and leaves
//!   its global row first, so only an index already damaged holds one.
//! - Each batch commits [`LABEL_IDENTITY_COPY_PROGRESS`] with its rows: the
//!   last hash row it copied, how many hash rows the copy has passed up to it,
//!   and the newest SlateDB sequence number among them. An open resumes after
//!   that row only if every hash row up to it is one the copy read: as many as
//!   it counted, and none newer. SlateDB keeps a row's sequence number through
//!   compaction, a merged row carries its newest operand's, and every later
//!   write takes a later one. So any writer that changed a hash row the copy had
//!   passed, a binary before label identity among them, which keeps the hash
//!   rows current but not the exact copy, leaves a newer row or a missing one,
//!   and the copy clears every exact row and starts over. Writer epochs play no
//!   part: managed epochs need not be consecutive, and an open rejected before
//!   any write still takes one.
//! - The publishing commit also deletes [`LABEL_IDENTITY_COPY_PROGRESS`]. Once
//!   version 6 is published nothing reads the hash rows:
//!   [`delete_hash_label_rows_once`] removes them in background migration
//!   steps while the writer serves, recording [`LABEL_HASH_CLEANUP_PROGRESS`]
//!   as it goes and [`LABEL_HASH_CLEANUP_READY`] at the end. Every writer takes
//!   these steps, a failover writer, which runs no migration, included, so a
//!   writer that takes over finishes the cleanup. They run only while the
//!   background migration worker is enabled; with
//!   [`crate::config::MigrationWorkerMode::Disabled`] the hash rows stay until a
//!   step is taken.
//!
//! # Cost
//!
//! The writer open blocks on the copy, which is linear in label rows. One
//! [`LabelRowWalk`] reads every scope's hash rows in key order on one
//! iterator, seeking past every other row, so a scope without label rows
//! costs a few seeks rather than scans of its own, and a batch spans scopes.
//! The copy reads the records of the two naming members of each node and edge
//! row (and of any stale or dead members before them), up to
//! [`NAMING_CONCURRENCY`] at once, and writes every exact row. A resumed copy
//! first walks the hash rows it passed again, without reading records. An open
//! that is stopped keeps every committed batch for the next to resume from.
//! Progress is logged at most every [`PROGRESS_INTERVAL`]. The cleanup walks
//! the hash rows once more after the writer is serving: each step deletes at
//! most one batch and enters at most [`CLEANUP_SCOPES_PER_STEP`] scopes, so a
//! step over many tenants with nothing left to delete stays short too.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::ops::Bound;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::TryStreamExt;
use roaring::RoaringTreemap;
use slatedb::config::WriteOptions;
use slatedb::{Db, DbTransaction, IsolationLevel, KeyValue};

use crate::config::MigrationTuning;
use crate::encoding::indexes::{
    hash_property_value, label, EdgeDirection, PropertyIndexKey, ValueHash,
};
use crate::encoding::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
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

/// Metadata key of an unpublished copy's progress.
const LABEL_IDENTITY_COPY_PROGRESS: &[u8] = b"kv_migration_progress:label_identity_copy";
/// Metadata marker of a published label identity migration whose hash rows are
/// all deleted.
const LABEL_HASH_CLEANUP_READY: &[u8] = b"kv_migration_ready:label_hash_cleanup";
/// Metadata key of an unfinished cleanup's progress.
const LABEL_HASH_CLEANUP_PROGRESS: &[u8] = b"kv_migration_progress:label_hash_cleanup";

/// Rows copied or deleted per commit of the copy. Neighbor rows dominate: one
/// per endpoint, direction, and edge label.
const BATCH_ROWS: usize = 8192;
/// Bytes of the keys and values one commit of the copy stages, bounding one
/// batch's memory.
const BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Hash rows whose members' records are read at once while naming rows. Each
/// read may miss the cache and go to the object store, so they overlap.
const NAMING_CONCURRENCY: usize = 64;
/// Shortest interval between two progress logs.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
/// Scopes one cleanup step enters after the one it starts in.
const CLEANUP_SCOPES_PER_STEP: usize = 256;

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
    /// Scopes whose hash rows this open copied.
    pub(crate) scopes: usize,
    /// Whether it resumed after an earlier open's last committed batch.
    pub(crate) resumed: bool,
    /// Hash rows copied to their exact key.
    pub(crate) copied: u64,
    /// Hash rows no live member names, or neighbor rows no live edge label
    /// explains; nothing reads them once the migration is published.
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
    started: Instant,
    reported: Instant,
}

impl Progress {
    fn new() -> Self {
        Self {
            rows: LabelIdentityMigration::default(),
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
            scopes = self.rows.scopes,
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

    /// Counts one row whose staged writes hold `bytes`; whether the batch
    /// reached `max_rows` rows or `max_bytes` bytes.
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

/// A forward walk over three label row families of every scope, on one
/// iterator: the unscoped scope first, then every tenant by ID, and in each
/// scope its families in the order given, which must be their key order.
///
/// A scope's rows of one family are one contiguous key range. Past it the walk
/// seeks to the scope's next family, and past a scope's last family to the
/// next tenant, so it reads no other row: a scope without label rows costs a
/// few seeks on the open iterator, not scans of its own.
struct LabelRowWalk<F: Copy> {
    rows: slatedb::DbIterator,
    families: [F; 3],
    logical_prefixes: [Bytes; 3],
    scope: DataScope,
    family: usize,
    /// `scope`'s physical prefix of `families[family]`.
    prefix: Bytes,
    /// A row read past the walk's family, examined against the next one.
    pending: Option<KeyValue>,
    /// The scope of the last row read.
    entered: DataScope,
    /// Further scopes the walk may read rows of, or `None` for every scope.
    scopes_left: Option<usize>,
    /// The first label row prefix of the scope the walk stopped before, once
    /// it stops at its scope limit.
    stopped_before: Option<Bytes>,
}

impl<F: Copy> LabelRowWalk<F> {
    /// Opens a walk of `families`, whose logical prefixes `prefix_of` gives,
    /// from the first row after `after`, or from the first row of all. With a
    /// `scope_limit`, it stops before reading rows of more scopes than that
    /// after the one it starts in. A scope's rows other than label rows count:
    /// the walk reads one to learn the scope is there.
    async fn open(
        db: &Db,
        families: [F; 3],
        prefix_of: fn(F) -> Bytes,
        after: Option<&Bytes>,
        scope_limit: Option<usize>,
    ) -> Result<Self> {
        let logical_prefixes = families.map(prefix_of);
        let (scope, family) = match after {
            Some(after) => locate(&logical_prefixes, after)?,
            None => (DataScope::LegacyUnscoped, 0),
        };
        let prefix = DataKey::data_prefix(scope, logical_prefixes[family].clone());
        let start = match after {
            Some(after) => Bound::Excluded(after.clone()),
            None => Bound::Included(prefix.clone()),
        };
        // Tenant envelopes sort after every unscoped label row, and last of
        // every data scope.
        let end = Bound::Excluded(Bytes::from_static(&[TENANT_KEY_PREFIX + 1]));
        // A bulk scan: it fetches blocks ahead, several at once, without
        // filling the block cache. One block per fetch makes every block a
        // round trip.
        let rows = db
            .scan_with_options(
                (start, end),
                &crate::config::IndexLifecycleScanTuning::default().scan_options(),
            )
            .await?;
        Ok(Self {
            rows,
            families,
            logical_prefixes,
            scope,
            family,
            prefix,
            pending: None,
            entered: scope,
            scopes_left: scope_limit,
            stopped_before: None,
        })
    }

    /// The next row, with its scope and family; `None` once every scope is
    /// walked, or once the walk stops at its scope limit.
    async fn next(&mut self) -> Result<Option<(DataScope, F, KeyValue)>> {
        loop {
            let row = match self.pending.take() {
                Some(row) => row,
                None => {
                    let Some(row) = self.rows.next().await? else {
                        return Ok(None);
                    };
                    row
                }
            };
            let row_scope = match DataScope::strip_tenant_envelope(&row.key) {
                Some((tenant, _)) => DataScope::Tenant(tenant),
                None if row.key.first() == Some(&TENANT_KEY_PREFIX) => {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "label row walk encountered an invalid tenant envelope".to_string(),
                    ));
                }
                None => DataScope::LegacyUnscoped,
            };
            if row_scope != self.entered {
                self.entered = row_scope;
                match &mut self.scopes_left {
                    Some(0) => {
                        self.stopped_before = Some(DataKey::data_prefix(
                            row_scope,
                            self.logical_prefixes[0].clone(),
                        ));
                        return Ok(None);
                    }
                    Some(left) => *left -= 1,
                    None => {}
                }
            }
            if row.key.starts_with(&self.prefix) {
                return Ok(Some((self.scope, self.families[self.family], row)));
            }
            if row.key < self.prefix {
                // Records and other indexes sort between label families.
                self.rows.seek(&self.prefix).await?;
                continue;
            }
            // Past this family of this scope: on to the scope's next family,
            // or to the next scope.
            let (scope, family) = if row_scope > self.scope {
                (row_scope, 0)
            } else if self.family + 1 < self.families.len() {
                (self.scope, self.family + 1)
            } else {
                let next = match self.scope {
                    DataScope::LegacyUnscoped => Some(TenantId::from_u128(0)),
                    DataScope::Tenant(tenant) => {
                        tenant.as_u128().checked_add(1).map(TenantId::from_u128)
                    }
                };
                let Some(next) = next else {
                    return Ok(None);
                };
                (DataScope::Tenant(next), 0)
            };
            self.scope = scope;
            self.family = family;
            self.prefix = DataKey::data_prefix(scope, self.logical_prefixes[family].clone());
            self.pending = Some(row);
        }
    }

    /// The first label row prefix of the scope the walk stopped before, if it
    /// stopped at its scope limit.
    fn stopped_before(&self) -> Option<&Bytes> {
        self.stopped_before.as_ref()
    }
}

/// The scope and family of `key`, a row or family prefix under one of
/// `logical_prefixes`.
fn locate(logical_prefixes: &[Bytes; 3], key: &[u8]) -> Result<(DataScope, usize)> {
    let (scope, logical) = DataScope::strip_tenant_envelope(key)
        .map_or((DataScope::LegacyUnscoped, key), |(tenant, logical)| {
            (DataScope::Tenant(tenant), logical)
        });
    let Some(family) = logical_prefixes
        .iter()
        .position(|prefix| logical.starts_with(prefix))
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "label identity progress names no label row".to_string(),
        });
    };
    Ok((scope, family))
}

/// A walk of every hash label row after `after`, or of every one.
async fn hash_rows(
    db: &Db,
    after: Option<&Bytes>,
    scope_limit: Option<usize>,
) -> Result<LabelRowWalk<label_hash::LegacyLabelScanPrefix>> {
    LabelRowWalk::open(
        db,
        label_hash::LegacyLabelScanPrefix::ALL,
        label_hash::LegacyLabelScanPrefix::to_bytes,
        after,
        scope_limit,
    )
    .await
}

/// A walk of every exact label row.
async fn exact_rows(db: &Db) -> Result<LabelRowWalk<label::LabelScanPrefix>> {
    LabelRowWalk::open(
        db,
        label::LabelScanPrefix::ALL,
        label::LabelScanPrefix::to_bytes,
        None,
        None,
    )
    .await
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
    migrate_label_identity_in_batches(db, BATCH_ROWS).await
}

/// [`migrate_label_identity`], committing the copy every `batch_rows` hash
/// rows at most.
async fn migrate_label_identity_in_batches(
    db: &Db,
    batch_rows: usize,
) -> Result<LabelIdentityMigration> {
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

    let mut progress = Progress::new();
    let resume = match recorded {
        None => None,
        Some(recorded) => {
            if copied_rows_unchanged(db, &recorded).await? {
                progress.rows.resumed = true;
                Some(recorded)
            } else {
                clear_exact_rows(db, &progress_key, &mut progress).await?;
                None
            }
        }
    };
    tracing::info!(
        resumed = progress.rows.resumed,
        cleared = progress.rows.cleared,
        "label identity migration copying hash label rows"
    );
    copy_rows(db, resume, batch_rows, &mut progress).await?;

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

/// Whether every hash row up to `recorded`'s resume point is one the copy
/// read: as many as it counted, and none written after the newest it read.
async fn copied_rows_unchanged(
    db: &Db,
    recorded: &label_migration::LabelCopyProgressValue,
) -> Result<bool> {
    let mut walk = hash_rows(db, None, None).await?;
    let mut rows = 0u64;
    while let Some((_, _, row)) = walk.next().await? {
        if row.key.as_ref() > recorded.resume_after().as_ref() {
            break;
        }
        if row.seq > recorded.newest_seq() {
            return Ok(false);
        }
        rows += 1;
    }
    Ok(rows == recorded.rows().get())
}

/// Deletes every exact label row, which a copy another writer made stale left,
/// and then the copy's progress.
async fn clear_exact_rows(db: &Db, progress_key: &Bytes, progress: &mut Progress) -> Result<()> {
    let mut walk = exact_rows(db).await?;
    let mut batch = Batch::begin(db).await?;
    while let Some((_, _, row)) = walk.next().await? {
        batch.transaction.delete(&row.key)?;
        progress.rows.cleared += 1;
        if batch.staged(row.key.len(), BATCH_ROWS, BATCH_BYTES) {
            batch.commit(false).await?;
            batch = Batch::begin(db).await?;
            progress.batch_written();
        }
    }
    // Last, so an open stopped before it clears again rather than resuming.
    batch.transaction.delete(progress_key)?;
    batch.commit(false).await
}

/// One hash row, parsed and, for node and edge rows, named.
enum ParsedRow {
    /// A node row, and its label unless no member names it.
    Node(Option<RowLabel>),
    /// An edge row: its hash, and its label unless no member names it.
    Edge {
        hash: ValueHash,
        label: Option<RowLabel>,
    },
    /// A neighbor row, named from its scope's edge labels.
    Neighbor(label_hash::LegacyEdgeLabelNeighborKey),
}

/// Copies every hash row after `resume`'s, or every one, to its exact key,
/// committing each batch of at most `batch_rows` rows with the copy's
/// progress.
///
/// Rows arrive in key order, a scope at a time, and a scope's edge rows
/// before its neighbor rows: the edge rows record each exact edge label by
/// the hash its neighbor rows use. A copy resumed inside a scope reloads the
/// labels of the edge rows it copied there.
async fn copy_rows(
    db: &Db,
    resume: Option<label_migration::LabelCopyProgressValue>,
    batch_rows: usize,
    progress: &mut Progress,
) -> Result<()> {
    let progress_key =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_IDENTITY_COPY_PROGRESS);
    let (mut newest_seq, mut rows) = resume
        .as_ref()
        .map_or((0, 0), |resume| (resume.newest_seq(), resume.rows().get()));
    let walk = hash_rows(
        db,
        resume.as_ref().map(|resume| resume.resume_after()),
        None,
    )
    .await?;
    let mut edge_labels = match resume {
        Some(_) => {
            progress.rows.scopes += 1;
            Some((walk.scope, copied_edge_labels(db, walk.scope).await?))
        }
        None => None,
    };
    // Naming reads overlap, but rows are staged in key order, so a batch's
    // progress is the last row it holds.
    let named = futures::stream::try_unfold(walk, |mut walk| async move {
        Ok::<_, HelixDbError>(walk.next().await?.map(|row| (row, walk)))
    })
    .map_ok(
        |(scope, family, row): (DataScope, _, KeyValue)| async move {
            let Some(logical) = scope.strip_key(&row.key) else {
                return Err(HelixDbError::InvariantViolation(
                    "label identity walk returned a key outside its scope".to_string(),
                ));
            };
            let members = SecondaryEqualityValue::decode(&row.value)?.into_ids();
            let parsed = match family {
                label_hash::LegacyLabelScanPrefix::NodeLabels => {
                    let PropertyIndexKey::Equality(key) =
                        PropertyIndexKey::parse_from_slice(logical)?
                    else {
                        unreachable!("the legacy node label prefix holds equality rows");
                    };
                    ParsedRow::Node(
                        row_label(
                            db,
                            scope,
                            &members,
                            |id| DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
                            |label| hash_property_value(label) == key.value_hash,
                        )
                        .await?,
                    )
                }
                label_hash::LegacyLabelScanPrefix::EdgeLabels => {
                    let key = label_hash::LegacyEdgeLabelKey::parse_from_slice(logical)?;
                    ParsedRow::Edge {
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
            Ok((scope, row, members, parsed))
        },
    )
    .try_buffered(NAMING_CONCURRENCY);
    futures::pin_mut!(named);

    let mut batch = Batch::begin(db).await?;
    let mut unrecorded = None;
    while let Some((scope, row, mut members, parsed)) = named.try_next().await? {
        if edge_labels
            .as_ref()
            .is_none_or(|(labels_scope, _)| *labels_scope != scope)
        {
            progress.rows.scopes += 1;
            edge_labels = Some((scope, BTreeMap::new()));
        }
        let (_, labels) = edge_labels
            .as_mut()
            .expect("the row's scope has its edge labels");
        let exact = match parsed {
            ParsedRow::Node(None) | ParsedRow::Edge { label: None, .. } => None,
            ParsedRow::Node(Some(RowLabel { label, stale })) => {
                progress.rows.stale_members += stale.len();
                members -= stale;
                Some(ExactRows::Node.key(scope, label.identity()))
            }
            ParsedRow::Edge {
                hash,
                label: Some(RowLabel { label, stale }),
            } => {
                progress.rows.stale_members += stale.len();
                members -= stale;
                let exact = ExactRows::Edge.key(scope, label.identity());
                // One hash row per hash, so each hash names one edge label.
                labels.insert(hash, label);
                Some(exact)
            }
            ParsedRow::Neighbor(key) => labels.get(key.label_hash()).map(|label| {
                ExactRows::Neighbor(key.direction(), key.node_id()).key(scope, label.identity())
            }),
        };
        let staged = match exact {
            Some(exact) => {
                let value = SecondaryEqualityBitmapValue::new(members).encode();
                let staged = exact.len() + value.len();
                batch.transaction.merge(exact, value)?;
                progress.rows.copied += 1;
                staged
            }
            None => {
                progress.rows.skipped += 1;
                0
            }
        };
        rows += 1;
        newest_seq = newest_seq.max(row.seq);
        let recorded = label_migration::LabelCopyProgressValue::new(
            newest_seq,
            NonZeroU64::new(rows).expect("the copy counts this row"),
            row.key,
        );
        if !batch.staged(staged, batch_rows, BATCH_BYTES) {
            unrecorded = Some(recorded);
            continue;
        }
        Mutation::put(&batch.transaction, &progress_key, recorded.encode())?;
        batch.commit(false).await?;
        batch = Batch::begin(db).await?;
        unrecorded = None;
        progress.batch_written();
    }
    unrecorded
        .map(|recorded| Mutation::put(&batch.transaction, &progress_key, recorded.encode()))
        .transpose()?;
    batch.commit(false).await?;
    progress.batch_written();
    Ok(())
}

/// The exact edge labels `scope`'s copied edge rows hold, by the hash their
/// neighbor rows use.
async fn copied_edge_labels(
    db: &Db,
    scope: DataScope,
) -> Result<BTreeMap<ValueHash, label::SharedLabel>> {
    let mut rows = db
        .scan_prefix(
            DataKey::data_prefix(scope, label::LabelScanPrefix::EdgeLabels.to_bytes()),
            ..,
        )
        .await?;
    let mut labels = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let DataKey::Data {
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(key)),
            ..
        } = DataKey::parse_from_slice(scope, &row.key)?
        else {
            unreachable!("the edge label prefix holds edge label rows");
        };
        labels.insert(
            *label_hash::LegacyEdgeLabelKey::for_label(key.identity().label()).label_hash(),
            label::SharedLabel::new(key.identity()),
        );
    }
    Ok(labels)
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
    /// One bounded step: a full batch, or the rows of every scope it entered
    /// before its scope limit. A later step resumes after it.
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

/// Deletes the next retired hash label rows once the label identity migration
/// is published: at most `tuning`'s batch rows and bytes, from at most
/// [`CLEANUP_SCOPES_PER_STEP`] scopes after the one it starts in. It records
/// [`LABEL_HASH_CLEANUP_READY`] with the last.
///
/// Nothing reads those rows by then, and graph writes never touch them, so it
/// runs beside the serving writer, of any open intent. Each step commits
/// [`LABEL_HASH_CLEANUP_PROGRESS`] with its deletes: the last row it deleted,
/// or the scope it stopped before. The next step, of this open or a later one,
/// starts there rather than walking deleted rows or empty scopes again.
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
    let after = db
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
    let mut walk = hash_rows(db, after.as_ref(), Some(CLEANUP_SCOPES_PER_STEP)).await?;
    let mut batch = Batch::begin(db).await?;
    let mut deleted = 0u64;
    while let Some((_, _, row)) = walk.next().await? {
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
    let Some(stopped_before) = walk.stopped_before().cloned() else {
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
        return Ok(LabelHashCleanup::Done { deleted });
    };
    Mutation::put(
        &batch.transaction,
        &progress_key,
        label_migration::LabelCleanupProgressValue::new(stopped_before).encode(),
    )?;
    batch.commit(false).await?;
    Ok(LabelHashCleanup::Advanced { deleted })
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

    let mut walk = exact_rows(db).await?;
    let batch = Batch::begin(db).await?;
    while let Some((scope, _, row)) = walk.next().await? {
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
                label_hash::LegacyEdgeLabelKey::for_label(key.identity().label()).to_bytes(),
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

    /// A malformed hash row sorting first among `scope`'s `family` rows: a copy
    /// fails there after committing every row before it.
    fn malformed_row(scope: DataScope, family: label_hash::LegacyLabelScanPrefix) -> Bytes {
        let mut logical = family.to_bytes().to_vec();
        logical.push(0x00);
        DataKey::data_prefix(scope, Bytes::from(logical))
    }

    /// Stops a copy at the malformed row `before`, after it committed every
    /// hash row before it a batch per row, and removes the row: the store an
    /// open stopped there leaves. Returns the copy's progress.
    async fn stop_copy_before(db: &Db, before: Bytes) -> label_migration::LabelCopyProgressValue {
        put_legacy_row(db, &before, &[1]).await;
        assert!(matches!(
            migrate_label_identity_in_batches(db, 1).await,
            Err(HelixDbError::Encoding(_))
        ));
        db.delete(&before).await.unwrap();
        copy_progress(db)
            .await
            .expect("the stopped copy recorded progress")
    }

    /// A managed failover writer of the store at `writer_epoch`, with the
    /// default background migration worker.
    async fn open_failover(
        store: &Arc<dyn ObjectStore>,
        writer_epoch: u64,
    ) -> Result<crate::HelixDB> {
        crate::HelixDB::open_writer_inner_with_index_scheduling(
            DB_PATH.to_string(),
            Arc::clone(store),
            crate::GcBoundaryFiles::AsConfigured,
            crate::config::DbConfig::new(),
            crate::WriterOpenMode::Managed {
                writer_epoch: NonZeroU64::new(writer_epoch).unwrap(),
                intent: crate::ManagedWriterOpenIntent::Failover,
            },
            crate::IndexLifecycleScheduling::Configured,
        )
        .await
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

        // An open was stopped after the unscoped scope's node and edge rows
        // were copied; every hash row stays in place.
        let stopped = stop_copy_before(
            &inner,
            malformed_row(
                DataScope::LegacyUnscoped,
                label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors,
            ),
        )
        .await;
        assert_eq!(stopped.rows().get(), 4);
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
        assert_eq!(migration.copied, exact_before.len() as u64 - 4);
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

    /// Writer epochs play no part in resuming: writers that wrote no hash row
    /// may open between, at any later epoch, a failover rejected for the
    /// version-5 store among them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_copy_resumes_across_writers_that_wrote_no_hash_row() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let stopped = stop_copy_before(
            &db.inner_db(),
            malformed_row(
                DataScope::LegacyUnscoped,
                label_hash::LegacyLabelScanPrefix::EdgeLabelNeighbors,
            ),
        )
        .await;
        let stopped_epoch = db.inner_db().manifest().writer_epoch();
        db.close().await.unwrap();

        open_raw(&store).await.close().await.unwrap();
        let Err(error) = open_failover(&store, stopped_epoch + 10).await else {
            panic!("failover opens only a current store");
        };
        assert!(
            matches!(error, HelixDbError::WriterMigrationRequired { .. }),
            "{error}"
        );

        let raw = open_raw(&store).await;
        assert!(raw.manifest().writer_epoch() > stopped_epoch + 10);
        let migration = migrate_label_identity(&raw).await.unwrap();
        assert!(migration.resumed);
        assert_eq!(migration.cleared, 0);
        assert_eq!(
            migration.copied,
            exact_before.len() as u64 - stopped.rows().get()
        );
        assert_eq!(label_rows(&raw).await.0, exact_before);
        raw.close().await.unwrap();

        let db = open_writer(&store).await;
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
        let hash_rows = legacy.len();
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
        let resume_after = progress.resume_after();
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
        // Every full batch was kept, across families; only the last, partial
        // one is copied again.
        assert_eq!(migration.copied as usize, hash_rows % BATCH_ROWS);
        let (exact, legacy) = label_rows(&raw).await;
        let expected = legacy
            .iter()
            .map(|(key, ids)| (exact_key_of(key, &labels), ids.clone()))
            .collect::<Rows>();
        assert_eq!(exact.len(), legacy.len());
        assert_eq!(exact, expected);
        raw.close().await.unwrap();
    }

    /// A writer that changed a hash row the stopped copy had passed, as a
    /// binary before label identity keeps them current, leaves a newer row, a
    /// missing one, or a new scope before the resume point, and the copy starts
    /// over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_copy_starts_over_when_a_hash_row_it_passed_changed() {
        #[derive(Debug, Clone, Copy)]
        enum Change {
            Rewritten,
            Deleted,
            EarlierScope,
        }
        let earlier =
            DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000005").unwrap());
        for change in [Change::Rewritten, Change::Deleted, Change::EarlierScope] {
            let (store, db, reads, exact_before) = legacy_store().await;
            // The unscoped scope and the first tenant were copied.
            let stopped = stop_copy_before(
                &db.inner_db(),
                malformed_row(scopes()[2], label_hash::LegacyLabelScanPrefix::NodeLabels),
            )
            .await;
            let copied = 2 * exact_before.len() / scopes().len();
            assert_eq!(stopped.rows().get() as usize, copied);
            let (ada, bob, _acme) = fixture_nodes(&label_rows(db.inner_db().as_ref()).await.1);
            db.close().await.unwrap();

            let old = open_raw(&store).await;
            match change {
                // Bob leaves the Person row.
                Change::Rewritten => put_legacy_row(&old, &node_row("Person"), &[ada]).await,
                Change::Deleted => {
                    old.delete(node_row("Company")).await.unwrap();
                }
                // A row no live member names.
                Change::EarlierScope => {
                    put_legacy_row(
                        &old,
                        &DataKey::data_prefix(earlier, edge_row("KNOWS")),
                        &[1],
                    )
                    .await;
                }
            }
            old.close().await.unwrap();

            let raw = open_raw(&store).await;
            let migration = migrate_label_identity(&raw).await.unwrap();
            assert!(!migration.resumed, "{change:?}");
            assert_eq!(migration.cleared as usize, copied, "{change:?}");
            let (exact, _legacy) = label_rows(&raw).await;
            match change {
                Change::Rewritten => {
                    assert_eq!(migration.copied as usize, exact_before.len());
                    assert_eq!(
                        exact[&exact_node_row("Person")],
                        RoaringTreemap::from_iter([ada])
                    );
                    assert!(exact_before[&exact_node_row("Person")].contains(bob));
                }
                Change::Deleted => {
                    assert_eq!(migration.copied as usize, exact_before.len() - 1);
                    assert!(!exact.contains_key(&exact_node_row("Company")));
                }
                Change::EarlierScope => {
                    assert_eq!(
                        (migration.copied as usize, migration.skipped),
                        (exact_before.len(), 1)
                    );
                    assert_eq!(migration.scopes, scopes().len() + 1);
                    assert_eq!(exact, exact_before);
                }
            }
            raw.close().await.unwrap();

            let db = open_writer(&store).await;
            let mut unscoped = reads[0].clone();
            match change {
                Change::Rewritten => {
                    unscoped["people"] = serde_json::json!(1);
                    unscoped["employers"] = serde_json::json!(1);
                }
                Change::Deleted => {
                    unscoped["companies"] = serde_json::json!(0);
                    unscoped["employees"] = serde_json::json!(0);
                }
                Change::EarlierScope => {}
            }
            assert_eq!(
                label_reads(&db, DataScope::LegacyUnscoped).await,
                unscoped,
                "{change:?}"
            );
            db.close().await.unwrap();
        }
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
        db.close().await.unwrap();

        // The default configuration runs the background migration worker.
        let db = crate::HelixDB::open_with_object_store_for_tests(DB_PATH, Arc::clone(&store))
            .await
            .expect("writer opens");
        let inner = db.inner_db();
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
    async fn a_failed_copy_leaves_a_version_5_store_that_a_retry_finishes() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        // A malformed edge row fails the copy after the node rows committed.
        let malformed = malformed_row(
            DataScope::LegacyUnscoped,
            label_hash::LegacyLabelScanPrefix::EdgeLabels,
        );
        put_legacy_row(&inner, &malformed, &[1]).await;
        assert!(matches!(
            migrate_label_identity_in_batches(&inner, 1).await,
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
        assert_eq!(progress.rows().get(), 2);
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

        // A retry resumes after the committed rows, in this open as in a later
        // one.
        inner.delete(&malformed).await.unwrap();
        let migration = migrate_label_identity(&inner).await.unwrap();
        assert!(migration.resumed);
        assert_eq!(migration.cleared, 0);
        assert_eq!(migration.copied, exact_before.len() as u64 - 2);
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

    /// A failover writer runs no migration, but it finishes the cleanup of a
    /// published migration whose writer it took over from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failover_writer_finishes_the_cleanup() {
        let (store, db, reads, exact_before) = legacy_store().await;
        migrate_label_identity(&db.inner_db()).await.unwrap();
        let epoch = db.inner_db().manifest().writer_epoch();
        db.close().await.unwrap();

        let db = open_failover(&store, epoch + 1)
            .await
            .expect("failover opens a published store");
        let inner = db.inner_db();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !present(&inner, LABEL_HASH_CLEANUP_READY).await {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the failover writer deletes the hash rows");
        let (exact, legacy) = label_rows(inner.as_ref()).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    /// A cleanup step reads rows of a bounded number of scopes, so one over
    /// many tenants with no hash row left stays short, and the next step
    /// starts at the scope it stopped before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cleanup_steps_cross_a_bounded_number_of_scopes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        let inner = db.inner_db();
        let tenants = CLEANUP_SCOPES_PER_STEP as u128 + 44;
        let batch = Batch::begin(&inner).await.unwrap();
        for tenant in 1..=tenants {
            Mutation::put(
                &batch.transaction,
                DataKey::Data {
                    scope: DataScope::Tenant(TenantId::from_u128(tenant)),
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(1)),
                }
                .to_bytes(),
                encode_properties(&[Property::string("$label", "Person")]),
            )
            .unwrap();
        }
        batch.commit(false).await.unwrap();
        let last = DataScope::Tenant(TenantId::from_u128(tenants));
        for key in [
            edge_row("KNOWS"),
            DataKey::data_prefix(last, edge_row("KNOWS")),
        ] {
            put_legacy_row(&inner, &key, &[1]).await;
        }

        let tuning = MigrationTuning::default();
        assert_eq!(
            delete_hash_label_rows_once(&inner, tuning).await.unwrap(),
            LabelHashCleanup::Advanced { deleted: 1 }
        );
        let cursor = inner
            .get(metadata(LABEL_HASH_CLEANUP_PROGRESS))
            .await
            .unwrap()
            .expect("the step recorded the scope it stopped before");
        let stopped_before =
            DataScope::Tenant(TenantId::from_u128(CLEANUP_SCOPES_PER_STEP as u128 + 1));
        assert_eq!(
            cursor,
            DataKey::data_prefix(
                stopped_before,
                label_hash::LegacyLabelScanPrefix::NodeLabels.to_bytes()
            )
        );
        assert_eq!(
            delete_hash_label_rows_once(&inner, tuning).await.unwrap(),
            LabelHashCleanup::Done { deleted: 1 }
        );
        assert!(label_rows(inner.as_ref()).await.1.is_empty());
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
