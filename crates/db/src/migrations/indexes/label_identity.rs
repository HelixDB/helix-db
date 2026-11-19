//! Background migration of hash-keyed label index rows to exact label keys.
//!
//! Before storage version 7 every label index row was keyed by the 64-bit
//! SipHash of its label, so two labels with one hash shared a bitmap. This
//! migration files every node label, global edge label, and edge-label
//! neighbor row of every data scope under its exact label (see
//! [`crate::encoding::indexes::label`]) while the writer serves, publishes
//! [`IndexStorageVersion::LABEL_IDENTITY`], and only then deletes the hash
//! rows.
//!
//! # Contract
//!
//! - A migration-authorized writer open starts it with
//!   [`start_label_identity_copy`]: one commit that moves a version-5 store to
//!   [`IndexStorageVersion::LABEL_COPY`], or a store with no hash label row,
//!   such as a new one, straight to [`IndexStorageVersion::LABEL_IDENTITY`].
//!   The open copies nothing, so it takes about as long on a store of any
//!   size. Binaries that support at most version 5
//!   refuse the store from then on: they would keep only the hash rows
//!   current, so the exact rows the copy already passed would go stale.
//! - At version 6 readers read the hash rows, which stay complete, and every
//!   graph write files each label row change under both its hash and its
//!   exact key with the same members (see
//!   [`crate::index_lifecycle::repository::LabelRowWrites`]). The exact rows
//!   lack only what was written before the copy began.
//! - [`copy_hash_label_rows_once`] copies the next batch of hash rows in a
//!   background migration step. A batch's transaction begins before it reads
//!   anything, and stages each exact row as a disjoint membership merge whose
//!   conflict tokens are the members it adds: the tokens a graph write's
//!   change to those members carries. So a graph write that removes or
//!   relabels a member a batch copies, committed after the batch began,
//!   conflicts with the batch whichever commits second, and a conflicted
//!   batch is taken again from a later snapshot. Writes that only add other
//!   members do not conflict, and replaying a batch adds the same members
//!   again.
//! - Each batch commits [`LABEL_IDENTITY_COPY_PROGRESS`] with its rows: the
//!   last row it copied or, for a row with more members than one batch
//!   copies, the last of its members copied. The next step resumes there,
//!   whichever writer of the store takes it, since every writer that opens a
//!   version-6 store files both keys. Batches bound their conflict tokens, so
//!   a label of millions of nodes is copied a range of members at a time and
//!   never holds a transaction's conflict quota.
//! - A node or edge row's label is the `$label` of its lowest live member
//!   whose label hashes to the row. A live member whose label hashes
//!   elsewhere, or whose record has no string `$label`, is a stale leftover of
//!   a label change: it is not copied. The highest live member whose label
//!   hashes to the row must have the same label, else two labels share the
//!   row's hash and the copy fails closed; so does a label too long for any
//!   exact key. These two members name the whole row: the migration relies on
//!   no two labels of a scope sharing a hash, which production stores were
//!   verified to satisfy before it shipped. A row with no such member indexes
//!   nothing and is not copied.
//! - Edge-label neighbor rows name opposite endpoints, not edges. Their labels
//!   come from the scope's exact edge label rows, which the copy fills before
//!   the scope's neighbor rows and graph writes keep current, so a label a
//!   write added since the copy passed its edge rows is found too. A neighbor
//!   row whose hash no edge label has is not copied: no graph write leaves
//!   one, so only an index already damaged holds one.
//! - Once a step finds no hash row left, it flushes and publishes
//!   [`IndexStorageVersion::LABEL_IDENTITY`] in one commit that also deletes
//!   the progress, so readers switch from hash rows to complete exact rows at
//!   once. Until its writer has seen publication, a graph transaction reads
//!   the storage marker in its own serializable snapshot, so one that read
//!   version 6 conflicts with the publishing commit rather than committing
//!   hash rows after it.
//! - Once version 7 is published nothing reads the hash rows:
//!   [`delete_hash_label_rows_once`] removes them in background migration
//!   steps while the writer serves, recording [`LABEL_HASH_CLEANUP_PROGRESS`]
//!   as it goes and [`LABEL_HASH_CLEANUP_READY`] at the end.
//! - Every writer takes the copy and cleanup steps, a failover writer, which
//!   runs no other migration, included, so a writer that takes over finishes
//!   them. They run only while the background migration worker is enabled;
//!   with [`crate::config::MigrationWorkerMode::Disabled`] the store keeps
//!   serving at version 6, reading hash rows and writing both, until a step
//!   is taken.
//!
//! # Cost
//!
//! A writer open costs one commit. The copy is linear in label rows, spread
//! over background steps of at most [`BATCH_ROWS`] rows, [`BATCH_BYTES`]
//! staged bytes, and [`BATCH_MEMBERS`] members. Each step opens one
//! [`LabelRowWalk`], which reads every scope's hash rows in key order on one
//! iterator, seeking past every other row, so a scope without label rows costs
//! a few seeks, and a batch spans scopes. The copy reads the records of the two
//! naming members of each node and edge row (and of any stale or dead members
//! before them), up to [`NAMING_CONCURRENCY`] at once, and writes every exact
//! row once. A scope's exact edge labels are read once per step at most, and
//! only when a neighbor row names a label the step has not seen. Progress is
//! logged at most every [`PROGRESS_INTERVAL`]. The cleanup walks the hash rows
//! once more after publication: each step deletes at most one batch and enters
//! at most [`CLEANUP_SCOPES_PER_STEP`] scopes, so a step over many tenants with
//! nothing left to delete stays short too.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::TryStreamExt;
use roaring::RoaringTreemap;
use slatedb::config::WriteOptions;
use slatedb::{Db, DbTransaction, IsolationLevel, KeyValue};

use crate::encoding::indexes::{
    hash_property_value, label, EdgeDirection, PropertyIndexKey, ValueHash,
};
use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::encoding::keys::{EdgePropertyByIdKey, NodePropertyKey};
use crate::encoding::property::decode_properties;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::label_hash;
use crate::encoding::v2::values::indexes::{BitmapMembershipDelta, SecondaryEqualityValue};
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

/// Hash rows one copy step reads at most. Neighbor rows dominate: one per
/// endpoint, direction, and edge label, mostly of a member or two.
const BATCH_ROWS: usize = 16_384;
/// Bytes of the keys and values one step stages at most. A step also stages
/// at most 1/[`STEP_SHARE`] of the writer's `max_unflushed_bytes`, so no single
/// commit takes a large part of the room foreground writes share.
const BATCH_BYTES: usize = 8 * 1024 * 1024;
/// The share of `max_unflushed_bytes` one step may stage: 1 in this many.
const STEP_SHARE: usize = 16;
/// Steps stop while the flush backlog, the immutable memtables and unflushed
/// WAL waiting for object storage, reaches 1/this of `max_unflushed_bytes`...
const BACKLOG_HIGH_SHARE: usize = 4;
/// ...and start again once it is below 1/this, so they do not flap at one mark.
const BACKLOG_LOW_SHARE: usize = 8;
/// How long a held-back step waits before the next check.
const YIELD_WAIT: Duration = Duration::from_millis(50);
/// A step whose durable commit takes longer than this halves the next step;
/// a quicker commit grows it back by a quarter, up to the limits above.
const SLOW_COMMIT: Duration = Duration::from_millis(500);
/// Hash rows one cleanup step deletes at most.
const CLEANUP_BATCH_ROWS: usize = 16_384;
/// Members one copy step adds: each is a conflict token the step's
/// transaction holds, and a committed transaction's tokens stay retained, in
/// a quota shared with graph writes, while any older transaction is open.
const BATCH_MEMBERS: u64 = 256 * 1024;
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

/// Rows the copy has copied, skipped, or left members out of, for its log.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LabelIdentityMigration {
    /// Hash rows copied to their exact key.
    pub(crate) copied: u64,
    /// Members copied into exact rows, counting a row copied a range at a
    /// time as it goes, so progress shows while one large row is copied.
    pub(crate) members: u64,
    /// Hash rows no live member names, or neighbor rows no edge label
    /// explains; nothing reads them once the migration is published.
    pub(crate) skipped: u64,
    /// Members left out of their copied row: live records whose label hashes
    /// to another row, or that have no string `$label`.
    pub(crate) stale_members: u64,
    /// Batches a concurrent graph write made conflict, each taken again.
    pub(crate) conflicts: u64,
}

impl LabelIdentityMigration {
    fn add(&mut self, batch: Self) {
        self.copied += batch.copied;
        self.members += batch.members;
        self.skipped += batch.skipped;
        self.stale_members += batch.stale_members;
        self.conflicts += batch.conflicts;
    }
}

/// What one background writer keeps between label steps: the pacing that
/// holds steps back while the writer's flush backlog is high, the step limits
/// conflicts and slow commits narrow, the exact edge labels of the scope the
/// copy is in, and running totals for the progress log. Nothing in it is
/// needed to resume: a fresh session, of this writer or another, picks up from
/// the stored progress.
pub(crate) struct LabelCopySession {
    max_rows: usize,
    max_members: u64,
    /// [`BATCH_BYTES`], or the writer's share of its unflushed budget if that
    /// is smaller; each copy step sets it before it stages anything.
    max_bytes: usize,
    /// The current limits of a copy step: a conflict or a slow commit halves
    /// them, so a batch a hot row's writes keep hitting shrinks to that row
    /// alone, and each quick commit grows them back by a quarter.
    rows: usize,
    members: u64,
    bytes: usize,
    cleanup_rows: usize,
    pacing: FlushPacing,
    /// Exact edge labels of one scope by the hash its neighbor rows carry.
    edge_labels: Option<(DataScope, BTreeMap<ValueHash, label::SharedLabel>)>,
    totals: LabelIdentityMigration,
    /// Hash rows the cleanup deleted.
    deleted: u64,
    started: Instant,
    /// When this session's first cleanup step began: the cleanup's rate and
    /// elapsed time count from here, not from the copy.
    cleanup_started: Option<Instant>,
    reported: Instant,
    /// Runs before the next batch commits, so a test can write between a
    /// batch's reads and its commit.
    #[cfg(test)]
    before_commit: Option<tests::CommitPause>,
}

impl LabelCopySession {
    pub(crate) fn new() -> Self {
        Self::with_limits(BATCH_ROWS, BATCH_MEMBERS)
    }

    fn with_limits(rows: usize, members: u64) -> Self {
        assert!(rows > 0 && members > 0, "a copy step copies something");
        Self {
            max_rows: rows,
            max_members: members,
            max_bytes: BATCH_BYTES,
            rows,
            members,
            bytes: BATCH_BYTES,
            cleanup_rows: CLEANUP_BATCH_ROWS,
            pacing: FlushPacing::default(),
            edge_labels: None,
            totals: LabelIdentityMigration::default(),
            deleted: 0,
            started: Instant::now(),
            cleanup_started: None,
            reported: Instant::now(),
            #[cfg(test)]
            before_commit: None,
        }
    }

    /// A session whose cleanup deletes at most `rows` hash rows a step.
    #[cfg(test)]
    pub(crate) fn with_cleanup_rows(rows: usize) -> Self {
        assert!(rows > 0, "a cleanup step deletes something");
        Self {
            cleanup_rows: rows,
            ..Self::new()
        }
    }

    /// Whether a paced background step may run now: the label steps and the
    /// datetime string conversion, which the worker admits together, so they
    /// share one pacing. While the writer's flush backlog is high it waits
    /// [`YIELD_WAIT`] and declines, so the caller takes the step later and
    /// foreground writes keep the room meanwhile.
    pub(crate) async fn admit_step(&mut self, db: &Db) -> Result<bool> {
        let unflushed = db.unflushed_bytes()?;
        if self.pacing.admits(unflushed) {
            return Ok(true);
        }
        let waited = Instant::now();
        tokio::time::sleep(YIELD_WAIT).await;
        self.pacing.yielded += waited.elapsed();
        Ok(false)
    }

    fn conflicted(&mut self) {
        self.totals.conflicts += 1;
        self.narrow();
    }

    fn narrow(&mut self) {
        self.rows = (self.rows / 2).max(1);
        self.members = (self.members / 2).max(1);
        self.bytes = (self.bytes / 2).max(1);
    }

    fn committed(&mut self, batch: LabelIdentityMigration, commit: Duration) {
        self.totals.add(batch);
        if commit > SLOW_COMMIT {
            self.narrow();
        } else {
            self.rows = self
                .rows
                .saturating_add(self.rows / 4 + 1)
                .min(self.max_rows);
            self.members = self
                .members
                .saturating_add(self.members / 4 + 1)
                .min(self.max_members);
            self.bytes = self
                .bytes
                .saturating_add(self.bytes / 4 + 1)
                .min(self.max_bytes);
        }
        if self.reported.elapsed() < PROGRESS_INTERVAL {
            return;
        }
        self.reported = Instant::now();
        let elapsed = self.started.elapsed();
        tracing::info!(
            copied = self.totals.copied,
            members = self.totals.members,
            skipped = self.totals.skipped,
            stale_members = self.totals.stale_members,
            conflicts = self.totals.conflicts,
            rows_per_second = per_second(self.totals.copied + self.totals.skipped, elapsed),
            step_rows = self.rows,
            step_bytes = self.bytes,
            last_commit_ms = u64::try_from(commit.as_millis()).unwrap_or(u64::MAX),
            yields = self.pacing.yields,
            yielded_ms = u64::try_from(self.pacing.yielded.as_millis()).unwrap_or(u64::MAX),
            flush_backlog_bytes = self.pacing.backlog,
            elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            "label identity copy in progress"
        );
    }

    fn cleaned(&mut self, deleted: u64) {
        self.deleted += deleted;
        if self.reported.elapsed() < PROGRESS_INTERVAL {
            return;
        }
        self.reported = Instant::now();
        let elapsed = self
            .cleanup_started
            .get_or_insert_with(Instant::now)
            .elapsed();
        tracing::info!(
            deleted = self.deleted,
            rows_per_second = per_second(self.deleted, elapsed),
            yields = self.pacing.yields,
            yielded_ms = u64::try_from(self.pacing.yielded.as_millis()).unwrap_or(u64::MAX),
            flush_backlog_bytes = self.pacing.backlog,
            elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            "label hash cleanup in progress"
        );
    }

    /// `scope`'s exact edge labels, starting afresh in a new scope.
    fn edge_labels(&mut self, scope: DataScope) -> &mut BTreeMap<ValueHash, label::SharedLabel> {
        if self
            .edge_labels
            .as_ref()
            .is_none_or(|(labels_scope, _)| *labels_scope != scope)
        {
            self.edge_labels = Some((scope, BTreeMap::new()));
        }
        let (_, labels) = self
            .edge_labels
            .as_mut()
            .expect("the scope's edge labels were just set");
        labels
    }

    /// The totals of every batch this session committed.
    #[cfg(test)]
    pub(crate) const fn totals(&self) -> LabelIdentityMigration {
        self.totals
    }
}

fn per_second(rows: u64, elapsed: Duration) -> u64 {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 {
        return 0;
    }
    (rows as f64 / seconds) as u64
}

/// Holds paced background steps (the label steps and the datetime string
/// conversion) back while the writer's flush backlog is high.
///
/// Every write, foreground or background, waits once the writer's unflushed
/// bytes reach `max_unflushed_bytes` (SlateDB backpressure). The backlog that
/// fills that room is the immutable memtables and the WAL waiting for object
/// storage; the active memtable fills and freezes in normal operation. Paced
/// steps stop once the backlog reaches 1/[`BACKLOG_HIGH_SHARE`] of the limit
/// and start again below 1/[`BACKLOG_LOW_SHARE`], so they only use flush
/// capacity foreground writes leave spare.
#[derive(Debug, Default)]
struct FlushPacing {
    /// Whether steps are waiting for the backlog to fall below the low mark.
    yielding: bool,
    /// How often steps started waiting, and for how long in all.
    yields: u64,
    yielded: Duration,
    /// The flush backlog last observed, in bytes.
    backlog: usize,
}

impl FlushPacing {
    /// Whether a step may run under `unflushed`.
    fn admits(&mut self, unflushed: slatedb::UnflushedBytes) -> bool {
        self.backlog = unflushed
            .immutable_memtable_bytes
            .saturating_add(unflushed.wal_bytes);
        let was_yielding = self.yielding;
        self.yielding = if was_yielding {
            self.backlog > unflushed.max_unflushed_bytes / BACKLOG_LOW_SHARE
        } else {
            self.backlog >= unflushed.max_unflushed_bytes / BACKLOG_HIGH_SHARE
        };
        if self.yielding && !was_yielding {
            self.yields += 1;
        }
        !self.yielding
    }
}

/// What one copy step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelCopyStep {
    /// The store is before version 6, or no V2 writer has opened it: no
    /// writer has started the copy.
    NotStarted,
    /// One batch committed; a later step copies the rest.
    Copied,
    /// A concurrent graph write conflicted with the batch, which a later step
    /// takes again with narrower limits.
    Conflicted,
    /// No hash row was left, and this step published exact label rows.
    Published,
    /// An earlier step published exact label rows.
    AlreadyPublished,
}

impl LabelCopyStep {
    /// Whether a later step has more to copy.
    pub(crate) const fn advanced(self) -> bool {
        matches!(self, Self::Copied | Self::Conflicted)
    }
}

/// Starts the label identity copy of a version-5 store: moves it to
/// [`IndexStorageVersion::LABEL_COPY`] in one commit, after which every graph
/// write files label rows under both keys. A store with no hash label row,
/// such as a new one, has nothing to copy and moves straight to
/// [`IndexStorageVersion::LABEL_IDENTITY`]: it serves no write yet. A store
/// already copying or published is left alone.
///
/// Finding the first hash row reads one row, or a few seeks per scope before
/// the first scope that has one. The commit also deletes any copy progress,
/// which only an unpublished copy of a binary before this background copy
/// left: nothing reads it at version 5.
pub(crate) async fn start_label_identity_copy(db: &Db) -> Result<()> {
    let version = storage_version(db).await?;
    if version >= IndexStorageVersion::LABEL_COPY {
        return Ok(());
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
    let copy = hash_rows(db, None, None).await?.next().await?.is_some();
    let target = if copy {
        IndexStorageVersion::LABEL_COPY
    } else {
        IndexStorageVersion::LABEL_IDENTITY
    };
    advance_storage_version(
        db,
        IndexStorageVersion::OPERATION_QUEUES..target,
        target,
        |transaction| {
            Ok(transaction.delete(super::super::scoped_metadata_key(
                DataScope::LegacyUnscoped,
                LABEL_IDENTITY_COPY_PROGRESS,
            ))?)
        },
    )
    .await?;
    if copy {
        tracing::info!(
            "label identity copy started: graph writes file label rows under both keys until \
             the background copy publishes"
        );
    } else {
        tracing::info!("label identity published at once: the store has no hash label row");
    }
    Ok(())
}

/// Copies the next batch of hash label rows to their exact keys, or
/// publishes [`IndexStorageVersion::LABEL_IDENTITY`] once none is left; see
/// the module contract.
pub(crate) async fn copy_hash_label_rows_once(
    writer: &crate::HelixWriter,
    session: &mut LabelCopySession,
) -> Result<LabelCopyStep> {
    let db = writer.db();
    // A store no V2 writer has opened is before the copy too.
    let Some(version) = crate::migrations::startup::stored_storage_version(db).await? else {
        return Ok(LabelCopyStep::NotStarted);
    };
    if version >= IndexStorageVersion::LABEL_IDENTITY {
        writer.observe_storage_version(Some(version));
        return Ok(LabelCopyStep::AlreadyPublished);
    }
    if version < IndexStorageVersion::LABEL_COPY {
        return Ok(LabelCopyStep::NotStarted);
    }
    let progress_key =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_IDENTITY_COPY_PROGRESS);
    // Every read of the batch follows the snapshot its transaction begins at,
    // so a graph write that changes a copied member after it conflicts with it.
    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    let recorded = transaction
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
    // No step stages more than its share of the writer's unflushed budget, so
    // the step limit, and the progress log, never exceed it.
    session.max_bytes = BATCH_BYTES
        .min(db.unflushed_bytes()?.max_unflushed_bytes / STEP_SHARE)
        .max(1);
    session.bytes = session.bytes.min(session.max_bytes);
    let mut batch = CopyBatch::new(session);
    // The scope whose exact edge labels this step read.
    let mut reloaded = None;
    let after = match &recorded {
        None => None,
        Some(
            label_migration::LabelCopyProgressValue::Rows { row }
            | label_migration::LabelCopyProgressValue::Members { row, .. },
        ) => Some(row.clone()),
    };
    // A row an earlier step copied part of comes first, from its next member.
    let partial = match recorded {
        Some(label_migration::LabelCopyProgressValue::Members { row, through }) => {
            db.get_key_value(&row).await?.map(|row| (row, through))
        }
        Some(label_migration::LabelCopyProgressValue::Rows { .. }) | None => None,
    };
    let full = match partial {
        Some((row, through)) => {
            let (scope, family) = locate(
                &label_hash::LegacyLabelScanPrefix::ALL
                    .map(label_hash::LegacyLabelScanPrefix::to_bytes),
                &row.key,
            )?;
            let named = name_row(
                db,
                scope,
                label_hash::LegacyLabelScanPrefix::ALL[family],
                row,
            )
            .await?;
            stage_row(db, session, &mut batch, &mut reloaded, named, Some(through)).await?
        }
        None => false,
    };
    let mut exhausted = false;
    if !full {
        let walk = hash_rows(db, after.as_ref(), None).await?;
        // Naming reads overlap, but rows are staged in key order, so a batch's
        // progress is the last row it holds.
        let named = futures::stream::try_unfold(walk, |mut walk| async move {
            Ok::<_, HelixDbError>(walk.next().await?.map(|row| (row, walk)))
        })
        .map_ok(|(scope, family, row)| name_row(db, scope, family, row))
        .try_buffered(NAMING_CONCURRENCY);
        futures::pin_mut!(named);
        loop {
            let Some(row) = named.try_next().await? else {
                exhausted = true;
                break;
            };
            if stage_row(db, session, &mut batch, &mut reloaded, row, None).await? {
                break;
            }
        }
    }

    let CopyBatch {
        rows,
        progress,
        totals,
        ..
    } = batch;
    let Some(progress) = progress else {
        // Nothing after the recorded progress: every hash row is copied.
        transaction.rollback();
        return publish(writer, session).await;
    };
    if !rows.is_empty() {
        let mut merges = Mutation::merge_batch(&transaction, rows.len())?;
        for (key, members) in rows {
            merges.bitmap(
                crate::transaction::merges::Key::Encoded(&key),
                &BitmapMembershipDelta::from_additions(members),
            )?;
        }
        merges.stage()?;
    }
    Mutation::put(&transaction, &progress_key, progress.encode())?;
    #[cfg(test)]
    tests::pause(session.before_commit.take()).await;
    // A durable commit keeps at most one step of the copy waiting in the WAL,
    // so foreground writes never queue behind a backlog the copy built.
    let commit_started = Instant::now();
    let committed = transaction
        .commit_with_options(&WriteOptions {
            await_durable: true,
            ..WriteOptions::default()
        })
        .await;
    match committed {
        Ok(_) => {}
        Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
            session.conflicted();
            return Ok(LabelCopyStep::Conflicted);
        }
        Err(error) => return Err(error.into()),
    }
    session.committed(totals, commit_started.elapsed());
    if exhausted {
        return publish(writer, session).await;
    }
    Ok(LabelCopyStep::Copied)
}

/// Publishes the finished copy: flushes its batches, then moves the store to
/// [`IndexStorageVersion::LABEL_IDENTITY`] and deletes the progress in one
/// durable commit.
async fn publish(writer: &crate::HelixWriter, session: &LabelCopySession) -> Result<LabelCopyStep> {
    let db = writer.db();
    db.flush().await?;
    let progress_key =
        super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_IDENTITY_COPY_PROGRESS);
    advance_storage_version(
        db,
        IndexStorageVersion::LABEL_COPY..IndexStorageVersion::LABEL_IDENTITY,
        IndexStorageVersion::LABEL_IDENTITY,
        |transaction| Ok(transaction.delete(&progress_key)?),
    )
    .await?;
    writer.observe_storage_version(Some(IndexStorageVersion::LABEL_IDENTITY));
    let totals = session.totals;
    tracing::info!(
        copied = totals.copied,
        members = totals.members,
        skipped = totals.skipped,
        stale_members = totals.stale_members,
        conflicts = totals.conflicts,
        elapsed_ms = u64::try_from(session.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "label identity copy published exact label rows"
    );
    Ok(LabelCopyStep::Published)
}

/// Exact label rows one copy step stages, with the progress to commit beside
/// them.
struct CopyBatch {
    rows: Vec<(Bytes, RoaringTreemap)>,
    /// Hash rows the batch passed, copied or skipped.
    passed: usize,
    bytes: usize,
    members: u64,
    max_rows: usize,
    max_members: u64,
    max_bytes: usize,
    /// The last row passed, or the last member copied of a row too large for
    /// one batch; `None` until the batch passes a row.
    progress: Option<label_migration::LabelCopyProgressValue>,
    totals: LabelIdentityMigration,
}

impl CopyBatch {
    fn new(session: &LabelCopySession) -> Self {
        Self {
            rows: Vec::new(),
            passed: 0,
            bytes: 0,
            members: 0,
            max_rows: session.rows,
            max_members: session.members,
            max_bytes: session.bytes,
            progress: None,
            totals: LabelIdentityMigration::default(),
        }
    }

    /// Passes a hash row the copy leaves out; whether the batch is full.
    fn skip(&mut self, row: Bytes) -> bool {
        self.totals.skipped += 1;
        self.pass(row)
    }

    fn pass(&mut self, row: Bytes) -> bool {
        self.progress = Some(label_migration::LabelCopyProgressValue::rows(row));
        self.passed += 1;
        self.passed >= self.max_rows
            || self.bytes >= self.max_bytes
            || self.members >= self.max_members
    }

    /// Offers hash row `row`'s `members`, after any it copied before, to its
    /// exact key `exact`. Takes them all if they fit; else, in a batch that
    /// holds no member yet, the first of them that do, and stops there;
    /// else stops before the row. Returns whether the batch is full.
    fn copy(&mut self, row: Bytes, exact: Bytes, mut members: RoaringTreemap, stale: u64) -> bool {
        let count = members.len();
        if self.members + count <= self.max_members {
            self.totals.copied += 1;
            self.totals.stale_members += stale;
            self.totals.members += count;
            if !members.is_empty() {
                self.members += count;
                self.bytes = self
                    .bytes
                    .saturating_add(exact.len() + members.serialized_size());
                self.rows.push((exact, members));
            }
            return self.pass(row);
        }
        if self.members > 0 {
            return true;
        }
        let through = members
            .select(self.max_members - 1)
            .expect("a row larger than the batch has a member at its limit");
        members.remove_range(through + 1..);
        self.totals.members += members.len();
        self.members = self.max_members;
        self.rows.push((exact, members));
        self.progress = Some(label_migration::LabelCopyProgressValue::members(
            row, through,
        ));
        true
    }
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

/// A hash row with its members, parsed and named.
struct NamedRow {
    scope: DataScope,
    row: KeyValue,
    members: RoaringTreemap,
    parsed: ParsedRow,
}

/// Parses `scope`'s hash row `row` of `family` and names it from its members'
/// records.
async fn name_row(
    db: &Db,
    scope: DataScope,
    family: label_hash::LegacyLabelScanPrefix,
    row: KeyValue,
) -> Result<NamedRow> {
    let Some(logical) = scope.strip_key(&row.key) else {
        return Err(HelixDbError::InvariantViolation(
            "label identity walk returned a key outside its scope".to_string(),
        ));
    };
    let members = SecondaryEqualityValue::decode(&row.value)?.into_ids();
    let parsed = match family {
        label_hash::LegacyLabelScanPrefix::NodeLabels => {
            let PropertyIndexKey::Equality(key) = PropertyIndexKey::parse_from_slice(logical)?
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
    Ok(NamedRow {
        scope,
        row,
        members,
        parsed,
    })
}

/// Stages `named` into `batch`, its members after `through` only when given;
/// returns whether the batch is full.
///
/// A neighbor row whose label `session` does not hold reads the scope's exact
/// edge labels, at most once a step (`reloaded`): an edge row this or an
/// earlier step copied, or a graph write since, files every label there.
async fn stage_row(
    db: &Db,
    session: &mut LabelCopySession,
    batch: &mut CopyBatch,
    reloaded: &mut Option<DataScope>,
    named: NamedRow,
    through: Option<u64>,
) -> Result<bool> {
    let NamedRow {
        scope,
        row,
        mut members,
        parsed,
    } = named;
    let (exact, stale) = match parsed {
        ParsedRow::Node(None) | ParsedRow::Edge { label: None, .. } => {
            return Ok(batch.skip(row.key));
        }
        ParsedRow::Node(Some(RowLabel { label, stale })) => {
            let exact = ExactRows::Node.key(scope, label.identity());
            members -= &stale;
            (exact, stale.len())
        }
        ParsedRow::Edge {
            hash,
            label: Some(RowLabel { label, stale }),
        } => {
            let exact = ExactRows::Edge.key(scope, label.identity());
            record_edge_label(session.edge_labels(scope), scope, hash, label)?;
            members -= &stale;
            (exact, stale.len())
        }
        ParsedRow::Neighbor(key) => {
            let mut label = session.edge_labels(scope).get(key.label_hash()).cloned();
            if label.is_none() && *reloaded != Some(scope) {
                *reloaded = Some(scope);
                let labels = session.edge_labels(scope);
                for (hash, copied) in copied_edge_labels(db, scope).await? {
                    record_edge_label(labels, scope, hash, copied)?;
                }
                label = labels.get(key.label_hash()).cloned();
            }
            let Some(label) = label else {
                return Ok(batch.skip(row.key));
            };
            (
                ExactRows::Neighbor(key.direction(), key.node_id()).key(scope, label.identity()),
                0,
            )
        }
    };
    let Some(through) = through else {
        return Ok(batch.copy(row.key, exact, members, stale));
    };
    members.remove_range(..=through);
    Ok(batch.copy(row.key, exact, members, stale))
}

/// Records that `scope`'s edge label `label` hashes to `hash`, failing closed
/// on another label already there: two labels sharing a hash, which the copy
/// cannot tell apart in neighbor rows.
fn record_edge_label(
    labels: &mut BTreeMap<ValueHash, label::SharedLabel>,
    scope: DataScope,
    hash: ValueHash,
    label: label::SharedLabel,
) -> Result<()> {
    let recorded = labels.entry(hash).or_insert_with(|| label.clone());
    if *recorded != label {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "{scope:?} edge labels {:?} and {:?} share one label hash; neither's \
                 neighbor rows can be copied to their exact keys",
                recorded.identity().label(),
                label.identity().label()
            ),
        });
    }
    Ok(())
}

/// The exact edge labels `scope`'s exact edge rows hold, by the hash their
/// neighbor rows use.
async fn copied_edge_labels(
    db: &Db,
    scope: DataScope,
) -> Result<Vec<(ValueHash, label::SharedLabel)>> {
    let mut rows = db
        .scan_prefix(
            DataKey::data_prefix(scope, label::LabelScanPrefix::EdgeLabels.to_bytes()),
            ..,
        )
        .await?;
    let mut labels = Vec::new();
    while let Some(row) = rows.next().await? {
        let DataKey::Data {
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(key)),
            ..
        } = DataKey::parse_from_slice(scope, &row.key)?
        else {
            unreachable!("the edge label prefix holds edge label rows");
        };
        labels.push((
            *label_hash::LegacyEdgeLabelKey::for_label(key.identity().label()).label_hash(),
            label::SharedLabel::new(key.identity()),
        ));
    }
    Ok(labels)
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
        let end = Bound::Excluded(Bytes::from_static(DataScope::PHYSICAL_KEYS_END));
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
            let (row_scope, _) = DataScope::split_physical_key(&row.key).map_err(|error| {
                HelixDbError::IndexCatalogCorruption(format!(
                    "label row walk encountered a key outside every data scope: {error}"
                ))
            })?;
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
    let (scope, logical) =
        DataScope::split_physical_key(key).map_err(|error| HelixDbError::MigrationRequired {
            reason: format!("label identity progress names no data key: {error}"),
        })?;
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

/// A walk of every exact label row after `after`, or of every one.
#[cfg(test)]
async fn exact_rows(
    db: &Db,
    after: Option<&Bytes>,
) -> Result<LabelRowWalk<label::LabelScanPrefix>> {
    LabelRowWalk::open(
        db,
        label::LabelScanPrefix::ALL,
        label::LabelScanPrefix::to_bytes,
        after,
        None,
    )
    .await
}

/// Whether [`LABEL_HASH_CLEANUP_READY`] is set; a malformed marker fails
/// closed.
async fn cleanup_ready(db: &Db) -> Result<bool> {
    let Some(marker) = db
        .get(super::super::scoped_metadata_key(
            DataScope::LegacyUnscoped,
            LABEL_HASH_CLEANUP_READY,
        ))
        .await?
    else {
        return Ok(false);
    };
    label_migration::LabelCleanupReadyValue::decode(&marker)
        .map(|_| true)
        .map_err(|error| HelixDbError::MigrationRequired {
            reason: format!("label hash cleanup marker is malformed: {error}"),
        })
}

/// The label naming one node or edge hash row, and the live members whose
/// labels hash to another row or who carry no label.
struct RowLabel {
    label: label::SharedLabel,
    stale: RoaringTreemap,
}

/// What one member's record says about its label.
enum MemberLabel {
    /// The record is gone: a dead member, which reads skip and the copy keeps.
    Gone,
    /// The record has no string `$label`, so the member carries no label: a
    /// stale leftover, as a member whose label hashes to another row is.
    Unlabeled,
    /// The record's `$label`.
    Labeled(String),
}

/// What the record of member `id` says about its label. `record` names a
/// member's property record.
async fn member_label(
    db: &Db,
    scope: DataScope,
    record: &impl Fn(u64) -> DataKeyKind<'static>,
    id: u64,
) -> Result<MemberLabel> {
    let Some(stored) = db
        .get(
            DataKey::Data {
                scope,
                kind: record(id),
            }
            .to_bytes(),
        )
        .await?
    else {
        return Ok(MemberLabel::Gone);
    };
    Ok(decode_properties(&stored)?
        .into_iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str().map(str::to_owned))
        .map_or(MemberLabel::Unlabeled, MemberLabel::Labeled))
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
        match member_label(db, scope, &record, member).await? {
            MemberLabel::Gone => {}
            MemberLabel::Labeled(label) if hashes_to_row(&label) => {
                lowest = Some((member, label));
                break;
            }
            MemberLabel::Unlabeled | MemberLabel::Labeled(_) => {
                stale.insert(member);
            }
        }
    }
    let Some((lowest, label)) = lowest else {
        return Ok(None);
    };
    for member in members.iter().rev().take_while(|member| *member > lowest) {
        match member_label(db, scope, &record, member).await? {
            MemberLabel::Gone => {}
            MemberLabel::Labeled(highest) if hashes_to_row(&highest) => {
                if highest != label {
                    return Err(HelixDbError::MigrationRequired {
                        reason: format!(
                            "{scope:?} labels {label:?} and {highest:?} share one label hash \
                             row; neither can be copied to its exact key"
                        ),
                    });
                }
                break;
            }
            MemberLabel::Unlabeled | MemberLabel::Labeled(_) => {
                stale.insert(member);
            }
        }
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
/// is published: at most `session`'s cleanup rows, and as many bytes as a copy
/// step may stage, from at most [`CLEANUP_SCOPES_PER_STEP`] scopes after the
/// one it starts in. It records [`LABEL_HASH_CLEANUP_READY`] with the last.
///
/// Nothing reads those rows by then, and graph writes never touch them, so it
/// runs beside the serving writer, of any open intent. Each step commits
/// [`LABEL_HASH_CLEANUP_PROGRESS`] with its deletes, durably like a copy step:
/// the last row it deleted, or the scope it stopped before. The next step, of
/// this open or a later one, starts there rather than walking deleted rows or
/// empty scopes again.
pub(crate) async fn delete_hash_label_rows_once(
    db: &Db,
    session: &mut LabelCopySession,
) -> Result<LabelHashCleanup> {
    if cleanup_ready(db).await? {
        return Ok(LabelHashCleanup::AlreadyDone);
    }
    if crate::migrations::startup::stored_storage_version(db)
        .await?
        .is_none_or(|version| version < IndexStorageVersion::LABEL_IDENTITY)
    {
        return Ok(LabelHashCleanup::Unpublished);
    }
    let started = *session.cleanup_started.get_or_insert_with(Instant::now);
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
    let max_bytes = BATCH_BYTES
        .min(db.unflushed_bytes()?.max_unflushed_bytes / STEP_SHARE)
        .max(1);
    let mut walk = hash_rows(db, after.as_ref(), Some(CLEANUP_SCOPES_PER_STEP)).await?;
    let mut batch = Batch::begin(db).await?;
    let mut deleted = 0u64;
    while let Some((_, _, row)) = walk.next().await? {
        batch.transaction.delete(&row.key)?;
        deleted += 1;
        if batch.staged(row.key.len(), session.cleanup_rows, max_bytes) {
            Mutation::put(
                &batch.transaction,
                &progress_key,
                label_migration::LabelCleanupProgressValue::new(row.key).encode(),
            )?;
            batch.commit(true).await?;
            session.cleaned(deleted);
            return Ok(LabelHashCleanup::Advanced { deleted });
        }
    }
    let Some(stopped_before) = walk.stopped_before().cloned() else {
        batch.transaction.delete(&progress_key)?;
        Mutation::put(
            &batch.transaction,
            super::super::scoped_metadata_key(DataScope::LegacyUnscoped, LABEL_HASH_CLEANUP_READY),
            label_migration::LabelCleanupReadyValue.encode(),
        )?;
        batch.commit(true).await?;
        session.cleaned(deleted);
        tracing::info!(
            deleted = session.deleted,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "label identity cleanup deleted the last retired hash label rows"
        );
        return Ok(LabelHashCleanup::Done { deleted });
    };
    Mutation::put(
        &batch.transaction,
        &progress_key,
        label_migration::LabelCleanupProgressValue::new(stopped_before).encode(),
    )?;
    batch.commit(true).await?;
    session.cleaned(deleted);
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
    use crate::encoding::v2::values::indexes::SecondaryEqualityBitmapValue;
    use crate::index_lifecycle::IndexV2MetadataValue;

    let mut walk = exact_rows(db, None).await?;
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

/// How far a test store's label identity migration has gone, for tests of
/// what reads label rows at each stage a writer may serve.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelStage {
    /// Version 5, before the copy: label rows are hash-keyed only.
    BeforeCopy,
    /// Version 6, one member copied: reads use the hash rows, every graph
    /// write files both keys, and the exact rows lack what was written
    /// before the copy began, but for that one member.
    Copying,
    /// Version 7: copied, published, and the hash rows cleaned up.
    Published,
}

#[cfg(test)]
impl LabelStage {
    pub(crate) const ALL: [Self; 3] = [Self::BeforeCopy, Self::Copying, Self::Published];

    /// Rewinds `db` to version 5 with every label row under its hash key
    /// ([`make_legacy_label_fixture`]), so rows written next are filed as a
    /// binary before label identity files them. The caller must quiesce
    /// every write.
    pub(crate) async fn rewind(db: &Db) {
        make_legacy_label_fixture(db)
            .await
            .expect("the store rewinds before label identity");
    }

    /// Moves `db`, rewound by [`Self::rewind`] and holding hash rows, to
    /// this stage through the migration's own steps.
    pub(crate) async fn reach(self, db: &Db) {
        let version = match self {
            Self::BeforeCopy => IndexStorageVersion::OPERATION_QUEUES,
            Self::Copying => {
                let writer = crate::HelixWriter::new(std::sync::Arc::new(db.clone()), 4, 2);
                start_label_identity_copy(db)
                    .await
                    .expect("the copy starts");
                // One member a step: a row of two members is copied in part.
                let step =
                    copy_hash_label_rows_once(&writer, &mut LabelCopySession::with_limits(1, 1))
                        .await
                        .expect("one copy step runs");
                assert_eq!(
                    step,
                    LabelCopyStep::Copied,
                    "the store has hash rows to copy"
                );
                IndexStorageVersion::LABEL_COPY
            }
            Self::Published => {
                Self::publish(db).await;
                IndexStorageVersion::LABEL_IDENTITY
            }
        };
        assert_eq!(storage_version(db).await.unwrap(), version);
    }

    /// Takes `db`, rewound by [`Self::rewind`] or moved to any stage since,
    /// to [`Self::Published`]: starts the copy unless it runs, copies what
    /// is left, publishes, and deletes every hash row.
    pub(crate) async fn publish(db: &Db) {
        let writer = crate::HelixWriter::new(std::sync::Arc::new(db.clone()), 4, 2);
        start_label_identity_copy(db)
            .await
            .expect("the copy starts");
        let mut session = LabelCopySession::new();
        while copy_hash_label_rows_once(&writer, &mut session)
            .await
            .expect("a copy step runs")
            .advanced()
        {}
        while delete_hash_label_rows_once(db, &mut session)
            .await
            .expect("a cleanup step runs")
            .advanced()
        {}
        assert!(
            hash_rows(db, None, None)
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_none(),
            "the cleanup deletes every hash row"
        );
        assert_eq!(
            storage_version(db).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
    }
}

/// Where a test build's store stands in the label identity migration while
/// the build runs.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelTimeline {
    /// The store stays at one stage for the whole build.
    Fixed(LabelStage),
    /// The store is [`LabelStage::Copying`] when the build starts, and the
    /// copy publishes and cleans up between two of its steps
    /// ([`Self::between_steps`]): steps before read the hash rows, steps
    /// after the exact ones.
    PublishedMidBuild,
}

#[cfg(test)]
impl LabelTimeline {
    pub(crate) const ALL: [Self; 4] = [
        Self::Fixed(LabelStage::BeforeCopy),
        Self::Fixed(LabelStage::Copying),
        Self::Fixed(LabelStage::Published),
        Self::PublishedMidBuild,
    ];

    /// The stage the store reaches before the build starts.
    pub(crate) fn start(self) -> LabelStage {
        match self {
            Self::Fixed(stage) => stage,
            Self::PublishedMidBuild => LabelStage::Copying,
        }
    }

    /// Moves `db` as this timeline does between a build's steps.
    pub(crate) async fn between_steps(self, db: &Db) {
        match self {
            Self::Fixed(_) => {}
            Self::PublishedMidBuild => LabelStage::publish(db).await,
        }
    }
}

#[cfg(test)]
#[path = "label_identity_tests.rs"]
mod tests;
