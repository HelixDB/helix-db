//! Durable queue discovery for startup accounting and reconciliation.
//!
//! Startup reads every queue key of every data scope (one key per generation
//! with outstanding work in the map layout, one row per operation in the row
//! layout) in one forward pass, charges each queue as the scan yields it, and
//! drops it before the next, so open holds one queue at a time rather than a
//! scope's whole backlog. Each queue is validated against the canonical
//! record that still names its generation; a scope's records are read only
//! once it yields a queue. Tenant scopes have no registry, so a scan from
//! past each tenant's keyspace finds the next: a tenant costs at most two
//! seeks besides its own queue rows, which is linear in the number of tenant
//! scopes that contain any rows.

use std::collections::HashMap;

use bytes::Bytes;
use slatedb::{Db, DbIterator, DbReadOps, DbTransaction, IsolationLevel};

use crate::config::QueueLayout;
use crate::encoding::v2::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
#[cfg(test)]
use crate::encoding::v2::values::indexes::operation_queue::OperationQueue;
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexId, IndexRecordV2, ValidatedDynamicIndexDefinition};

use super::backlog::IndexOperationBacklog;
use super::storage::{
    discovery_range, stage_queue_delete, DiscoveredQueue, QueueDiscovery, QueueStore,
};
use super::QueueTarget;

/// Reads and validates one generation queue through `reader`.
///
/// Absence is the empty queue; a present value must decode completely and
/// belong to `family`, otherwise the read fails closed.
#[cfg(test)]
pub(crate) async fn read_queue(
    reader: &(impl DbReadOps + Sync),
    target: QueueTarget,
    family: QueueFamily,
) -> Result<Option<OperationQueue>> {
    let Some(value) = reader.get(target.key()).await? else {
        return Ok(None);
    };
    let queue = OperationQueue::decode(&value)?;
    if queue.family() != family {
        return Err(HelixDbError::IndexCatalogCorruption(format!(
            "operation queue for index {} generation {} holds another family",
            target.index_id.get(),
            target.generation.get()
        )));
    }
    Ok(Some(queue))
}

/// Discovers every data scope with at least one stored row.
#[cfg(any(test, feature = "async-index-benchmark"))]
pub(crate) async fn discover_scopes(reader: &(impl DbReadOps + Sync)) -> Result<Vec<DataScope>> {
    use std::ops::Bound;

    let mut scopes = vec![DataScope::LegacyUnscoped];
    let mut next = Some(TenantId::from_u128(0));
    while let Some(tenant) = next {
        let mut start = Vec::new();
        DataScope::Tenant(tenant).encode_key_prefix(&mut start);
        let mut rows = reader
            .scan((
                Bound::Included(Bytes::from(start)),
                Bound::Excluded(Bytes::from_static(&[TENANT_KEY_PREFIX + 1])),
            ))
            .await?;
        let Some(row) = rows.next().await? else {
            break;
        };
        let Some((found, _)) = DataScope::strip_tenant_envelope(&row.key) else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "tenant discovery encountered an invalid envelope".to_string(),
            ));
        };
        if found < tenant {
            return Err(HelixDbError::InvariantViolation(
                "tenant discovery did not advance".to_string(),
            ));
        }
        scopes.push(DataScope::Tenant(found));
        next = found.as_u128().checked_add(1).map(TenantId::from_u128);
    }
    Ok(scopes)
}

/// Loads every durable outstanding operation into the ledger.
///
/// Writers call this after the catalog is loaded and before accepting graph
/// writes, so admission starts from exact retained usage. Every queue key in
/// every scope is loaded, including queues of retired generations and
/// dropped indexes, so the publisher can discard work that no canonical
/// record still owns. Queues are charged in key order, scope by scope, and
/// each is dropped once charged: peak memory is one queue's stored value and
/// framing (see [`super::storage::QueueDiscovery`]) plus the ledger, never
/// the whole backlog. Each operation's payload is validated in place, never
/// decoded.
///
/// Only the ledger is loaded: publication starts with no retained queue,
/// schedule, or held entity, and each target's first attempt reads and
/// groups its queue itself (see [`super::storage::StoredQueue`]).
///
/// # One scan per scope
///
/// Each scope's queue range ([`discovery_range`]) is read by one scan, which
/// reads every queue once: a queue the catalog does not own (an orphan of a
/// recreated index or a corrupt record) is charged for the publisher to
/// discard, a queue of the other layout fails the load with
/// [`HelixDbError::Config`], and any other key is corruption. A tombstoned
/// queue (see "Delete on drain" on [`QueueStore::stage_acknowledge`]) is not
/// merged, but the scan still steps over its shadowed history in every
/// sorted run that holds it. Reading known queue keys by point get would not
/// save that while the rest of the range is still checked, since a table's
/// index records only each block's first key, and it would read a live
/// queue twice.
///
/// # Drained queues
///
/// The load records each map-layout queue key whose stored history it
/// resolved to the empty queue: it reads as absent, yet every read of it
/// merges that history again. The scan opens no transaction, as one held
/// across it would retain every commit made meanwhile in the transaction
/// manager and lengthen each of their conflict checks;
/// [`LoadedBacklog::tombstone_drained`] begins one only once the scan found
/// such a key.
pub(crate) async fn load_backlog(
    reader: &(impl DbReadOps + Sync),
    store: &QueueStore,
    backlog: &IndexOperationBacklog,
) -> Result<LoadedBacklog> {
    let (summary, empty) =
        crate::merge_operator::observe_empty_queue_resolutions(load_scopes(reader, store, backlog))
            .await;
    let drained = match store.layout() {
        QueueLayout::Map => empty
            .iter()
            .filter_map(|key| match ManagedIndexKey::parse_data_from_slice(key) {
                Ok(ManagedIndexKey::Data {
                    scope,
                    kind: ScopedKey::IndexOperationQueue(key),
                }) => Some(QueueTarget::new(scope, key.index_id, key.generation)),
                Ok(ManagedIndexKey::Data { .. } | ManagedIndexKey::Global { .. }) | Err(_) => None,
            })
            .collect(),
        // Row-layout queues are rows that acknowledgements delete.
        QueueLayout::Rows => Vec::new(),
    };
    Ok(LoadedBacklog {
        summary: summary?,
        drained,
    })
}

/// Walks every scope's queue range through `read`, in key order.
///
/// Tenant scopes have no registry, so a scan from the start of the tenant
/// keyspace, and then from past each tenant's keyspace, finds each tenant
/// from its first row. A first row inside the tenant's queue range starts
/// it, and the same scan continues through it; a first row before the range
/// costs one more seek, and one past it means the tenant holds no queue. So
/// a tenant costs at most two seeks besides its queue rows, which is linear
/// in the number of tenant scopes that contain any rows. A fresh scan per
/// tenant, rather than seeking one iterator: SlateDB's merge iterator drops
/// the entry it peeked past a merged row when it seeks, so a seek could skip
/// a queue row. The walk starts at the tenant keyspace itself, so a
/// malformed envelope sorting before the first tenant (such as a lone
/// `TENANT_KEY_PREFIX`) is read and fails the load.
async fn load_scopes(
    read: &(impl DbReadOps + Sync),
    store: &QueueStore,
    backlog: &IndexOperationBacklog,
) -> Result<LoadedQueueSummary> {
    let mut walk = Walk {
        backlog,
        discovery: store.discovery(),
        owners: None,
        summary: LoadedQueueSummary::default(),
    };
    walk.rows(
        read,
        read.scan(discovery_range(DataScope::LegacyUnscoped))
            .await?,
        None,
    )
    .await?;
    let tenants_end = Bytes::from_static(&[TENANT_KEY_PREFIX + 1]);
    let mut next = Bytes::from_static(&[TENANT_KEY_PREFIX]);
    loop {
        let mut rows = read.scan(next..tenants_end.clone()).await?;
        let Some(first) = rows.next().await? else {
            break;
        };
        let Some((tenant, _)) = DataScope::strip_tenant_envelope(&first.key) else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "tenant discovery encountered an invalid envelope".to_string(),
            ));
        };
        let range = discovery_range(DataScope::Tenant(tenant));
        if first.key < range.start {
            walk.rows(read, read.scan(range).await?, None).await?;
        } else if range.contains(&first.key) {
            walk.push(read, &first.key, &first.value).await?;
            walk.rows(read, rows, Some(&range)).await?;
        }
        let Some(following) = tenant.as_u128().checked_add(1) else {
            break;
        };
        let mut prefix = Vec::new();
        DataScope::Tenant(TenantId::from_u128(following)).encode_key_prefix(&mut prefix);
        next = Bytes::from(prefix);
    }
    let Some(queue) = walk.discovery.finish() else {
        return Ok(walk.summary);
    };
    load_queue(read, backlog, &mut walk.owners, &mut walk.summary, queue).await?;
    Ok(walk.summary)
}

/// State of one load as it walks scopes in key order.
struct Walk<'a> {
    backlog: &'a IndexOperationBacklog,
    discovery: QueueDiscovery<'a>,
    /// Index records of the scope that yielded the previous queue (see
    /// [`load_queue`]).
    owners: Option<(DataScope, HashMap<IndexId, IndexRecordV2>)>,
    summary: LoadedQueueSummary,
}

impl Walk<'_> {
    /// Pushes every row `rows` yields, or, given `within`, every row until the
    /// first outside it.
    async fn rows(
        &mut self,
        read: &(impl DbReadOps + Sync),
        mut rows: DbIterator,
        within: Option<&std::ops::Range<Bytes>>,
    ) -> Result<()> {
        while let Some(row) = rows.next().await? {
            if within.is_some_and(|range| !range.contains(&row.key)) {
                return Ok(());
            }
            self.push(read, &row.key, &row.value).await?;
        }
        Ok(())
    }

    /// Pushes one row of a queue range and charges the queue it completes.
    async fn push(
        &mut self,
        read: &(impl DbReadOps + Sync),
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let Some(queue) = self.discovery.push(key, value)? else {
            return Ok(());
        };
        load_queue(
            read,
            self.backlog,
            &mut self.owners,
            &mut self.summary,
            queue,
        )
        .await
    }
}

/// A finished load.
pub(crate) struct LoadedBacklog {
    pub(crate) summary: LoadedQueueSummary,
    /// Map-layout queues whose stored history the load resolved empty.
    drained: Vec<QueueTarget>,
}

/// What [`LoadedBacklog::tombstone_drained`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainedTombstones {
    /// No queue the load resolved empty is still drained.
    NothingDrained,
    /// One transaction deleted this many drained queue keys.
    Committed { queues: u64 },
    /// A write to one of those keys committed after the transaction began,
    /// so nothing was deleted; a later open finds the keys again.
    Conflicted { queues: u64 },
}

/// The deletes [`LoadedBacklog::stage_tombstones`] staged, not yet committed.
pub(crate) struct StagedTombstones {
    transaction: DbTransaction,
    queues: u64,
}

impl LoadedBacklog {
    /// Deletes, in one transaction over `db`, every queue key the load
    /// resolved empty that is still drained once that transaction began
    /// ([`Self::stage_tombstones`]), so a later read stops at the tombstone
    /// instead of merging the history again.
    ///
    /// One transaction, so a crash deletes all of the keys or none, and any
    /// write to one of them that commits after it began aborts every delete.
    /// A deleted key reads absent with no history, so the next open finds
    /// nothing to delete: repeating it is harmless. A commit that fails
    /// otherwise than by a conflict fails with its storage error, as any
    /// write of a writer that cannot commit does.
    pub(crate) async fn tombstone_drained(
        self,
        db: &Db,
        backlog: &IndexOperationBacklog,
    ) -> Result<DrainedTombstones> {
        let Some(staged) = self.stage_tombstones(db, backlog).await? else {
            return Ok(DrainedTombstones::NothingDrained);
        };
        staged.commit().await
    }

    /// Begins a transaction over `db` and stages in it the delete of every
    /// queue key the load resolved empty that is still drained: `backlog`
    /// charges its target nothing and a point get in the transaction reads it
    /// absent. Returns `None` when no key is still drained, having begun no
    /// transaction when the load found none, as on every open after the
    /// first and always in the row layout.
    ///
    /// Each delete is the one "Delete on drain" on
    /// [`QueueStore::stage_acknowledge`] stages, under the same rules: the
    /// ledger is asked after the transaction began, so an enqueue that
    /// committed before then is charged (or read by the point get), and the
    /// key is marked read ([`stage_queue_delete`]), so one that commits
    /// after it began aborts the commit. Writers run it before accepting
    /// graph writes, so that guards against an enqueue only by construction.
    pub(crate) async fn stage_tombstones(
        self,
        db: &Db,
        backlog: &IndexOperationBacklog,
    ) -> Result<Option<StagedTombstones>> {
        if self.drained.is_empty() {
            return Ok(None);
        }
        let transaction = db.begin(IsolationLevel::Snapshot).await?;
        let mut queues = 0_u64;
        // Sequential point gets: a load finds few drained keys, and only the
        // first open after they drained finds any.
        for target in self.drained {
            if backlog.has_charges(target) || transaction.get(target.key()).await?.is_some() {
                continue;
            }
            stage_queue_delete(&transaction, target)?;
            queues += 1;
        }
        Ok((queues > 0).then_some(StagedTombstones {
            transaction,
            queues,
        }))
    }
}

impl StagedTombstones {
    /// Commits every staged delete at once.
    pub(crate) async fn commit(self) -> Result<DrainedTombstones> {
        let queues = self.queues;
        match self.transaction.commit().await {
            Ok(_) => Ok(DrainedTombstones::Committed { queues }),
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                Ok(DrainedTombstones::Conflicted { queues })
            }
            Err(error) => Err(HelixDbError::from_storage_commit(error)),
        }
    }
}

/// Validates `queue` against the canonical record that still names its
/// generation, if any, and charges it.
///
/// `owners` caches the index records of the scope that yielded the previous
/// queue; queues arrive scope by scope, so each scope's records are read at
/// most once.
async fn load_queue(
    reader: &(impl DbReadOps + Sync),
    backlog: &IndexOperationBacklog,
    owners: &mut Option<(DataScope, HashMap<IndexId, IndexRecordV2>)>,
    summary: &mut LoadedQueueSummary,
    queue: DiscoveredQueue,
) -> Result<()> {
    let scope = queue.target.scope;
    let records = match owners.take() {
        Some((loaded, records)) if loaded == scope => records,
        Some(_) | None => {
            let prefix = ManagedIndexKey::data_prefix(
                scope,
                ScopedKey::logical_prefix(RecordKind::IndexRecord),
            );
            let mut rows = reader.scan_prefix(&prefix, ..).await?;
            let mut records = HashMap::new();
            while let Some(row) = rows.next().await? {
                let record = decode_index_record(&row.value)?;
                records.insert(record.index_id(), record);
            }
            records
        }
    };
    records
        .get(&queue.target.index_id)
        .filter(|record| record.state().generation() == queue.target.generation)
        .map_or(Ok(()), |record| validate_owner(record, &queue))?;
    *owners = Some((scope, records));
    summary.queues += 1;
    summary.operations += queue.frames.len() as u64;
    backlog.load_durable(
        queue.target,
        queue
            .frames
            .iter()
            .map(|frame| (frame.id, frame.entity, frame.retained_bytes)),
    );
    Ok(())
}

/// Fails closed when any scope holds queues written with a layout other than
/// `layout`.
///
/// Readers run this at open instead of loading the backlog: their search
/// overlays read `layout` alone, so a mismatch would silently drop every
/// pending operation from strong searches. It costs one seek per tenant scope,
/// so only builds that can select [`crate::config::QueueLayout::Rows`] compile
/// it; product handles all run the map layout. It sees the queues present at
/// open: a reader opened while no other-layout queue exists cannot notice one
/// written later.
#[cfg(any(test, feature = "async-index-benchmark"))]
pub(crate) async fn require_layout(
    reader: &(impl DbReadOps + Sync),
    layout: crate::config::QueueLayout,
) -> Result<()> {
    for scope in discover_scopes(reader).await? {
        super::storage::require_scope_layout(reader, layout, scope).await?;
    }
    Ok(())
}

/// Checks that a queue matches the family and element kind of the canonical
/// record that still names its generation.
fn validate_owner(record: &IndexRecordV2, queue: &DiscoveredQueue) -> Result<()> {
    let (family, element_kind) = match record.definition() {
        ValidatedDynamicIndexDefinition::Vector(definition) => {
            (QueueFamily::Vector, definition.element_kind())
        }
        ValidatedDynamicIndexDefinition::Text(definition) => {
            (QueueFamily::Text, definition.element_kind())
        }
        ValidatedDynamicIndexDefinition::Secondary(_) => {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "secondary index {} owns an operation queue",
                queue.target.index_id.get()
            )));
        }
    };
    if queue.family != family
        || queue
            .frames
            .iter()
            .any(|frame| frame.entity.kind != element_kind)
    {
        return Err(HelixDbError::IndexCatalogCorruption(format!(
            "operation queue for index {} does not match its canonical definition",
            queue.target.index_id.get()
        )));
    }
    Ok(())
}

/// Counts reported after startup reconstruction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LoadedQueueSummary {
    pub(crate) queues: u64,
    pub(crate) operations: u64,
}
