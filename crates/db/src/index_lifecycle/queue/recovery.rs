//! Durable queue discovery for startup accounting and reconciliation.
//!
//! Startup reads every queue key of every data scope (one key per generation
//! with outstanding work in the map layout, one row per operation in the row
//! layout) in one forward pass, charges each queue as the scan yields it, and
//! drops it before the next, so open holds one queue at a time rather than a
//! scope's whole backlog. Each queue is validated against the canonical
//! record that still names its generation; a scope's records are read only
//! once it yields a queue. Tenant scopes have no registry, so one iterator
//! over the tenant keyspace seeks from each tenant's queue range to the
//! next tenant's: a tenant costs at most two seeks besides its own queue
//! rows, which is linear in the number of tenant scopes that contain any
//! rows.

use std::collections::HashMap;

use bytes::Bytes;
use slatedb::{DbIterator, DbReadOps, KeyValue};

use crate::encoding::v2::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
#[cfg(test)]
use crate::encoding::v2::values::indexes::operation_queue::OperationQueue;
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexId, IndexRecordV2, ValidatedDynamicIndexDefinition};

use super::backlog::IndexOperationBacklog;
use super::storage::{discovery_range, DiscoveredQueue, QueueStore};
#[cfg(test)]
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
pub(crate) async fn load_backlog(
    reader: &(impl DbReadOps + Sync),
    store: &QueueStore,
    backlog: &IndexOperationBacklog,
) -> Result<LoadedQueueSummary> {
    let mut discovery = store.discovery();
    let mut owners = None;
    let mut summary = LoadedQueueSummary::default();
    let mut legacy = reader
        .scan(discovery_range(DataScope::LegacyUnscoped))
        .await?;
    while let Some(row) = legacy.next().await? {
        let Some(queue) = discovery.push(&row.key, &row.value)? else {
            continue;
        };
        load_queue(reader, backlog, &mut owners, &mut summary, queue).await?;
    }
    drop(legacy);
    let mut tenants = reader
        .scan(
            discovery_range(DataScope::Tenant(TenantId::from_u128(0))).start
                ..Bytes::from_static(&[TENANT_KEY_PREFIX + 1]),
        )
        .await?;
    while let Some(row) = next_tenant_queue_row(&mut tenants).await? {
        let Some(queue) = discovery.push(&row.key, &row.value)? else {
            continue;
        };
        load_queue(reader, backlog, &mut owners, &mut summary, queue).await?;
    }
    let Some(queue) = discovery.finish() else {
        return Ok(summary);
    };
    load_queue(reader, backlog, &mut owners, &mut summary, queue).await?;
    Ok(summary)
}

/// Advances `rows`, an iterator over the tenant keyspace, to its next row
/// inside its own tenant's [`discovery_range`].
///
/// Only ever seeks forward: a row before its tenant's range seeks into that
/// range, and a row past it seeks to the next tenant's range, so a tenant
/// costs at most two seeks however many rows it holds outside its queues.
async fn next_tenant_queue_row(rows: &mut DbIterator) -> Result<Option<KeyValue>> {
    while let Some(row) = rows.next().await? {
        let Some((tenant, _)) = DataScope::strip_tenant_envelope(&row.key) else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "tenant discovery encountered an invalid envelope".to_string(),
            ));
        };
        let range = discovery_range(DataScope::Tenant(tenant));
        if range.contains(&row.key) {
            return Ok(Some(row));
        }
        if row.key < range.start {
            rows.seek(range.start).await?;
            continue;
        }
        let Some(following) = tenant.as_u128().checked_add(1) else {
            return Ok(None);
        };
        rows.seek(discovery_range(DataScope::Tenant(TenantId::from_u128(following))).start)
            .await?;
    }
    Ok(None)
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
