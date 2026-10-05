//! Durable queue discovery for startup accounting and reconciliation.
//!
//! Startup scans each data scope's queue prefix (one key per generation with
//! outstanding work in the map layout, one row per operation in the row
//! layout) and validates each queue against the canonical record that still
//! names its generation. Tenant scopes have no
//! registry; a bounded seek per tenant envelope discovers them without
//! scanning tenant data. That cost is linear in the number of tenant scopes
//! that contain any rows.

use std::collections::HashMap;
use std::ops::Bound;

use bytes::Bytes;
use slatedb::DbReadOps;

use crate::encoding::v2::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::indexes::operation_queue::{OperationQueue, QueueFamily};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexRecordV2, ValidatedDynamicIndexDefinition};

use super::backlog::IndexOperationBacklog;
use super::storage::QueueStore;
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
pub(crate) async fn discover_scopes(reader: &(impl DbReadOps + Sync)) -> Result<Vec<DataScope>> {
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
/// every scope is loaded, including queues of retired generations, so the
/// publisher can discard work that no canonical record still owns.
pub(crate) async fn load_backlog(
    reader: &(impl DbReadOps + Sync),
    store: &QueueStore,
    backlog: &IndexOperationBacklog,
) -> Result<LoadedQueueSummary> {
    let mut summary = LoadedQueueSummary::default();
    for scope in discover_scopes(reader).await? {
        let prefix =
            ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
        let mut rows = reader.scan_prefix(&prefix, ..).await?;
        let mut records = HashMap::new();
        while let Some(row) = rows.next().await? {
            let record = decode_index_record(&row.value)?;
            records.insert(record.index_id(), record);
        }
        for (target, queue) in store.discover(reader, scope).await? {
            if let Some(record) = records
                .get(&target.index_id)
                .filter(|record| record.state().generation() == target.generation)
            {
                validate_owner(record, target, &queue)?;
            }
            summary.queues += 1;
            summary.operations += queue.operations().len() as u64;
            backlog.load_durable(
                target,
                queue.operations().iter().map(|operation| {
                    (
                        operation.id(),
                        operation.entity(),
                        operation.retained_bytes(),
                    )
                }),
            );
        }
    }
    Ok(summary)
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
fn validate_owner(
    record: &IndexRecordV2,
    target: QueueTarget,
    queue: &OperationQueue,
) -> Result<()> {
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
                target.index_id.get()
            )));
        }
    };
    if queue.family() != family
        || queue
            .operations()
            .iter()
            .any(|operation| operation.entity().kind != element_kind)
    {
        return Err(HelixDbError::IndexCatalogCorruption(format!(
            "operation queue for index {} does not match its canonical definition",
            target.index_id.get()
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
