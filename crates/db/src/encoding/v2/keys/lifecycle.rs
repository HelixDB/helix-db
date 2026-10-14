//! Scoped catalog, operation, build-delta, applied-state, and queue keys.

use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexIdentity, IndexOperationId,
};

/// Entity identity used by build-delta and applied-state keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexEntity {
    pub(crate) kind: IndexElementKind,
    pub(crate) id: IndexEntityId,
}

/// Canonical catalog-record key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IndexRecordKey {
    pub(crate) identity: IndexIdentity,
}

/// Scoped operation record key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexOperationKey {
    pub(crate) operation_id: IndexOperationId,
}

/// Coalesced build delta or builder-applied state key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexEntityStateKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) entity: IndexEntity,
}

/// Directly addressable immutable index-operation queue for one generation.
///
/// Scope comes from the physical key envelope, so one key names exactly one
/// `(scope, logical index, generation)` queue. An absent key is an empty queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexOperationQueueKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
}

/// One immutable index operation stored as its own row.
///
/// Row-layout queues (the benchmark baseline) key each operation by a
/// writer-allocated sequence, so a generation prefix scan returns operations
/// in enqueue order. An absent row is an acknowledged operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexOperationRowKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) sequence: u64,
}
