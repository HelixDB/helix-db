//! Which rows a dropped generation owns.
//!
//! Every key kind is classified exactly once, by an exhaustive match over its
//! enum, so adding a kind does not compile until its ownership is decided.
//! [`owned_row`] is the only place a compaction filter learns that a row may
//! be discarded: it recognises the two vector lanes keyed by physical index
//! id and the managed-index record kinds keyed by generation, and nothing
//! else. Catalog, operation, global and graph rows are never matched.

use bytes::Bytes;

use crate::encoding::v2::keys::indexes::vector::{
    VectorStorageLane, VECTOR_HOT_KEYSPACE_PREFIX, VECTOR_INDEX_PREFIX_LEN,
    VECTOR_L0_KEYSPACE_PREFIX,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    GlobalKind, KeyPrefix, RecordKind, ScopedKey, GLOBAL_SENTINEL, KIND_LEN, U64_LEN,
};

/// Length of `[0x06][kind][index_id:8][generation:8]`, the prefix shared by
/// every row of one record kind and generation.
const GENERATION_PREFIX_LEN: usize = KIND_LEN + KIND_LEN + U64_LEN + U64_LEN;

/// What decides whether a row of one key kind dies with a dropped generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowOwner {
    /// The row's key carries a vector physical index id right after its lane
    /// byte, and the row belongs to that physical index alone.
    PhysicalIndex,
    /// The row's key carries `[index_id:8][generation:8]` right after its
    /// record kind, and the row belongs to that generation alone.
    Generation,
    /// No dropped generation owns the row through its key: a filter keeps it,
    /// and a drop that must remove it deletes it directly.
    Never,
}

impl RowOwner {
    /// Ownership of the rows in one vector storage lane.
    ///
    /// Core rows share `[0x03][0x03]` with legacy graph property-index rows,
    /// so no filter may match them: a drop deletes them directly.
    pub(crate) const fn of_vector_lane(lane: VectorStorageLane) -> Self {
        match lane {
            VectorStorageLane::Hot | VectorStorageLane::Layer0 => Self::PhysicalIndex,
            VectorStorageLane::Core => Self::Never,
        }
    }

    /// Ownership of the managed-index rows of one record kind.
    ///
    /// Partition mappings are keyed by generation but stay until the dropped
    /// generation's entry is deleted, because they alone list a partitioned
    /// generation's physical ids. Operation queues and rows are keyed by
    /// generation too, and belong to the operation lifecycle.
    pub(crate) const fn of_record_kind(kind: RecordKind) -> Self {
        match kind {
            RecordKind::BuildDelta
            | RecordKind::AppliedState
            | RecordKind::SecondaryEntry
            | RecordKind::SecondaryEqualityBitmap
            | RecordKind::TextManifestRoot
            | RecordKind::TextManifestPage
            | RecordKind::TextBuildArtifact
            | RecordKind::TextEntityState
            | RecordKind::TextCorpusStatistics
            | RecordKind::TextTermStatistics
            | RecordKind::TextStatisticsEntity => Self::Generation,
            RecordKind::IndexRecord
            | RecordKind::Operation
            | RecordKind::VectorPartitionMapping
            | RecordKind::IndexOperationQueue
            | RecordKind::IndexOperationRow => Self::Never,
        }
    }

    /// Ownership of graph rows: none belongs to a generation.
    pub(crate) const fn of_key_prefix(prefix: KeyPrefix) -> Self {
        match prefix {
            KeyPrefix::Adjacency
            | KeyPrefix::EdgePropertyPair
            | KeyPrefix::EdgePropertyById
            | KeyPrefix::NodeProperty
            | KeyPrefix::PropertyIndex
            | KeyPrefix::EdgeEndpoints
            | KeyPrefix::EdgePairIndex
            | KeyPrefix::Metadata => Self::Never,
        }
    }

    /// Ownership of global rows: none belongs to a generation, including the
    /// dropped-generation entries themselves.
    pub(crate) const fn of_global_kind(kind: GlobalKind) -> Self {
        match kind {
            GlobalKind::StorageVersion
            | GlobalKind::LogicalIndexIdWatermark
            | GlobalKind::VectorPhysicalIdWatermark
            | GlobalKind::OperationPointer
            | GlobalKind::LegacyVectorPhysicalReservation
            | GlobalKind::TextCompactionPointer
            | GlobalKind::DroppedGeneration => Self::Never,
        }
    }
}

/// The owner a row key names.
///
/// Ids are the raw key bytes: an id no index ever had matches no dropped
/// generation, so nothing validates them here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OwnerKey {
    /// One vector physical index of one scope.
    PhysicalIndex {
        scope: DataScope,
        physical_index_id: u64,
    },
    /// One generation of one logical index of one scope.
    Generation {
        scope: DataScope,
        index_id: u64,
        generation: u64,
    },
}

/// Every record kind [`RowOwner::of_record_kind`] gives to a generation.
const GENERATION_RECORD_KINDS: [RecordKind; 11] = [
    RecordKind::BuildDelta,
    RecordKind::AppliedState,
    RecordKind::SecondaryEntry,
    RecordKind::TextManifestRoot,
    RecordKind::TextManifestPage,
    RecordKind::TextBuildArtifact,
    RecordKind::TextEntityState,
    RecordKind::TextCorpusStatistics,
    RecordKind::TextTermStatistics,
    RecordKind::TextStatisticsEntity,
    RecordKind::SecondaryEqualityBitmap,
];

impl OwnerKey {
    /// Every key prefix under which [`owned_row`] reports this owner: one per
    /// owned vector lane or generation-owned record kind.
    pub(crate) fn prefixes(self) -> Vec<Bytes> {
        let scoped = |scope: DataScope, logical: &[u8]| {
            let mut key = Vec::with_capacity(scope.encoded_len() + logical.len());
            scope.encode_key_prefix(&mut key);
            key.extend_from_slice(logical);
            Bytes::from(key)
        };
        match self {
            Self::PhysicalIndex {
                scope,
                physical_index_id,
            } => VectorStorageLane::ALL
                .into_iter()
                .filter(|lane| RowOwner::of_vector_lane(*lane) == RowOwner::PhysicalIndex)
                .map(|lane| {
                    scoped(
                        scope,
                        &[&lane.scan_prefix()[..], &physical_index_id.to_be_bytes()].concat(),
                    )
                })
                .collect(),
            Self::Generation {
                scope,
                index_id,
                generation,
            } => GENERATION_RECORD_KINDS
                .into_iter()
                .map(|kind| {
                    scoped(
                        scope,
                        &[
                            &[ScopedKey::key_prefix(), kind.as_u8()][..],
                            &index_id.to_be_bytes(),
                            &generation.to_be_bytes(),
                        ]
                        .concat(),
                    )
                })
                .collect(),
        }
    }
}

/// A row key of an owned kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OwnedRow<'a> {
    /// The owner the key names.
    pub(crate) owner: OwnerKey,
    /// The leading bytes of the key that name the owner. Every key that
    /// starts with them has the same owner, and keys sort contiguously
    /// under them.
    pub(crate) prefix: &'a [u8],
}

/// Classifies one physical key.
///
/// Returns `None` for every key of a kind classified [`RowOwner::Never`], for
/// global keys, and for keys too short to name an owner.
pub(crate) fn owned_row(key: &[u8]) -> Option<OwnedRow<'_>> {
    // A global key opens with 0xFE, which is no tenant envelope and no owned
    // lane, so it falls through to `None` below.
    let (scope, logical) = DataScope::split_physical_key(key).ok()?;
    let envelope_len = key.len() - logical.len();
    let (&first, rest) = logical.split_first()?;
    let owner = if first == ScopedKey::key_prefix() {
        let (&kind, ids) = rest.split_first()?;
        let kind = RecordKind::try_from_u8(kind).ok()?;
        match RowOwner::of_record_kind(kind) {
            RowOwner::Generation => {}
            RowOwner::PhysicalIndex | RowOwner::Never => return None,
        }
        let (index_id, ids) = ids.split_first_chunk::<U64_LEN>()?;
        let (generation, _) = ids.split_first_chunk::<U64_LEN>()?;
        OwnedRow {
            owner: OwnerKey::Generation {
                scope,
                index_id: u64::from_be_bytes(*index_id),
                generation: u64::from_be_bytes(*generation),
            },
            prefix: &key[..envelope_len + GENERATION_PREFIX_LEN],
        }
    } else {
        let lane = match first {
            VECTOR_HOT_KEYSPACE_PREFIX => VectorStorageLane::Hot,
            VECTOR_L0_KEYSPACE_PREFIX => VectorStorageLane::Layer0,
            _ => {
                // A graph row, a global row, or no row this database writes.
                // Their classification is consulted in debug builds only, so
                // that a kind classified as owned cannot reach a filter
                // unparsed; a release compaction pays nothing per graph row.
                let owner = match key.strip_prefix(&GLOBAL_SENTINEL[..]) {
                    Some([kind, ..]) => GlobalKind::try_from_u8(*kind)
                        .map_or(RowOwner::Never, RowOwner::of_global_kind),
                    Some([]) | None => {
                        KeyPrefix::from_u8(first).map_or(RowOwner::Never, RowOwner::of_key_prefix)
                    }
                };
                debug_assert_eq!(
                    owner,
                    RowOwner::Never,
                    "no graph or global key names an owner in its key"
                );
                return None;
            }
        };
        match RowOwner::of_vector_lane(lane) {
            RowOwner::PhysicalIndex => {}
            RowOwner::Generation | RowOwner::Never => return None,
        }
        let (physical_index_id, _) = rest.split_first_chunk::<U64_LEN>()?;
        OwnedRow {
            owner: OwnerKey::PhysicalIndex {
                scope,
                physical_index_id: u64::from_be_bytes(*physical_index_id),
            },
            prefix: &key[..envelope_len + VECTOR_INDEX_PREFIX_LEN],
        }
    };
    Some(owner)
}

#[cfg(test)]
#[path = "../../../tests/unit/dropped_row_ownership.rs"]
mod tests;
