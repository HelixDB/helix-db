//! Canonical typed database key construction and parsing.

pub(crate) mod codec;
mod data;
pub(crate) mod global;
pub(crate) mod graph;
pub mod indexes;
pub(crate) mod lifecycle;
mod managed_index;
pub(crate) mod metadata;
pub mod scope;

/// Deprecated compatibility path for the former unversioned tenant module.
///
/// ```
/// #![allow(deprecated)]
/// use db::encoding::keys::tenant::{DataScope, TenantId};
///
/// let tenant = TenantId::from_ulid_str("00000000000000000000000001").unwrap();
/// assert!(!DataScope::Tenant(tenant).is_unscoped());
/// ```
#[deprecated(note = "use encoding::v2::keys::scope")]
pub mod tenant {
    pub use super::scope::{DataScope, TenantId};
}

pub(crate) use data::{DataKey, DataKeyKind, GlobalKeyKind, KeyPrefix, ID_LEN, PREFIX_LEN};
pub use data::{EdgeId, NodeId};
pub(crate) use global::{GlobalKey, GlobalKind, TextCompactionTarget, GLOBAL_SENTINEL};
pub(crate) use graph::{
    AdjacencyKey, EdgeEndpointsKey, EdgePairIndexKey, EdgePropertyByIdKey, NodePropertyKey,
};
pub(crate) use indexes::text::{
    BlobHash, PartitionFingerprint, TextBuildArtifactKey, TextCorpusStatisticsKey,
    TextEntityStateKey, TextManifestPageKey, TextManifestRootKey, TextStatisticsEntityKey,
    TextTermFingerprint, TextTermStatisticsKey,
};
pub(crate) use indexes::vector::VectorPartitionMappingKey;
pub(crate) use indexes::{
    CanonicalSecondaryValue, SecondaryEntryKey, SecondaryEntryLane, SecondaryEqualityBitmapKey,
};
#[allow(unused_imports)]
pub(crate) use lifecycle::{IndexEntity, IndexEntityStateKey, IndexOperationKey, IndexRecordKey};
pub(crate) use managed_index::{
    decode_generation, decode_identity, decode_index_id, decode_operation_id, encode_identity,
    identity_encoded_len, model_key_error, KeyDecoder, ManagedIndexKey, RecordKind, ScopedKey,
    HASH_LEN, KEY_MAX_LEN, KIND_LEN, U32_LEN, U64_LEN, UUID_LEN,
};
pub(crate) use metadata::MetadataKey;
pub use scope::{DataScope, TenantId};
