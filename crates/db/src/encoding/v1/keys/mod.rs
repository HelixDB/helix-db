//! Deprecated graph-key paths.

#![allow(deprecated, unused_imports)]

#[deprecated(note = "use encoding::v2::keys::metadata")]
pub(crate) mod metadata {
    #[deprecated(note = "use encoding::v2::keys::metadata")]
    pub(crate) use crate::encoding::v2::keys::metadata::*;
    #[deprecated(note = "use encoding::v2::legacy::index_catalog::catalog_scan_prefix")]
    pub(crate) use crate::encoding::v2::legacy::index_catalog::catalog_scan_prefix as dynamic_index_prefix_scoped;
    #[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
    #[deprecated(note = "use encoding::v2::legacy::index_catalog::catalog_storage_key")]
    pub(crate) use crate::encoding::v2::legacy::index_catalog::catalog_storage_key as dynamic_index_storage_key_scoped;
    #[deprecated(note = "use encoding::v2::legacy::text::storage_keys")]
    pub(crate) use crate::encoding::v2::legacy::text::storage_keys::{
        definition_guard_key as text_definition_guard_key_scoped,
        live_state_key as text_index_live_state_key_scoped,
        live_state_prefix as text_index_live_state_prefix_scoped,
        manifest_key as text_index_manifest_key_scoped,
        manifest_prefix as text_index_manifest_prefix_scoped,
        manifest_scan_prefix as text_index_manifest_scan_prefix_scoped,
        transaction_guard_key as text_index_txn_guard_key_scoped,
        version_counter_key as text_index_version_counter_key_scoped,
        LegacyTextMetadataElement as TextMetadataElement,
    };
}

#[deprecated(note = "use encoding::v2::keys::scope")]
pub mod tenant {
    #[deprecated(note = "use encoding::v2::keys::scope")]
    pub use crate::encoding::v2::keys::scope::*;
}

#[deprecated(note = "use encoding::v2::keys::indexes::vector")]
pub(crate) mod vectors {
    #[deprecated(note = "use encoding::v2::keys::indexes::vector")]
    pub(crate) use crate::encoding::v2::keys::indexes::vector::*;
    #[deprecated(
        note = "use encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey"
    )]
    pub(crate) use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey as VectorTxnGuardKey;
}

#[deprecated(note = "use encoding::v2::keys")]
pub(crate) use crate::encoding::v2::keys::{
    AdjacencyKey, DataKey, DataKey as Key, DataKeyKind, EdgeEndpointsKey, EdgePairIndexKey,
    EdgePropertyByIdKey, GlobalKeyKind, KeyPrefix, MetadataKey, NodePropertyKey, ID_LEN,
    PREFIX_LEN,
};
#[deprecated(note = "use encoding::v2::keys")]
pub use crate::encoding::v2::keys::{EdgeId, NodeId};
#[deprecated(note = "use encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey")]
pub(crate) use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey as EdgePropertyPairKey;
