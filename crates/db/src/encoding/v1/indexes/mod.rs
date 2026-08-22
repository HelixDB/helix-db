//! Deprecated property-index key paths.

#![allow(deprecated, unused_imports)]

#[deprecated(note = "use encoding::v2::keys::indexes::equality")]
pub(crate) mod equality {
    #[deprecated(note = "use encoding::v2::keys::indexes::equality")]
    pub(crate) use crate::encoding::v2::keys::indexes::equality::*;
}

#[deprecated(note = "use encoding::v2::keys::indexes::label")]
pub(crate) mod label {
    #[deprecated(note = "use encoding::v2::keys::indexes::label")]
    pub(crate) use crate::encoding::v2::keys::indexes::label::*;
}

#[deprecated(note = "use encoding::v2::keys::indexes::range")]
pub(crate) mod range {
    #[deprecated(note = "use encoding::v2::keys::indexes::range")]
    pub(crate) use crate::encoding::v2::keys::indexes::range::*;
}

#[deprecated(note = "use encoding::v2::keys::indexes::prefix")]
pub(crate) mod scan_prefixes {
    #[deprecated(note = "use encoding::v2::keys::indexes::equality::scans")]
    pub(crate) use crate::encoding::v2::keys::indexes::equality::scans::*;
    #[deprecated(note = "use encoding::v2::keys::indexes::label")]
    pub(crate) use crate::encoding::v2::keys::indexes::label::{
        EdgeLabelNeighborScanPrefix, EdgeLabelScanPrefix,
    };
    #[deprecated(note = "use encoding::v2::keys::indexes::prefix")]
    pub(crate) use crate::encoding::v2::keys::indexes::prefix::*;
    #[deprecated(note = "use encoding::v2::keys::indexes::range::scans")]
    pub(crate) use crate::encoding::v2::keys::indexes::range::scans::*;
}

#[deprecated(note = "use encoding::v2::keys::indexes")]
pub(crate) use crate::encoding::v2::keys::indexes::{
    hash_property_name, hash_property_value, EdgeDirection, IndexPrefix, PropertyHash,
    PropertyIndexKey as IndexKey, ValueHash, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
    PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN,
};
