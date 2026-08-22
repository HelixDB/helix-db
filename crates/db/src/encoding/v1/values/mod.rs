//! Deprecated stored-value paths.

#![allow(deprecated, unused_imports)]

#[deprecated(note = "use encoding::v2::values::edge_endpoints")]
pub(crate) mod edge_endpoints {
    #[deprecated(note = "use encoding::v2::values::edge_endpoints")]
    pub(crate) use crate::encoding::v2::values::edge_endpoints::*;
}

#[deprecated(note = "use encoding::v2::values::adjacency")]
pub mod edges {
    #[deprecated(note = "use encoding::v2::values::adjacency")]
    pub use crate::encoding::v2::values::adjacency::*;
}

#[deprecated(note = "use encoding::v2::values::id_allocation")]
pub(crate) mod id_allocation {
    #[deprecated(note = "use encoding::v2::values::id_allocation")]
    pub(crate) use crate::encoding::v2::values::id_allocation::*;
}

#[deprecated(note = "use encoding::v2::values::indexes::equality")]
pub(crate) mod secondary {
    #[deprecated(note = "use encoding::v2::values::indexes::equality")]
    pub(crate) use crate::encoding::v2::values::indexes::SecondaryEqualityValue;
}

#[cfg(any(test, feature = "fuzzing", feature = "production-coverage"))]
#[deprecated(note = "use encoding::v2::values::indexes::text")]
pub(crate) mod text_index {
    #[deprecated(note = "use encoding::v2::legacy::text")]
    pub(crate) use crate::encoding::v2::legacy::text::{
        live_state::decode as decode_live_state, manifest::decode as decode_manifest,
        version_counter::decode as decode_version_counter,
    };
    #[cfg(any(test, feature = "production-coverage"))]
    #[deprecated(note = "use encoding::v2::legacy::text")]
    pub(crate) use crate::encoding::v2::legacy::text::{
        live_state::encode_for_contract as encode_live_state,
        manifest::encode_for_contract as encode_manifest,
        version_counter::encode_for_contract as encode_version_counter,
    };
}

#[deprecated(note = "use encoding::v2::values::indexes::vector::generation")]
pub(crate) mod vector_generation {
    #[deprecated(note = "use encoding::v2::values::indexes::vector::generation")]
    pub(crate) use crate::encoding::v2::values::indexes::vector::*;
}

#[deprecated(note = "use encoding::v2::values::indexes::vector")]
pub mod vectors {
    #[deprecated(note = "use encoding::v2::values::indexes::vector")]
    pub use crate::encoding::v2::values::indexes::vector::{
        decode_layer0_neighbors, decode_layer0_neighbors_and_simhash, encode_layer0_neighbors,
        encode_layer0_record, ENCODING_TYPE_LAYER0_NEIGHBORS, ENCODING_TYPE_LAYER0_RECORD,
    };

    #[deprecated(note = "use encoding::v2::values::indexes::vector::entry_candidate")]
    pub mod entry {
        #[deprecated(note = "use encoding::v2::values::indexes::vector::entry_candidate")]
        pub use crate::encoding::v2::values::indexes::vector::entry_candidate::*;
    }
    #[deprecated(note = "use encoding::v2::values::indexes::vector::item")]
    pub(crate) mod item {
        #[deprecated(note = "use encoding::v2::values::indexes::vector::item")]
        pub(crate) use crate::encoding::v2::values::indexes::vector::item::*;
    }
    #[deprecated(note = "use encoding::v2::values::indexes::vector::markers")]
    pub(crate) mod markers {
        #[deprecated(note = "use encoding::v2::legacy::vector::transaction_guard")]
        pub(crate) use crate::encoding::v2::legacy::vector::transaction_guard::decode_active_txn_guard;
        #[cfg(any(test, feature = "fuzzing", feature = "production-coverage"))]
        #[deprecated(note = "use encoding::v2::legacy::vector::transaction_guard")]
        pub(crate) use crate::encoding::v2::legacy::vector::transaction_guard::encode_active_txn_guard;
        #[deprecated(note = "use encoding::v2::values::indexes::vector::markers")]
        pub(crate) use crate::encoding::v2::values::indexes::vector::markers::*;
    }
    #[deprecated(note = "use encoding::v2::values::indexes::vector::metadata")]
    pub(crate) mod metadata {
        #[deprecated(note = "use encoding::v2::legacy::vector::metadata")]
        pub(crate) use crate::encoding::v2::legacy::vector::metadata::decode_legacy_metadata;
        #[cfg(any(test, feature = "production-coverage"))]
        #[deprecated(note = "use encoding::v2::legacy::vector::metadata")]
        pub(crate) use crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract;
        #[deprecated(note = "use encoding::v2::values::indexes::vector::metadata")]
        pub(crate) use crate::encoding::v2::values::indexes::vector::metadata::*;
    }
    #[deprecated(note = "use encoding::v2::values::indexes::vector::neighbors")]
    pub mod neighbors {
        #[deprecated(note = "use encoding::v2::values::indexes::vector::neighbors")]
        pub use crate::encoding::v2::values::indexes::vector::neighbors::*;
    }
    #[deprecated(note = "use encoding::v2::values::indexes::vector::simhash")]
    pub(crate) mod simhash {
        #[deprecated(note = "use encoding::v2::values::indexes::vector::simhash")]
        pub(crate) use crate::encoding::v2::values::indexes::vector::simhash::*;
    }
}
