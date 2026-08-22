//! Canonical typed database value construction and parsing.

pub mod adjacency;
mod codec;
pub(crate) mod edge_endpoints;
mod global;
pub(crate) mod id_allocation;
pub mod indexes;
mod lifecycle;
pub mod property;

pub(crate) use codec::*;
pub(crate) use global::{decode_metadata_value, encode_metadata_value};
pub(crate) use indexes::text::{
    decode_build_artifact, decode_corpus_statistics,
    decode_entity_state as decode_text_entity_state, decode_manifest_page, decode_manifest_root,
    decode_statistics_entity, decode_term_statistics, encode_build_artifact,
    encode_corpus_statistics, encode_entity_state as encode_text_entity_state,
    encode_manifest_page, encode_manifest_root, encode_statistics_entity, encode_term_statistics,
};
pub(crate) use indexes::vector::{decode_partition_mapping, encode_partition_mapping};
pub(crate) use indexes::{
    decode_secondary_entry, encode_secondary_entry, SecondaryEqualityBitmapValue,
};
pub(crate) use lifecycle::{
    decode_applied_state, decode_build_delta, decode_index_record, decode_operation_record,
    decode_operation_record_with_compatibility, encode_applied_state, encode_build_delta,
    encode_index_record, encode_operation_record,
};
