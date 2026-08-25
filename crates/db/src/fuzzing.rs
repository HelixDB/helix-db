//! Feature-gated decoder entry points for out-of-process fuzz targets.
//!
//! The fuzz package cannot name private persisted DTOs, so this module exposes
//! only byte-slice consumers and no decoded values. Each entry point dispatches
//! into the real `encoding/v2` decoder for a closed record family. The default
//! database build omits this module entirely, and enabling it changes neither
//! key construction nor value serialization.

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::MergeOperator;

use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::v2::legacy::{
    text::{live_state, manifest, version_counter},
    vector::transaction_guard,
};
use crate::encoding::v2::{
    keys::{
        scope::{DataScope, TenantId},
        GlobalKey, ManagedIndexKey, ScopedKey, SecondaryEntryLane, SecondaryEqualityBitmapKey,
    },
    values::{
        decode_applied_state, decode_build_artifact, decode_build_delta, decode_corpus_statistics,
        decode_index_record, decode_manifest_page, decode_manifest_root, decode_metadata_value,
        decode_operation_record, decode_partition_mapping, decode_secondary_entry,
        decode_statistics_entity, decode_term_statistics, decode_text_entity_state,
        indexes::{equality as secondary_equality, range as secondary_range, vector as vectors},
        property::equality_index_value::{project_equality_value, EqualityValueProjection},
        SecondaryEqualityBitmapValue,
    },
};
use crate::index_lifecycle::{IndexElementKind, IndexGenerationId, IndexId};
use crate::merge_operator::{encode_bitmap_add, HelixMergeOperator};

/// Exercises the complete physical framing boundary for V2 index keys.
///
/// The selector covers legacy-unscoped, tenant-scoped, and database-global
/// storage envelopes. Tenant decoding deliberately uses a fixed scope so
/// cross-scope frames reach the same production rejection path as storage
/// reads rather than a fuzz-only parser.
pub fn decode_current_index_v2_key(selector: u8, data: &[u8]) {
    match selector % 3 {
        0 => {
            let _ = ManagedIndexKey::parse_from_slice(DataScope::LegacyUnscoped, data);
        }
        1 => {
            let scope = DataScope::Tenant(TenantId::from_u128(u128::MAX));
            let _ = ManagedIndexKey::parse_from_slice(scope, data);
        }
        _ => {
            let _ = GlobalKey::parse_from_slice(data);
        }
    }
}

/// Exercises canonical V2 catalog, operation, and global-control values.
///
/// Inputs flow unchanged into the hand-written production codecs. Successful
/// decoding therefore proves the exact frozen V2 record framing rather than a
/// serde or harness-specific representation.
pub fn decode_current_index_v2_record(selector: u8, data: &[u8]) {
    match selector % 3 {
        0 => {
            let _ = decode_index_record(data);
        }
        1 => {
            let _ = decode_operation_record(data);
        }
        _ => {
            let _ = decode_metadata_value(data);
        }
    }
}

/// Exercises all V2 physical-work, upload, proof, reachability, and GC values.
pub fn decode_current_index_v2_work(data: &[u8]) {
    let _ = typed_work_value_is_valid(data);
}

/// Exercises the portable V4 bitmap value and its typed merge dispatch.
pub fn decode_current_index_v2_bitmap(selector: u8, data: &[u8]) {
    let _ = SecondaryEqualityBitmapValue::decode(data);
    let EqualityValueProjection::Indexed(value) =
        project_equality_value(&PropertyValue::String("fuzz".to_string()))
    else {
        unreachable!("a fixed string is always equality-indexable")
    };
    let scope = if selector & 1 == 0 {
        DataScope::LegacyUnscoped
    } else {
        DataScope::Tenant(TenantId::from_u128(u128::MAX))
    };
    let key = ManagedIndexKey::Data {
        scope,
        kind: ScopedKey::SecondaryEqualityBitmap(
            SecondaryEqualityBitmapKey::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                IndexElementKind::Node,
                value,
            )
            .expect("fixed fuzz bitmap key validates"),
        ),
    }
    .to_bytes();
    let valid_bitmap =
        SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter([1, 7])).encode();
    let operator = HelixMergeOperator::new();
    let _ = match selector % 4 {
        0 => operator.merge(&key, None, Bytes::copy_from_slice(data)),
        1 => operator.merge(&key, Some(valid_bitmap), Bytes::copy_from_slice(data)),
        2 => operator.merge(
            &key,
            Some(Bytes::copy_from_slice(data)),
            encode_bitmap_add(9),
        ),
        _ => operator.merge_batch(
            &key,
            Some(valid_bitmap),
            &[Bytes::copy_from_slice(data), encode_bitmap_add(11)],
        ),
    };
}

fn typed_work_value_is_valid(data: &[u8]) -> bool {
    decode_build_delta(data).is_ok()
        || decode_applied_state(data).is_ok()
        || [
            SecondaryEntryLane::NodeEquality,
            SecondaryEntryLane::NodeUniqueEquality,
            SecondaryEntryLane::NodeRangeAscending,
            SecondaryEntryLane::NodeRangeDescending,
            SecondaryEntryLane::EdgeEquality,
            SecondaryEntryLane::EdgeRangeAscending,
            SecondaryEntryLane::EdgeRangeDescending,
        ]
        .into_iter()
        .any(|lane| decode_secondary_entry(lane, data).is_ok())
        || decode_manifest_root(data).is_ok()
        || decode_manifest_page(data).is_ok()
        || decode_build_artifact(data).is_ok()
        || decode_text_entity_state(data).is_ok()
        || decode_partition_mapping(data).is_ok()
        || decode_corpus_statistics(data).is_ok()
        || decode_term_statistics(data).is_ok()
        || decode_statistics_entity(data).is_ok()
}

/// Exercises one deployed secondary row decoder.
///
/// `selector` chooses a closed decoder family. Invalid bytes are expected to
/// return a typed error; successful decoding must satisfy the production DTO's
/// serde or structural invariants. The function intentionally returns no value
/// so the fuzz crate cannot use private persisted representations.
pub fn decode_current_secondary_record(selector: u8, data: &[u8]) {
    match selector % 2 {
        0 => {
            let _ = secondary_range::SecondaryRangePresence::decode(data);
        }
        _ => {
            let _ = secondary_equality::SecondaryEqualityValue::decode(data);
        }
    }
}

/// Exercises one deployed text or vector value decoder.
///
/// The selector covers current text manifests/version/live-state rows, every
/// vector row component. Decoders receive the
/// caller's bytes unchanged; the harness never synthesizes a replacement
/// physical representation.
pub fn decode_current_search_record(selector: u8, data: &[u8]) {
    match selector % 11 {
        0 => {
            let _ = manifest::decode(data);
        }
        1 => {
            let _ = version_counter::decode(data);
        }
        2 => {
            let _ = live_state::decode(data);
        }
        3 => {
            let _ = vectors::entry_candidate::decode_entry_candidate_layer(data);
        }
        4 => {
            let _ = vectors::neighbors::decode_flat_neighbors(data);
        }
        5 => {
            let _ = vectors::neighbors::decode_upper_neighbors(data);
        }
        6 => {
            let _ = vectors::metadata::decode_metadata(data);
        }
        7 => {
            let _ = vectors::simhash::decode_simhash(data);
        }
        8 => {
            let _ = vectors::decode_layer0_neighbors_and_simhash(data);
        }
        9 => {
            let _ = transaction_guard::decode_active_txn_guard(data);
        }
        _ => {
            let header_len = data.first().copied().map_or(0, usize::from);
            let _ = vectors::item::split_item_parts(data, header_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_seed(seed: &[u8]) -> Vec<u8> {
        let encoded = seed
            .strip_prefix(b"hex:")
            .expect("V2 golden corpus seed uses the reviewable hex envelope");
        let encoded = encoded.strip_suffix(b"\n").unwrap_or(encoded);
        assert_eq!(encoded.len() % 2, 0, "hex seed contains complete bytes");
        encoded
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("hex seed is ASCII");
                u8::from_str_radix(pair, 16).expect("hex seed contains hexadecimal bytes")
            })
            .collect()
    }

    #[test]
    fn malformed_inputs_are_total_across_every_decoder_family() {
        for selector in 0..3 {
            decode_current_index_v2_key(selector, b"malformed");
            decode_current_index_v2_record(selector, b"malformed");
        }
        decode_current_index_v2_work(b"malformed");
        for selector in 0..4 {
            decode_current_index_v2_bitmap(selector, b"malformed");
        }
        for selector in 0..2 {
            decode_current_secondary_record(selector, b"malformed");
        }
        for selector in 0..11 {
            decode_current_search_record(selector, b"malformed");
        }
    }

    #[test]
    fn valid_seed_shapes_reach_current_and_additive_decoders() {
        decode_current_secondary_record(1, b"");
        decode_current_search_record(1, b"1");
        decode_current_search_record(2, br#"{"logical_version":1,"live":true}"#);
        decode_current_search_record(7, b"12345678");
    }

    #[test]
    fn checked_in_corpus_seeds_are_contract_valid() {
        let manifest = include_bytes!("../fuzz/corpus/current_search_records/text-manifest.json");
        assert!(crate::encoding::v2::legacy::text::manifest::decode(
            &manifest[1..manifest.len() - 1]
        )
        .is_ok());
        let live = include_bytes!("../fuzz/corpus/current_search_records/text-live-state.json");
        assert!(live_state::decode(&live[1..live.len() - 1]).is_ok());
        let version = include_bytes!("../fuzz/corpus/current_search_records/text-version.json");
        assert!(version_counter::decode(&version[1..version.len() - 1]).is_ok());
        let entry = include_bytes!("../fuzz/corpus/current_search_records/vector-entry.bin");
        assert!(
            vectors::entry_candidate::decode_entry_candidate_layer(&entry[1..entry.len() - 1])
                .is_ok()
        );
        let simhash = include_bytes!("../fuzz/corpus/current_search_records/vector-simhash.bin");
        assert!(vectors::simhash::decode_simhash(&simhash[1..simhash.len() - 1]).is_ok());
        let layer0 =
            include_bytes!("../fuzz/corpus/current_search_records/vector-layer0-empty.bin");
        assert!(vectors::decode_layer0_neighbors_and_simhash(&layer0[1..layer0.len() - 1]).is_ok());
    }

    #[test]
    fn checked_in_v2_goldens_reach_their_exact_production_decoders() {
        let unscoped = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_keys/valid-unscoped-operation"
        ));
        assert!(
            ManagedIndexKey::parse_from_slice(DataScope::LegacyUnscoped, &unscoped[1..]).is_ok()
        );

        let tenant = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_keys/valid-tenant-operation"
        ));
        let tenant_scope = DataScope::Tenant(TenantId::from_u128(u128::MAX));
        assert!(ManagedIndexKey::parse_from_slice(tenant_scope, &tenant[1..]).is_ok());

        let bitmap = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_keys/valid-unscoped-v4-bitmap"
        ));
        assert!(ManagedIndexKey::parse_from_slice(DataScope::LegacyUnscoped, &bitmap[1..]).is_ok());
        for corrupt in [
            include_bytes!("../fuzz/corpus/current_index_v2_keys/v4-bitmap-digest-mismatch")
                .as_slice(),
            include_bytes!("../fuzz/corpus/current_index_v2_keys/v4-bitmap-length-mismatch")
                .as_slice(),
        ] {
            let corrupt = hex_seed(corrupt);
            assert!(
                ManagedIndexKey::parse_from_slice(DataScope::LegacyUnscoped, &corrupt[1..])
                    .is_err()
            );
        }

        let global = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_keys/valid-global-storage-version"
        ));
        assert!(GlobalKey::parse_from_slice(&global[1..]).is_ok());

        let index = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_records/valid-index-record"
        ));
        assert!(decode_index_record(&index[1..]).is_ok());
        let operation = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_records/valid-operation-record"
        ));
        decode_operation_record(&operation[1..])
            .expect("checked-in operation record reaches the current decoder");
        let metadata = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_records/valid-storage-version"
        ));
        assert!(decode_metadata_value(&metadata[1..]).is_ok());

        let delta = include_bytes!("../fuzz/corpus/current_index_v2_work/valid-coalesced-delta");
        assert!(typed_work_value_is_valid(&hex_seed(delta)));
    }

    #[test]
    fn checked_in_v4_bitmap_corpus_replays_valid_and_corrupt_boundaries() {
        let valid = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_bitmap/valid-portable-empty"
        ));
        assert!(SecondaryEqualityBitmapValue::decode(&valid[1..]).is_ok());
        let add = hex_seed(include_bytes!(
            "../fuzz/corpus/current_index_v2_bitmap/valid-add-operand"
        ));
        decode_current_index_v2_bitmap(add[0], &add[1..]);
        for seed in [
            include_bytes!("../fuzz/corpus/current_index_v2_bitmap/truncated-portable").as_slice(),
            include_bytes!("../fuzz/corpus/current_index_v2_bitmap/corrupt-roaring").as_slice(),
            include_bytes!("../fuzz/corpus/current_index_v2_bitmap/malformed-operand").as_slice(),
        ] {
            let seed = hex_seed(seed);
            decode_current_index_v2_bitmap(seed[0], &seed[1..]);
        }
    }
}
