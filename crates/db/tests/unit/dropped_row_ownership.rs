//! Contracts of [`owned_row`]: which keys a dropped generation owns, under
//! which prefixes, and that every key encoder of the store agrees with it.
//! They build adversarial keys byte by byte and keys of every kind, legacy
//! ones included, so they live outside the runtime module.

use bytes::{BufMut, Bytes};
use proptest::prelude::*;

use super::*;
use crate::encoding::v2::keys::scope::{TenantId, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey};
use crate::index_lifecycle::{IndexGenerationId, IndexId};

const SCOPES: [DataScope; 2] = [
    DataScope::LegacyUnscoped,
    DataScope::Tenant(TenantId::from_u128(
        0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
    )),
];

fn scoped(scope: DataScope, logical: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    scope.encode_key_prefix(&mut key);
    key.put_slice(logical);
    key
}

/// The owner [`owned_row`] may report for a logical key that opens with
/// `first` and, for a managed-index key, continues with `second`,
/// derived from the typed classification alone.
fn expected_owner(first: u8, second: u8) -> RowOwner {
    let lane = VectorStorageLane::ALL
        .into_iter()
        .find(|lane| lane.scan_prefix().as_ref() == [first]);
    if let Some(lane) = lane {
        return RowOwner::of_vector_lane(lane);
    }
    if first == ScopedKey::key_prefix() {
        return RecordKind::try_from_u8(second).map_or(RowOwner::Never, RowOwner::of_record_kind);
    }
    KeyPrefix::from_u8(first).map_or(RowOwner::Never, RowOwner::of_key_prefix)
}

#[test]
fn an_owner_lists_exactly_the_prefixes_its_rows_are_matched_under() {
    let kinds = (0..=u8::MAX)
        .filter_map(|kind| RecordKind::try_from_u8(kind).ok())
        .filter(|kind| RowOwner::of_record_kind(*kind) == RowOwner::Generation)
        .collect::<Vec<_>>();
    let mut listed = GENERATION_RECORD_KINDS.to_vec();
    listed.sort_by_key(|kind| kind.as_u8());
    assert_eq!(listed, kinds);
    for scope in SCOPES {
        let generation = OwnerKey::Generation {
            scope,
            index_id: 7,
            generation: 9,
        };
        let physical = OwnerKey::PhysicalIndex {
            scope,
            physical_index_id: 11,
        };
        assert_eq!(generation.prefixes().len(), 11);
        assert_eq!(physical.prefixes().len(), 2);
        for owner in [generation, physical] {
            for prefix in owner.prefixes() {
                let row = owned_row(&prefix).expect("a listed prefix names its owner");
                assert_eq!(row.owner, owner);
                assert_eq!(row.prefix, prefix.as_ref());
            }
        }
    }
}

#[test]
fn every_leading_byte_pair_matches_the_typed_classification() {
    let ids = [0u8, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9, 0xAA, 0xBB];
    for scope in SCOPES {
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let mut logical = vec![first, second];
                logical.put_slice(&ids);
                let key = scoped(scope, &logical);
                let matched = owned_row(&key);
                // A tenant envelope inside a tenant envelope, or opening
                // an unscoped key, is not a logical key of this scope.
                if first == TENANT_KEY_PREFIX {
                    if scope == DataScope::LegacyUnscoped {
                        // Parsed as an envelope whose logical key starts
                        // later; covered by the tenant scope iteration.
                        continue;
                    }
                    assert_eq!(matched, None);
                    continue;
                }
                match expected_owner(first, second) {
                    RowOwner::Never => {
                        assert_eq!(matched, None, "{first:#04x} {second:#04x}")
                    }
                    RowOwner::PhysicalIndex => {
                        let row = matched.expect("vector lane rows are owned");
                        assert_eq!(
                            row.owner,
                            OwnerKey::PhysicalIndex {
                                scope,
                                physical_index_id: u64::from_be_bytes([
                                    second, 0, 0, 0, 0, 0, 0, 0
                                ]),
                            }
                        );
                        assert_eq!(row.prefix, &key[..scope.encoded_len() + 9]);
                    }
                    RowOwner::Generation => {
                        let row = matched.expect("generation rows are owned");
                        assert_eq!(
                            row.owner,
                            OwnerKey::Generation {
                                scope,
                                index_id: 7,
                                generation: 9,
                            }
                        );
                        assert_eq!(row.prefix, &key[..scope.encoded_len() + 18]);
                    }
                }
            }
        }
    }
}

#[test]
fn the_allow_list_is_exactly_two_vector_lanes_and_the_generation_record_kinds() {
    let owned_first_bytes = (0..=u8::MAX)
        .filter(|first| {
            (0..=u8::MAX).any(|second| expected_owner(*first, second) != RowOwner::Never)
        })
        .collect::<Vec<_>>();
    assert_eq!(owned_first_bytes, [0x06, 0xF0, 0xF1]);
    let owned_kinds = (0..=u8::MAX)
        .filter(|kind| expected_owner(0x06, *kind) == RowOwner::Generation)
        .collect::<Vec<_>>();
    assert_eq!(
        owned_kinds,
        [0x03, 0x04, 0x05, 0x06, 0x07, 0x09, 0x0C, 0x10, 0x11, 0x12, 0x13]
    );
    assert!((0..=u8::MAX).all(|kind| {
        GlobalKind::try_from_u8(kind).map_or(true, |kind| {
            RowOwner::of_global_kind(kind) == RowOwner::Never
        })
    }));
}

#[test]
fn generation_rows_are_matched_under_the_prefix_row_by_row_cleanup_scans() {
    let index_id = IndexId::new(0x0102_0304_0506_0708).unwrap();
    let generation = IndexGenerationId::new(0x1112_1314_1516_1718).unwrap();
    for scope in SCOPES {
        for kind in (0..=u8::MAX).filter_map(|kind| RecordKind::try_from_u8(kind).ok()) {
            let prefix = ManagedIndexKey::data_prefix(
                scope,
                ScopedKey::generation_prefix(kind, index_id, generation),
            );
            let mut key = prefix.to_vec();
            key.put_slice(b"suffix");
            let matched = owned_row(&key);
            match RowOwner::of_record_kind(kind) {
                RowOwner::Generation => {
                    let row = matched.expect("generation rows are owned");
                    assert_eq!(row.prefix, prefix.as_ref());
                    assert_eq!(
                        row.owner,
                        OwnerKey::Generation {
                            scope,
                            index_id: index_id.get(),
                            generation: generation.get(),
                        }
                    );
                    // The bare prefix names the owner too.
                    assert_eq!(owned_row(&prefix), Some(row));
                }
                RowOwner::Never => assert_eq!(matched, None, "{kind:?}"),
                RowOwner::PhysicalIndex => unreachable!("no record kind names a physical id"),
            }
        }
    }
}

#[test]
fn vector_rows_are_matched_under_their_lane_prefix_and_core_rows_never() {
    let physical_index_id = 0x2122_2324_2526_2728;
    for scope in SCOPES {
        for lane in VectorStorageLane::ALL {
            let prefix = scoped(scope, &lane.prefix_key(physical_index_id).to_bytes());
            let mut key = prefix.clone();
            key.put_slice(&[0x16, 0, 0, 0, 0, 0, 0, 0, 1]);
            match RowOwner::of_vector_lane(lane) {
                RowOwner::PhysicalIndex => {
                    let row = owned_row(&key).expect("lane rows are owned");
                    assert_eq!(row.prefix, prefix.as_slice());
                    assert_eq!(
                        row.owner,
                        OwnerKey::PhysicalIndex {
                            scope,
                            physical_index_id,
                        }
                    );
                }
                RowOwner::Never => assert_eq!(owned_row(&key), None),
                RowOwner::Generation => unreachable!("no lane is keyed by generation"),
            }
        }
    }
}

/// Every scoped key the store can hold, encoded by its own encoder, is
/// matched under its generation's prefix exactly when its kind is owned
/// by a generation: a new key kind, or a change to an encoder, cannot
/// move rows into or out of a dropped generation's reach unseen.
#[test]
fn every_scoped_key_encoder_agrees_with_the_filter() {
    use std::collections::BTreeSet;

    use crate::encoding::v2::keys::wire_fixtures;

    let fixtures = wire_fixtures::scoped_fixtures();
    // Every record kind a generation owns has a fixture, so a kind that
    // becomes owned without one fails here, before its encoder can go
    // unchecked. A kind without a fixture is one no generation owns.
    let encoded = fixtures
        .iter()
        .map(|(_, key)| key.record_kind().as_u8())
        .collect::<BTreeSet<_>>();
    for kind in (0..=u8::MAX).filter_map(|kind| RecordKind::try_from_u8(kind).ok()) {
        assert!(
            encoded.contains(&kind.as_u8()) || RowOwner::of_record_kind(kind) == RowOwner::Never,
            "{kind:?} is owned by a generation and has no encoder fixture"
        );
    }
    let mut owned = BTreeSet::new();
    for scope in SCOPES {
        for (name, key) in fixtures.clone() {
            let kind = key.record_kind();
            let encoded = ManagedIndexKey::Data { scope, kind: key }.to_bytes();
            match RowOwner::of_record_kind(kind) {
                RowOwner::Generation => {
                    let row = owned_row(&encoded)
                        .unwrap_or_else(|| panic!("{name}: a generation-keyed row is owned"));
                    assert_eq!(
                        row.owner,
                        OwnerKey::Generation {
                            scope,
                            index_id: wire_fixtures::index_id().get(),
                            generation: wire_fixtures::generation().get(),
                        },
                        "{name}"
                    );
                    assert_eq!(
                        row.prefix,
                        ManagedIndexKey::data_prefix(
                            scope,
                            ScopedKey::generation_prefix(
                                kind,
                                wire_fixtures::index_id(),
                                wire_fixtures::generation(),
                            ),
                        )
                        .as_ref(),
                        "{name}"
                    );
                    owned.insert(kind.as_u8());
                }
                RowOwner::Never => assert_eq!(owned_row(&encoded), None, "{name}"),
                RowOwner::PhysicalIndex => unreachable!("no record kind names a physical id"),
            }
        }
    }
    // All eleven generation-keyed kinds were encoded and matched.
    assert_eq!(
        owned,
        GENERATION_RECORD_KINDS
            .into_iter()
            .map(RecordKind::as_u8)
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(owned.len(), 11);
}

/// Every vector key the store can hold, encoded by its own encoder, is
/// matched under its physical index's lane prefix exactly when its lane
/// is one compactions may discard, both of them, and never in the core
/// lane.
#[test]
fn every_vector_key_encoder_agrees_with_the_filter() {
    use std::collections::BTreeSet;

    use crate::encoding::v2::keys::indexes::vector::*;
    use crate::encoding::v2::keys::{DataKey, DataKeyKind};
    use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey;

    const ID: u64 = 0x0102_0304_0506_0708;
    const NODE: u64 = 0x2122_2324_2526_2728;
    const OTHER: u64 = 0x3132_3334_3536_3738;
    const ORDER: u64 = 0x1112_1314_1516_1718;
    const LAYER: u16 = 0x4243;
    // No wildcard: a vector key variant added to the enum does not
    // compile until it is given a place here, and a fixture below.
    fn variant(key: &VectorKey) -> usize {
        match key {
            VectorKey::IndexMetadata(_) => 0,
            VectorKey::IndexPrefix(_) => 1,
            VectorKey::TxnGuard(_) => 2,
            VectorKey::Layer0Neighbors(_) => 3,
            VectorKey::VectorPrefix(_) => 4,
            VectorKey::Vector(_) => 5,
            VectorKey::SimHashDirectoryPrefix(_) => 6,
            VectorKey::SimHashDirectory(_) => 7,
            VectorKey::EntryCandidatePrefix(_) => 8,
            VectorKey::EntryCandidateSorted(_) => 9,
            VectorKey::EntryCandidateNode(_) => 10,
            VectorKey::MemoryPrefix(_) => 11,
            VectorKey::L0Prefix(_) => 12,
            VectorKey::UpperNeighbors(_) => 13,
            VectorKey::SimHash(_) => 14,
            VectorKey::UpperVector(_) => 15,
            VectorKey::ReverseEdgePrefix(_) => 16,
            VectorKey::ReverseEdge(_) => 17,
        }
    }
    let keys = [
        VectorKey::IndexMetadata(VectorIndexMetadataKey::new(ID)),
        VectorKey::IndexPrefix(VectorIndexPrefixKey::new(ID)),
        VectorKey::TxnGuard(LegacyVectorTxnGuardKey::new(ID)),
        VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(ID, NODE)),
        VectorKey::VectorPrefix(VectorItemPrefixKey::new(ID)),
        VectorKey::Vector(VectorItemKey::new(ID, ORDER, NODE)),
        VectorKey::SimHashDirectoryPrefix(VectorSimHashDirectoryPrefixKey::new(ID)),
        VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(ID, ORDER, NODE)),
        VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(ID)),
        VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(ID, LAYER, NODE)),
        VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(ID, NODE)),
        VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(ID)),
        VectorKey::L0Prefix(VectorL0PrefixKey::new(ID)),
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(ID, LAYER, NODE)),
        VectorKey::SimHash(VectorSimHashKey::new(ID, NODE)),
        VectorKey::UpperVector(VectorUpperVectorKey::new(ID, NODE)),
        VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(ID, NODE)),
        VectorKey::ReverseEdge(VectorReverseEdgeKey::new(ID, NODE, LAYER, OTHER)),
    ];
    assert_eq!(
        keys.iter().map(variant).collect::<BTreeSet<_>>(),
        (0..18).collect::<BTreeSet<_>>()
    );
    let mut discarded = BTreeSet::new();
    for scope in SCOPES {
        for key in keys {
            assert_eq!(key.index_id(), ID);
            let lane = key.storage_lane();
            let encoded = DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(key),
            }
            .to_bytes();
            match RowOwner::of_vector_lane(lane) {
                RowOwner::PhysicalIndex => {
                    let row = owned_row(&encoded)
                        .unwrap_or_else(|| panic!("{key:?}: a lane row is owned"));
                    assert_eq!(
                        row.owner,
                        OwnerKey::PhysicalIndex {
                            scope,
                            physical_index_id: ID,
                        },
                        "{key:?}"
                    );
                    assert_eq!(
                        row.prefix,
                        scoped(scope, &lane.prefix_key(ID).to_bytes()).as_slice(),
                        "{key:?}"
                    );
                    discarded.insert(lane.scan_prefix().to_vec());
                }
                RowOwner::Never => {
                    assert_eq!(lane, VectorStorageLane::Core, "{key:?}");
                    assert_eq!(owned_row(&encoded), None, "{key:?}");
                }
                RowOwner::Generation => unreachable!("no lane is keyed by generation"),
            }
        }
    }
    // Both lanes compactions may discard, and no other.
    assert_eq!(discarded, BTreeSet::from([vec![0xF0], vec![0xF1]]));
}

#[test]
fn global_rows_and_truncated_keys_are_never_matched() {
    let entry = ManagedIndexKey::Global {
        kind: GlobalKey::DroppedGeneration {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
        },
    }
    .to_bytes();
    assert_eq!(owned_row(&entry), None);
    // Global keys of every kind byte, known or not, with a suffix shaped
    // like an owned row.
    for kind in 0..=u8::MAX {
        let mut key = GLOBAL_SENTINEL.to_vec();
        key.put_u8(kind);
        key.put_slice(&[0xF0; 32]);
        assert_eq!(owned_row(&key), None);
    }
    for scope in SCOPES {
        let hot = scoped(scope, &[0xF0, 0, 0, 0, 0, 0, 0, 0, 1]);
        let generation = scoped(
            scope,
            &[0x06, 0x05, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
        );
        for key in [hot, generation] {
            assert!(owned_row(&key).is_some());
            for len in 0..key.len() {
                assert_eq!(owned_row(&key[..len]), None, "{len} of {}", key.len());
            }
        }
    }
}

proptest! {
    /// No key matches unless it opens, after an optional tenant envelope,
    /// with an owned lane byte or with the managed-index prefix and a
    /// generation-owned record kind; and a match never depends on bytes
    /// past the reported prefix.
    #[test]
    fn only_allow_listed_keys_match(
        tenant in proptest::option::of(any::<u128>()),
        logical in proptest::collection::vec(any::<u8>(), 0..48),
        // Bias towards the interesting leading bytes.
        first in proptest::option::of(prop_oneof![
            Just(0x03u8), Just(0x06), Just(0xF0), Just(0xF1), Just(0xFD), Just(0xFE), Just(0xFF)
        ]),
    ) {
        let mut logical = logical;
        if let (Some(first), Some(slot)) = (first, logical.first_mut()) {
            *slot = first;
        }
        let scope = tenant.map_or(DataScope::LegacyUnscoped, |tenant| {
            DataScope::Tenant(TenantId::from_u128(tenant))
        });
        let key = Bytes::from(scoped(scope, &logical));
        let allowed = match logical.as_slice() {
            [0xF0 | 0xF1, rest @ ..] => rest.len() >= 8,
            [0x06, kind, rest @ ..] => {
                rest.len() >= 16
                    && RecordKind::try_from_u8(*kind)
                        .is_ok_and(|kind| RowOwner::of_record_kind(kind) == RowOwner::Generation)
            }
            _ => false,
        };
        // An unscoped key that itself opens a tenant envelope is a tenant
        // key; its logical part decides, which the tenant case covers.
        prop_assume!(scope != DataScope::LegacyUnscoped || logical.first() != Some(&0xFD));
        let matched = owned_row(&key);
        prop_assert_eq!(matched.is_some(), allowed);
        if let Some(row) = matched {
            prop_assert!(key.starts_with(row.prefix));
            let mut sibling = row.prefix.to_vec();
            sibling.put_slice(b"another suffix");
            prop_assert_eq!(owned_row(&sibling).map(|row| row.owner), Some(row.owner));
        }
    }
}
