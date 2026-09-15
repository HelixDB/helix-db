//! Independent legacy framing and allocation checks for topology operands.
use super::{adjacency, indexes};
use roaring::RoaringTreemap;

// Keep the original framing independent of the production encoder. Round trips
// alone would permit both encoder and decoder to drift to a different format.
fn legacy_bitmap(additions: &RoaringTreemap, removals: &RoaringTreemap) -> Vec<u8> {
    let mut bytes = b"HLXRBM2\0".to_vec();
    for bitmap in [additions, removals] {
        let mut payload = Vec::new();
        bitmap.serialize_into(&mut payload).unwrap();
        bytes.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(&payload);
    }
    bytes
}

fn legacy_adjacency(reset_out: bool, outgoing: &[u8], incoming: &[u8]) -> Vec<u8> {
    let mut bytes = b"HLXADJ2\0".to_vec();
    bytes.push(u8::from(reset_out));
    for payload in [outgoing, incoming] {
        bytes.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(payload);
    }
    bytes
}

fn profiles() -> Vec<RoaringTreemap> {
    let mut profiles = [0, 1, 4096, 4097, 8193]
        .into_iter()
        .map(|count| {
            let mut bitmap = RoaringTreemap::new();
            for id in 0..count {
                bitmap.insert(id);
            }
            bitmap
        })
        .collect::<Vec<_>>();
    let mut sparse = RoaringTreemap::new();
    for id in [0, u64::from(u16::MAX), 1 << 16, 1 << 32, u64::MAX]
        .into_iter()
        .chain((1..65).map(|id| id << 16))
        .chain((1..65).map(|id| id << 32))
    {
        sparse.insert(id);
    }
    profiles.push(sparse);
    profiles
}

#[test]
fn delta_encoding_preparation_and_nested_writes_need_no_framing_allocation() {
    let mut delta = indexes::BitmapMembershipDelta::default();
    delta.add(7);
    delta.remove(u64::MAX);
    let expected = legacy_bitmap(
        &RoaringTreemap::from_iter([7]),
        &RoaringTreemap::from_iter([u64::MAX]),
    );
    let (prepared, allocations) = crate::allocation_testing::observe(|| delta.prepare_encoding());
    assert_eq!(allocations.allocations, 0);
    assert_eq!(prepared.encoded_len(), expected.len());
    let prefix = b"existing prefix";
    let mut destination = Vec::with_capacity(prefix.len() + prepared.encoded_len());
    destination.extend_from_slice(prefix);
    let ((), allocations) =
        crate::allocation_testing::observe(|| prepared.write_into(&mut destination));
    assert_eq!(allocations.allocations, 0);
    assert_eq!(
        destination,
        [prefix.as_slice(), expected.as_slice()].concat()
    );

    let mut insufficient = Vec::new();
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        delta.prepare_encoding().write_into(&mut insufficient);
    }));
    assert!(rejected.is_err());
    assert!(insufficient.is_empty(), "reject before writing any prefix");
    assert_eq!(insufficient.capacity(), 0, "reject before buffer growth");

    let adjacency = adjacency::AdjacencyMembershipDelta::from_directions(
        delta,
        indexes::BitmapMembershipDelta::default(),
    );
    let (prepared, allocations) =
        crate::allocation_testing::observe(|| adjacency.prepare_encoding());
    assert_eq!(allocations.allocations, 0);
    let expected = legacy_adjacency(
        false,
        &expected,
        &legacy_bitmap(&RoaringTreemap::new(), &RoaringTreemap::new()),
    );
    assert_eq!(prepared.encoded_len(), expected.len());
    let (encoded, allocations) = crate::allocation_testing::observe(|| prepared.encode());
    assert_eq!(allocations.allocations, 1);
    assert_eq!(allocations.bytes, expected.len());
    assert_eq!(encoded.as_ref(), expected);
}

#[test]
fn bitmap_delta_encoding_uses_one_exact_buffer_and_preserves_legacy_bytes() {
    for initial in profiles() {
        for remove_all in [false, true] {
            let mut delta = indexes::BitmapMembershipDelta::from_additions(initial.clone());
            let mut additions = initial.clone();
            let mut removals = RoaringTreemap::new();
            for id in initial.iter().filter(|id| remove_all || id % 2 == 1) {
                delta.remove(id);
                additions.remove(id);
                removals.insert(id);
            }
            let expected = legacy_bitmap(&additions, &removals);
            let (encoded, allocations) = crate::allocation_testing::observe(|| delta.encode());
            assert_eq!(encoded.as_ref(), expected);
            assert_eq!(
                allocations.allocations,
                1,
                "{} encoded bytes requested {} allocation bytes",
                encoded.len(),
                allocations.bytes
            );
            assert_eq!(allocations.bytes, encoded.len());
            assert_eq!(
                indexes::BitmapMembershipDelta::decode_if_delta(&encoded).unwrap(),
                Some(delta)
            );
        }
    }
}

#[test]
fn adjacency_delta_encoding_uses_one_exact_buffer_and_preserves_legacy_bytes() {
    for initial in profiles() {
        for reset_out in [false, true] {
            let mut outgoing = indexes::BitmapMembershipDelta::from_additions(initial.clone());
            let mut out_additions = initial.clone();
            let mut out_removals = RoaringTreemap::new();
            for id in initial.iter().filter(|id| id % 2 == 1) {
                outgoing.remove(id);
                out_additions.remove(id);
                out_removals.insert(id);
            }
            let mut incoming = indexes::BitmapMembershipDelta::default();
            let mut in_additions = RoaringTreemap::new();
            let mut in_removals = RoaringTreemap::new();
            if !initial.is_empty() {
                for id in [17, 1 << 48, u64::MAX] {
                    incoming.add(id);
                    in_additions.insert(id);
                }
                incoming.remove(19);
                in_removals.insert(19);
            }
            let mut delta =
                adjacency::AdjacencyMembershipDelta::from_directions(outgoing, incoming);
            if reset_out {
                let mut newer = adjacency::AdjacencyMembershipDelta::default();
                newer.reset_out();
                newer.add_out(u64::MAX);
                newer.remove_in(17);
                delta.compose(&newer);
                out_additions.clear();
                out_additions.insert(u64::MAX);
                out_removals.clear();
                in_additions.remove(17);
                in_removals.insert(17);
            }
            let expected = legacy_adjacency(
                reset_out,
                &legacy_bitmap(&out_additions, &out_removals),
                &legacy_bitmap(&in_additions, &in_removals),
            );
            let (encoded, allocations) = crate::allocation_testing::observe(|| delta.encode());
            assert_eq!(encoded.as_ref(), expected);
            assert_eq!(
                allocations.allocations,
                1,
                "{} encoded bytes requested {} allocation bytes",
                encoded.len(),
                allocations.bytes
            );
            assert_eq!(allocations.bytes, encoded.len());
            assert_eq!(
                adjacency::AdjacencyMembershipDelta::decode_if_delta(&encoded).unwrap(),
                Some(delta)
            );
        }
    }
}

#[test]
fn delta_encoding_preserves_optimized_run_containers() {
    let mut optimized = false;
    for initial in profiles().into_iter().skip(2) {
        let initial = RoaringTreemap::from_bitmaps(initial.bitmaps().map(|(partition, bitmap)| {
            let mut bitmap = bitmap.clone();
            optimized |= bitmap.optimize();
            (partition, bitmap)
        }));
        let bitmap = indexes::BitmapMembershipDelta::from_additions(initial.clone());
        let expected_bitmap = legacy_bitmap(&initial, &RoaringTreemap::new());
        assert_eq!(bitmap.encode().as_ref(), expected_bitmap);
        let delta = adjacency::AdjacencyMembershipDelta::from_directions(
            bitmap,
            indexes::BitmapMembershipDelta::default(),
        );
        let expected = legacy_adjacency(
            false,
            &expected_bitmap,
            &legacy_bitmap(&RoaringTreemap::new(), &RoaringTreemap::new()),
        );
        // Roaring itself may allocate a run-container header bitmap. This test
        // pins compatibility without treating that scratch as a framing buffer.
        assert_eq!(delta.encode().as_ref(), expected);
        assert_eq!(
            adjacency::AdjacencyMembershipDelta::decode_if_delta(&expected).unwrap(),
            Some(delta)
        );
    }
    assert!(optimized, "the fixture must exercise optimized containers");
}
