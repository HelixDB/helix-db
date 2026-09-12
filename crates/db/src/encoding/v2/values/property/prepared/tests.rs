use super::*;
use crate::{allocation_testing, encoding::v2::values::property};
use std::collections::BTreeMap;

#[test]
fn prepared_selection_bounds_actual_allocations_for_every_stored_variant() {
    let values = vec![
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::I64(i64::MIN),
        PropertyValue::DateTime(i64::MAX),
        PropertyValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
        PropertyValue::F32(-0.0),
        PropertyValue::String("文字🪄".repeat(64)),
        PropertyValue::Bytes(vec![0, 1, 255]),
        PropertyValue::I64Array(vec![i64::MIN, i64::MAX]),
        PropertyValue::F64Array(vec![f64::NEG_INFINITY, -0.0, f64::NAN]),
        PropertyValue::F32Array(vec![f32::INFINITY, f32::NAN]),
        PropertyValue::StringArray(vec![String::new(), "text".repeat(128)]),
        PropertyValue::Array(vec![
            PropertyValue::Null,
            PropertyValue::Object(BTreeMap::from([(
                "nested".into(),
                PropertyValue::String("value".into()),
            )])),
        ]),
        PropertyValue::Object(BTreeMap::new()),
        PropertyValue::Object(
            (0..512)
                .map(|i| {
                    (
                        format!("key{i:04}"),
                        PropertyValue::String("x".repeat(i % 32)),
                    )
                })
                .collect(),
        ),
        PropertyValue::String(String::new()),
        PropertyValue::Array(vec![]),
    ];
    let properties = values
        .into_iter()
        .enumerate()
        .map(|(i, value)| Property::new(format!("key{i}"), value))
        .collect::<Vec<_>>();
    let encoded = property::encode_properties(&properties);
    let names = ["key0", "key6", "key6", "absent"];
    let keys = BTreeSet::from(["key12".into(), "key14".into()]);
    for selection in [
        Selection::All,
        Selection::Names(&names),
        Selection::Keys {
            names: &keys,
            required: "key3",
        },
    ] {
        let (archive, alignment) = allocation_testing::observe(|| Archive::new(&encoded));
        assert_eq!(alignment.allocations, 1);
        assert_eq!(alignment.bytes, encoded.len());
        let (prepared, preflight) = allocation_testing::observe(|| archive.prepare(selection));
        let prepared = prepared.unwrap();
        assert_eq!(
            preflight.allocations, 0,
            "validation and preflight borrow every subtree"
        );
        let bound = prepared.owned_bytes();
        let (decoded, allocation) = allocation_testing::observe(|| prepared.decode());
        let decoded = decoded.unwrap();
        assert!(
            allocation.bytes <= bound,
            "allocated {} beyond {bound}",
            allocation.bytes
        );
        let expected = properties
            .iter()
            .filter(|p| selection.contains(&p.name))
            .collect::<Vec<_>>();
        assert_eq!(decoded.len(), expected.len());
        assert_eq!(decoded.capacity(), decoded.len());
        assert!(decoded
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual.same_v1_representation(expected)));
    }
    let archive = Archive::new(&encoded);
    let all = archive.prepare(Selection::All).unwrap().owned_bytes();
    let one = archive
        .prepare(Selection::Names(&["key0"]))
        .unwrap()
        .owned_bytes();
    assert!(
        one * 100 < all,
        "unselected map/string payloads must not be decoded"
    );
}

#[test]
fn empty_missing_unaligned_and_malformed_archives_keep_distinct_contracts() {
    let (archive, allocations) = allocation_testing::observe(|| Archive::new(&[]));
    assert_eq!(allocations.allocations, 0);
    let empty = archive.prepare(Selection::All).unwrap();
    assert_eq!(empty.owned_bytes(), 0);
    let (decoded, allocations) = allocation_testing::observe(|| empty.decode());
    assert!(decoded.unwrap().is_empty());
    assert_eq!(allocations.allocations, 0);
    let properties = vec![Property::string("key", "x".repeat(32))];
    let encoded = property::encode_properties(&properties);
    for offset in 0..16 {
        let mut bytes = vec![0; offset];
        bytes.extend_from_slice(&encoded);
        let archive = Archive::new(&bytes[offset..offset + encoded.len()]);
        assert_eq!(
            archive.prepare(Selection::All).unwrap().decode().unwrap(),
            properties
        );
        let missing = archive.prepare(Selection::Names(&["missing"])).unwrap();
        assert_eq!(missing.owned_bytes(), 0);
        let (decoded, allocation) = allocation_testing::observe(|| missing.decode());
        assert!(decoded.unwrap().is_empty());
        assert_eq!(allocation.allocations, 0);
    }
    for bytes in [vec![1, 2, 3], encoded[..encoded.len() - 1].to_vec(), {
        let mut bytes = encoded.to_vec();
        bytes[0] = 255;
        bytes
    }] {
        assert!(matches!(
            Archive::new(&bytes).prepare(Selection::All),
            Err(EncodingError::Rkyv(_))
        ));
    }
}

#[test]
fn structural_nesting_is_bounded_before_preflight_and_owned_decode() {
    for depth in [0, 48, validation::MAX_ARCHIVE_DEPTH + 1] {
        let mut value = PropertyValue::I64(1);
        for _ in 0..depth {
            value = PropertyValue::Array(vec![value]);
        }
        let properties = vec![Property::new("nested", value)];
        let encoded = property::encode_properties(&properties);
        let archive = Archive::new(&encoded);
        let (prepared, allocation) =
            allocation_testing::observe(|| archive.prepare(Selection::All));
        assert_eq!(
            allocation.allocations, 0,
            "even depth rejection must not build a recursive error trace"
        );
        match prepared {
            Ok(prepared) => {
                assert!(depth <= 48);
                assert_eq!(prepared.decode().unwrap(), properties);
            }
            Err(EncodingError::PropertyNestingLimit) => {
                assert!(depth > validation::MAX_ARCHIVE_DEPTH)
            }
            Err(error) => panic!("unexpected validation error: {error}"),
        }
    }
}

#[test]
fn deepest_accepted_arrays_and_maps_decode_on_the_standard_test_stack() {
    for object in [false, true] {
        let mut last_accepted = 0;
        let mut first_rejected = None;
        for depth in [48, 96, 128, 192, 253, 254, 255, 256] {
            let value = (0..depth).fold(PropertyValue::I64(1), |value, _| {
                if object {
                    PropertyValue::Object(BTreeMap::from([("nested".into(), value)]))
                } else {
                    PropertyValue::Array(vec![value])
                }
            });
            let properties = vec![Property::new("root", value)];
            let encoded = property::encode_properties(&properties);
            let archive = Archive::new(&encoded);
            match archive.prepare(Selection::All) {
                Ok(prepared) => {
                    assert!(first_rejected.is_none(), "depth acceptance is monotonic");
                    last_accepted = depth;
                    let bound = prepared.owned_bytes();
                    let (decoded, allocations) = allocation_testing::observe(|| prepared.decode());
                    assert!(allocations.bytes <= bound);
                    assert_eq!(decoded.unwrap(), properties);
                }
                Err(EncodingError::PropertyNestingLimit) => {
                    first_rejected.get_or_insert(depth);
                }
                Err(error) => panic!("unexpected archive validation: {error}"),
            }
        }
        assert!(last_accepted >= 96);
        assert!(first_rejected.is_some());
    }
}
