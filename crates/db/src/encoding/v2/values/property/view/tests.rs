use std::collections::BTreeMap;

use proptest::{collection, prelude::*};

use super::*;
use crate::encoding::v2::values::property::{decode_properties, encode_properties};

/// Places `row` at `offset` bytes past a 64-byte boundary, so offsets cover
/// every alignment the validator can observe.
fn placed(row: &[u8], offset: usize) -> Bytes {
    let mut buffer = AlignedVec::<64>::with_capacity(offset + row.len());
    buffer.extend_from_slice(&vec![0; offset]);
    buffer.extend_from_slice(row);
    Bytes::from_owner(buffer).slice(offset..)
}

/// Error text with the validator's absolute buffer addresses masked, so
/// errors from different copies of the same bytes compare equal.
pub(crate) fn masked(error: &str) -> String {
    let mut masked = String::with_capacity(error.len());
    let mut rest = error;
    while let Some(start) = rest.find("0x") {
        masked.push_str(&rest[..start]);
        masked.push_str("0x?");
        rest = rest[start + 2..].trim_start_matches(|c: char| c.is_ascii_hexdigit());
    }
    masked.push_str(rest);
    masked
}

fn same(left: &[Property], right: &[Property]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.same_v1_representation(right))
}

fn value() -> impl Strategy<Value = PropertyValue> {
    let leaf = prop_oneof![
        Just(PropertyValue::Null),
        any::<bool>().prop_map(PropertyValue::Bool),
        any::<i64>().prop_map(PropertyValue::I64),
        any::<i64>().prop_map(PropertyValue::DateTime),
        proptest::num::f64::ANY.prop_map(PropertyValue::F64),
        proptest::num::f32::ANY.prop_map(|value| PropertyValue::F32(f64::from(value))),
        ".{0,12}".prop_map(PropertyValue::String),
        collection::vec(any::<u8>(), 0..40).prop_map(PropertyValue::Bytes),
        collection::vec(any::<i64>(), 0..8).prop_map(PropertyValue::I64Array),
        collection::vec(proptest::num::f64::ANY, 0..8).prop_map(PropertyValue::F64Array),
        collection::vec(proptest::num::f32::ANY, 0..64).prop_map(PropertyValue::F32Array),
        collection::vec(".{0,6}", 0..4).prop_map(PropertyValue::StringArray),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            collection::vec(inner.clone(), 0..4).prop_map(PropertyValue::Array),
            collection::btree_map("[a-c.]{0,3}", inner, 0..4).prop_map(PropertyValue::Object),
        ]
    })
}

/// Rows over a tiny name alphabet, so duplicate names are common.
fn properties() -> impl Strategy<Value = Vec<Property>> {
    collection::vec(
        ("[a-c$.]{0,2}", value()).prop_map(|(name, value)| Property::new(name, value)),
        0..12,
    )
}

fn keep_from(mask: u8) -> impl Fn(&str) -> bool {
    move |name: &str| {
        name.bytes()
            .next()
            .map_or(mask & 1 != 0, |first| mask & (1 << (first % 7 + 1)) != 0)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn in_place_reads_match_the_full_decoder(
        properties in properties(),
        offset in 0_usize..32,
        mask in any::<u8>(),
    ) {
        let encoded = encode_properties(&properties);
        let data = placed(&encoded, offset);
        let full = decode_properties(&data).unwrap();
        prop_assert!(same(&full, &properties));

        let keep = keep_from(mask);
        let mut scratch = Scratch::new();
        let mut expected = full.clone();
        expected.retain(|property| keep(&property.name));
        prop_assert!(same(&decode_selected(&data, &mut scratch, &keep).unwrap(), &expected));

        let row = Row::new(data.clone(), &mut Buffers::default()).unwrap();
        prop_assert_eq!(
            matches!(row.0, Storage::Shared(_)),
            !data.is_empty() && data.as_ptr().align_offset(PROPERTY_ALIGNMENT) == 0
        );
        prop_assert!(same(&row.decode().unwrap(), &full));
        for name in full
            .iter()
            .map(|property| property.name.as_str())
            .chain(["missing", ""])
        {
            let first = full.iter().find(|property| property.name == name);
            match (row.value(name).unwrap(), first) {
                (Some(actual), Some(first)) => {
                    prop_assert!(actual.same_v1_representation(&first.value));
                }
                (None, None) => {}
                (actual, first) => prop_assert!(false, "{name}: {actual:?} vs {first:?}"),
            }
        }
    }

    #[test]
    fn corrupt_rows_fail_exactly_when_the_full_decoder_fails(
        properties in properties(),
        offset in 0_usize..32,
        corruption in prop_oneof![
            (any::<usize>(), 1_u8..).prop_map(|(at, mask)| (Some(at), mask, None)),
            any::<usize>().prop_map(|length| (None, 0, Some(length))),
        ],
        noise in collection::vec(any::<u8>(), 0..64),
    ) {
        let mut row = encode_properties(&properties).to_vec();
        let (flip, mask, truncate) = corruption;
        if let Some(at) = flip.filter(|_| !row.is_empty()) {
            let at = at % row.len();
            row[at] ^= mask;
        }
        if let Some(length) = truncate {
            row.truncate(length % (row.len() + 1));
        }
        for candidate in [row, noise] {
            let data = placed(&candidate, offset);
            let expected = decode_properties(&data);
            let selected = decode_selected(&data, &mut Scratch::new(), |_| true);
            let read = Row::new(data.clone(), &mut Buffers::default()).and_then(|row| row.decode());
            for actual in [selected, read] {
                match (&expected, actual) {
                    (Ok(expected), Ok(actual)) => prop_assert!(same(&actual, expected)),
                    (Err(expected), Err(actual)) => {
                        prop_assert!(matches!(actual, EncodingError::Rkyv(_)));
                        prop_assert_eq!(masked(&actual.to_string()), masked(&expected.to_string()));
                    }
                    (expected, actual) => {
                        prop_assert!(false, "{expected:?} vs {actual:?}");
                    }
                }
            }
        }
    }
}

#[test]
fn empty_rows_are_empty_without_validation() {
    let mut scratch = Scratch::new();
    assert!(access(&[], &mut scratch).unwrap().is_empty());
    assert!(decode_selected(&[], &mut scratch, |_| true)
        .unwrap()
        .is_empty());
    let row = Row::new(Bytes::new(), &mut Buffers::default()).unwrap();
    assert!(matches!(row.0, Storage::Empty));
    assert!(row.properties().is_empty());
    assert_eq!(row.value("anything").unwrap(), None);
    assert_eq!(row.decode().unwrap(), Vec::new());
    assert_eq!(scratch.capacity(), 0);
}

#[test]
fn large_rows_read_identically_at_every_offset() {
    let properties = vec![
        Property::string("$label", "Document"),
        Property::f32_array("embedding", (0..1536).map(|value| value as f32).collect()),
        Property::bytes("blob", vec![7; 70_000]),
        Property::new(
            "meta",
            PropertyValue::Object(BTreeMap::from([(
                "deep".to_string(),
                PropertyValue::F64Array(vec![f64::NAN; 512]),
            )])),
        ),
    ];
    let encoded = encode_properties(&properties);
    let mut scratch = Scratch::new();
    for offset in 0..PROPERTY_ALIGNMENT * 2 {
        let data = placed(&encoded, offset);
        assert!(same(
            &decode_selected(&data, &mut scratch, |name| name == "embedding").unwrap(),
            &properties[1..2]
        ));
        let row = Row::new(data, &mut Buffers::default()).unwrap();
        assert!(same(&row.decode().unwrap(), &properties));
        assert_eq!(
            row.value("embedding").unwrap(),
            Some(properties[1].value.clone())
        );
    }
}

#[test]
fn unaligned_rows_reuse_released_copies_and_aligned_rows_never_copy() {
    let encoded = encode_properties(&[Property::string("name", "ada")]);
    let unaligned = placed(&encoded, 1);
    let aligned = placed(&encoded, 0);
    let mut buffers = Buffers::default();

    let row = Row::new(aligned.clone(), &mut buffers).unwrap();
    assert!(matches!(&row.0, Storage::Shared(bytes) if bytes.as_ptr() == aligned.as_ptr()));
    row.recycle(&mut buffers);
    assert!(buffers.0.is_empty());

    let row = Row::new(unaligned.clone(), &mut buffers).unwrap();
    let Storage::Copied(copy) = &row.0 else {
        panic!("an unaligned row is copied");
    };
    let address = copy.as_ptr();
    assert_eq!(copy.as_slice(), unaligned.as_ref());
    row.recycle(&mut buffers);
    assert_eq!(buffers.0.len(), 1);

    let row = Row::new(unaligned.clone(), &mut buffers).unwrap();
    assert!(buffers.0.is_empty());
    assert!(matches!(&row.0, Storage::Copied(copy) if copy.as_ptr() == address));
    assert_eq!(
        row.value("name").unwrap(),
        Some(PropertyValue::String("ada".into()))
    );
    row.recycle(&mut buffers);

    // A rejected row drops the copy it validated.
    let corrupt = placed(b"corrupt", 1);
    assert!(Row::new(corrupt.clone(), &mut buffers).is_err());
    assert!(buffers.0.is_empty());
    assert!(decode_properties(&corrupt).is_err());

    // The access scratch is only written for unaligned rows.
    let mut scratch = Scratch::new();
    access(&aligned, &mut scratch).unwrap();
    assert_eq!(scratch.capacity(), 0);
    access(&unaligned, &mut scratch).unwrap();
    assert_eq!(scratch.as_slice(), unaligned.as_ref());
}
