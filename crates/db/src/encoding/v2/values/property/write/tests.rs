use super::*;
use crate::{allocation_testing, encoding::v2::values::property};
use std::collections::BTreeMap;

#[test]
fn prepared_write_preserves_every_native_variant_and_bounds_clone_and_scratch() {
    let values = [
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::I64(i64::MIN),
        PropertyValue::DateTime(i64::MAX),
        PropertyValue::F64(-0.0),
        PropertyValue::F32(f64::from_bits(0x7ff8_0000_0000_0017)),
        PropertyValue::String("文字".repeat(500)),
        PropertyValue::Bytes(vec![255; 65536]),
        PropertyValue::I64Array(vec![1, 2]),
        PropertyValue::F64Array(vec![f64::NAN, -0.0]),
        PropertyValue::F32Array(vec![f32::INFINITY, f32::NAN]),
        PropertyValue::StringArray(vec!["s".repeat(100), String::new()]),
        PropertyValue::Array(vec![PropertyValue::Object(BTreeMap::new())]),
        PropertyValue::Object(BTreeMap::new()),
    ];
    let mut properties: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(i, value)| Property::new(format!("key{i}"), value))
        .collect();
    for width in [0, 1, 5, 6, 30, 31, 216, 1024] {
        properties.push(Property::new(
            "map",
            PropertyValue::Object(
                (0..width)
                    .map(|i| {
                        (
                            format!("k{i}"),
                            PropertyValue::StringArray(vec!["v".repeat(i % 33)]),
                        )
                    })
                    .collect(),
            ),
        ));
        let (prepared, allocation) = allocation_testing::observe(|| Prepared::new(&properties));
        assert_eq!(allocation.allocations, 0);
        let prepared = prepared.unwrap();
        let (cloned, allocation) = allocation_testing::observe(|| properties.clone());
        assert!(
            allocation.bytes <= prepared.clone_bytes(),
            "{} > {}",
            allocation.bytes,
            prepared.clone_bytes()
        );
        assert!(prepared.retained_bytes(properties.capacity()) >= prepared.clone_bytes());
        let mut scratch = vec![MaybeUninit::uninit(); prepared.scratch_bytes()];
        let len = prepared.encoded_len(&mut scratch).unwrap();
        let encoded = prepared.encode(Vec::new(), &mut scratch).unwrap();
        assert_eq!(len, encoded.len());
        assert_eq!(encoded, property::encode_properties(&properties));
        assert!(property::decode_properties(&encoded)
            .unwrap()
            .iter()
            .zip(cloned)
            .all(|(left, right)| left.same_v1_representation(&right)));
        properties.pop();
    }
    let prepared = Prepared::new(&[]).unwrap();
    assert_eq!(prepared.clone_bytes(), 0);
    assert_eq!(prepared.scratch_bytes(), 0);
    assert!(prepared.encode(Vec::new(), &mut []).unwrap().is_empty());
}

#[test]
fn write_depth_is_checked_without_allocation_before_recursive_serialization() {
    for maps in [false, true] {
        for depth in [
            0,
            MAX_WRITE_DEPTH - 1,
            MAX_WRITE_DEPTH,
            MAX_WRITE_DEPTH + 100,
        ] {
            let value = (0..depth).fold(PropertyValue::I64(1), |child, _| {
                if maps {
                    PropertyValue::Object(BTreeMap::from([("k".into(), child)]))
                } else {
                    PropertyValue::Array(vec![child])
                }
            });
            let properties = vec![Property::new("nested", value)];
            let (prepared, allocation) = allocation_testing::observe(|| Prepared::new(&properties));
            assert_eq!(allocation.allocations, 0);
            if depth < MAX_WRITE_DEPTH {
                let prepared = prepared.unwrap();
                let mut scratch = vec![MaybeUninit::uninit(); prepared.scratch_bytes()];
                assert_eq!(
                    prepared.encode(Vec::new(), &mut scratch).unwrap(),
                    property::encode_properties(&properties)
                );
            } else {
                assert!(matches!(prepared, Err(EncodingError::PropertyNestingLimit)));
            }
        }
    }
}

#[test]
fn retained_write_preflight_counts_spare_capacity_and_pruned_maps() {
    let mut string = String::with_capacity(8192);
    string.push('s');
    let mut array = Vec::with_capacity(1024);
    array.push(string);
    let mut map = BTreeMap::from([("gone".into(), PropertyValue::I64(1))]);
    map.remove("gone");
    let mut properties = Vec::with_capacity(128);
    properties.push(Property::new("spare", PropertyValue::StringArray(array)));
    properties.push(Property::new("empty", PropertyValue::Object(map)));
    let prepared = Prepared::new(&properties).unwrap();
    assert!(
        prepared.retained_bytes(properties.capacity())
            >= 8192 + 1024 * size_of::<String>() + 128 * size_of::<Property>()
    );
    let (_, allocation) = allocation_testing::observe(|| properties.clone());
    assert!(allocation.bytes <= prepared.clone_bytes());
    assert!(prepared.encode(Vec::new(), &mut []).is_err());
    assert_eq!(scratch::<u8>(usize::MAX), 0);
    assert_eq!(scratch::<Property>(usize::MAX), usize::MAX);
    use rkyv::ser::Writer;
    let mut measure = Measure(usize::MAX);
    let (result, allocation) = allocation_testing::observe(|| measure.write(&[1]));
    assert!(result.is_err());
    assert_eq!(allocation.allocations, 0);
}
