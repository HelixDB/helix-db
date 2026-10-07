use super::*;
use crate::allocation_testing;

#[test]
fn nesting_preflight_matches_common_values_including_implicit_typed_array_elements() {
    for base in [
        P::I64(1),
        P::Array(vec![]),
        P::Object(BTreeMap::new()),
        P::I64Array(vec![]),
        P::I64Array(vec![1]),
        P::F64Array(vec![1.0]),
        P::F32Array(vec![1.0]),
        P::StringArray(vec!["x".into()]),
    ] {
        for depth in [
            r::MAX_EXPRESSION_DEPTH - 2,
            r::MAX_EXPRESSION_DEPTH - 1,
            r::MAX_EXPRESSION_DEPTH,
            96,
        ] {
            let extra_element = matches!(&base, P::I64Array(xs) if !xs.is_empty())
                || matches!(&base, P::F64Array(xs) if !xs.is_empty())
                || matches!(&base, P::F32Array(xs) if !xs.is_empty())
                || matches!(&base, P::StringArray(xs) if !xs.is_empty());
            let input = (0..depth).fold(base.clone(), |value, _| P::Array(vec![value]));
            let (conversion, preflight) = allocation_testing::observe(|| Conversion::new(input));
            assert_eq!(preflight.allocations, 0);
            let bound = conversion.owned_bytes();
            let (result, allocations) = allocation_testing::observe(|| conversion.finish());
            if depth + usize::from(extra_element) < r::MAX_EXPRESSION_DEPTH {
                result.unwrap().validate_shape().unwrap();
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.category, "ResourceLimit");
                assert_eq!(error.detail, "StoredValueNestingLimit");
                assert_eq!(allocations.allocations, 3, "no partial output containers");
                assert_eq!(allocations.bytes, bound);
            }
        }
    }
}

#[test]
fn preflight_bounds_allocations_and_retained_capacity_for_every_supported_shape() {
    let mut spare = String::with_capacity(8192);
    spare.push_str("short");
    let mut spare_array = Vec::with_capacity(8192);
    spare_array.push(P::String(spare));
    let mut inputs = vec![
        P::Null,
        P::Bool(true),
        P::I64(i64::MIN),
        P::I64(i64::MAX),
        P::F64(-0.0),
        P::F32(f64::INFINITY),
        P::String(String::new()),
        P::String("文字🪄".repeat(1024)),
        P::Array(spare_array),
        P::Object(BTreeMap::new()),
        P::Array(vec![]),
        P::I64Array(vec![]),
        P::F64Array(vec![]),
        P::F32Array(vec![]),
        P::StringArray(vec![]),
    ];
    for length in [1, 2, 7, 11, 12, 13, 127, 512, 4097] {
        inputs.extend([
            P::I64Array((0..length as i64).collect()),
            P::F64Array(vec![1.5; length]),
            P::F32Array(vec![-1.5; length]),
            P::StringArray((0..length).map(|i| "x".repeat(i % 17)).collect()),
            P::Array((0..length).map(|i| P::Bool(i % 2 == 0)).collect()),
            P::Object(
                (0..length)
                    .map(|i| (format!("key{i:04}"), P::Array(vec![P::I64(i as i64)])))
                    .collect(),
            ),
        ]);
    }
    let nested = (0..r::MAX_EXPRESSION_DEPTH - 1).fold(P::I64(1), |value, i| {
        if i % 2 == 0 {
            P::Array(vec![value])
        } else {
            P::Object(BTreeMap::from([("child".into(), value)]))
        }
    });
    inputs.push(nested);
    for input in inputs {
        let (conversion, preflight) = allocation_testing::observe(|| Conversion::new(input));
        assert_eq!(preflight.allocations, 0);
        let bound = conversion.owned_bytes();
        let (output, allocations) = allocation_testing::observe(|| conversion.finish());
        let output = output.unwrap();
        assert!(
            allocations.bytes <= bound,
            "allocation {} > {bound}",
            allocations.bytes
        );
        let retained = output.allocated_bytes() - size_of::<r::Value>();
        assert!(retained <= bound, "retained {retained} > {bound}");
    }
}

#[test]
fn conversion_preserves_lossless_numeric_bits_and_moves_string_payloads() {
    for number in [
        f64::NEG_INFINITY,
        -0.0,
        f64::INFINITY,
        f64::from_bits(0x7ff8_0000_0000_0001),
    ] {
        for input in [P::F64(number), P::F32(number)] {
            let r::Value::Float(actual) = Conversion::new(input).finish().unwrap() else {
                panic!("stored floating point value");
            };
            assert_eq!(actual.to_bits(), number.to_bits());
        }
        let r::Value::List(actual) = Conversion::new(P::F64Array(vec![number])).finish().unwrap()
        else {
            panic!("typed float array");
        };
        let r::Value::Float(actual) = actual[0] else {
            panic!("float element")
        };
        assert_eq!(actual.to_bits(), number.to_bits());
    }
    for number in [f32::NEG_INFINITY, -0.0, f32::INFINITY, f32::NAN] {
        let r::Value::List(actual) = Conversion::new(P::F32Array(vec![number])).finish().unwrap()
        else {
            panic!("typed float array");
        };
        let r::Value::Float(actual) = actual[0] else {
            panic!("float element")
        };
        assert_eq!(actual.to_bits(), f64::from(number).to_bits());
    }
    let mut input = String::with_capacity(4096);
    input.push_str("kept");
    let pointer = input.as_ptr();
    let conversion = Conversion::new(P::String(input));
    assert_eq!(conversion.owned_bytes(), 4096);
    let (output, allocations) = allocation_testing::observe(|| conversion.finish());
    let r::Value::String(output) = output.unwrap() else {
        panic!("string")
    };
    assert_eq!(output.as_ptr(), pointer);
    assert_eq!(output.capacity(), 4096);
    assert_eq!(allocations.allocations, 0);
}

#[test]
fn unsupported_subtrees_create_only_one_error_without_partial_containers() {
    for unsupported in [P::DateTime(1), P::Bytes(vec![7; 8192])] {
        for input in [
            unsupported.clone(),
            P::Array(vec![unsupported.clone(), P::I64Array(vec![1; 8192])]),
            P::Array(vec![P::I64Array(vec![1; 8192]), unsupported.clone()]),
            P::Object(BTreeMap::from([
                ("first".into(), P::I64Array(vec![1; 8192])),
                ("last".into(), P::Array(vec![unsupported.clone()])),
            ])),
        ] {
            let (conversion, preflight) = allocation_testing::observe(|| Conversion::new(input));
            assert_eq!(preflight.allocations, 0);
            let bound = conversion.owned_bytes();
            let (result, allocations) = allocation_testing::observe(|| conversion.finish());
            let error = result.unwrap_err();
            assert_eq!(error.category, "UnsupportedFeature");
            assert_eq!(error.detail, "StoredValueType");
            assert_eq!(error.phase, r::ErrorPhase::Runtime);
            assert_eq!(allocations.allocations, 3);
            assert_eq!(allocations.bytes, bound);
        }
    }
}
