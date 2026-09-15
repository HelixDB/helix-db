use std::cell::Cell;

use super::*;

#[test]
fn large_literal_domains_have_bounded_comparison_work() {
    let count = 4096_usize;
    let comparisons = Cell::new(0_usize);
    let result = dedup_by(
        (0..count).rev().collect(),
        |left, right| {
            comparisons.set(comparisons.get() + 1);
            left.cmp(right)
        },
        |left, right| {
            comparisons.set(comparisons.get() + 1);
            left == right
        },
    );
    assert_eq!(result, (0..count).rev().collect::<Vec<_>>());
    eprintln!(
        "literal domain: {count} inputs, {} comparisons",
        comparisons.get()
    );
    assert!(
        comparisons.get() <= 4 * count * count.ilog2() as usize,
        "{} comparisons for {count} literals exceed the n log n construction bound",
        comparisons.get()
    );
}

#[test]
fn literal_order_is_total_and_preserves_typed_nested_identity() {
    let values = vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::Bool(true),
        PropertyValue::I64(i64::MIN),
        PropertyValue::I64(0),
        PropertyValue::I64(1),
        PropertyValue::I64(9_007_199_254_740_993),
        PropertyValue::I64(i64::MAX),
        PropertyValue::DateTime(0),
        PropertyValue::DateTime(1),
        PropertyValue::F64(f64::NEG_INFINITY),
        PropertyValue::F64(-0.0),
        PropertyValue::F64(0.0),
        PropertyValue::F64(1.0),
        PropertyValue::F64(1.5),
        PropertyValue::F64(9_007_199_254_740_992.0),
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::F64(f64::NAN),
        PropertyValue::F64(-f64::NAN),
        PropertyValue::F32(-0.0),
        PropertyValue::F32(0.0),
        PropertyValue::F32(1.0),
        PropertyValue::F32(1.5),
        PropertyValue::F32(f32::NAN),
        PropertyValue::String(String::new()),
        PropertyValue::String("κλειδί".into()),
        PropertyValue::Bytes(vec![]),
        PropertyValue::Bytes(vec![0, 255]),
        PropertyValue::I64Array(vec![]),
        PropertyValue::I64Array(vec![1]),
        PropertyValue::F64Array(vec![]),
        PropertyValue::F64Array(vec![-0.0]),
        PropertyValue::F64Array(vec![0.0]),
        PropertyValue::F64Array(vec![0.0, 1.0]),
        PropertyValue::F64Array(vec![1.0]),
        PropertyValue::F64Array(vec![f64::NAN]),
        PropertyValue::F32Array(vec![]),
        PropertyValue::F32Array(vec![-0.0]),
        PropertyValue::F32Array(vec![0.0]),
        PropertyValue::F32Array(vec![0.0, 1.0]),
        PropertyValue::F32Array(vec![1.0]),
        PropertyValue::StringArray(vec![]),
        PropertyValue::StringArray(vec!["x".into()]),
        PropertyValue::Array(vec![]),
        PropertyValue::array([PropertyValue::I64(1)]),
        PropertyValue::array([PropertyValue::F64(1.0)]),
        PropertyValue::array([PropertyValue::F64(1.0), PropertyValue::I64(2)]),
        PropertyValue::array([PropertyValue::F64(f64::NAN)]),
        PropertyValue::Object(Default::default()),
        PropertyValue::object([("x", PropertyValue::I64(1))]),
        PropertyValue::object([("x", PropertyValue::F64(1.0))]),
        PropertyValue::object([("y", PropertyValue::I64(1))]),
        PropertyValue::object([("x", 1), ("y", 2)]),
    ];
    for order in [LiteralOrder::Typed, LiteralOrder::ScalarNumeric] {
        for left in &values {
            for right in &values {
                let comparison = order.compare(left, right);
                assert_eq!(comparison, order.compare(right, left).reverse());
                if left == right {
                    assert_eq!(comparison, Ordering::Equal);
                }
                for third in &values {
                    if comparison.is_le() && order.compare(right, third).is_le() {
                        assert!(
                            order.compare(left, third).is_le(),
                            "{order:?}: {left:?}, {right:?}, {third:?}"
                        );
                    }
                }
            }
        }
    }
    assert_eq!(
        LiteralOrder::ScalarNumeric.compare(&PropertyValue::I64(1), &PropertyValue::F64(1.0)),
        Ordering::Equal
    );
    assert_ne!(
        LiteralOrder::Typed.compare(&PropertyValue::I64(1), &PropertyValue::F64(1.0)),
        Ordering::Equal
    );
    assert_ne!(
        LiteralOrder::ScalarNumeric.compare(
            &PropertyValue::I64(9_007_199_254_740_993),
            &PropertyValue::F64(9_007_199_254_740_992.0)
        ),
        Ordering::Equal
    );
    for (left, right) in [
        (
            PropertyValue::array([PropertyValue::I64(1)]),
            PropertyValue::array([PropertyValue::F64(1.0)]),
        ),
        (
            PropertyValue::object([("x", PropertyValue::I64(1))]),
            PropertyValue::object([("x", PropertyValue::F64(1.0))]),
        ),
    ] {
        assert_ne!(
            LiteralOrder::ScalarNumeric.compare(&left, &right),
            Ordering::Equal
        );
    }
    // Compare selected original positions, so even NaN payloads and signed-zero
    // representatives are checked without relying on non-reflexive value equality.
    for size in [0, 1, 16, 17, values.len(), values.len() * 3] {
        let input: Vec<_> = (0..size).map(|index| index % values.len()).collect();
        let expected = input.iter().copied().fold(Vec::new(), |mut unique, index| {
            if !unique
                .iter()
                .any(|previous| values[*previous] == values[index])
            {
                unique.push(index);
            }
            unique
        });
        assert_eq!(
            dedup_by(
                input,
                |left, right| LiteralOrder::Typed.compare(&values[*left], &values[*right]),
                |left, right| values[*left] == values[*right]
            ),
            expected
        );
    }
}

#[test]
fn stable_dedup_bounds_work_for_wide_and_duplicate_heavy_domains() {
    for count in [0_usize, 1, 16, 17, 1024, 4096, 65_536] {
        for pattern in 0..5 {
            let input: Vec<usize> = (0..count)
                .map(|index| match pattern {
                    0 => index,
                    1 => count - index - 1,
                    2 => index.wrapping_mul(65_521) % count,
                    3 => index % 7,
                    _ => {
                        if index % 2 == 0 {
                            index / 2
                        } else {
                            count - index / 2 - 1
                        }
                    }
                })
                .collect();
            let comparisons = Cell::new(0_usize);
            let result = dedup_by(
                input.clone(),
                |left, right| {
                    comparisons.set(comparisons.get() + 1);
                    left.cmp(right)
                },
                |left, right| {
                    comparisons.set(comparisons.get() + 1);
                    left == right
                },
            );
            let mut seen = std::collections::BTreeSet::new();
            let expected: Vec<_> = input
                .into_iter()
                .filter(|value| seen.insert(*value))
                .collect();
            assert_eq!(result, expected);
            assert!(
                comparisons.get() <= 4 * count * count.max(2).ilog2() as usize,
                "{count} values, pattern {pattern}: {} literal comparisons",
                comparisons.get()
            );
        }
    }
}
