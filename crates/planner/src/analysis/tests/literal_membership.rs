use std::cell::Cell;

use super::*;

#[test]
fn wide_disjoint_intersections_have_bounded_comparison_work() {
    let count = 4096_usize;
    let right: Vec<_> = (count..count * 2).rev().collect();
    let comparisons = Cell::new(0_usize);
    let contains = membership_by(
        &right,
        count,
        |left, right| {
            comparisons.set(comparisons.get() + 1);
            left.cmp(right)
        },
        |left, right| {
            comparisons.set(comparisons.get() + 1);
            left == right
        },
    );
    assert_eq!((0..count).filter(contains).count(), 0);
    eprintln!(
        "literal intersection: {count} by {count}, {} comparisons",
        comparisons.get()
    );
    assert!(
        comparisons.get() <= 4 * count * count.ilog2() as usize,
        "{} comparisons exceed the n log n intersection bound",
        comparisons.get()
    );
}

#[test]
fn membership_matches_a_set_model_with_bounded_work_across_sizes_and_skew() {
    for (left_count, right_count) in [
        (0_usize, 0_usize),
        (0, 4096),
        (4096, 0),
        (1, 1),
        (1, 4096),
        (4096, 1),
        (16, 16),
        (16, 4096),
        (4096, 16),
        (17, 17),
        (17, 4096),
        (4096, 17),
        (1024, 1024),
        (4096, 4096),
        (65_536, 65_536),
    ] {
        for shape in 0..4 {
            let left: Vec<_> = (0..left_count).rev().collect();
            let right: Vec<_> = (0..right_count)
                .map(|index| match shape {
                    0 => index,
                    1 => left_count + index,
                    2 => index / 2,
                    _ => index.wrapping_mul(65_521) % right_count,
                })
                .collect();
            let model: std::collections::BTreeSet<_> = right.iter().copied().collect();
            let expected: Vec<_> = left
                .iter()
                .copied()
                .filter(|value| model.contains(value))
                .collect();
            let comparisons = Cell::new(0_usize);
            let contains = membership_by(
                &right,
                left_count,
                |left, right| {
                    comparisons.set(comparisons.get() + 1);
                    left.cmp(right)
                },
                |left, right| {
                    comparisons.set(comparisons.get() + 1);
                    left == right
                },
            );
            let actual: Vec<_> = left.into_iter().filter(contains).collect();
            assert_eq!(actual, expected);
            let bound = 8 * (left_count + right_count) * right_count.max(2).ilog2() as usize;
            assert!(
                comparisons.get() <= bound,
                "{left_count} by {right_count}, shape {shape}: {} comparisons exceed {bound}",
                comparisons.get()
            );
        }
    }
}

#[test]
fn membership_borrows_non_clone_payloads_and_keeps_probe_order_and_duplicates() {
    struct Payload {
        key: usize,
        bytes: Vec<u8>,
    }
    let right: Vec<_> = (0..40)
        .map(|key| Payload {
            key,
            bytes: vec![key as u8; 1024],
        })
        .collect();
    let pointers: Vec<_> = right.iter().map(|value| value.bytes.as_ptr()).collect();
    for probes in [0, 1, 16, 17, 4096] {
        let contains = membership_by(
            &right,
            probes,
            |left, right| left.key.cmp(&right.key),
            |left, right| left.key == right.key,
        );
        let actual: Vec<_> = [35, 3, 35, 80, 0, 81]
            .into_iter()
            .filter(|key| {
                contains(&Payload {
                    key: *key,
                    bytes: Vec::new(),
                })
            })
            .collect();
        assert_eq!(actual, [35, 3, 35, 0]);
        assert_eq!(
            right
                .iter()
                .map(|value| value.bytes.as_ptr())
                .collect::<Vec<_>>(),
            pointers
        );
        assert!(right
            .iter()
            .all(|value| value.bytes.iter().all(|byte| *byte == value.key as u8)));
    }
}

#[test]
fn ordered_membership_requires_equality_for_non_reflexive_values() {
    let values = [
        f64::NAN,
        -f64::NAN,
        -0.0,
        0.0,
        1.0,
        f64::NEG_INFINITY,
        f64::INFINITY,
    ];
    for size in [0, 1, 16, 17, 128] {
        let right: Vec<_> = (0..size)
            .map(|index| values[index % values.len()])
            .collect();
        for probes in [1, 17] {
            let contains = membership_by(
                &right,
                probes,
                |left, right| compare_f64(*left, *right),
                PartialEq::eq,
            );
            for value in values.into_iter().chain([2.0]) {
                assert_eq!(contains(&value), right.contains(&value));
            }
        }
    }
}
