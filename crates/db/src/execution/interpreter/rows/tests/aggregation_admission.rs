//! The shared accumulator owns each input; probing DISTINCT must not copy it.
use crate::allocation_testing;
use helix_planner::relational as r;
use std::collections::BTreeMap;

#[test]
fn distinct_duplicate_probes_reuse_owned_inputs_without_allocating() {
    let values = [
        r::Value::String("x".repeat(64 * 1024)),
        r::Value::List(vec![r::Value::String("x".repeat(16 * 1024))]),
        r::Value::Map(BTreeMap::from([(
            "key".into(),
            r::Value::String("x".repeat(16 * 1024)),
        )])),
    ];
    for function in [
        r::Aggregate::Count,
        r::Aggregate::Min,
        r::Aggregate::Max,
        r::Aggregate::Collect,
    ] {
        for value in &values {
            let mut accumulator = r::Accumulator::new(function, true);
            accumulator.push(value.clone(), 1, 1024 * 1024).unwrap();
            let retained = accumulator.allocated_bytes();
            let input = value.clone();
            let (result, allocations) =
                allocation_testing::observe(|| accumulator.push(input, 1, retained));
            result.unwrap();
            assert_eq!(
                allocations.allocations, 0,
                "{function:?}: duplicate probe copied an admitted input"
            );
            assert_eq!(accumulator.allocated_bytes(), retained);
            let expected = match function {
                r::Aggregate::Count => r::Value::Integer(1),
                r::Aggregate::Collect => r::Value::List(vec![value.clone()]),
                r::Aggregate::Min | r::Aggregate::Max => value.clone(),
                r::Aggregate::Sum | r::Aggregate::Avg => unreachable!(),
            };
            assert_eq!(accumulator.finish().unwrap(), expected);
        }
    }
}

#[test]
fn distinct_rejected_inputs_do_not_copy_payloads_or_poison_accumulator_state() {
    for function in [
        r::Aggregate::Count,
        r::Aggregate::Min,
        r::Aggregate::Max,
        r::Aggregate::Collect,
    ] {
        let mut accumulator = r::Accumulator::new(function, true);
        let first = r::Value::String("a".repeat(64 * 1024));
        accumulator.push(first.clone(), 10, 1024 * 1024).unwrap();
        let retained = accumulator.allocated_bytes();
        let next = r::Value::String("b".repeat(64 * 1024));
        let input = next.clone();
        let (result, allocations) =
            allocation_testing::observe(|| accumulator.push(input, 10, retained));
        assert_eq!(result.unwrap_err().detail, "MemoryLimit");
        // The structured error owns short strings; it must not copy the value.
        assert!(
            allocations.bytes < 1024,
            "{function:?}: rejected input copied before admission: {allocations:?}"
        );
        assert_eq!(accumulator.allocated_bytes(), retained);
        accumulator.push(next.clone(), 10, 1024 * 1024).unwrap();
        let expected = match function {
            r::Aggregate::Count => r::Value::Integer(2),
            r::Aggregate::Collect => r::Value::List(vec![first, next]),
            r::Aggregate::Min => first,
            r::Aggregate::Max => next,
            r::Aggregate::Sum | r::Aggregate::Avg => unreachable!(),
        };
        assert_eq!(accumulator.finish().unwrap(), expected);
    }
}

#[test]
fn accumulator_admission_failure_preserves_every_state_and_ignored_inputs_skip_it() {
    for function in [
        r::Aggregate::Count,
        r::Aggregate::Sum,
        r::Aggregate::Avg,
        r::Aggregate::Min,
        r::Aggregate::Max,
        r::Aggregate::Collect,
    ] {
        for distinct in [false, true] {
            let mut accumulator = r::Accumulator::new(function, distinct);
            let mut control = r::Accumulator::new(function, distinct);
            accumulator
                .push(r::Value::Integer(7), 100, 1024 * 1024)
                .unwrap();
            control
                .push(r::Value::Integer(7), 100, 1024 * 1024)
                .unwrap();
            let before = accumulator.allocated_bytes();
            let mut calls = 0;
            let result =
                accumulator.push_with_admission(r::Value::Integer(9), 100, 1024 * 1024, |bound| {
                    assert!(bound >= before);
                    calls += 1;
                    Err(r::QueryError::runtime(
                        "ResourceLimit",
                        "MemoryLimit",
                        "test admission refused",
                    ))
                });
            assert_eq!(result.unwrap_err().detail, "MemoryLimit");
            assert_eq!(calls, 1);
            assert_eq!(accumulator.allocated_bytes(), before);
            accumulator
                .push_with_admission::<r::QueryError>(r::Value::Null, 100, 0, |_| {
                    panic!("null input requested admission")
                })
                .unwrap();
            if distinct {
                accumulator
                    .push_with_admission::<r::QueryError>(r::Value::Integer(7), 100, 0, |_| {
                        panic!("duplicate input requested admission")
                    })
                    .unwrap();
            }
            assert_eq!(accumulator.finish().unwrap(), control.finish().unwrap());
        }
    }
    let mut sum = r::Accumulator::new(r::Aggregate::Sum, false);
    let result =
        sum.push_with_admission::<r::QueryError>(r::Value::String("bad".into()), 10, 1024, |_| {
            panic!("invalid numeric input requested admission")
        });
    assert!(result.is_err());
    assert_eq!(sum.finish().unwrap(), r::Value::Integer(0));
}

#[test]
fn accumulator_growth_is_admitted_before_clones_and_matches_real_allocations() {
    use crate::query_resources::Budget;
    for function in [
        r::Aggregate::Count,
        r::Aggregate::Min,
        r::Aggregate::Max,
        r::Aggregate::Collect,
    ] {
        for distinct in [false, true] {
            for prefix in [0, 1, 3, 7, 14, 28, 56, 112] {
                let mut accumulator = r::Accumulator::new(function, distinct);
                for index in 0..prefix {
                    accumulator
                        .push(
                            r::Value::String(format!("{index:04}{}", "x".repeat(256))),
                            1000,
                            16 * 1024 * 1024,
                        )
                        .unwrap();
                }
                let before = accumulator.allocated_bytes();
                let budget = Budget::new(16 * 1024 * 1024);
                let mut retained = budget.reserve(before).unwrap();
                let value = r::Value::String(format!("{prefix:04}{}", "x".repeat(256)));
                let input = budget.reserve(value.allocated_bytes()).unwrap();
                let mut admitted = 0;
                let (result, allocations) = allocation_testing::observe(|| {
                    accumulator.push_with_admission::<crate::cypher::Error>(
                        value,
                        1000,
                        16 * 1024 * 1024,
                        |bound| {
                            admitted = bound - before;
                            retained.resize(bound).map_err(Into::into)
                        },
                    )
                });
                result.unwrap();
                assert!(allocations.bytes<=admitted,"{function:?} distinct={distinct}, prefix={prefix}: {allocations:?} exceeds {admitted}");
                retained.shrink_to(accumulator.allocated_bytes());
                drop(input);
                drop(accumulator);
                drop(retained);
                assert_eq!(budget.available(), 16 * 1024 * 1024);
            }
        }
    }
    for function in [
        r::Aggregate::Count,
        r::Aggregate::Min,
        r::Aggregate::Max,
        r::Aggregate::Collect,
    ] {
        let mut accumulator = r::Accumulator::new(function, true);
        let value = r::Value::String("x".repeat(64 * 1024));
        let (result, allocations) = allocation_testing::observe(|| {
            accumulator.push_with_admission(value, 100, 1024 * 1024, |_| {
                Err(r::QueryError::runtime(
                    "ResourceLimit",
                    "MemoryLimit",
                    "test refusal",
                ))
            })
        });
        assert_eq!(result.unwrap_err().detail, "MemoryLimit");
        assert!(
            allocations.bytes < 1024,
            "{function:?}: copied before admission: {allocations:?}"
        );
    }
}
