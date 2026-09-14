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
