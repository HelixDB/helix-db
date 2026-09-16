//! The shared accumulator owns each input; probing DISTINCT must not copy it.
use crate::allocation_testing;
use helix_planner::relational as r;
use std::collections::BTreeMap;

#[path = "../../../../../tests/production_support/mixed_aggregation.rs"]
mod mixed_aggregation;

#[tokio::test]
async fn mixed_aggregate_failures_release_groups_and_allow_retry() {
    use super::super::*;
    use crate::execution::interpreter::test_support;
    use helix_planner::context;

    let db = test_support::open_db("mixed-aggregate-failure-admission").await;
    for (memory_bytes, divisor, expected_error) in [
        (512, 1, Some("MemoryLimit")),
        (64 * 1024, 0, Some("DivisionByZero")),
        (64 * 1024, 1, None),
    ] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(memory_bytes));
        for _ in 0..2 {
            let rows = Rows::new(
                vec![vec![r::Value::Integer(3), r::Value::Null]],
                ctx.row_budget(),
            )
            .unwrap();
            let items = r::ProjectionProgram::new(vec![r::Projection {
                slot: r::Slot(1),
                expression: r::Expression::Binary(
                    r::Binary::Divide,
                    Box::new(r::Expression::Aggregate {
                        function: r::Aggregate::Sum,
                        argument: Some(Box::new(r::Expression::Slot(r::Slot(0)))),
                        distinct: false,
                    }),
                    Box::new(r::Expression::Literal(r::Value::Integer(divisor))),
                ),
            }])
            .unwrap();
            let result = ctx
                .project_rows(
                    rows,
                    2,
                    projection::Projection {
                        items: &items,
                        distinct: false,
                        ordering: &[],
                        predicate: None,
                        skip: None,
                        limit: None,
                    },
                    &BTreeMap::new(),
                    Limits {
                        memory_bytes,
                        ..Default::default()
                    },
                )
                .await;
            match (result, expected_error) {
                (Err(Error::Query(error)), Some(detail)) => assert_eq!(error.detail, detail),
                (Ok(rows), None) => {
                    assert_eq!(rows.data, vec![vec![r::Value::Null, r::Value::Integer(3)]])
                }
                (Err(error), expected) => panic!("expected {expected:?}, got {error:?}"),
                (Ok(rows), Some(detail)) => panic!("expected {detail}, got {:?}", rows.data),
            }
            assert_eq!(ctx.row_budget().available(), memory_bytes);
        }
    }
    db.close().await.unwrap();
}

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

#[tokio::test]
async fn mixed_aggregate_memory_and_cancellation_boundaries_release_every_owner() {
    use super::super::*;
    use crate::execution::interpreter::test_support;
    use helix_planner::context;

    let db = test_support::open_db("mixed-aggregate-boundaries").await;
    for (grouped, count) in [(false, 0), (false, 4), (true, 4)] {
        let mut items = Vec::new();
        if grouped {
            items.push(r::Projection {
                slot: r::Slot(2),
                expression: r::Expression::Slot(r::Slot(0)),
            });
        }
        items.push(r::Projection {
            slot: r::Slot(3),
            expression: r::Expression::Binary(
                r::Binary::Add,
                Box::new(r::Expression::Aggregate {
                    function: r::Aggregate::Count,
                    argument: None,
                    distinct: false,
                }),
                Box::new(r::Expression::Literal(r::Value::Integer(1))),
            ),
        });
        let items = r::ProjectionProgram::new(items).unwrap();
        let mut memory_failures = 0;
        let mut cancellations = 0;
        let mut successes = 0;
        for (memory_bytes, checkpoint) in (128..=8192)
            .step_by(32)
            .map(|bytes| (bytes, usize::MAX))
            .chain((0..64).map(|check| (8192, check)))
        {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.row_memory = Some(memory::Budget::new(memory_bytes));
            // An unreachable checkpoint leaves the memory sweep uncancelled.
            ctx.fail_deadline_after(checkpoint);
            let data = (0..count)
                .map(|value| {
                    vec![
                        r::Value::Integer(value % 2),
                        r::Value::Null,
                        r::Value::Null,
                        r::Value::Null,
                    ]
                })
                .collect();
            let result = match Rows::new(data, ctx.row_budget()) {
                Ok(rows) => {
                    ctx.project_rows(
                        rows,
                        4,
                        projection::Projection {
                            items: &items,
                            distinct: false,
                            ordering: &[],
                            predicate: None,
                            skip: None,
                            limit: None,
                        },
                        &BTreeMap::new(),
                        Limits {
                            memory_bytes,
                            batch_rows: 1,
                            ..Default::default()
                        },
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(rows) => {
                    successes += 1;
                    assert_eq!(rows.len(), if grouped { 2 } else { 1 });
                    for row in rows.iter() {
                        assert_eq!(
                            row[3],
                            r::Value::Integer(if grouped { 3 } else { count + 1 })
                        );
                    }
                }
                Err(Error::Query(error)) if error.detail == "MemoryLimit" => memory_failures += 1,
                Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)) => {
                    cancellations += 1
                }
                Err(error) => panic!("unexpected failure: {error:?}"),
            }
            assert_eq!(ctx.row_budget().available(), memory_bytes, "grouped={grouped}, count={count}, memory={memory_bytes}, checkpoint={checkpoint:?}");
        }
        assert!(memory_failures > 0 && cancellations > 0 && successes > 0);
    }
    db.close().await.unwrap();
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
