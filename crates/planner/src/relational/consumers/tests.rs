use super::*;
use crate::{context, exec, ir};
use std::{cell::Cell, sync::Arc};

thread_local! {
    static VISITS: Cell<[usize;4]> = const { Cell::new([0;4]) };
}
pub(super) fn visited(index: usize) {
    let mut visits = VISITS.get();
    visits[index] += 1;
    VISITS.set(visits);
}

fn query(stages: u32, width: u32) -> r::Query {
    let mut operators = Vec::new();
    for stage in 0..stages {
        operators.push(r::Operator::Match {
            pattern: r::Pattern {
                nodes: (stage * width..(stage + 1) * width)
                    .map(|slot| r::NodePattern {
                        slot: r::Slot(slot),
                        label: None,
                        properties: vec![],
                    })
                    .collect(),
                relationships: vec![],
                paths: vec![],
            },
            optional: false,
            predicate: None,
        });
        operators.push(r::Operator::Project {
            items: r::ProjectionProgram::new(
                (stage * width..(stage + 1) * width)
                    .map(|slot| r::Projection {
                        slot: r::Slot(slot),
                        expression: r::Expression::Slot(r::Slot(slot)),
                    })
                    .collect(),
            )
            .unwrap(),
            distinct: false,
            ordering: vec![],
            predicate: None,
            skip: None,
            limit: None,
        });
    }
    r::Query::new(
        (0..stages * width)
            .map(|slot| r::Binding {
                name: format!("n{slot}"),
                kind: r::BindingType::Node,
                nullable: false,
                value_type: r::ValueType::Node,
            })
            .collect(),
        operators,
        vec![],
    )
    .unwrap()
}

#[test]
fn physical_proofs_visit_sources_and_steps_once_and_never_during_lookup() {
    for (stages, width) in [(1, 1), (256, 1), (64, 32)] {
        let query = query(stages, width);
        let matches = r::reference::matches(&query).unwrap();
        let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
        VISITS.set([0; 4]);
        let consumers = prepare(&pipeline, &matches);
        assert_eq!(consumers.len(), stages as usize);
        assert_eq!(
            VISITS.get(),
            [
                (stages * width) as usize,
                (stages * width) as usize,
                (stages * 2) as usize,
                (stages * 2) as usize
            ]
        );
    }
    let selected = r::plan(query(32, 2), &context::PlannerContext::default()).unwrap();
    let expected = (0..64)
        .map(|source| selected.batch_consumer(source))
        .collect::<Vec<_>>();
    VISITS.set([0; 4]);
    for _ in 0..1024 {
        for (source, consumer) in expected.iter().enumerate() {
            assert_eq!(selected.batch_consumer(source), *consumer);
        }
    }
    assert_eq!(VISITS.get(), [0; 4]);
    let materialized = selected
        .clone()
        .with_execution(r::RowExecution::Materialized);
    assert_eq!(VISITS.get(), [0; 4]);
    assert!((0..64).all(|source| materialized.batch_consumer(source).is_none()));
    let restored = materialized.with_execution(r::RowExecution::Batched);
    assert_eq!(restored, selected);
    assert!(VISITS.get().iter().all(|count| *count > 0));
    assert!(serde_json::to_value(&restored)
        .unwrap()
        .get("consumers")
        .is_none());
}

#[test]
fn unsupported_access_clips_each_suffix_at_its_last_safe_projection() {
    let query = query(4, 1);
    let mut matches = r::reference::matches(&query).unwrap();
    let pipeline = r::RowPipeline::new(Arc::new(query.clone()), r::RowExecution::Batched);
    assert_eq!(
        prepare(&pipeline, &matches)[&0],
        r::BatchConsumer::Pipeline { end: 7 }
    );
    // A validated native parameter source has no resumable node cursor.
    // Such a selected source must stay with its native executor.
    let access = &matches[&4].sources[0].access;
    let mut steps = access.steps().to_vec();
    steps[0].op = exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::FromParam {
                param: ir::NonEmptyString::new("nodes").unwrap(),
            },
        )),
    };
    matches.get_mut(&4).unwrap().sources[0].access = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::try_from_vec(steps).unwrap(),
        exec::ExecStepId::new(1).unwrap(),
        crate::trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .unwrap()
    .into();
    let consumers = prepare(&pipeline, &matches);
    assert_eq!(consumers[&0], r::BatchConsumer::Pipeline { end: 3 });
    assert_eq!(
        consumers[&2],
        r::BatchConsumer::Project {
            termination: r::Termination::Drain
        }
    );
    assert!(!consumers.contains_key(&4));
    assert_eq!(
        consumers[&6],
        r::BatchConsumer::Project {
            termination: r::Termination::Drain
        }
    );

    // With no projection before the unsupported access, there is no safe
    // partial consumer; the later producer can still stream independently.
    let mut operators = query.operators().to_vec();
    operators.remove(3);
    operators.remove(1);
    let query = r::Query::new(query.bindings().to_vec(), operators, vec![]).unwrap();
    let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
    let matches = matches
        .into_iter()
        .map(|(index, plan)| {
            (
                match index {
                    0 => 0,
                    2 => 1,
                    4 => 2,
                    6 => 4,
                    _ => unreachable!(),
                },
                plan,
            )
        })
        .collect();
    let consumers = prepare(&pipeline, &matches);
    assert!(!consumers.contains_key(&0));
    assert!(!consumers.contains_key(&1));
    assert!(!consumers.contains_key(&2));
    assert_eq!(
        consumers[&4],
        r::BatchConsumer::Project {
            termination: r::Termination::Drain
        }
    );
}

/// Projects the first node, optionally matches it with a disconnected second
/// node that needs its own scan, and bounds the given projections. Returns the
/// query and the crossed match's index.
fn crossed_optional(limits: &[Option<i64>], predicate: bool) -> (r::Query, usize) {
    let node = |slot| r::NodePattern {
        slot: r::Slot(slot),
        label: None,
        properties: vec![],
    };
    let project = |slots: &[u32], limit: &Option<i64>| r::Operator::Project {
        items: r::ProjectionProgram::new(
            slots
                .iter()
                .map(|slot| r::Projection {
                    slot: r::Slot(*slot),
                    expression: r::Expression::Slot(r::Slot(*slot)),
                })
                .collect(),
        )
        .unwrap(),
        distinct: false,
        ordering: vec![],
        predicate: None,
        skip: None,
        limit: limit.map(|limit| r::Expression::Literal(r::Value::Integer(limit))),
    };
    let mut operators = vec![r::Operator::Match {
        pattern: r::Pattern {
            nodes: vec![node(0)],
            relationships: vec![],
            paths: vec![],
        },
        optional: false,
        predicate: None,
    }];
    operators.extend(limits.iter().map(|limit| project(&[0], limit)));
    let crossed = operators.len();
    operators.push(r::Operator::Match {
        pattern: r::Pattern {
            nodes: vec![node(0), node(1)],
            relationships: vec![],
            paths: vec![],
        },
        optional: true,
        predicate: predicate.then(|| {
            r::SelectionProgram::new(r::Expression::Unary(
                r::Unary::IsNull,
                Box::new(r::Expression::Slot(r::Slot(1))),
            ))
            .unwrap()
        }),
    });
    operators.push(project(&[0, 1], &Some(1)));
    let binding = |name: &str, kind, nullable, value_type| r::Binding {
        name: name.into(),
        kind,
        nullable,
        value_type,
    };
    let query = r::Query::new(
        vec![
            binding("a", r::BindingType::Node, false, r::ValueType::Node),
            binding("b", r::BindingType::Node, true, r::ValueType::Node),
        ],
        operators,
        vec![],
    )
    .unwrap();
    (query, crossed)
}

#[test]
fn consumers_never_split_a_window_proof_across_an_unsupported_match() {
    let unsupported = |matches: &mut BTreeMap<usize, r::MatchPlan>, index: usize| {
        let source = matches
            .get_mut(&index)
            .unwrap()
            .sources
            .iter_mut()
            .find(|source| source.slot == r::Slot(1))
            .unwrap();
        let mut steps = source.access.steps().to_vec();
        steps[0].op = exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromParam {
                    param: ir::NonEmptyString::new("nodes").unwrap(),
                },
            )),
        };
        source.access = exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::try_from_vec(steps).unwrap(),
            exec::ExecStepId::new(1).unwrap(),
            crate::trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .unwrap()
        .into();
    };
    // Without a projection limit before the match, a clipped Project consumer
    // would drain a source sized to demand; with one, a shorter chain would
    // stop before the proven window. Neither consumer may be prepared.
    for limits in [vec![None], vec![Some(5), None]] {
        let (query, crossed) = crossed_optional(&limits, false);
        let end = crossed + 1;
        let mut matches = r::reference::matches(&query).unwrap();
        let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
        assert_eq!(pipeline.input_window(0).unwrap().last_projection(), end);
        assert_eq!(
            prepare(&pipeline, &matches)[&0],
            r::BatchConsumer::Pipeline { end }
        );
        unsupported(&mut matches, crossed);
        assert!(!prepare(&pipeline, &matches).contains_key(&0), "{limits:?}");
    }
    // A filtered match ends the proof, so the existing clip remains.
    let (query, crossed) = crossed_optional(&[None], true);
    let mut matches = r::reference::matches(&query).unwrap();
    let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
    assert!(pipeline.input_window(0).is_none());
    unsupported(&mut matches, crossed);
    assert_eq!(
        prepare(&pipeline, &matches)[&0],
        r::BatchConsumer::Project {
            termination: r::Termination::Drain
        }
    );
}
