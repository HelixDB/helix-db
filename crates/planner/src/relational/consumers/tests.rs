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
    .unwrap();
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
