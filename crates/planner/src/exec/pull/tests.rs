//! The previous complete traversal is a small-plan oracle, never a runtime fallback.
use super::*;
use crate::exec::{
    tests::{executable, id, name, step},
    ExecExecutionStage, ExecSchedule,
};

fn reference_derive(
    steps: &[ExecStep],
    order: &ExecExecutionOrder,
    root: ExecStepId,
) -> ExecProgram {
    let by_id = steps
        .iter()
        .map(|step| (step.id, step))
        .collect::<BTreeMap<_, _>>();
    let mut uses = BTreeMap::<ExecStepId, usize>::new();
    for step in steps {
        for id in &step.dependencies {
            *uses.entry(*id).or_default() += 1;
        }
        if let ExecCondition::PreviousStepNotEmpty { dependency } = step.condition {
            *uses.entry(dependency).or_default() += 1;
        }
    }
    *uses.entry(root).or_default() += 1;
    // Captures and effects separate epochs even if an unrelated ready step
    // appears between a producer and consumer in topological order.
    let mut epochs = BTreeMap::new();
    let mut epoch = 0usize;
    for id in order.step_ids() {
        let step = by_id[&id];
        if ExecPullCapability::of(&step.op) == ExecPullCapability::Boundary {
            epoch += 1;
        }
        epochs.insert(id, epoch);
        if !matches!(step.output, ir::BatchOutputPlan::Discard) {
            epoch += 1;
        }
    }
    let mut program = ExecProgram::default();
    let ids = order.step_ids().collect::<Vec<_>>();
    for id in ids.into_iter().rev() {
        if program.absorbed.contains(&id) {
            continue;
        }
        let terminal = by_id[&id];
        if ExecPullCapability::of(&terminal.op) == ExecPullCapability::Boundary {
            continue;
        }
        let mut members = BTreeSet::from([id]);
        let mut pending = vec![id];
        while let Some(current) = pending.pop() {
            for dependency in &by_id[&current].dependencies {
                let parent = by_id[dependency];
                if uses[dependency] != 1
                    || !matches!(parent.output, ir::BatchOutputPlan::Discard)
                    || parent.condition != terminal.condition
                    || epochs[dependency] != epochs[&id]
                    || ExecPullCapability::of(&parent.op) == ExecPullCapability::Boundary
                {
                    continue;
                }
                if members.insert(*dependency) {
                    pending.push(*dependency);
                }
            }
        }
        // Without a window or terminal cardinality consumer, ordinary
        // whole-value operators avoid per-row polling overhead. Their
        // existing implementations also serve effect/materialization edges.
        let needs_demand = members.iter().any(|id| {
            matches!(
                by_id[id].op,
                ExecOp::Limit { .. }
                    | ExecOp::Range { .. }
                    | ExecOp::Count { .. }
                    | ExecOp::Project {
                        projection: ir::ProjectionPlan::Exists
                    }
            )
        });
        if members.len() > 1 && needs_demand {
            let steps = order.step_ids().filter(|id| members.contains(id)).collect();
            members.remove(&id);
            program.absorbed.extend(members);
            program.regions.insert(id, ExecPullRegion { steps });
        }
    }
    program
}

#[test]
fn components_match_reference_across_conditions_captures_and_shared_outputs() {
    for seed in 0..128usize {
        let mut steps = Vec::new();
        for n in 1..=24 {
            let dependencies = if n == 1 {
                vec![]
            } else if n > 3 && (seed + n) % 5 == 0 {
                vec![id(n - 1), id(n - 3)]
            } else {
                vec![id(n - 1)]
            };
            let mut step = step(n, dependencies, ExecSchedule::Pipeline);
            if (seed + n) % 7 == 0 {
                step.op = ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(1),
                };
            }
            if (seed + n) % 11 == 0 {
                step.output = ir::BatchOutputPlan::Bind(name(&format!("saved{n}")));
            }
            if n > 1 && (seed + n) % 13 == 0 {
                step.condition = ExecCondition::PreviousStepNotEmpty {
                    dependency: id(n - 1),
                };
            }
            if (seed + n) % 17 == 0 {
                step.op = ExecOp::Barrier {
                    name: name("barrier"),
                };
                step.schedule = ExecSchedule::Barrier;
            }
            steps.push(step);
        }
        let plan = executable(ir::AtLeast::try_from_vec(steps).unwrap(), id(24)).unwrap();
        assert_eq!(
            plan.execution_program(),
            &reference_derive(plan.steps(), &plan.execution_order(), plan.root()),
            "seed {seed}"
        );
        // Stable IDs need not follow dependency order. Reverse the IDs while
        // preserving the same validated topology and observable boundaries.
        let remap = |old: ExecStepId| id(25 - old.get());
        let mut reversed = plan.steps().to_vec();
        for step in &mut reversed {
            step.id = remap(step.id);
            step.dependencies
                .iter_mut()
                .for_each(|dependency| *dependency = remap(*dependency));
            let ExecCondition::PreviousStepNotEmpty { dependency } = &mut step.condition else {
                continue;
            };
            *dependency = remap(*dependency);
        }
        let order = ExecExecutionOrder::new(
            ir::AtLeast::try_from_vec(
                plan.execution_order()
                    .step_ids()
                    .map(|old| ExecExecutionStage::Single(remap(old)))
                    .collect(),
            )
            .unwrap(),
        );
        assert_eq!(
            ExecProgram::derive(&reversed, &order, remap(plan.root())),
            reference_derive(&reversed, &order, remap(plan.root())),
            "reversed seed {seed}"
        );
    }
}

#[test]
fn derivation_visits_each_unbounded_step_and_edge_once() {
    for size in [512, 1024, 2048, 4096] {
        let steps: Vec<_> = (1..=size)
            .map(|n| {
                step(
                    n,
                    if n == 1 { vec![] } else { vec![id(n - 1)] },
                    ExecSchedule::Pipeline,
                )
            })
            .collect();
        let order = ExecExecutionOrder::new(
            ir::AtLeast::try_from_vec(
                (1..=size)
                    .map(|n| ExecExecutionStage::Single(id(n)))
                    .collect(),
            )
            .unwrap(),
        );
        let program = ExecProgram::derive(&steps, &order, id(size));
        assert_eq!(program.regions().count(), 0);
        assert_eq!(DERIVATION_VISITS.get(), (size, size - 1));
    }
}

#[test]
fn independent_bounded_regions_do_not_rescan_the_execution_order() {
    for size in [512, 1024, 2048] {
        let mut steps = Vec::new();
        for n in 0..size {
            steps.push(step(
                n * 2 + 1,
                if n == 0 { vec![] } else { vec![id(n * 2)] },
                ExecSchedule::Pipeline,
            ));
            let mut limit = step(n * 2 + 2, vec![id(n * 2 + 1)], ExecSchedule::Pipeline);
            limit.op = ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            };
            limit.output = ir::BatchOutputPlan::Bind(name(&format!("result{n}")));
            steps.push(limit);
        }
        let order = ExecExecutionOrder::new(
            ir::AtLeast::try_from_vec(
                (1..=size * 2)
                    .map(|n| ExecExecutionStage::Single(id(n)))
                    .collect(),
            )
            .unwrap(),
        );
        let program = ExecProgram::derive(&steps, &order, id(size * 2));
        assert_eq!(program.regions().count(), size);
        assert_eq!(DERIVATION_VISITS.get(), (size * 2, size * 2 - 1));
        for (n, (_, region)) in program.regions().enumerate() {
            assert_eq!(region.steps(), &[id(n * 2 + 1), id(n * 2 + 2)]);
        }
    }
}

/// Manual timing companion to the deterministic visit-count regressions.
#[test]
#[ignore = "manual optimized planner benchmark"]
fn benchmark_region_derivation() {
    use std::{hint::black_box, time::Instant};
    for size in [512, 1024, 2048, 4096] {
        let steps = (1..=size)
            .map(|n| {
                step(
                    n,
                    if n == 1 { vec![] } else { vec![id(n - 1)] },
                    ExecSchedule::Pipeline,
                )
            })
            .collect::<Vec<_>>();
        let order = ExecExecutionOrder::new(
            ir::AtLeast::try_from_vec(
                (1..=size)
                    .map(|n| ExecExecutionStage::Single(id(n)))
                    .collect(),
            )
            .unwrap(),
        );
        let mut before = Vec::new();
        let mut after = Vec::new();
        for _ in 0..7 {
            let start = Instant::now();
            let expected = black_box(reference_derive(black_box(&steps), &order, id(size)));
            before.push(start.elapsed());
            let start = Instant::now();
            let actual = black_box(ExecProgram::derive(black_box(&steps), &order, id(size)));
            after.push(start.elapsed());
            assert_eq!(actual, expected);
        }
        before.sort();
        after.sort();
        println!(
            "steps={size} before_us={} after_us={}",
            before[3].as_micros(),
            after[3].as_micros()
        );
    }
}
