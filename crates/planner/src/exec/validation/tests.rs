use super::*;
use crate::exec::{
    ExecCondition, ExecCountDependency, ExecCountPlan, ExecCountStreamPlan,
    ExecCountValidationError, ExecCountWindowPlan, ExecExecutionStage, ExecOp, ExecPlanError,
    ExecSchedule, ExecStep, ExecStepId,
};
use crate::{cost, ir, properties};

fn id(value: usize) -> ExecStepId {
    ExecStepId::new(value).unwrap()
}

fn steps(items: Vec<ExecStep>) -> ir::AtLeast<ExecStep, 1> {
    ir::AtLeast::<_, 1>::try_from_vec(items).unwrap()
}

fn step(value: usize, dependencies: Vec<usize>) -> ExecStep {
    ExecStep {
        id: id(value),
        dependencies: dependencies.into_iter().map(id).collect(),
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: ExecCondition::Always,
        op: ExecOp::Noop,
        schedule: ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

#[test]
fn validated_step_index_rejects_duplicate_ids_before_graph_checks() {
    let duplicate_steps = steps(vec![step(1, vec![]), step(1, vec![])]);
    let Err(err) = index::ValidatedStepIndex::new(&duplicate_steps, id(1)) else {
        panic!("duplicate step IDs must be rejected");
    };
    assert_eq!(err, ExecPlanError::DuplicateStepId { id: id(1) });
}

#[test]
fn graph_reachability_supports_transitive_previous_conditions() {
    let mut root = step(3, vec![2]);
    root.condition = ExecCondition::PreviousStepNotEmpty { dependency: id(1) };
    let graph_steps = steps(vec![step(1, vec![]), step(2, vec![1]), root]);
    let index = index::ValidatedStepIndex::new(&graph_steps, id(3)).unwrap();

    assert!(graph::dependency_reachable(&index, &[id(2)], id(1)));
    assert!(!graph::dependency_reachable(&index, &[id(2)], id(99)));
}

/// Graph validation keeps its traversal state on the heap, so a dependency
/// chain far longer than the thread's stack could recurse through still
/// validates, and still names the step that closes a cycle.
#[test]
fn long_dependency_chains_validate_on_a_small_stack() {
    const STEPS: usize = 50_000;
    let chain = |first_dependency: Option<usize>| {
        (1..=STEPS)
            .map(|value| match value {
                1 => step(value, first_dependency.into_iter().collect()),
                _ => step(value, vec![value - 1]),
            })
            .collect::<Vec<_>>()
    };
    std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(move || {
            let acyclic = steps(chain(None));
            let index = index::ValidatedStepIndex::new(&acyclic, id(STEPS)).unwrap();
            assert!(graph::dependency_reachable(&index, &[id(STEPS - 1)], id(1)));

            let cyclic = steps(chain(Some(STEPS)));
            let Err(error) = index::ValidatedStepIndex::new(&cyclic, id(STEPS)) else {
                panic!("a chain closed into a loop must be rejected");
            };
            assert_eq!(error, ExecPlanError::DependencyCycle { step: id(1) });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn order_stage_contract_distinguishes_single_parallel_and_empty_sets() {
    assert_eq!(
        order::stage_from_ready(vec![id(1)]).unwrap(),
        ExecExecutionStage::Single(id(1))
    );
    let ExecExecutionStage::Parallel(stage) = order::stage_from_ready(vec![id(1), id(2)]).unwrap()
    else {
        panic!("two ready steps should form a parallel stage");
    };
    assert_eq!(stage.ids(), &[id(1), id(2)]);
    assert_eq!(stage.max_concurrency().get(), 2);
    assert!(stage.preserve_order());
    assert_eq!(
        order::stage_from_ready(Vec::new()).unwrap_err(),
        ExecPlanError::InvalidExecutionStage { actual: 0 }
    );
}

fn count_step(value: usize, dependencies: Vec<usize>, plan: ExecCountPlan) -> ExecStep {
    let mut step = step(value, dependencies);
    step.op = ExecOp::Count {
        plan: Box::new(plan),
    };
    step
}

#[test]
fn validated_step_index_enforces_count_dependency_shapes() {
    for (plan, dependencies, expected) in [
        (
            ExecCountPlan::InputRows {
                window: ExecCountWindowPlan::identity(),
            },
            Vec::new(),
            ExecCountDependency::Rows,
        ),
        (
            ExecCountPlan::InputScalars {
                window: ExecCountWindowPlan::identity(),
            },
            vec![1, 2],
            ExecCountDependency::Scalars,
        ),
        (
            ExecCountPlan::Constant(0),
            vec![1, 2],
            ExecCountDependency::Direct,
        ),
    ] {
        let graph_steps = steps(vec![
            step(1, Vec::new()),
            step(2, Vec::new()),
            count_step(3, dependencies.clone(), plan),
        ]);
        let Err(error) = index::ValidatedStepIndex::new(&graph_steps, id(3)) else {
            panic!("invalid count dependency shape must be rejected")
        };
        assert!(error.to_string().contains(&format!("{expected:?} input")));
        assert_eq!(
            error,
            ExecPlanError::InvalidCountDependencyCount {
                step: id(3),
                dependency: expected,
                actual: dependencies.len(),
            }
        );
    }

    let sequenced_direct = steps(vec![
        step(1, Vec::new()),
        count_step(2, vec![1], ExecCountPlan::Constant(0)),
    ]);
    assert!(index::ValidatedStepIndex::new(&sequenced_direct, id(2)).is_ok());

    let one_row_input = steps(vec![
        step(1, Vec::new()),
        count_step(
            2,
            vec![1],
            ExecCountPlan::InputRows {
                window: ExecCountWindowPlan::identity(),
            },
        ),
    ]);
    assert!(index::ValidatedStepIndex::new(&one_row_input, id(2)).is_ok());
}

#[test]
fn validated_step_index_rejects_malformed_count_programs() {
    let malformed = ExecCountPlan::Stream(ExecCountStreamPlan {
        cursor: crate::exec::ExecCountCursorPlan::Intersect {
            driver: Box::new(crate::exec::ExecCountCursorPlan::InputRows),
            rest: ir::AtLeast::from_one(crate::exec::ExecCountCursorPlan::InputRows),
        },
        window: ExecCountWindowPlan::identity(),
    });
    let graph_steps = steps(vec![count_step(1, Vec::new(), malformed)]);
    let Err(error) = index::ValidatedStepIndex::new(&graph_steps, id(1)) else {
        panic!("malformed count program must be rejected")
    };
    assert!(error.to_string().contains("invalid program"));

    assert_eq!(
        error,
        ExecPlanError::InvalidCountProgram {
            step: id(1),
            reason: ExecCountValidationError::MultipleRowInputs,
        }
    );
}

#[test]
fn executable_validation_rejects_false_access_ordering() {
    let mut access = step(1, vec![]);
    access.op = ExecOp::Access {
        plan: Box::new(crate::exec::ExecAccessPlan::Node(
            crate::exec::ExecNodeAccessPlan::AllScan,
        )),
    };
    access.delivered.ordering =
        properties::DeliveredOrdering::ByKeys(ir::OrderKeys::from(ir::OrderKey {
            property: ir::NonEmptyString::new("last_seen").unwrap(),
            order: helix_ast::traversal::Order::Desc,
        }));
    let graph_steps = steps(vec![access]);
    let Err(error) = index::ValidatedStepIndex::new(&graph_steps, id(1)) else {
        panic!("unordered access must not claim property order");
    };
    assert_eq!(error, ExecPlanError::InvalidAccessOrdering { step: id(1) });
    assert!(error.to_string().contains("ordering"));
}

/// The ordering derivation before steps were indexed by position: maps keyed
/// by step ID throughout. An oracle for the position-based derivation only.
mod reference {
    use std::collections::{BTreeMap, BTreeSet};

    use super::index::ValidatedStepIndex;
    use crate::exec::{
        ExecExecutionOrder, ExecExecutionStage, ExecParallelStage, ExecParallelStagePolicy,
        ExecPlanError, ExecSchedule, ExecStep, ExecStepId,
    };
    use crate::ir;

    pub(super) fn execution_order(
        steps: &ir::AtLeast<ExecStep, 1>,
        root: ExecStepId,
    ) -> Result<ExecExecutionOrder, ExecPlanError> {
        let index = ValidatedStepIndex::new(steps, root)?;
        let schedules = index
            .steps()
            .map(|step| (step.id, &step.schedule))
            .collect::<BTreeMap<_, _>>();
        let parallel_dependency_groups = parallel_dependency_groups(&index);
        let mut dependents = BTreeMap::<ExecStepId, Vec<ExecStepId>>::new();
        let mut remaining_dependencies = BTreeMap::<ExecStepId, usize>::new();
        for step in index.steps() {
            remaining_dependencies.insert(step.id, step.dependencies.len());
            for dependency in &step.dependencies {
                dependents.entry(*dependency).or_default().push(step.id);
            }
        }
        for ids in dependents.values_mut() {
            ids.sort();
        }
        let mut ready = remaining_dependencies
            .iter()
            .filter_map(|(id, count)| (*count == 0).then_some(*id))
            .collect::<BTreeSet<_>>();
        let mut stages = Vec::new();
        let mut emitted = BTreeSet::new();
        while !ready.is_empty() {
            let (ids, policy) =
                drain_next_ready_stage(&mut ready, &schedules, &parallel_dependency_groups)?;
            stages.push(stage(ids.clone(), policy)?);
            for id in ids {
                emitted.insert(id);
                for dependent in dependents.get(&id).into_iter().flatten() {
                    let count = remaining_dependencies
                        .get_mut(dependent)
                        .expect("dependents are steps");
                    *count = count.saturating_sub(1);
                    if *count == 0 && !emitted.contains(dependent) {
                        ready.insert(*dependent);
                    }
                }
            }
        }
        let stages = ir::AtLeast::<_, 1>::try_from_vec(stages)
            .ok_or(ExecPlanError::InvalidExecutionStage { actual: 0 })?;
        if emitted.len() != index.len() {
            return Err(ExecPlanError::IncompleteExecutionOrder {
                emitted: emitted.len(),
                total: index.len(),
            });
        }
        Ok(ExecExecutionOrder::new(stages))
    }

    type Group = (Vec<ExecStepId>, Vec<ExecStepId>, ExecParallelStagePolicy);

    fn drain_next_ready_stage(
        ready: &mut BTreeSet<ExecStepId>,
        schedules: &BTreeMap<ExecStepId, &ExecSchedule>,
        groups: &BTreeMap<ExecStepId, Vec<Group>>,
    ) -> Result<(Vec<ExecStepId>, ExecParallelStagePolicy), ExecPlanError> {
        let Some(first) = ready.iter().next().copied() else {
            return Err(ExecPlanError::InvalidExecutionStage { actual: 0 });
        };
        let barrier = |id: &ExecStepId| matches!(schedules[id], ExecSchedule::Barrier);
        if barrier(&first) {
            ready.remove(&first);
            return Ok((vec![first], ExecParallelStagePolicy::for_ready_width(1)));
        }
        let group = groups.get(&first).and_then(|groups| {
            groups.iter().find(|(_, sorted, _)| {
                !sorted.iter().any(barrier)
                    && ready
                        .iter()
                        .copied()
                        .take(sorted.len())
                        .eq(sorted.iter().copied())
            })
        });
        let (ids, policy) = match group {
            Some((dependencies, _, policy)) => (dependencies.clone(), *policy),
            None => {
                let ids = ready
                    .iter()
                    .copied()
                    .take_while(|id| !barrier(id) && (*id == first || !groups.contains_key(id)))
                    .collect::<Vec<_>>();
                let policy = ExecParallelStagePolicy::for_ready_width(ids.len());
                (ids, policy)
            }
        };
        for id in &ids {
            ready.remove(id);
        }
        Ok((ids, policy))
    }

    fn stage(
        ids: Vec<ExecStepId>,
        policy: ExecParallelStagePolicy,
    ) -> Result<ExecExecutionStage, ExecPlanError> {
        match ids.as_slice() {
            [] => Err(ExecPlanError::InvalidExecutionStage { actual: 0 }),
            [id] => Ok(ExecExecutionStage::Single(*id)),
            [first, second, rest @ ..] => Ok(ExecExecutionStage::Parallel(ExecParallelStage::new(
                ir::AtLeast::<_, 2>::from_pair_and_rest(*first, *second, rest.to_vec()),
                policy,
            ))),
        }
    }

    fn parallel_dependency_groups(
        index: &ValidatedStepIndex<'_>,
    ) -> BTreeMap<ExecStepId, Vec<Group>> {
        let mut groups = BTreeMap::<ExecStepId, Vec<Group>>::new();
        for step in index.steps() {
            let ExecSchedule::Parallel {
                max_concurrency,
                preserve_order,
            } = &step.schedule
            else {
                continue;
            };
            let mut sorted = step.dependencies.clone();
            sorted.sort();
            let Some(first) = sorted.first().copied() else {
                continue;
            };
            groups.entry(first).or_default().push((
                step.dependencies.clone(),
                sorted,
                ExecParallelStagePolicy::new(*max_concurrency, *preserve_order),
            ));
        }
        for groups in groups.values_mut() {
            groups.sort_by_key(|(dependencies, _, _)| dependencies.len());
        }
        groups
    }
}

/// A small deterministic generator, so failures name a reproducible seed.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) as usize % bound
    }
}

/// A random valid DAG of `size` steps whose IDs are `rename(1..=size)`: each
/// step depends on earlier ones, some are barriers or parallel fan-ins, and a
/// root depends on every sink so all steps are reachable.
fn random_dag(seed: u64, size: usize, rename: impl Fn(usize) -> usize) -> (Vec<ExecStep>, usize) {
    let mut random = Lcg(seed);
    let mut used = vec![false; size + 1];
    let mut dag = Vec::new();
    for n in 1..=size {
        let mut dependencies = (0..random.below(4).min(n - 1))
            .map(|_| 1 + random.below(n - 1))
            .collect::<Vec<_>>();
        dependencies.sort_unstable();
        dependencies.dedup();
        dependencies
            .iter()
            .for_each(|dependency| used[*dependency] = true);
        let mut generated = step(rename(n), dependencies.iter().map(|d| rename(*d)).collect());
        generated.schedule = match (random.below(6), dependencies.len()) {
            (0, _) => ExecSchedule::Barrier,
            (1 | 2, 2..) => ExecSchedule::Parallel {
                max_concurrency: properties::PositiveUsize::new(1 + random.below(3)).unwrap(),
                preserve_order: random.below(2) == 0,
            },
            _ => ExecSchedule::Pipeline,
        };
        dag.push(generated);
    }
    let root = size + 1;
    let sinks = (1..=size)
        .filter(|n| !used[*n])
        .map(&rename)
        .collect::<Vec<_>>();
    dag.push(step(rename(root), sinks));
    (dag, rename(root))
}

/// How generated steps are numbered: consecutively as planning numbers them,
/// with gaps, or against dependency order.
#[derive(Clone, Copy, Debug)]
enum Ids {
    Consecutive,
    Gapped,
    Reversed,
}

impl Ids {
    fn rename(self, size: usize, n: usize) -> usize {
        match self {
            Self::Consecutive => n,
            Self::Gapped => n * 3 + 7,
            Self::Reversed => size + 2 - n,
        }
    }
}

#[test]
fn execution_order_matches_the_map_based_derivation() {
    let mut parallel_stages = 0;
    for seed in 0..256u64 {
        let size = 1 + (seed as usize % 40);
        for scheme in [Ids::Consecutive, Ids::Gapped, Ids::Reversed] {
            let rename = |n| scheme.rename(size, n);
            let (mut dag, root) = random_dag(seed, size, rename);
            // Validation must not depend on the order steps are listed in.
            if seed % 2 == 1 {
                dag.reverse();
            }
            let dag = steps(dag);
            let derived = order::execution_order(&dag, id(root));
            assert_eq!(
                derived,
                reference::execution_order(&dag, id(root)),
                "{scheme:?} seed {seed}"
            );
            parallel_stages += derived
                .expect("generated DAGs are valid")
                .stages()
                .iter()
                .filter(|stage| matches!(stage, ExecExecutionStage::Parallel(_)))
                .count();
        }
    }
    assert!(
        parallel_stages > 100,
        "only {parallel_stages} parallel stages"
    );
}
