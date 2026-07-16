//! Deterministic executable execution-order derivation.

use std::collections::{BTreeMap, BTreeSet};

use crate::ir;

use super::index::ValidatedStepIndex;
use crate::exec::{
    ExecExecutionOrder, ExecExecutionStage, ExecParallelStage, ExecParallelStagePolicy,
    ExecPlanError, ExecSchedule, ExecStep, ExecStepId,
};

pub(in crate::exec) fn execution_order(
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
        let current = drain_next_ready_stage(&mut ready, &schedules, &parallel_dependency_groups)?;
        stages.push(stage_from_ready_with_policy(
            current.ids.clone(),
            current.policy,
        )?);
        for id in current.ids {
            emitted.insert(id);
            for dependent in dependents.get(&id).into_iter().flatten() {
                let Some(count) = remaining_dependencies.get_mut(dependent) else {
                    return Err(ExecPlanError::MissingDependency {
                        step: *dependent,
                        dependency: id,
                    });
                };
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

fn drain_next_ready_stage(
    ready: &mut BTreeSet<ExecStepId>,
    schedules: &BTreeMap<ExecStepId, &ExecSchedule>,
    parallel_dependency_groups: &BTreeMap<ExecStepId, Vec<ParallelDependencyGroup>>,
) -> Result<ReadyStage, ExecPlanError> {
    let Some(first) = ready.iter().next().copied() else {
        return Err(ExecPlanError::InvalidExecutionStage { actual: 0 });
    };
    if is_barrier_step(first, schedules) {
        ready.remove(&first);
        return Ok(ReadyStage::serial(first));
    }

    if let Some(group) =
        ready_parallel_group_at(first, ready, schedules, parallel_dependency_groups)
    {
        for id in &group.dependencies {
            ready.remove(id);
        }
        return Ok(ReadyStage::new(group.dependencies.clone(), group.policy));
    }

    let stage = ready
        .iter()
        .copied()
        .take_while(|id| {
            !is_barrier_step(*id, schedules)
                && (*id == first || !parallel_dependency_groups.contains_key(id))
        })
        .collect::<Vec<_>>();
    for id in &stage {
        ready.remove(id);
    }
    Ok(ReadyStage::default_parallel(stage))
}

fn is_barrier_step(id: ExecStepId, schedules: &BTreeMap<ExecStepId, &ExecSchedule>) -> bool {
    let schedule = schedules
        .get(&id)
        .expect("ready stage IDs must come from validated executable steps");
    matches!(schedule, ExecSchedule::Barrier)
}

#[cfg(test)]
pub(super) fn stage_from_ready(ids: Vec<ExecStepId>) -> Result<ExecExecutionStage, ExecPlanError> {
    let policy = ExecParallelStagePolicy::for_ready_width(ids.len());
    stage_from_ready_with_policy(ids, policy)
}

fn stage_from_ready_with_policy(
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

#[derive(Debug, Clone)]
struct ReadyStage {
    ids: Vec<ExecStepId>,
    policy: ExecParallelStagePolicy,
}

impl ReadyStage {
    fn new(ids: Vec<ExecStepId>, policy: ExecParallelStagePolicy) -> Self {
        Self { ids, policy }
    }

    fn serial(id: ExecStepId) -> Self {
        Self::new(vec![id], ExecParallelStagePolicy::for_ready_width(1))
    }

    fn default_parallel(ids: Vec<ExecStepId>) -> Self {
        let policy = ExecParallelStagePolicy::for_ready_width(ids.len());
        Self::new(ids, policy)
    }
}

#[derive(Debug, Clone)]
struct ParallelDependencyGroup {
    dependencies: Vec<ExecStepId>,
    sorted_dependencies: Vec<ExecStepId>,
    policy: ExecParallelStagePolicy,
}

impl ParallelDependencyGroup {
    fn from_step(step: &ExecStep) -> Option<Self> {
        let ExecSchedule::Parallel {
            max_concurrency,
            preserve_order,
        } = &step.schedule
        else {
            return None;
        };
        let mut sorted_dependencies = step.dependencies.clone();
        sorted_dependencies.sort();
        Some(Self {
            dependencies: step.dependencies.clone(),
            sorted_dependencies,
            policy: ExecParallelStagePolicy::new(*max_concurrency, *preserve_order),
        })
    }

    fn first_dependency(&self) -> Option<ExecStepId> {
        self.sorted_dependencies.first().copied()
    }

    fn is_ready_prefix(
        &self,
        ready: &BTreeSet<ExecStepId>,
        schedules: &BTreeMap<ExecStepId, &ExecSchedule>,
    ) -> bool {
        self.sorted_dependencies
            .iter()
            .all(|id| !is_barrier_step(*id, schedules))
            && ready
                .iter()
                .copied()
                .take(self.sorted_dependencies.len())
                .eq(self.sorted_dependencies.iter().copied())
    }
}

fn parallel_dependency_groups(
    index: &ValidatedStepIndex<'_>,
) -> BTreeMap<ExecStepId, Vec<ParallelDependencyGroup>> {
    let mut groups = BTreeMap::<ExecStepId, Vec<ParallelDependencyGroup>>::new();
    for group in index.steps().filter_map(ParallelDependencyGroup::from_step) {
        if let Some(first) = group.first_dependency() {
            groups.entry(first).or_default().push(group);
        }
    }
    for groups in groups.values_mut() {
        groups.sort_by_key(|group| group.dependencies.len());
    }
    groups
}

fn ready_parallel_group_at<'a>(
    first: ExecStepId,
    ready: &BTreeSet<ExecStepId>,
    schedules: &BTreeMap<ExecStepId, &ExecSchedule>,
    parallel_dependency_groups: &'a BTreeMap<ExecStepId, Vec<ParallelDependencyGroup>>,
) -> Option<&'a ParallelDependencyGroup> {
    parallel_dependency_groups
        .get(&first)?
        .iter()
        .find(|group| group.is_ready_prefix(ready, schedules))
}
