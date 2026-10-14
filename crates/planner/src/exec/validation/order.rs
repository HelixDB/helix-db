//! Deterministic executable execution-order derivation.
//!
//! Steps are handled by position (ascending ID order, see
//! [`StepPositions`](crate::exec::positions::StepPositions)): dependency
//! counts are a vector, each step's dependents a slice of one flat vector, and
//! the ready set a bitset, so ordering a DAG allocates a handful of vectors
//! rather than a map entry per step and edge.

use std::collections::BTreeMap;

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
    let parallel_dependency_groups = parallel_dependency_groups(&index);
    let dependents = Dependents::new(&index);
    let mut remaining_dependencies = index
        .steps()
        .map(|step| step.dependencies.len())
        .collect::<Vec<_>>();
    let mut ready = ReadySet::new(index.len());
    remaining_dependencies
        .iter()
        .enumerate()
        .filter(|(_, count)| **count == 0)
        .for_each(|(position, _)| ready.insert(position));
    let mut stages = Vec::new();
    let mut emitted = vec![false; index.len()];
    let mut emitted_count = 0;

    while !ready.is_empty() {
        let current = drain_next_ready_stage(&mut ready, &index, &parallel_dependency_groups)?;
        stages.push(stage_from_ready_with_policy(
            current
                .positions
                .iter()
                .map(|position| index.at(*position).id)
                .collect(),
            current.policy,
        )?);
        for position in current.positions {
            emitted[position] = true;
            emitted_count += 1;
            for dependent in dependents.of(position) {
                let count = &mut remaining_dependencies[*dependent];
                *count = count.saturating_sub(1);
                if *count == 0 && !emitted[*dependent] {
                    ready.insert(*dependent);
                }
            }
        }
    }

    let stages = ir::AtLeast::<_, 1>::try_from_vec(stages)
        .ok_or(ExecPlanError::InvalidExecutionStage { actual: 0 })?;
    if emitted_count != index.len() {
        return Err(ExecPlanError::IncompleteExecutionOrder {
            emitted: emitted_count,
            total: index.len(),
        });
    }
    Ok(ExecExecutionOrder::new(stages))
}

/// The position of a dependency of a validated step.
fn dependency_position(index: &ValidatedStepIndex<'_>, dependency: ExecStepId) -> usize {
    index
        .position(dependency)
        .expect("validated dependencies are steps")
}

/// Each step's dependents, in ascending position, as slices of one vector.
struct Dependents {
    /// Dependents of position `p` are `dependents[offsets[p]..offsets[p + 1]]`.
    offsets: Vec<usize>,
    dependents: Vec<usize>,
}

impl Dependents {
    fn new(index: &ValidatedStepIndex<'_>) -> Self {
        let mut offsets = vec![0; index.len() + 1];
        for step in index.steps() {
            for dependency in &step.dependencies {
                offsets[dependency_position(index, *dependency) + 1] += 1;
            }
        }
        for position in 0..index.len() {
            offsets[position + 1] += offsets[position];
        }
        let mut next = offsets[..index.len()].to_vec();
        let mut dependents = vec![0; offsets[index.len()]];
        // Steps are visited in ascending position, so each slice fills in
        // ascending order.
        for (position, step) in index.steps().enumerate() {
            for dependency in &step.dependencies {
                let slot = &mut next[dependency_position(index, *dependency)];
                dependents[*slot] = position;
                *slot += 1;
            }
        }
        Self {
            offsets,
            dependents,
        }
    }

    fn of(&self, position: usize) -> &[usize] {
        &self.dependents[self.offsets[position]..self.offsets[position + 1]]
    }
}

/// Ready step positions, iterated in ascending order.
struct ReadySet {
    words: Vec<u64>,
    /// No word before this one has a bit set.
    lowest_word: usize,
    count: usize,
}

impl ReadySet {
    const WORD_BITS: usize = u64::BITS as usize;

    fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(Self::WORD_BITS)],
            lowest_word: 0,
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn insert(&mut self, position: usize) {
        let (word, bit) = (position / Self::WORD_BITS, position % Self::WORD_BITS);
        if self.words[word] & (1 << bit) == 0 {
            self.words[word] |= 1 << bit;
            self.count += 1;
            self.lowest_word = self.lowest_word.min(word);
        }
    }

    fn remove(&mut self, position: usize) {
        let (word, bit) = (position / Self::WORD_BITS, position % Self::WORD_BITS);
        if self.words[word] & (1 << bit) != 0 {
            self.words[word] &= !(1 << bit);
            self.count -= 1;
        }
    }

    /// Positions in ascending order. Advances the lowest-word hint past empty
    /// words first, so draining stage by stage scans each word about once.
    fn iter(&mut self) -> impl Iterator<Item = usize> + '_ {
        while self.lowest_word < self.words.len() && self.words[self.lowest_word] == 0 {
            self.lowest_word += 1;
        }
        self.words
            .iter()
            .enumerate()
            .skip(self.lowest_word)
            .flat_map(|(word_index, word)| {
                let base = word_index * Self::WORD_BITS;
                // Each step clears the lowest set bit.
                std::iter::successors((*word != 0).then_some(*word), |rest| {
                    let rest = rest & (rest - 1);
                    (rest != 0).then_some(rest)
                })
                .map(move |bits| base + bits.trailing_zeros() as usize)
            })
    }
}

fn drain_next_ready_stage(
    ready: &mut ReadySet,
    index: &ValidatedStepIndex<'_>,
    parallel_dependency_groups: &BTreeMap<usize, Vec<ParallelDependencyGroup>>,
) -> Result<ReadyStage, ExecPlanError> {
    let Some(first) = ready.iter().next() else {
        return Err(ExecPlanError::InvalidExecutionStage { actual: 0 });
    };
    let stage = match is_barrier_step(index, first) {
        true => ReadyStage::serial(first),
        false => match ready_parallel_group_at(first, ready, index, parallel_dependency_groups) {
            Some((dependencies, policy)) => ReadyStage::new(dependencies, policy),
            None => ReadyStage::default_parallel(
                ready
                    .iter()
                    .take_while(|position| {
                        !is_barrier_step(index, *position)
                            && (*position == first
                                || !parallel_dependency_groups.contains_key(position))
                    })
                    .collect(),
            ),
        },
    };
    for position in &stage.positions {
        ready.remove(*position);
    }
    Ok(stage)
}

fn is_barrier_step(index: &ValidatedStepIndex<'_>, position: usize) -> bool {
    matches!(index.at(position).schedule, ExecSchedule::Barrier)
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
    positions: Vec<usize>,
    policy: ExecParallelStagePolicy,
}

impl ReadyStage {
    fn new(positions: Vec<usize>, policy: ExecParallelStagePolicy) -> Self {
        Self { positions, policy }
    }

    fn serial(position: usize) -> Self {
        Self::new(vec![position], ExecParallelStagePolicy::for_ready_width(1))
    }

    fn default_parallel(positions: Vec<usize>) -> Self {
        let policy = ExecParallelStagePolicy::for_ready_width(positions.len());
        Self::new(positions, policy)
    }
}

#[derive(Debug, Clone)]
struct ParallelDependencyGroup {
    /// Dependency positions in the step's own order, which the stage keeps.
    dependencies: Vec<usize>,
    sorted_dependencies: Vec<usize>,
    policy: ExecParallelStagePolicy,
}

impl ParallelDependencyGroup {
    fn from_step(index: &ValidatedStepIndex<'_>, step: &ExecStep) -> Option<Self> {
        let ExecSchedule::Parallel {
            max_concurrency,
            preserve_order,
        } = &step.schedule
        else {
            return None;
        };
        let dependencies = step
            .dependencies
            .iter()
            .map(|dependency| dependency_position(index, *dependency))
            .collect::<Vec<_>>();
        let mut sorted_dependencies = dependencies.clone();
        sorted_dependencies.sort_unstable();
        Some(Self {
            dependencies,
            sorted_dependencies,
            policy: ExecParallelStagePolicy::new(*max_concurrency, *preserve_order),
        })
    }

    fn is_ready_prefix(&self, ready: &mut ReadySet, index: &ValidatedStepIndex<'_>) -> bool {
        self.sorted_dependencies
            .iter()
            .all(|position| !is_barrier_step(index, *position))
            && ready
                .iter()
                .take(self.sorted_dependencies.len())
                .eq(self.sorted_dependencies.iter().copied())
    }
}

/// Parallel steps' dependency groups, keyed by their first dependency's
/// position and shortest first.
fn parallel_dependency_groups(
    index: &ValidatedStepIndex<'_>,
) -> BTreeMap<usize, Vec<ParallelDependencyGroup>> {
    let mut groups = BTreeMap::<usize, Vec<ParallelDependencyGroup>>::new();
    for group in index
        .steps()
        .filter_map(|step| ParallelDependencyGroup::from_step(index, step))
    {
        let Some(first) = group.sorted_dependencies.first().copied() else {
            continue;
        };
        groups.entry(first).or_default().push(group);
    }
    for groups in groups.values_mut() {
        groups.sort_by_key(|group| group.dependencies.len());
    }
    groups
}

/// The dependencies and policy of the first group at `first` whose
/// dependencies are exactly the start of the ready set.
fn ready_parallel_group_at(
    first: usize,
    ready: &mut ReadySet,
    index: &ValidatedStepIndex<'_>,
    parallel_dependency_groups: &BTreeMap<usize, Vec<ParallelDependencyGroup>>,
) -> Option<(Vec<usize>, ExecParallelStagePolicy)> {
    parallel_dependency_groups
        .get(&first)?
        .iter()
        .find(|group| group.is_ready_prefix(ready, index))
        .map(|group| (group.dependencies.clone(), group.policy))
}
