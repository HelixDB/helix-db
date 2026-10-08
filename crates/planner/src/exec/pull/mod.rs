//! Validated pull regions. These describe execution, never access-path selection.
use std::collections::{BTreeMap, BTreeSet};

use super::positions::StepPositions;
use super::{ExecCondition, ExecExecutionOrder, ExecOp, ExecStep, ExecStepId, ExecVariableOp};
use crate::ir;

/// Whether an operation can participate in a demand-driven region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecPullCapability {
    /// Output can be produced incrementally.
    Incremental,
    /// Some ordered or membership state must be prepared first.
    Prepared,
    /// Any demanded output needs the complete input.
    FullInput,
    /// Effects, scope or a data-dependent value kind require whole-value execution.
    Boundary,
}

impl ExecPullCapability {
    /// Exhaustive operation contract; new executable operations require a decision.
    pub fn of(op: &ExecOp) -> Self {
        match op {
            ExecOp::Access { .. }
            | ExecOp::KvRead(_)
            | ExecOp::Expand { .. }
            | ExecOp::Merge { .. } => Self::Prepared,
            ExecOp::Filter { .. }
            | ExecOp::IndexMembership { .. }
            | ExecOp::Limit { .. }
            | ExecOp::Skip { .. }
            | ExecOp::Range { .. }
            | ExecOp::Distinct
            | ExecOp::Noop => Self::Incremental,
            ExecOp::Project { projection } => match projection {
                ir::ProjectionPlan::Exists
                | ir::ProjectionPlan::Id
                | ir::ProjectionPlan::Values(_)
                | ir::ProjectionPlan::ValueMap(_)
                | ir::ProjectionPlan::Project(_)
                | ir::ProjectionPlan::ProjectBindings { .. }
                | ir::ProjectionPlan::Label
                | ir::ProjectionPlan::EdgeProperties => Self::Incremental,
            },
            ExecOp::Count { .. }
            | ExecOp::Order { .. }
            | ExecOp::Aggregate { .. }
            | ExecOp::VectorSearch { .. }
            | ExecOp::TextSearch { .. }
            | ExecOp::ShortestPath { .. } => Self::FullInput,
            ExecOp::Variable { op } => match op {
                ExecVariableOp::SourceInject { .. } => Self::Incremental,
                ExecVariableOp::Stream(op) => match op {
                    ir::StreamVariableOp::As(_) | ir::StreamVariableOp::Store(_) => Self::Boundary,
                    ir::StreamVariableOp::Bind(_)
                    | ir::StreamVariableOp::Within(_)
                    | ir::StreamVariableOp::Without(_)
                    | ir::StreamVariableOp::Select(_)
                    | ir::StreamVariableOp::Inject(_) => Self::Incremental,
                },
            },
            ExecOp::Reserved { op } => match op {
                ir::ReservedOp::Fold | ir::ReservedOp::Unfold => Self::FullInput,
                ir::ReservedOp::Path
                | ir::ReservedOp::SimplePath
                | ir::ReservedOp::WithSack(_)
                | ir::ReservedOp::SackSet(_)
                | ir::ReservedOp::SackAdd(_)
                | ir::ReservedOp::SackGet => Self::Incremental,
            },
            ExecOp::Branch { plan } => {
                let pure = match plan {
                    super::ExecBranchPlan::Union(branches) => {
                        branches.as_ref().iter().all(Self::pure_subplan)
                    }
                    super::ExecBranchPlan::Coalesce(branches) => {
                        branches.as_ref().iter().all(Self::pure_subplan)
                    }
                    super::ExecBranchPlan::Optional(branch) => Self::pure_subplan(branch),
                    // A conditional can return rows, scalars, a count or a
                    // folded value depending on which child is selected. Keep
                    // that whole-value boundary; each child's own program can
                    // still satisfy demand internally.
                    super::ExecBranchPlan::Choose { .. }
                    | super::ExecBranchPlan::ChooseElse { .. } => return Self::Boundary,
                };
                if pure {
                    Self::Prepared
                } else {
                    Self::Boundary
                }
            }
            ExecOp::Repeat { plan } => {
                if Self::pure_subplan(&plan.body) {
                    Self::Prepared
                } else {
                    Self::Boundary
                }
            }
            ExecOp::ForEach { .. }
            | ExecOp::Mutation { .. }
            | ExecOp::IndexDdl { .. }
            | ExecOp::Barrier { .. } => Self::Boundary,
        }
    }
    /// A pure, exclusively consumed tree can suspend without exposing a frame's
    /// partial results or skipping effects. Other subplans remain boundaries.
    pub fn pure_subplan(plan: &super::ExecutableSubplan) -> bool {
        let positions = StepPositions::new(plan.steps().iter().map(|step| step.id))
            .expect("validated subplans have unique step IDs");
        let position = |id| {
            positions
                .position(id)
                .expect("validated dependencies are steps")
        };
        let mut uses = vec![0usize; positions.len()];
        for step in plan.steps() {
            for dependency in &step.dependencies {
                uses[position(*dependency)] += 1;
            }
        }
        uses[position(plan.root())] += 1;
        // Purity is independent of whether this subplan itself has a window:
        // an enclosing branch can supply demand to an exclusive child tree.
        plan.steps().iter().all(|step| {
            uses[position(step.id)] == 1
                && matches!(step.condition, ExecCondition::Always)
                && matches!(step.output, ir::BatchOutputPlan::Discard)
                && Self::of(&step.op) != Self::Boundary
        })
    }
}

/// An exclusive producer tree, stored in dependency order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecPullRegion {
    steps: Vec<ExecStepId>,
}

impl ExecPullRegion {
    /// Original executable IDs, including the terminal step.
    pub fn steps(&self) -> &[ExecStepId] {
        &self.steps
    }
}

/// Derived, non-serialized execution regions for one validated DAG.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecProgram {
    regions: BTreeMap<ExecStepId, ExecPullRegion>,
    absorbed: BTreeSet<ExecStepId>,
}

#[cfg(test)]
thread_local! {
    pub(super) static DERIVATION_VISITS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

impl ExecProgram {
    /// Per-step state lives in vectors indexed by step position (see
    /// [`StepPositions`]); a component's members are a list plus marks that
    /// are cleared again before the next component.
    pub(in crate::exec) fn derive(
        steps: &[ExecStep],
        order: &ExecExecutionOrder,
        root: ExecStepId,
    ) -> Self {
        #[cfg(test)]
        DERIVATION_VISITS.set((0, 0));
        let positions = StepPositions::new(steps.iter().map(|step| step.id))
            .expect("validated steps have unique IDs");
        let position = |id| positions.position(id).expect("validated IDs are steps");
        let mut by_position = steps.iter().collect::<Vec<_>>();
        by_position.sort_unstable_by_key(|step| step.id);
        let capabilities = by_position
            .iter()
            .map(|step| ExecPullCapability::of(&step.op))
            .collect::<Vec<_>>();
        let mut uses = vec![0usize; positions.len()];
        for step in steps {
            for id in &step.dependencies {
                uses[position(*id)] += 1;
            }
            let ExecCondition::PreviousStepNotEmpty { dependency } = step.condition else {
                continue;
            };
            uses[position(dependency)] += 1;
        }
        uses[position(root)] += 1;
        // Captures and effects separate epochs even if an unrelated ready step
        // appears between a producer and consumer in topological order.
        let ids = order.step_ids().collect::<Vec<_>>();
        let mut epochs = vec![0usize; positions.len()];
        let mut ranks = vec![0usize; positions.len()];
        let mut epoch = 0usize;
        for (rank, id) in ids.iter().enumerate() {
            let at = position(*id);
            if capabilities[at] == ExecPullCapability::Boundary {
                epoch += 1;
            }
            epochs[at] = epoch;
            ranks[at] = rank;
            if !matches!(by_position[at].output, ir::BatchOutputPlan::Discard) {
                epoch += 1;
            }
        }
        let mut program = Self::default();
        // Eligible edges have one consumer, so components do not overlap.
        // Rejected components contain no demand anywhere in their ancestry and
        // need not be reconsidered for each of their remaining steps.
        let mut examined = vec![false; positions.len()];
        let mut member = vec![false; positions.len()];
        for id in ids.into_iter().rev() {
            let at = position(id);
            if examined[at] || capabilities[at] == ExecPullCapability::Boundary {
                continue;
            }
            let terminal = by_position[at];
            member[at] = true;
            let mut members = vec![at];
            let mut pending = vec![at];
            while let Some(current) = pending.pop() {
                #[cfg(test)]
                DERIVATION_VISITS.with(|visits| {
                    let (nodes, edges) = visits.get();
                    visits.set((nodes + 1, edges));
                });
                for dependency in &by_position[current].dependencies {
                    #[cfg(test)]
                    DERIVATION_VISITS.with(|visits| {
                        let (nodes, edges) = visits.get();
                        visits.set((nodes, edges + 1));
                    });
                    let parent_at = position(*dependency);
                    let parent = by_position[parent_at];
                    if uses[parent_at] != 1
                        || !matches!(parent.output, ir::BatchOutputPlan::Discard)
                        || parent.condition != terminal.condition
                        || epochs[parent_at] != epochs[at]
                        || capabilities[parent_at] == ExecPullCapability::Boundary
                        || member[parent_at]
                    {
                        continue;
                    }
                    member[parent_at] = true;
                    members.push(parent_at);
                    pending.push(parent_at);
                }
            }
            for at in &members {
                examined[*at] = true;
                member[*at] = false;
            }
            // Without a window or terminal cardinality consumer, ordinary
            // whole-value operators avoid per-row polling overhead. Their
            // existing implementations also serve effect/materialization edges.
            let needs_demand = members.iter().any(|at| {
                matches!(
                    by_position[*at].op,
                    ExecOp::Limit { .. }
                        | ExecOp::Range { .. }
                        | ExecOp::Count { .. }
                        | ExecOp::Project {
                            projection: ir::ProjectionPlan::Exists
                        }
                )
            });
            if members.len() > 1 && needs_demand {
                members.sort_unstable_by_key(|at| ranks[*at]);
                program.absorbed.extend(
                    members
                        .iter()
                        .filter(|member| **member != at)
                        .map(|member| by_position[*member].id),
                );
                program.regions.insert(
                    id,
                    ExecPullRegion {
                        steps: members.iter().map(|at| by_position[*at].id).collect(),
                    },
                );
            }
        }
        program
    }

    /// Regions in deterministic terminal-ID order. Original step IDs can be
    /// joined to `ExecPullCapability::of` for preparation and barrier details.
    pub fn regions(&self) -> impl Iterator<Item = (ExecStepId, &ExecPullRegion)> {
        self.regions.iter().map(|(id, region)| (*id, region))
    }

    /// A source step executed only when its region requests rows.
    pub fn is_absorbed(&self, id: ExecStepId) -> bool {
        self.absorbed.contains(&id)
    }

    /// Region whose observable output belongs to this step.
    pub fn region(&self, id: ExecStepId) -> Option<&ExecPullRegion> {
        self.regions.get(&id)
    }
}

#[cfg(test)]
mod tests;
