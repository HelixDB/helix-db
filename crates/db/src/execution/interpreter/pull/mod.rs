//! Request-scoped, demand-driven execution of planner-validated pure regions.
mod branch;
mod build;
#[path = "count.rs"]
pub(super) mod cardinality;
#[cfg(test)]
pub(super) mod metrics;
mod poll;
mod repeat;
mod scope;
mod source;
mod value;

use super::*;
use futures::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet};
use value::{Items, Shape};

struct Cursor<'a> {
    shape: Shape,
    node: Node<'a>,
    produced_rows: usize,
    row_mode: bool,
    name: &'static str,
}

enum Node<'a> {
    Items(Items),
    StreamCount {
        plan: &'a exec::ExecCountStreamPlan,
        input: Option<Box<Cursor<'a>>>,
    },
    Inject {
        input: Box<Cursor<'a>>,
        variable: &'a ir::NonEmptyString,
        appended: Option<Items>,
        input_done: bool,
    },
    InputCount {
        input: Option<Box<Cursor<'a>>>,
        window: count::EvaluatedCountWindow,
    },
    Membership {
        input: Box<Cursor<'a>>,
        variable: &'a ir::NonEmptyString,
        exclude: bool,
        members: Option<BTreeSet<ElementRef>>,
    },
    Branch(branch::Branch<'a>),
    Repeat(repeat::Repeat<'a>),
    Scoped {
        input: Box<Cursor<'a>>,
        context: ExecutionValue,
    },
    CountLeaf {
        plan: &'a exec::ExecCountCursorPlan,
        dependency: Option<ExecutionValue>,
        pending: Option<Items>,
    },
    OrderedDistinct {
        input: Box<Cursor<'a>>,
        previous: Option<ExecutionRow>,
    },
    CountSet {
        driver: Box<Cursor<'a>>,
        inputs: std::vec::IntoIter<Cursor<'a>>,
        current: Option<Box<Cursor<'a>>>,
        intersect: bool,
        sets: Option<Vec<BTreeSet<ExecutionRow>>>,
        seen: BTreeSet<ExecutionRow>,
        in_driver: bool,
    },
    Concat {
        inputs: std::vec::IntoIter<Cursor<'a>>,
        current: Option<Box<Cursor<'a>>>,
    },
    Intersect {
        driver: Box<Cursor<'a>>,
        rest: Option<Vec<Cursor<'a>>>,
        sets: Vec<BTreeSet<ExecutionRow>>,
        emitted: BTreeSet<ExecutionRow>,
    },
    Source {
        source: Box<source::Source<'a>>,
        input: Option<Box<Cursor<'a>>>,
    },
    Expand {
        plan: &'a ir::ExpandPlan,
        input: Box<Cursor<'a>>,
        label: Option<Option<access::expand::EdgeOutputExpansionLabel<'a>>>,
        parent: Option<ExecutionRow>,
        ids: std::vec::IntoIter<u64>,
    },
    Map {
        op: &'a exec::ExecOp,
        input: Box<Cursor<'a>>,
        pending: Items,
        distinct: Option<BTreeSet<stream::DistinctKey<ExecutionScalar>>>,
    },
    Window {
        input: Box<Cursor<'a>>,
        skip: usize,
        remaining: Demand,
    },
    Distinct {
        input: Box<Cursor<'a>>,
        rows: BTreeSet<stream::RowDistinctKey>,
        scalars: BTreeSet<stream::DistinctKey<ExecutionScalar>>,
    },
    Exists(Option<Box<Cursor<'a>>>),
    Filter {
        predicate: &'a ir::PredicatePlan,
        input: Box<Cursor<'a>>,
    },
    Full {
        op: &'a exec::ExecOp,
        input: Option<Box<Cursor<'a>>>,
    },
}

enum Demand {
    All,
    Take(std::num::NonZeroUsize),
    Done,
}
impl Demand {
    fn take(count: usize) -> Self {
        std::num::NonZeroUsize::new(count).map_or(Self::Done, Self::Take)
    }
    fn consume(&mut self) {
        if let Self::Take(n) = self {
            *self = Self::take(n.get() - 1);
        }
    }
}

impl ExecutionContext<'_> {
    pub(super) async fn execute_access_cursor(
        &mut self,
        plan: &exec::ExecAccessPlan,
    ) -> Result<ExecutionValue> {
        let mut source = source::Source::new(self, source::Plan::Access(plan))?;
        let mut out = ExecutionValue::Stream(Vec::new());
        while let Some(item) = Box::pin(source.next(self)).await? {
            value::append(&mut out, item)?;
        }
        Ok(out)
    }

    pub(super) async fn execute_pull_region(
        &mut self,
        region: &exec::ExecPullRegion,
        by_id: &BTreeMap<exec::ExecStepId, &exec::ExecStep>,
    ) -> Result<ExecutionValue> {
        let terminal = *region.steps().last().expect("region is nonempty");
        tracing::trace!(
            terminal = terminal.get(),
            operators = region.steps().len(),
            "execute pull region"
        );
        let first = by_id[&region.steps()[0]];
        let allowed = self.condition_allows(&first.condition)?;
        for id in region.steps() {
            self.release_condition_reference(&by_id[id].condition);
        }
        if !allowed {
            for id in region.steps() {
                self.release_dependency_references(&by_id[id].dependencies);
            }
            return Ok(ExecutionValue::Stream(Vec::new()));
        }
        // Resolve deferred writes before any source iterator is opened. No
        // mutation is permitted inside a pull region.
        for id in region.steps() {
            self.flush_required_mutations(mutation::visibility::required_for(&by_id[id].op))
                .await?;
        }
        let mut cursors = BTreeMap::new();
        for id in region.steps() {
            let step = by_id[id];
            let mut inputs = Vec::new();
            for dependency in &step.dependencies {
                let input = match cursors.remove(dependency) {
                    Some(cursor) => {
                        self.release_dependency_references(&[*dependency]);
                        cursor
                    }
                    None => Cursor::materialized(self.dependency_input(&[*dependency])?)?,
                };
                inputs.push(input);
            }
            let cursor = match &step.op {
                exec::ExecOp::Merge { mode } => Cursor::merge(inputs, *mode)?,
                exec::ExecOp::Access { .. }
                | exec::ExecOp::Count { .. }
                | exec::ExecOp::KvRead(_)
                | exec::ExecOp::Expand { .. }
                | exec::ExecOp::VectorSearch { .. }
                | exec::ExecOp::TextSearch { .. }
                | exec::ExecOp::Filter { .. }
                | exec::ExecOp::Limit { .. }
                | exec::ExecOp::Skip { .. }
                | exec::ExecOp::Range { .. }
                | exec::ExecOp::Distinct
                | exec::ExecOp::Order { .. }
                | exec::ExecOp::Project { .. }
                | exec::ExecOp::Aggregate { .. }
                | exec::ExecOp::Variable { .. }
                | exec::ExecOp::Branch { .. }
                | exec::ExecOp::Repeat { .. }
                | exec::ExecOp::ShortestPath { .. }
                | exec::ExecOp::Mutation { .. }
                | exec::ExecOp::IndexDdl { .. }
                | exec::ExecOp::Reserved { .. }
                | exec::ExecOp::ForEach { .. }
                | exec::ExecOp::Barrier { .. }
                | exec::ExecOp::Noop => Cursor::concat(inputs)?.wrap(self, &step.op)?,
            };
            cursors.insert(*id, cursor);
        }
        cursors
            .remove(&terminal)
            .expect("region has a terminal cursor")
            .drain(self)
            .await
    }
}
