//! Resumable pure per-row branches. A child frame is activated only when asked
//! for its first output; captures and effects are excluded by planner validation.
use super::*;

pub(super) struct Branch<'a> {
    pub(super) plan: &'a exec::ExecBranchPlan,
    pub(super) input: Box<Cursor<'a>>,
    parent: Option<ExecutionRow>,
    next_branch: usize,
    current: Option<Box<Cursor<'a>>>,
    emitted: bool,
    saw_bindings: bool,
    kinds: BTreeSet<bool>,
}

impl<'a> Branch<'a> {
    pub(super) fn new(plan: &'a exec::ExecBranchPlan, input: Box<Cursor<'a>>) -> Self {
        Self {
            plan,
            input,
            parent: None,
            next_branch: 0,
            current: None,
            emitted: false,
            saw_bindings: false,
            kinds: BTreeSet::new(),
        }
    }

    pub(super) async fn next(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<ExecutionValue>> {
        let name = match self.plan {
            exec::ExecBranchPlan::Union(_) => "branch.union",
            exec::ExecBranchPlan::Coalesce(_) => "branch.coalesce",
            exec::ExecBranchPlan::Optional(_) => "branch.optional",
            exec::ExecBranchPlan::Choose { .. } | exec::ExecBranchPlan::ChooseElse { .. } => {
                unreachable!()
            }
        };
        loop {
            ctx.check_execution_deadline()?;
            if let Some(current) = &mut self.current {
                if let Some(item) = current.next(ctx).await? {
                    let rows = ctx.stream_rows(item, name)?;
                    for row in &rows {
                        self.saw_bindings |= !row.bindings.is_empty();
                        if let Some(element) = &row.current {
                            self.kinds.insert(matches!(element, ElementRef::Node(_)));
                        }
                    }
                    if matches!(self.plan, exec::ExecBranchPlan::Union(_))
                        && self.saw_bindings
                        && self.kinds.len() > 1
                    {
                        return Err(HelixDbError::Query(
                            "union row branches produced mixed current element types".into(),
                        ));
                    }
                    self.emitted = true;
                    return Ok(Some(ExecutionValue::Stream(rows)));
                }
                self.current = None;
                if self.emitted && !matches!(self.plan, exec::ExecBranchPlan::Union(_)) {
                    self.parent = None;
                }
            }
            if self.parent.is_none() {
                let Some(item) = self.input.next(ctx).await? else {
                    return Ok(None);
                };
                self.parent = ctx.stream_rows(item, name)?.pop();
                self.next_branch = 0;
                self.emitted = false;
            }
            let branch = match self.plan {
                exec::ExecBranchPlan::Union(branches) => branches.as_ref().get(self.next_branch),
                exec::ExecBranchPlan::Coalesce(branches) => branches.as_ref().get(self.next_branch),
                exec::ExecBranchPlan::Optional(branch) => {
                    (self.next_branch == 0).then_some(branch.as_ref())
                }
                exec::ExecBranchPlan::Choose { .. } | exec::ExecBranchPlan::ChooseElse { .. } => {
                    unreachable!("whole-input branches use the full-input adapter")
                }
            };
            let Some(branch) = branch else {
                let parent = self.parent.take().expect("branch has a parent");
                if matches!(self.plan, exec::ExecBranchPlan::Optional(_)) && !self.emitted {
                    return Ok(Some(ExecutionValue::Stream(vec![parent])));
                }
                continue;
            };
            self.next_branch += 1;
            let context = ExecutionValue::Stream(vec![self
                .parent
                .as_ref()
                .expect("branch has a parent")
                .clone()]);
            let child = Cursor::scoped_subplan(ctx, branch, context)?;
            child.shape.require_rows(name)?;
            self.current = Some(Box::new(child));
        }
    }
}

impl<'a> Cursor<'a> {
    pub(super) fn scoped_subplan(
        ctx: &mut ExecutionContext<'_>,
        plan: &'a exec::ExecutableSubplan,
        context: ExecutionValue,
    ) -> Result<Self> {
        let variable = ir::NonEmptyString::new("$context").expect("constant variable");
        let previous = ctx.variables.insert(variable.clone(), context.clone());
        let result = (|| {
            let mut cursors = BTreeMap::new();
            let by_id = plan
                .steps()
                .iter()
                .map(|step| (step.id, step))
                .collect::<BTreeMap<_, _>>();
            for id in plan.execution_order().step_ids() {
                let step = by_id[&id];
                let inputs = step
                    .dependencies
                    .iter()
                    .map(|dependency| {
                        cursors
                            .remove(dependency)
                            .expect("validated exclusive subplan tree")
                    })
                    .collect();
                let cursor = match &step.op {
                    exec::ExecOp::Merge { mode } => Self::merge(inputs, *mode)?,
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
                    | exec::ExecOp::Noop => Self::concat(inputs)?.wrap(ctx, &step.op)?,
                };
                cursors.insert(id, cursor);
            }
            let input = cursors.remove(&plan.root()).expect("subplan root exists");
            Ok(Self {
                shape: input.shape,
                node: Node::Scoped {
                    input: Box::new(input),
                    context,
                },
                produced_rows: 0,
                row_mode: false,
                name: "branch frame",
            })
        })();
        match previous {
            Some(value) => {
                ctx.variables.insert(variable, value);
            }
            None => {
                ctx.variables.remove(&variable);
            }
        }
        result
    }
}
