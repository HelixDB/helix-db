//! Cursor construction and value-shape contracts.
use super::*;

impl<'a> Cursor<'a> {
    pub(super) fn materialized(value: ExecutionValue) -> Result<Self> {
        Ok(Self {
            shape: Shape::of(&value),
            node: Node::Items(Items::new(value)),
            produced_rows: 0,
            row_mode: false,
            name: "input()",
        })
    }

    pub(super) fn concat(mut inputs: Vec<Self>) -> Result<Self> {
        if inputs.len() == 1 {
            return Ok(inputs.pop().expect("one input"));
        }
        if inputs.iter().any(|input| input.shape == Shape::Lifecycle) {
            return Err(HelixDbError::Query(
                "cannot concatenate index lifecycle dependency outputs".into(),
            ));
        }
        if inputs.iter().any(|input| input.shape == Shape::Folded) {
            return Err(HelixDbError::Query(
                "cannot concatenate folded stream dependency output; unfold it first".into(),
            ));
        }
        let shape = inputs
            .first()
            .map_or(Ok(Shape::Rows), |input| input.shape.window("concat"))?;
        for input in &inputs {
            if input.shape.window("concat")? != shape {
                return Err(HelixDbError::Query(
                    "cannot concatenate mixed stream and scalar dependency outputs".into(),
                ));
            }
        }
        Ok(Self {
            shape,
            node: Node::Concat {
                inputs: inputs.into_iter(),
                current: None,
            },
            produced_rows: 0,
            row_mode: false,
            name: "concat()",
        })
    }

    pub(super) fn merge(mut inputs: Vec<Self>, mode: exec::ExecMergeMode) -> Result<Self> {
        // Merge consumes row streams; ordinary dependency concatenation also
        // supports scalar terminals and is a separate contract.
        for input in &inputs {
            input.shape.require_rows("merge")?;
        }
        match mode {
            exec::ExecMergeMode::Concat => Self::concat(inputs),
            exec::ExecMergeMode::Union => {
                let input = Self::concat(inputs)?;
                Ok(Self {
                    shape: Shape::Rows,
                    node: Node::Distinct {
                        input: Box::new(input),
                        rows: BTreeSet::new(),
                        scalars: BTreeSet::new(),
                    },
                    produced_rows: 0,
                    row_mode: false,
                    name: "union()",
                })
            }
            exec::ExecMergeMode::Intersect => {
                if inputs.is_empty() {
                    return Self::materialized(ExecutionValue::Stream(Vec::new()));
                }
                let driver = Box::new(inputs.remove(0));
                Ok(Self {
                    shape: Shape::Rows,
                    node: Node::Intersect {
                        driver,
                        rest: Some(inputs),
                        sets: Vec::new(),
                        emitted: BTreeSet::new(),
                    },
                    produced_rows: 0,
                    row_mode: false,
                    name: "intersect()",
                })
            }
        }
    }

    pub(super) fn wrap(self, ctx: &ExecutionContext<'_>, op: &'a exec::ExecOp) -> Result<Self> {
        if let exec::ExecOp::Count { plan } = op {
            cardinality::validate_contract(ctx, plan)?;
            if matches!(plan.dependency(), Ok(exec::ExecCountDependency::Rows))
                && self.shape != Shape::Rows
            {
                return Err(HelixDbError::InvariantViolation(
                    "count plan expected rows".into(),
                ));
            }
            if matches!(plan.dependency(), Ok(exec::ExecCountDependency::Scalars))
                && !matches!(self.shape, Shape::Scalars | Shape::Count | Shape::Bool)
            {
                return Err(HelixDbError::InvariantViolation(
                    "count plan expected scalar items".into(),
                ));
            }
        }
        let mut shape = self.shape;
        let input = Box::new(self);
        let node = match op {
            exec::ExecOp::Count { plan }
                if matches!(plan.as_ref(), exec::ExecCountPlan::Stream(_)) =>
            {
                let exec::ExecCountPlan::Stream(plan) = plan.as_ref() else {
                    unreachable!()
                };
                shape = Shape::Count;
                Node::StreamCount {
                    plan,
                    input: Some(input),
                }
            }
            exec::ExecOp::Repeat { plan } => {
                shape.require_rows("repeat")?;
                Node::Repeat(repeat::Repeat::new(plan, input))
            }
            exec::ExecOp::Count { plan }
                if matches!(
                    plan.as_ref(),
                    exec::ExecCountPlan::InputRows { .. } if shape == Shape::Rows
                ) || matches!(plan.as_ref(), exec::ExecCountPlan::InputScalars { .. })
                    && matches!(shape, Shape::Scalars | Shape::Count | Shape::Bool) =>
            {
                let (exec::ExecCountPlan::InputRows { window }
                | exec::ExecCountPlan::InputScalars { window }) = plan.as_ref()
                else {
                    unreachable!()
                };
                shape = Shape::Count;
                Node::InputCount {
                    input: Some(input),
                    window: ctx.count_window(window)?,
                }
            }
            exec::ExecOp::Variable {
                op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Inject(variable)),
            } => {
                shape.require_rows("inject")?;
                Shape::of(ctx.variable_value(variable)?).require_rows("inject")?;
                Node::Inject {
                    input,
                    variable,
                    appended: None,
                    input_done: false,
                }
            }
            exec::ExecOp::Variable {
                op:
                    exec::ExecVariableOp::Stream(
                        ir::StreamVariableOp::Within(variable)
                        | ir::StreamVariableOp::Without(variable),
                    ),
            } => {
                shape.require_rows("membership")?;
                Shape::of(ctx.variable_value(variable)?).require_membership()?;
                Node::Membership {
                    input,
                    variable,
                    exclude: matches!(
                        op,
                        exec::ExecOp::Variable {
                            op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Without(_))
                        }
                    ),
                    members: None,
                }
            }
            exec::ExecOp::Branch { plan }
                if matches!(
                    plan,
                    exec::ExecBranchPlan::Union(_)
                        | exec::ExecBranchPlan::Coalesce(_)
                        | exec::ExecBranchPlan::Optional(_)
                ) =>
            {
                shape.require_rows("branch")?;
                Node::Branch(branch::Branch::new(plan, input))
            }
            exec::ExecOp::Access { plan } => {
                shape = Shape::Rows;
                Node::Source {
                    source: Box::new(source::Source::new(ctx, source::Plan::Access(plan))?),
                    input: Some(input),
                }
            }
            exec::ExecOp::KvRead(plan) => {
                shape = Shape::Rows;
                Node::Source {
                    source: Box::new(source::Source::new(ctx, source::Plan::Kv(plan))?),
                    input: Some(input),
                }
            }
            exec::ExecOp::Limit { count } => {
                shape = shape.window("limit")?;
                Node::Window {
                    input,
                    skip: 0,
                    remaining: Demand::take(stream::eval_stream_bound(count, &ctx.params)?),
                }
            }
            exec::ExecOp::Skip { count } => {
                shape = shape.window("skip")?;
                Node::Window {
                    input,
                    skip: stream::eval_stream_bound(count, &ctx.params)?,
                    remaining: Demand::All,
                }
            }
            exec::ExecOp::Range { range } => {
                shape = shape.window("range")?;
                let (start, end) = ctx.stream_range(range)?;
                Node::Window {
                    input,
                    skip: start,
                    remaining: Demand::take(end.saturating_sub(start)),
                }
            }
            exec::ExecOp::Distinct => {
                shape = shape.window("distinct")?;
                Node::Distinct {
                    input,
                    rows: BTreeSet::new(),
                    scalars: BTreeSet::new(),
                }
            }
            exec::ExecOp::Project {
                projection: ir::ProjectionPlan::Exists,
            } => {
                shape = shape.projection(&ir::ProjectionPlan::Exists)?;
                Node::Exists(Some(input))
            }
            exec::ExecOp::Expand { plan } => {
                shape.require_rows("expand")?;
                Node::Expand {
                    plan,
                    input,
                    label: None,
                    parent: None,
                    ids: Vec::new().into_iter(),
                }
            }
            exec::ExecOp::Filter { predicate } => {
                shape.require_rows("filter")?;
                Node::Filter { predicate, input }
            }
            exec::ExecOp::IndexMembership { plan } => {
                shape.require_rows("index membership")?;
                Node::IndexMembership {
                    plan,
                    input,
                    prepared: None,
                }
            }
            exec::ExecOp::Project { .. } | exec::ExecOp::Noop => {
                if let exec::ExecOp::Project { projection } = op {
                    shape = shape.projection(projection)?;
                }
                let distinct = matches!(
                    op,
                    exec::ExecOp::Project {
                        projection: ir::ProjectionPlan::ProjectBindings {
                            dedup: ir::ProjectionDedupMode::Distinct,
                            ..
                        }
                    }
                )
                .then(BTreeSet::new);
                Node::Map {
                    op,
                    input,
                    pending: Items::Rows(Vec::new().into_iter()),
                    distinct,
                }
            }
            exec::ExecOp::Reserved {
                op:
                    ir::ReservedOp::Path
                    | ir::ReservedOp::SimplePath
                    | ir::ReservedOp::WithSack(_)
                    | ir::ReservedOp::SackSet(_)
                    | ir::ReservedOp::SackAdd(_)
                    | ir::ReservedOp::SackGet,
            }
            | exec::ExecOp::Variable {
                op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Bind(_)),
            } => {
                shape.require_rows(row_mode::op_name(op))?;
                Node::Map {
                    op,
                    input,
                    pending: Items::Rows(Vec::new().into_iter()),
                    distinct: None,
                }
            }
            exec::ExecOp::Count { .. }
            | exec::ExecOp::VectorSearch { .. }
            | exec::ExecOp::TextSearch { .. }
            | exec::ExecOp::Order { .. }
            | exec::ExecOp::Aggregate { .. }
            | exec::ExecOp::Variable { .. }
            | exec::ExecOp::Branch { .. }
            | exec::ExecOp::ShortestPath { .. }
            | exec::ExecOp::Mutation { .. }
            | exec::ExecOp::IndexDdl { .. }
            | exec::ExecOp::Merge { .. }
            | exec::ExecOp::Reserved { .. }
            | exec::ExecOp::ForEach { .. }
            | exec::ExecOp::Barrier { .. } => {
                // Validate full-input contracts even if zero demand prevents polling.
                shape = match op {
                    exec::ExecOp::Access { .. } | exec::ExecOp::KvRead(_) => Shape::Rows,
                    exec::ExecOp::VectorSearch { .. }
                    | exec::ExecOp::TextSearch { .. }
                    | exec::ExecOp::Order { .. } => {
                        shape.require_rows(row_mode::op_name(op))?;
                        Shape::Rows
                    }
                    exec::ExecOp::Count { .. } => Shape::Count,
                    exec::ExecOp::Aggregate {
                        aggregate:
                            ir::AggregatePlan::AggregateBy {
                                function: helix_ast::traversal::AggregateFunction::Count,
                                ..
                            },
                    } if matches!(shape, Shape::Scalars | Shape::Count | Shape::Bool) => {
                        Shape::Count
                    }
                    exec::ExecOp::Aggregate { aggregate } => {
                        shape.window("aggregate")?;
                        if shape != Shape::Rows {
                            return Err(HelixDbError::Query(format!(
                                "aggregate {aggregate:?} expected element stream input, got scalar terminal input"
                            )));
                        }
                        Shape::Scalars
                    }
                    exec::ExecOp::ShortestPath { .. } => Shape::Scalars,
                    exec::ExecOp::Reserved {
                        op: ir::ReservedOp::Fold,
                    } => {
                        shape.require_rows("fold")?;
                        Shape::Folded
                    }
                    exec::ExecOp::Reserved {
                        op: ir::ReservedOp::Unfold,
                    } => {
                        if !matches!(shape, Shape::Rows | Shape::Folded) {
                            return Err(HelixDbError::Query(
                                "unfold expected stream or folded stream input".into(),
                            ));
                        }
                        Shape::Rows
                    }
                    exec::ExecOp::Variable {
                        op:
                            exec::ExecVariableOp::SourceInject { variable }
                            | exec::ExecVariableOp::Stream(ir::StreamVariableOp::Select(variable)),
                    } => Shape::of(ctx.variable_value(variable)?),
                    exec::ExecOp::Expand { .. }
                    | exec::ExecOp::Filter { .. }
                    | exec::ExecOp::IndexMembership { .. }
                    | exec::ExecOp::Limit { .. }
                    | exec::ExecOp::Skip { .. }
                    | exec::ExecOp::Range { .. }
                    | exec::ExecOp::Distinct
                    | exec::ExecOp::Project { .. }
                    | exec::ExecOp::Variable { .. }
                    | exec::ExecOp::Branch { .. }
                    | exec::ExecOp::Repeat { .. }
                    | exec::ExecOp::Mutation { .. }
                    | exec::ExecOp::IndexDdl { .. }
                    | exec::ExecOp::Merge { .. }
                    | exec::ExecOp::Reserved { .. }
                    | exec::ExecOp::ForEach { .. }
                    | exec::ExecOp::Barrier { .. }
                    | exec::ExecOp::Noop => shape,
                };
                Node::Full {
                    op,
                    input: Some(input),
                }
            }
        };
        Ok(Self {
            shape,
            node,
            produced_rows: 0,
            row_mode: false,
            name: row_mode::op_name(op),
        })
    }
}
