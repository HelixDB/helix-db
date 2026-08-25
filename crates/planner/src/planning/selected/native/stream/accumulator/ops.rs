//! Native access-stream wrapper append operations.

use helix_ast::expr::{Predicate, StreamBound};

use super::NativeAccessStream;
use crate::planning::selected::native::parameter_specialization;
use crate::{analysis, context, error, ir, logical};

impl NativeAccessStream {
    /// Append a residual predicate filter.
    pub(in crate::planning::selected::native) fn filter(
        self,
        ctx: &context::PlannerContext,
        predicate: &Predicate,
    ) -> Result<Self, error::PlannerError> {
        let _ = ir::PredicatePlan::new(predicate.clone())?;
        let predicate = parameter_specialization::predicate(ctx, predicate)?;
        let predicate_plan = ir::PredicatePlan::new(predicate.clone())
            .expect("specializing validated parameters preserves predicate validity");
        let _ = analysis::prune_statically_impossible_branches(&predicate)?;
        Ok(self.filter_plan(predicate_plan))
    }

    /// Append an already validated residual predicate filter.
    pub(in crate::planning::selected::native) fn filter_plan(
        mut self,
        predicate: ir::PredicatePlan,
    ) -> Self {
        self.ops
            .push(logical::StreamPipelineOp::Filter { predicate });
        self
    }

    /// Append a limit operation.
    pub(in crate::planning::selected::native) fn limit(
        mut self,
        count: &StreamBound,
    ) -> Result<Self, error::PlannerError> {
        match super::super::bounds::stream_bound_plan(count.clone())? {
            ir::StreamBoundPlan::Literal(count) => {
                self.push_window(|window| window.then_limit(count));
            }
            count @ ir::StreamBoundPlan::Expr(_) => {
                self.ops.push(logical::StreamPipelineOp::Limit { count });
            }
        }
        Ok(self)
    }

    /// Append a skip operation.
    pub(in crate::planning::selected::native) fn skip(
        mut self,
        count: &StreamBound,
    ) -> Result<Self, error::PlannerError> {
        match super::super::bounds::stream_bound_plan(count.clone())? {
            ir::StreamBoundPlan::Literal(count) => {
                self.push_window(|window| window.then_skip(count));
            }
            count @ ir::StreamBoundPlan::Expr(_) => {
                self.ops.push(logical::StreamPipelineOp::Skip { count });
            }
        }
        Ok(self)
    }

    /// Append a range operation.
    pub(in crate::planning::selected::native) fn range(
        mut self,
        start: &StreamBound,
        end: &StreamBound,
    ) -> Result<Self, error::PlannerError> {
        match super::super::bounds::stream_range_plan(start.clone(), end.clone())? {
            ir::StreamRangePlan::Literal(range) => {
                self.push_window(|window| window.then_range(&range));
            }
            range @ ir::StreamRangePlan::Dynamic(_) => {
                self.ops.push(logical::StreamPipelineOp::Range { range });
            }
        }
        Ok(self)
    }

    /// Append a required ordering.
    pub(in crate::planning::selected::native) fn order(mut self, ordering: ir::OrderKeys) -> Self {
        self.ops.push(logical::StreamPipelineOp::Order { ordering });
        self
    }

    /// Append stream deduplication.
    pub(in crate::planning::selected::native) fn distinct(mut self) -> Self {
        self.ops.push(logical::StreamPipelineOp::Distinct);
        self
    }

    /// Append a side-effect-free variable operation.
    pub(in crate::planning::selected::native) fn variable(
        mut self,
        op: logical::PureStreamVariableOp,
    ) -> Self {
        self.ops.push(logical::StreamPipelineOp::Variable { op });
        self
    }

    /// Append a state-writing variable operation.
    pub(in crate::planning::selected::native) fn variable_write(
        mut self,
        op: logical::StreamVariableWriteOp,
    ) -> Self {
        self.ops
            .push(logical::StreamPipelineOp::VariableWrite { op });
        self
    }
}
