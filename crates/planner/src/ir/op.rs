use serde::{Deserialize, Serialize};

use super::{
    AggregatePlan, BranchPlan, EdgeAccessPlan, ExpandPlan, FilterPlan, IndexDdlPlan, MutationPlan,
    NodeAccessPlan, OrderPlan, ProjectionPlan, RepeatPlan, ReservedOp, ShortestPathPlan,
    StreamBoundPlan, StreamRangePlan, VariablePlan,
};

/// Physical operation tree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalOp {
    /// Node access.
    NodeAccess(NodeAccessPlan),
    /// Edge access.
    EdgeAccess(EdgeAccessPlan),
    /// Graph expansion.
    Expand {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Expansion plan.
        plan: ExpandPlan,
    },
    /// Filter.
    Filter {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Filter execution plan.
        plan: FilterPlan,
    },
    /// Limit.
    Limit {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Bound.
        count: StreamBoundPlan,
    },
    /// Skip.
    Skip {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Bound.
        count: StreamBoundPlan,
    },
    /// Range.
    Range {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Checked range bounds.
        range: StreamRangePlan,
    },
    /// Deduplication.
    Distinct { input: Box<PhysicalOp> },
    /// Ordering.
    Order {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Order execution plan.
        plan: OrderPlan,
    },
    /// Projection terminal.
    Project {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Projection.
        projection: ProjectionPlan,
    },
    /// Aggregate terminal.
    Aggregate {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Aggregate.
        aggregate: AggregatePlan,
    },
    /// Store/select/bind/inject variable operation.
    Variable(VariablePlan),
    /// Branching operation.
    Branch {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Branch plan.
        plan: BranchPlan,
    },
    /// Repeat operation.
    Repeat {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Repeat plan.
        plan: RepeatPlan,
    },
    /// Unweighted shortest path.
    ShortestPath(ShortestPathPlan),
    /// Mutation operation.
    Mutation(MutationPlan),
    /// Index DDL.
    IndexDdl(IndexDdlPlan),
    /// Reserved/no-op state operation.
    Reserved {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Reserved operation name.
        op: ReservedOp,
    },
}
