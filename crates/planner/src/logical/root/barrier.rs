//! Root-only barrier contracts.
//!
//! Mutation, index-DDL, branch, and repeat roots preserve executable payloads
//! while using logical children instead of compatibility physical subtrees.

use serde::{Deserialize, Serialize};

use crate::ir;
use crate::logical::LogicalExpr;

/// Root mutation with the executable mutation payload preserved.
///
/// This contract marks an observable write boundary and carries enough data for
/// selected executable lowering without consulting the legacy physical tree.
/// It is selected as a root mutation directly, and `RootStream::Mutation` wraps
/// the same payload when later stream work consumes the mutation output.
///
/// ```
/// use helix_planner::ir::{
///     MutationInput, MutationPlan, NonEmptyString, PropertyAssignments,
/// };
/// use helix_planner::logical::RootMutation;
///
/// let root = RootMutation::new(MutationPlan::AddNode {
///     input: MutationInput::Source,
///     label: NonEmptyString::new("User").unwrap(),
///     properties: PropertyAssignments::default(),
/// });
///
/// assert!(matches!(root.plan(), MutationPlan::AddNode { .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RootMutation {
    plan: ir::MutationPlan<LogicalExpr>,
}

impl RootMutation {
    /// Build a root mutation contract.
    pub const fn new(plan: ir::MutationPlan<LogicalExpr>) -> Self {
        Self { plan }
    }

    /// Mutation payload to lower.
    pub const fn plan(&self) -> &ir::MutationPlan<LogicalExpr> {
        &self.plan
    }
}

/// Root index DDL with the executable DDL payload preserved.
///
/// ```
/// use helix_planner::{catalog, ir};
/// use helix_planner::logical::RootIndexDdl;
///
/// let root = RootIndexDdl::new(ir::IndexDdlPlan::Drop {
///     spec: ir::IndexDdlDropSpec::NodeEquality {
///         key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
///         uniqueness: catalog::IndexUniqueness::NonUnique,
///     },
/// });
///
/// assert!(matches!(root.plan(), ir::IndexDdlPlan::Drop { .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootIndexDdl {
    plan: ir::IndexDdlPlan,
}

impl RootIndexDdl {
    /// Build a root index-DDL contract.
    pub const fn new(plan: ir::IndexDdlPlan) -> Self {
        Self { plan }
    }

    /// Index-DDL payload to lower.
    pub const fn plan(&self) -> &ir::IndexDdlPlan {
        &self.plan
    }
}

/// Root branch control flow with executable payloads preserved.
///
/// Children are logical expressions rather than physical compatibility
/// subtrees, so branch roots cannot represent an unselectable child once they
/// have crossed the logical-algebra boundary.
///
/// ```
/// use helix_planner::ir::{AtLeast, BranchPlan, NodeAccessPlan, NodeAccessSourcePlan};
/// use helix_planner::logical::{AccessPath, LogicalExpr, NodeAccessPath, RootBranch};
///
/// let node = || {
///     LogicalExpr::AccessPath(AccessPath::Node(NodeAccessPath::new(
///         NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap(),
///     )))
/// };
///
/// let branch = RootBranch::new(
///     node(),
///     BranchPlan::Union(AtLeast::<_, 2>::from_pair(node(), node())),
/// );
///
/// assert!(matches!(branch.plan(), BranchPlan::Union(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RootBranch {
    input: Box<LogicalExpr>,
    plan: ir::BranchPlan<LogicalExpr>,
}

impl RootBranch {
    /// Build a root branch control-flow contract.
    pub fn new(input: LogicalExpr, plan: ir::BranchPlan<LogicalExpr>) -> Self {
        Self {
            input: Box::new(input),
            plan,
        }
    }

    /// Branch input payload.
    pub fn input(&self) -> &LogicalExpr {
        self.input.as_ref()
    }

    /// Branch payload.
    pub const fn plan(&self) -> &ir::BranchPlan<LogicalExpr> {
        &self.plan
    }
}

/// Root repeat control flow with executable payloads preserved.
///
/// ```
/// use std::num::NonZeroUsize;
/// use helix_planner::ir::{
///     NodeAccessPlan, NodeAccessSourcePlan, RepeatEmitPlan, RepeatPlan, RepeatStopPlan,
/// };
/// use helix_planner::logical::{AccessPath, LogicalExpr, NodeAccessPath, RootRepeat};
///
/// let node = || {
///     LogicalExpr::AccessPath(AccessPath::Node(NodeAccessPath::new(
///         NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap(),
///     )))
/// };
///
/// let repeat = RootRepeat::new(
///     node(),
///     RepeatPlan {
///         body: Box::new(node()),
///         stop: RepeatStopPlan::MaxDepthOnly,
///         emit: RepeatEmitPlan::None,
///         max_depth: NonZeroUsize::new(2).unwrap(),
///     },
/// );
///
/// assert_eq!(repeat.plan().max_depth.get(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RootRepeat {
    input: Box<LogicalExpr>,
    plan: ir::RepeatPlan<LogicalExpr>,
}

impl RootRepeat {
    /// Build a root repeat control-flow contract.
    pub fn new(input: LogicalExpr, plan: ir::RepeatPlan<LogicalExpr>) -> Self {
        Self {
            input: Box::new(input),
            plan,
        }
    }

    /// Repeat input payload.
    pub fn input(&self) -> &LogicalExpr {
        self.input.as_ref()
    }

    /// Repeat payload.
    pub const fn plan(&self) -> &ir::RepeatPlan<LogicalExpr> {
        &self.plan
    }
}
