//! Memo expression/group records and mutation errors.

use serde::{Deserialize, Serialize};

use super::children::MemoChildGroups;
use super::ids::{MemoExprId, MemoGroupId};
use crate::{cost, digest, exec, logical, properties};

/// One memo expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoExpr {
    /// Expression ID.
    pub id: MemoExprId,
    /// Owning group.
    pub group: MemoGroupId,
    /// Stable digest of the logical expression contract.
    pub digest: digest::PlanDigest,
    /// Logical expression.
    pub expr: logical::LogicalExpr,
    /// Child groups.
    pub children: MemoChildGroups,
}

/// One memo group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoGroup {
    /// Group ID.
    pub id: MemoGroupId,
    /// Stable digest of the expression that seeded this equivalence group.
    pub digest: digest::PlanDigest,
    /// Expressions in the group.
    pub expressions: Vec<MemoExpr>,
}

/// Expression inserted into a memo group.
///
/// The pair is produced only by memo insertion APIs, so callers do not have to
/// guess which expression ID belongs to a newly created or extended group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertedMemoExpr {
    /// Group that owns the expression.
    pub group: MemoGroupId,
    /// Inserted expression ID.
    pub expr: MemoExprId,
}

/// Memo mutation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoError {
    /// Target group does not exist in this memo.
    MissingGroup { group: MemoGroupId },
    /// The memo group ID cursor cannot allocate another stable ID.
    GroupIdSpaceExhausted,
    /// The memo expression ID cursor cannot allocate another stable ID.
    ExprIdSpaceExhausted,
}

impl std::fmt::Display for MemoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingGroup { group } => write!(f, "missing memo group {}", group.get()),
            Self::GroupIdSpaceExhausted => f.write_str("memo group ID space exhausted"),
            Self::ExprIdSpaceExhausted => f.write_str("memo expression ID space exhausted"),
        }
    }
}

/// Best physical implementation selected for one group and requirement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BestPlan {
    /// Group.
    pub group: MemoGroupId,
    /// Required properties.
    pub required: properties::RequiredProperties,
    /// Root executable step.
    pub root_step: exec::ExecStepId,
    /// Selected cost.
    pub cost: cost::CostVector,
}
