//! Search access builder result contracts.

use crate::ir;

/// Validated search access plan plus the index id chosen during catalog lookup.
///
/// ```
/// use helix_planner::{ir, planning};
///
/// let plan = planning::search::SearchAccessPlan {
///     plan: ir::NodeAccessPlan::AllScan,
///     index_id: ir::NonEmptyString::new("idx").unwrap(),
/// };
///
/// assert_eq!(plan.index_id.as_ref(), "idx");
/// assert!(matches!(plan.plan, ir::NodeAccessPlan::AllScan));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SearchAccessPlan<T> {
    /// Residual-free search access plan.
    pub plan: T,
    /// Chosen index id for trace/provenance.
    pub index_id: ir::NonEmptyString,
}
