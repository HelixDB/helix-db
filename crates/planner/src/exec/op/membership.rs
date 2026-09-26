//! Executable row-preserving node index membership.

use serde::{Deserialize, Serialize};

use crate::{exec, ir};

/// Interpreter contract for a node secondary-index membership filter.
///
/// The interpreter resolves `set` once per operator execution through the
/// request-authorized Active catalog and retains rows in place:
///
/// * a node in `set` is kept without reading its record;
/// * a node carrying `label` outside `set` is dropped without reading it;
/// * other nodes follow `outside_label`;
/// * edge and element-free rows evaluate `predicate`.
///
/// When the set cannot be served from indexes alone, including runtime
/// parameters that bind null or exceed their bound and an index that is no
/// longer Active, every row evaluates `predicate` instead. The operator never
/// scans a keyspace.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_ast::value::PropertyValue;
/// use helix_planner::catalog::{NodeEqualityIndexMeta, ScopedPropertyKey};
/// use helix_planner::exec::{ExecNodeIndexMembershipPlan, ExecNodeSecondarySetPlan};
/// use helix_planner::ir::{
///     IndexValue, NodeAccessPlan, NodeAccessSourcePlan, NodeIndexMembershipPlan,
///     NodeMembershipOutsideLabel, PredicatePlan, SecondaryIndexLiteral,
/// };
///
/// let plan = NodeIndexMembershipPlan::new(
///     NodeAccessSourcePlan::new(NodeAccessPlan::EqualityIndex {
///         index: NodeEqualityIndexMeta::try_new("item_kind").unwrap(),
///         key: ScopedPropertyKey::try_new("Item", "kind").unwrap(),
///         value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("B")).unwrap()),
///     })
///     .unwrap(),
///     PredicatePlan::new(Predicate::eq("kind", "B")).unwrap(),
/// )
/// .unwrap();
/// let exec = ExecNodeIndexMembershipPlan::from(&plan);
///
/// assert!(matches!(exec.set, ExecNodeSecondarySetPlan::Bitmap(_)));
/// assert_eq!(exec.label.as_ref(), "Item");
/// assert_eq!(exec.outside_label, NodeMembershipOutsideLabel::Evaluate);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeIndexMembershipPlan {
    /// Exact secondary-ID set of label nodes satisfying `predicate`.
    pub set: exec::ExecNodeSecondarySetPlan,
    /// Node label shared by every set leaf.
    pub label: ir::NonEmptyString,
    /// Predicate evaluated for rows the set cannot decide.
    pub predicate: ir::PredicatePlan,
    /// Decision for node rows outside `label`.
    pub outside_label: ir::NodeMembershipOutsideLabel,
}

impl From<&ir::NodeIndexMembershipPlan> for ExecNodeIndexMembershipPlan {
    fn from(plan: &ir::NodeIndexMembershipPlan) -> Self {
        Self {
            set: exec::node_secondary_set(plan.set().as_ref())
                .expect("validated membership sets contain only secondary-index leaves"),
            label: plan.label().clone(),
            predicate: plan.predicate().clone(),
            outside_label: plan.outside_label(),
        }
    }
}
