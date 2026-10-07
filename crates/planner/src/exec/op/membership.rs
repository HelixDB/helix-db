//! Executable row-preserving node index membership.

use serde::{Deserialize, Serialize};

use crate::{exec, ir};

/// Executable set of an index membership, lowered from
/// [`ir::NodeMembershipSet`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[expect(
    clippy::large_enum_variant,
    reason = "every executable membership plan is already boxed in its operator"
)]
pub enum ExecNodeMembershipSet {
    /// Nodes of `label` satisfying the decided conjuncts, read from secondary
    /// indexes.
    Index {
        /// Secondary-ID set of `label` nodes.
        set: exec::ExecNodeSecondarySetPlan,
        /// Node label shared by every set leaf.
        label: ir::NonEmptyString,
        /// Decision for nodes outside `label`.
        outside_label: ir::NodeMembershipOutsideLabel,
    },
    /// Nodes carrying any listed label, read from `$label` bitmaps; every
    /// other node fails the decided conjuncts.
    Labels(ir::AtLeast<ir::NonEmptyString, 1>),
}

/// Interpreter contract for a node index membership filter.
///
/// * The first node row that needs deciding resolves the set once through
///   the request-authorized Active catalog, reusing an equal set already
///   resolved in the request, whatever the predicate and residual of the
///   plan that resolved it. Streams without node rows never resolve it.
/// * A node in the set evaluates `residual` only, and is kept without
///   reading its record when there is none.
/// * A `label` node outside an [`ExecNodeMembershipSet::Index`] set is
///   dropped without reading its record.
/// * Other nodes evaluate `predicate` under the
///   [`Evaluate`](ir::NodeMembershipOutsideLabel::Evaluate) policy, and are
///   dropped under [`Reject`](ir::NodeMembershipOutsideLabel::Reject) or for
///   an [`ExecNodeMembershipSet::Labels`] set.
/// * Edge and element-free rows evaluate `predicate`.
///
/// When the set cannot be served from indexes alone, including runtime
/// parameters that bind null or exceed their bound, a range scan, and an
/// index that is no longer Active, every row evaluates `predicate` instead.
/// The operator never scans a keyspace or an index range.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_ast::value::PropertyValue;
/// use helix_planner::catalog::{NodeEqualityIndexMeta, ScopedPropertyKey};
/// use helix_planner::exec::{
///     ExecNodeIndexMembershipPlan, ExecNodeMembershipSet, ExecNodeSecondarySetPlan,
/// };
/// use helix_planner::ir::{
///     IndexValue, NodeAccessPlan, NodeAccessSourcePlan, NodeIndexMembershipPlan,
///     NodeMembershipOutsideLabel, PredicatePlan, SecondaryIndexLiteral,
/// };
///
/// let title = PredicatePlan::new(Predicate::contains("title", "x")).unwrap();
/// let predicate = PredicatePlan::new(Predicate::and(vec![
///     Predicate::eq("kind", "B"),
///     title.predicate().clone(),
/// ]))
/// .unwrap();
/// let plan = NodeIndexMembershipPlan::new(
///     NodeAccessSourcePlan::new(NodeAccessPlan::EqualityIndex {
///         index: NodeEqualityIndexMeta::try_new("item_kind").unwrap(),
///         key: ScopedPropertyKey::try_new("Item", "kind").unwrap(),
///         value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("B")).unwrap()),
///     })
///     .unwrap(),
///     predicate.clone(),
///     Some(title.clone()),
/// )
/// .unwrap();
/// let exec = ExecNodeIndexMembershipPlan::from(&plan);
///
/// let ExecNodeMembershipSet::Index { set, label, outside_label } = &exec.set else {
///     panic!("an equality set lowers to an index set");
/// };
/// assert!(matches!(set, ExecNodeSecondarySetPlan::Bitmap(_)));
/// assert_eq!(label.as_ref(), "Item");
/// assert_eq!(*outside_label, NodeMembershipOutsideLabel::Evaluate);
/// assert_eq!(exec.predicate, predicate);
/// assert_eq!(exec.residual, Some(title.clone()));
///
/// let labels = NodeIndexMembershipPlan::labels(
///     PredicatePlan::new(Predicate::and(vec![
///         Predicate::eq("$label", "Item"),
///         title.predicate().clone(),
///     ]))
///     .unwrap(),
///     Some(title.clone()),
/// )
/// .unwrap();
/// let exec = ExecNodeIndexMembershipPlan::from(&labels);
/// assert!(matches!(
///     &exec.set,
///     ExecNodeMembershipSet::Labels(labels) if labels.as_ref()[0].as_ref() == "Item"
/// ));
/// assert_eq!(exec.residual, Some(title));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeIndexMembershipPlan {
    /// Set of nodes satisfying the decided conjuncts.
    pub set: ExecNodeMembershipSet,
    /// Whole filter predicate, evaluated for rows the set cannot decide.
    pub predicate: ir::PredicatePlan,
    /// Conjuncts of `predicate` that nodes in `set` still evaluate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residual: Option<ir::PredicatePlan>,
}

impl From<&ir::NodeIndexMembershipPlan> for ExecNodeIndexMembershipPlan {
    fn from(plan: &ir::NodeIndexMembershipPlan) -> Self {
        Self {
            set: match plan.set() {
                ir::NodeMembershipSet::Index {
                    set,
                    label,
                    outside_label,
                } => ExecNodeMembershipSet::Index {
                    set: exec::node_secondary_set(set.as_ref())
                        .expect("validated membership sets contain only secondary-index leaves"),
                    label: label.clone(),
                    outside_label: *outside_label,
                },
                ir::NodeMembershipSet::Labels(labels) => {
                    ExecNodeMembershipSet::Labels(labels.clone())
                }
            },
            predicate: plan.predicate().clone(),
            residual: plan.residual().cloned(),
        }
    }
}
