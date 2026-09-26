//! Row-preserving node secondary-index membership contract.
//!
//! A membership filter replaces a per-row stored-property predicate inside a
//! stream pipeline. The planner proves that, for nodes carrying the set label,
//! the predicate holds exactly when the node ID is in the secondary-index set.
//! Rows the set cannot decide keep the original predicate, so the operator is
//! exact for mixed-label, edge, and element-free rows.

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;
use serde::{Deserialize, Serialize};

use crate::ir;

use super::{NodeAccessPlan, NodeAccessSourcePlan};

/// How node rows outside the membership label are decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeMembershipOutsideLabel {
    /// The predicate requires the set label, so other nodes never match.
    Reject,
    /// Other nodes may match and evaluate the predicate row by row.
    Evaluate,
}

/// Invalid node membership construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeIndexMembershipError {
    /// The set contains a scan, point, search, or residual source.
    NotSecondarySet,
    /// Set leaves do not prove one shared node label.
    NoCommonLabel,
    /// A literal null equality needs an authoritative scan, not an index read.
    AuthoritativeNull,
    /// The predicate requires a label other than the set label.
    LabelMismatch,
}

impl std::fmt::Display for NodeIndexMembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotSecondarySet => "membership set must contain only secondary-index leaves",
            Self::NoCommonLabel => "membership set leaves must share one node label",
            Self::AuthoritativeNull => "membership set cannot contain literal null equality",
            Self::LabelMismatch => "membership predicate requires a different label",
        })
    }
}

/// Exact node membership filter applied to rows inside a stream pipeline.
///
/// Contract, for a row whose current element is node `n`:
///
/// * `n` in `set` keeps the row;
/// * `n` carries `label` but is not in `set` drops the row;
/// * any other node follows [`NodeMembershipOutsideLabel`];
///
/// and every edge or element-free row evaluates `predicate` exactly like the
/// residual filter it replaces. The set label and the outside-label policy are
/// derived from the validated inputs, so they cannot disagree with them.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_ast::value::PropertyValue;
/// use helix_planner::catalog::{NodeEqualityIndexMeta, ScopedPropertyKey};
/// use helix_planner::ir::{
///     IndexValue, NodeAccessPlan, NodeAccessSourcePlan, NodeIndexMembershipError,
///     NodeIndexMembershipPlan, NodeMembershipOutsideLabel, PredicatePlan,
///     SecondaryIndexLiteral,
/// };
///
/// let equality = |value: PropertyValue| {
///     NodeAccessSourcePlan::new(NodeAccessPlan::EqualityIndex {
///         index: NodeEqualityIndexMeta::try_new("item_kind").unwrap(),
///         key: ScopedPropertyKey::try_new("Item", "kind").unwrap(),
///         value: IndexValue::Literal(SecondaryIndexLiteral::new(value).unwrap()),
///     })
///     .unwrap()
/// };
/// let unscoped = PredicatePlan::new(Predicate::eq("kind", "B")).unwrap();
/// let membership =
///     NodeIndexMembershipPlan::new(equality(PropertyValue::from("B")), unscoped.clone())
///         .unwrap();
/// assert_eq!(membership.label().as_ref(), "Item");
/// assert_eq!(membership.outside_label(), NodeMembershipOutsideLabel::Evaluate);
///
/// let scoped = PredicatePlan::new(Predicate::and(vec![
///     Predicate::eq("$label", "Item"),
///     Predicate::eq("kind", "B"),
/// ]))
/// .unwrap();
/// let membership =
///     NodeIndexMembershipPlan::new(equality(PropertyValue::from("B")), scoped).unwrap();
/// assert_eq!(membership.outside_label(), NodeMembershipOutsideLabel::Reject);
///
/// assert_eq!(
///     NodeIndexMembershipPlan::new(equality(PropertyValue::Null), unscoped),
///     Err(NodeIndexMembershipError::AuthoritativeNull)
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "NodeIndexMembershipPlanUnchecked",
    into = "NodeIndexMembershipPlanUnchecked"
)]
pub struct NodeIndexMembershipPlan {
    set: NodeAccessSourcePlan,
    label: ir::NonEmptyString,
    predicate: ir::PredicatePlan,
    outside_label: NodeMembershipOutsideLabel,
}

#[derive(Serialize, Deserialize)]
struct NodeIndexMembershipPlanUnchecked {
    set: NodeAccessSourcePlan,
    predicate: ir::PredicatePlan,
}

impl NodeIndexMembershipPlan {
    /// Validate a secondary-index set and the predicate it decides.
    ///
    /// The caller proves that `predicate` holds for a node with the set label
    /// exactly when the node is in `set`.
    pub fn new(
        set: NodeAccessSourcePlan,
        predicate: ir::PredicatePlan,
    ) -> Result<Self, NodeIndexMembershipError> {
        if !set.is_secondary_set_eligible() {
            return Err(NodeIndexMembershipError::NotSecondarySet);
        }
        if requires_authoritative_null(set.as_ref()) {
            return Err(NodeIndexMembershipError::AuthoritativeNull);
        }
        let Some(label) = set.common_label().cloned() else {
            return Err(NodeIndexMembershipError::NoCommonLabel);
        };
        let outside_label = outside_label(predicate.as_ref(), &label)?;
        Ok(Self {
            set,
            label,
            predicate,
            outside_label,
        })
    }

    /// Secondary-index set of label nodes that satisfy the predicate.
    pub const fn set(&self) -> &NodeAccessSourcePlan {
        &self.set
    }

    /// Node label shared by every set leaf.
    pub const fn label(&self) -> &ir::NonEmptyString {
        &self.label
    }

    /// Predicate decided by the set for label nodes and evaluated for others.
    pub const fn predicate(&self) -> &ir::PredicatePlan {
        &self.predicate
    }

    /// Decision for node rows outside the set label.
    pub const fn outside_label(&self) -> NodeMembershipOutsideLabel {
        self.outside_label
    }
}

impl TryFrom<NodeIndexMembershipPlanUnchecked> for NodeIndexMembershipPlan {
    type Error = NodeIndexMembershipError;

    fn try_from(plan: NodeIndexMembershipPlanUnchecked) -> Result<Self, Self::Error> {
        Self::new(plan.set, plan.predicate)
    }
}

impl From<NodeIndexMembershipPlan> for NodeIndexMembershipPlanUnchecked {
    fn from(plan: NodeIndexMembershipPlan) -> Self {
        Self {
            set: plan.set,
            predicate: plan.predicate,
        }
    }
}

fn requires_authoritative_null(plan: &NodeAccessPlan) -> bool {
    match plan {
        NodeAccessPlan::EqualityIndex {
            value: ir::IndexValue::Literal(value),
            ..
        } => value.semantics() == ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull,
        NodeAccessPlan::Union(children) | NodeAccessPlan::Intersect(children) => children
            .iter()
            .any(|child| requires_authoritative_null(child.as_ref())),
        NodeAccessPlan::Empty
        | NodeAccessPlan::PointIds { .. }
        | NodeAccessPlan::FromParam { .. }
        | NodeAccessPlan::FromVar { .. }
        | NodeAccessPlan::AllScan
        | NodeAccessPlan::LabelScan { .. }
        | NodeAccessPlan::EqualityIndex { .. }
        | NodeAccessPlan::RangeIndex { .. }
        | NodeAccessPlan::VectorSearch { .. }
        | NodeAccessPlan::TextSearch { .. }
        | NodeAccessPlan::ScanThenFilter { .. } => false,
    }
}

/// Derives the outside-label policy from direct `$label` conjuncts.
///
/// Only a syntactic top-level label equality proves rejection. Any other label
/// shape conservatively evaluates the predicate for other nodes.
fn outside_label(
    predicate: &Predicate,
    label: &ir::NonEmptyString,
) -> Result<NodeMembershipOutsideLabel, NodeIndexMembershipError> {
    let conjuncts = match predicate {
        Predicate::And { predicates } => predicates.as_slice(),
        predicate => core::slice::from_ref(predicate),
    };
    conjuncts.iter().filter_map(direct_label_literal).try_fold(
        NodeMembershipOutsideLabel::Evaluate,
        |_, required| {
            (required == label.as_ref())
                .then_some(NodeMembershipOutsideLabel::Reject)
                .ok_or(NodeIndexMembershipError::LabelMismatch)
        },
    )
}

fn direct_label_literal(predicate: &Predicate) -> Option<&str> {
    let (Predicate::Eq { left, right }
    | Predicate::Compare {
        left,
        op: CompareOp::Eq,
        right,
    }) = predicate
    else {
        return None;
    };
    match (left, right) {
        (Expr::Property(property), Expr::Constant(PropertyValue::String(label)))
        | (Expr::Constant(PropertyValue::String(label)), Expr::Property(property))
            if property == "$label" =>
        {
            Some(label)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;

    fn equality(label: &str, property: &str, value: ir::IndexValue) -> NodeAccessSourcePlan {
        NodeAccessSourcePlan::new(NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new(format!("{label}_{property}")).unwrap(),
            key: catalog::ScopedPropertyKey::try_new(label, property).unwrap(),
            value,
        })
        .unwrap()
    }

    fn literal(value: impl Into<PropertyValue>) -> ir::IndexValue {
        ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value.into()).unwrap())
    }

    fn range(label: &str, property: &str) -> NodeAccessSourcePlan {
        NodeAccessSourcePlan::new(NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new(format!("{label}_{property}_range"))
                .unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                label,
                property,
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
            iteration: ir::RangeScanIteration::Forward,
        })
        .unwrap()
    }

    fn predicate(predicate: Predicate) -> ir::PredicatePlan {
        ir::PredicatePlan::new(predicate).unwrap()
    }

    #[test]
    fn membership_accepts_nested_secondary_sets_with_one_label() {
        let set =
            NodeAccessSourcePlan::new(NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
                NodeAccessSourcePlan::new(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                    equality("Item", "kind", literal("A")),
                    equality("Item", "kind", literal("B")),
                )))
                .unwrap(),
                range("Item", "rank"),
            )))
            .unwrap();
        let plan = NodeIndexMembershipPlan::new(
            set.clone(),
            predicate(Predicate::and(vec![
                Predicate::is_in(
                    "kind",
                    PropertyValue::StringArray(vec!["A".into(), "B".into()]),
                ),
                Predicate::gte("rank", 1),
            ])),
        )
        .unwrap();

        assert_eq!(plan.set(), &set);
        assert_eq!(plan.label().as_ref(), "Item");
        assert_eq!(plan.outside_label(), NodeMembershipOutsideLabel::Evaluate);
    }

    #[test]
    fn membership_rejects_non_secondary_mixed_label_and_null_sets() {
        let kind = predicate(Predicate::eq("kind", "B"));
        assert_eq!(
            NodeIndexMembershipPlan::new(
                NodeAccessSourcePlan::new(NodeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new("Item").unwrap(),
                })
                .unwrap(),
                kind.clone(),
            ),
            Err(NodeIndexMembershipError::NotSecondarySet)
        );
        assert_eq!(
            NodeIndexMembershipPlan::new(
                NodeAccessSourcePlan::new(NodeAccessPlan::Empty).unwrap(),
                kind.clone(),
            ),
            Err(NodeIndexMembershipError::NoCommonLabel)
        );
        assert_eq!(
            NodeIndexMembershipPlan::new(
                NodeAccessSourcePlan::new(NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                    equality("Item", "kind", literal("B")),
                    equality("Group", "kind", literal("B")),
                )))
                .unwrap(),
                kind.clone(),
            ),
            Err(NodeIndexMembershipError::NoCommonLabel)
        );
        assert_eq!(
            NodeIndexMembershipPlan::new(
                NodeAccessSourcePlan::new(NodeAccessPlan::Intersect(
                    ir::AtLeast::<_, 2>::from_pair(
                        equality("Item", "kind", literal("B")),
                        equality("Item", "deleted_at", literal(PropertyValue::Null)),
                    ),
                ))
                .unwrap(),
                kind,
            ),
            Err(NodeIndexMembershipError::AuthoritativeNull)
        );
    }

    #[test]
    fn membership_accepts_runtime_parameters_for_runtime_classification() {
        let param = ir::NonEmptyString::new("kind").unwrap();
        let plan = NodeIndexMembershipPlan::new(
            equality("Item", "kind", ir::IndexValue::Param(param)),
            predicate(Predicate::eq_param("kind", "kind")),
        )
        .unwrap();

        assert_eq!(plan.outside_label(), NodeMembershipOutsideLabel::Evaluate);
    }

    #[test]
    fn outside_label_policy_follows_direct_label_conjuncts() {
        let set = || equality("Item", "kind", literal("B"));
        for (predicate_value, expected) in [
            (
                Predicate::eq("$label", "Item"),
                Ok(NodeMembershipOutsideLabel::Reject),
            ),
            (
                Predicate::Compare {
                    left: Expr::Constant(PropertyValue::from("Item")),
                    op: CompareOp::Eq,
                    right: Expr::Property("$label".to_owned()),
                },
                Ok(NodeMembershipOutsideLabel::Reject),
            ),
            (
                Predicate::and(vec![
                    Predicate::eq("kind", "B"),
                    Predicate::eq("$label", "Item"),
                ]),
                Ok(NodeMembershipOutsideLabel::Reject),
            ),
            (
                Predicate::and(vec![
                    Predicate::eq("$label", "Item"),
                    Predicate::eq("$label", "Group"),
                ]),
                Err(NodeIndexMembershipError::LabelMismatch),
            ),
            (
                Predicate::eq("$label", "Group"),
                Err(NodeIndexMembershipError::LabelMismatch),
            ),
            (
                Predicate::or(vec![
                    Predicate::eq("$label", "Item"),
                    Predicate::eq("kind", "B"),
                ]),
                Ok(NodeMembershipOutsideLabel::Evaluate),
            ),
            (
                Predicate::eq("$label", 7),
                Ok(NodeMembershipOutsideLabel::Evaluate),
            ),
            (
                Predicate::Compare {
                    left: Expr::Property("$label".to_owned()),
                    op: CompareOp::Neq,
                    right: Expr::Constant(PropertyValue::from("Item")),
                },
                Ok(NodeMembershipOutsideLabel::Evaluate),
            ),
            (
                Predicate::eq("name", "Item"),
                Ok(NodeMembershipOutsideLabel::Evaluate),
            ),
        ] {
            assert_eq!(
                NodeIndexMembershipPlan::new(set(), predicate(predicate_value))
                    .map(|plan| plan.outside_label()),
                expected
            );
        }
    }

    #[test]
    fn membership_serde_revalidates_and_rederives_label_policy() {
        let plan = NodeIndexMembershipPlan::new(
            equality("Item", "kind", literal("B")),
            predicate(Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                Predicate::eq("kind", "B"),
            ])),
        )
        .unwrap();
        let json = serde_json::to_value(&plan).unwrap();
        let mut keys = json.as_object().unwrap().keys().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, ["predicate", "set"]);
        assert_eq!(
            serde_json::from_value::<NodeIndexMembershipPlan>(json.clone()).unwrap(),
            plan
        );

        let mut null = json;
        null["set"] =
            serde_json::to_value(equality("Item", "kind", literal(PropertyValue::Null))).unwrap();
        assert!(serde_json::from_value::<NodeIndexMembershipPlan>(null).is_err());
        for error in [
            NodeIndexMembershipError::NotSecondarySet,
            NodeIndexMembershipError::NoCommonLabel,
            NodeIndexMembershipError::AuthoritativeNull,
            NodeIndexMembershipError::LabelMismatch,
        ] {
            assert!(!error.to_string().is_empty());
        }
    }
}
