//! Row-preserving node membership contract.
//!
//! A membership filter replaces a per-row predicate inside a stream pipeline.
//! The planner proves that some top-level conjuncts of the predicate, the
//! decided conjuncts, hold for a node exactly when the node is in a set read
//! from indexes: a secondary-index set of one label, or the `$label` bitmaps
//! of a finite label domain. Nodes in the set evaluate only the remaining
//! residual conjuncts, and rows the set cannot decide keep the whole
//! predicate, so the operator is exact for mixed-label, edge, and
//! element-free rows.

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;
use serde::{Deserialize, Serialize};

use crate::{analysis, ir};

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
    /// A range leaf verifies every in-range record with its own authoritative
    /// read, so its cost follows the label's range instead of the stream.
    RangeScan,
    /// The predicate requires a label other than the set label.
    LabelMismatch,
    /// The residual is not a strict sub-conjunction of the predicate: one of
    /// its top-level conjuncts is not a distinct predicate conjunct, or it
    /// leaves no conjunct for the set to decide.
    ResidualNotConjunct,
    /// The decided conjuncts are not a non-empty finite `$label` domain.
    NotLabelDomain,
}

impl std::fmt::Display for NodeIndexMembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotSecondarySet => "membership set must contain only secondary-index leaves",
            Self::NoCommonLabel => "membership set leaves must share one node label",
            Self::AuthoritativeNull => "membership set cannot contain literal null equality",
            Self::RangeScan => "membership set cannot contain a range scan",
            Self::LabelMismatch => "membership predicate requires a different label",
            Self::ResidualNotConjunct => {
                "membership residual must be a strict sub-conjunction of the predicate"
            }
            Self::NotLabelDomain => "label membership must decide a non-empty finite label domain",
        })
    }
}

/// Set that decides a membership's decided conjuncts for nodes.
///
/// A [`NodeIndexMembershipPlan`] constructor derives the variant and its
/// fields from the validated inputs, so it cannot disagree with the predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeMembershipSet {
    /// Nodes of `label` satisfying the decided conjuncts, read from secondary
    /// indexes.
    Index {
        /// Secondary-index set of `label` nodes.
        set: NodeAccessSourcePlan,
        /// Node label shared by every set leaf.
        label: ir::NonEmptyString,
        /// Decision for nodes outside `label`, derived from the whole
        /// predicate.
        outside_label: NodeMembershipOutsideLabel,
    },
    /// Nodes carrying any listed label, read from `$label` bitmaps; every
    /// other node fails its decided conjuncts. Labels keep their first
    /// occurrence order in the predicate, without duplicates.
    Labels(ir::AtLeast<ir::NonEmptyString, 1>),
}

/// Exact node membership filter applied to rows inside a stream pipeline.
///
/// `predicate` is the whole filter predicate and `residual` a strict
/// sub-conjunction of it. Contract, for a row whose current element is node
/// `n`:
///
/// * `n` in the set satisfies `predicate` exactly when it satisfies
///   `residual` (always, when there is none);
/// * a node the set cannot contain, a `label` node outside an
///   [`NodeMembershipSet::Index`] set, fails `predicate`;
/// * any other node follows the set's policy: [`NodeMembershipOutsideLabel`]
///   for an index set, and failure for [`NodeMembershipSet::Labels`];
///
/// and every edge or element-free row evaluates `predicate` exactly like the
/// filter it replaces.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_ast::value::PropertyValue;
/// use helix_planner::catalog::{NodeEqualityIndexMeta, ScopedPropertyKey};
/// use helix_planner::ir::{
///     IndexValue, NodeAccessPlan, NodeAccessSourcePlan, NodeIndexMembershipError,
///     NodeIndexMembershipPlan, NodeMembershipOutsideLabel, NodeMembershipSet, PredicatePlan,
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
///     NodeIndexMembershipPlan::new(equality(PropertyValue::from("B")), unscoped.clone(), None)
///         .unwrap();
/// let NodeMembershipSet::Index { label, outside_label, .. } = membership.set() else {
///     panic!("an equality set is an index set");
/// };
/// assert_eq!(label.as_ref(), "Item");
/// assert_eq!(*outside_label, NodeMembershipOutsideLabel::Evaluate);
///
/// // Nodes in the set evaluate only the residual conjunct.
/// let title = PredicatePlan::new(Predicate::contains("title", "x")).unwrap();
/// let scoped = PredicatePlan::new(Predicate::and(vec![
///     Predicate::eq("$label", "Item"),
///     Predicate::eq("kind", "B"),
///     title.predicate().clone(),
/// ]))
/// .unwrap();
/// let membership =
///     NodeIndexMembershipPlan::new(equality(PropertyValue::from("B")), scoped, Some(title))
///         .unwrap();
/// assert!(matches!(
///     membership.set(),
///     NodeMembershipSet::Index { outside_label: NodeMembershipOutsideLabel::Reject, .. }
/// ));
/// assert!(membership.residual().is_some());
///
/// // A pure label predicate is decided by the `$label` bitmaps.
/// let labels = NodeIndexMembershipPlan::labels(
///     PredicatePlan::new(Predicate::eq("$label", "Item")).unwrap(),
///     None,
/// )
/// .unwrap();
/// assert!(matches!(labels.set(), NodeMembershipSet::Labels(labels) if labels.len() == 1));
///
/// assert_eq!(
///     NodeIndexMembershipPlan::new(equality(PropertyValue::Null), unscoped, None),
///     Err(NodeIndexMembershipError::AuthoritativeNull)
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "NodeIndexMembershipPlanUnchecked",
    into = "NodeIndexMembershipPlanUnchecked"
)]
pub struct NodeIndexMembershipPlan {
    set: NodeMembershipSet,
    predicate: ir::PredicatePlan,
    residual: Option<ir::PredicatePlan>,
}

/// Serialized form: `set` is absent for `$label` bitmaps, so an index set
/// without a residual serializes as exactly `{set, predicate}`.
#[derive(Serialize, Deserialize)]
struct NodeIndexMembershipPlanUnchecked {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    set: Option<NodeAccessSourcePlan>,
    predicate: ir::PredicatePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    residual: Option<ir::PredicatePlan>,
}

impl NodeIndexMembershipPlan {
    /// Validate a secondary-index set, the whole filter predicate, and the
    /// residual conjuncts that set matches still evaluate.
    ///
    /// The caller proves that the conjuncts of `predicate` outside `residual`
    /// hold for a node with the set label exactly when the node is in `set`.
    /// The outside-label policy follows the whole predicate.
    pub fn new(
        set: NodeAccessSourcePlan,
        predicate: ir::PredicatePlan,
        residual: Option<ir::PredicatePlan>,
    ) -> Result<Self, NodeIndexMembershipError> {
        if !set.is_secondary_set_eligible() {
            return Err(NodeIndexMembershipError::NotSecondarySet);
        }
        unservable_leaf(set.as_ref()).map_or(Ok(()), Err)?;
        let Some(label) = set.common_label().cloned() else {
            return Err(NodeIndexMembershipError::NoCommonLabel);
        };
        decided_conjuncts(predicate.as_ref(), residual.as_ref())?;
        let outside_label = outside_label(predicate.as_ref(), &label)?;
        Ok(Self {
            set: NodeMembershipSet::Index {
                set,
                label,
                outside_label,
            },
            predicate,
            residual,
        })
    }

    /// Validate a membership decided by `$label` bitmaps.
    ///
    /// The conjuncts of `predicate` outside `residual` must be a finite,
    /// non-empty `$label` domain with nothing left over, and the set's labels
    /// are derived from them.
    pub fn labels(
        predicate: ir::PredicatePlan,
        residual: Option<ir::PredicatePlan>,
    ) -> Result<Self, NodeIndexMembershipError> {
        let decided = Predicate::and(
            decided_conjuncts(predicate.as_ref(), residual.as_ref())?
                .into_iter()
                .cloned()
                .collect(),
        );
        let Some((domain, None)) = analysis::conjunctive_label_domain(&decided) else {
            return Err(NodeIndexMembershipError::NotLabelDomain);
        };
        let labels = ir::AtLeast::try_from_vec(analysis::domain_labels(domain))
            .ok_or(NodeIndexMembershipError::NotLabelDomain)?;
        Ok(Self {
            set: NodeMembershipSet::Labels(labels),
            predicate,
            residual,
        })
    }

    /// Set that decides the decided conjuncts for nodes.
    pub const fn set(&self) -> &NodeMembershipSet {
        &self.set
    }

    /// Whole filter predicate, evaluated for rows the set cannot decide.
    pub const fn predicate(&self) -> &ir::PredicatePlan {
        &self.predicate
    }

    /// Conjuncts of `predicate` that nodes in the set still evaluate.
    pub const fn residual(&self) -> Option<&ir::PredicatePlan> {
        self.residual.as_ref()
    }
}

impl TryFrom<NodeIndexMembershipPlanUnchecked> for NodeIndexMembershipPlan {
    type Error = NodeIndexMembershipError;

    fn try_from(plan: NodeIndexMembershipPlanUnchecked) -> Result<Self, Self::Error> {
        match plan.set {
            Some(set) => Self::new(set, plan.predicate, plan.residual),
            None => Self::labels(plan.predicate, plan.residual),
        }
    }
}

impl From<NodeIndexMembershipPlan> for NodeIndexMembershipPlanUnchecked {
    fn from(plan: NodeIndexMembershipPlan) -> Self {
        Self {
            set: match plan.set {
                NodeMembershipSet::Index { set, .. } => Some(set),
                NodeMembershipSet::Labels(_) => None,
            },
            predicate: plan.predicate,
            residual: plan.residual,
        }
    }
}

/// Top-level conjuncts: the children of a conjunction, or the predicate
/// itself.
fn conjuncts(predicate: &Predicate) -> &[Predicate] {
    match predicate {
        Predicate::And { predicates } => predicates.as_slice(),
        predicate => core::slice::from_ref(predicate),
    }
}

/// Conjuncts of `predicate` the set decides: all of them minus one distinct
/// match per top-level conjunct of `residual`. At least one must remain.
fn decided_conjuncts<'p>(
    predicate: &'p Predicate,
    residual: Option<&ir::PredicatePlan>,
) -> Result<Vec<&'p Predicate>, NodeIndexMembershipError> {
    let residual = residual.map_or(&[][..], |residual| conjuncts(residual.as_ref()));
    let decided = residual.iter().try_fold(
        conjuncts(predicate).iter().collect::<Vec<_>>(),
        |mut decided, conjunct| {
            let position = decided
                .iter()
                .position(|candidate| *candidate == conjunct)
                .ok_or(NodeIndexMembershipError::ResidualNotConjunct)?;
            decided.remove(position);
            Ok(decided)
        },
    )?;
    (!decided.is_empty())
        .then_some(decided)
        .ok_or(NodeIndexMembershipError::ResidualNotConjunct)
}

/// First set leaf whose cost is not bounded by index reads alone.
///
/// Literal null equality needs an authoritative keyspace scan. A range leaf
/// verifies each in-range record with its own serial authoritative read, so
/// it can never beat the batched per-row filter it would replace.
fn unservable_leaf(plan: &NodeAccessPlan) -> Option<NodeIndexMembershipError> {
    match plan {
        NodeAccessPlan::EqualityIndex {
            value: ir::IndexValue::Literal(value),
            ..
        } if value.semantics() == ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
            Some(NodeIndexMembershipError::AuthoritativeNull)
        }
        NodeAccessPlan::RangeIndex { .. } => Some(NodeIndexMembershipError::RangeScan),
        NodeAccessPlan::Union(children) | NodeAccessPlan::Intersect(children) => children
            .iter()
            .find_map(|child| unservable_leaf(child.as_ref())),
        NodeAccessPlan::Empty
        | NodeAccessPlan::PointIds { .. }
        | NodeAccessPlan::FromParam { .. }
        | NodeAccessPlan::FromVar { .. }
        | NodeAccessPlan::AllScan
        | NodeAccessPlan::LabelScan { .. }
        | NodeAccessPlan::EqualityIndex { .. }
        | NodeAccessPlan::VectorSearch { .. }
        | NodeAccessPlan::TextSearch { .. }
        | NodeAccessPlan::ScanThenFilter { .. }
        | NodeAccessPlan::BranchResidualUnion(_) => None,
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
    conjuncts(predicate)
        .iter()
        .filter_map(direct_label_literal)
        .try_fold(NodeMembershipOutsideLabel::Evaluate, |_, required| {
            (required == label.as_ref())
                .then_some(NodeMembershipOutsideLabel::Reject)
                .ok_or(NodeIndexMembershipError::LabelMismatch)
        })
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

    fn outside_label_of(plan: &NodeIndexMembershipPlan) -> NodeMembershipOutsideLabel {
        let NodeMembershipSet::Index { outside_label, .. } = plan.set() else {
            panic!("expected an index set, got {:?}", plan.set());
        };
        *outside_label
    }

    fn labels_of(plan: &NodeIndexMembershipPlan) -> Vec<&str> {
        let NodeMembershipSet::Labels(labels) = plan.set() else {
            panic!("expected a label set, got {:?}", plan.set());
        };
        labels.iter().map(AsRef::as_ref).collect()
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
                equality("Item", "status", literal("live")),
            )))
            .unwrap();
        let plan = NodeIndexMembershipPlan::new(
            set.clone(),
            predicate(Predicate::and(vec![
                Predicate::is_in(
                    "kind",
                    PropertyValue::StringArray(vec!["A".into(), "B".into()]),
                ),
                Predicate::eq("status", "live"),
            ])),
            None,
        )
        .unwrap();

        assert_eq!(
            plan.set(),
            &NodeMembershipSet::Index {
                set,
                label: ir::NonEmptyString::new("Item").unwrap(),
                outside_label: NodeMembershipOutsideLabel::Evaluate,
            }
        );
        assert_eq!(plan.residual(), None);
    }

    #[test]
    fn membership_rejects_range_leaves_at_any_depth() {
        let rank = predicate(Predicate::gte("rank", 1));
        let nested = |children: fn(ir::AtLeast<NodeAccessSourcePlan, 2>) -> NodeAccessPlan| {
            NodeAccessSourcePlan::new(children(ir::AtLeast::<_, 2>::from_pair(
                equality("Item", "kind", literal("B")),
                range("Item", "rank"),
            )))
            .unwrap()
        };
        for set in [
            range("Item", "rank"),
            nested(NodeAccessPlan::Intersect),
            nested(NodeAccessPlan::Union),
        ] {
            assert_eq!(
                NodeIndexMembershipPlan::new(set, rank.clone(), None),
                Err(NodeIndexMembershipError::RangeScan)
            );
        }
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
                None,
            ),
            Err(NodeIndexMembershipError::NotSecondarySet)
        );
        assert_eq!(
            NodeIndexMembershipPlan::new(
                NodeAccessSourcePlan::new(NodeAccessPlan::Empty).unwrap(),
                kind.clone(),
                None,
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
                None,
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
                None,
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
            None,
        )
        .unwrap();

        assert_eq!(
            outside_label_of(&plan),
            NodeMembershipOutsideLabel::Evaluate
        );
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
                NodeIndexMembershipPlan::new(set(), predicate(predicate_value), None)
                    .map(|plan| outside_label_of(&plan)),
                expected
            );
        }
    }

    #[test]
    fn membership_residual_must_be_a_strict_subset_of_the_predicate_conjuncts() {
        let set = || equality("Item", "kind", literal("B"));
        let kind = Predicate::eq("kind", "B");
        let title = Predicate::contains("title", "x");
        let fused = predicate(Predicate::and(vec![kind.clone(), title.clone()]));

        let plan =
            NodeIndexMembershipPlan::new(set(), fused.clone(), Some(predicate(title.clone())))
                .unwrap();
        assert_eq!(plan.predicate(), &fused);
        assert_eq!(plan.residual(), Some(&predicate(title.clone())));
        assert_eq!(
            outside_label_of(&plan),
            NodeMembershipOutsideLabel::Evaluate
        );

        for (whole, residual) in [
            // Not among the conjuncts.
            (fused.clone(), Predicate::contains("title", "y")),
            // Leaves nothing decided.
            (fused.clone(), fused.predicate().clone()),
            // A lone predicate has nothing left to decide.
            (predicate(kind.clone()), kind.clone()),
            // Each residual conjunct consumes a distinct predicate conjunct.
            (
                fused.clone(),
                Predicate::and(vec![title.clone(), title.clone()]),
            ),
        ] {
            assert_eq!(
                NodeIndexMembershipPlan::new(set(), whole, Some(predicate(residual))),
                Err(NodeIndexMembershipError::ResidualNotConjunct)
            );
        }

        // A label conjunct left to the residual still proves rejection.
        let label = Predicate::eq("$label", "Item");
        let plan = NodeIndexMembershipPlan::new(
            set(),
            predicate(Predicate::and(vec![label.clone(), kind])),
            Some(predicate(label)),
        )
        .unwrap();
        assert_eq!(outside_label_of(&plan), NodeMembershipOutsideLabel::Reject);
    }

    #[test]
    fn label_membership_derives_labels_from_label_conjuncts() {
        let labels = |whole: Predicate, residual: Option<Predicate>| {
            NodeIndexMembershipPlan::labels(predicate(whole), residual.map(predicate))
        };
        let item = || Predicate::eq("$label", "Item");
        let title = || Predicate::contains("title", "x");

        assert_eq!(labels_of(&labels(item(), None).unwrap()), ["Item"]);
        assert_eq!(
            labels_of(
                &labels(
                    Predicate::is_in(
                        "$label",
                        PropertyValue::StringArray(vec![
                            "Item".to_owned(),
                            "Group".to_owned(),
                            "Item".to_owned(),
                        ]),
                    ),
                    None,
                )
                .unwrap()
            ),
            ["Item", "Group"]
        );
        assert_eq!(
            labels_of(
                &labels(
                    Predicate::or(vec![item(), Predicate::eq("$label", "Group")]),
                    None
                )
                .unwrap()
            ),
            ["Item", "Group"]
        );
        let fused = labels(Predicate::and(vec![item(), title()]), Some(title())).unwrap();
        assert_eq!(labels_of(&fused), ["Item"]);
        assert_eq!(fused.residual(), Some(&predicate(title())));

        for (whole, residual) in [
            (title(), None),
            (Predicate::and(vec![item(), title()]), None),
            (
                Predicate::is_in("$label", PropertyValue::StringArray(Vec::new())),
                None,
            ),
            (
                Predicate::and(vec![item(), Predicate::eq("$label", "Group")]),
                None,
            ),
        ] {
            assert_eq!(
                labels(whole, residual),
                Err(NodeIndexMembershipError::NotLabelDomain)
            );
        }
        assert_eq!(
            labels(item(), Some(item())),
            Err(NodeIndexMembershipError::ResidualNotConjunct)
        );
    }

    #[test]
    fn membership_serde_revalidates_and_rederives_label_policy() {
        let plan = NodeIndexMembershipPlan::new(
            equality("Item", "kind", literal("B")),
            predicate(Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                Predicate::eq("kind", "B"),
            ])),
            None,
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
            NodeIndexMembershipError::RangeScan,
            NodeIndexMembershipError::LabelMismatch,
            NodeIndexMembershipError::ResidualNotConjunct,
            NodeIndexMembershipError::NotLabelDomain,
        ] {
            assert!(!error.to_string().is_empty());
        }
    }

    #[test]
    fn membership_serde_round_trips_residuals_and_label_sets() {
        let title = Predicate::contains("title", "x");
        let keys = |plan: &NodeIndexMembershipPlan| {
            let json = serde_json::to_value(plan).unwrap();
            assert_eq!(
                &serde_json::from_value::<NodeIndexMembershipPlan>(json.clone()).unwrap(),
                plan
            );
            let mut keys = json
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            keys
        };

        let fused = NodeIndexMembershipPlan::new(
            equality("Item", "kind", literal("B")),
            predicate(Predicate::and(vec![
                Predicate::eq("kind", "B"),
                title.clone(),
            ])),
            Some(predicate(title.clone())),
        )
        .unwrap();
        assert_eq!(keys(&fused), ["predicate", "residual", "set"]);

        let label =
            NodeIndexMembershipPlan::labels(predicate(Predicate::eq("$label", "Item")), None)
                .unwrap();
        assert_eq!(keys(&label), ["predicate"]);

        let label_fused = NodeIndexMembershipPlan::labels(
            predicate(Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                title.clone(),
            ])),
            Some(predicate(title)),
        )
        .unwrap();
        assert_eq!(keys(&label_fused), ["predicate", "residual"]);

        let mut invalid = serde_json::to_value(&fused).unwrap();
        invalid["residual"] =
            serde_json::to_value(predicate(Predicate::contains("title", "y"))).unwrap();
        assert!(serde_json::from_value::<NodeIndexMembershipPlan>(invalid).is_err());
    }
}
