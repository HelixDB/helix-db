//! Node index-set branches with per-branch residuals.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::ir;

use super::NodeAccessSourcePlan;

/// One disjunct of a partly indexed predicate: an index-only node set and the
/// conjuncts of that disjunct no index decides.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::catalog::{NodeEqualityIndexMeta, ScopedPropertyKey};
/// use helix_planner::ir::{
///     IndexValue, NodeAccessPlan, NodeAccessSourcePlan, NodeResidualBranch, PredicatePlan,
///     SecondaryIndexLiteral,
/// };
///
/// let source = NodeAccessSourcePlan::new(NodeAccessPlan::EqualityIndex {
///     index: NodeEqualityIndexMeta::try_new("item_p0").unwrap(),
///     key: ScopedPropertyKey::try_new("Item", "p0").unwrap(),
///     value: IndexValue::Literal(SecondaryIndexLiteral::new(7.into()).unwrap()),
/// })
/// .unwrap();
/// let residual = PredicatePlan::new(Predicate::gte("rank", 3)).unwrap();
/// let branch = NodeResidualBranch::new(source.clone(), Some(residual.clone()));
///
/// assert_eq!(branch.source(), &source);
/// assert_eq!(branch.residual(), Some(&residual));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeResidualBranch {
    source: NodeAccessSourcePlan,
    residual: Option<ir::PredicatePlan>,
}

impl NodeResidualBranch {
    /// Pair an index set with the residual its rows must still satisfy.
    pub fn new(source: NodeAccessSourcePlan, residual: Option<ir::PredicatePlan>) -> Self {
        Self { source, residual }
    }

    /// The index set whose rows this branch considers.
    pub fn source(&self) -> &NodeAccessSourcePlan {
        &self.source
    }

    /// The conjuncts no index decides, evaluated on this branch's rows only.
    pub fn residual(&self) -> Option<&ir::PredicatePlan> {
        self.residual.as_ref()
    }
}

/// Branches of a [`super::NodeAccessPlan::BranchResidualUnion`]: at least two,
/// at least one with a residual, and every source an index-only set.
///
/// Construction and deserialization enforce all three, so a union that
/// could hide a scan behind a branch is unrepresentable.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NodeResidualBranches(ir::AtLeast<NodeResidualBranch, 2>);

impl NodeResidualBranches {
    /// Validate the branch contract, or `None` when it does not hold.
    pub fn new(branches: Vec<NodeResidualBranch>) -> Option<Self> {
        let branches = ir::AtLeast::<_, 2>::try_from_vec(branches)?;
        (branches.iter().any(|branch| branch.residual.is_some())
            && branches
                .iter()
                .all(|branch| branch.source.is_secondary_set_eligible()))
        .then_some(Self(branches))
    }
}

impl AsRef<[NodeResidualBranch]> for NodeResidualBranches {
    fn as_ref(&self) -> &[NodeResidualBranch] {
        self.0.as_ref()
    }
}

impl<'de> Deserialize<'de> for NodeResidualBranches {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Vec::deserialize(deserializer)?).ok_or_else(|| {
            D::Error::custom(
                "branch residual unions need two index-only branches and at least one residual",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;

    fn equality(property: &str) -> NodeAccessSourcePlan {
        NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new(property).unwrap(),
            key: catalog::ScopedPropertyKey::try_new("Item", property).unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(7)).unwrap(),
            ),
        })
        .unwrap()
    }

    fn residual() -> Option<ir::PredicatePlan> {
        Some(ir::PredicatePlan::new(helix_ast::expr::Predicate::gte("rank", 3)).unwrap())
    }

    #[test]
    fn branches_need_two_index_only_sets_and_a_residual() {
        let with = NodeResidualBranch::new(equality("p0"), residual());
        let without = NodeResidualBranch::new(equality("p1"), None);
        let scan = NodeResidualBranch::new(
            NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("Item").unwrap(),
            })
            .unwrap(),
            residual(),
        );

        assert!(NodeResidualBranches::new(vec![with.clone()]).is_none());
        assert!(NodeResidualBranches::new(vec![without.clone(), without.clone()]).is_none());
        assert!(NodeResidualBranches::new(vec![scan, without.clone()]).is_none());
        let branches = NodeResidualBranches::new(vec![with, without]).unwrap();
        assert_eq!(branches.as_ref().len(), 2);

        // Deserialization enforces the same contract.
        let encoded = serde_json::to_value(&branches).unwrap();
        assert_eq!(
            serde_json::from_value::<NodeResidualBranches>(encoded.clone()).unwrap(),
            branches
        );
        let first_only = serde_json::Value::Array(vec![encoded[0].clone()]);
        assert!(serde_json::from_value::<NodeResidualBranches>(first_only).is_err());
    }
}
