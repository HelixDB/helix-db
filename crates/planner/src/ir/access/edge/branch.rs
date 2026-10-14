//! Edge index-set branches with per-branch residuals.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::ir;

use super::EdgeAccessSourcePlan;

/// One disjunct of a partly indexed predicate: an index-only edge set and the
/// conjuncts of that disjunct no index decides.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::catalog::{EdgeEqualityIndexMeta, ScopedPropertyKey};
/// use helix_planner::ir::{
///     IndexValue, EdgeAccessPlan, EdgeAccessSourcePlan, EdgeResidualBranch, PredicatePlan,
///     SecondaryIndexLiteral,
/// };
///
/// let source = EdgeAccessSourcePlan::new(EdgeAccessPlan::EqualityIndex {
///     index: EdgeEqualityIndexMeta::try_new("link_p0").unwrap(),
///     key: ScopedPropertyKey::try_new("Link", "p0").unwrap(),
///     value: IndexValue::Literal(SecondaryIndexLiteral::new(7.into()).unwrap()),
/// })
/// .unwrap();
/// let residual = PredicatePlan::new(Predicate::gte("rank", 3)).unwrap();
/// let branch = EdgeResidualBranch::new(source.clone(), Some(residual.clone()));
///
/// assert_eq!(branch.source(), &source);
/// assert_eq!(branch.residual(), Some(&residual));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeResidualBranch {
    source: EdgeAccessSourcePlan,
    residual: Option<ir::PredicatePlan>,
}

impl EdgeResidualBranch {
    /// Pair an index set with the residual its rows must still satisfy.
    pub fn new(source: EdgeAccessSourcePlan, residual: Option<ir::PredicatePlan>) -> Self {
        Self { source, residual }
    }

    /// The index set whose rows this branch considers.
    pub fn source(&self) -> &EdgeAccessSourcePlan {
        &self.source
    }

    /// The conjuncts no index decides, evaluated on this branch's rows only.
    pub fn residual(&self) -> Option<&ir::PredicatePlan> {
        self.residual.as_ref()
    }
}

/// Branches of a [`super::EdgeAccessPlan::BranchResidualUnion`]: at least two,
/// at least one with a residual, and every source an index-only set.
///
/// Construction and deserialization enforce all three, so a union that
/// could hide a scan behind a branch is unrepresentable.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EdgeResidualBranches(ir::AtLeast<EdgeResidualBranch, 2>);

impl EdgeResidualBranches {
    /// Validate the branch contract, or `None` when it does not hold.
    pub fn new(branches: Vec<EdgeResidualBranch>) -> Option<Self> {
        let branches = ir::AtLeast::<_, 2>::try_from_vec(branches)?;
        (branches.iter().any(|branch| branch.residual.is_some())
            && branches
                .iter()
                .all(|branch| branch.source.is_secondary_set_eligible()))
        .then_some(Self(branches))
    }
}

impl AsRef<[EdgeResidualBranch]> for EdgeResidualBranches {
    fn as_ref(&self) -> &[EdgeResidualBranch] {
        self.0.as_ref()
    }
}

impl<'de> Deserialize<'de> for EdgeResidualBranches {
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
