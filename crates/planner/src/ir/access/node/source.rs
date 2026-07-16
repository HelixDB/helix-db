//! Residual-free node access source contract.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ir;

use super::{analysis, NodeAccessPlan};

/// Residual-free node candidate source.
///
/// `ScanThenFilter` wraps a candidate source with a residual predicate. Allowing
/// another `ScanThenFilter` in that source position would make the residual
/// layering ambiguous, so this wrapper validates the boundary for direct
/// construction and deserialization.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::ir::{
///     NodeAccessPlan, NodeAccessSourcePlan, PredicatePlan,
/// };
///
/// let source = NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap();
/// let filtered = NodeAccessPlan::ScanThenFilter {
///     source: source.clone(),
///     residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
/// };
///
/// assert!(NodeAccessSourcePlan::new(filtered).is_none());
/// assert_eq!(source.as_ref(), &NodeAccessPlan::AllScan);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct NodeAccessSourcePlan {
    plan: Box<NodeAccessPlan>,
}

impl NodeAccessSourcePlan {
    /// Build a source plan, rejecting residual filter wrappers.
    pub fn new(plan: NodeAccessPlan) -> Option<Self> {
        match plan {
            NodeAccessPlan::ScanThenFilter { .. } => None,
            plan => Some(Self {
                plan: Box::new(plan),
            }),
        }
    }

    pub(crate) fn from_unfiltered(plan: NodeAccessPlan) -> Self {
        assert!(
            !matches!(&plan, NodeAccessPlan::ScanThenFilter { .. }),
            "filtered node access cannot be used as an unfiltered node source"
        );
        Self {
            plan: Box::new(plan),
        }
    }

    /// Return a proven hard upper cardinality bound for this source.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, ElementIds, NodeAccessPlan, NodeAccessSourcePlan};
    ///
    /// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::PointIds { ids }).unwrap();
    ///
    /// assert_eq!(source.hard_cardinality_upper_bound(), Some(2));
    /// ```
    pub fn hard_cardinality_upper_bound(&self) -> Option<usize> {
        analysis::hard_cardinality_upper_bound(self.as_ref())
    }

    /// Return the label common to every branch of this residual-free source.
    ///
    /// ```
    /// use helix_planner::catalog::{NodeEqualityIndexMeta, ScopedPropertyKey};
    /// use helix_planner::ir::{
    ///     AtLeast, IndexValue, NodeAccessPlan, NodeAccessSourcePlan,
    ///     NonEmptyString, SecondaryIndexLiteral,
    /// };
    /// use helix_ast::value::PropertyValue;
    ///
    /// let label = NonEmptyString::new("User").unwrap();
    /// let left = NodeAccessSourcePlan::new(NodeAccessPlan::LabelScan {
    ///     label: label.clone(),
    /// }).unwrap();
    /// let right = NodeAccessSourcePlan::new(NodeAccessPlan::EqualityIndex {
    ///     index: NodeEqualityIndexMeta::try_new("user_email").unwrap(),
    ///     key: ScopedPropertyKey::try_new("User", "email").unwrap(),
    ///     value: IndexValue::Literal(
    ///         SecondaryIndexLiteral::new(PropertyValue::from("a@example.test")).unwrap(),
    ///     ),
    /// }).unwrap();
    /// let source = NodeAccessSourcePlan::new(NodeAccessPlan::Union(
    ///     AtLeast::<_, 2>::from_pair(left, right),
    /// )).unwrap();
    ///
    /// assert_eq!(source.common_label(), Some(&label));
    /// ```
    pub fn common_label(&self) -> Option<&ir::NonEmptyString> {
        analysis::common_label(self.as_ref())
    }

    pub(crate) fn has_set_canonicalization_candidate(&self) -> bool {
        analysis::set_canonicalization_candidate(self.as_ref())
    }

    pub(crate) fn has_set_subsumption_candidate(&self) -> bool {
        analysis::set_subsumption_candidate(self.as_ref())
    }

    pub(crate) fn subsumes(&self, subset: &Self) -> bool {
        analysis::subsumes(self.as_ref(), subset.as_ref())
    }
}

impl AsRef<NodeAccessPlan> for NodeAccessSourcePlan {
    fn as_ref(&self) -> &NodeAccessPlan {
        self.plan.as_ref()
    }
}

impl std::ops::Deref for NodeAccessSourcePlan {
    type Target = NodeAccessPlan;

    fn deref(&self) -> &Self::Target {
        self.plan.as_ref()
    }
}

impl From<NodeAccessSourcePlan> for NodeAccessPlan {
    fn from(source: NodeAccessSourcePlan) -> Self {
        *source.plan
    }
}

impl Serialize for NodeAccessSourcePlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.plan.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeAccessSourcePlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let plan = NodeAccessPlan::deserialize(deserializer)?;
        Self::new(plan).ok_or_else(|| {
            D::Error::custom("filtered node access cannot be used as an unfiltered node source")
        })
    }
}
