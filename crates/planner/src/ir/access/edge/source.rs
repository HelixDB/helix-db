//! Residual-free edge access source contract.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ir;

use super::{analysis, EdgeAccessPlan};

/// Residual-free edge candidate source.
///
/// This is the edge-access counterpart to [`crate::ir::NodeAccessSourcePlan`].
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::ir::{
///     EdgeAccessPlan, EdgeAccessSourcePlan, PredicatePlan,
/// };
///
/// let source = EdgeAccessSourcePlan::new(EdgeAccessPlan::AllScan).unwrap();
/// let filtered = EdgeAccessPlan::ScanThenFilter {
///     source: source.clone(),
///     residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
/// };
///
/// assert!(EdgeAccessSourcePlan::new(filtered).is_none());
/// assert_eq!(source.as_ref(), &EdgeAccessPlan::AllScan);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeAccessSourcePlan {
    plan: Box<EdgeAccessPlan>,
}

impl EdgeAccessSourcePlan {
    /// Build a source plan, rejecting residual filter wrappers.
    pub fn new(plan: EdgeAccessPlan) -> Option<Self> {
        match plan {
            EdgeAccessPlan::ScanThenFilter { .. } => None,
            plan => Some(Self {
                plan: Box::new(plan),
            }),
        }
    }

    pub(crate) fn from_unfiltered(plan: EdgeAccessPlan) -> Self {
        assert!(
            !matches!(&plan, EdgeAccessPlan::ScanThenFilter { .. }),
            "filtered edge access cannot be used as an unfiltered edge source"
        );
        Self {
            plan: Box::new(plan),
        }
    }

    /// Return a proven hard upper cardinality bound for this source.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, EdgeAccessPlan, EdgeAccessSourcePlan, ElementIds};
    ///
    /// let ids = ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();
    /// let source = EdgeAccessSourcePlan::new(EdgeAccessPlan::PointIds { ids }).unwrap();
    ///
    /// assert_eq!(source.hard_cardinality_upper_bound(), Some(2));
    /// ```
    pub fn hard_cardinality_upper_bound(&self) -> Option<usize> {
        analysis::hard_cardinality_upper_bound(self.as_ref())
    }

    /// Return the label common to every branch of this residual-free source.
    ///
    /// ```
    /// use helix_planner::catalog::{EdgeRangeIndexMeta, ScopedPropertyDirectionKey};
    /// use helix_planner::ir::{
    ///     AtLeast, EdgeAccessPlan, EdgeAccessSourcePlan, IndexRange, NonEmptyString,
    /// };
    /// use helix_ast::index::RangeIndexDirection;
    ///
    /// let label = NonEmptyString::new("LIKES").unwrap();
    /// let left = EdgeAccessSourcePlan::new(EdgeAccessPlan::LabelScan {
    ///     label: label.clone(),
    /// }).unwrap();
    /// let right = EdgeAccessSourcePlan::new(EdgeAccessPlan::RangeIndex {
    ///     index: EdgeRangeIndexMeta::try_new("likes_weight").unwrap(),
    ///     key: ScopedPropertyDirectionKey::try_new(
    ///         "LIKES",
    ///         "weight",
    ///         RangeIndexDirection::Asc,
    ///     ).unwrap(),
    ///     range: IndexRange::All,
    /// }).unwrap();
    /// let source = EdgeAccessSourcePlan::new(EdgeAccessPlan::Intersect(
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

impl AsRef<EdgeAccessPlan> for EdgeAccessSourcePlan {
    fn as_ref(&self) -> &EdgeAccessPlan {
        self.plan.as_ref()
    }
}

impl std::ops::Deref for EdgeAccessSourcePlan {
    type Target = EdgeAccessPlan;

    fn deref(&self) -> &Self::Target {
        self.plan.as_ref()
    }
}

impl From<EdgeAccessSourcePlan> for EdgeAccessPlan {
    fn from(source: EdgeAccessSourcePlan) -> Self {
        *source.plan
    }
}

impl Serialize for EdgeAccessSourcePlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.plan.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EdgeAccessSourcePlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let plan = EdgeAccessPlan::deserialize(deserializer)?;
        Self::new(plan).ok_or_else(|| {
            D::Error::custom("filtered edge access cannot be used as an unfiltered edge source")
        })
    }
}
