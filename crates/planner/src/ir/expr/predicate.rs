//! Validated shared predicate with a native serialization compatibility view.

use helix_ast::expr::Predicate;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::contracts::AtLeast;
use super::error::ExprPlanError;
use super::validation::validate_predicate;

/// Runtime predicate with validated parameter and property names.
#[derive(Debug, Clone, PartialEq)]
pub struct PredicatePlan {
    resolved: std::sync::Arc<super::native::Expression>,
    predicate: std::sync::Arc<Predicate>,
}

impl PredicatePlan {
    /// Build a predicate plan after recursively validating embedded names.
    pub fn new(predicate: Predicate) -> Result<Self, ExprPlanError> {
        validate_predicate(&predicate)?;
        Ok(Self {
            resolved: std::sync::Arc::new(super::native::predicate(&predicate)),
            predicate: std::sync::Arc::new(predicate),
        })
    }

    /// Build a validated conjunction from two or more already-validated predicates.
    ///
    /// This preserves the `PredicatePlan` invariant without re-validating every
    /// child: each child already carries the validated predicate contract, and
    /// [`AtLeast`] makes an empty conjunction unrepresentable.
    ///
    /// ```
    /// use helix_ast::expr::Predicate;
    /// use helix_planner::ir::{AtLeast, PredicatePlan};
    ///
    /// let first = PredicatePlan::new(Predicate::eq("active", true)).unwrap();
    /// let second = PredicatePlan::new(Predicate::eq("tenant", "acme")).unwrap();
    /// let merged = PredicatePlan::conjunction(&AtLeast::<_, 2>::from_pair(first, second));
    ///
    /// assert!(matches!(merged.predicate(), Predicate::And { predicates } if predicates.len() == 2));
    /// ```
    pub fn conjunction(predicates: &AtLeast<Self, 2>) -> Self {
        let predicate = Predicate::and(
            predicates
                .as_ref()
                .iter()
                .map(|predicate| predicate.as_ref().clone())
                .collect(),
        );
        Self {
            resolved: std::sync::Arc::new(super::native::predicate(&predicate)),
            predicate: std::sync::Arc::new(predicate),
        }
    }

    /// Borrow the validated predicate.
    ///
    /// ```
    /// use helix_ast::expr::Predicate;
    /// use helix_planner::ir::PredicatePlan;
    ///
    /// let predicate = Predicate::eq("active", true);
    /// let plan = PredicatePlan::new(predicate.clone()).unwrap();
    /// assert_eq!(plan.predicate(), &predicate);
    /// ```
    pub fn predicate(&self) -> &Predicate {
        &self.predicate
    }

    /// Borrow the shared scalar representation with native predicate semantics.
    pub fn resolved(&self) -> &super::native::Expression {
        &self.resolved
    }
}

impl AsRef<Predicate> for PredicatePlan {
    fn as_ref(&self) -> &Predicate {
        &self.predicate
    }
}

impl PartialEq<Predicate> for PredicatePlan {
    fn eq(&self, other: &Predicate) -> bool {
        self.predicate.as_ref() == other
    }
}

impl Serialize for PredicatePlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.predicate.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PredicatePlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let predicate = Predicate::deserialize(deserializer)?;
        Self::new(predicate).map_err(D::Error::custom)
    }
}
