//! Validated shared predicate with a native serialization compatibility view.

use helix_ast::expr::Predicate;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::contracts::AtLeast;
use super::error::ExprPlanError;
use super::native;
use super::validation::validate_predicate;

#[cfg(test)]
#[path = "tests/composition.rs"]
mod tests;

/// Resolved native predicate, constructed only after native name validation.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedPredicate(native::Expression);
impl ResolvedPredicate {
    pub fn expression(&self) -> &native::Expression {
        &self.0
    }
}
impl crate::relational::ExpressionInput for ResolvedPredicate {
    fn validate(&self) -> crate::relational::Result<()> {
        Ok(())
    }
    fn references(&self) -> std::collections::BTreeSet<crate::relational::Slot> {
        self.0.slots()
    }
}

/// Runtime predicate with validated parameter and property names.
#[derive(Debug, Clone, PartialEq)]
pub struct PredicatePlan {
    program: crate::relational::SelectionProgram<ResolvedPredicate>,
    predicate: std::sync::Arc<Predicate>,
}

impl PredicatePlan {
    /// Build a predicate plan after recursively validating embedded names.
    pub fn new(predicate: Predicate) -> Result<Self, ExprPlanError> {
        validate_predicate(&predicate)?;
        Ok(Self {
            program: crate::relational::SelectionProgram::new(ResolvedPredicate(
                native::predicate(&predicate),
            ))
            .expect("native predicate was validated"),
            predicate: std::sync::Arc::new(predicate),
        })
    }

    /// Build a validated conjunction from two or more already-validated predicates.
    ///
    /// This preserves the `PredicatePlan` invariant without re-validating every
    /// child: composition uses each child's resolved expression directly, and
    /// [`AtLeast`] makes an empty conjunction unrepresentable.
    ///
    /// ```
    /// use helix_ast::expr::Predicate;
    /// use helix_planner::ir::{AtLeast, PredicatePlan, native};
    ///
    /// let first = PredicatePlan::new(Predicate::eq("active", true)).unwrap();
    /// let second = PredicatePlan::new(Predicate::eq("tenant", "acme")).unwrap();
    /// let merged = PredicatePlan::conjunction(&AtLeast::<_, 2>::from_pair(first, second));
    ///
    /// assert!(matches!(merged.predicate(), Predicate::And { predicates } if predicates.len() == 2));
    /// assert!(matches!(merged.resolved(), native::Expression::Function(native::Function::All, children) if children.len() == 2));
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
            program: crate::relational::SelectionProgram::new(ResolvedPredicate(
                native::Expression::Function(
                    native::Function::All,
                    predicates
                        .iter()
                        .map(|child| child.resolved().clone())
                        .collect(),
                ),
            ))
            .expect("native predicate was validated"),
            predicate: std::sync::Arc::new(predicate),
        }
    }

    /// Flatten resolved predicates from a nonempty filter run. Flatten only
    /// outer conjunctions; retain OR, NOT and CASE structure and operand order.
    /// The required first child makes an empty result unrepresentable.
    pub(crate) fn flattened_conjunction(first: &Self, rest: &[Self]) -> Self {
        let children = std::iter::once(first).chain(rest);
        // Count borrowed common-expression leaves before cloning. Composed
        // plans retain exact vectors instead of geometric spare capacity.
        let mut count = 0_usize;
        for child in children.clone() {
            visit_resolved_conjuncts(child.resolved(), &mut |_| {
                count = count
                    .checked_add(1)
                    .expect("native conjunct count fits usize");
            });
        }
        let mut expressions = Vec::with_capacity(count);
        let mut predicates = Vec::with_capacity(count);
        for child in children {
            visit_resolved_conjuncts(child.resolved(), &mut |expression| {
                expressions.push(expression.clone());
            });
            flatten_compatibility(child.predicate(), &mut predicates);
        }
        assert_eq!(
            predicates.len(),
            count,
            "native compatibility view preserves conjuncts"
        );
        Self {
            program: crate::relational::SelectionProgram::new(ResolvedPredicate(
                native::Expression::Function(native::Function::All, expressions),
            ))
            .expect("flattening validated predicates preserves dependencies"),
            predicate: std::sync::Arc::new(Predicate::and(predicates)),
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

    /// Common selection contract; the native adapter retains two-valued semantics.
    pub fn program(&self) -> &crate::relational::SelectionProgram<ResolvedPredicate> {
        &self.program
    }

    /// Borrow the shared scalar representation with native predicate semantics.
    pub fn resolved(&self) -> &native::Expression {
        self.program.expression().expression()
    }
}

fn visit_resolved_conjuncts(
    expression: &native::Expression,
    visit: &mut impl FnMut(&native::Expression),
) {
    match expression {
        native::Expression::Function(native::Function::All, children) => {
            for child in children {
                visit_resolved_conjuncts(child, visit);
            }
        }
        expression => visit(expression),
    }
}

fn flatten_compatibility(predicate: &Predicate, output: &mut Vec<Predicate>) {
    match predicate {
        Predicate::And { predicates } => {
            for predicate in predicates {
                flatten_compatibility(predicate, output);
            }
        }
        predicate => output.push(predicate.clone()),
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
