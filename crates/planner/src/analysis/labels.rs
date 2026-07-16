use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use crate::error::PlannerError;
use crate::ir::{NameField, NonEmptyString};

/// Label constraint extracted from a predicate tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LabelScope {
    /// Predicate cannot match any label.
    Impossible,
    /// Predicate may match rows and carries the label scope known for them.
    Feasible(FeasibleLabelScope),
}

/// Label scope for predicates that may match rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FeasibleLabelScope {
    /// No label can be proven for every candidate row.
    Unscoped,
    /// Every candidate row must have this label.
    Scoped(NonEmptyString),
}

impl LabelScope {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Impossible, _) | (_, Self::Impossible) => Self::Impossible,
            (Self::Feasible(left), Self::Feasible(right)) => left.and(right),
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::Impossible, Self::Impossible) => Self::Impossible,
            (Self::Impossible, scope) | (scope, Self::Impossible) => scope,
            (Self::Feasible(left), Self::Feasible(right)) => left.or(right),
        }
    }
}

impl FeasibleLabelScope {
    fn and(self, other: Self) -> LabelScope {
        match (self, other) {
            (Self::Unscoped, scope) | (scope, Self::Unscoped) => LabelScope::Feasible(scope),
            (Self::Scoped(left), Self::Scoped(right)) if left == right => {
                LabelScope::Feasible(Self::Scoped(left))
            }
            (Self::Scoped(_), Self::Scoped(_)) => LabelScope::Impossible,
        }
    }

    fn or(self, other: Self) -> LabelScope {
        match (self, other) {
            (Self::Unscoped, _) | (_, Self::Unscoped) => LabelScope::Feasible(Self::Unscoped),
            (Self::Scoped(left), Self::Scoped(right)) if left == right => {
                LabelScope::Feasible(Self::Scoped(left))
            }
            (Self::Scoped(_), Self::Scoped(_)) => LabelScope::Feasible(Self::Unscoped),
        }
    }
}

pub(crate) fn label_scope(predicate: &Predicate) -> Result<LabelScope, PlannerError> {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => match property_literal_string(left, right)
            .filter(|(property, _value)| property == "$label")
        {
            Some((_property, value)) => NonEmptyString::new(value)
                .map(|label| LabelScope::Feasible(FeasibleLabelScope::Scoped(label)))
                .ok_or(PlannerError::InvalidEmptyName {
                    field: NameField::Label,
                }),
            None => Ok(LabelScope::Feasible(FeasibleLabelScope::Unscoped)),
        },
        Predicate::And { predicates } => predicates.iter().map(label_scope).try_fold(
            LabelScope::Feasible(FeasibleLabelScope::Unscoped),
            |scope, next| Ok(scope.and(next?)),
        ),
        Predicate::Or { predicates } => predicates
            .iter()
            .map(label_scope)
            .try_fold(None::<LabelScope>, |scope, next| {
                Ok::<_, PlannerError>(Some(match scope {
                    Some(scope) => scope.or(next?),
                    None => next?,
                }))
            })?
            .map_or_else(
                || Ok(LabelScope::Feasible(FeasibleLabelScope::Unscoped)),
                Ok,
            ),
        Predicate::Neq { .. }
        | Predicate::Gt { .. }
        | Predicate::Gte { .. }
        | Predicate::Lt { .. }
        | Predicate::Lte { .. }
        | Predicate::Between { .. }
        | Predicate::HasKey { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::StartsWith { .. }
        | Predicate::EndsWith { .. }
        | Predicate::Contains { .. }
        | Predicate::IsIn { .. }
        | Predicate::Not { .. }
        | Predicate::Compare {
            op: CompareOp::Neq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte,
            ..
        } => Ok(LabelScope::Feasible(FeasibleLabelScope::Unscoped)),
    }
}

pub(crate) fn label_equality_atom(predicate: &Predicate) -> Option<String> {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => property_literal_string(left, right)
            .filter(|(property, _value)| property == "$label")
            .map(|(_property, value)| value),
        Predicate::Neq { .. }
        | Predicate::Gt { .. }
        | Predicate::Gte { .. }
        | Predicate::Lt { .. }
        | Predicate::Lte { .. }
        | Predicate::Between { .. }
        | Predicate::HasKey { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::StartsWith { .. }
        | Predicate::EndsWith { .. }
        | Predicate::Contains { .. }
        | Predicate::IsIn { .. }
        | Predicate::And { .. }
        | Predicate::Or { .. }
        | Predicate::Not { .. }
        | Predicate::Compare {
            op: CompareOp::Neq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte,
            ..
        } => None,
    }
}

fn property_literal_string(left: &Expr, right: &Expr) -> Option<(String, String)> {
    match (left, right) {
        (Expr::Property(property), Expr::Constant(PropertyValue::String(value))) => {
            Some((property.clone(), value.clone()))
        }
        (Expr::Constant(PropertyValue::String(value)), Expr::Property(property)) => {
            Some((property.clone(), value.clone()))
        }
        (Expr::Property(_), Expr::Constant(PropertyValue::Null))
        | (Expr::Property(_), Expr::Constant(PropertyValue::Bool(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::I64(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::DateTime(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::F64(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::F32(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::Bytes(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::I64Array(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::F64Array(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::F32Array(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::StringArray(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::Array(_)))
        | (Expr::Property(_), Expr::Constant(PropertyValue::Object(_)))
        | (Expr::Property(_), Expr::Param(_))
        | (Expr::Property(_), Expr::Property(_))
        | (Expr::Property(_), Expr::Id)
        | (Expr::Property(_), Expr::Timestamp)
        | (Expr::Property(_), Expr::DateTimeNow)
        | (Expr::Property(_), Expr::Add { .. })
        | (Expr::Property(_), Expr::Sub { .. })
        | (Expr::Property(_), Expr::Mul { .. })
        | (Expr::Property(_), Expr::Div { .. })
        | (Expr::Property(_), Expr::Mod { .. })
        | (Expr::Property(_), Expr::Neg { .. })
        | (Expr::Property(_), Expr::Case { .. })
        | (Expr::Constant(_), _)
        | (Expr::Param(_), _)
        | (Expr::Id, _)
        | (Expr::Timestamp, _)
        | (Expr::DateTimeNow, _)
        | (Expr::Add { .. }, _)
        | (Expr::Sub { .. }, _)
        | (Expr::Mul { .. }, _)
        | (Expr::Div { .. }, _)
        | (Expr::Mod { .. }, _)
        | (Expr::Neg { .. }, _)
        | (Expr::Case { .. }, _) => None,
    }
}
