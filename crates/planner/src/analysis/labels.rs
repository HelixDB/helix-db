use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use crate::error::PlannerError;
use crate::ir::{self, NameField, NonEmptyString};

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

/// Finite set of labels a predicate admits, deduplicated in first-occurrence
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FiniteLabelDomain {
    Empty,
    One(ir::NonEmptyString),
    Many(ir::AtLeast<ir::NonEmptyString, 2>),
}

/// Label domain of the top-level conjuncts of `predicate` that constrain only
/// `$label`, with the remaining conjuncts as a residual. `None` when no
/// conjunct is a pure label predicate.
pub(crate) fn conjunctive_label_domain(
    predicate: &Predicate,
) -> Option<(FiniteLabelDomain, Option<Predicate>)> {
    let Predicate::And { predicates } = predicate else {
        return pure_label_domain(predicate).map(|domain| (domain, None));
    };
    let mut domain = None;
    let mut residual = Vec::new();
    for predicate in predicates {
        match pure_label_domain(predicate) {
            Some(next) => {
                domain = Some(match domain {
                    Some(domain) => intersect_domains(domain, next),
                    None => next,
                });
            }
            None => residual.push(predicate.clone()),
        }
    }
    let domain = domain?;
    let residual = match residual.len() {
        0 => None,
        1 => residual.pop(),
        _ => Some(Predicate::and(residual)),
    };
    Some((domain, residual))
}

fn pure_label_domain(predicate: &Predicate) -> Option<FiniteLabelDomain> {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => label_equality(left, right),
        Predicate::IsIn { value, values } => label_membership(value, values),
        Predicate::And { predicates } => predicates
            .iter()
            .map(pure_label_domain)
            .try_fold(None, |domain, next| {
                Some(Some(match domain {
                    Some(domain) => intersect_domains(domain, next?),
                    None => next?,
                }))
            })
            .flatten(),
        Predicate::Or { predicates } => predicates
            .iter()
            .map(pure_label_domain)
            .try_fold(None, |domain, next| {
                Some(Some(match domain {
                    Some(domain) => union_domains(domain, next?),
                    None => next?,
                }))
            })
            .flatten(),
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
        | Predicate::Not { .. }
        | Predicate::Compare {
            op: CompareOp::Neq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte,
            ..
        } => None,
    }
}

fn label_equality(left: &Expr, right: &Expr) -> Option<FiniteLabelDomain> {
    match (left, right) {
        (Expr::Property(property), Expr::Constant(PropertyValue::String(label)))
        | (Expr::Constant(PropertyValue::String(label)), Expr::Property(property))
            if property == "$label" =>
        {
            Some(domain_from_labels([label.clone()]))
        }
        _ => None,
    }
}

fn label_membership(value: &Expr, values: &Expr) -> Option<FiniteLabelDomain> {
    let (Expr::Property(property), Expr::Constant(values)) = (value, values) else {
        return None;
    };
    if property != "$label" {
        return None;
    }
    let labels = match values {
        PropertyValue::String(label) => vec![label.clone()],
        PropertyValue::StringArray(labels) => labels.clone(),
        PropertyValue::Array(values) => values
            .iter()
            .filter_map(|value| match value {
                PropertyValue::String(label) => Some(label.clone()),
                _ => None,
            })
            .collect(),
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::Object(_) => Vec::new(),
    };
    Some(domain_from_labels(labels))
}

fn domain_from_labels(labels: impl IntoIterator<Item = String>) -> FiniteLabelDomain {
    let mut labels = labels.into_iter().fold(Vec::new(), |mut unique, label| {
        let Some(label) = ir::NonEmptyString::new(label) else {
            return unique;
        };
        if !unique.contains(&label) {
            unique.push(label);
        }
        unique
    });
    match labels.len() {
        0 => FiniteLabelDomain::Empty,
        1 => FiniteLabelDomain::One(
            labels
                .pop()
                .expect("one-label domain contains exactly one label"),
        ),
        _ => FiniteLabelDomain::Many(
            ir::AtLeast::try_from_vec(labels)
                .expect("multi-label domain contains at least two labels"),
        ),
    }
}

fn intersect_domains(left: FiniteLabelDomain, right: FiniteLabelDomain) -> FiniteLabelDomain {
    domain_from_labels(
        domain_labels(left)
            .into_iter()
            .filter(|label| domain_contains(&right, label))
            .map(ir::NonEmptyString::into_string),
    )
}

fn union_domains(left: FiniteLabelDomain, right: FiniteLabelDomain) -> FiniteLabelDomain {
    domain_from_labels(
        domain_labels(left)
            .into_iter()
            .chain(domain_labels(right))
            .map(ir::NonEmptyString::into_string),
    )
}

/// Labels of `domain`, in its order.
pub(crate) fn domain_labels(domain: FiniteLabelDomain) -> Vec<ir::NonEmptyString> {
    match domain {
        FiniteLabelDomain::Empty => Vec::new(),
        FiniteLabelDomain::One(label) => vec![label],
        FiniteLabelDomain::Many(labels) => labels.into_iter().collect(),
    }
}

/// Whether `domain` admits `label`.
pub(crate) fn domain_contains(domain: &FiniteLabelDomain, label: &ir::NonEmptyString) -> bool {
    match domain {
        FiniteLabelDomain::Empty => false,
        FiniteLabelDomain::One(candidate) => candidate == label,
        FiniteLabelDomain::Many(labels) => labels.contains(label),
    }
}

#[cfg(test)]
mod label_domain_tests {
    use super::*;

    #[test]
    fn pure_label_domains_normalize_intersections_unions_and_non_strings() {
        let predicate = Predicate::and(vec![
            Predicate::is_in(
                "$label",
                PropertyValue::StringArray(vec!["Person".to_owned(), "Organization".to_owned()]),
            ),
            Predicate::or(vec![
                Predicate::eq("$label", "Person"),
                Predicate::is_in(
                    "$label",
                    PropertyValue::array(["Team", "Organization", "Organization"]),
                ),
            ]),
        ]);

        assert_eq!(
            pure_label_domain(&predicate),
            Some(domain_from_labels([
                "Person".to_owned(),
                "Organization".to_owned()
            ]))
        );
        assert_eq!(
            pure_label_domain(&Predicate::is_in(
                "$label",
                PropertyValue::I64Array(vec![1, 2]),
            )),
            Some(FiniteLabelDomain::Empty)
        );
    }
}
