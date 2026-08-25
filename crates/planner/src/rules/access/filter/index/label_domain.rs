//! Bounded finite label-domain access.

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use super::super::AccessFilterRewrite;
use crate::{context, ir, logical};

#[derive(Debug, Clone, PartialEq, Eq)]
enum FiniteLabelDomain {
    Empty,
    One(ir::NonEmptyString),
    Many(ir::AtLeast<ir::NonEmptyString, 2>),
}

pub(in crate::rules) fn has_candidate(predicate: &Predicate) -> bool {
    conjunctive_label_domain(predicate).is_some()
}

pub(super) fn rewrite(
    access: &logical::AccessPath,
    predicate: &Predicate,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterRewrite {
    let Some((domain, residual)) = conjunctive_label_domain(predicate) else {
        return AccessFilterRewrite::NotApplicable;
    };
    if let FiniteLabelDomain::Many(labels) = &domain {
        let context::IndexUnionBranchLimit::Limited(limit) =
            planner_limits.max_index_union_branches
        else {
            return AccessFilterRewrite::NotApplicable;
        };
        if labels.len() > limit.get() {
            return AccessFilterRewrite::NotApplicable;
        }
    }

    let access = match access {
        logical::AccessPath::Node(path) => logical::AccessPath::Node(logical::NodeAccessPath::new(
            node_source(path.source(), &domain),
        )),
        logical::AccessPath::Edge(path) => logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            edge_source(path.source(), &domain),
        )),
    };
    if access.is_direct_empty() {
        return AccessFilterRewrite::Rewritten(access);
    }
    let Some(residual) = residual else {
        return AccessFilterRewrite::Rewritten(access);
    };
    let residual = ir::PredicatePlan::new(residual)
        .expect("label-domain residual comes from a validated access predicate");
    AccessFilterRewrite::RewrittenPipeline(
        logical::AccessPipeline::new(
            access,
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Filter {
                predicate: residual,
            }),
        )
        .expect("one label-domain residual is a valid access pipeline"),
    )
}

fn node_source(
    existing: &ir::NodeAccessSourcePlan,
    domain: &FiniteLabelDomain,
) -> ir::NodeAccessSourcePlan {
    if let Some(label) = existing.common_label() {
        return if domain_contains(domain, label) {
            existing.clone()
        } else {
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty)
        };
    }
    let domain = node_domain_source(domain);
    if matches!(existing.as_ref(), ir::NodeAccessPlan::AllScan) {
        domain
    } else {
        ir::NodeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::node_intersection_from_sources(vec![
                existing.clone(),
                domain,
            ]),
        )
    }
}

fn edge_source(
    existing: &ir::EdgeAccessSourcePlan,
    domain: &FiniteLabelDomain,
) -> ir::EdgeAccessSourcePlan {
    if let Some(label) = existing.common_label() {
        return if domain_contains(domain, label) {
            existing.clone()
        } else {
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty)
        };
    }
    let domain = edge_domain_source(domain);
    if matches!(existing.as_ref(), ir::EdgeAccessPlan::AllScan) {
        domain
    } else {
        ir::EdgeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::edge_intersection_from_sources(vec![
                existing.clone(),
                domain,
            ]),
        )
    }
}

fn node_domain_source(domain: &FiniteLabelDomain) -> ir::NodeAccessSourcePlan {
    match domain {
        FiniteLabelDomain::Empty => {
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty)
        }
        FiniteLabelDomain::One(label) => {
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::LabelScan {
                label: label.clone(),
            })
        }
        FiniteLabelDomain::Many(labels) => ir::NodeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::node_union_from_sources(
                labels
                    .iter()
                    .map(|label| {
                        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::LabelScan {
                            label: label.clone(),
                        })
                    })
                    .collect(),
            ),
        ),
    }
}

fn edge_domain_source(domain: &FiniteLabelDomain) -> ir::EdgeAccessSourcePlan {
    match domain {
        FiniteLabelDomain::Empty => {
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty)
        }
        FiniteLabelDomain::One(label) => {
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::LabelScan {
                label: label.clone(),
            })
        }
        FiniteLabelDomain::Many(labels) => ir::EdgeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::edge_union_from_sources(
                labels
                    .iter()
                    .map(|label| {
                        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::LabelScan {
                            label: label.clone(),
                        })
                    })
                    .collect(),
            ),
        ),
    }
}

fn conjunctive_label_domain(
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

fn domain_labels(domain: FiniteLabelDomain) -> Vec<ir::NonEmptyString> {
    match domain {
        FiniteLabelDomain::Empty => Vec::new(),
        FiniteLabelDomain::One(label) => vec![label],
        FiniteLabelDomain::Many(labels) => labels.into_iter().collect(),
    }
}

fn domain_contains(domain: &FiniteLabelDomain, label: &ir::NonEmptyString) -> bool {
    match domain {
        FiniteLabelDomain::Empty => false,
        FiniteLabelDomain::One(candidate) => candidate == label,
        FiniteLabelDomain::Many(labels) => labels.contains(label),
    }
}

#[cfg(test)]
mod tests {
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
