//! Bounded finite label-domain access.

use helix_ast::expr::Predicate;

use super::super::AccessFilterRewrite;
use crate::{analysis, context, ir, logical};

pub(in crate::rules) fn has_candidate(predicate: &Predicate) -> bool {
    analysis::conjunctive_label_domain(predicate).is_some()
}

pub(super) fn rewrite(
    access: &logical::AccessPath,
    predicate: &Predicate,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterRewrite {
    let Some((domain, residual)) = analysis::conjunctive_label_domain(predicate) else {
        return AccessFilterRewrite::NotApplicable;
    };
    if let analysis::FiniteLabelDomain::Many(labels) = &domain {
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
    domain: &analysis::FiniteLabelDomain,
) -> ir::NodeAccessSourcePlan {
    if let Some(label) = existing.common_label() {
        return if analysis::domain_contains(domain, label) {
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
    domain: &analysis::FiniteLabelDomain,
) -> ir::EdgeAccessSourcePlan {
    if let Some(label) = existing.common_label() {
        return if analysis::domain_contains(domain, label) {
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

fn node_domain_source(domain: &analysis::FiniteLabelDomain) -> ir::NodeAccessSourcePlan {
    match domain {
        analysis::FiniteLabelDomain::Empty => {
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty)
        }
        analysis::FiniteLabelDomain::One(label) => {
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::LabelScan {
                label: label.clone(),
            })
        }
        analysis::FiniteLabelDomain::Many(labels) => ir::NodeAccessSourcePlan::from_unfiltered(
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

fn edge_domain_source(domain: &analysis::FiniteLabelDomain) -> ir::EdgeAccessSourcePlan {
    match domain {
        analysis::FiniteLabelDomain::Empty => {
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty)
        }
        analysis::FiniteLabelDomain::One(label) => {
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::LabelScan {
                label: label.clone(),
            })
        }
        analysis::FiniteLabelDomain::Many(labels) => ir::EdgeAccessSourcePlan::from_unfiltered(
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
