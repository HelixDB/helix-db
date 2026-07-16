//! Root control-flow proof contracts.

use super::super::super::access_path_result;
use crate::{ir, logical, optimizer, properties};

pub(super) fn control_flow_delivered() -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        materialization: properties::Materialization::Materialized,
        effect: properties::EffectKind::Barrier,
        ..properties::DeliveredProperties::default()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ControlFlowInputRewrite {
    NonEmptyInput,
    EmptyAccess(logical::AccessPath),
}

impl ControlFlowInputRewrite {
    pub(super) const fn is_empty_access(&self) -> bool {
        matches!(self, Self::EmptyAccess(_))
    }

    pub(super) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::NonEmptyInput => optimizer::RuleResult::NotApplicable,
            Self::EmptyAccess(access) => access_path_result(access),
        }
    }
}

pub(super) fn empty_access_for_input(input: &logical::LogicalExpr) -> ControlFlowInputRewrite {
    match input {
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(path))
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty) =>
        {
            ControlFlowInputRewrite::EmptyAccess(empty_access_path(properties::ElementKind::Node))
        }
        logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(path))
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty) =>
        {
            ControlFlowInputRewrite::EmptyAccess(empty_access_path(properties::ElementKind::Edge))
        }
        _ => ControlFlowInputRewrite::NonEmptyInput,
    }
}

fn empty_access_path(element: properties::ElementKind) -> logical::AccessPath {
    match element {
        properties::ElementKind::Node => logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty),
        )),
        properties::ElementKind::Edge => logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_access(plan: ir::NodeAccessPlan) -> logical::LogicalExpr {
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(plan),
        )))
    }

    #[test]
    fn empty_input_rewrite_distinguishes_empty_access_from_non_empty_input() {
        let empty = empty_access_for_input(&node_access(ir::NodeAccessPlan::Empty));
        let non_empty = empty_access_for_input(&node_access(ir::NodeAccessPlan::AllScan));

        assert!(empty.is_empty_access());
        assert_eq!(non_empty, ControlFlowInputRewrite::NonEmptyInput);
    }

    #[test]
    fn empty_input_rewrite_converts_non_empty_to_not_applicable() {
        assert_eq!(
            ControlFlowInputRewrite::NonEmptyInput.into_rule_result(),
            optimizer::RuleResult::NotApplicable
        );
    }

    #[test]
    fn control_flow_delivered_is_materialized_barrier() {
        let delivered = control_flow_delivered();

        assert_eq!(
            delivered.materialization,
            properties::Materialization::Materialized
        );
        assert_eq!(delivered.effect, properties::EffectKind::Barrier);
    }
}
