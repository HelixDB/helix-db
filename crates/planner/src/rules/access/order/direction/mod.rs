//! Range-index direction rewrites for access ordering.
//!
//! The facade owns the optimizer-facing rule boundary. Contract outcomes,
//! source matching, and node/edge rewrite application live in narrower sibling
//! modules so each invariant can be tested at the layer that owns it.

mod contracts;
mod edge;
mod node;
mod source;

pub(in crate::rules::access) use contracts::AccessOrderRangeDirectionRewrite;

use crate::{catalog, logical};

pub(in crate::rules::access) fn rewrite_access_order_range_direction(
    order: &logical::AccessOrder,
    indexes: &catalog::IndexCatalogSnapshot,
) -> AccessOrderRangeDirectionRewrite {
    match order.access() {
        logical::AccessPath::Node(path) => contracts::direction_application_rewrite(
            node::rewrite_access_order_range_direction(path.source(), order.ordering(), indexes),
            |source| logical::AccessPath::Node(logical::NodeAccessPath::new(source)),
        ),
        logical::AccessPath::Edge(path) => contracts::direction_application_rewrite(
            edge::rewrite_access_order_range_direction(path.source(), order.ordering(), indexes),
            |source| logical::AccessPath::Edge(logical::EdgeAccessPath::new(source)),
        ),
    }
}

pub(super) fn order_for_range_direction(
    direction: helix_ast::index::RangeIndexDirection,
) -> helix_ast::traversal::Order {
    match direction {
        helix_ast::index::RangeIndexDirection::Asc => helix_ast::traversal::Order::Asc,
        helix_ast::index::RangeIndexDirection::Desc => helix_ast::traversal::Order::Desc,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ir, optimizer};

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn lower_range(value: i64) -> ir::IndexRange {
        ir::IndexRange::Lower {
            lower: ir::IndexBound::Inclusive(
                ir::RangeIndexValue::literal(helix_ast::value::PropertyValue::from(value)).unwrap(),
            ),
        }
    }

    fn node_access(plan: ir::NodeAccessPlan) -> logical::AccessOrder {
        logical::AccessOrder::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::from_unfiltered(plan),
            )),
            ir::OrderKeys::from(ir::OrderKey {
                property: ir::NonEmptyString::new("age").unwrap(),
                order: helix_ast::traversal::Order::Asc,
            }),
        )
    }

    fn node_source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
        ir::NodeAccessSourcePlan::from_unfiltered(plan)
    }

    fn edge_source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
        ir::EdgeAccessSourcePlan::from_unfiltered(plan)
    }

    fn node_range_source(
        property: &str,
        direction: helix_ast::index::RangeIndexDirection,
    ) -> ir::NodeAccessSourcePlan {
        node_source(ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new("User", property, direction).unwrap(),
            range: lower_range(18),
        })
    }

    fn edge_range_source(
        property: &str,
        direction: helix_ast::index::RangeIndexDirection,
    ) -> ir::EdgeAccessSourcePlan {
        edge_source(ir::EdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::try_new("edge_range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new("LIKES", property, direction)
                .unwrap(),
            range: lower_range(3),
        })
    }

    fn desc_age_ordering() -> ir::OrderKeys {
        ir::OrderKeys::from(ir::OrderKey {
            property: name("age"),
            order: helix_ast::traversal::Order::Desc,
        })
    }

    #[test]
    fn range_direction_rewrite_distinguishes_unavailable_rewrite() {
        let indexes = catalog::IndexCatalogSnapshot::default();
        let rewrite = rewrite_access_order_range_direction(
            &node_access(ir::NodeAccessPlan::AllScan),
            &indexes,
        );

        assert_eq!(rewrite, AccessOrderRangeDirectionRewrite::NotApplicable);
        assert!(!rewrite.is_rewritten());
    }

    #[test]
    fn range_direction_rewrite_converts_not_applicable_to_rule_result() {
        assert_eq!(
            AccessOrderRangeDirectionRewrite::NotApplicable.into_rule_result(),
            optimizer::RuleResult::NotApplicable
        );
    }

    #[test]
    fn range_direction_source_rewrite_reports_missing_index_and_rewrites_node_and_edge() {
        let desc_node_key = catalog::ScopedPropertyDirectionKey::try_new(
            "User",
            "age",
            helix_ast::index::RangeIndexDirection::Desc,
        )
        .unwrap();
        let desc_edge_key = catalog::ScopedPropertyDirectionKey::try_new(
            "LIKES",
            "age",
            helix_ast::index::RangeIndexDirection::Desc,
        )
        .unwrap();
        let indexes = catalog::IndexCatalogSnapshot::default()
            .with_node_range(desc_node_key.clone())
            .with_edge_range(desc_edge_key.clone());

        assert_eq!(
            node::rewrite_access_order_range_direction(
                &node_range_source("age", helix_ast::index::RangeIndexDirection::Asc),
                &desc_age_ordering(),
                &catalog::IndexCatalogSnapshot::default(),
            ),
            contracts::RangeDirectionRewriteApplication::NotApplicable(
                contracts::RangeDirectionRewriteRejection::MissingIndex
            )
        );
        assert!(matches!(
            node::rewrite_access_order_range_direction(
                &node_range_source("age", helix_ast::index::RangeIndexDirection::Asc),
                &desc_age_ordering(),
                &indexes,
            ),
            contracts::RangeDirectionRewriteApplication::Rewritten(source)
                if matches!(source.as_ref(), ir::NodeAccessPlan::RangeIndex { key, .. } if key == &desc_node_key)
        ));
        assert!(matches!(
            edge::rewrite_access_order_range_direction(
                &edge_range_source("age", helix_ast::index::RangeIndexDirection::Asc),
                &desc_age_ordering(),
                &indexes,
            ),
            contracts::RangeDirectionRewriteApplication::Rewritten(source)
                if matches!(source.as_ref(), ir::EdgeAccessPlan::RangeIndex { key, .. } if key == &desc_edge_key)
        ));
    }
}
