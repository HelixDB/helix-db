use super::*;

#[test]
fn access_path_rule_delivers_index_ordering_cardinality_and_locality() {
    let rule = AccessPathImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let unique = catalog::NodeEqualityIndexMeta::try_new("user_email")
        .unwrap()
        .with_uniqueness(catalog::IndexUniqueness::Unique);
    let equality = node_access_expr(ir::NodeAccessPlan::EqualityIndex {
        index: unique,
        key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
        value: ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from("a@example.com"))
                .unwrap(),
        ),
    });
    let range = edge_access_expr(ir::EdgeAccessPlan::RangeIndex {
        index: catalog::EdgeRangeIndexMeta::try_new("edge_weight").unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "LIKES",
            "weight",
            helix_ast::index::RangeIndexDirection::Desc,
        )
        .unwrap(),
        range: ir::IndexRange::Lower {
            lower: ir::IndexBound::Inclusive(
                ir::RangeIndexValue::literal(helix_ast::value::PropertyValue::from(1)).unwrap(),
            ),
        },
    });

    let equality = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &equality,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let range = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &range,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(equality.delivered.cardinality.upper(), Some(1));
    assert_eq!(
        equality.delivered.key_locality,
        properties::KeyLocality::Close
    );
    assert!(matches!(
        equality.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::EqualityIndex,
            ..
        }
    ));
    assert!(matches!(
        range.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(ref keys)
            if keys.as_ref()[0].property.as_ref() == "weight"
                && keys.as_ref()[0].order == helix_ast::traversal::Order::Desc
    ));
    assert_eq!(range.delivered.key_locality, properties::KeyLocality::Close);
    assert!(matches!(
        range.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::RangeIndex,
            ..
        }
    ));
}
