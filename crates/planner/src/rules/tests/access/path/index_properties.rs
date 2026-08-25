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
            access: physical::PhysicalAccess::NodeExact(exact),
            ..
        } if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::Unique { .. })
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

#[test]
fn access_path_rule_selects_every_exact_equality_physical_family() {
    let rule = AccessPathImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let index = catalog::NodeEqualityIndexMeta::try_new("node_eq:User:status").unwrap();
    let physical = |value: ir::IndexValue, index: catalog::NodeEqualityIndexMeta| {
        let expr = node_access_expr(ir::NodeAccessPlan::EqualityIndex {
            index,
            key: key.clone(),
            value,
        });
        let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }));
        let physical::PhysicalExpr::Access { access, .. } = alternative.expr else {
            panic!("equality implementation must be an access alternative")
        };
        (access, alternative.delivered.cardinality)
    };

    let indexed = ir::IndexValue::Literal(
        ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from("active")).unwrap(),
    );
    assert!(matches!(
        physical(indexed.clone(), index.clone()).0,
        physical::PhysicalAccess::NodeExact(exact)
            if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::Bitmap { .. })
    ));
    assert!(matches!(
        physical(
            indexed,
            index
                .clone()
                .with_uniqueness(catalog::IndexUniqueness::Unique),
        )
        .0,
        physical::PhysicalAccess::NodeExact(exact)
            if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::Unique { .. })
    ));
    let (null, null_cardinality) = physical(
        ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::Null).unwrap(),
        ),
        index.clone(),
    );
    assert!(matches!(
        null,
        physical::PhysicalAccess::NodeExact(exact)
            if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::AuthoritativeScan { .. })
    ));
    assert_eq!(null_cardinality, properties::CardinalityBounds::unknown());
    let (nan, nan_cardinality) = physical(
        ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::F64(f64::NAN)).unwrap(),
        ),
        index.clone(),
    );
    assert!(matches!(
        nan,
        physical::PhysicalAccess::NodeExact(exact)
            if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::Empty)
    ));
    assert_eq!(nan_cardinality, properties::CardinalityBounds::exact(0));
    let (dynamic, dynamic_cardinality) = physical(
        ir::IndexValue::Param(name("late_status")),
        index.with_uniqueness(catalog::IndexUniqueness::Unique),
    );
    assert!(matches!(
        dynamic,
        physical::PhysicalAccess::NodeExact(exact)
            if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::DynamicEquality { .. })
    ));
    assert_eq!(
        dynamic_cardinality,
        properties::CardinalityBounds::unknown()
    );
}
