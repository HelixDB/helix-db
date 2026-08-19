use super::*;
use crate::properties;

#[test]
fn limited_access_flattens_nested_limits_to_tightest_bound() {
    let access = ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan)
        .limited(properties::PositiveUsize::new(10).unwrap())
        .limited(properties::PositiveUsize::new(3).unwrap());

    let ExecAccessPlan::Limited(limited) = access else {
        panic!("expected limited access wrapper");
    };
    assert_eq!(limited.limit().get(), 3);
    assert!(matches!(
        limited.source(),
        ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan)
    ));
}

#[test]
fn access_read_limit_applies_only_when_bounded() {
    let unbounded =
        ExecAccessReadLimit::Unbounded.apply_to(ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan));
    assert!(matches!(
        unbounded,
        ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan)
    ));

    let bounded = ExecAccessReadLimit::bounded(properties::PositiveUsize::new(4).unwrap())
        .apply_to(ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan));
    assert!(matches!(
        bounded,
        ExecAccessPlan::Limited(limited) if limited.limit().get() == 4
    ));
}

#[test]
fn access_read_limit_elides_when_access_hard_upper_is_tighter() {
    let limit = ExecAccessReadLimit::bounded(properties::PositiveUsize::new(4).unwrap());

    assert_eq!(limit.elide_if_covered_by_hard_upper(None), limit);
    assert_eq!(limit.elide_if_covered_by_hard_upper(Some(8)), limit);
    assert_eq!(
        limit.elide_if_covered_by_hard_upper(Some(4)),
        ExecAccessReadLimit::Unbounded
    );
    assert_eq!(
        limit.elide_if_covered_by_hard_upper(Some(1)),
        ExecAccessReadLimit::Unbounded
    );
    assert_eq!(
        limit.elide_if_covered_by_hard_upper(Some(0)),
        ExecAccessReadLimit::Unbounded
    );
    assert_eq!(
        ExecAccessReadLimit::Unbounded.elide_if_covered_by_hard_upper(Some(1)),
        ExecAccessReadLimit::Unbounded
    );
}

#[test]
fn secondary_set_wire_contract_round_trips_logical_identity_only() {
    let key = crate::catalog::ScopedPropertyKey::try_new("User", "name").unwrap();
    let values = crate::ir::AtLeast::<_, 2>::try_from_vec(vec![
        crate::exec::ExecIndexedEqualityValue::try_from(
            crate::ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from("alice"))
                .unwrap(),
        )
        .unwrap(),
        crate::exec::ExecIndexedEqualityValue::try_from(
            crate::ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from("bob"))
                .unwrap(),
        )
        .unwrap(),
    ])
    .unwrap();
    let access = ExecAccessPlan::Node(ExecNodeAccessPlan::SecondarySet {
        set: ExecNodeSecondarySetPlan::Bitmap(crate::exec::ExecNodeBitmapExpr::BatchedUnionRead {
            index: crate::exec::ExecNodeNonUniqueEqualityIndex::try_from(
                crate::catalog::NodeEqualityIndexMeta::try_new("user_name").unwrap(),
            )
            .unwrap(),
            key,
            values,
        }),
    });

    let json = serde_json::to_string(&access).unwrap();
    assert!(!json.contains("generation"));
    assert!(!json.contains("physical"));
    assert_eq!(
        serde_json::from_str::<ExecAccessPlan>(&json).unwrap(),
        access
    );
}

#[test]
fn exact_equality_access_round_trips() {
    let access = ExecAccessPlan::Edge(ExecEdgeAccessPlan::Bitmap {
        bitmap: crate::exec::ExecEdgeBitmapExpr::PointRead {
            index: crate::exec::ExecEdgeNonUniqueEqualityIndex::new(
                crate::catalog::EdgeEqualityIndexMeta::try_new("follows_status").unwrap(),
            ),
            key: crate::catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            value: crate::exec::ExecIndexedEqualityValue::try_from(
                crate::ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(
                    "active",
                ))
                .unwrap(),
            )
            .unwrap(),
        },
    });

    let json = serde_json::to_string(&access).unwrap();
    assert_eq!(
        serde_json::from_str::<ExecAccessPlan>(&json).unwrap(),
        access
    );
}

fn equality_literal(value: helix_ast::value::PropertyValue) -> crate::ir::IndexValue {
    crate::ir::IndexValue::Literal(crate::ir::SecondaryIndexLiteral::new(value).unwrap())
}

#[test]
fn node_equality_classification_covers_every_literal_and_runtime_family() {
    let key = crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
    let non_unique = crate::catalog::NodeEqualityIndexMeta::try_new("node_eq:User:email").unwrap();
    let unique = non_unique
        .clone()
        .with_uniqueness(crate::catalog::IndexUniqueness::Unique);

    assert!(matches!(
        ExecNodeAccessPlan::exact_equality(
            non_unique.clone(),
            key.clone(),
            equality_literal(helix_ast::value::PropertyValue::from("alice@example.com")),
        ),
        ExecNodeAccessPlan::Bitmap {
            bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { .. }
        }
    ));
    assert!(matches!(
        ExecNodeAccessPlan::exact_equality(
            unique,
            key.clone(),
            equality_literal(helix_ast::value::PropertyValue::from("alice@example.com")),
        ),
        ExecNodeAccessPlan::Unique { .. }
    ));
    assert!(matches!(
        ExecNodeAccessPlan::exact_equality(
            non_unique.clone(),
            key.clone(),
            equality_literal(helix_ast::value::PropertyValue::Null),
        ),
        ExecNodeAccessPlan::AuthoritativeScan {
            predicate: crate::exec::ExecNodeAuthoritativeScanPredicate::NullEquality { .. }
        }
    ));
    for nan in [
        helix_ast::value::PropertyValue::F32(f32::NAN),
        helix_ast::value::PropertyValue::F32(f32::from_bits(0x7fc0_0001)),
        helix_ast::value::PropertyValue::F64(f64::NAN),
        helix_ast::value::PropertyValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
    ] {
        assert_eq!(
            ExecNodeAccessPlan::exact_equality(
                non_unique.clone(),
                key.clone(),
                equality_literal(nan)
            ),
            ExecNodeAccessPlan::Empty
        );
    }
    let param = crate::ir::NonEmptyString::new("late_email").unwrap();
    assert!(matches!(
        ExecNodeAccessPlan::exact_equality(
            non_unique,
            key,
            crate::ir::IndexValue::Param(param.clone()),
        ),
        ExecNodeAccessPlan::DynamicEquality { param: actual, .. } if actual == param
    ));
}

#[test]
fn edge_equality_classification_has_no_unique_algorithm() {
    let key = crate::catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let index = crate::catalog::EdgeEqualityIndexMeta::try_new("edge_eq:FOLLOWS:status").unwrap();
    assert!(matches!(
        ExecEdgeAccessPlan::exact_equality(
            index.clone(),
            key.clone(),
            equality_literal(helix_ast::value::PropertyValue::from("active")),
        ),
        ExecEdgeAccessPlan::Bitmap {
            bitmap: crate::exec::ExecEdgeBitmapExpr::PointRead { .. }
        }
    ));
    assert!(matches!(
        ExecEdgeAccessPlan::exact_equality(
            index.clone(),
            key.clone(),
            equality_literal(helix_ast::value::PropertyValue::Null),
        ),
        ExecEdgeAccessPlan::AuthoritativeScan { .. }
    ));
    assert_eq!(
        ExecEdgeAccessPlan::exact_equality(
            index.clone(),
            key.clone(),
            equality_literal(helix_ast::value::PropertyValue::F64(f64::NAN)),
        ),
        ExecEdgeAccessPlan::Empty
    );
    assert!(matches!(
        ExecEdgeAccessPlan::exact_equality(
            index,
            key,
            crate::ir::IndexValue::Param(crate::ir::NonEmptyString::new("late_status").unwrap()),
        ),
        ExecEdgeAccessPlan::DynamicEquality { .. }
    ));
}

#[test]
fn same_index_equalities_encode_point_batch_and_ordered_union_exactly() {
    let key = crate::catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let index = crate::catalog::NodeEqualityIndexMeta::try_new("node_eq:User:status").unwrap();
    let active = equality_literal(helix_ast::value::PropertyValue::from("active"));
    let inactive = equality_literal(helix_ast::value::PropertyValue::from("inactive"));

    assert!(matches!(
        ExecNodeSecondarySetPlan::exact_equalities(
            index.clone(),
            key.clone(),
            crate::ir::AtLeast::from_one(active.clone()),
        ),
        ExecNodeSecondarySetPlan::Bitmap(crate::exec::ExecNodeBitmapExpr::PointRead { .. })
    ));
    let batched = ExecNodeSecondarySetPlan::exact_equalities(
        index.clone(),
        key.clone(),
        crate::ir::AtLeast::<_, 1>::try_from_vec(vec![active.clone(), active.clone()]).unwrap(),
    );
    let ExecNodeSecondarySetPlan::Bitmap(crate::exec::ExecNodeBitmapExpr::BatchedUnionRead {
        values,
        ..
    }) = batched
    else {
        panic!("two same-index values must encode one literal batch")
    };
    assert_eq!(
        values.len(),
        2,
        "equivalent keys remain explicit batch inputs"
    );

    let mixed = ExecNodeSecondarySetPlan::exact_equalities(
        index,
        key,
        crate::ir::AtLeast::try_from_vec(vec![
            active,
            equality_literal(helix_ast::value::PropertyValue::Null),
            inactive,
        ])
        .unwrap(),
    );
    let ExecNodeSecondarySetPlan::Union { driver, rest } = mixed else {
        panic!("different physical families must remain an explicit ordered union")
    };
    assert!(matches!(
        driver.as_ref(),
        ExecNodeSecondarySetPlan::Bitmap(crate::exec::ExecNodeBitmapExpr::PointRead { .. })
    ));
    assert!(matches!(
        rest.first(),
        Some(ExecNodeSecondarySetPlan::AuthoritativeScan(_))
    ));
    assert!(matches!(
        rest.get(1),
        Some(ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::PointRead { .. }
        ))
    ));
}

#[test]
fn exact_set_driver_rest_contract_round_trips_and_rejects_empty_rest() {
    let point = ExecNodeSecondarySetPlan::exact_equalities(
        crate::catalog::NodeEqualityIndexMeta::try_new("node_eq:User:status").unwrap(),
        crate::catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
        crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::from(
            "active",
        ))),
    );
    let set = ExecNodeSecondarySetPlan::Intersect {
        driver: Box::new(point.clone()),
        rest: crate::ir::AtLeast::from_one(point),
    };
    let json = serde_json::to_string(&set).unwrap();
    assert_eq!(
        serde_json::from_str::<ExecNodeSecondarySetPlan>(&json).unwrap(),
        set
    );

    let mut malformed = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    malformed["intersect"]["rest"] = serde_json::json!([]);
    assert!(serde_json::from_value::<ExecNodeSecondarySetPlan>(malformed).is_err());
}

#[test]
fn edge_same_index_equalities_preserve_batch_arity_and_mixed_order() {
    let index = crate::catalog::EdgeEqualityIndexMeta::try_new("edge_eq:FOLLOWS:status").unwrap();
    let key = crate::catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let active = equality_literal(helix_ast::value::PropertyValue::from("active"));
    let batch = ExecEdgeSecondarySetPlan::exact_equalities(
        index.clone(),
        key.clone(),
        crate::ir::AtLeast::<_, 1>::try_from_vec(vec![active.clone(), active.clone()]).unwrap(),
    );
    let ExecEdgeSecondarySetPlan::Bitmap(crate::exec::ExecEdgeBitmapExpr::BatchedUnionRead {
        values,
        ..
    }) = batch
    else {
        panic!("two edge values must encode one literal batch")
    };
    assert_eq!(values.len(), 2);

    let mixed = ExecEdgeSecondarySetPlan::exact_equalities(
        index,
        key,
        crate::ir::AtLeast::<_, 1>::try_from_vec(vec![
            crate::ir::IndexValue::Param(crate::ir::NonEmptyString::new("late_status").unwrap()),
            active,
        ])
        .unwrap(),
    );
    let ExecEdgeSecondarySetPlan::Union { driver, rest } = mixed else {
        panic!("dynamic and literal edge equalities remain an ordered union")
    };
    assert!(matches!(
        driver.as_ref(),
        ExecEdgeSecondarySetPlan::DynamicEquality { .. }
    ));
    assert!(matches!(
        rest.first(),
        Some(ExecEdgeSecondarySetPlan::Bitmap(
            crate::exec::ExecEdgeBitmapExpr::PointRead { .. }
        ))
    ));
}

#[test]
fn node_exact_equalities_cover_unique_null_nan_dynamic_and_singleton_shapes() {
    let key = crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
    let unique = crate::catalog::NodeEqualityIndexMeta::try_new("node_eq:User:email")
        .unwrap()
        .with_uniqueness(crate::catalog::IndexUniqueness::Unique);
    assert!(matches!(
        ExecNodeSecondarySetPlan::exact_equalities(
            unique.clone(),
            key.clone(),
            crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::from(
                "alice@example.test"
            ))),
        ),
        ExecNodeSecondarySetPlan::Unique { .. }
    ));
    assert!(matches!(
        ExecNodeSecondarySetPlan::exact_equalities(
            unique,
            key.clone(),
            crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::Null)),
        ),
        ExecNodeSecondarySetPlan::AuthoritativeScan(_)
    ));
    assert_eq!(
        ExecNodeSecondarySetPlan::exact_equalities(
            crate::catalog::NodeEqualityIndexMeta::try_new("node_eq:User:email").unwrap(),
            key.clone(),
            crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::F64(
                f64::NAN
            ))),
        ),
        ExecNodeSecondarySetPlan::Empty
    );
    assert!(matches!(
        ExecNodeSecondarySetPlan::exact_equalities(
            crate::catalog::NodeEqualityIndexMeta::try_new("node_eq:User:email").unwrap(),
            key,
            crate::ir::AtLeast::from_one(crate::ir::IndexValue::Param(
                crate::ir::NonEmptyString::new("late_email").unwrap()
            )),
        ),
        ExecNodeSecondarySetPlan::DynamicEquality { .. }
    ));
}

#[test]
fn edge_exact_equalities_cover_singleton_null_nan_and_ordered_union_shapes() {
    let key = crate::catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap();
    let index = crate::catalog::EdgeEqualityIndexMeta::try_new("edge_eq:LIKES:status").unwrap();
    assert!(matches!(
        ExecEdgeSecondarySetPlan::exact_equalities(
            index.clone(),
            key.clone(),
            crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::from(
                "active"
            ))),
        ),
        ExecEdgeSecondarySetPlan::Bitmap(crate::exec::ExecEdgeBitmapExpr::PointRead { .. })
    ));
    assert!(matches!(
        ExecEdgeSecondarySetPlan::exact_equalities(
            index.clone(),
            key.clone(),
            crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::Null)),
        ),
        ExecEdgeSecondarySetPlan::AuthoritativeScan(_)
    ));
    assert_eq!(
        ExecEdgeSecondarySetPlan::exact_equalities(
            index.clone(),
            key.clone(),
            crate::ir::AtLeast::from_one(equality_literal(helix_ast::value::PropertyValue::F32(
                f32::NAN
            ))),
        ),
        ExecEdgeSecondarySetPlan::Empty
    );
    assert!(matches!(
        ExecEdgeSecondarySetPlan::exact_equalities(
            index,
            key,
            crate::ir::AtLeast::from_one_and_rest(
                equality_literal(helix_ast::value::PropertyValue::Null),
                vec![equality_literal(helix_ast::value::PropertyValue::from(
                    "active"
                ))],
            ),
        ),
        ExecEdgeSecondarySetPlan::Union { .. }
    ));
}
