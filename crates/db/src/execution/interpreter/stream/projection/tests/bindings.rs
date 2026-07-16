use super::*;

#[tokio::test]
async fn binding_projection_reads_current_binding_and_coalesced_values() {
    let db = test_support::open_db("projection-binding-contracts").await;
    let current = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("nickname", PropertyValue::Null)],
    )
    .await;
    let bound = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("ada"))],
    )
    .await;
    let binding = name("person");
    let mut row = ExecutionRow::current(ElementRef::Node(current));
    row.bindings
        .insert(binding.clone(), ElementRef::Node(bound));
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.binding_target(&row, &ir::BindingTargetPlan::Current),
        Some((ElementRef::Node(current), RowVirtualProperties::empty()))
    );
    assert_eq!(
        ctx.binding_projection(
            &row,
            &ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(binding.clone()),
                source: name("name"),
                alias: name("display"),
            },
        )
        .await
        .unwrap(),
        Some((
            "display".to_string(),
            DbPropertyValue::String("ada".to_string()),
        ))
    );
    assert_eq!(
        ctx.binding_projection(
            &row,
            &ir::BindingProjectionPlan::Coalesce {
                refs: binding_refs(vec![
                    ir::BindingValueRefPlan {
                        target: ir::BindingTargetPlan::Current,
                        source: name("nickname"),
                    },
                    ir::BindingValueRefPlan {
                        target: ir::BindingTargetPlan::Binding(binding),
                        source: name("name"),
                    },
                ]),
                alias: name("display"),
            },
        )
        .await
        .unwrap(),
        Some((
            "display".to_string(),
            DbPropertyValue::String("ada".to_string()),
        ))
    );
    assert_eq!(
        ctx.binding_target(&row, &ir::BindingTargetPlan::Binding(name("missing"))),
        None
    );
    assert_eq!(
        ctx.binding_projection(
            &row,
            &ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(name("missing")),
                source: name("name"),
                alias: name("display"),
            },
        )
        .await
        .unwrap(),
        None
    );
    assert_eq!(
        ctx.binding_projection(
            &row,
            &ir::BindingProjectionPlan::Coalesce {
                refs: binding_refs(vec![ir::BindingValueRefPlan {
                    target: ir::BindingTargetPlan::Binding(name("missing")),
                    source: name("name"),
                }]),
                alias: name("display"),
            },
        )
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
async fn binding_projection_reads_snapshotted_virtual_properties() {
    let db = test_support::open_db("projection-binding-virtual-properties").await;
    let hit = test_support::add_user(&db, "hit").await;
    let neighbor = test_support::add_user(&db, "neighbor").await;
    let binding = name("hit");
    let distance = name("$distance");
    let mut row = ExecutionRow::current(ElementRef::Node(neighbor));
    row.bindings.insert(binding.clone(), ElementRef::Node(hit));
    row.binding_virtual_properties.insert(
        binding.clone(),
        RowVirtualProperties::from_one(distance.clone(), DbPropertyValue::F64(0.25)),
    );
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.binding_projection(
            &row,
            &ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(binding),
                source: distance.clone(),
                alias: name("hit_distance"),
            },
        )
        .await
        .unwrap(),
        Some(("hit_distance".to_string(), DbPropertyValue::F64(0.25)))
    );
    assert_eq!(
        ctx.binding_projection(
            &row,
            &ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: distance,
                alias: name("current_distance"),
            },
        )
        .await
        .unwrap(),
        None
    );
}
