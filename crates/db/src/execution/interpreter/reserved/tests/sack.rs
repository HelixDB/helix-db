use super::support::*;

#[tokio::test]
async fn sack_operations_track_row_local_state() {
    let db = test_support::open_db("reserved_sack_state").await;
    let alice = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("alice")),
            ("score", PropertyValue::from(2_i64)),
        ],
    )
    .await;

    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let with_sack_id = exec::ExecStepId::new(2).expect("positive step id");
    let add_id = exec::ExecStepId::new(3).expect("positive step id");
    let start = test_support::name("start");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_param_step(1, start.clone()),
            reserved_step(
                2,
                access_id.get(),
                ir::ReservedOp::WithSack(helix_ast::value::PropertyValue::I64(1)),
            ),
            reserved_step(
                3,
                with_sack_id.get(),
                ir::ReservedOp::SackAdd(test_support::name("score")),
            ),
            reserved_step(4, add_id.get(), ir::ReservedOp::SackGet),
        ],
        4,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(start, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("sack plan executes");

    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("sack_get should preserve stream shape");
    };
    assert_eq!(rows.len(), 1);
    assert!(rows[0].sack.is_visible());
    assert_eq!(rows[0].sack.value(), Some(&DbPropertyValue::I64(3)));
}

#[tokio::test]
async fn sack_set_replaces_state_with_current_property() {
    let db = test_support::open_db("reserved_sack_set_state").await;
    let alice = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("alice")),
            ("score", PropertyValue::from(7_i64)),
        ],
    )
    .await;

    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let set_id = exec::ExecStepId::new(2).expect("positive step id");
    let start = test_support::name("start");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_param_step(1, start.clone()),
            reserved_step(
                2,
                access_id.get(),
                ir::ReservedOp::SackSet(test_support::name("score")),
            ),
            reserved_step(3, set_id.get(), ir::ReservedOp::SackGet),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(start, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("sack set plan executes");

    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("sack_get should preserve stream shape");
    };
    assert_eq!(rows[0].sack.value(), Some(&DbPropertyValue::I64(7)));
}

#[tokio::test]
async fn missing_sack_properties_clear_on_set_and_are_ignored_on_add() {
    let db = test_support::open_db("reserved_sack_missing_property").await;
    let alice = test_support::add_node_with_properties(&db, "User", Vec::new()).await;

    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let with_sack_id = exec::ExecStepId::new(2).expect("positive step id");
    let set_id = exec::ExecStepId::new(3).expect("positive step id");
    let add_id = exec::ExecStepId::new(4).expect("positive step id");
    let start = test_support::name("start");
    let missing = test_support::name("missing");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_param_step(1, start.clone()),
            reserved_step(
                2,
                access_id.get(),
                ir::ReservedOp::WithSack(helix_ast::value::PropertyValue::I64(1)),
            ),
            reserved_step(
                3,
                with_sack_id.get(),
                ir::ReservedOp::SackSet(missing.clone()),
            ),
            reserved_step(4, set_id.get(), ir::ReservedOp::SackAdd(missing)),
            reserved_step(5, add_id.get(), ir::ReservedOp::SackGet),
        ],
        5,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(start, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("missing sack properties are handled");

    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("sack_get should preserve stream shape");
    };
    assert_eq!(rows[0].sack.value(), None);
}
