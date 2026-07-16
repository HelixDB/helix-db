use super::support::*;

#[tokio::test]
async fn path_marks_tracked_expansion_path_visible() {
    let db = test_support::open_db("reserved_path_tracks_expansion").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    test_support::add_edge(&db, alice, bob, "FOLLOWS").await;

    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let expand_id = exec::ExecStepId::new(2).expect("positive step id");
    let start = test_support::name("start");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_param_step(1, start.clone()),
            expand_out_step(2, access_id.get()),
            reserved_step(3, expand_id.get(), ir::ReservedOp::Path),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(start, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("path plan executes");

    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("path should return a stream");
    };
    assert_eq!(rows.len(), 1);
    assert!(rows[0].path_visible);
    assert_eq!(rows[0].current, Some(ElementRef::Node(bob)));
    assert_eq!(
        rows[0].path.elements(),
        &[ElementRef::Node(alice), ElementRef::Node(bob)]
    );
}

#[tokio::test]
async fn simple_path_filters_repeated_path_elements() {
    let db = test_support::open_db("reserved_simple_path_filters_cycles").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    test_support::add_edge(&db, alice, bob, "FOLLOWS").await;
    test_support::add_edge(&db, bob, alice, "FOLLOWS").await;

    let first_expand_id = exec::ExecStepId::new(2).expect("positive step id");
    let second_expand_id = exec::ExecStepId::new(3).expect("positive step id");
    let simple_id = exec::ExecStepId::new(4).expect("positive step id");
    let start = test_support::name("start");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_param_step(1, start.clone()),
            expand_out_step(2, 1),
            expand_out_step(3, first_expand_id.get()),
            reserved_step(4, second_expand_id.get(), ir::ReservedOp::SimplePath),
            project_count_step(5, simple_id.get()),
        ],
        5,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(start, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("simple path plan executes");

    assert_eq!(result.last, Some(ExecutionValue::Count(0)));
}
