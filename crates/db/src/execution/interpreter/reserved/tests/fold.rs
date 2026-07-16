use super::support::*;

#[tokio::test]
async fn fold_unfold_round_trips_materialized_stream_rows() {
    let db = test_support::open_db("reserved_fold_unfold_round_trip").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;

    let fold_id = exec::ExecStepId::new(2).expect("positive step id");
    let unfold_id = exec::ExecStepId::new(3).expect("positive step id");
    let mut fold = reserved_step(2, 1, ir::ReservedOp::Fold);
    fold.schedule = exec::ExecSchedule::Barrier;
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            all_nodes_step(1),
            fold,
            reserved_step(3, fold_id.get(), ir::ReservedOp::Unfold),
            project_id_step(4, unfold_id.get()),
        ],
        4,
    );

    let result = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect("reserved fold/unfold plan executes");

    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("project id should return scalar IDs");
    };
    let ids = values
        .into_iter()
        .map(|value| match value {
            ExecutionScalar::NodeId(id) => id,
            other @ (ExecutionScalar::EdgeId(_)
            | ExecutionScalar::String(_)
            | ExecutionScalar::Value(_)
            | ExecutionScalar::Object(_)) => panic!("expected node ID, got {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(ids, BTreeSet::from([alice, bob]));
}

#[tokio::test]
async fn fold_root_exposes_folded_stream_contract() {
    let db = test_support::open_db("reserved_fold_contract").await;
    test_support::add_user(&db, "alice").await;
    test_support::add_user(&db, "bob").await;

    let mut fold = reserved_step(2, 1, ir::ReservedOp::Fold);
    fold.schedule = exec::ExecSchedule::Barrier;
    let plan = test_support::executable(ir::PlanKind::Read, vec![all_nodes_step(1), fold], 2);

    let result = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect("reserved fold plan executes");

    let Some(ExecutionValue::FoldedStream(folded)) = result.last else {
        panic!("fold should return folded stream contract");
    };
    assert_eq!(folded.len(), 1);
    assert_eq!(folded.rows().len(), 2);
}

#[tokio::test]
async fn non_materializing_reserved_ops_pass_stream_through() {
    let db = test_support::open_db("reserved_pass_through").await;
    test_support::add_user(&db, "alice").await;
    test_support::add_user(&db, "bob").await;

    let pass_through_ops = [ir::ReservedOp::Unfold];

    for (index, op) in pass_through_ops.into_iter().enumerate() {
        let reserved_id = exec::ExecStepId::new(2).expect("positive step id");
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![
                all_nodes_step(1),
                reserved_step(2, 1, op),
                project_count_step(3, reserved_id.get()),
            ],
            3,
        );

        let result = db
            .execute(&plan, context::ParamBindings::default())
            .await
            .unwrap_or_else(|err| panic!("reserved pass-through op {index} failed: {err}"));
        assert_eq!(result.last, Some(ExecutionValue::Count(2)));
    }
}

#[tokio::test]
async fn unfold_rejects_scalar_input() {
    let db = test_support::open_db("reserved_unfold_scalar_rejected").await;
    test_support::add_user(&db, "alice").await;

    let project_id = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            all_nodes_step(1),
            project_count_step(2, 1),
            reserved_step(3, project_id.get(), ir::ReservedOp::Unfold),
        ],
        3,
    );

    let err = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect_err("unfold rejects scalar input");
    assert!(
        err.to_string().contains("unfold expected stream"),
        "unexpected error: {err}"
    );
}
