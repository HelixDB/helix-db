use super::support::*;

#[tokio::test]
async fn edge_output_expansion_honors_direction_and_labels() {
    let db = test_support::open_db("access-edge-output-directions").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let knows_id = test_support::add_edge(&db, alice, bob, "KNOWS").await;
    let follows_id = test_support::add_edge(&db, bob, alice, "FOLLOWS").await;
    let from_param = test_support::name("from");

    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Out,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(knows_id)])
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::In,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(follows_id)])
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Both,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(knows_id),
            ExecutionScalar::EdgeId(follows_id),
        ])
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Out,
            ir::ExpandLabelPlan::Label(test_support::name("FOLLOWS")),
        )
        .await,
        ExecutionValue::Scalars(Vec::new())
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::In,
            ir::ExpandLabelPlan::Label(test_support::name("FOLLOWS")),
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(follows_id)])
    );
}

#[tokio::test]
async fn edge_output_expansion_preserves_input_multiplicity() {
    let db = test_support::open_db("access-edge-output-multiplicity").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let knows_id = test_support::add_edge(&db, alice, bob, "KNOWS").await;
    let from_param = test_support::name("from");

    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64Array(vec![alice as i64, alice as i64]),
            ir::ExpandDirection::Out,
            ir::ExpandLabelPlan::Label(test_support::name("KNOWS")),
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(knows_id),
            ExecutionScalar::EdgeId(knows_id),
        ])
    );
}

#[tokio::test]
async fn edge_output_expansion_deduplicates_self_loop_for_both_direction() {
    let db = test_support::open_db("access-edge-output-self-loop").await;
    let alice = test_support::add_user(&db, "alice").await;
    let self_id = test_support::add_edge(&db, alice, alice, "SELF").await;
    let from_param = test_support::name("from");

    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Both,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(self_id)])
    );
}
