use super::super::scalar::project_scalar_items;
use super::*;

#[test]
fn scalar_projection_contract_preserves_terminal_only_shapes() {
    assert_eq!(
        project_scalar_items(
            ExecutionValue::Scalars(vec![
                ExecutionScalar::NodeId(1),
                ExecutionScalar::Value(DbPropertyValue::I64(9)),
                ExecutionScalar::EdgeId(2),
            ]),
            &ir::ProjectionPlan::Id,
        )
        .unwrap(),
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(1), ExecutionScalar::EdgeId(2)])
    );
    assert_eq!(
        project_scalar_items(ExecutionValue::Bool(true), &ir::ProjectionPlan::Exists).unwrap(),
        ExecutionValue::Bool(true)
    );
    assert!(project_scalar_items(
        ExecutionValue::Count(1),
        &ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
    )
    .unwrap_err()
    .to_string()
    .contains("expected element stream input"));
}

#[test]
fn scalar_projection_exists_and_ids_are_terminal_item_aware() {
    let scalar_ids = ExecutionValue::Scalars(vec![
        ExecutionScalar::NodeId(1),
        ExecutionScalar::Value(DbPropertyValue::I64(9)),
        ExecutionScalar::EdgeId(2),
    ]);

    assert_eq!(
        project_scalar_items(scalar_ids.clone(), &ir::ProjectionPlan::Exists).unwrap(),
        ExecutionValue::Bool(true)
    );
    assert_eq!(
        project_scalar_items(
            ExecutionValue::Scalars(Vec::new()),
            &ir::ProjectionPlan::Exists
        )
        .unwrap(),
        ExecutionValue::Bool(false)
    );
    assert_eq!(
        project_scalar_items(ExecutionValue::Count(0), &ir::ProjectionPlan::Id).unwrap(),
        ExecutionValue::Scalars(Vec::new())
    );
}

#[tokio::test]
async fn project_dispatches_scalar_inputs_to_terminal_projection() {
    let db = test_support::open_db("projection-scalar-dispatch").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.project(
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(7)]),
            &ir::ProjectionPlan::Id,
        )
        .await
        .unwrap(),
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(7)])
    );
}

#[tokio::test]
async fn project_rejects_index_lifecycle_values() {
    let db = test_support::open_db("projection-index-lifecycle").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let lifecycle = ExecutionValue::IndexDdlReceipt(
        crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
            operation_id: crate::index_lifecycle::IndexOperationId::from_bytes([7; 16]).unwrap(),
        },
    );

    let error = ctx
        .project(lifecycle, &ir::ProjectionPlan::Id)
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("project cannot consume an index lifecycle value"));
}
