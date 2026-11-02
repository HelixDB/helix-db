use super::*;

#[test]
fn scalability_fixture_rejects_zero_scale() {
    assert!(
        PlanScalabilityFixture::new(PlanningScalabilityShape::WideBooleanPredicates, 0).is_none()
    );
}

#[test]
fn batched_root_reuse_keeps_memo_work_constant() {
    let fixture =
        PlanScalabilityFixture::new(PlanningScalabilityShape::BatchedRootReuse, 128).unwrap();
    let plan = fixture.case().plan_checked().unwrap();

    assert_eq!(plan.steps().len(), 128);
    assert!(plan.metrics().memo_groups <= 16);
    assert!(plan.metrics().memo_exprs <= 32);
    assert!(plan.metrics().alternatives_considered <= 16);
}

#[test]
fn foreach_body_root_reuse_keeps_recursive_memo_work_constant() {
    let fixture =
        PlanScalabilityFixture::new(PlanningScalabilityShape::ForEachBodyRootReuse, 128).unwrap();
    let plan = fixture.case().plan_checked().unwrap();

    assert_eq!(plan.steps().len(), 1);
    match &plan.steps()[0].op {
        crate::exec::ExecOp::ForEach { body, .. } => {
            assert_eq!(body.steps().len(), 128);
        }
        other => panic!("expected foreach executable step, got {other:?}"),
    }
    assert!(plan.metrics().memo_groups <= 16);
    assert!(plan.metrics().memo_exprs <= 32);
    assert!(plan.metrics().alternatives_considered <= 16);
}
