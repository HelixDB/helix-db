use super::*;
use crate::ir;

#[test]
fn scalability_fixture_rejects_zero_scale() {
    assert!(
        PlanScalabilityFixture::new(PlanningScalabilityShape::WideBooleanPredicates, 0).is_none()
    );
}

#[test]
fn scalability_case_exposes_fixture_context_batch_and_thresholds() {
    let fixture =
        PlanScalabilityFixture::new(PlanningScalabilityShape::DeepTraversalChain, 4).unwrap();
    let case = fixture.case();

    assert_eq!(case.fixture(), fixture);
    assert_eq!(case.read_batch().unwrap().entries.len(), 1);
    assert!(case.write_batch().is_none());
    assert!(case
        .context()
        .stats
        .node_label_cardinality
        .contains_key(&ir::NonEmptyString::new("User").unwrap()));
    assert!(case.thresholds().max_rule_fires().get() > 0);
}

#[test]
fn write_scalability_case_exposes_write_workload() {
    let fixture =
        PlanScalabilityFixture::new(PlanningScalabilityShape::MutationHeavyBatches, 4).unwrap();
    let case = fixture.case();

    assert!(case.read_batch().is_none());
    assert_eq!(case.write_batch().unwrap().entries.len(), 16);
    assert!(case.thresholds().max_rule_fires().get() > 0);
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
