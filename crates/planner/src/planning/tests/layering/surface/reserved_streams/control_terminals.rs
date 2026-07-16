use super::super::*;

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_control_reserved_stream_project() {
    let batch = read_batch()
        .var_as(
            "count",
            g().n(NodeRef::all())
                .optional(sub().out(Some("FOLLOWS")))
                .path()
                .count(),
        )
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 4);
    assert!(plan.metrics().memo_groups >= 4);
    assert!(plan.metrics().alternatives_considered >= 4);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(_)
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Branch {
            plan: crate::exec::ExecBranchPlan::Optional(body)
        } if body.steps().len() == 2
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[3].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count
        }
    ));
    assert_eq!(
        plan.steps()[3].dependencies,
        vec![crate::exec::ExecStepId::new(3).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[3].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_control_reserved_stream_aggregate() {
    let batch = read_batch()
        .var_as(
            "groups",
            g().n(NodeRef::all())
                .optional(sub().out(Some("FOLLOWS")))
                .path()
                .group("kind"),
        )
        .returning(["groups"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 4);
    assert!(plan.metrics().memo_groups >= 4);
    assert!(plan.metrics().alternatives_considered >= 4);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(_)
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Branch {
            plan: crate::exec::ExecBranchPlan::Optional(body)
        } if body.steps().len() == 2
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[3].op,
        crate::exec::ExecOp::Aggregate {
            aggregate: AggregatePlan::Group(property),
        } if property.as_ref() == "kind"
    ));
    assert_eq!(plan.steps()[3].schedule, crate::exec::ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[3].dependencies,
        vec![crate::exec::ExecStepId::new(3).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[3].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "groups"
    ));
}
