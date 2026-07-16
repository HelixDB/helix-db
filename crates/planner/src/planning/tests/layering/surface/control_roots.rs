use super::*;

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_branch_root() {
    let batch = read_batch()
        .var_as(
            "expanded",
            g().n(NodeRef::all()).optional(sub().out(Some("FOLLOWS"))),
        )
        .returning(["expanded"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 3);
    assert_eq!(plan.metrics().alternatives_considered, 3);
    let crate::exec::ExecOp::Branch {
        plan: crate::exec::ExecBranchPlan::Optional(body),
    } = &plan.steps()[1].op
    else {
        panic!("expected selected branch executable step");
    };
    assert_eq!(body.steps().len(), 2);
    assert!(matches!(
        &body.steps()[0].op,
        crate::exec::ExecOp::Variable { .. }
    ));
    assert!(matches!(
        &body.steps()[1].op,
        crate::exec::ExecOp::Expand { .. }
    ));
    assert_eq!(body.root(), body.steps()[1].id);
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "expanded"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_repeat_root() {
    let batch = read_batch()
        .var_as(
            "repeated",
            g().n(NodeRef::all())
                .repeat(RepeatConfig::new(sub().out(Some("FOLLOWS"))).times(2)),
        )
        .returning(["repeated"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 3);
    assert_eq!(plan.metrics().alternatives_considered, 3);
    let crate::exec::ExecOp::Repeat { plan: repeat } = &plan.steps()[1].op else {
        panic!("expected selected repeat executable step");
    };
    assert_eq!(repeat.max_depth.get(), 100);
    assert!(matches!(repeat.stop, RepeatStopPlan::Times { .. }));
    assert_eq!(repeat.body.steps().len(), 2);
    assert!(matches!(
        &repeat.body.steps()[0].op,
        crate::exec::ExecOp::Variable { .. }
    ));
    assert!(matches!(
        &repeat.body.steps()[1].op,
        crate::exec::ExecOp::Expand { .. }
    ));
    assert_eq!(repeat.body.root(), repeat.body.steps()[1].id);
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "repeated"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_control_root_pipeline() {
    let batch = read_batch()
        .var_as(
            "likes",
            g().n(NodeRef::all())
                .optional(sub().out(Some("FOLLOWS")))
                .out_e(Some("LIKES")),
        )
        .returning(["likes"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 4);
    assert_eq!(plan.metrics().alternatives_considered, 4);
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
        crate::exec::ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::Out,
                output: ExpandOutput::Edges,
                label: ExpandLabelPlan::Label(label),
            },
        } if label.as_ref() == "LIKES"
    ));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "likes"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_control_root_terminal() {
    let batch = read_batch()
        .var_as(
            "count",
            g().n(NodeRef::all())
                .optional(sub().out(Some("FOLLOWS")))
                .count(),
        )
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 4);
    assert_eq!(plan.metrics().alternatives_considered, 4);
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
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_control_pipeline_terminal() {
    let batch = read_batch()
        .var_as(
            "count",
            g().n(NodeRef::all())
                .optional(sub().out(Some("FOLLOWS")))
                .out_e(Some("LIKES"))
                .count(),
        )
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(plan.steps().len(), 4);
    assert!(plan.metrics().memo_groups >= 5);
    assert_eq!(plan.metrics().alternatives_considered, 5);
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
        crate::exec::ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::Out,
                output: ExpandOutput::Edges,
                label: ExpandLabelPlan::Label(label),
            },
        } if label.as_ref() == "LIKES"
    ));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[3].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
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
