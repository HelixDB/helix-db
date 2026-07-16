use super::*;

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_variable_source() {
    let batch = read_batch()
        .var_as("cached", g().inject("users"))
        .returning(["cached"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 1);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "users"
    ));
    assert!(matches!(
        &plan.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "cached"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_variable_source_project() {
    let batch = read_batch()
        .var_as("count", g().inject("users").count())
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "users"
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_variable_source_pipeline() {
    let batch = read_batch()
        .var_as("selected", g().inject("users").select("cached").dedup())
        .returning(["selected"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "users"
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert!(matches!(&plan.steps()[2].op, crate::exec::ExecOp::Distinct));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "selected"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_variable_source_pipeline_project() {
    let batch = read_batch()
        .var_as("count", g().inject("users").select("cached").count())
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 2);
    assert_eq!(plan.metrics().alternatives_considered, 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "users"
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count
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
fn single_run_executable_entrypoint_uses_cascades_selected_access_variable_stream() {
    let batch = read_batch()
        .var_as("selected", g().n(NodeRef::all()).select("cached"))
        .returning(["selected"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "selected"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_stream_variable_write() {
    let batch = read_batch()
        .var_as("selected", g().n(NodeRef::all()).store("cached"))
        .returning(["selected"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[1].schedule, crate::exec::ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "selected"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_stateful_access_pipeline() {
    let batch = read_batch()
        .var_as(
            "likes",
            g().n(NodeRef::all()).store("cached").out_e(Some("LIKES")),
        )
        .returning(["likes"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[1].schedule, crate::exec::ExecSchedule::Barrier);
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
    assert_eq!(
        plan.steps()[2].delivered.effect,
        crate::properties::EffectKind::Barrier
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "likes"
    ));
}
