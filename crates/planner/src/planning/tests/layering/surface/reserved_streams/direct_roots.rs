use super::super::*;

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_stream_reserved() {
    let batch = read_batch()
        .var_as("paths", g().n(NodeRef::all()).path())
        .returning(["paths"]);

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
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "paths"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_reserved_stream_pipeline() {
    let batch = read_batch()
        .var_as("paths", g().n(NodeRef::all()).path().dedup())
        .returning(["paths"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 2);
    assert_eq!(plan.metrics().alternatives_considered, 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(&plan.steps()[2].op, crate::exec::ExecOp::Distinct));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "paths"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_reserved_variable_pipeline() {
    let batch = read_batch()
        .var_as("selected", g().n(NodeRef::all()).path().select("cached"))
        .returning(["selected"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 2);
    assert_eq!(plan.metrics().alternatives_considered, 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
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
fn single_run_executable_entrypoint_uses_cascades_selected_reserved_variable_write() {
    let batch = read_batch()
        .var_as("paths", g().n(NodeRef::all()).path().store("cached"))
        .returning(["paths"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 2);
    assert_eq!(plan.metrics().alternatives_considered, 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[2].schedule, crate::exec::ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "paths"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_reserved_stream_project() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("count".to_string()),
            root: AstNode::Count {
                input: boxed(AstNode::Path {
                    input: boxed(nodes_root()),
                }),
            },
            condition: None,
        }))],
        returns: vec!["count".to_string()],
    };

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 2);
    assert_eq!(plan.metrics().alternatives_considered, 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
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
fn single_run_executable_entrypoint_uses_cascades_selected_reserved_stream_aggregate() {
    let batch = read_batch()
        .var_as("groups", g().n(NodeRef::all()).path().group("kind"))
        .returning(["groups"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(plan.metrics().memo_groups >= 2);
    assert_eq!(plan.metrics().alternatives_considered, 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Path
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Aggregate {
            aggregate: AggregatePlan::Group(property),
        } if property.as_ref() == "kind"
    ));
    assert_eq!(plan.steps()[2].schedule, crate::exec::ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "groups"
    ));
}
