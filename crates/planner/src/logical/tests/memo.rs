use super::*;

#[test]
fn logical_expr_memo_children_skip_parent_local_access_and_root_stream_inputs() {
    let access = node_access_path(ir::NodeAccessPlan::AllScan);
    let access_pipeline = LogicalExpr::AccessPipeline(
        AccessPipeline::new(
            access.clone(),
            ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::Distinct),
        )
        .unwrap(),
    );
    assert!(access_pipeline.memo_children().is_empty());

    let input = RootStream::VariableSource(VariableSource::new(name("seed")));
    let root_pipeline = RootPipeline::new(
        input.clone(),
        ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        }),
    )
    .unwrap();
    assert!(LogicalExpr::RootPipeline(root_pipeline.clone())
        .memo_children()
        .is_empty());
    assert_eq!(
        LogicalExpr::StreamProject(StreamProject::new(
            RootStream::Pipeline(Box::new(root_pipeline.clone())),
            ir::ProjectionPlan::Exists,
        ))
        .memo_children(),
        vec![LogicalExpr::RootPipeline(root_pipeline)]
    );
    assert!(
        LogicalExpr::VariableSource(VariableSource::new(name("seed")))
            .memo_children()
            .is_empty()
    );
}

#[test]
fn logical_expr_memo_children_preserve_nested_terminal_lineage() {
    let seed = RootStream::VariableSource(VariableSource::new(name("seed")));
    let project = StreamProject::new(seed, ir::ProjectionPlan::Exists);
    let write = StreamVariableWrite::new(
        RootStream::Project(Box::new(project.clone())),
        StreamVariableWriteOp::Store(name("counted")),
    );
    let aggregate = StreamAggregate::new(
        RootStream::VariableWrite(Box::new(write.clone())),
        ir::AggregatePlan::Group(name("kind")),
    );

    assert_eq!(
        LogicalExpr::StreamAggregate(aggregate).memo_children(),
        vec![LogicalExpr::StreamVariableWrite(write.clone())]
    );
    assert_eq!(
        LogicalExpr::StreamVariableWrite(write).memo_children(),
        vec![LogicalExpr::StreamProject(project.clone())]
    );
    assert!(LogicalExpr::StreamProject(project)
        .memo_children()
        .is_empty());
}

#[test]
fn logical_expr_memo_children_cover_control_and_mutation_payload_inputs() {
    let node_child = LogicalExpr::AccessPath(node_access_path(ir::NodeAccessPlan::AllScan));
    let edge_child = LogicalExpr::AccessPath(edge_access_path(ir::EdgeAccessPlan::AllScan));

    let branch = LogicalExpr::RootBranch(RootBranch::new(
        node_root(),
        ir::BranchPlan::ChooseElse {
            condition: predicate(),
            then_plan: Box::new(node_root()),
            else_plan: Box::new(edge_root()),
        },
    ));
    assert_eq!(
        branch.memo_children(),
        vec![node_child.clone(), node_child.clone(), edge_child.clone()]
    );

    let repeat = LogicalExpr::RootRepeat(RootRepeat::new(
        node_root(),
        ir::RepeatPlan {
            body: Box::new(edge_root()),
            stop: ir::RepeatStopPlan::MaxDepthOnly,
            emit: ir::RepeatEmitPlan::None,
            max_depth: NonZeroUsize::new(2).unwrap(),
        },
    ));
    assert_eq!(repeat.memo_children(), vec![node_child.clone(), edge_child]);

    let mutation = LogicalExpr::RootMutation(RootMutation::new(ir::MutationPlan::SetProperty {
        input: Box::new(node_root()),
        name: name("active"),
        value: ir::PropertyInputPlan::Value(helix_ast::value::PropertyValue::Bool(true)),
    }));
    assert_eq!(mutation.memo_children(), vec![node_child]);

    let source_mutation = LogicalExpr::RootMutation(RootMutation::new(ir::MutationPlan::AddNode {
        input: ir::MutationInput::Source,
        label: name("User"),
        properties: ir::PropertyAssignments::default(),
    }));
    assert!(source_mutation.memo_children().is_empty());

    let input_mutation = LogicalExpr::RootMutation(RootMutation::new(ir::MutationPlan::AddNode {
        input: ir::MutationInput::FromInput {
            input: Box::new(node_root()),
        },
        label: name("User"),
        properties: ir::PropertyAssignments::default(),
    }));
    assert_eq!(
        input_mutation.memo_children(),
        vec![LogicalExpr::AccessPath(node_access_path(
            ir::NodeAccessPlan::AllScan
        ))]
    );
}
