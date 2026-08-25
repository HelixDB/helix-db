use super::*;
use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::test_selected_root_provenance;
use crate::exec::selected::SelectedRootConstructionError;
use crate::{cost, ir, logical, physical, properties};

fn selected_physical(expr: physical::PhysicalExpr) -> SelectedPhysicalPlan {
    SelectedPhysicalPlan::new(
        expr,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}

fn child_pipeline_root() -> SelectedRootPipeline {
    SelectedRootPipeline::new(
        selected_physical(physical::PhysicalExpr::Pipeline(
            physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
            )),
        )),
        test_selected_root_provenance(),
        SelectedRootStreamInput::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::from_static("seed"),
        )),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
    )
    .unwrap()
}

#[test]
fn root_pipeline_constructor_preserves_contract_parts() {
    let alternative = selected_physical(physical::PhysicalExpr::Pipeline(
        physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
        )),
    ));
    let provenance = test_selected_root_provenance();
    let input = SelectedRootStreamInput::VariableSource(logical::VariableSource::new(
        ir::NonEmptyString::from_static("seed"),
    ));
    let ops = ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct);

    let root = SelectedRootPipeline::new(
        alternative.clone(),
        provenance.clone(),
        input.clone(),
        ops.clone(),
    )
    .unwrap();

    assert_eq!(root.alternative(), &alternative);
    assert_eq!(root.provenance(), &provenance);
    assert_eq!(root.input(), &input);
    assert!(root.input_prefix().as_slice().is_empty());
    assert_eq!(root.ops(), &ops);
    let input_prefix = root.input_prefix().clone();
    assert_eq!(
        root.into_parts(),
        (alternative, provenance, input, input_prefix, ops)
    );
}

#[test]
fn root_terminal_constructor_preserves_contract_parts() {
    let alternative = selected_physical(physical::PhysicalExpr::Pipeline(
        physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Reserved),
        )),
    ));
    let provenance = test_selected_root_provenance();
    let plan = SelectedRootTerminal::Reserved {
        input: SelectedRootStreamInput::Access(logical::AccessStream::Path(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
            )),
        )),
        op: ir::ReservedOp::Fold,
    };

    let root = SelectedRootTerminalPlan::new(alternative.clone(), provenance.clone(), plan.clone())
        .unwrap();

    assert_eq!(root.alternative(), &alternative);
    assert_eq!(root.provenance(), &provenance);
    assert!(root.input_prefix().as_slice().is_empty());
    assert_eq!(root.plan(), &plan);
    let input_prefix = root.input_prefix().clone();
    assert_eq!(
        root.into_parts(),
        (alternative, provenance, input_prefix, plan)
    );
}

#[test]
fn root_pipeline_constructor_preserves_parent_local_prefix() {
    let prefix = physical::PhysicalPipelineOp::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::LabelScan,
    };
    let alternative = selected_physical(physical::PhysicalExpr::Pipeline(
        physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
            prefix.clone(),
            vec![physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Distinct,
            )],
        )),
    ));
    let root = SelectedRootPipeline::new(
        alternative,
        test_selected_root_provenance(),
        SelectedRootStreamInput::Access(logical::AccessStream::Path(logical::AccessPath::Node(
            logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
            ),
        ))),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
    )
    .unwrap();

    assert_eq!(root.input_prefix().as_slice(), &[prefix]);
}

#[test]
fn root_terminal_constructor_preserves_parent_local_prefix() {
    let prefix = physical::PhysicalPipelineOp::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::LabelScan,
    };
    let alternative = selected_physical(physical::PhysicalExpr::Pipeline(
        physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
            prefix.clone(),
            vec![physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Project,
            )],
        )),
    ));
    let root = SelectedRootTerminalPlan::new(
        alternative,
        test_selected_root_provenance(),
        SelectedRootTerminal::Project {
            input: SelectedRootStreamInput::Access(logical::AccessStream::Path(
                logical::AccessPath::Node(logical::NodeAccessPath::new(
                    ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
                )),
            )),
            projection: ir::ProjectionPlan::Exists,
        },
    )
    .unwrap();

    assert_eq!(root.input_prefix().as_slice(), &[prefix]);
}

#[test]
fn root_pipeline_constructor_rejects_invalid_physical_suffix() {
    let provenance = test_selected_root_provenance();
    let input = SelectedRootStreamInput::VariableSource(logical::VariableSource::new(
        ir::NonEmptyString::from_static("seed"),
    ));

    assert_eq!(
        SelectedRootPipeline::new(
            selected_physical(physical::PhysicalExpr::NoOp),
            provenance.clone(),
            input.clone(),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        ),
        Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
    );

    assert_eq!(
        SelectedRootPipeline::new(
            selected_physical(physical::PhysicalExpr::Pipeline(
                physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
                )),
            )),
            provenance.clone(),
            input.clone(),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Distinct,
                vec![logical::StreamPipelineOp::Distinct],
            ),
        ),
        Err(SelectedRootConstructionError::RootPipelineLogicalSuffixTooLong)
    );

    assert_eq!(
        SelectedRootPipeline::new(
            selected_physical(physical::PhysicalExpr::Pipeline(
                physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
                )),
            )),
            provenance,
            input,
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        ),
        Err(SelectedRootConstructionError::RootPipelinePhysicalSuffixMismatch)
    );
}

#[test]
fn root_terminal_constructor_rejects_invalid_physical_suffix() {
    let plan = SelectedRootTerminal::Project {
        input: SelectedRootStreamInput::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::from_static("seed"),
        )),
        projection: ir::ProjectionPlan::Exists,
    };

    assert_eq!(
        SelectedRootTerminalPlan::new(
            selected_physical(physical::PhysicalExpr::NoOp),
            test_selected_root_provenance(),
            plan.clone(),
        ),
        Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
    );
    assert_eq!(
        SelectedRootTerminalPlan::new(
            selected_physical(physical::PhysicalExpr::Pipeline(
                physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate,),
                )),
            )),
            test_selected_root_provenance(),
            plan,
        ),
        Err(SelectedRootConstructionError::RootTerminalPhysicalSuffixMismatch)
    );
}

#[test]
fn root_pipeline_constructor_rejects_recursive_input_with_parent_prefix() {
    let child = child_pipeline_root();

    assert_eq!(
        SelectedRootPipeline::new(
            selected_physical(physical::PhysicalExpr::Pipeline(
                physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                    vec![physical::PhysicalPipelineOp::Stream(
                        physical::PhysicalStreamOp::Distinct,
                    )],
                )),
            )),
            test_selected_root_provenance(),
            SelectedRootStreamInput::Pipeline(Box::new(child)),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        ),
        Err(SelectedRootConstructionError::RecursiveRootStreamInputNonLocalizedPrefix)
    );
}

#[test]
fn root_terminal_constructor_rejects_recursive_input_with_parent_prefix() {
    let child = child_pipeline_root();

    assert_eq!(
        SelectedRootTerminalPlan::new(
            selected_physical(physical::PhysicalExpr::Pipeline(
                physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                    vec![physical::PhysicalPipelineOp::Stream(
                        physical::PhysicalStreamOp::Project,
                    )],
                )),
            )),
            test_selected_root_provenance(),
            SelectedRootTerminal::Project {
                input: SelectedRootStreamInput::Pipeline(Box::new(child)),
                projection: ir::ProjectionPlan::Exists,
            },
        ),
        Err(SelectedRootConstructionError::RecursiveRootStreamInputNonLocalizedPrefix)
    );
}
