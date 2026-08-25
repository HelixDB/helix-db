use super::super::super::rejection;
use super::super::terminal::TerminalRootPayload;
use super::*;
use crate::{cost, ir, logical, physical, properties};

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

fn variable_stream() -> logical::RootStream {
    logical::RootStream::VariableSource(logical::VariableSource::new(name("seed")))
}

fn pipeline_expr() -> physical::PhysicalExpr {
    physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
        ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Project,
        )),
    ))
}

fn access_expr() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    )))
}

#[test]
fn classifies_selected_terminal_case() {
    let source = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        variable_stream(),
        ir::ProjectionPlan::Exists,
    ));

    assert!(matches!(
        SelectedRootPlanCase::classify(&source, &pipeline_expr()).unwrap(),
        SelectedRootPlanCase::Terminal(TerminalRootPayload::Project(_))
    ));
}

#[test]
fn classifies_generic_alternative_case() {
    let physical = physical::PhysicalExpr::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::LabelScan,
    };

    match SelectedRootPlanCase::classify(&access_expr(), &physical).unwrap() {
        SelectedRootPlanCase::GenericAlternative(family) => assert_eq!(
            family,
            exec::SelectedExecutableAlternativeFamily::NODE_ACCESS_PATH
        ),
        _ => panic!("access path should classify as a generic alternative"),
    }
}

#[test]
fn rejects_selected_root_physical_mismatch_before_generic_support() {
    let ddl =
        logical::LogicalExpr::RootIndexDdl(logical::RootIndexDdl::new(ir::IndexDdlPlan::Drop {
            spec: ir::IndexDdlDropSpec::NodeEquality {
                key: crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: crate::catalog::IndexUniqueness::NonUnique,
            },
        }));

    match SelectedRootPlanCase::classify(&ddl, &pipeline_expr()) {
        Ok(_) => panic!("mismatched selected root must be rejected"),
        Err(error) => assert_eq!(
            error,
            rejection::unsupported(rejection::Reason::SelectedRootPhysicalMismatch)
        ),
    }
}

#[test]
fn rejects_unsupported_generic_alternative() {
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Empty);
    let physical = physical::PhysicalExpr::Empty;

    match SelectedRootPlanCase::classify(&source, &physical) {
        Ok(_) => panic!("unsupported generic alternative must be rejected"),
        Err(error) => assert_eq!(
            error,
            rejection::unsupported(rejection::Reason::SelectedAlternativeUnsupported)
        ),
    }
}

#[test]
fn classification_does_not_need_costed_physical_alternative() {
    let selected = physical::PhysicalAlternative::new(
        pipeline_expr(),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );
    let source = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        variable_stream(),
        ir::ProjectionPlan::Exists,
    ));

    assert!(matches!(
        SelectedRootPlanCase::classify(&source, &selected.expr).unwrap(),
        SelectedRootPlanCase::Terminal(TerminalRootPayload::Project(_))
    ));
}

#[test]
fn count_classification_carries_the_validated_physical_payload() {
    let source =
        logical::LogicalExpr::StreamCardinality(logical::StreamCardinality::new(variable_stream()));
    let plan = exec::ExecCountPlan::Constant(3);
    let physical = physical::PhysicalExpr::Cardinality(Box::new(physical::PhysicalCountPlan::new(
        plan.clone(),
    )));

    let SelectedRootPlanCase::Count(_, count) =
        SelectedRootPlanCase::classify(&source, &physical).unwrap()
    else {
        panic!("cardinality pair must retain its typed physical payload")
    };
    assert_eq!(count.executable(), &plan);
}
