use super::physical_pipeline_contract;
use crate::{cost, ir, logical, physical, properties};

fn predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap()
}

#[test]
fn physical_pipeline_contract_covers_all_pure_pipeline_shapes() {
    let storage = cost::StorageCostProfile::default();
    let ops = ir::AtLeast::<_, 1>::from_one_and_rest(
        logical::PureLogicalOp::NoOp,
        vec![
            logical::PureLogicalOp::Empty,
            logical::PureLogicalOp::Source {
                element: properties::ElementKind::Node,
            },
            logical::PureLogicalOp::Filter {
                predicate: predicate(),
            },
            logical::PureLogicalOp::Order {
                ordering: properties::RequiredOrdering::Any,
            },
            logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Literal(4),
            },
            logical::PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Literal(1),
            },
            logical::PureLogicalOp::Range {
                range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 3).unwrap()),
            },
            logical::PureLogicalOp::Distinct,
            logical::PureLogicalOp::Expand {
                element: properties::ElementKind::Edge,
            },
            logical::PureLogicalOp::Project,
            logical::PureLogicalOp::Aggregate,
            logical::PureLogicalOp::Variable,
            logical::PureLogicalOp::Reserved,
        ],
    );

    let (pipeline, delivered, plan_cost) = physical_pipeline_contract(&ops, &storage);

    assert_eq!(pipeline.ops().len(), ops.as_ref().len());
    assert!(matches!(
        pipeline.ops()[0],
        physical::PhysicalPipelineOp::NoOp
    ));
    assert!(matches!(
        pipeline.ops()[1],
        physical::PhysicalPipelineOp::Empty
    ));
    assert!(matches!(
        pipeline.ops()[2],
        physical::PhysicalPipelineOp::Access {
            element: properties::ElementKind::Node,
            access: physical::PhysicalAccess::Kv(_)
        }
    ));
    assert!(matches!(
        pipeline.ops()[3],
        physical::PhysicalPipelineOp::ResidualFilter
    ));
    assert!(matches!(
        pipeline.ops()[4],
        physical::PhysicalPipelineOp::Sort
    ));
    assert!(matches!(
        &pipeline.ops()[5..],
        [
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Expand),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Reserved),
        ]
    ));
    assert_eq!(delivered.element, None);
    assert_eq!(
        delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_ne!(plan_cost, cost::CostVector::ZERO);
}
