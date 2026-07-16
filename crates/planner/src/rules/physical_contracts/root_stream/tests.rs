use super::{access, contract, delivered};
use crate::{context, cost, ir, logical, physical, properties};
use helix_ast::expr::Predicate;
use helix_ast::traversal::Order;

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

fn variable_stream() -> logical::RootStream {
    logical::RootStream::VariableSource(logical::VariableSource::new(name("seed")))
}

fn node_access_stream() -> logical::RootStream {
    logical::RootStream::Access(logical::AccessStream::Path(node_access_path()))
}

fn node_access_path() -> logical::AccessPath {
    logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    ))
}

fn literal_limit(count: usize) -> logical::StreamPipelineOp {
    logical::StreamPipelineOp::Limit {
        count: ir::StreamBoundPlan::Literal(count),
    }
}

fn literal_range(start: usize, end: usize) -> logical::StreamPipelineOp {
    logical::StreamPipelineOp::Range {
        range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(start, end).unwrap()),
    }
}

fn predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap()
}

fn order_keys() -> ir::OrderKeys {
    ir::OrderKeys::from(ir::OrderKey {
        property: name("age"),
        order: Order::Asc,
    })
}

fn root_pipeline(
    input: logical::RootStream,
    first: logical::StreamPipelineOp,
    rest: Vec<logical::StreamPipelineOp>,
) -> logical::RootPipeline {
    logical::RootPipeline::new(input, ir::AtLeast::<_, 1>::from_one_and_rest(first, rest)).unwrap()
}

#[test]
fn root_stream_pipeline_family_keeps_prefix_contracts_explicit() {
    assert!(matches!(
        contract::RootStreamPipelineFamily::classify(&node_access_stream()),
        contract::RootStreamPipelineFamily::Access(_)
    ));
    assert!(matches!(
        contract::RootStreamPipelineFamily::classify(&variable_stream()),
        contract::RootStreamPipelineFamily::VariableSource
    ));

    let nested = logical::RootStream::Pipeline(Box::new(root_pipeline(
        variable_stream(),
        literal_limit(4),
        Vec::new(),
    )));
    assert!(matches!(
        contract::RootStreamPipelineFamily::classify(&nested),
        contract::RootStreamPipelineFamily::Localized(_)
    ));
}

#[test]
fn access_stream_pipeline_contract_adapts_every_access_stream_family() {
    let storage = cost::StorageCostProfile::default();
    let stats = context::StatsSnapshot::default();
    let access_path = node_access_path();

    let filter = access::access_stream_pipeline_contract(
        &logical::AccessStream::Filter(logical::AccessFilter::new(
            access_path.clone(),
            predicate(),
        )),
        &storage,
        &stats,
    );
    assert!(matches!(
        filter.ops.as_slice(),
        [
            physical::PhysicalPipelineOp::Access { .. },
            physical::PhysicalPipelineOp::ResidualFilter,
        ]
    ));
    assert_ne!(filter.cost, cost::CostVector::ZERO);

    let window = access::access_stream_pipeline_contract(
        &logical::AccessStream::Window(logical::AccessWindow::new(
            access_path.clone(),
            logical::AccessWindowRange::new(1, Some(3)).unwrap(),
        )),
        &storage,
        &stats,
    );
    assert!(matches!(
        window.ops.as_slice(),
        [
            physical::PhysicalPipelineOp::Access { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ]
    ));
    assert_eq!(
        window.delivered.cardinality,
        properties::CardinalityBounds::zero_to(Some(2))
    );

    let ordered = access::access_stream_pipeline_contract(
        &logical::AccessStream::Order(logical::AccessOrder::new(access_path.clone(), order_keys())),
        &storage,
        &stats,
    );
    assert!(matches!(
        ordered.ops.as_slice(),
        [
            physical::PhysicalPipelineOp::Access { .. },
            physical::PhysicalPipelineOp::Sort,
        ]
    ));
    assert!(matches!(
        ordered.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        ordered.delivered.materialization,
        properties::Materialization::Materialized
    );

    let distinct = access::access_stream_pipeline_contract(
        &logical::AccessStream::Distinct(logical::AccessDistinct::new(access_path.clone())),
        &storage,
        &stats,
    );
    assert!(matches!(
        distinct.ops.as_slice(),
        [
            physical::PhysicalPipelineOp::Access { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
        ]
    ));
    assert_eq!(
        distinct.delivered.materialization,
        properties::Materialization::Materialized
    );

    let pipeline = access::access_stream_pipeline_contract(
        &logical::AccessStream::Pipeline(
            logical::AccessPipeline::new(
                access_path,
                ir::AtLeast::<_, 1>::from_one_and_rest(
                    logical::StreamPipelineOp::Filter {
                        predicate: predicate(),
                    },
                    vec![logical::StreamPipelineOp::Order {
                        ordering: order_keys(),
                    }],
                ),
            )
            .unwrap(),
        ),
        &storage,
        &stats,
    );
    assert!(matches!(
        pipeline.ops.as_slice(),
        [
            physical::PhysicalPipelineOp::Access { .. },
            physical::PhysicalPipelineOp::ResidualFilter,
            physical::PhysicalPipelineOp::Sort,
        ]
    ));
    assert_ne!(pipeline.cost, cost::CostVector::ZERO);
}

#[test]
fn root_pipeline_contract_appends_outer_ops_to_inlinable_sources() {
    let storage = cost::StorageCostProfile::default();
    let stats = context::StatsSnapshot::default();
    let pipeline = root_pipeline(variable_stream(), literal_limit(4), Vec::new());

    let (physical, delivered, plan_cost) =
        contract::root_pipeline_physical_contract(&pipeline, &storage, &stats);

    assert_eq!(
        physical.ops(),
        &[
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
        ]
    );
    assert_eq!(delivered.cardinality.upper(), Some(4));
    assert_ne!(plan_cost, cost::CostVector::ZERO);
}

#[test]
fn localized_root_streams_contribute_delivered_properties_without_prefix_ops() {
    let storage = cost::StorageCostProfile::default();
    let stats = context::StatsSnapshot::default();
    let inner = root_pipeline(variable_stream(), literal_limit(5), Vec::new());
    let outer = root_pipeline(
        logical::RootStream::Pipeline(Box::new(inner)),
        literal_range(1, 3),
        Vec::new(),
    );

    let (physical, delivered, _) =
        contract::root_pipeline_physical_contract(&outer, &storage, &stats);

    assert_eq!(
        physical.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Range
        )]
    );
    assert_eq!(
        delivered.cardinality,
        properties::CardinalityBounds::zero_to(Some(2))
    );
}

#[test]
fn terminal_contracts_append_required_tails_and_preserve_output_shape() {
    let storage = cost::StorageCostProfile::default();
    let stats = context::StatsSnapshot::default();
    let project = logical::StreamProject::new(node_access_stream(), ir::ProjectionPlan::Count);

    let (physical, delivered, plan_cost) =
        contract::stream_project_pipeline_contract(&project, &storage, &stats);

    assert!(matches!(
        physical.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                ..
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
        ]
    ));
    assert_eq!(
        delivered.cardinality,
        properties::CardinalityBounds::exact(1)
    );
    assert_eq!(
        delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_ne!(plan_cost, cost::CostVector::ZERO);
}

#[test]
fn delivered_family_tracks_terminal_and_barrier_boundaries() {
    let pipeline_stream = logical::RootStream::Pipeline(Box::new(root_pipeline(
        variable_stream(),
        literal_limit(4),
        Vec::new(),
    )));
    assert!(matches!(
        delivered::RootStreamDeliveredFamily::classify(&pipeline_stream),
        delivered::RootStreamDeliveredFamily::Pipeline(_)
    ));

    let project_stream = logical::RootStream::Project(Box::new(logical::StreamProject::new(
        variable_stream(),
        ir::ProjectionPlan::Id,
    )));
    assert!(matches!(
        delivered::RootStreamDeliveredFamily::classify(&project_stream),
        delivered::RootStreamDeliveredFamily::Project(_)
    ));
}
