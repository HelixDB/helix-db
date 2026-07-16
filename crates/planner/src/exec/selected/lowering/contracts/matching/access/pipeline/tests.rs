use super::super::source;
use super::contracts::{
    SelectedAccessFilterPipelineMatch, SelectedAccessFilterPipelineMismatch,
    SelectedAccessPipelineMatch, SelectedAccessPipelineMismatch,
};
use super::filter::selected_access_filter_pipeline_access;
use super::prefix::selected_access_pipeline_parts;
use crate::{ir, logical, physical, properties};

fn node_empty_access_path() -> logical::AccessPath {
    logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap(),
    ))
}

fn pipeline(
    first: physical::PhysicalPipelineOp,
    rest: Vec<physical::PhysicalPipelineOp>,
) -> physical::PhysicalPipeline {
    physical::PhysicalPipeline::new(ir::AtLeast::from_one_and_rest(first, rest))
}

fn access_pipeline(
    element: properties::ElementKind,
    access: physical::PhysicalAccess,
    rest: Vec<physical::PhysicalPipelineOp>,
) -> physical::PhysicalPipeline {
    pipeline(
        physical::PhysicalPipelineOp::Access { element, access },
        rest,
    )
}

#[test]
fn access_pipeline_parts_reports_match_and_prefix_mismatches() {
    let access = node_empty_access_path();
    let matched_pipeline = access_pipeline(
        properties::ElementKind::Node,
        physical::PhysicalAccess::Empty,
        vec![physical::PhysicalPipelineOp::ResidualFilter],
    );
    let matched = selected_access_pipeline_parts(&access, &matched_pipeline);
    let SelectedAccessPipelineMatch::Matched(parts) = matched else {
        panic!("expected access pipeline match");
    };
    let (physical_access, ops) = parts.into_parts();
    assert!(matches!(physical_access, physical::PhysicalAccess::Empty));
    assert!(matches!(
        ops,
        [physical::PhysicalPipelineOp::ResidualFilter]
    ));

    assert!(matches!(
        selected_access_pipeline_parts(
            &access,
            &pipeline(physical::PhysicalPipelineOp::ResidualFilter, vec![]),
        ),
        SelectedAccessPipelineMatch::NotMatched(
            SelectedAccessPipelineMismatch::MissingAccessPrefix
        )
    ));

    assert!(matches!(
        selected_access_pipeline_parts(
            &access,
            &access_pipeline(
                properties::ElementKind::Edge,
                physical::PhysicalAccess::Empty,
                vec![],
            ),
        ),
        SelectedAccessPipelineMatch::NotMatched(SelectedAccessPipelineMismatch::ElementMismatch)
    ));

    assert!(matches!(
        selected_access_pipeline_parts(
            &access,
            &access_pipeline(
                properties::ElementKind::Node,
                physical::PhysicalAccess::LabelScan,
                vec![],
            ),
        ),
        SelectedAccessPipelineMatch::NotMatched(
            SelectedAccessPipelineMismatch::PhysicalAccessMismatch(
                source::SelectedAccessPathMismatch::Node(
                    source::SelectedAccessShapeMismatch::PhysicalAccessFamilyMismatch
                )
            )
        )
    ));
}

#[test]
fn access_filter_pipeline_access_reports_suffix_mismatch() {
    let access = node_empty_access_path();
    let filter = logical::AccessFilter::new(
        access.clone(),
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    );

    assert!(matches!(
        selected_access_filter_pipeline_access(
            &filter,
            &access_pipeline(
                properties::ElementKind::Node,
                physical::PhysicalAccess::Empty,
                vec![physical::PhysicalPipelineOp::ResidualFilter],
            ),
        ),
        SelectedAccessFilterPipelineMatch::Matched(physical::PhysicalAccess::Empty)
    ));

    assert!(matches!(
        selected_access_filter_pipeline_access(
            &filter,
            &access_pipeline(
                properties::ElementKind::Node,
                physical::PhysicalAccess::Empty,
                vec![physical::PhysicalPipelineOp::Sort],
            ),
        ),
        SelectedAccessFilterPipelineMatch::NotMatched(
            SelectedAccessFilterPipelineMismatch::PhysicalSuffixMismatch
        )
    ));
}
