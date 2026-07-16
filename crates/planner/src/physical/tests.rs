use super::*;
use crate::{cost, digest, ir, properties};

#[test]
fn physical_alternative_preserves_delivered_properties_and_cost() {
    let alternative = PhysicalAlternative::new(
        PhysicalExpr::Sort,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    assert_eq!(alternative.cost, cost::CostVector::ZERO);
    assert_eq!(
        alternative.digest,
        digest::PlanDigest::for_tagged_value(
            "physical_alternative:v1",
            &(
                PhysicalExpr::Sort,
                properties::DeliveredProperties::default()
            )
        )
    );
    assert_eq!(
        alternative.delivered,
        properties::DeliveredProperties::default()
    );
}

#[test]
fn physical_pipeline_preserves_non_empty_operator_contract() {
    let pipeline = PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
        PhysicalPipelineOp::ResidualFilter,
    ));

    assert_eq!(pipeline.ops(), &[PhysicalPipelineOp::ResidualFilter]);
}

#[test]
fn physical_pipeline_terminal_split_preserves_prefix_and_suffix() {
    let single = PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
        PhysicalPipelineOp::ResidualFilter,
    ));
    let single_split = single.terminal_split();
    assert!(single_split.prefix().is_empty());
    assert_eq!(single_split.terminal(), &PhysicalPipelineOp::ResidualFilter);

    let pipeline = PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
        PhysicalPipelineOp::ResidualFilter,
        vec![PhysicalPipelineOp::Stream(PhysicalStreamOp::Project)],
    ));
    let split = pipeline.terminal_split();

    assert_eq!(split.prefix(), &[PhysicalPipelineOp::ResidualFilter]);
    assert_eq!(
        split.terminal(),
        &PhysicalPipelineOp::Stream(PhysicalStreamOp::Project)
    );
}

#[test]
fn physical_alternative_digest_is_stable_and_ignores_cost_profile_changes() {
    let first = PhysicalAlternative::new(
        PhysicalExpr::Barrier,
        properties::DeliveredProperties::default(),
        cost::CostVector {
            latency: cost::LatencyEstimate::micros(10),
            ..cost::CostVector::ZERO
        },
    );
    let second = PhysicalAlternative::new(
        PhysicalExpr::Barrier,
        properties::DeliveredProperties::default(),
        cost::CostVector {
            latency: cost::LatencyEstimate::micros(50),
            ..cost::CostVector::ZERO
        },
    );
    let different = PhysicalAlternative::new(
        PhysicalExpr::ResidualFilter,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    assert_eq!(first.digest, second.digest);
    assert_ne!(first.digest, different.digest);
}
