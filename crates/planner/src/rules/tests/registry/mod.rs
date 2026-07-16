use super::*;

mod access_filter;
mod access_pipeline;
mod access_sets;
mod pipeline;
mod root_control;
mod smoke;

pub(super) fn optimize(
    optimizer: &crate::optimizer::CascadesOptimizer<'_>,
    expr: logical::LogicalExpr,
    config: &crate::optimizer::OptimizerConfig,
) -> crate::optimizer::OptimizationResult {
    optimizer
        .optimize(expr, config)
        .expect("test optimizer memo allocation should fit")
}
