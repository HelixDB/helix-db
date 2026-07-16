use crate::{catalog, context, cost, ir, logical, optimizer, physical, properties, rules};

#[derive(Clone)]
pub(super) struct StaticRule {
    pub(super) metadata: rules::RuleMetadata,
    result: optimizer::RuleResult,
}

impl StaticRule {
    pub(super) fn new(id: &str, kind: rules::RuleKind, result: optimizer::RuleResult) -> Self {
        Self {
            metadata: rules::RuleMetadata::new(rules::RuleId::new(id).unwrap(), kind),
            result,
        }
    }
}

impl optimizer::OptimizerRule for StaticRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, _input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        self.result.clone()
    }
}

pub(super) fn optimizer(
    rules: Vec<&dyn optimizer::OptimizerRule>,
) -> optimizer::CascadesOptimizer<'_> {
    optimizer::CascadesOptimizer::try_from_rules(rules)
        .expect("test rule registries must be non-empty with unique IDs")
}

pub(super) fn optimize(
    optimizer: &optimizer::CascadesOptimizer<'_>,
    root: logical::LogicalExpr,
    config: &optimizer::OptimizerConfig,
) -> optimizer::OptimizationResult {
    optimizer
        .optimize(root, config)
        .expect("test optimizer memo allocation should fit")
}

pub(super) fn optimize_many(
    optimizer: &optimizer::CascadesOptimizer<'_>,
    roots: ir::AtLeast<logical::LogicalExpr, 1>,
    config: &optimizer::OptimizerConfig,
) -> optimizer::OptimizationResult {
    optimizer
        .optimize_many(roots, config)
        .expect("test optimizer memo allocation should fit")
}

pub(super) struct RewriteNestedPipelineChildRule;

impl optimizer::OptimizerRule for RewriteNestedPipelineChildRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        static METADATA: std::sync::OnceLock<rules::RuleMetadata> = std::sync::OnceLock::new();
        METADATA.get_or_init(|| {
            rules::RuleMetadata::new(
                rules::RuleId::new("rewrite_nested_pipeline_child").unwrap(),
                rules::RuleKind::Exploration,
            )
        })
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let logical::RootStream::Pipeline(inner) = pipeline.input() else {
            return optimizer::RuleResult::NotApplicable;
        };
        if single_limit_count(inner) != Some(1) {
            return optimizer::RuleResult::NotApplicable;
        }
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(nested_variable_root_pipeline(2, 9)),
        ))
    }
}

pub(super) struct NestedPipelineCostRule;

impl optimizer::OptimizerRule for NestedPipelineCostRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        static METADATA: std::sync::OnceLock<rules::RuleMetadata> = std::sync::OnceLock::new();
        METADATA.get_or_init(|| {
            rules::RuleMetadata::new(
                rules::RuleId::new("nested_pipeline_cost").unwrap(),
                rules::RuleKind::Implementation,
            )
        })
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let latency = match pipeline.input() {
            logical::RootStream::Pipeline(inner) => match single_limit_count(inner) {
                Some(1) => 1,
                Some(2) => 50,
                _ => return optimizer::RuleResult::NotApplicable,
            },
            logical::RootStream::VariableSource(_) => match single_limit_count(pipeline) {
                Some(1) => 100,
                Some(2) => 1,
                _ => return optimizer::RuleResult::NotApplicable,
            },
            _ => return optimizer::RuleResult::NotApplicable,
        };
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(alternative(latency)),
        ))
    }
}

pub(super) struct OuterPipelineOnlyCostRule;

impl optimizer::OptimizerRule for OuterPipelineOnlyCostRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        static METADATA: std::sync::OnceLock<rules::RuleMetadata> = std::sync::OnceLock::new();
        METADATA.get_or_init(|| {
            rules::RuleMetadata::new(
                rules::RuleId::new("outer_pipeline_only").unwrap(),
                rules::RuleKind::Implementation,
            )
        })
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if !matches!(pipeline.input(), logical::RootStream::Pipeline(_)) {
            return optimizer::RuleResult::NotApplicable;
        }
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(alternative(1)),
        ))
    }
}

pub(super) fn source() -> logical::LogicalExpr {
    logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    })
}

pub(super) fn edge_source() -> logical::LogicalExpr {
    logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Edge,
    })
}

pub(super) fn limit() -> logical::LogicalExpr {
    logical::LogicalExpr::Pure(logical::PureLogicalOp::Limit {
        count: ir::StreamBoundPlan::Literal(1),
    })
}

pub(super) fn variable_root_pipeline(count: usize) -> logical::LogicalExpr {
    logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(
                ir::NonEmptyString::new("seed").unwrap(),
            )),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(count),
            }),
        )
        .unwrap(),
    )
}

pub(super) fn nested_variable_root_pipeline(
    inner_count: usize,
    outer_count: usize,
) -> logical::LogicalExpr {
    let inner = logical::RootPipeline::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::new("seed").unwrap(),
        )),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(inner_count),
        }),
    )
    .unwrap();
    logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Pipeline(Box::new(inner)),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(outer_count),
            }),
        )
        .unwrap(),
    )
}

fn single_limit_count(pipeline: &logical::RootPipeline) -> Option<usize> {
    match pipeline.ops() {
        [logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(count),
        }] => Some(*count),
        _ => None,
    }
}

pub(super) fn alternative(latency: u64) -> physical::PhysicalAlternative {
    alternative_with_expr(physical::PhysicalExpr::Sort, latency)
}

pub(super) fn alternative_with_expr(
    expr: physical::PhysicalExpr,
    latency: u64,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        expr,
        properties::DeliveredProperties::default(),
        cost::CostVector {
            latency: cost::LatencyEstimate::micros(latency),
            ..cost::CostVector::ZERO
        },
    )
}

pub(super) fn alternative_with_element(
    element: properties::ElementKind,
    latency: u64,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Sort,
        properties::DeliveredProperties {
            element: Some(element),
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector {
            latency: cost::LatencyEstimate::micros(latency),
            ..cost::CostVector::ZERO
        },
    )
}

pub(super) fn config() -> optimizer::OptimizerConfig {
    optimizer::OptimizerConfig {
        limits: context::OptimizerLimits {
            memo_groups: properties::PositiveUsize::new(8).unwrap(),
            memo_expressions: properties::PositiveUsize::new(8).unwrap(),
            rule_fires: properties::PositiveUsize::new(16).unwrap(),
            alternatives_per_group: properties::PositiveUsize::new(8).unwrap(),
            optimization_micros: properties::PositiveUsize::new(1_000_000).unwrap(),
        },
        planner_limits: context::PlannerLimits::default(),
        stats: context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    }
}
