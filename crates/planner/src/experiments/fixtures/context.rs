//! Planner context construction for scalability fixtures.

use crate::{catalog, context, ir, properties};

use super::shape::PlanningScalabilityShape;

pub(super) fn context_for(
    shape: PlanningScalabilityShape,
    scale: properties::PositiveUsize,
) -> context::PlannerContext {
    let mut ctx = match shape {
        PlanningScalabilityShape::WideBooleanPredicates
        | PlanningScalabilityShape::ManyAvailableIndexes
        | PlanningScalabilityShape::BatchedRootReuse
        | PlanningScalabilityShape::ForEachBodyRootReuse
        | PlanningScalabilityShape::ManyMemoAlternatives
        | PlanningScalabilityShape::OrderedRangeWindowPushdown => indexed_context(scale.get()),
        PlanningScalabilityShape::MutationHeavyBatches => mutation_context(),
        PlanningScalabilityShape::SearchIndexDdlWorkloads => context::PlannerContext::default(),
        PlanningScalabilityShape::RuntimeDerivedMixedQueries => runtime_mixed_context(),
        PlanningScalabilityShape::OverLimitIndexDisjunction => {
            let mut ctx = indexed_context(scale.get());
            ctx.limits.max_index_union_branches =
                context::IndexUnionBranchLimit::limited(8).unwrap();
            ctx
        }
        PlanningScalabilityShape::DeepTraversalChain
        | PlanningScalabilityShape::BranchHeavyQueries => indexed_context(4),
    };
    // Coverage instrumentation makes wall-clock budgets noisy; memo/rule
    // thresholds remain the deterministic scalability signal for fixtures.
    ctx.optimizer_limits.optimization_micros = properties::PositiveUsize::at_least_one(1_000_000);
    ctx
}

fn mutation_context() -> context::PlannerContext {
    context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default()
            .with_node_eq(
                catalog::ScopedPropertyKey::try_new("Audit", "event_id")
                    .expect("fixture mutation key is valid"),
            )
            .with_node_eq(
                catalog::ScopedPropertyKey::try_new("User", "username")
                    .expect("fixture mutation key is valid"),
            )
            .with_edge_eq(
                catalog::ScopedPropertyKey::try_new("MENTIONS", "event_id")
                    .expect("fixture mutation key is valid"),
            ),
        stats: context::StatsSnapshot::default()
            .with_node_label_cardinality(ir::NonEmptyString::new("Audit").unwrap(), 1_000_000)
            .with_node_label_cardinality(ir::NonEmptyString::new("User").unwrap(), 1_000_000)
            .with_edge_label_cardinality(ir::NonEmptyString::new("MENTIONS").unwrap(), 1_000_000),
        ..context::PlannerContext::default()
    }
}

fn runtime_mixed_context() -> context::PlannerContext {
    let indexes = mutation_context()
        .indexes
        .with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("fixture range key is valid"),
        )
        .with_vector(
            catalog::SearchIndexKey::try_new(catalog::ElementKind::Node, "Doc", "embedding")
                .expect("fixture search key is valid"),
            catalog::SearchIndexScope::try_new(Some("tenant_id"))
                .expect("fixture tenant scope is valid"),
        )
        .with_text(
            catalog::SearchIndexKey::try_new(catalog::ElementKind::Edge, "MENTIONS", "body")
                .expect("fixture search key is valid"),
            catalog::SearchIndexScope::Unscoped,
        );
    context::PlannerContext {
        indexes,
        stats: context::StatsSnapshot::default()
            .with_node_label_cardinality(ir::NonEmptyString::new("Audit").unwrap(), 1_000_000)
            .with_node_label_cardinality(ir::NonEmptyString::new("User").unwrap(), 1_000_000)
            .with_node_label_cardinality(ir::NonEmptyString::new("Doc").unwrap(), 1_000_000)
            .with_edge_label_cardinality(ir::NonEmptyString::new("MENTIONS").unwrap(), 1_000_000),
        ..context::PlannerContext::default()
    }
}

fn indexed_context(index_count: usize) -> context::PlannerContext {
    let indexes = (0..index_count).fold(
        catalog::IndexCatalogSnapshot::default(),
        |indexes, index| {
            indexes.with_node_eq(
                catalog::ScopedPropertyKey::try_new("User", format!("p{index}"))
                    .expect("fixture property names are non-empty"),
            )
        },
    );
    context::PlannerContext {
        indexes: indexes.with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("fixture range key is valid"),
        ),
        stats: context::StatsSnapshot::default()
            .with_node_label_cardinality(ir::NonEmptyString::new("User").unwrap(), 1_000_000),
        ..context::PlannerContext::default()
    }
}
