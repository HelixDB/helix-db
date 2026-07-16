use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use helix_planner::context::PlannerContext;
use helix_planner::exec::coalesce_multi_get_batches;
use helix_planner::experiments::{
    self, CostProfileComparisonFixture, CostProfileVariant, PlanScalabilityFixture,
    PlanningScalabilityShape,
};
use helix_planner::properties::{KeyLocality, PositiveUsize};

fn bench_shape(c: &mut Criterion, name: &str, shape: PlanningScalabilityShape) {
    let mut group = c.benchmark_group(name);
    for fixture in experiments::default_planning_scalability_fixtures()
        .into_iter()
        .filter(|fixture| fixture.shape() == shape)
    {
        bench_fixture(&mut group, fixture);
    }
    group.finish();
}

fn bench_fixture(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    fixture: PlanScalabilityFixture,
) {
    let scale = fixture.scale().get();
    let case = fixture.case();
    group.bench_with_input(
        BenchmarkId::from_parameter(scale),
        &case,
        |bencher, case| {
            bencher.iter(|| {
                let _plan = case.plan().expect("bench query should plan");
            });
        },
    );
}

fn wide_boolean_predicates(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_wide_boolean_predicates",
        PlanningScalabilityShape::WideBooleanPredicates,
    );
}

fn many_available_indexes(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_many_available_indexes",
        PlanningScalabilityShape::ManyAvailableIndexes,
    );
}

fn batched_root_reuse(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_batched_root_reuse",
        PlanningScalabilityShape::BatchedRootReuse,
    );
}

fn foreach_body_root_reuse(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_foreach_body_root_reuse",
        PlanningScalabilityShape::ForEachBodyRootReuse,
    );
}

fn deep_traversal_chain(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_deep_traversal_chain",
        PlanningScalabilityShape::DeepTraversalChain,
    );
}

fn many_memo_alternatives(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_many_memo_alternatives",
        PlanningScalabilityShape::ManyMemoAlternatives,
    );
}

fn over_limit_index_disjunction(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_over_limit_index_disjunction",
        PlanningScalabilityShape::OverLimitIndexDisjunction,
    );
}

fn branch_heavy_queries(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_branch_heavy_queries",
        PlanningScalabilityShape::BranchHeavyQueries,
    );
}

fn ordered_range_window_pushdown(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_ordered_range_window_pushdown",
        PlanningScalabilityShape::OrderedRangeWindowPushdown,
    );
}

fn mutation_heavy_batches(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_mutation_heavy_batches",
        PlanningScalabilityShape::MutationHeavyBatches,
    );
}

fn search_index_ddl_workloads(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_search_index_ddl_workloads",
        PlanningScalabilityShape::SearchIndexDdlWorkloads,
    );
}

fn runtime_derived_mixed_queries(c: &mut Criterion) {
    bench_shape(
        c,
        "planner_runtime_derived_mixed_queries",
        PlanningScalabilityShape::RuntimeDerivedMixedQueries,
    );
}

fn multi_get_coalescing(c: &mut Criterion) {
    let profile = PlannerContext::default().storage;
    let mut group = c.benchmark_group("planner_multi_get_coalescing");
    for keys in [64_usize, 512, 4096] {
        group.bench_with_input(BenchmarkId::from_parameter(keys), &keys, |bencher, _| {
            bencher.iter(|| {
                let keys = experiments::coalescing_keys(
                    PositiveUsize::new(keys).expect("bench key count is positive"),
                );
                let _batches =
                    coalesce_multi_get_batches(keys, KeyLocality::Close, &profile).unwrap();
            });
        });
    }
    group.finish();
}

fn cost_profile_comparisons(c: &mut Criterion) {
    let mut group = c.benchmark_group("planner_cost_profile_comparisons");
    for fixture in experiments::default_cost_profile_comparison_fixtures() {
        group.bench_with_input(
            BenchmarkId::from_parameter(cost_profile_comparison_id(fixture)),
            &fixture,
            |bencher, fixture| {
                bencher.iter(|| {
                    let _comparison = fixture.compare().expect("profile comparison should plan");
                });
            },
        );
    }
    group.finish();
}

fn cost_profile_comparison_id(fixture: CostProfileComparisonFixture) -> String {
    format!(
        "{:?}/{}:{}_to_{}",
        fixture.fixture().shape(),
        fixture.fixture().scale().get(),
        cost_profile_variant_name(fixture.baseline()),
        cost_profile_variant_name(fixture.candidate())
    )
}

const fn cost_profile_variant_name(variant: CostProfileVariant) -> &'static str {
    match variant {
        CostProfileVariant::Default => "default",
        CostProfileVariant::ExpensiveRangeScans => "expensive_range_scans",
        CostProfileVariant::BroadEqualityFallback => "broad_equality_fallback",
    }
}

criterion_group!(
    planner_benches,
    wide_boolean_predicates,
    many_available_indexes,
    batched_root_reuse,
    foreach_body_root_reuse,
    deep_traversal_chain,
    many_memo_alternatives,
    over_limit_index_disjunction,
    branch_heavy_queries,
    ordered_range_window_pushdown,
    mutation_heavy_batches,
    search_index_ddl_workloads,
    runtime_derived_mixed_queries,
    multi_get_coalescing,
    cost_profile_comparisons
);
criterion_main!(planner_benches);
