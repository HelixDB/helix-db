//! Report-only benchmark for V2-aware secondary-index plan quality.
//!
//! Run the same binary from the baseline and candidate revisions, setting
//! `HELIX_PLANNER_BENCH_VARIANT` to identify each JSON report. Wall-clock
//! measurements are informational; stable plan digests, costs, and operator
//! census fields make semantic plan changes reviewable.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use helix_ast::batch::{self, ReadBatch};
use helix_ast::expr::Predicate;
use helix_ast::index::RangeIndexDirection;
use helix_ast::traversal::{self, Order};
use helix_planner::{catalog, context, cost, diagnostics, digest, exec, ir, planning};
use serde::Serialize;

const SCHEMA_VERSION: u8 = 2;
const WARMUP_REPETITIONS: usize = 3;
const MEASURED_REPETITIONS: usize = 10;
const POPULATIONS: [u64; 2] = [1_000, 10_000];

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

struct TrackingAllocator {
    enabled: AtomicBool,
    allocations: AtomicU64,
    allocated_bytes: AtomicU64,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            allocations: AtomicU64::new(0),
            allocated_bytes: AtomicU64::new(0),
        }
    }

    fn start(&self) {
        self.allocations.store(0, Ordering::Relaxed);
        self.allocated_bytes.store(0, Ordering::Relaxed);
        self.enabled.store(true, Ordering::Release);
    }

    fn finish(&self) -> (u64, u64) {
        self.enabled.store(false, Ordering::Release);
        (
            self.allocations.load(Ordering::Relaxed),
            self.allocated_bytes.load(Ordering::Relaxed),
        )
    }
}

// SAFETY: allocations and deallocations preserve the system allocator's
// pointer and layout contracts. The relaxed counters are observational only.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if self.enabled.load(Ordering::Acquire) {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.allocated_bytes
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: this method preserves the caller-provided layout contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout came from the delegated allocation.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if self.enabled.load(Ordering::Acquire) {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.allocated_bytes
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: this method preserves the caller-provided layout contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if self.enabled.load(Ordering::Acquire) {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.allocated_bytes
                .fetch_add(new_size as u64, Ordering::Relaxed);
        }
        // SAFETY: this method preserves the delegated reallocation contract.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[derive(Debug, Clone, Copy)]
enum BenchmarkCase {
    NodeEquality,
    NodeSameIndexUnion,
    NodeMultiIndexIntersection,
    NodeUniqueEquality,
    NodeRangeAscendingLimit,
    NodeRangeDescendingLimit,
    NodeEqualityOrderedRange,
    EdgeEquality,
    EdgeSameIndexUnion,
    EdgeMultiIndexIntersection,
    EdgeRangeAscendingLimit,
    EdgeRangeDescendingLimit,
    EdgeEqualityOrderedRange,
}

impl BenchmarkCase {
    const ALL: [Self; 13] = [
        Self::NodeEquality,
        Self::NodeSameIndexUnion,
        Self::NodeMultiIndexIntersection,
        Self::NodeUniqueEquality,
        Self::NodeRangeAscendingLimit,
        Self::NodeRangeDescendingLimit,
        Self::NodeEqualityOrderedRange,
        Self::EdgeEquality,
        Self::EdgeSameIndexUnion,
        Self::EdgeMultiIndexIntersection,
        Self::EdgeRangeAscendingLimit,
        Self::EdgeRangeDescendingLimit,
        Self::EdgeEqualityOrderedRange,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::NodeEquality => "node_equality",
            Self::NodeSameIndexUnion => "node_same_index_union",
            Self::NodeMultiIndexIntersection => "node_multi_index_intersection",
            Self::NodeUniqueEquality => "node_unique_equality",
            Self::NodeRangeAscendingLimit => "node_range_ascending_limit",
            Self::NodeRangeDescendingLimit => "node_range_descending_limit",
            Self::NodeEqualityOrderedRange => "node_equality_ordered_range",
            Self::EdgeEquality => "edge_equality",
            Self::EdgeSameIndexUnion => "edge_same_index_union",
            Self::EdgeMultiIndexIntersection => "edge_multi_index_intersection",
            Self::EdgeRangeAscendingLimit => "edge_range_ascending_limit",
            Self::EdgeRangeDescendingLimit => "edge_range_descending_limit",
            Self::EdgeEqualityOrderedRange => "edge_equality_ordered_range",
        }
    }

    const fn element(self) -> catalog::ElementKind {
        match self {
            Self::NodeEquality
            | Self::NodeSameIndexUnion
            | Self::NodeMultiIndexIntersection
            | Self::NodeUniqueEquality
            | Self::NodeRangeAscendingLimit
            | Self::NodeRangeDescendingLimit
            | Self::NodeEqualityOrderedRange => catalog::ElementKind::Node,
            Self::EdgeEquality
            | Self::EdgeSameIndexUnion
            | Self::EdgeMultiIndexIntersection
            | Self::EdgeRangeAscendingLimit
            | Self::EdgeRangeDescendingLimit
            | Self::EdgeEqualityOrderedRange => catalog::ElementKind::Edge,
        }
    }

    fn batch(self) -> ReadBatch {
        let traversal = traversal::g();
        match self {
            Self::NodeEquality => batch::read_batch().var_as(
                "result",
                traversal.n_with_label_where("User", Predicate::eq("status", "active")),
            ),
            Self::NodeSameIndexUnion => batch::read_batch().var_as(
                "result",
                traversal.n_with_label_where(
                    "User",
                    Predicate::or(vec![
                        Predicate::eq("status", "active"),
                        Predicate::eq("status", "pending"),
                    ]),
                ),
            ),
            Self::NodeMultiIndexIntersection => batch::read_batch().var_as(
                "result",
                traversal.n_with_label_where(
                    "User",
                    Predicate::and(vec![
                        Predicate::eq("status", "active"),
                        Predicate::eq("region", "eu"),
                    ]),
                ),
            ),
            Self::NodeUniqueEquality => batch::read_batch().var_as(
                "result",
                traversal.n_with_label_where("User", Predicate::eq("email", "user@example.test")),
            ),
            Self::NodeRangeAscendingLimit => batch::read_batch().var_as(
                "result",
                traversal
                    .n_with_label_where("User", Predicate::between("age", 21_i64, 65_i64))
                    .order_by("age", Order::Asc)
                    .limit(100_usize),
            ),
            Self::NodeRangeDescendingLimit => batch::read_batch().var_as(
                "result",
                traversal
                    .n_with_label_where("User", Predicate::between("score", 100_i64, 900_i64))
                    .order_by("score", Order::Desc)
                    .limit(100_usize),
            ),
            Self::NodeEqualityOrderedRange => batch::read_batch().var_as(
                "result",
                traversal
                    .n_with_label_where(
                        "User",
                        Predicate::and(vec![
                            Predicate::eq("status", "active"),
                            Predicate::between("age", 21_i64, 65_i64),
                        ]),
                    )
                    .order_by("age", Order::Asc)
                    .limit(100_usize),
            ),
            Self::EdgeEquality => batch::read_batch().var_as(
                "result",
                traversal.e_with_label_where("FOLLOWS", Predicate::eq("status", "active")),
            ),
            Self::EdgeSameIndexUnion => batch::read_batch().var_as(
                "result",
                traversal.e_with_label_where(
                    "FOLLOWS",
                    Predicate::or(vec![
                        Predicate::eq("status", "active"),
                        Predicate::eq("status", "pending"),
                    ]),
                ),
            ),
            Self::EdgeMultiIndexIntersection => batch::read_batch().var_as(
                "result",
                traversal.e_with_label_where(
                    "FOLLOWS",
                    Predicate::and(vec![
                        Predicate::eq("status", "active"),
                        Predicate::eq("region", "eu"),
                    ]),
                ),
            ),
            Self::EdgeRangeAscendingLimit => batch::read_batch().var_as(
                "result",
                traversal
                    .e_with_label_where("FOLLOWS", Predicate::between("weight", 1_i64, 50_i64))
                    .order_by("weight", Order::Asc)
                    .limit(100_usize),
            ),
            Self::EdgeRangeDescendingLimit => batch::read_batch().var_as(
                "result",
                traversal
                    .e_with_label_where(
                        "FOLLOWS",
                        Predicate::between("created_at", 1_i64, 10_000_i64),
                    )
                    .order_by("created_at", Order::Desc)
                    .limit(100_usize),
            ),
            Self::EdgeEqualityOrderedRange => batch::read_batch().var_as(
                "result",
                traversal
                    .e_with_label_where(
                        "FOLLOWS",
                        Predicate::and(vec![
                            Predicate::eq("status", "active"),
                            Predicate::between("weight", 1_i64, 50_i64),
                        ]),
                    )
                    .order_by("weight", Order::Asc)
                    .limit(100_usize),
            ),
        }
    }
}

#[derive(Serialize)]
struct BenchmarkReport {
    schema_version: u8,
    variant: String,
    warmup_repetitions: usize,
    measured_repetitions: usize,
    cases: Vec<CaseReport>,
}

#[derive(Serialize)]
struct CaseReport {
    name: &'static str,
    element: catalog::ElementKind,
    population: u64,
    selected_shape: &'static str,
    plan_digest: digest::PlanDigest,
    selected_cost: cost::CostVector,
    planning_nanos_p50: u64,
    planning_nanos_p95: u64,
    planning_throughput_per_second_p50: f64,
    allocations_per_plan: f64,
    allocated_bytes_per_plan: f64,
    planner_statistics: diagnostics::PlannerStatistics,
}

fn main() {
    let variant =
        std::env::var("HELIX_PLANNER_BENCH_VARIANT").unwrap_or_else(|_| "unlabeled".to_string());
    let mut cases = Vec::with_capacity(BenchmarkCase::ALL.len() * POPULATIONS.len());
    for population in POPULATIONS {
        let ctx = planner_context(population);
        for case in BenchmarkCase::ALL {
            let batch = case.batch();
            for _ in 0..WARMUP_REPETITIONS {
                std::hint::black_box(
                    planning::plan_read_batch_with_diagnostics(&batch, &ctx)
                        .expect("V2 plan-quality warmup should plan"),
                );
            }

            let mut elapsed = Vec::with_capacity(MEASURED_REPETITIONS);
            let mut representative = None;
            ALLOCATOR.start();
            for _ in 0..MEASURED_REPETITIONS {
                let started = Instant::now();
                let output = planning::plan_read_batch_with_diagnostics(&batch, &ctx)
                    .expect("V2 plan-quality fixture should plan");
                elapsed.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                representative.get_or_insert(output);
            }
            let (allocations, allocated_bytes) = ALLOCATOR.finish();
            elapsed.sort_unstable();
            let output = representative.expect("measured repetitions are non-empty");
            let planning_nanos_p50 = percentile(&elapsed, 50);
            cases.push(CaseReport {
                name: case.name(),
                element: case.element(),
                population,
                selected_shape: selected_shape(output.plan()),
                plan_digest: digest::PlanDigest::for_tagged_value(
                    "v2_plan_quality_exec_steps:v1",
                    &output.plan().steps(),
                ),
                selected_cost: output.plan().metrics().selected_cost,
                planning_nanos_p50,
                planning_nanos_p95: percentile(&elapsed, 95),
                planning_throughput_per_second_p50: 1_000_000_000_f64 / planning_nanos_p50 as f64,
                allocations_per_plan: allocations as f64 / MEASURED_REPETITIONS as f64,
                allocated_bytes_per_plan: allocated_bytes as f64 / MEASURED_REPETITIONS as f64,
                planner_statistics: output.diagnostics().statistics.clone(),
            });
        }
    }

    let report = BenchmarkReport {
        schema_version: SCHEMA_VERSION,
        variant,
        warmup_repetitions: WARMUP_REPETITIONS,
        measured_repetitions: MEASURED_REPETITIONS,
        cases,
    };
    let json = serde_json::to_string_pretty(&report).expect("benchmark report serializes");
    if let Ok(output) = std::env::var("HELIX_PLANNER_BENCH_OUTPUT") {
        std::fs::write(output, &json).expect("benchmark report output is writable");
    }
    println!("{json}");
}

fn selected_shape(plan: &exec::ExecutablePlan) -> &'static str {
    let value = serde_json::to_value(plan.steps()).expect("executable steps serialize");
    classify_serialized_shape(&value)
}

fn classify_serialized_shape(value: &serde_json::Value) -> &'static str {
    let serialized = value.to_string();
    if serialized.contains("ordered_intersect") {
        return "ordered_range_bitmap_filter";
    }
    if serialized.contains("secondary_set") && serialized.contains("intersect") {
        return "bitmap_intersection";
    }
    if serialized.contains("secondary_set") && serialized.contains("union") {
        return "bitmap_union";
    }
    if serialized.contains("secondary_set") && has_multi_value_equality(value) {
        return "batched_bitmap_equality";
    }
    if serialized.contains("equality_index") && serialized.contains("\"uniqueness\":\"unique\"") {
        return "unique_equality_verified_point";
    }
    if serialized.contains("equality_index") || serialized.contains("secondary_set") {
        return "bitmap_equality_point";
    }
    if serialized.contains("range_index") {
        return "ordered_range_verified_scan";
    }
    if serialized.contains("access") {
        return "row_stream_access";
    }
    "no_access"
}

fn has_multi_value_equality(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Array(values) => values.iter().any(has_multi_value_equality),
        serde_json::Value::Object(fields) => fields.iter().any(|(key, value)| {
            (key == "values" && value.as_array().is_some_and(|values| values.len() > 1))
                || has_multi_value_equality(value)
        }),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => false,
    }
}

fn planner_context(population: u64) -> context::PlannerContext {
    let node_status = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let node_region = catalog::ScopedPropertyKey::try_new("User", "region").unwrap();
    let node_email = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
    let node_age =
        catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
            .unwrap();
    let node_score =
        catalog::ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Desc)
            .unwrap();
    let edge_status = catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let edge_region = catalog::ScopedPropertyKey::try_new("FOLLOWS", "region").unwrap();
    let edge_weight =
        catalog::ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
            .unwrap();
    let edge_created = catalog::ScopedPropertyDirectionKey::try_new(
        "FOLLOWS",
        "created_at",
        RangeIndexDirection::Desc,
    )
    .unwrap();

    let mut indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(node_status.clone())
        .with_node_eq(node_region.clone())
        .with_node_range(node_age.clone())
        .with_node_range(node_score.clone())
        .with_edge_eq(edge_status.clone())
        .with_edge_eq(edge_region.clone())
        .with_edge_range(edge_weight.clone())
        .with_edge_range(edge_created.clone());
    indexes.node_eq.insert(
        node_email.clone(),
        catalog::NodeEqualityIndexMeta::try_new("node_eq:User:email")
            .unwrap()
            .with_uniqueness(catalog::IndexUniqueness::Unique),
    );

    context::PlannerContext {
        indexes,
        stats: context::StatsSnapshot::default()
            .with_node_label_cardinality(ir::NonEmptyString::new("User").unwrap(), population)
            .with_edge_label_cardinality(ir::NonEmptyString::new("FOLLOWS").unwrap(), population)
            .with_node_eq_cardinality(node_status, population / 10)
            .with_node_eq_cardinality(node_region, population / 5)
            .with_node_eq_cardinality(node_email, 1)
            .with_node_range_cardinality(node_age, population / 2)
            .with_node_range_cardinality(node_score, population / 2)
            .with_edge_eq_cardinality(edge_status, population / 10)
            .with_edge_eq_cardinality(edge_region, population / 5)
            .with_edge_range_cardinality(edge_weight, population / 2)
            .with_edge_range_cardinality(edge_created, population / 2),
        ..context::PlannerContext::default()
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    assert!(!sorted.is_empty(), "percentiles require measured samples");
    assert!(percentile <= 100, "percentiles cannot exceed one hundred");
    let numerator = sorted.len().saturating_mul(percentile);
    let index = numerator.div_ceil(100).saturating_sub(1);
    sorted[index]
}
