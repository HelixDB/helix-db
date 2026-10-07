//! Corpus access and environment reporting shared by the benchmark binaries.

#![allow(dead_code)] // Each benchmark binary uses a different subset.

use std::sync::OnceLock;

use helix_ast::query::QueryRequest;
use helix_ast::testing::{self, Shape};

/// Every request shape, built once per benchmark process.
pub fn shapes() -> &'static [Shape] {
    static SHAPES: OnceLock<Vec<Shape>> = OnceLock::new();
    SHAPES.get_or_init(testing::all)
}

/// The shape named `name`.
///
/// # Panics
///
/// Panics when no shape has that name.
pub fn shape(name: &str) -> &'static Shape {
    shapes()
        .iter()
        .find(|shape| shape.name == name)
        .unwrap_or_else(|| panic!("no shape named {name}"))
}

/// Names of every shape, in corpus order.
pub fn shape_names() -> impl Iterator<Item = &'static str> {
    shapes().iter().map(|shape| shape.name.as_str())
}

/// Names of the shapes the planner accepts with an empty catalog.
pub fn plannable_shape_names() -> impl Iterator<Item = &'static str> {
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    NAMES
        .get_or_init(|| {
            shapes()
                .iter()
                .filter(|shape| {
                    let request =
                        QueryRequest::from_json_slice(&shape.json).expect("corpus shapes parse");
                    let (batch, parameters) = request.into_query();
                    let context = helix_ast_bench::planner_context(
                        helix_ast_bench::param_bindings(parameters),
                    );
                    helix_planner::planning::plan_with_diagnostics(&batch, &context).is_ok()
                })
                .map(|shape| shape.name.as_str())
                .collect()
        })
        .iter()
        .copied()
}

/// Shapes the multi-threaded runs use: small reads, a wide projection, a
/// wide batch and a bulk write.
pub const THROUGHPUT_SHAPES: [&str; 4] = [
    "dynamic-read",
    "ordered-range-wide-projection",
    "wide_batch/1000",
    "bulk_write_untyped/1000x768",
];

/// Print which SIMD path simd-json detected on this CPU, so every result
/// records what actually ran.
pub fn print_environment() {
    eprintln!(
        "simd-json: {:?} (runtime); allocator: mimalloc",
        simd_json::Deserializer::algorithm()
    );
}
