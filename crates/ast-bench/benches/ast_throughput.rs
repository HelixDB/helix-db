//! How request parsing scales when every core parses at once.
//!
//! Each iteration does a request's whole front-end life on one thread
//! (parse, nesting check, drop, and optionally planning) while the other
//! threads do the same, so allocator contention shows up as a per-iteration
//! slowdown. Scaling efficiency at `N` threads is `t(1) / t(N)`; 1.0 is
//! perfect scaling. Build with `--features mimalloc` to compare allocators.

use divan::Bencher;
use helix_ast::query::QueryRequest;

mod support;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    support::print_environment();
    divan::main();
}

const THREADS: [usize; 6] = [1, 2, 4, 8, 16, 0];

#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn sonic_parse_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.bench(|| {
        let request = QueryRequest::from_json_slice(json).expect("corpus shapes parse");
        request.check_nesting().expect("corpus shapes are bounded");
        drop(request);
    });
}

/// simd-json parses a fresh copy of the body made outside the timed region.
#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn simd_json_parse_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.with_inputs(|| json.clone()).bench_refs(|body| {
        let request = QueryRequest::from_json_slice_mut(body).expect("corpus shapes parse");
        request.check_nesting().expect("corpus shapes are bounded");
        drop(request);
    });
}

/// The front end up to and including planning, as `query_service` runs it.
#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn sonic_parse_plan_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.bench(|| {
        let request = QueryRequest::from_json_slice(json).expect("corpus shapes parse");
        request.check_nesting().expect("corpus shapes are bounded");
        let (batch, parameters) = request.into_query();
        let params = helix_ast_bench::param_bindings(parameters);
        let context = helix_ast_bench::planner_context(params.clone());
        let planning = helix_planner::planning::plan_with_diagnostics(&batch, &context)
            .expect("throughput shapes plan");
        drop((batch, params, context, planning));
    });
}
