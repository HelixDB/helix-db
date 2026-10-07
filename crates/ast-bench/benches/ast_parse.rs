//! Per-stage cost of turning a native request body into a plan.
//!
//! `parse` compares the JSON backends over every corpus shape, `drop` times
//! freeing what parsing built, and `stages` times each later step of
//! `db::query_service` on its own. Inputs are built outside the timed region,
//! and divan drops return values after each sample, so every number is the
//! named work alone.

use divan::counter::BytesCount;
use divan::Bencher;
use helix_ast::query::QueryRequest;
use helix_ast::testing;

mod support;

#[cfg(not(feature = "mimalloc"))]
#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: divan::AllocProfiler<mimalloc::MiMalloc> =
    divan::AllocProfiler::new(mimalloc::MiMalloc);

fn main() {
    support::print_environment();
    divan::main();
}

fn parse(json: &[u8]) -> QueryRequest {
    QueryRequest::from_json_slice(json).expect("corpus shapes parse")
}

mod parse {
    use super::*;

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn sonic(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .bench(|| QueryRequest::from_json_slice(json).expect("corpus shapes parse"));
    }

    /// simd-json rewrites its input, so each sample parses a fresh copy made
    /// outside the timed region.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn simd_json(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                QueryRequest::from_json_slice_mut(body).expect("corpus shapes parse")
            });
    }

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn simd_json_reused_buffers(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        let mut buffers = simd_json::Buffers::new(json.len());
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                QueryRequest::from_json_slice_mut_with_buffers(body, &mut buffers)
                    .expect("corpus shapes parse")
            });
    }

    /// What the embedded `query_json` path pays: it holds `&[u8]`, so
    /// simd-json needs its own mutable copy.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn simd_json_with_copy(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher.counter(BytesCount::of_slice(json)).bench(|| {
            let mut body = json.clone();
            QueryRequest::from_json_slice_mut(&mut body).expect("corpus shapes parse")
        });
    }
}

mod drop {
    use super::*;

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn request(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| parse(json))
            .bench_local_values(std::mem::drop);
    }

    /// The AST alone, which `query_service` keeps until execution ends.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn batch(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| parse(json).into_query().0)
            .bench_local_values(std::mem::drop);
    }

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn params(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| helix_ast_bench::param_bindings(parse(json).into_query().1))
            .bench_local_values(std::mem::drop);
    }
}

mod stages {
    use super::*;

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn depth_scan(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .bench(|| testing::json_depth_within_limit(json));
    }

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn check_nesting(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| parse(json))
            .bench_local_refs(|request| request.check_nesting());
    }

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn param_bindings(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| parse(json).into_query().1)
            .bench_local_values(helix_ast_bench::param_bindings);
    }

    /// The deep copy `query_service` makes to give the planner context its
    /// own parameters.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn params_clone(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| helix_ast_bench::param_bindings(parse(json).into_query().1))
            .bench_local_refs(|params| params.clone());
    }

    #[divan::bench(args = support::plannable_shape_names(), max_time = 1)]
    fn plan(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| {
                let (batch, parameters) = parse(json).into_query();
                let context =
                    helix_ast_bench::planner_context(helix_ast_bench::param_bindings(parameters));
                (batch, context)
            })
            .bench_local_refs(|(batch, context)| {
                helix_planner::planning::plan_with_diagnostics(batch, context)
                    .expect("plannable shapes plan")
            });
    }
}
