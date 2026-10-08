//! How request parsing scales when every core parses at once.
//!
//! Each iteration does a request's whole front-end life on one thread
//! (parse, nesting check, drop, and optionally planning) while the other
//! threads do the same, so allocator contention shows up as a per-iteration
//! slowdown. Arena arms take their arena from a shared [`Pool`], a fresh
//! [`Bump`], or a thread-local one, and need no nesting check: an arena tree
//! comes only from JSON, whose depth was bounded before parsing.
//!
//! Scaling efficiency at `N` threads is `t(1) / t(N)`; 1.0 is perfect
//! scaling. Like the server, the benchmark allocates through mimalloc.

use std::cell::RefCell;

use divan::Bencher;
use helix_ast::arena::{Bump, Pool, PoolConfig};
use helix_ast::query::{ArenaQueryRequest, QueryRequest};

mod support;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    support::print_environment();
    divan::main();
}

const THREADS: [usize; 6] = [1, 2, 4, 8, 16, 0];

/// Every arm parses a fresh copy of the body, made outside the timed region,
/// in place.
#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn parse_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.with_inputs(|| json.clone()).bench_refs(|body| {
        let request = QueryRequest::from_json_slice_mut(body).expect("corpus shapes parse");
        request.check_nesting().expect("corpus shapes are bounded");
        drop(request);
    });
}

/// The front end up to and including planning, as `query_service` runs it:
/// the batch is dropped once planned and the parameters move to execution.
#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn parse_plan_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.with_inputs(|| json.clone()).bench_refs(|body| {
        let request = QueryRequest::from_json_slice_mut(body).expect("corpus shapes parse");
        request.check_nesting().expect("corpus shapes are bounded");
        let (batch, parameters) = request.into_query();
        let context = helix_ast_bench::planner_context(helix_ast_bench::param_bindings(parameters));
        let planning = helix_planner::planning::plan_with_diagnostics(&batch, &context)
            .expect("throughput shapes plan");
        drop(batch);
        // Execution takes the parameters back from the context.
        let params = context.params.into_inner();
        drop((params, planning));
    });
}

/// Arenas shared by every benchmark thread, sized so steady-state requests
/// never grow a chunk; one 16 MiB body may grow its arena, which is then
/// freed rather than kept.
static POOL: Pool = Pool::new(PoolConfig {
    initial_chunk_bytes: 64 * 1024,
    retain_bytes: 4 * 1024 * 1024,
    max_idle: 64,
    allocation_limit: None,
});

#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn arena_pooled_parse_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.with_inputs(|| json.clone()).bench_refs(|body| {
        let bump = POOL.checkout();
        let request =
            ArenaQueryRequest::from_json_slice_mut(&bump, body).expect("corpus shapes parse");
        divan::black_box(request.query());
        drop(request);
        drop(bump);
    });
}

#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn arena_fresh_parse_drop(bencher: Bencher, name: &str) {
    let json = &support::shape(name).json;
    bencher.with_inputs(|| json.clone()).bench_refs(|body| {
        let bump = Bump::new();
        let request =
            ArenaQueryRequest::from_json_slice_mut(&bump, body).expect("corpus shapes parse");
        divan::black_box(request.query());
        drop(request);
        drop(bump);
    });
}

/// A per-thread arena: no pool lock, but unusable across awaits, because a
/// request may resume on another thread.
#[divan::bench(args = support::THROUGHPUT_SHAPES, threads = THREADS, max_time = 2)]
fn arena_thread_local_parse_drop(bencher: Bencher, name: &str) {
    thread_local! {
        static BUMP: RefCell<Bump> = RefCell::new(Bump::new());
    }
    let json = &support::shape(name).json;
    bencher.with_inputs(|| json.clone()).bench_refs(|body| {
        BUMP.with_borrow_mut(|bump| {
            bump.reset();
            let request =
                ArenaQueryRequest::from_json_slice_mut(bump, body).expect("corpus shapes parse");
            divan::black_box(request.query());
        });
    });
}
