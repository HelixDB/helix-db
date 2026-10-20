//! Per-stage cost of turning a native request body into a plan.
//!
//! `parse` compares owned against arena parsing with simd-json over
//! every corpus shape, `drop` times freeing what parsing built, `params`
//! compares owned and arena parameter values, and `stages` times each later
//! step of `db::query_service` on its own. Inputs are built outside the timed
//! region, and divan drops return values and inputs after each sample, so
//! every number is the named work alone. Arena arms return the request's
//! owned parameters, so neither side's timing includes freeing them.

use std::collections::BTreeMap;

use divan::counter::BytesCount;
use divan::Bencher;
use helix_ast::arena::{self, Bump, IntoOwned};
use helix_ast::query::{ArenaQueryRequest, QueryRequest, QueryValue};
use helix_ast::testing;

mod support;

/// The server's allocator, profiled.
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

/// Parse `body` in place into `bump` and keep only what outlives the arena
/// tree.
fn arena_parse(bump: &Bump, body: &mut [u8]) -> BTreeMap<String, QueryValue> {
    let request = ArenaQueryRequest::from_json_slice_mut(bump, body).expect("corpus shapes parse");
    divan::black_box(request.query());
    request.into_query().1
}

/// Arena bytes per body byte that holds every corpus shape without growing
/// its first chunk (from `ast_memory`).
const ARENA_BYTES_PER_BODY_BYTE: usize = 4;

/// simd-json rewrites its input, so every arm parses a fresh copy of the body
/// made outside the timed region, unless the copy is what it measures.
mod parse {
    use super::*;

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn owned(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                QueryRequest::from_json_slice_mut(body).expect("corpus shapes parse")
            });
    }

    /// simd-json's scratch buffers kept across requests.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn owned_reused_buffers(bencher: Bencher, name: &str) {
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
    fn owned_with_copy(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .bench(|| QueryRequest::from_json_slice(json).expect("corpus shapes parse"));
    }

    /// A fresh arena per request, growing chunk by chunk.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| (Bump::new(), json.clone()))
            .bench_local_refs(|(bump, body)| arena_parse(bump, body));
    }

    /// A fresh arena sized up front, so parsing never grows it.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena_presized(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| {
                (
                    Bump::with_capacity(json.len() * ARENA_BYTES_PER_BODY_BYTE),
                    json.clone(),
                )
            })
            .bench_local_refs(|(bump, body)| arena_parse(bump, body));
    }

    /// One arena reused across requests, reset before each, as a pool does.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena_reused(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        let mut bump = Bump::new();
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                bump.reset();
                arena_parse(&bump, body)
            });
    }

    /// A reused arena and reused simd-json scratch buffers.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena_reused_buffers(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        let mut bump = Bump::new();
        let mut buffers = simd_json::Buffers::new(json.len());
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                bump.reset();
                let request =
                    ArenaQueryRequest::from_json_slice_mut_with_buffers(&bump, body, &mut buffers)
                        .expect("corpus shapes parse");
                divan::black_box(request.query());
                request.into_query().1
            });
    }

    /// Arena parsing followed by the conversion to the owned request: what a
    /// single parser for both paths would cost the owned path.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena_then_into_owned(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        let mut bump = Bump::new();
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                bump.reset();
                ArenaQueryRequest::from_json_slice_mut(&bump, body)
                    .expect("corpus shapes parse")
                    .into_owned()
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

    /// Freeing an arena tree: the arena's chunks, whatever the tree's size.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| {
                let bump = Bump::new();
                drop(arena_parse(&bump, &mut json.clone()));
                bump
            })
            .bench_local_values(std::mem::drop);
    }

    /// Resetting an arena tree's arena for reuse: frees all but one chunk.
    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena_reset(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| {
                let bump = Bump::new();
                drop(arena_parse(&bump, &mut json.clone()));
                bump
            })
            .bench_local_refs(Bump::reset);
    }
}

/// Parameter values on their own: the whole body read as a generic JSON
/// value, owned against arena-backed, which is what a separate parameter
/// arena would change for bulk writes.
mod params {
    use super::*;

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn owned(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| json.clone())
            .bench_local_refs(|body| {
                simd_json::serde::from_slice::<QueryValue>(body).expect("corpus shapes are JSON")
            });
    }

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn arena(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .counter(BytesCount::of_slice(json))
            .with_inputs(|| (Bump::new(), json.clone()))
            .bench_local_refs(|(bump, body)| {
                let mut deserializer =
                    simd_json::Deserializer::from_slice(body).expect("corpus shapes are JSON");
                let value: arena::QueryValue<'_> = serde::de::DeserializeSeed::deserialize(
                    arena::Seed::new(bump),
                    &mut deserializer,
                )
                .expect("corpus shapes are JSON");
                divan::black_box(value);
            });
    }

    #[divan::bench(args = support::shape_names(), max_time = 1)]
    fn drop_owned(bencher: Bencher, name: &str) {
        let json = &support::shape(name).json;
        bencher
            .with_inputs(|| {
                simd_json::serde::from_slice::<QueryValue>(&mut json.clone())
                    .expect("corpus shapes are JSON")
            })
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
