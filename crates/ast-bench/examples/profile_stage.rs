//! Repeat one front-end stage on one corpus shape for a fixed time, so a
//! sampling profiler sees only that work:
//!
//! ```text
//! cargo build --release -p helix-ast-bench --example profile_stage
//! perf record -g target/release/examples/profile_stage plan wide_batch/1000 10
//! ```
//!
//! Stages: `parse` (simd-json into the owned AST, from a copy of the body), `plan` (parse, then plan;
//! parsing is a few percent of it for most shapes), and `front_end` (parse,
//! nesting check and plan, as `query_service` runs them).

use std::time::{Duration, Instant};

use helix_ast::query::QueryRequest;
use helix_ast::testing;

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(stage), Some(shape), Some(seconds)) = (args.next(), args.next(), args.next()) else {
        panic!("usage: profile_stage <parse|plan|front_end> <shape> <seconds>");
    };
    let seconds = seconds.parse::<u64>().expect("seconds is a whole number");
    let json = testing::all()
        .into_iter()
        .find(|candidate| candidate.name == shape)
        .unwrap_or_else(|| panic!("no shape named {shape}"))
        .json;
    let parse = || QueryRequest::from_json_slice(&json).expect("corpus shapes parse");
    let plan = |request: QueryRequest| {
        let (batch, parameters) = request.into_query();
        let context = helix_ast_bench::planner_context(helix_ast_bench::param_bindings(parameters));
        std::hint::black_box(
            helix_planner::planning::plan_with_diagnostics(&batch, &context)
                .expect("profiled shapes plan"),
        );
    };
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut iterations = 0_u64;
    while Instant::now() < deadline {
        match stage.as_str() {
            "parse" => drop(std::hint::black_box(parse())),
            "plan" => plan(parse()),
            "front_end" => {
                let request = parse();
                request.check_nesting().expect("corpus shapes are bounded");
                plan(request);
            }
            other => panic!("unknown stage {other}"),
        }
        iterations += 1;
    }
    eprintln!("{iterations} iterations of {stage} on {shape}");
}
