//! Resident memory and wall time of a server-like front-end workload, with
//! the server's allocator (mimalloc).
//!
//! Every core runs the native front end as `query_service` does (parse, bound
//! the nesting, bind parameters, plan, free the batch, take the parameters
//! back) over a rotation of request shapes. Requested bytes are allocator
//! independent; resident memory is what the allocator's caching and
//! fragmentation add on top, so this measures what a server process actually
//! holds. Linux reads `VmHWM`/`VmRSS` from `/proc/self/status`; other
//! platforms report the peak only.

use std::time::Instant;

use helix_ast::query::QueryRequest;

mod support;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Shapes in the rotation: small reads, a wide batch, a bulk write, and a
/// parameter-heavy count.
const SHAPES: [&str; 5] = [
    "dynamic-read",
    "ordered-range-wide-projection",
    "wide_batch/1000",
    "bulk_write_untyped/1000x768",
    "count_with_unused_params/8388608",
];

const ROUNDS: usize = 20;

fn front_end(json: &[u8]) {
    let request = QueryRequest::from_json_slice(json).expect("corpus shapes parse");
    request.check_nesting().expect("corpus shapes are bounded");
    let (batch, parameters) = request.into_query();
    let context = helix_ast_bench::planner_context(helix_ast_bench::param_bindings(parameters));
    let planning = helix_planner::planning::plan_with_diagnostics(&batch, &context)
        .expect("rotation shapes plan");
    drop(batch);
    let params = context.params.into_inner();
    drop((params, planning));
}

/// `(current, peak)` resident KiB; the current figure is Linux only.
fn resident_kib() -> (Option<u64>, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|kib| kib.parse::<u64>().ok())
    };
    // SAFETY: `getrusage` only writes the zeroed struct it is given.
    let peak = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        // Linux reports KiB, macOS bytes.
        if cfg!(target_os = "macos") {
            usage.ru_maxrss as u64 / 1024
        } else {
            usage.ru_maxrss as u64
        }
    };
    (field("VmRSS:"), field("VmHWM:").unwrap_or(peak))
}

fn main() {
    support::print_environment();
    let shapes = SHAPES
        .iter()
        .map(|name| support::shape(name).json.as_slice())
        .collect::<Vec<_>>();
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let (before, _) = resident_kib();
    let started = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let shapes = &shapes;
            scope.spawn(move || {
                for round in 0..ROUNDS {
                    // Each thread starts the rotation at a different shape,
                    // so every shape is in flight at once.
                    for offset in 0..shapes.len() {
                        front_end(shapes[(thread + round + offset) % shapes.len()]);
                    }
                }
            });
        }
    });
    let elapsed = started.elapsed();
    let (after, peak) = resident_kib();
    let requests = threads * ROUNDS * SHAPES.len();
    let mib = |kib: Option<u64>| {
        kib.map_or("n/a".to_owned(), |kib| {
            format!("{:.1}", kib as f64 / 1024.0)
        })
    };
    println!(
        "threads {threads}, requests {requests}, wall {:.2} s, {:.1} requests/s, RSS before {} MiB, peak {} MiB, after {} MiB",
        elapsed.as_secs_f64(),
        requests as f64 / elapsed.as_secs_f64(),
        mib(before),
        mib(Some(peak)),
        mib(after),
    );
}
