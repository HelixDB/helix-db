//! Heap and stack a native request costs from body to plan.
//!
//! A calling-thread counting allocator (the `db` crate's
//! `allocation_testing` pattern) records every allocation request, so the
//! numbers are requested bytes, independent of allocator overhead and RSS.
//! Sections:
//!
//! - `sizes`: in-memory size of the AST's core types.
//! - `parse`: what parsing each shape allocates, its peak, what it retains,
//!   and that dropping the request returns every byte, owned and into an
//!   arena; for arena rows, the arena's chunk bytes and how many it filled.
//! - `parameters`: a body read as one generic JSON value, owned against in an
//!   arena: what a separate parameter arena would hold instead of the heap.
//! - `lifecycle`: live heap at each `query_service` stage boundary, while
//!   execution would run, and the peak across the request, for the order
//!   values were freed in before this change and the order they are now.
//! - `stack`: the smallest thread stack that parses and drops the deepest
//!   accepted chain, found by re-running this binary in a child process,
//!   because a stack overflow aborts the process.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::process::Command;

use helix_ast::arena::{self, Bump};
use helix_ast::query::{ArenaQueryRequest, QueryRequest, QueryValue};
use helix_ast::testing;

mod support;

#[derive(Clone, Copy, Debug, Default)]
struct Count {
    allocations: usize,
    bytes: usize,
    live: isize,
    peak: isize,
}

thread_local! {
    static COUNT: Cell<Option<Count>> = const { Cell::new(None) };
}

fn record(allocated: usize, released: usize) {
    let _ = COUNT.try_with(|cell| {
        let Some(mut count) = cell.get() else {
            return;
        };
        if allocated > 0 {
            count.allocations += 1;
            count.bytes += allocated;
        }
        count.live += allocated as isize - released as isize;
        count.peak = count.peak.max(count.live);
        cell.set(Some(count));
    });
}

struct CountingAllocator;

// SAFETY: Every operation forwards its caller's valid arguments to mimalloc,
// the server's allocator, unchanged; counting is allocation-free thread-local
// arithmetic.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc`'s contract.
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size(), 0);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        let pointer = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size(), 0);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::realloc`'s contract.
        let resized = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, size) };
        if !resized.is_null() {
            record(size, layout.size());
        }
        resized
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(0, layout.size());
        // SAFETY: The caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Count the allocations `operation` makes on this thread. `Count::live` is
/// what it retains; `Count::peak` is relative to the start.
fn observe<T>(operation: impl FnOnce() -> T) -> (T, Count) {
    COUNT.with(|cell| {
        assert!(cell.get().is_none(), "observations cannot nest");
        cell.set(Some(Count::default()));
    });
    let result = operation();
    let count = COUNT.with(|cell| cell.take().expect("observation is active"));
    (result, count)
}

/// Live bytes so far in the active observation.
fn live() -> isize {
    COUNT.with(|cell| cell.get().expect("observation is active").live)
}

fn mib(bytes: isize) -> String {
    format!("{:.3}", bytes as f64 / (1024.0 * 1024.0))
}

fn sizes() {
    println!("## sizes\n");
    println!("| type | bytes |\n|---|---:|");
    [
        ("AstNode", size_of::<helix_ast::traversal::AstNode>()),
        ("Predicate", size_of::<helix_ast::expr::Predicate>()),
        ("Expr", size_of::<helix_ast::expr::Expr>()),
        (
            "PropertyValue",
            size_of::<helix_ast::value::PropertyValue>(),
        ),
        (
            "PropertyInput",
            size_of::<helix_ast::value::PropertyInput>(),
        ),
        ("QueryValue", size_of::<helix_ast::query::QueryValue>()),
        ("BatchEntry", size_of::<helix_ast::batch::BatchEntry>()),
        ("QueryRequest", size_of::<QueryRequest>()),
    ]
    .into_iter()
    .for_each(|(name, bytes)| println!("| {name} | {bytes} |"));
}

fn parse() {
    println!("\n## parse\n");
    println!(
        "| shape | parser | body MiB | allocations | allocated MiB | peak MiB | retained MiB | retained/body | arena chunks MiB | arena filled MiB | leaked bytes |"
    );
    println!("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    let row = |shape: &testing::Shape,
               parser: String,
               parsed: Count,
               arena: Option<(usize, usize)>,
               leaked: isize| {
        let (chunks, filled) = arena.map_or(
            (String::from("–"), String::from("–")),
            |(chunks, filled)| (mib(chunks as isize), mib(filled as isize)),
        );
        println!(
            "| {} | {parser} | {} | {} | {} | {} | {} | {:.2} | {chunks} | {filled} | {leaked} |",
            shape.name,
            mib(shape.json.len() as isize),
            parsed.allocations,
            mib(parsed.bytes as isize),
            mib(parsed.peak),
            mib(parsed.live),
            parsed.live as f64 / shape.json.len() as f64,
        );
    };
    for shape in support::shapes() {
        // simd-json parses the transport's body in place.
        let mut body = shape.json.clone();
        let (request, parsed) =
            observe(|| QueryRequest::from_json_slice_mut(&mut body).expect("corpus shapes parse"));
        let ((), dropped) = observe(|| drop(request));
        row(
            shape,
            String::from("owned"),
            parsed,
            None,
            parsed.live + dropped.live,
        );
        let mut body = shape.json.clone();
        let bump = Bump::new();
        let (request, parsed) = observe(|| {
            ArenaQueryRequest::from_json_slice_mut(&bump, &mut body).expect("corpus shapes parse")
        });
        let chunks = bump.allocated_bytes();
        let filled = chunks - bump.chunk_capacity();
        let ((), dropped_request) = observe(|| drop(request));
        let ((), dropped_arena) = observe(|| drop(bump));
        row(
            shape,
            String::from("arena"),
            parsed,
            Some((chunks, filled)),
            parsed.live + dropped_request.live + dropped_arena.live,
        );
    }
}

fn parameters() {
    println!("\n## parameters (whole body as one JSON value)\n");
    println!(
        "| shape | body MiB | owned allocations | owned retained MiB | arena allocations | arena chunks MiB | arena filled MiB |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for shape in support::shapes() {
        let mut body = shape.json.clone();
        let (value, owned) = observe(|| {
            simd_json::serde::from_slice::<QueryValue>(&mut body).expect("corpus shapes are JSON")
        });
        drop(value);
        let mut body = shape.json.clone();
        let bump = Bump::new();
        let ((), parsed) = observe(|| {
            let mut deserializer =
                simd_json::Deserializer::from_slice(&mut body).expect("corpus shapes are JSON");
            let value: arena::QueryValue<'_> =
                serde::de::DeserializeSeed::deserialize(arena::Seed::new(&bump), &mut deserializer)
                    .expect("corpus shapes are JSON");
            std::hint::black_box(value);
        });
        let chunks = bump.allocated_bytes();
        println!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            shape.name,
            mib(shape.json.len() as isize),
            owned.allocations,
            mib(owned.live),
            parsed.allocations,
            mib(chunks as isize),
            mib((chunks - bump.chunk_capacity()) as isize),
        );
    }
}

/// The order a request's front-end values are freed in.
#[derive(Clone, Copy)]
enum Retention {
    /// Before this change: the transport holds the body and `query_service`
    /// holds the batch until execution ends, and the planner context gets its
    /// own copy of the parameters.
    HeldUntilExecution,
    /// The body is freed after parsing, the batch after planning, and planning
    /// and execution share one parameter copy.
    FreedEarly,
}

impl Retention {
    fn name(self) -> &'static str {
        match self {
            Self::HeldUntilExecution => "held until execution (before)",
            Self::FreedEarly => "freed early (current)",
        }
    }
}

/// Live heap at each `query_service` stage boundary, the peak across the
/// request, and the heap still live while execution would run.
fn lifecycle(retention: Retention) {
    println!("\n## lifecycle: {}\n", retention.name());
    println!(
        "| shape | body | +parse | +check_nesting | +bindings | +context | +plan | during execution | peak | after drop |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for name in support::plannable_shape_names() {
        let json = &support::shape(name).json;
        let (boundaries, count) = observe(|| {
            let mut body = json.clone();
            let after_body = live();
            let request =
                QueryRequest::from_json_slice_mut(&mut body).expect("corpus shapes parse");
            let body = match retention {
                Retention::HeldUntilExecution => Some(body),
                Retention::FreedEarly => {
                    drop(body);
                    None
                }
            };
            let after_parse = live();
            request.check_nesting().expect("corpus shapes are bounded");
            let after_check = live();
            let (batch, parameters) = request.into_query();
            let params = helix_ast_bench::param_bindings(parameters);
            let after_bindings = live();
            let (context, params) = match retention {
                Retention::HeldUntilExecution => (
                    helix_ast_bench::planner_context(params.clone()),
                    Some(params),
                ),
                Retention::FreedEarly => (helix_ast_bench::planner_context(params), None),
            };
            let after_context = live();
            let planning = helix_planner::planning::plan_with_diagnostics(&batch, &context)
                .expect("plannable shapes plan");
            let after_plan = live();
            let batch = match retention {
                Retention::HeldUntilExecution => Some(batch),
                Retention::FreedEarly => {
                    drop(batch);
                    None
                }
            };
            // Execution runs with exactly these alive.
            let during_execution = live();
            drop((body, batch, params, context, planning));
            [
                after_body,
                after_parse,
                after_check,
                after_bindings,
                after_context,
                after_plan,
                during_execution,
            ]
        });
        let [body, parse, check, bindings, context, plan, execution] = boundaries.map(mib);
        println!(
            "| {name} | {body} | {parse} | {check} | {bindings} | {context} | {plan} | {execution} | {} | {} |",
            mib(count.peak),
            count.live
        );
    }
}

const STACK_PROBE: &str = "HELIX_AST_STACK_PROBE";

#[derive(Clone, Copy)]
enum StackStage {
    Parse,
    Drop,
    ArenaParse,
}

impl StackStage {
    const ALL: [Self; 3] = [Self::Parse, Self::Drop, Self::ArenaParse];

    fn name(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::Drop => "drop",
            Self::ArenaParse => "arena_parse",
        }
    }
}

/// Exit status of a probe whose stack overflowed.
const OVERFLOWED: i32 = 3;

extern "C" fn exit_on_overflow(_signal: libc::c_int) {
    // SAFETY: `_exit` is async-signal-safe and ends the process at once.
    unsafe { libc::_exit(OVERFLOWED) }
}

/// Run one probe in this child process: exits 0 when `stage` fits in a
/// thread stack of `kib` KiB and [`OVERFLOWED`] when it does not. A stack
/// overflow faults on the thread's guard page; std's own handler would abort
/// (and leave an OS crash report per probe), so the child replaces it with
/// one that exits quietly. std still gives every spawned thread an alternate
/// signal stack, which this handler runs on.
fn stack_probe_child(probe: &str) {
    // SAFETY: The action is fully initialised, its handler only calls the
    // async-signal-safe `_exit`, and `SA_ONSTACK` runs it on the alternate
    // stack std installs for each thread.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = exit_on_overflow as *const () as usize;
        action.sa_flags = libc::SA_ONSTACK;
        for signal in [libc::SIGSEGV, libc::SIGBUS] {
            assert_eq!(
                libc::sigaction(signal, &action, std::ptr::null_mut()),
                0,
                "overflow handler installs"
            );
        }
    }
    let Some((stage, kib)) = probe.split_once(':') else {
        panic!("malformed probe {probe}");
    };
    let stage = StackStage::ALL
        .into_iter()
        .find(|candidate| candidate.name() == stage)
        .expect("known stage");
    let kib = kib.parse::<usize>().expect("stack size in KiB");
    let mut body = testing::deep_chain(testing::MAX_DEEP_CHAIN_STEPS).json;
    let prepared = match stage {
        StackStage::Parse | StackStage::ArenaParse => None,
        StackStage::Drop => {
            Some(QueryRequest::from_json_slice_mut(&mut body.clone()).expect("corpus shapes parse"))
        }
    };
    std::thread::Builder::new()
        .stack_size(kib << 10)
        .spawn(move || match (stage, prepared) {
            (StackStage::ArenaParse, _) => {
                let bump = Bump::new();
                let request = ArenaQueryRequest::from_json_slice_mut(&bump, &mut body)
                    .expect("corpus shapes parse");
                std::hint::black_box(request.request_type());
            }
            (StackStage::Parse | StackStage::Drop, None) => std::mem::forget(
                QueryRequest::from_json_slice_mut(&mut body).expect("corpus shapes parse"),
            ),
            (StackStage::Parse | StackStage::Drop, Some(request)) => drop(request),
        })
        .expect("probe thread spawns")
        .join()
        .expect("probe thread finishes");
}

fn stack() {
    println!(
        "\n## stack (deep_chain/{} = deepest accepted request)\n",
        testing::MAX_DEEP_CHAIN_STEPS
    );
    println!("| stage | smallest stack KiB |\n|---|---:|");
    let exe = std::env::current_exe().expect("benchmark binary path");
    let fits = |stage: StackStage, kib: usize| {
        Command::new(&exe)
            .env(STACK_PROBE, format!("{}:{kib}", stage.name()))
            .output()
            .expect("probe process runs")
            .status
            .code()
            .is_some_and(|code| match code {
                0 => true,
                OVERFLOWED => false,
                other => panic!("stack probe failed with status {other}"),
            })
    };
    for stage in StackStage::ALL {
        // Binary search in 16 KiB steps between 16 KiB and 16 MiB.
        let (mut low, mut high) = (1_usize, 1_024_usize);
        if !fits(stage, high * 16) {
            println!("| {} | > {} |", stage.name(), high * 16);
            continue;
        }
        while low < high {
            let middle = (low + high) / 2;
            match fits(stage, middle * 16) {
                true => high = middle,
                false => low = middle + 1,
            }
        }
        println!("| {} | {} |", stage.name(), low * 16);
    }
}

fn main() {
    match std::env::var(STACK_PROBE) {
        Ok(probe) => stack_probe_child(&probe),
        Err(_) => {
            support::print_environment();
            sizes();
            parse();
            parameters();
            lifecycle(Retention::HeldUntilExecution);
            lifecycle(Retention::FreedEarly);
            stack();
        }
    }
}
