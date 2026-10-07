//! Heap and stack a native request costs from body to plan.
//!
//! A calling-thread counting allocator (the `db` crate's
//! `allocation_testing` pattern) records every allocation request, so the
//! numbers are requested bytes, independent of allocator overhead and RSS.
//! Sections:
//!
//! - `sizes`: in-memory size of the AST's core types.
//! - `parse`: what parsing each shape allocates, its peak, what it retains,
//!   and that dropping the request returns every byte.
//! - `lifecycle`: live heap at each `query_service` stage boundary and the
//!   peak across the request, in today's stage order.
//! - `stack`: the smallest thread stack that parses and drops the deepest
//!   accepted chain, found by re-running this binary in a child process,
//!   because a stack overflow aborts the process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::process::Command;

use helix_ast::query::QueryRequest;
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

// SAFETY: Every operation forwards its caller's valid arguments to System
// unchanged; counting is allocation-free thread-local arithmetic.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc`'s contract.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size(), 0);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size(), 0);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::realloc`'s contract.
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            record(size, layout.size());
        }
        resized
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(0, layout.size());
        // SAFETY: The caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { System.dealloc(pointer, layout) };
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

#[derive(Clone, Copy, Debug)]
enum Backend {
    Sonic,
    SimdJson,
}

impl Backend {
    const ALL: [Self; 2] = [Self::Sonic, Self::SimdJson];

    fn name(self) -> &'static str {
        match self {
            Self::Sonic => "sonic",
            Self::SimdJson => "simd_json",
        }
    }

    /// Parse `body` as the transport would; simd-json rewrites it in place.
    fn parse(self, body: &mut [u8]) -> QueryRequest {
        match self {
            Self::Sonic => QueryRequest::from_json_slice(body).expect("corpus shapes parse"),
            Self::SimdJson => QueryRequest::from_json_slice_mut(body).expect("corpus shapes parse"),
        }
    }
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
        "| shape | backend | body MiB | allocations | allocated MiB | peak MiB | retained MiB | retained/body | leaked bytes |"
    );
    println!("|---|---|---:|---:|---:|---:|---:|---:|---:|");
    for shape in support::shapes() {
        for backend in Backend::ALL {
            let mut body = shape.json.clone();
            let (request, parsed) = observe(|| backend.parse(&mut body));
            let ((), dropped) = observe(|| drop(request));
            let leaked = parsed.live + dropped.live;
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {:.2} | {leaked} |",
                shape.name,
                backend.name(),
                mib(shape.json.len() as isize),
                parsed.allocations,
                mib(parsed.bytes as isize),
                mib(parsed.peak),
                mib(parsed.live),
                parsed.live as f64 / shape.json.len() as f64,
            );
        }
    }
}

/// Today's `query_service` order: the body lives until the handler returns,
/// the planner context gets a copy of the parameters, and the batch lives
/// until execution ends.
fn lifecycle() {
    println!("\n## lifecycle (today's stage order, sonic)\n");
    println!(
        "| shape | body | +parse | +check_nesting | +bindings | +context copy | +plan | peak | after drop |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for name in support::plannable_shape_names() {
        let json = &support::shape(name).json;
        let (boundaries, count) = observe(|| {
            let body = json.clone();
            let after_body = live();
            let request = QueryRequest::from_json_slice(&body).expect("corpus shapes parse");
            let after_parse = live();
            request.check_nesting().expect("corpus shapes are bounded");
            let after_check = live();
            let (batch, parameters) = request.into_query();
            let params = helix_ast_bench::param_bindings(parameters);
            let after_bindings = live();
            let context = helix_ast_bench::planner_context(params.clone());
            let after_context = live();
            let planning = helix_planner::planning::plan_with_diagnostics(&batch, &context)
                .expect("plannable shapes plan");
            let after_plan = live();
            // Execution would run here with all of these still alive.
            drop((body, batch, params, context, planning));
            [
                after_body,
                after_parse,
                after_check,
                after_bindings,
                after_context,
                after_plan,
            ]
        });
        let [body, parse, check, bindings, context, plan] = boundaries.map(mib);
        println!(
            "| {name} | {body} | {parse} | {check} | {bindings} | {context} | {plan} | {} | {} |",
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
}

impl StackStage {
    fn name(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::Drop => "drop",
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
    let mut parts = probe.split(':');
    let (Some(backend), Some(stage), Some(kib)) = (parts.next(), parts.next(), parts.next()) else {
        panic!("malformed probe {probe}");
    };
    let backend = Backend::ALL
        .into_iter()
        .find(|candidate| candidate.name() == backend)
        .expect("known backend");
    let stage = [StackStage::Parse, StackStage::Drop]
        .into_iter()
        .find(|candidate| candidate.name() == stage)
        .expect("known stage");
    let kib = kib.parse::<usize>().expect("stack size in KiB");
    let mut body = testing::deep_chain(testing::MAX_DEEP_CHAIN_STEPS).json;
    let prepared = match stage {
        StackStage::Parse => None,
        StackStage::Drop => Some(backend.parse(&mut body.clone())),
    };
    std::thread::Builder::new()
        .stack_size(kib << 10)
        .spawn(move || match prepared {
            None => std::mem::forget(backend.parse(&mut body)),
            Some(request) => drop(request),
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
    println!("| backend | stage | smallest stack KiB |\n|---|---|---:|");
    let exe = std::env::current_exe().expect("benchmark binary path");
    let fits = |backend: Backend, stage: StackStage, kib: usize| {
        Command::new(&exe)
            .env(
                STACK_PROBE,
                format!("{}:{}:{kib}", backend.name(), stage.name()),
            )
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
    for backend in Backend::ALL {
        for stage in [StackStage::Parse, StackStage::Drop] {
            // Binary search in 16 KiB steps between 16 KiB and 16 MiB.
            let (mut low, mut high) = (1_usize, 1_024_usize);
            if !fits(backend, stage, high * 16) {
                println!(
                    "| {} | {} | > {} |",
                    backend.name(),
                    stage.name(),
                    high * 16
                );
                continue;
            }
            while low < high {
                let middle = (low + high) / 2;
                match fits(backend, stage, middle * 16) {
                    true => high = middle,
                    false => low = middle + 1,
                }
            }
            println!("| {} | {} | {} |", backend.name(), stage.name(), low * 16);
        }
    }
}

fn main() {
    match std::env::var(STACK_PROBE) {
        Ok(probe) => stack_probe_child(&probe),
        Err(_) => {
            support::print_environment();
            sizes();
            parse();
            lifecycle();
            stack();
        }
    }
}
