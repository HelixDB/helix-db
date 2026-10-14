//! What planning allocates, and where.
//!
//! A calling-thread counting allocator (as in `ast_memory`) wraps mimalloc.
//! Sections:
//!
//! - `volume`: per workload, the allocations and bytes one plan requests, its
//!   peak, and how much of it the returned plan still holds. Whatever the
//!   plan does not hold was a temporary, which is what an arena for
//!   planning's scratch state could take over.
//! - `sites`: for a few representative workloads, sampled allocation
//!   backtraces grouped by the innermost planner function (any frame whose
//!   source is under `crates/planner/src`) and by the container that
//!   allocated. Build with line tables so inlined planner frames resolve:
//!
//! ```text
//! CARGO_PROFILE_BENCH_DEBUG=line-tables-only cargo bench -p helix-ast-bench --bench planner_alloc
//! ```

use std::alloc::{GlobalAlloc, Layout};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

mod support;

#[derive(Clone, Copy, Debug, Default)]
struct Count {
    /// Fresh allocations, not counting reallocations.
    allocations: usize,
    reallocations: usize,
    /// Bytes requested by allocations and reallocations.
    bytes: usize,
    live_allocations: isize,
    live: isize,
    peak: isize,
}

/// Every `every`-th allocation's backtrace is kept, with its size.
#[derive(Clone, Copy)]
struct Sampling {
    every: usize,
    seen: usize,
}

thread_local! {
    static COUNT: Cell<Option<Count>> = const { Cell::new(None) };
    static SAMPLING: Cell<Option<Sampling>> = const { Cell::new(None) };
    /// Set while the sampler itself allocates, so it neither counts nor
    /// samples its own allocations.
    static SAMPLER_BUSY: Cell<bool> = const { Cell::new(false) };
    static SAMPLES: RefCell<Vec<(usize, backtrace::Backtrace)>> = const { RefCell::new(Vec::new()) };
}

fn record(allocated: usize, released: usize, fresh: isize) {
    if SAMPLER_BUSY.get() {
        return;
    }
    let Some(mut count) = COUNT.get() else {
        return;
    };
    match (fresh, allocated) {
        (1, _) => count.allocations += 1,
        (0, 1..) => count.reallocations += 1,
        _ => {}
    }
    count.bytes += allocated;
    count.live_allocations += fresh;
    count.live += allocated as isize - released as isize;
    count.peak = count.peak.max(count.live);
    COUNT.set(Some(count));
    let Some(sampling) = SAMPLING.get().filter(|_| fresh == 1) else {
        return;
    };
    SAMPLING.set(Some(Sampling {
        seen: sampling.seen + 1,
        ..sampling
    }));
    if sampling.seen % sampling.every == 0 {
        SAMPLER_BUSY.set(true);
        let trace = backtrace::Backtrace::new_unresolved();
        SAMPLES.with_borrow_mut(|samples| samples.push((allocated, trace)));
        SAMPLER_BUSY.set(false);
    }
}

struct CountingAllocator;

// SAFETY: Every operation forwards its caller's valid arguments to mimalloc
// unchanged. Counting is thread-local arithmetic, and the sampler's own
// allocations re-enter this allocator only to be forwarded uncounted.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc`'s contract.
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size(), 0, 1);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        let pointer = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size(), 0, 1);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::realloc`'s contract.
        let resized = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, size) };
        if !resized.is_null() {
            record(size, layout.size(), 0);
        }
        resized
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(0, layout.size(), -1);
        // SAFETY: The caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Count what `operation` allocates on this thread.
fn observe<T>(operation: impl FnOnce() -> T) -> (T, Count) {
    assert!(COUNT.get().is_none(), "observations cannot nest");
    COUNT.set(Some(Count::default()));
    let result = operation();
    let count = COUNT.take().expect("observation is active");
    (result, count)
}

fn kib(bytes: isize) -> String {
    format!("{:.1}", bytes as f64 / 1024.0)
}

fn volume() {
    println!("## volume (one plan, then freeing it)\n");
    println!(
        "| workload | allocations | reallocations | requested KiB | bytes/allocation | peak KiB | plan holds (allocations) | plan holds KiB | temporaries | leaked bytes |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (name, input) in support::plan_inputs() {
        let (planned, count) = observe(|| input.plan());
        let ((), freed) = observe(|| drop(planned));
        println!(
            "| {name} | {} | {} | {} | {:.0} | {} | {} | {} | {:.1}% | {} |",
            count.allocations,
            count.reallocations,
            kib(count.bytes as isize),
            count.bytes as f64 / (count.allocations + count.reallocations) as f64,
            kib(count.peak),
            count.live_allocations,
            kib(count.live),
            100.0 * (1.0 - count.live_allocations as f64 / count.allocations as f64),
            count.live + freed.live,
        );
    }
}

/// Workloads whose allocation sites are listed: small reads and writes, a
/// wide batch, a deep chain, heavy predicates, and the fixtures that stress
/// the memo and rule exploration.
const SITE_WORKLOADS: [&str; 8] = [
    "dynamic-read",
    "ordered-range-wide-projection",
    "wide_batch/1000",
    "deep_chain/123",
    "predicate_heavy/1024",
    "fixture/ManyMemoAlternatives/64",
    "fixture/BranchHeavyQueries/16",
    "fixture/WideBooleanPredicates/64",
];

/// Backtraces kept per workload.
const SAMPLES_PER_WORKLOAD: usize = 4_000;

/// Allocation sites listed per workload.
const TOP_SITES: usize = 25;

/// Planner frames shown per site: the innermost, then its planner callers.
const CALLER_DEPTH: usize = 3;

/// One resolved frame: its function name, and its source location when
/// known.
struct Frame {
    name: String,
    file: Option<String>,
    line: Option<u32>,
}

/// The standard-library container a planner function allocated through:
/// the outermost frame below it whose source file is one. Inlined frames
/// carry only short names, so source files identify containers reliably.
fn container(frames: &[Frame]) -> &'static str {
    const CONTAINERS: [(&str, &str); 10] = [
        ("alloc/src/collections/btree", "BTreeMap/BTreeSet"),
        ("hashbrown", "HashMap/HashSet"),
        ("alloc/src/collections/vec_deque", "VecDeque"),
        ("alloc/src/sync.rs", "Arc"),
        ("alloc/src/rc.rs", "Rc"),
        ("alloc/src/string.rs", "String"),
        ("alloc/src/fmt.rs", "String (format!)"),
        ("alloc/src/vec", "Vec"),
        ("alloc/src/slice.rs", "Vec"),
        ("alloc/src/boxed", "Box"),
    ];
    frames
        .iter()
        .rev()
        .filter_map(|frame| frame.file.as_deref())
        .find_map(|file| {
            CONTAINERS
                .iter()
                .find_map(|(needle, name)| file.contains(needle).then_some(*name))
        })
        .unwrap_or("other")
}

const PLANNER_SOURCE: &str = "crates/planner/src/";

/// The planner source file `frame` is in, relative to the planner's `src`.
fn planner_file(frame: &Frame) -> Option<&str> {
    let file = frame.file.as_deref()?;
    file.find(PLANNER_SOURCE)
        .map(|start| &file[start + PLANNER_SOURCE.len()..])
}

fn sites() {
    println!("\n## sites (sampled backtraces, scaled to one plan)\n");
    for name in SITE_WORKLOADS {
        let input = support::plan_input(name);
        let (planned, count) = observe(|| input.plan());
        drop(planned);
        let every = count.allocations.div_ceil(SAMPLES_PER_WORKLOAD).max(1);
        observe(|| {
            SAMPLING.set(Some(Sampling { every, seen: 0 }));
            drop(input.plan());
            SAMPLING.set(None);
        });
        let samples = SAMPLES.take();
        let mut by_site = BTreeMap::<(String, &'static str), (usize, usize)>::new();
        let mut by_container = BTreeMap::<&'static str, usize>::new();
        for (bytes, mut trace) in samples {
            trace.resolve();
            let frames = trace
                .frames()
                .iter()
                .flat_map(backtrace::BacktraceFrame::symbols)
                .map(|symbol| Frame {
                    name: symbol
                        .name()
                        .map_or_else(String::new, |name| format!("{name:#}")),
                    file: symbol
                        .filename()
                        .map(|file| file.to_string_lossy().into_owned()),
                    line: symbol.lineno(),
                })
                .collect::<Vec<_>>();
            let planner = frames
                .iter()
                .enumerate()
                .filter_map(|(index, frame)| planner_file(frame).map(|file| (index, frame, file)))
                .collect::<Vec<_>>();
            // The innermost planner frame with its line, then its planner
            // callers, skipping frames of the same function.
            let mut chain = Vec::<String>::new();
            let mut last_name = None;
            for (_, frame, file) in &planner {
                if chain.len() == CALLER_DEPTH {
                    break;
                }
                if last_name == Some(frame.name.as_str()) {
                    continue;
                }
                last_name = Some(frame.name.as_str());
                chain.push(format!(
                    "{} ({file}:{})",
                    frame.name,
                    frame.line.unwrap_or_default()
                ));
            }
            let (site, below) = match planner.first() {
                Some((index, _, _)) => (chain.join(" ← "), &frames[..*index]),
                None => (String::from("(outside the planner)"), &frames[..]),
            };
            let kind = container(below);
            let entry = by_site.entry((site, kind)).or_default();
            entry.0 += every;
            entry.1 += bytes * every;
            *by_container.entry(kind).or_default() += every;
        }
        let mut ranked = by_site.into_iter().collect::<Vec<_>>();
        ranked.sort_by_key(|(_, (allocations, _))| std::cmp::Reverse(*allocations));
        println!(
            "### {name}: {} allocations, one sample per {every}\n",
            count.allocations
        );
        let mut containers = by_container.into_iter().collect::<Vec<_>>();
        containers.sort_by_key(|(_, allocations)| std::cmp::Reverse(*allocations));
        println!(
            "By container: {}\n",
            containers
                .iter()
                .map(|(kind, allocations)| format!(
                    "{kind} {:.0}%",
                    100.0 * *allocations as f64 / count.allocations as f64
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("| share | allocations | KiB | container | innermost planner function |");
        println!("|---:|---:|---:|---|---|");
        for ((site, kind), (allocations, bytes)) in ranked.into_iter().take(TOP_SITES) {
            println!(
                "| {:.1}% | {allocations} | {} | {kind} | `{site}` |",
                100.0 * allocations as f64 / count.allocations as f64,
                kib(bytes as isize),
            );
        }
        println!();
    }
}

fn main() {
    support::print_environment();
    // Plan everything once first, so lazily initialized process-wide state
    // is not counted against the first workload.
    support::plan_inputs()
        .iter()
        .for_each(|(_, input)| drop(input.plan()));
    volume();
    sites();
}
