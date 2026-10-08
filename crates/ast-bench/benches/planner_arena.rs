//! How much any planner arena could save.
//!
//! Every arm plans one workload and frees the result. `heap` allocates
//! through mimalloc, the server's allocator. `arena` serves every allocation
//! planning makes from a per-thread bump region that is reset after each plan
//! ([`support::scoped_bump`]), so allocating is a pointer bump and freeing is
//! free: the upper bound of any planner arena. An arena that holds only
//! planning's temporaries would still build the returned plan on the heap;
//! `plan_clone` times cloning a plan on the heap and freeing the clone, so
//! `arena` + `plan_clone` estimates that design. (The plan shares its
//! resolved predicates and expressions through `Arc`s, which a clone only
//! counts, so the estimate is a lower bound.)
//!
//! Workloads are the plannable corpus shapes against an empty catalog and
//! the planner's own scalability fixtures with their catalogs. Every thread
//! plans its own copy of each workload, as every request has its own planner
//! context; a lookup by name precedes each plan in every arm.

use divan::Bencher;

mod support;

use support::scoped_bump;

#[global_allocator]
static ALLOC: scoped_bump::ScopedBump = scoped_bump::ScopedBump;

const THREADS: [usize; 2] = [1, 0];

fn main() {
    support::print_environment();
    // Plan everything once on the heap, so process-wide state planning
    // initializes lazily is not allocated in a region, then once in a
    // region, which proves each workload frees everything it allocates.
    support::plan_inputs().iter().for_each(|(_, input)| {
        drop(input.plan());
        scoped_bump::scope(|| drop(input.plan()));
    });
    divan::main();
}

#[divan::bench(args = support::plan_input_names(), threads = THREADS, max_time = 1)]
fn heap(bencher: Bencher, name: &str) {
    bencher.bench(|| {
        support::with_thread_plan_input(name, |input| drop(divan::black_box(input.plan())));
    });
}

#[divan::bench(args = support::plan_input_names(), threads = THREADS, max_time = 1)]
fn arena(bencher: Bencher, name: &str) {
    bencher.bench(|| {
        support::with_thread_plan_input(name, |input| {
            scoped_bump::scope(|| drop(divan::black_box(input.plan())));
        });
    });
}

#[divan::bench(args = support::plan_input_names(), threads = THREADS, max_time = 1)]
fn plan_clone(bencher: Bencher, name: &str) {
    let planned = support::plan_input(name).plan();
    bencher.bench(|| drop(divan::black_box(planned.clone())));
}
