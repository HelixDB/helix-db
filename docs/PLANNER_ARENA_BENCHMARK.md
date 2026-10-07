# Planner arenas: what they could save, and what saves more

Measured on an Apple M4 Pro (10 performance and 4 efficiency cores, macOS), `rustc 1.101.0-nightly
(75a75c3e0 2026-09-26)`, release `bench` profile, mimalloc as the global allocator, as the server uses.
Follow-up F7 of [AST_ARENA_PROTOTYPE_BENCHMARK.md](AST_ARENA_PROTOTYPE_BENCHMARK.md).

## Summary

**Planning allocates a lot, but almost all of it is short-lived.** Planning a 1 KB read makes 762
allocations. The returned plan holds 36 of them. Across all 56 workloads, 78–100% of planning's
allocations are freed before planning returns (median 92%).

**An arena would save at most about a tenth of planning time.** The test served every allocation planning
makes from a per-thread bump region and reset the region after each plan, so allocating was a pointer bump
and freeing cost nothing:

- **One thread:** a median saving of 9.9% (2–21%).
- **All 14 cores:** a median saving of 16.5% (3–35%).

A real arena would keep the plan it returns on the heap, so it would save less again. mimalloc already
makes allocation cheap. The earlier profile that put malloc and free at 40–50% of small-request planning
predates the switch to mimalloc.

**Two small changes beat that ceiling where planning is slowest.** Both are in the working tree, and the
planner (1,438 tests), its doctests and the db lib suite pass with them.

| Change | Planning time, one thread |
| --- | --- |
| **Shared names.** `NonEmptyString` holds an `Arc<str>`, so cloning a name counts a reference instead of copying it. Each request's index catalog snapshot gets its own copies of the names (`NonEmptyString::detached`). | median −7.1%; −62% on `BatchedRootReuse/256` and `ForEachBodyRootReuse/256`, −59% on `ManyAvailableIndexes/1024`, −26% on `OrderedRangeWindowPushdown/64` |
| **No throwaway map.** The contradiction check for a single atomic predicate stops building a `BTreeMap` for its one property. | a further median −1.5% (up to −5%) |

With both, allocations per plan fall by a median of 26% (by up to 93%), and requested bytes also by 26%
(by up to 49%). Requests with no names to share stay within run-to-run noise: `dynamic-delete` measured
+3% in one paired run and +10% in another.

**Recommendation.**

1. Keep the two changes.
2. Don't build a planner arena now. After the two changes, a real arena that held only planning's
   temporaries would save a median of about 6%. That saving would cost a lifetime threaded through the
   memo, the rules, the analysis passes and the IR, all of which own their data today.
3. Watch the arena's memory. An arena that never reuses memory holds every byte planning ever asked for.
   That is 4–25× what the heap holds at its peak (median 4.2×). For `wide_batch/1000` it is 34 MiB per
   in-flight plan, against 8 MiB of heap.
4. Pursue the remaining allocation sites listed below one by one instead.

## Method

Two benchmark binaries in `helix-ast-bench`.

**`planner_alloc`** counts what one plan allocates on the calling thread:

- allocations, requested bytes, and the peak;
- what the returned plan still holds;
- for eight representative workloads, sampled allocation backtraces grouped by the innermost planner
  function and by the standard container that allocated.

Grouping uses source files, because inlined frames carry only short names. Build it with line tables:

```bash
CARGO_PROFILE_BENCH_DEBUG=line-tables-only cargo bench -p helix-ast-bench --bench planner_alloc
```

**`planner_arena`** times one plan plus freeing its result, in three arms:

- `heap`: mimalloc.
- `arena`: `support::scoped_bump`, a global allocator. Inside a scope it bump-allocates from a 1 GiB
  per-thread region, grows the most recent allocation in place, and pops it when it is freed first. It
  asserts that every region allocation was freed before the scope resets the region.
- `plan_clone`: cloning a finished plan on the heap and freeing the clone. `arena` + `plan_clone`
  estimates an arena that holds only temporaries. It is a lower bound, because the plan shares its resolved
  predicates and expressions through `Arc`s, which a clone only counts.

```bash
cargo bench -p helix-ast-bench --bench planner_arena
```

**Workloads.**

- The 21 plannable corpus shapes, planned against an empty catalog as `query_service` plans them.
- The planner's 35 scalability fixtures, each with its own catalog.

Every thread plans its own copy of each workload, because every request has its own planner context.

**Statistics.** Times are divan medians at 1 and 14 threads. The 14-thread runs include the efficiency
cores and vary by ±10% between runs on this laptop. Full tables are in
[planner_arena_benchmark_results.json](planner_arena_benchmark_results.json).

## Where planning allocates

Per plan, before the two changes → after them:

| Workload | Allocations | Requested | Heap peak | Requested ÷ peak |
| --- | ---: | ---: | ---: | ---: |
| dynamic-read | 762 → 657 | 0.10 → 0.06 MiB | 0.01 → 0.01 MiB | 6.7× → 4.5× |
| ordered-range-wide-projection | 1,389 → 885 | 0.12 → 0.08 MiB | 0.02 → 0.02 MiB | 5.9× → 4.7× |
| deep_chain/123 | 12,665 → 10,280 | 2.07 → 1.88 MiB | 0.11 → 0.11 MiB | 18.3× → 16.8× |
| wide_batch/1000 | 361,772 → 282,772 | 48.0 → 34.5 MiB | 8.38 → 8.15 MiB | 5.7× → 4.2× |
| predicate_heavy/1024 | 176,581 → 155,041 | 18.9 → 9.8 MiB | 0.93 → 0.93 MiB | 20.2× → 10.5× |
| fixture/ManyMemoAlternatives/64 | 9,523 → 3,999 | 1.49 → 0.76 MiB | 0.12 → 0.10 MiB | 12.4× → 7.6× |
| fixture/BatchedRootReuse/256 | 225,980 → 23,691 | 15.2 → 9.7 MiB | 0.41 → 0.38 MiB | 37.4× → 25.5× |
| fixture/ManyAvailableIndexes/1024 | 6,784 → 484 | 0.46 → 0.29 MiB | 0.37 → 0.23 MiB | 1.2× → 1.3× |
| fixture/MutationHeavyBatches/64 | 71,928 → 54,124 | 11.5 → 8.4 MiB | 2.63 → 2.51 MiB | 4.4× → 3.4× |

The last column is how much more memory an arena that never reuses would hold than the heap does.

**Before the changes, names dominated.** Cloning `NonEmptyString`s, on their own or inside
`AtLeast<NonEmptyString>` lists and access plans, made up:

| Workload | Name clones, share of allocations |
| --- | ---: |
| dynamic-read | 13% |
| fixture/BranchHeavyQueries/16 | 20% |
| wide_batch/1000 | 23% |
| fixture/WideBooleanPredicates/64 | over 30% |
| fixture/ManyMemoAlternatives/64 | over 39% |
| ordered-range-wide-projection | 41% |

The next largest site was `atomic_predicate_is_statically_impossible`. It built a `BTreeMap` node of
about 600 bytes for every atomic predicate, which was 12 MiB of the 49 MiB that `wide_batch/1000` requests.

**What remains after the changes:**

| Site | Share of allocations | Possible fix |
| --- | --- | --- |
| `property_literal_value` (`analysis/scalar/extract.rs`) | 10–22% on predicate workloads | It copies each constrained property's name; returning a `&str` borrowed from the predicate removes the copy. |
| Making each name's `Arc` (`NonEmptyString::new`) | 5–10% | Unavoidable while the AST hands names over as `String`s. |
| `ResolvedPredicate` clones in `relational/selection.rs` | 20% on deep chains | These clone a `BTreeMap`. |
| `access_stream_parts` | 19% on deep chains | — |
| `exec/pull` `derive`, `exec/validation/order.rs` `execution_order`, `exec/plan/subplan.rs` | 9–16% on batch-heavy plans | `BTreeMap`-keyed DAG bookkeeping, rebuilt for every plan and subplan. |
| The candidate list in `optimizer/driver/schedule` | 6–9% on small requests | — |

## The arena ceiling

Median change in planning time against `heap`, over the 56 workloads:

| | One thread | 14 threads |
| --- | ---: | ---: |
| Bump arena, before the changes | −9.9% (−2 to −21%) | −16.5% (−3 to −35%) |
| Bump arena, after the changes | −7.7% (−2 to −20%) | −16.9% |
| Arena for temporaries only (arena + `plan_clone`), after the changes | −6.1% (−2 to −19%) | — |
| The two changes, against the original heap | −7.1% (−62 to +10%) | −3.1% |
| The two changes plus a bump arena, against the original heap | −14.8% | — |

- **The arena gains most at high thread counts.** Turning mimalloc's page purging off
  (`MIMALLOC_PURGE_DELAY=-1`) or stretching it to 1 s moved `heap` by no more than the noise, so the gap is
  not mimalloc returning pages to the OS.
- **The arena gains least on big plans.** `wide_batch/1000` and the 256-entry root-reuse fixtures stream
  through 10–50 MiB of fresh memory per plan. mimalloc reuses memory that is still in cache.
- **The thread-local access dominates a naive bump.** A first version of the bump allocator read and wrote
  its thread-local twice per call. On macOS each access is a call through the TLV getter, and that version
  was 1–16% *slower* than mimalloc. Any production arena reached through a thread-local inherits that cost.

## Strategies compared

| Strategy | Time saved, one thread | Memory | Cost | Verdict |
| --- | --- | --- | --- | --- |
| Shared names (`Arc<str>`), with detached catalog snapshots | median 7%, up to 62% | fewer, smaller allocations (`NonEmptyString` shrinks from 24 to 16 bytes) | one type and one snapshot builder | **keep** |
| No throwaway map in the atomic contradiction check | median 1.5%, up to 5% | 12 MiB less requested for `wide_batch/1000` | one function | **keep** |
| Fix the remaining sites above | about 1–5% each, by analogy | less | local to each site | next |
| Typed arena (bumpalo) for planning's temporaries | about 6% | holds 4–25× the heap's peak until reset | a lifetime through the memo, the rules, the analysis passes and the IR | not now |
| Bump the whole planning call through the global allocator | 8–10% (17% on 14 cores) | as above | unsound in production | measurement only |
| Per-thread reusable scratch collections | not measured | retained per thread | per collection | possible, after the targeted fixes |

The global-allocator scope is unsound outside a benchmark:

- anything planning stores beyond the call would be freed under it;
- a region pointer freed on another thread would be passed to mimalloc;
- a cloned plan still shares the `Arc`s it made inside the scope (the benchmark's first copy-out arm
  caught exactly this).

## One hazard of shared names: counters shared between requests

An `Arc` clone writes a reference count. When concurrent requests clone the *same* `Arc`, every core
writes one cache line:

- When the benchmark shared one context between all 14 threads, `ManyAvailableIndexes/64` planned 3.3×
  slower with shared names (109 µs against 33 µs).
- Per-request contexts showed no such effect.

Production builds a fresh `IndexCatalogSnapshot` for every request, but it used to copy the snapshot's
keys from the database's long-lived catalog. With shared names, those copies would share the catalog's
counters across every in-flight request. `RuntimeIndexCatalog::planner_snapshot` therefore gives each request
detached copies of the label and property names (`NonEmptyString::detached`). Nothing else long-lived
feeds planning: there is no plan cache, and parameter names and AST names are converted per request.

Any future shared planner state must hand out detached names the same way.

## Changes in the working tree

- `crates/planner/src/ir/contracts/non_empty_string.rs`: `NonEmptyString` stores an `Arc<str>` and gains
  `detached()`. Its API, ordering, hashing, `Debug` output and serde format are unchanged. `into_string`
  now copies.
- `crates/db/src/config/indexes.rs`: `planner_snapshot` detaches the key names it hands each request.
- `crates/planner/src/analysis/scalar/constraints/collect.rs`: atomic predicates check against one
  `ScalarPropertyConstraint` through a small `ConstraintSink` trait, which the conjunction's map also
  implements.
- `crates/planner/src/exec/op/membership.rs`: drops its `clippy::large_enum_variant` expectation, which
  the smaller `NonEmptyString` no longer triggers.
- `crates/ast-bench`: the `planner_alloc` and `planner_arena` benchmarks, the `scoped_bump` allocator and
  per-thread workload copies.
