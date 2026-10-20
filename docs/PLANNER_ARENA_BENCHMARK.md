# Planner time and allocation: arenas measured, targeted fixes kept

Follow-up F7 of [AST_ARENA_PROTOTYPE_BENCHMARK.md](AST_ARENA_PROTOTYPE_BENCHMARK.md). The question was
whether a planner arena would pay off. The answer is no: targeted changes to the planner's data
structures saved far more than any arena could, and are in this branch.

Two platforms were measured. **Graviton4 decides:** it runs Linux, as production does, and a macOS
result has already misled once (see [shared names](#shared-names-and-aarch64-atomics)).

- **AWS Graviton4:** i8g.4xlarge (16 Neoverse V2 cores), Amazon Linux 2023 (glibc 2.34),
  `rustc 1.97.1 (8bab26f4f 2026-07-14)`.
- **Apple M4 Pro:** 10 performance and 4 efficiency cores, macOS, `rustc 1.101.0-nightly`.

Both use the release `bench` profile with mimalloc as the global allocator, as the server does.

## Summary

**Planning is 1.8× faster on Graviton4.** Planning time changed as follows from `7e7c9e15` (before any
planner change in this branch) to `c6878dd4`, over 56 workloads:

| Threads | Median | Smallest gain | Largest gain |
| --- | ---: | ---: | ---: |
| 1 | −45.2% (1.82×) | −30.3% (`predicate_heavy/1024`) | −82.2% (`ManyAvailableIndexes/1024`) |
| 16 | −43.2% (1.76×) | −29.1% (`WideBooleanPredicates/8`) | −81.0% (`ManyAvailableIndexes/1024`) |

Every workload got faster. Allocations per plan fell by a median of 36% (by up to 93%), requested
bytes by 33% (by up to 87%), and the heap peak by 9% (by up to 96%).

**A planner arena is not worth building.** On Graviton4, serving every planning allocation from a bump
region would have saved:

- a median 8.6% on one thread, and 9.9% on 16 threads, before these changes;
- about 4% for an arena that held only temporaries, which is what a real arena could do.

The changes above have since removed a third of the allocations, so the ceiling is lower still. An arena
would also need a lifetime threaded through the memo, the rules, the analysis passes and the IR.

**Shared names (`Arc<str>`) won on macOS and lost on Graviton4, so they were reverted.** On the M4 Pro
they saved a median 7.1%. On Graviton4 the same change cost a median +2.6% on one thread and +3.5% on
16 threads, because every reference-count update on aarch64 Linux is a call to an atomics helper (see
[Shared names and aarch64 atomics](#shared-names-and-aarch64-atomics)). Only the map-free
contradiction check from that commit stayed.

**Recommendations.**

1. Keep the changes in this branch.
2. Don't build a planner arena.
3. Ship the server with the `dist` profile (thin LTO, one codegen unit), which this branch adds; see
   [the build profile](#the-build-profile). `+lse` no longer helps planning.
4. Work through [what remains](#what-remains), measured on Graviton rather than macOS.

## Method

**Benchmarks**, both in `helix-ast-bench`:

- **`planner_alloc`** counts what one plan allocates on the calling thread:
  - allocations, requested bytes and the heap peak;
  - what the returned plan still holds;
  - sampled allocation sites for eight workloads, with up to three planner callers each.

  Build it with line tables so that inlined frames resolve:

  ```bash
  CARGO_PROFILE_BENCH_DEBUG=line-tables-only cargo bench -p helix-ast-bench --bench planner_alloc
  ```

- **`planner_arena`** times one plan plus freeing its result, in three arms:
  - **`heap`:** mimalloc.
  - **`arena`:** `support::scoped_bump`, a global allocator that bump-allocates from a per-thread region
    inside a scope.
  - **`plan_clone`:** cloning a finished plan. `arena` + `plan_clone` estimates an arena that holds only
    temporaries.

  ```bash
  cargo bench -p helix-ast-bench --bench planner_arena -- heap
  ```

**Workloads.**

- The 21 plannable corpus shapes, planned against an empty catalog, as `query_service` plans them.
- The planner's 35 scalability fixtures, each with its own catalog.
- Every thread plans its own copy of each workload, because every request has its own planner context.

**A/B protocol on Graviton4.** For each change:

- The base tree is the commit before the change, and the candidate is the change.
- Both are built in separate target directories.
- The `heap` arm runs three times per tree, alternating base and candidate.
- Each workload's result is the median of its three divan medians.
- The base spread (slowest base run ÷ fastest − 1) shows the noise; it was under 3% for nearly every
  workload.
- A change stayed only if it saved clearly more than the spread somewhere. The one slowdown beyond the
  spread is noted under the per-change table.

The final comparison ran the same way at 1 and 16 threads. Older revisions lack the benchmarks, so both
trees used this branch's `helix-ast-bench`.

Full tables are in [planner_arena_benchmark_results.json](planner_arena_benchmark_results.json), under
`graviton4` and `apple_m4_pro`.

## Results on Graviton4

### Before and after

Planning time from `7e7c9e15` to `c6878dd4`:

| Workload | One thread | 16 threads |
| --- | --- | --- |
| dynamic-read | 30.5 → 16.9 µs (−45%) | 33.6 → 19.4 µs (−42%) |
| dynamic-write | 9.26 → 4.87 µs (−48%) | 10.2 → 5.68 µs (−44%) |
| bulk_write_typed/1000x768 | 10.9 → 6.73 µs (−39%) | 11.9 → 7.30 µs (−39%) |
| ordered-range-wide-projection | 55.9 → 24.9 µs (−55%) | 49.3 → 27.8 µs (−44%) |
| deep_chain/123 | 786 → 251 µs (−68%) | 842 → 256 µs (−70%) |
| string_heavy_projection/256x64 | 257 → 142 µs (−45%) | 271 → 152 µs (−44%) |
| wide_batch/1000 | 30.0 → 11.3 ms (−62%) | 32.1 → 13.1 ms (−59%) |
| predicate_heavy/1024 | 6.91 → 4.82 ms (−30%) | 6.96 → 4.85 ms (−30%) |
| fixture/BatchedRootReuse/256 | 3.74 ms → 793 µs (−79%) | 3.73 ms → 721 µs (−81%) |
| fixture/ForEachBodyRootReuse/256 | 3.35 ms → 680 µs (−80%) | 3.52 ms → 718 µs (−80%) |
| fixture/ManyAvailableIndexes/1024 | 96.3 → 17.1 µs (−82%) | 98.8 → 18.8 µs (−81%) |
| fixture/DeepTraversalChain/32 | 117 → 45.5 µs (−61%) | 122 → 48.7 µs (−60%) |
| fixture/OrderedRangeWindowPushdown/64 | 685 → 285 µs (−58%) | 751 → 291 µs (−61%) |
| fixture/SearchIndexDdlWorkloads/64 | 2.27 → 1.12 ms (−51%) | 2.42 → 1.25 ms (−48%) |
| fixture/OverLimitIndexDisjunction/512 | 4.76 → 2.56 ms (−46%) | 4.85 → 2.69 ms (−44%) |
| fixture/MutationHeavyBatches/64 | 4.62 → 2.52 ms (−46%) | 4.90 → 2.84 ms (−42%) |
| fixture/ManyMemoAlternatives/64 | 402 → 247 µs (−38%) | 408 → 251 µs (−38%) |
| fixture/WideBooleanPredicates/64 | 410 → 268 µs (−35%) | 413 → 270 µs (−35%) |

Allocation per plan:

| Workload | Allocations | Requested | Heap peak |
| --- | ---: | ---: | ---: |
| dynamic-read | 762 → 429 | 102 → 55 KiB | 15.2 → 12.1 KiB |
| ordered-range-wide-projection | 1,389 → 592 | 124 → 66 KiB | 20.9 → 14.4 KiB |
| deep_chain/123 | 12,665 → 4,948 | 2.07 → 1.56 MiB | 116 → 112 KiB |
| wide_batch/1000 | 361,772 → 207,944 | 48.0 → 30.0 MiB | 8.38 → 8.20 MiB |
| predicate_heavy/1024 | 176,581 → 98,755 | 18.9 → 7.5 MiB | 955 → 792 KiB |
| fixture/BatchedRootReuse/256 | 225,980 → 19,095 | 15.2 → 2.0 MiB | 416 → 416 KiB |
| fixture/ManyAvailableIndexes/1024 | 6,784 → 447 | 468 → 61 KiB | 379 → 13 KiB |
| fixture/ManyMemoAlternatives/64 | 9,523 → 7,076 | 1.49 → 0.83 MiB | 123 → 111 KiB |
| fixture/OverLimitIndexDisjunction/512 | 73,649 → 55,075 | 11.6 → 6.6 MiB | 914 → 820 KiB |
| fixture/MutationHeavyBatches/64 | 71,928 → 56,791 | 11.5 → 8.1 MiB | 2.63 → 2.65 MiB |

### Each change

Each row is one commit, measured against its parent. Times are for one thread; the median is over all 56
workloads.

| Commit | Change | Median | Largest gains |
| --- | --- | ---: | --- |
| `c35d6c59` | Executable DAG validation, ordering and pull-region derivation keep per-step state in vectors indexed by dense step position (`StepPositions`), not in `BTreeMap`s keyed by step ID. | −6.4% | `wide_batch/100` −34%, `wide_batch/1000` −32%, `SearchIndexDdlWorkloads/64` −26% |
| `25089ba3` | Catalog index lookups use borrowed `(label, property)` key views instead of building owned keys. | 0.0% | `predicate_heavy/64`, `dynamic-read` and `wide_batch/100` −7% |
| `d97c76f2` | `OptimizerConfig` borrows the index catalog and statistics unless runtime feedback changes them. | −0.5% | `ManyAvailableIndexes/1024` −36%, `/256` −20% |
| `7bd7323f` | The seed optimizer is built once per process; rules are `Send + Sync`. | −1.3% | `dynamic-write` −28%, small writes and counts −22 to −25% |
| `d027924c` | Contradiction analysis keys constraints by borrowed property names. | −1.6% | `ordered-range-wide-projection` −7% |
| `9dc078e7` | Lowering moves pipeline parts into each step instead of cloning the pipeline per step. | −0.3% | `deep_chain/123` −48%, `deep_chain/64` −35%, `DeepTraversalChain/32` −18% |
| `852f1b53`, `2abe3bba` | Digests hash a compact binary serde encoding instead of formatting JSON. Physical tie-breaks keep the JSON digest, so selected plans are unchanged. Executable returns resolve bindings from one index instead of a reverse scan per binding. | −8.2% | `string_heavy_projection` −30%, `wide_batch/1000` −19%, `DeepTraversalChain/32` −16% |
| `88f315f3` | Projection property lists are shared behind an `Arc`. | +0.8% | `ordered-range-wide-projection` −29% |
| `2bc5bdfb` | Index translation extends conjunction branches in place instead of copying them. | −0.9% | `predicate_heavy/64` −7%, `/1024` −6% |
| `bdd6959e` | A batch scopes the planner context once (once per foreach body) instead of cloning it, index catalog included, for every query root. | −1.7% | `ForEachBodyRootReuse/256` −72%, `BatchedRootReuse/256` −67%, `ManyAvailableIndexes/1024` −62% |
| `5146e284` | A physical alternative's tie-break digest is computed only when two costs tie, then cached. | −5.4% | `dynamic-delete` −11%, small writes −8 to −10% |
| `e424d9a0` | The diagnostics analyzer keeps its per-step state by position instead of in SipHash maps. | −1.3% | `wide_batch/1000` −6%, `DeepTraversalChain/32` −6% |
| `0c988d62` | Digests hash struct fields by position instead of by name, marking skipped fields. | −3.2% | `string_heavy_projection` −12%, `DeepTraversalChain/32` −10% |
| `71b6c730` | An OR's literal equality branches are grouped by sorting borrowed keys, not by a quadratic search over cloned ones. | −0.6% | `OverLimitIndexDisjunction/512` −18%, `/128` −9%, `ManyMemoAlternatives/64` −8% |
| `4fbc1b11` | Index rules borrow pruned predicates. Lowering validates filters without building and discarding a `PredicatePlan`. | −3.8% | `BatchedRootReuse/256` −20%, `ForEachBodyRootReuse/256` −13%, `predicate_heavy/64` −8% |
| `2afd3952` | Sets wider than eight sources compare source digests before testing two sources for equality in the subsumption check. | −0.7% | `OverLimitIndexDisjunction/512` −16% |
| `c6878dd4` | A memo expression caches its identity digest, which the memoizer, the memo's insertion and its duplicate check each computed. | −2.0% | `DeepTraversalChain/32` −9%, `string_heavy_projection` −6% |

The largest slowdown any single change measured was +4.3%, on two 7 µs requests under the shared
projection lists, where the base spread was 1.5–2.6%. In the final comparison every workload is faster.

### Rejected

| Change | Result on Graviton4 | Why it was dropped |
| --- | --- | --- |
| Shared names: `NonEmptyString` holding an `Arc<str>` (`7ac71ad2`, reverted in `d37f522d`) | median +2.6% on one thread (up to +13%), +3.5% on 16 threads (up to +74%) | atomics; see below |
| Shared names built with `+lse` | median +1.7% on one thread, +2.3% on 16 threads | still slower than copying short names |
| `Arc`-shared selection internals in `relational/selection.rs` | median −0.2% (−1.6 to +1.9%) | no effect |
| foldhash instead of SipHash for the catalog maps | median −0.3% (−2.7 to +2.6%) | no effect |
| Binary digests for physical tie-breaks too | median −10.9% | changed which of two equal-cost plans won for 100 reviewed Cypher queries. Keeping the JSON digest for tie-breaks (−8.2%) and then computing it only on ties (−5.4%) saved more. |

## Shared names and aarch64 atomics

On `aarch64-unknown-linux-gnu`, Rust compiles every atomic read-modify-write to a call to a helper such
as `__aarch64_ldadd8_rel`. The helper checks at run time whether the CPU has the ARMv8.1 LSE atomic
instructions. macOS assumes those instructions and inlines them, and allocation costs more there. That
is why shared names won on the Mac and lost on Graviton4:

- **The helpers dominated.** With shared names, 15% of Graviton4 planning time was in the
  `__aarch64_ldadd8_*` helpers.
- **Inlining helped, but not enough.** Built with `-C target-feature=+lse`, the helpers disappear, but
  an atomic update on Graviton4 still costs more than mimalloc copying a short name.
- **Contention hurt even with private counters.** With the helper-call build, 16 threads each updating
  only their own counters slowed `dynamic-read` from 33 µs to 57 µs. `+lse` removed that.

**A name as a reference into a per-request arena needs no atomics,** but it would gain little. Making
every planning allocation free saved only 4.7% on `BatchedRootReuse/256`, and nothing (+0.3%) on
`ForEachBodyRootReuse/256`, the two most name-heavy plans. Names are only part of those allocations.
Against that, it would cost:

- a lifetime through about 1,400 uses of `NonEmptyString` in the planner and db;
- an arena that travels with the plan until execution ends, since the plan keeps its names into async
  execution.

**What `+lse` would mean for the server.**

- After this branch, the helpers still take 3.7% of planning time, mostly to count references when
  cloning and dropping the `Arc`s that plans share.
- On the planner alone, `+lse` measured −1.0% to +0.1%.
- The production Dockerfile builds aarch64 with default flags, so any `Arc`-heavy server path pays for
  the helper calls.
- Every Graviton generation from Graviton2 supports LSE. ARMv8.0 cores such as Graviton1 do not.

It is worth measuring on the server itself.

## The arena ceiling

The median change in planning time against `heap`, over the 56 workloads. The first row was measured on
`7e7c9e15`, before this branch's planner changes. The temporaries-only row was measured one commit later,
on `7ac71ad2`:

| | Graviton4, 1 thread | Graviton4, 16 threads | M4 Pro, 1 thread | M4 Pro, 14 threads |
| --- | ---: | ---: | ---: | ---: |
| Bump arena for all of planning | −8.6% (+0.3 to −15%) | −9.9% (+10 to −19%) | −9.9% (−2 to −21%) | −16.5% (−3 to −35%) |
| Arena for temporaries only (arena + `plan_clone`) | about −4% | — | about −6% | — |

**Why no arena:**

- **It saves less than the targeted fixes.** The ceiling is about a tenth of planning time, against the
  45% the targeted fixes saved. Those fixes also removed a third of the allocations the arena would have
  served.
- **It holds far more memory.** An arena that never reuses memory holds every byte planning asks for. On
  the M4 Pro, before these changes, that was 4–25× the heap's peak (median 4.2×). For `wide_batch/1000`
  it was 34 MiB per in-flight plan, against 8 MiB of heap.
- **Thread-local access costs too.** A first bump allocator that read its thread-local twice per call was
  1–16% *slower* than mimalloc on macOS. Any arena reached through a thread-local pays for that access.
- **The benchmark arm is not production-safe.** The global-allocator scope is a measurement tool only:
  - anything planning keeps beyond the call would be freed under it;
  - a pointer freed on another thread would reach mimalloc;
  - a cloned plan still shares the `Arc`s made inside the scope.

## After merging main

Four more planner changes followed. Each was measured against its parent as above, with both sides
rebuilt from clean (see the next section).

| Commit | Change | Largest gains |
| --- | --- | --- |
| `b34ff8fc` | One cache per optimization run holds each filter's index rewrite. The exploration rule and the implementation rules that defer to it had derived the same rewrite about three times per filter (over four in the mixed workloads). | `predicate_heavy` −34 to −38%, `OverLimitIndexDisjunction/128` −10%, `ManyMemoAlternatives/64` −9% |
| `90b4c4e2` | The memoizer compares against the memo's copy of each expression instead of keeping its own. | median −2.6%, `string_heavy_projection` −10% |
| `91857501` | Cache lookups skip serializing a predicate whose allocation the cache already knows, and hits write nothing. | `predicate_heavy` −5 to −6% |
| `50d73a0a` | A source filter's specialized predicate moves into its plan instead of being copied twice more. | `BatchedRootReuse/256` −18%, `ForEachBodyRootReuse/256` −7% |

`b34ff8fc` measured a median −0.1%: most workloads derive few rewrites. Keying the cache by predicate
allocation alone lost hits for predicates that rules rebuild (`deep_chain/123` +8%), so other
allocations still match by digest.

Two measurement problems turned up along the way:

- **Stale candidates.** `rsync` kept the source files' modification times, so a file edited before
  the previous build finished could look older than that build, and cargo skipped it. Every
  candidate is now rebuilt from clean. A stale build makes both sides identical, so it can only hide a
  change, never invent one. Every change kept here showed gains on the workloads it targets, so none
  was affected. The two experiments rejected for no effect may have been. Re-measured with both sides
  built as the server ships, sharing the selection program's slot set saved 5–6.5% on deep chains,
  4% on wide batches and 3% on `dynamic-read` (median −0.8%), but one 5 µs count measured +5.5%
  against a 2.1% spread. It is not in this branch; it is the first follow-up below.
- **Codegen-unit noise.** `release` compiles each crate in 16 codegen units. An edit anywhere can move
  functions between units and change inlining in unrelated hot code: `deep_chain/123` flipped
  between about 231 and 250 µs across builds whose planning code for it was identical, and executed
  3% more instructions in one of them. Since the build-profile measurements below, A/B builds use one
  codegen unit and thin LTO on both sides, as the server ships; the selection re-measurement was the
  first such run.

## The build profile

The same source, built three ways, against `release` (single thread, three alternating runs):

| Profile | Median planning time | Range | Clean server build, 16 cores | Server binary |
| --- | ---: | --- | ---: | ---: |
| `release` (16 codegen units, no cross-crate LTO) | — | — | 157 s | 68.6 MB |
| One codegen unit | −5.2% | −11.1 to +0.7% | — | — |
| Thin LTO, one codegen unit (`dist`) | −8.0% | −13.2 to −2.4% | 292 s | 47.2 MB |
| Fat LTO, one codegen unit | −8.9% | −13.9 to −2.7% | 424 s | 43.6 MB |

At 16 threads, fat LTO measured −8.2%, and −7.2% with `+lse` as well, so inlined atomics no longer
help. The server image now builds with `dist`. Local and benchmark builds keep `release`.

## What remains

A Graviton4 profile of `c6878dd4` (one thread, all workloads), as shares of planning time. The index
rewrite cache since removed most of the access-filter work:

| Where | Share | Possible fix |
| --- | ---: | --- |
| Allocation and freeing (mimalloc and Rust's allocation shims) | 23% | Keep removing clones; the remaining volume is spread thinly. |
| Drop glue | 12% (inclusive) | Fewer intermediate trees, as above. |
| Access-filter rules | 23% (inclusive), 19% in `required_index_access_filter` | Three rules (`RootStreamAccessRewriteRule`, `AccessSourceIndexFilterRule`, `StreamProjectImplementationRule`) each run `required_index_access_filter`. Caching its result per filter would share that work. |
| Digests | 9% (inclusive) | Most now hash each memo expression once. Expressions still nest whole access pipelines, so the hashing grows with pipeline length. |
| Diagnostics analyzer | 6% (inclusive) | Runs on every plan; computing its statistics during lowering would avoid a second walk. |
| Executable validation, ordering and pull regions | 5% (inclusive) | Share `StepPositions` between validation and derivation instead of rebuilding them. |
| Outlined atomics | 4% | `+lse`; see above. |
| Static predicate analysis (`static_predicate_value`, `label_scope`) | 3% | Recomputed by each pruning call; cache per predicate. |
| Subsumption checks over set sources | 3% | Still quadratic in set width; group sources by kind and label first. |

Measured but not yet in the branch: sharing `SelectionProgram`'s expression and slot set behind one
`Arc` (`relational/selection.rs`), so cloning a predicate stops copying its slot set. See
[after merging main](#after-merging-main) for its numbers.

## Changes in this branch

**Kept:**

- `crates/planner/src/exec/positions.rs`: `StepPositions`, and position-indexed state in:
  - `exec/validation` (with a CSR dependents list and a ready-set bitset);
  - `exec/pull`;
  - the diagnostics analyzer.
- `crates/planner/src/catalog/property.rs`: `ScopedPropertyKeyView` and `ScopedPropertyDirectionKeyView`,
  borrowed key views for catalog lookups.
- `crates/planner/src/optimizer/config.rs`: `OptimizerConfig<'ctx>` borrows the catalog and stats through
  `Cow`.
- `crates/planner/src/rules/registry.rs`: `SeedRuleSet::shared_optimizer()`, a process-wide seed
  optimizer.
- `crates/planner/src/digest.rs`: a binary serde digest serializer. Struct fields are hashed by
  position. `PlanDigest::for_tie_break` keeps the JSON digest for physical alternatives.
- `crates/planner/src/physical/alternative.rs` and `optimizer/ordering.rs`: lazy tie-break digests and
  `compare_alternatives`.
- `crates/planner/src/planning/selected/native/batch`: the scoped planner context.
- `crates/planner/src/ir/access.rs`: digest-filtered subsumption for wide sets.
- `crates/planner/src/memo/expression.rs`: memo expressions cache their identity digest.
- `crates/planner/src/rules/access/filter/index/cache.rs`: the per-run index rewrite cache.
- `Cargo.toml` and `Dockerfile`: the `dist` profile the server image builds with.
- Smaller moves and borrows in:
  - `exec/returns.rs`;
  - `ir/projection/property.rs`;
  - `logical/access`;
  - `rules/access/filter`;
  - `analysis/scalar`;
  - `ir/expr/predicate.rs` (`PredicatePlan::validate`).
- `crates/ast-bench`: the `planner_alloc` and `planner_arena` benchmarks, the `scoped_bump` allocator, and
  per-thread workload copies.

**Reverted:** the shared names of `7ac71ad2`. The map-free contradiction check from that commit stays.
