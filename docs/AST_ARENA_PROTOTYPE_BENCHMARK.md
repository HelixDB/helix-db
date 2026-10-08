# Native request parsing and planning: arena prototype, Graviton4 results and follow-ups

Measured on an Apple M4 Pro (macOS) and an AWS i8g.8xlarge (Graviton4, Linux).

## Summary

**Arena parsing.** Arena parsing works and is proven equivalent to owned parsing. Measured on its own:

- it parses 1.05–1.4× faster;
- freeing a parsed tree costs a few chunk frees whatever its size;
- it makes a handful of allocations instead of one per node.

Parsing is only 3–13% of the time to parse and plan a typical request, so it saves 1–3% of that front end.
It is **not wired into the planner**; Part 2 has the evidence and the migration sketch.

**What moved the numbers** were six fixes the benchmarks pointed to, plus mimalloc as the server's
allocator. Measured on Graviton4, from `main` to this branch:

| Request | Front end (parse + check + plan), main → branch | Peak heap, main → branch |
| --- | --- | --- |
| `dynamic-read` (1 KB read) | 47.7 → **32.6 µs** (1.46×) | 0.017 → 0.016 MiB |
| `ordered-range-wide-projection` | 88.1 → **54.2 µs** (1.62×) | 0.031 → 0.026 MiB |
| `wide_batch/1000` (1,000 lookups) | 36.9 → **30.8 ms** (1.20×) | 10.1 → 9.7 MiB |
| bulk insert, 1,000 rows × 768 floats (9.1 MiB) | 45.5 → **30.5 ms** (1.49×) | 113.3 → **33.2 MiB** (−71%) |
| bulk insert, 10,000 rows × 96 floats | 66.4 → **42.8 ms** (1.55×) | 165.2 → **47.8 MiB** (−71%) |
| count carrying 8 MiB of unused parameters | 159 → **24.7 ms** (6.45×) | 171.2 → **43.2 MiB** (−75%) |

These figures were measured with sonic-rs. The branch has since switched to simd-json (see "JSON backend"
below), which raises the bulk peaks again. The heap held while the bulk insert executes fell from 65.1 to 24.1 MiB. With 32 cores each running parse
+ plan + free, throughput rose 1.36–1.83×: `dynamic-read` 58.8 → 39.1 µs per request, and the bulk insert
63.2 → 34.7 ms.

Peak heap figures are requested bytes. "main" is `main`'s stage order: the body and AST are held through
execution and the parameters are copied. Front-end times are single-threaded medians of three runs.

**JSON backend.** JSON is now parsed with simd-json instead of sonic-rs (`b74a1a4b`), and serialised
with serde_json. simd-json widens every `f32` to `f64` digits when it serialises (`0.9` becomes
`0.8999999761581421`): a batch of 1,100 embeddings of 768 values grew past the 16 MiB body limit, so
requests and responses are written with serde_json, which keeps the shortest form, as sonic-rs did.
This was a decision, not something the benchmarks below asked for. simd-json picks AVX2, SSE4.2, NEON or a
portable path at runtime, so one generic x86_64 image runs SIMD code, which closes F5. What it costs:

- **Parsing.** Up to 1.4× faster on small and structural requests, but 5–18% slower on float-heavy bodies
  (Graviton4).
- **Peak heap on large bodies.** simd-json first builds a tape of 24 bytes per JSON value. It also copies
  the body twice (an aligned input buffer and a string buffer), and the tape grows by doubling. Requested
  bytes across the whole front end, sonic-rs (`9b75409c`) → simd-json (`b74a1a4b`):

  | Request | Peak heap |
  | --- | --- |
  | bulk insert, 1,000 rows × 768 floats | 33.2 → **73.5 MiB** |
  | bulk insert, 10,000 rows × 96 floats | 47.8 → **95.3 MiB** |
  | count carrying 8 MiB of unused parameters | 43.2 → **75.2 MiB** |
  | `wide_batch/1000` | 9.7 → 9.7 MiB |
  | `predicate_heavy/1024` | 1.13 → 1.20 MiB |
  | small reads and writes | at most 8 KiB more (`ordered-range-wide-projection`: 0.025 → 0.033 MiB) |

  The tape and scratch buffers are freed before planning, so the heap held during execution is unchanged
  (24.1 MiB for the bulk insert).
- **Stack.** Less: the deepest accepted request parses in 144 KiB, against 272 KiB. Unoptimized builds need
  more than 2 MiB on the deepest planner fixtures, so that test runs on an 8 MiB thread.
- **Behaviour.** Integers outside i64/u64 still parse as f64 (simd-json's `big-int-as-float`). A struct
  written in sequence form may now carry extra trailing elements (§6).

HTTP and gRPC parse a uniquely owned body in place and copy a shared one. The embedded `query_json(&[u8])`
copies.

The complete numbers are in
[ast_arena_prototype_benchmark_results.json](ast_arena_prototype_benchmark_results.json), under `linux`,
`simd_json_switch` and the macOS top level.

## Part 1. Follow-up optimisations, measured on Graviton4

### Changes

| Commit | Change | Effect on Graviton4 |
| --- | --- | --- |
| `c09f74ab` | Parameter validation renders a path only for the error it reports, instead of one `format!` per array element. | Bulk insert parse 4× faster (measured on macOS before Linux runs). |
| `8a6923a1` | HTTP and gRPC free the body once parsed. `query_service` frees the AST once planned, and planning and execution share one parameter copy. | Bulk insert heap held during execution halves. |
| `262f870e` | The planner shares request parameters (`SharedParamBindings`, one `Arc`) instead of copying them into every cardinality expression and rewrite. Memo digests stop serializing them. | Planning the bulk insert: 12.5 ms → 73 µs. Planning the 8 MiB count: 132 ms → 78 µs. Its transient planning heap is gone. |
| `5dbfe0c3` | Parameter nesting is bounded when parameters are inserted, now counting a typed parameter's declared array levels, so `check_nesting` walks only the batch. | `check_nesting` on bulk requests: 2.6–3.1 ms → about 2 µs. |
| `e46d3a23` | Parameter arrays keep exactly their length: they reserve from a length hint and shrink after parsing. | Bulk insert retained heap: 31.9 → 24.1 MiB (−24%). The 8 MiB count: −33%. |
| `577956d7` | The seed rule registry is validated once per process, not on every optimisation. | Planning small reads 16–34% faster on macOS. |
| `9b75409c` | The server allocates through mimalloc. | See "Allocator" below. |
| `a1571d5a` | A test from `main` that overflowed the 2 MiB test stack on aarch64 Linux debug builds now runs on 8 MiB. | — |
| `b74a1a4b` | simd-json replaces sonic-rs across the workspace. | See "JSON backend" in the summary. |

The `SharedParamBindings` change keeps `PlannerContext`'s wire format unchanged. All 1,438 planner tests
pass unchanged, including the plan-shape ones, so leaving parameters out of memo identity changed no plan.
The Cypher transfer test still proves that execution receives the original parameter allocation with no
copy, and a new planner test proves the same for native counts.

### Front end, per request, single thread (median of 3 runs)

`main` is `7630cd43` (the arena commits) with glibc malloc. The branch is `9b75409c` with mimalloc.

| Shape | Parse | `check_nesting` | Plan | Front end |
| --- | --- | --- | --- | --- |
| dynamic-read | 1.46 → 1.49 µs | 83 → 45 ns | 46.2 → 31.1 µs | 47.7 → 32.6 µs |
| dynamic-write | 1.25 → 1.25 µs | 78 → 43 ns | 18.1 → 9.52 µs | 19.4 → 10.8 µs |
| ordered-range-wide-projection | 7.81 → 6.99 µs | 109 → 98 ns | 80.2 → 47.1 µs | 88.1 → 54.2 µs |
| deep_chain/123 | 23.9 → 21.3 µs | 837 → 525 ns | 1.02 ms → 801 µs | 1.05 ms → 823 µs |
| wide_batch/1000 | 1.75 → 1.50 ms | 41 → 30 µs | 35.1 → 29.3 ms | 36.9 → 30.8 ms |
| predicate_heavy/1024 | 411 → 367 µs | 10.6 → 10.8 µs | 9.09 → 6.88 ms | 9.51 → 7.26 ms |
| bulk insert 1000 × 768 | 30.3 → 30.4 ms | 2.58 ms → 2.3 µs | 12.5 ms → 73 µs | 45.5 → 30.5 ms |
| bulk insert 10000 × 96 | 43.5 → 42.7 ms | 3.13 ms → 2.4 µs | 19.8 ms → 73 µs | 66.4 → 42.8 ms |
| count + 1 MiB unused params | 2.99 → 3.00 ms | 345 µs → 0.4 µs | 14.6 ms → 33 µs | 18.0 → 3.03 ms |
| count + 8 MiB unused params | 24.3 → 24.6 ms | 2.76 ms → 0.5 µs | 132 ms → 78 µs | 159 → 24.7 ms |

- **Bulk inserts are now bound by the parser.** A profile (sonic-rs) shows about 76% of bulk parse time is
  the parser turning numbers into `QueryValue`s; validation and the depth scan take 8% and 6.5%.
- **Small requests are bound by planning.** About 40–50% of their planning time is allocation (macOS
  profile).

### Throughput across 32 cores (median per-request time; E = t(1) / t(32))

| Shape | Parse + plan + free, main | Parse + plan + free, branch |
| --- | --- | --- |
| dynamic-read | 51.6 µs → 58.8 µs at 32 threads (E 0.88) | 33.5 µs → 39.1 µs (E 0.86) |
| ordered-range-wide-projection | 101 → 115 µs (E 0.88) | 55 → 63 µs (E 0.87) |
| wide_batch/1000 | 40.6 → 47.3 ms (E 0.86) | 31.1 → 34.8 ms (E 0.89) |
| bulk insert 1000 × 768 | 53.2 → 63.2 ms (E 0.84) | 32.4 → 34.7 ms (E 0.93) |

- **glibc scales well on Graviton4.** Owned parsing alone scales at E 0.92–0.99. The allocator contention
  seen on macOS mostly does not apply here.
- **The arena's scaling advantage is small on Linux.** On `wide_batch/1000` the arena reaches 0.97–0.99
  and owned parsing 0.95–0.99.

### Memory

Requested heap during the request, from the stage-boundary simulation in `ast_memory`.

| Shape | Held during execution, main → branch | Peak, main → branch |
| --- | --- | --- |
| wide_batch/1000 | 4.75 → 3.02 MiB | 10.1 → 9.7 MiB |
| bulk insert 1000 × 768 | 65.1 → 24.1 MiB | 113.3 → 33.2 MiB |
| bulk insert 10000 × 96 | 93.5 → 35.9 MiB | 165.2 → 47.8 MiB |
| count + 8 MiB unused params | 64.5 → 21.3 MiB | 171.2 → 43.2 MiB |

The bulk insert's peak is now the body plus its parsed parameters, both live during parsing. Nothing else
remains.

### Allocator

The server now uses mimalloc, as requested. One measurement is useful for operating it. The `ast_rss`
workload has 32 threads running parse, plan and free over small reads, a 1,000-entry batch, the bulk
insert and the 8 MiB count.

| Allocator | Throughput | Peak RSS | RSS after the work |
| --- | ---: | ---: | ---: |
| glibc (round 2) | 1,315 requests/s | 1.50 GiB | 1.16 GiB |
| mimalloc, final branch | 1,688 requests/s | 2.29 GiB | 1.4–2.2 GiB |
| mimalloc with `MIMALLOC_PURGE_DELAY=0` | about 870 requests/s | 0.65 GiB | 0.08 GiB |

mimalloc's default holds freed pages for reuse. `MIMALLOC_PURGE_DELAY` is the runtime setting that trades
that throughput for resident memory, if memory ever matters more than throughput.

### JSON backends on Graviton4

Owned parsing, final branch:

| Shape | sonic-rs | simd-json | simd-json, reused buffers |
| --- | ---: | ---: | ---: |
| dynamic-read | 1.49 µs | 1.33 µs | 1.06 µs |
| ordered-range-wide-projection | 6.99 µs | 5.29 µs | 5.09 µs |
| wide_batch/1000 | 1.50 ms | 1.46 ms | 1.44 ms |
| bulk insert 1000 × 768 | 30.4 ms | 32.9 ms | 32.6 ms |
| bulk insert 10000 × 96 | 42.7 ms | 44.7 ms | 44.4 ms |
| count + 8 MiB unused params | 24.6 ms | 29.0 ms | 29.2 ms |

- **simd-json wins on small and structural requests** (up to 1.4×).
- **It loses 5–18% on float-heavy bodies.**
- **simd-json is now the only backend** (`b74a1a4b`), chosen for runtime CPU detection rather than these
  numbers. Its x86_64 paths are still unmeasured.

### Verification on Graviton4

Every check passes on Linux aarch64:

- fmt, workspace clippy, and all-target clippy of the AST, planner and bench crates;
- the helix-ast suites, with and without features;
- the derive and bench crates;
- the planner (1,438 tests);
- the full db lib suite (2,545 tests);
- the server tests and the 73 production contracts.

## Part 2. The arena prototype (macOS investigation)

## What was built

| Commit | Change |
| --- | --- |
| `55db49a7` | `helix_ast::testing` shape corpus: the Docker fixtures, deep chains up to the 255-level JSON limit (123 steps), wide batches, predicate-heavy filters, string-heavy projections, bulk writes (typed and untyped), and counts carrying unused parameters. |
| `f94237a5` | `helix-ast-bench`: `ast_parse`, `ast_throughput` and `ast_memory`. Adds the optional simd-json backend (`QueryRequest::from_json_slice_mut`), which detects the CPU at runtime. |
| `c09f74ab` | Parameter validation renders a path only when it reports an error (previously one `format!` per array element). |
| `8a6923a1` | HTTP and gRPC free the request body after parsing. `query_service` frees the AST after planning, and planning shares the request's parameters with execution instead of deep-copying them. |
| `648728a4` | The arena: `#[derive(ArenaMirror)]` (new `helix-ast-arena-derive` crate) generates a `Copy` mirror of every request type, with a seed-driven deserializer equivalent to serde_derive's. It adds `ArenaQueryRequest::from_json_slice` (sonic-rs) and `from_json_slice_mut` (simd-json), `arena::Map` and `arena::Pool`. |
| `dcf12102`, `7ae453af` | Equivalence suite and the `query_json_arena` fuzz target. |
| `636284f4` | Arena benchmark arms. |

### Design notes

- The arena holds only the AST. Parameters stay owned because they live through execution.
- `ArenaQueryValue` measures what a separate parameter arena would do.
- The AST arena can be freed or reset right after planning.
- Strings are copied into the arena rather than borrowed from the body, so the body is freed as soon as it
  is parsed.
- Every arena allocation is fallible, so `PoolConfig::allocation_limit` gives a per-request memory cap that
  malloc cannot.
- `#![deny(unsafe_code)]` still holds in `helix-ast`.

## Protocol

- **Machine:** Apple M4 Pro, 14 cores (10 performance, 4 efficiency), 48 GiB, macOS 26.5.1.
- **Build:** release `bench` profile, `rustc 1.101.0-nightly (75a75c3e0 2026-09-26)`.
- **SIMD paths that ran:**
  - sonic-rs: aarch64 NEON, chosen at compile time;
  - simd-json 0.18.1: NEON, chosen at runtime.
- **Timing:** divan 0.1.21.
  - Parse and stage benchmarks run for up to 1 s per shape; throughput benchmarks for up to 2 s per shape
    and thread count.
  - Three independent process runs; each reported time is the median of the three per-run medians.
  - Nothing else compiled or ran during the runs.
- **Memory:** a calling-thread counting allocator (the `allocation_testing` pattern) records requested
  bytes. It does not see malloc's per-allocation overhead, so owned-tree figures are understated relative
  to RSS.
- **Stack:** the smallest thread stack that survives, found by binary search in child processes.
- **Linux results are in Part 1.** Every number in Part 2 is from macOS on aarch64.

## 1. Fixes already on this branch

Bulk insert with 768-float embeddings (1,000 rows, a 9.1 MiB body). Times are owned sonic-rs parsing.

| | Before (`f94237a5`) | After |
| --- | ---: | ---: |
| Parse, 1000 × 768 | 67.7 ms | 16.6 ms (4.1×) |
| Parse, 10,000 × 96 | 91.6 ms | 24.9 ms (3.7×) |
| Allocations while parsing, 1000 × 768 | 1,555,917 | 13,024 |
| Heap live during execution, 1000 × 768 | 65.1 MiB | 31.9 MiB |
| Peak heap across the request, 1000 × 768 | 113.3 MiB | 80.1 MiB |
| Heap live during execution, `wide_batch/1000` | 4.75 MiB | 3.02 MiB |

The remaining 48 MiB peak above the live heap happens during planning; see F1.

## 2. Arena parsing on its own

### Parse time

Sonic-rs unless the column says simd-json. The reused columns reset a single arena before each request, as
the pool does.

| Shape | Owned | Arena (reused) | Speedup | Owned simd-json, reused buffers | Arena simd-json, reused | Arena then `into_owned` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| dynamic-read | 853 ns | 625 ns | 1.36× | 545 ns | 463 ns | 728 ns |
| ordered-range-wide-projection | 4.28 µs | 3.56 µs | 1.20× | 3.32 µs | 2.81 µs | 4.41 µs |
| deep_chain/123 | 15.9 µs | 12.1 µs | 1.31× | 11.9 µs | 10.8 µs | 16.0 µs |
| wide_batch/1000 | 963 µs | 796 µs | 1.21× | 776 µs | 683 µs | 958 µs |
| predicate_heavy/1024 | 225 µs | 199 µs | 1.13× | 192 µs | 172 µs | 234 µs |
| string_heavy_projection/256x64 | 34.7 µs | 30.1 µs | 1.15× | 31.8 µs | 28.6 µs | 35.4 µs |
| bulk_write_untyped/1000x768 | 16.6 ms | 16.6 ms | 1.00× | 15.9 ms | 15.8 ms | 16.7 ms |

- A fresh arena per request and a pre-sized arena perform within noise of a reused arena.
- Bulk writes do not change because their cost is the owned parameter values: 1.5M `QueryValue`s and their
  float parsing.

### Free time

| Shape | Drop owned AST | Free arena | Reset arena |
| --- | ---: | ---: | ---: |
| dynamic-read | 92.8 ns | 27.4 ns | 12.8 ns |
| deep_chain/123 | 2.97 µs | 72.7 ns | 69.7 ns |
| wide_batch/1000 | 114 µs | 482 ns | 416 ns |
| predicate_heavy/1024 | 23.0 µs | 190 ns | 181 ns |

Freeing an arena costs one deallocation per chunk. Chunks double in size, so their number grows with the
logarithm of the tree's size.

### Memory and stack

Allocations and requested bytes retained after parsing, sonic-rs.

| Shape | Owned allocations | Arena allocations | Owned retained | Arena chunks (filled) |
| --- | ---: | ---: | ---: | ---: |
| dynamic-read | 11 | 2 | 1.4 KiB | 1.4 KiB (1.4 KiB) |
| deep_chain/123 | 190 | 6 | 35.8 KiB | 30.7 KiB (30.7 KiB) |
| predicate_heavy/1024 | 2,662 | 9 | 0.195 MiB | 0.249 MiB (0.247 MiB) |
| wide_batch/1000 | 14,018 | 12 | 1.369 MiB | 1.999 MiB (1.231 MiB) |

- The arena retains about as much as the owned tree (0.9–1.5×). Most of the difference is chunk-doubling
  slack and abandoned vector growth buffers, since sonic-rs gives no length hints.
- Owned figures leave out malloc's 16-byte size classes and headers.

Stack needed to parse the deepest accepted request:

| | sonic-rs | simd-json |
| --- | ---: | ---: |
| Owned parse | 272 KiB | 144 KiB |
| Arena parse | 208 KiB | 144 KiB |
| Owned drop | 16 KiB | 16 KiB |

Both fit tokio's 2 MiB worker stacks easily. Unoptimized test builds need several times more, which is why
the depth tests run on a larger stack.

### A separate parameter arena

The parameter values from the bodies above, parsed as one JSON value.

| Shape | Owned time | Arena time | Owned retained | Arena chunks (filled) |
| --- | ---: | ---: | ---: | ---: |
| wide_batch/1000 | 1.21 ms | 739 µs | 13.1 MiB | 2.0 MiB (1.7 MiB) |
| bulk_write_untyped/1000x768 | 13.9 ms | 13.4 ms | 31.9 MiB | 32.0 MiB (23.7 MiB) |
| bulk_write_untyped/10000x96 | 20.4 ms | 18.0 ms | 45.9 MiB | 32.0 MiB (31.6 MiB) |
| count_with_unused_params/8388608 | 12.8 ms | 11.4 ms | 32.0 MiB | 64.0 MiB (56.0 MiB) |

- For structured JSON the arena is 1.3–1.6× faster and several times smaller.
- For float arrays, the shape real bulk writes have, it is 1.0–1.1× faster.
- Its memory varies with the shape, from 30% smaller to 2× larger. One huge array grows by doubling inside
  the arena and abandons each outgrown buffer.
- A dense typed vector (4 bytes per `f32`, against 24–32 bytes per value in either representation) would
  beat both; see F6.

## 3. Share of the front end

Owned = parse + `check_nesting` + AST drop. Arena = parse into a reused arena + reset. Saving is the
difference as a share of owned + planning. Execution, which reads storage, is excluded, so real savings are
smaller still.

| Shape | Owned | Plan | Parse share | Arena saving |
| --- | ---: | ---: | ---: | ---: |
| dynamic-read | 1.01 µs | 29.4 µs | 3.3% | 1.2% |
| dynamic-write | 0.84 µs | 10.9 µs | 7.2% | 2.5% |
| ordered-range-wide-projection | 4.92 µs | 46.0 µs | 9.7% | 2.6% |
| deep_chain/123 | 19.9 µs | 482 µs | 4.0% | 1.5% |
| wide_batch/1000 | 1.10 ms | 21.7 ms | 4.8% | 1.3% |
| predicate_heavy/1024 | 253 µs | 5.67 ms | 4.3% | 0.9% |
| string_heavy_projection/256x64 | 38.5 µs | 256 µs | 13.1% | 2.8% |
| bulk_write_untyped/1000x768 | 17.6 ms | 2.62 ms | 87.1% | 5.0% |
| count_with_unused_params/8388608 | 19.4 ms | 47.2 ms | 29.1% | 6.8% |

On the bulk shapes, about 1 ms of each saving is `check_nesting`, which the arena skips. That pass is
redundant for any JSON request, owned or arena (F2).

## 4. Scaling across cores

Per-iteration median time at 1, 8 and 14 threads. E(14) = t(1) / t(14), where 1.0 is perfect scaling.
Threads 11–14 run on efficiency cores, which lowers every E(14).

| Shape | Arm | System malloc t=1 / t=8 / t=14 | E(14) | mimalloc t=1 / t=8 / t=14 | E(14) |
| --- | --- | --- | ---: | --- | ---: |
| wide_batch/1000 | owned sonic, parse + drop | 1.12 / 1.59 / 1.65 ms | 0.68 | 0.99 / 1.09 / 1.14 ms | 0.87 |
| wide_batch/1000 | arena (pooled) sonic | 0.79 / 0.84 / 0.86 ms | 0.93 | 0.80 / 0.84 / 0.84 ms | 0.94 |
| wide_batch/1000 | arena (pooled) simd-json | 0.68 / 0.74 / 0.77 ms | 0.88 | 0.70 / 0.75 / 0.77 ms | 0.91 |
| wide_batch/1000 | owned sonic, parse + **plan** + drop | 24.4 / 33.5 / 37.6 ms | 0.65 | 19.8 / 22.7 / 26.4 ms | 0.75 |
| ordered-range-wide-projection | owned sonic, parse + drop | 5.4 / 12.2 / 11.7 µs | 0.46 | 3.7 / 6.5 / 7.3 µs | 0.50 |
| ordered-range-wide-projection | arena (thread-local) sonic | 3.7 / 7.7 / 7.1 µs | 0.52 | 3.2 / 6.0 / 5.9 µs | 0.55 |
| ordered-range-wide-projection | owned sonic, parse + **plan** + drop | 58 / 102 / 106 µs | 0.55 | 36 / 43 / 46 µs | 0.78 |
| dynamic-read | owned sonic, parse + **plan** + drop | 33.5 / 74.3 / 78.8 µs | 0.43 | 21.9 / 27.4 / 30.4 µs | 0.72 |
| bulk_write_untyped/1000x768 | owned sonic, parse + drop | 18.0 / 21.7 / 26.1 ms | 0.69 | 17.7 / 19.4 / 20.5 ms | 0.86 |

- **The arena removes allocator contention from parsing.** With the system allocator, its scaling
  efficiency stays at 0.93 where owned parsing falls to 0.68. At 14 threads the arena delivers 1.9× the
  owned parse throughput (1.4× on one thread), on `wide_batch/1000`.
- **mimalloc closes most of that gap without the arena.**
- **Planning dominates the front end, and it scales worse than parsing.** mimalloc speeds up planning by
  1.2–1.6× on one thread. At 14 threads, parse + plan throughput is 1.2–2.6× higher with mimalloc. The
  arena cannot reach planning allocations.
- **Sub-microsecond arms are unreliable here.** For requests under 5 µs, divan's per-sample thread
  synchronisation dominates the measurement, so their E values say little about allocators.
- **One row is a warm-up artefact.** The first arm the process ran (fresh arena, bulk shape at t=1) took
  32.9 ms; it is excluded above.

## 5. JSON backends

On this aarch64 machine both parsers use NEON. simd-json is faster on every shape:

| Shape | sonic-rs (owned) | simd-json (owned) | simd-json, reused buffers |
| --- | ---: | ---: | ---: |
| dynamic-read | 853 ns | 795 ns | 545 ns |
| ordered-range-wide-projection | 4.28 µs | 3.58 µs | 3.32 µs |
| wide_batch/1000 | 963 µs | 770 µs | 776 µs |
| predicate_heavy/1024 | 225 µs | 196 µs | 192 µs |
| bulk_write_untyped/1000x768 | 16.6 ms | 16.7 ms | 15.9 ms |

That is up to 1.25× faster with fresh buffers, and 1.3–1.6× on small requests when buffers are reused.
Bulk writes are within ±5% either way. Copying the body for simd-json (the embedded path) costs 1–2%. simd-json's tape doubles peak parse memory (67 MiB against
32 MiB for the 1000 × 768 bulk write).

**Decided in `b74a1a4b`: simd-json.** The rest of this section is the analysis as it stood before.

**The production-relevant comparison was not measured.** sonic-rs enables its x86_64 SIMD fast path only
at compile time (`avx2 + pclmulqdq`). The Docker image sets neither feature, so production x86_64 servers
most likely run sonic-rs's portable fallback, while simd-json would detect AVX2 at runtime.

D6 has to be decided on an x86_64 host by running these commands, each with and without the target
features:

```bash
cargo bench -p helix-ast-bench --bench ast_parse -- parse
```

```bash
RUSTFLAGS="-C target-feature=+avx2,+pclmulqdq" cargo bench -p helix-ast-bench --bench ast_parse -- parse
```

Use a separate `CARGO_TARGET_DIR` for each build.

## 6. Correctness

- **Accepted inputs.** `testing::every_variant` is 145 valid requests: every variant of `AstNode` (80),
  `Predicate`, `Expr`, `PropertyValue` and `IndexSpec`, every batch condition, nested `for_each`, and
  every typed and untyped parameter kind. Exhaustive guards stop the build when a variant is added without
  a sample. Together with every benchmark shape, these parse to identical requests through owned and arena
  parsing on both backends.
- **Rejected inputs.** 29,215 single edits of those requests (2,908 accepted, 26,307 rejected) went
  through four parse paths each. Within each backend, arena and owned parsing returned the same request or
  the same error message, position included. The two backends agreed on every verdict.
- **Hand-written cases** cover:
  - duplicate fields and duplicate parameters;
  - sequence and variant-index forms;
  - numbers at their edges;
  - error order when there is trailing input;
  - mutations at every nested read-batch position;
  - nesting one level past the limit;
  - invalid UTF-8;
  - allocation limits and the pool;
  - Send and Sync;
  - a `Send + 'static` future that holds a parsed tree across an await.
- **Fuzzing.** `query_json_arena` made 3.8M executions in 5 minutes with no failures.
- **Bugs found.** The suite found one bug in the generator: serde says "with 1 element", singular. It also
  pinned one backend difference: simd-json accepts a struct in sequence form with extra trailing elements,
  which sonic-rs rejects. Owned and arena parsing agree within each backend, so the arena is not involved.
- **Since `b74a1a4b`** the suite and the fuzz target compare owned and arena parsing on simd-json alone.

## 7. Go/no-go

| Criterion | Threshold | Result | |
| --- | --- | --- | --- |
| D1 CPU | ≥15% of front-end CPU on 2+ shapes | 0.9–2.8% typical, ≤7% bulk | **fail** |
| D2 scaling | arena holds E ≥ 0.9 where mimalloc doesn't | parse: arena 0.93 against mimalloc 0.87 (`wide_batch/1000`); the full front end is planning-bound | partial |
| D3 memory | ≥30% lower lifecycle peak beyond the P3 fixes | retained 0.9–1.5× owned; peak set by planning | **fail** |
| D4 single parser | arena + `into_owned` ≤ owned parse | from 15% faster (`dynamic-read`, 728 ns against 853 ns) to 4% slower (`predicate_heavy/1024`) | roughly met, no gain |
| D5 correctness | zero divergences | zero (29k edits, 3.8M fuzz executions) | **pass** |
| D6 backend | simd-json ≥15% faster under production flags | up to 25% faster on aarch64 (up to 60% with reused buffers); 5–18% slower on float-heavy bodies on Graviton4; x86_64 unmeasured | simd-json adopted anyway (`b74a1a4b`) |

**Recommendation.** Do not move the planner onto arena types now. That migration would touch about 62
non-test planner files that match on `AstNode` today, plus the builders and about 127 planner test files,
for under 3% of front-end CPU.

Keep the arena on this branch as a proven, measured foundation. It becomes worth wiring up when either of
these is true:

- the planner itself allocates into an arena (a planner scratch arena, F7), so a request's whole front end
  lives in one or two arenas;
- per-request memory caps become a requirement (`allocation_limit`).

The migration sketch:

1. Transports parse into a `PooledBump` taken from a static `Pool`.
2. The planner input layer takes `arena::AstNode<'a>`, which is `Copy`, so match arms barely change.
3. The points where the planner clones `Predicate`, `Expr` or `PropertyValue` into the plan switch to
   `into_owned()`. The plan and the interpreter are unchanged.
4. The arena is reset after planning.

## 8. Findings from the macOS investigation, and their status

1. **F1. Done in `262f870e`.** The planner deep-copied every parameter, even unused ones. A count query carrying 8 MiB of
   unused parameters plans in 47 ms, against 12 µs with none. That is 4,000× slower, and it adds 107 MiB
   of transient heap. Copies happen at:
   - `optimizer/config.rs:37`, once per planning session;
   - `planning/selected/native/terminal/expr.rs:42`, once per cardinality terminal;
   - `rules/access/pipeline/mod.rs:94` and `rules/root/stream_access.rs:69`, once per rule rewrite.

   Sharing them (`Arc<ParamBindings>`) or borrowing would remove most of the bulk-write peak (80 MiB peak
   against 32 MiB live).
2. **F4. Done in `9b75409c`.** Switch the server's global allocator to mimalloc. Parse + plan throughput at 14 threads rises
   1.2–2.6×, and single-threaded planning speeds up 1.2–1.6×. It is a one-line change, though it adds a C
   dependency to the image.
3. **Done on this branch.** Lazy parameter paths (4× faster bulk parsing) and early freeing (half the heap
   held during execution).
4. **F2. Done in `5dbfe0c3`,** by bounding parameters when inserted rather than with a JSON-only proof.
   Skip `check_nesting` for parsed JSON. It costs 1–4 ms per bulk request and is redundant once
   the 255-level text scan has run. A type-level "parsed from bounded JSON" proof can let
   `query_service` skip it.
5. **F5. Closed by `b74a1a4b`.** sonic-rs ran without its x86 SIMD fast path in the production image.
   simd-json detects AVX2 or SSE4.2 at runtime instead.
6. **F6. Open; now the largest remaining bulk-insert lever.** Bulk float parameters cost 24–32 bytes per
   value plus one allocation per row. A dense typed
   vector parameter (4 bytes per `f32`) would cut bulk-insert parameter memory by roughly 8× and remove
   most of that parse time, which neither the arena nor a JSON backend can do.
7. **F3. The embedded `query_json_scoped(&[u8])` has no body-size cap** and borrows the body through
   execution. **F8.** The Rust SDK's embedded path serializes the request and then parses it again.
8. **F7. Planner arena: measured in [PLANNER_ARENA_BENCHMARK.md](PLANNER_ARENA_BENCHMARK.md).** It
   would save at most about a tenth of planning time with mimalloc. Targeted changes to the planner's
   data structures made planning 1.8× faster on Graviton4 instead. An arena for the Cypher syntax tree
   (`crates/cypher/src/syntax.rs`) is still unmeasured.
9. **F9. A parameter arena needs exact length hints.** simd-json provides them; sonic-rs does not. Without
   them, large arrays waste up to 2× through doubling inside the arena (see §2).

Remaining opportunities the Graviton4 profiles point to:

- **Planning.** The planner changes in this branch addressed the planner items these profiles showed:
  - allocation volume;
  - JSON memo digests;
  - `BTreeMap`-keyed plan assembly.

  What is left is listed in [PLANNER_ARENA_BENCHMARK.md](PLANNER_ARENA_BENCHMARK.md#what-remains).

## Limitations

- **No x86_64 host.** Both machines are aarch64, so simd-json's AVX2 and SSE4.2 paths are unmeasured.
- **Efficiency cores.** Thread counts above 10 on macOS include efficiency cores; Graviton4 has none.
- **Divan's thread synchronisation.** It limits what the sub-microsecond scaling results can show.
- **Memory counts requested bytes, not RSS.**
