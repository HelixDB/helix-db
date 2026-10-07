# Arena-backed native request parsing: prototype and benchmark

Measured 2026-10-07.

**Verdict.** Arena parsing works and is proven equivalent to owned parsing. Measured on its own:

- single-threaded parsing is 1.05–1.4× faster;
- freeing a parsed tree costs a few chunk frees whatever the tree's size. That is 3× faster than dropping a
  1 KB request's owned tree and 240× faster for a 1,000-entry batch;
- parsing makes a handful of allocations instead of one per node (14,018 → 12 for a 1,000-entry batch);
- arena parsing scales almost linearly across cores. On a 1,000-entry batch, scaling efficiency at 14
  threads is 0.93 for the arena against 0.68 for owned parsing on the system allocator.

None of that moves the front end much. For typical requests parsing is 3–13% of the time to parse and plan
a request, so the arena saves only **0.9–2.8%** of that front-end CPU. Bulk writes save up to 7%, and most
of that is the nesting check, which can be dropped without an arena. The arena also does not reduce
retained memory. Go/no-go criteria D1 and D3 fail, so moving the planner onto arena types is **not
recommended yet**.

The larger wins are:

- the two fixes this branch already makes, 4× faster bulk-write parsing and half the heap held during
  execution;
- three more it reports: the planner's parameter copies (F1), mimalloc (F4) and SIMD on x86_64 (F5).

The complete numbers are in
[ast_arena_prototype_benchmark_results.json](ast_arena_prototype_benchmark_results.json).

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
- **No Linux or x86_64 machine was available.** Every number is from macOS on aarch64; see Limitations.

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

## 7. Go/no-go

| Criterion | Threshold | Result | |
| --- | --- | --- | --- |
| D1 CPU | ≥15% of front-end CPU on 2+ shapes | 0.9–2.8% typical, ≤7% bulk | **fail** |
| D2 scaling | arena holds E ≥ 0.9 where mimalloc doesn't | parse: arena 0.93 against mimalloc 0.87 (`wide_batch/1000`); the full front end is planning-bound | partial |
| D3 memory | ≥30% lower lifecycle peak beyond the P3 fixes | retained 0.9–1.5× owned; peak set by planning | **fail** |
| D4 single parser | arena + `into_owned` ≤ owned parse | from 15% faster (`dynamic-read`, 728 ns against 853 ns) to 4% slower (`predicate_heavy/1024`) | roughly met, no gain |
| D5 correctness | zero divergences | zero (29k edits, 3.8M fuzz executions) | **pass** |
| D6 backend | simd-json ≥15% faster under production flags | up to 25% faster on aarch64 (up to 60% with reused buffers); x86_64 unmeasured | open |

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

## 8. Findings, ranked by expected throughput impact

1. **F1. The planner deep-copies every parameter, even unused ones.** A count query carrying 8 MiB of
   unused parameters plans in 47 ms, against 12 µs with none. That is 4,000× slower, and it adds 107 MiB
   of transient heap. Copies happen at:
   - `optimizer/config.rs:37`, once per planning session;
   - `planning/selected/native/terminal/expr.rs:42`, once per cardinality terminal;
   - `rules/access/pipeline/mod.rs:94` and `rules/root/stream_access.rs:69`, once per rule rewrite.

   Sharing them (`Arc<ParamBindings>`) or borrowing would remove most of the bulk-write peak (80 MiB peak
   against 32 MiB live).
2. **F4. Switch the server's global allocator to mimalloc.** Parse + plan throughput at 14 threads rises
   1.2–2.6×, and single-threaded planning speeds up 1.2–1.6×. It is a one-line change, though it adds a C
   dependency to the image.
3. **Done on this branch.** Lazy parameter paths (4× faster bulk parsing) and early freeing (half the heap
   held during execution).
4. **F2. Skip `check_nesting` for parsed JSON.** It costs 1–4 ms per bulk request and is redundant once
   the 255-level text scan has run. A type-level "parsed from bounded JSON" proof can let
   `query_service` skip it.
5. **F5. sonic-rs runs without its x86 SIMD fast path in the production image.** Measure simd-json with
   runtime detection, or build with `+avx2,+pclmulqdq` on a known CPU baseline. Note that `x86-64-v3`
   alone does not include `pclmulqdq`.
6. **F6. Bulk float parameters cost 24–32 bytes per value plus one allocation per row.** A dense typed
   vector parameter (4 bytes per `f32`) would cut bulk-insert parameter memory by roughly 8× and remove
   most of that parse time, which neither the arena nor a JSON backend can do.
7. **F3. The embedded `query_json_scoped(&[u8])` has no body-size cap** and borrows the body through
   execution. **F8.** The Rust SDK's embedded path serializes the request and then parses it again.
8. **F7. Possible follow-up arenas.** A planner scratch arena, and an arena for the Cypher syntax tree
   (`crates/cypher/src/syntax.rs`).
9. **F9. A parameter arena needs exact length hints.** simd-json provides them; sonic-rs does not. Without
   them, large arrays waste up to 2× through doubling inside the arena (see §2).

## Limitations

- **One machine.** Every number is from one aarch64 macOS machine. glibc malloc (production) and x86_64
  SIMD paths may change the scaling and backend conclusions; the allocator-heavy results especially should
  be re-run on the production host.
- **Efficiency cores.** Thread counts above 10 include efficiency cores.
- **Divan's thread synchronisation.** It limits what the sub-microsecond scaling results can show.
- **Memory counts requested bytes, not RSS.**
