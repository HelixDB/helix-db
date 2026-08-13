# Atomic batch deletion implementation report

## Scope and revisions

This stack starts at PR #58 head
`07f67f06923dffd9d5aa73e7b12762e511fca98b` (merge base
`e7baea4d24d2732ff2fa07fb90b4a9db54f865e6`). It preserves the public
deletion APIs, eager transaction atomicity, persisted codecs, lifecycle rules,
and ordered secondary update semantics.

| Stack branch | Benchmarked implementation revision | Delivered contract |
| --- | --- | --- |
| `codex/write-path-08-deletion-bench` | `e915a2d3dc3684243191287c4604e8ea1da87d81` | Release benchmark, typed workload matrix, phase/counter telemetry, raw JSONL, fixed-seed bootstrap comparison |
| `codex/write-path-09-graph-deletion` | `505145be5d51907b86d8ced79d6197ea636f3fbe` | Whole-batch node closure, deduplicated incident edges/pairs, bounded observations, one topology flush |
| `codex/write-path-10-index-deletion` | `53bd7bdd9c271c4f9b4212c1bac83328e5ce8347` | Bounded Active text epochs and `vector -> text` final preparation order |
| `codex/write-path-11-vector-deletion` | `af88b322aaa550682592e467537dd89de19adccc` | Deletion-only Active vector cohorts, visibility barriers, deterministic component repair |

`codex/write-path-12-deletion-publication` was intentionally omitted. Measured
post-commit publication is only 0.3-0.8 microseconds in the target cases, so the
proposed concurrency cannot satisfy the stack's performance gate and would add
coordination risk without measurable benefit.

## Implemented boundaries

- Graph deletion observes sorted chunks, fails closed on malformed topology,
  computes one deduplicated edge closure, and stages final rows in the same
  transaction. Duplicate/missing IDs remain idempotent no-ops.
- Text collection is `Collecting(ActiveTextEpoch) | Prepared`.
  `AdmissibleTextMutation` proves a mutation fits an empty epoch; oversized
  input cannot mutate or drain the current epoch. A 10,000-entity deletion
  drains through twenty 512-entity epochs with zero text payload uploads.
- Active vectors use a non-empty sorted deletion cohort grouped by exact
  generation and tenant partition. Ordinary vector mutation behavior and all
  Building delta behavior remain unchanged. Any later vector mutation or read
  barrier drains pending deletions first.
- HNSW repair snapshots the cohort, processes layers high-to-low and IDs in
  ascending order, removes cohort references before selection, repairs each
  connected frontier once, enforces degree bounds, derives reverse locators
  from final row differences, and stages metadata once. Missing target rows are
  residue cleanup; missing survivor vectors are removal-only repair.
- Secondary deletion coalescing was implemented experimentally and then
  removed because it failed the performance gate. The original ordered runtime
  therefore continues to preserve transient unique-conflict semantics exactly.

## Before/after measurements

Unless marked manual, results are release builds with five independent
serialized process runs and 25 measured samples per run. Fixtures, reopen,
warmup, quiescence, and correctness checks are outside timed phases. Allocation
and RSS cases use the benchmark's single-threaded runtime. `p95` is the pooled
sample percentile; significance uses the fixed-seed 10,000-resample
hierarchical bootstrap. Call-site counters are
`instrumented_logical_operations`; only object-store counters are physical
operations.

| Change and direct parent | Case | Parent p95 | Candidate p95 | Other measured result | Gate |
| --- | --- | ---: | ---: | --- | --- |
| Graph `e915a2d3 -> 505145be` | 500-node warm chain | 98.658 ms | 98.085 ms | Allocation calls -40.4%; bytes -26.7%; one physical request unchanged | Pass: operation/allocation gate; latency CI includes zero |
| Rejected secondary `505145be -> 2d94e43d` | 500 isolated Active-secondary nodes | 203.074 ms | 202.957 ms | Calls +0.1%; bytes +1.5%; eight physical requests unchanged; latency CI -0.175% to +0.217% | Fail; optimization removed |
| Vector `53bd7bdd -> af88b322` | 100 isolated Active-vector nodes | 204.318 ms | 204.236 ms | Calls -48.3%; bytes -63.6%; CPU -65.7%; physical put bytes -24.8%; latency CI -4.317% to +1.559% | Pass: allocation gate; p95 non-regressive |
| Vector control `53bd7bdd -> af88b322` | 100 isolated nodes, no indexes | 101.210 ms | 101.646 ms | Calls unchanged; bytes +0.007%; physical requests/bytes unchanged; p95 +0.43% | Pass: all unaffected metrics below 5% regression limit |
| Text capability | 10,000 Active text deletions | Failed above 512 before this stack | 20 bounded epochs | Zero text uploads; same transaction | Pass: capability gate |

The manual, non-statistical 10,000-vector boundary changed as follows:

| Metric | Parent `53bd7bdd` | Candidate `5fee0871` | Change |
| --- | ---: | ---: | ---: |
| Total | 5.262 s | 1.668 s | -68.3% |
| Staging | 4.783 s | 0.123 s | -97.4% |
| Preparation | 0.0004 s | 1.201 s | Work intentionally moved behind the preparation boundary |
| Commit | 0.478 s | 0.344 s | -28.1% |
| CPU | 5.172 s | 1.586 s | -69.3% |
| Allocation calls | 35,185,541 | 16,971,068 | -51.8% |
| Allocated bytes | 7.877 GB | 2.436 GB | -69.1% |
| Peak RSS | 885.9 MB | 819.8 MB | -7.5% |
| Physical requests | 17 | 8 | -52.9% |
| Physical put bytes | 13.59 MB | 9.75 MB | -28.2% |

This manual case verifies the supported boundary, not a confidence interval.
The 100,000-vector stress job remains manual and was not run for this stack.

## Reproduction

Run the same command at a candidate and its direct parent, changing only
`HELIX_DELETION_BENCH_SOURCE_COMMIT` and the checked-out revision:

```bash
HELIX_DELETION_BENCH_RUNS=5 \
HELIX_DELETION_BENCH_SAMPLES=25 \
HELIX_DELETION_BENCH_SIZES=100 \
HELIX_DELETION_BENCH_WORKLOADS=isolated_nodes \
HELIX_DELETION_BENCH_CACHE_POLICIES=warm \
HELIX_DELETION_BENCH_INDEXES=vector \
HELIX_DELETION_BENCH_LIFECYCLE=active \
HELIX_DELETION_BENCH_SOURCE_COMMIT=<revision> \
cargo bench -p db --bench deletion_batches \
  --features production-coverage,index-lifecycle-testing
```

Compare raw files with:

```bash
cargo run --release -p db --example deletion_benchmark_compare \
  --features production-coverage,index-lifecycle-testing -- \
  <parent.jsonl> <candidate.jsonl>
```

Raw observations are stored in [`raw`](raw). The pre-PR3 graph files predate
the expanded case descriptor, so their workload/cache tuple is reported from
the source record and their operation reductions are calculated directly from
the stored samples.

## Correctness and rollback boundaries

- Graph property tests compare random directed multigraph deletion against an
  in-memory model, including self-loops, parallel edges, survivor boundaries,
  duplicates, missing IDs, and cascades. Rolling back the graph PR restores the
  preceding per-operation observation without a codec or API migration.
- Text tests cover admission failure, 511/512/513 and 10,000 boundaries,
  repeated entities, delete-only manifests, zero uploads, same-request search,
  commit, and reopen. Rolling back restores the single-epoch 512 limit; no
  persisted representation changes.
- Vector tests cover all metrics, deterministic bytes across identical batch
  runs, semantic comparison with sequential deletion, forward/reverse residue,
  stale entry metadata, missing canonical/neighbor rows, degree limits,
  visibility barriers, delete/recreate, commit/reopen, partition routing, and
  Building isolation. Rolling back restores sequential Active deletion; no
  public or persisted contract changes.
- The final commit boundary remains `topology -> secondary -> vector -> text ->
  commit`, followed by awaited cache publication. No detached work was added.

## Validation evidence

- `cargo test --workspace` passed after installing the locked
  `sdks/typescript` dependencies required by the CLI's offline tarball test.
- The feature-complete `db` suite passed serially with
  `production-coverage,index-lifecycle-testing`: 1,203 library tests, 192
  encoding tests, 41 public production contracts, 27 internal contracts, 12
  vector-planner contracts, and every lifecycle/row/fence contract. Four
  release-only scale tests remain intentionally ignored.
- All 1,203 library tests and every integration contract also passed under
  nightly branch instrumentation. LLVM 23 then crashed with `SIGSEGV` while
  aggregating the whole-crate report. Source-scoped reports from the preserved
  profile succeeded for 17 of 19 changed production files; only
  `mutation.rs` and `secondary.rs` reproduce the reporter crash.
- Representative changed-file branch coverage is 100% for `topology.rs`,
  89.52% for vector `mutation.rs`, 83.33% for `index_context.rs`, 81.25% for
  `tx.rs`, 78.26% for vector `active.rs`, and 72% for text
  `active_runtime.rs`. These percentages describe compiler branches, while
  the focused tests separately exercise every new ADT state and transition.
- Doc tests, workspace/all-target Clippy with warnings denied, formatting, and
  whitespace validation pass at the committed head.
