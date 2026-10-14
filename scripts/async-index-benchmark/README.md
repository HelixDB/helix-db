# Async index queue layout benchmark

Compares the two index-operation queue layouts (`HELIX_INDEX_QUEUE_LAYOUT=map`,
the default, and `rows`, the baseline) under identical open-loop traces, using
the samples a benchmark image prints (`docker-image/build.sh
--async-index-benchmark`). The primary path is the isolated EC2/S3 topology
([EC2 usage](#ec2-usage-primary-after-cost-approval-only)). Local runs
([local usage](#local-usage-secondary-diagnostics-on-local-disk)) only validate
the harness and the layout switch; they are not performance evidence.

> **Nothing here may be provisioned without explicit cost approval.** No script
> calls AWS on its own. `infra.json`, `bootstrap.sh`, `clone_seed.py` and
> `s3_metrics.py` are for an approved EC2 run only. See [Cost](#cost).

## Files

| File | Purpose |
| --- | --- |
| `samples.py` | Parses and validates server samples; windows and diffs them (counters, gauges, lifetime maxima, lag histogram, merge, I/O, SlateDB). |
| `summarize.py` | Builds a run summary (client latency and outcomes, server windows per phase, drain, resources) and aggregates runs into a comparison. |
| `replay.py` | Open-loop HTTP replay with a bounded pool per kind. It never retries and records every outcome. |
| `workload.py` | Generates a deterministic trace (JSONL payloads) from a spec and a seed's entity IDs. |
| `matrix.py` | Plans the seven screens and the final matrix (layouts × load levels × warm/cold × repetitions). |
| `seed.py` | Seeds a fresh database: rows, index builds and sampled strong/eventual verification. |
| `node.py` | Container lifecycle on one host: start/clean stop (saves logs and samples), and snapshot/restore of a local-disk volume. |
| `run.py` | Single-host orchestration (local disk): `seed` per layout, then `run` one screen on an independent restored copy. |
| `dataset.py`, `small_fixture.py`, `../prepare-dbpedia-vector-fixture.py` | Verified fixtures (see [Fixtures](#fixtures)). |
| `clone_seed.py` | EC2: SHA-256-verified copy of a closed seed S3 prefix into a fresh run prefix. |
| `resources.py` | EC2: exact cgroup-v2 CPU and peak memory for one container. |
| `s3_metrics.py` | EC2: CloudWatch S3 request metrics, as observed by the service. |
| `infra.json`, `bootstrap.sh` | EC2: isolated CloudFormation stack and host bootstrap. |
| `test_*.py` | Stdlib `unittest` suites: `python3 -m unittest` in this directory. |

Everything runs on the Python 3.12+ standard library, except DBpedia
download/conversion (`numpy`, `pyarrow` and `certifi` are imported lazily) and
`clone_seed.py` (`botocore`).

## Server contract consumed

The writer and reader print one JSON line per `HELIX_BENCHMARK_SAMPLE_MS`
(default 1000). The line carries `"helix_benchmark_sample": 1` and cumulative
`queue`, `lag`, `merge`, `io` and `storage` counters, plus one final sample
after the database closes. Every run records its role
(`HELIX_BENCHMARK_ROLE`), layout, image ID and redacted environment. A
database must reopen with the layout that wrote it. Only benchmark images
read `HELIX_INDEX_QUEUE_LAYOUT`; product images always run `map`. Writers,
and readers at open, refuse a mismatch while queues hold work (`index
operation queues were written with a layout other than Map`), and `run.py`
also refuses a mismatch against the seed record. Writes carry `x-helix-await-durable: true`.

## Workflow

1. Prepare a fixture, then **seed each layout** from it. Seeding inserts rows
   first, then creates the indexes and waits for the builds. The writer then
   stops cleanly, and the closed database is the seed: an S3 prefix on EC2, a
   volume snapshot locally. IDs are sequential in a fresh database, so both
   layouts get the same `entity-ids.u64le`.
2. Plan screens or the final matrix (`matrix.py`), then **generate each trace
   once** (`workload.py`) and replay it byte for byte against every layout.
   Before starting, check the seed's ID hash against the trace manifest and
   the trace's hash (`run.py` does this locally).
3. **Each run** copies the closed seed to an independent location
   (`clone_seed.py` on S3, `node.py restore` locally). It starts a fresh
   writer process, plus a reader on the shared storage when searches go to
   it. It replays open-loop, waits (bounded) for the queue to drain
   (`node.py drain`), stops cleanly and summarizes.
4. **Compare** runs with `summarize.py RUN... --compare OUT`. The comparison
   gives the median and min–max per (screen, layout, cache) group.

## EC2 usage (primary; after cost approval only)

`infra.json` creates a dedicated
VPC and subnet, an S3 gateway endpoint, a private encrypted bucket with
request-metric filters (`AllBenchmarkRequests`, `runs/` and `seeds/`), an
instance role limited to that bucket plus SSM, and three AMD64 nodes in one
AZ. There is no public ingress and no SSH (use SSM):

| Role | Instance | Disk |
| --- | --- | --- |
| Writer | `r7i.4xlarge` | 500 GiB gp3, 12,000 IOPS, 500 MiB/s |
| Reader | `r7i.4xlarge` | same |
| Load and build host | `c7i.4xlarge` | same |

1. **Stack.** Record an explicit Ubuntu 24.04 AMD64 AMI, validate the
   template, and create it in `us-east-1` with `ImageId`,
   `AvailabilityZone=us-east-1a` and `CAPABILITY_IAM`. Keep the outputs and
   the template hash.
2. **Hosts.** Run `bootstrap.sh` through SSM on each node. It records package,
   kernel, CPU, storage and Docker versions under `/opt/helix-async/evidence`.
   Do not upgrade packages between comparisons.
3. **Images.** Move a verified `git archive` of the tested commit through the
   bucket. On the load host, build the benchmark image natively with
   `docker-image/build.sh --platform linux/amd64 --async-index-benchmark`,
   with digest-pinned `--rust-image/--runtime-image`. Run
   `docker-image/tests/index_queue_contracts.py --image IMAGE --scenario
   benchmark` and the image suite, then load the image on both DB hosts.
   Record image IDs and never compare moving tags. One benchmark image serves
   both layouts, selected by environment.
4. **Fixtures** on the load host: see [Fixtures](#fixtures). Verify before
   seeding. Keep downloads and transfers outside measurement windows.
5. **Seeds, one per layout, on the writer host.** Put `S3_BUCKET`,
   `S3_REGION` (and nothing secret; the instance role supplies credentials)
   in `s3.env`:
   ```sh
   python3 node.py start --name seed-map --image $IMAGE --role writer --layout map \
     --s3-env s3.env --db-path seeds/map-combined/db --bind 0.0.0.0 --port 8080 --output ev/seed-map/node
   python3 seed.py --fixture $FIXTURE --family combined --layout map \
     --writer http://$WRITER:8080 --output seed-map          # from the load host
   python3 node.py stop --name seed-map --output ev/seed-map/writer   # keep the container
   ```
   Repeat for `rows`. `cmp seed-map/entity-ids.u64le seed-rows/entity-ids.u64le`
   must succeed. Otherwise one trace cannot serve both layouts.
6. **Traces** on the load host: `matrix.py`, then `workload.py` once per trace.
   Keep `manifest.json` next to each run.
7. **Each run**, in plan order:
   - Writer host: `clone_seed.py --bucket $B --source seeds/$LAYOUT-combined/db
     --destination runs/$RUN/db --container seed-$LAYOUT --output ev/$RUN/clone`.
   - Cold runs: `sync; echo 3 > /proc/sys/vm/drop_caches` on both DB hosts.
     A fresh container starts with empty caches. S3 storage always caches on
     local disk, at `/var/cache/helix` in the container's writable layer, and
     `node.py` sets `HELIX_DISK_CACHE_WARM=off` so no startup warm reads S3
     during the replay. Runs on images built before S3 always cached on disk
     are not comparable.
   - Writer host, then reader host: `node.py start --role writer|reader
     --layout $LAYOUT --s3-env s3.env --db-path runs/$RUN/db --bind 0.0.0.0
     --port 8080 ...`.
   - Both DB hosts, as root: `resources.py --container NAME --output
     ev/$RUN/resources-<role> --duration-seconds N --start-unix-ns $START`.
     `$START` is the replay start plus the warm-up for warm runs. Capture
     `chronyc tracking` before and after.
   - Load host: `replay.py --trace trace.jsonl --output $RUN/replay --writer
     http://$WRITER:8080 --reader http://$READER:8080 --start-unix-ns $START`.
     Searches go to `--reader`; `--strong`/`--eventual` override that per kind.
   - Writer host: `node.py drain --name NAME --timeout-s 600` (exit 1 if the
     queue did not empty; the summary reports what was still pending). Then
     `node.py stop` on the reader, then the writer. Upload the evidence,
     then `node.py remove --name NAME` on both hosts: each run container's
     layer holds up to 8 GiB of disk cache.
   - Assemble the run directory: `trace-manifest.json`, `replay/`, `writer/`,
     `reader/`, `resources-writer/`, `resources-reader/`, and a `run.json`
     holding at least `screen`, `layout`, `cache`, `repetition`, the image ID
     and the trace hash. Then run `summarize.py RUN_DIR`.
   - After the CloudWatch delivery delay: `s3_metrics.py --bucket $B
     --filter-id BenchmarkDatabaseRequests --start-unix-seconds ...
     --end-unix-seconds ... --output ev/$RUN/s3` over whole UTC minutes.
     Compare it with the connector counters. Neither is wire bytes.
8. **Compare** with `summarize.py RUN... --compare OUT`.
9. **Stop instances when idle.** EBS still bills. Delete the stack's compute
   and network after collection. The bucket is retained deliberately; delete
   run prefixes you no longer need.

## Cost

These are **estimates, not quotes**: us-east-1 on-demand Linux list prices as
last known. **Re-check them on the AWS pricing pages before seeking
approval.** Assumptions: 730 h/month, no reserved or spot capacity, and all
traffic in one AZ over the gateway endpoint (no data-transfer or endpoint
charge).

| Item | Rate | Per hour |
| --- | --- | --- |
| 2 × r7i.4xlarge (DB nodes) | $1.0584/h each | $2.117 |
| 1 × c7i.4xlarge (load host) | $0.714/h | $0.714 |
| 3 × gp3 500 GiB, 12k IOPS, 500 MiB/s | $0.08/GB-mo + 9k IOPS × $0.005 + 375 MiB/s × $0.04 = $100/mo each | $0.411 (also while stopped) |
| 3 public IPv4 addresses | $0.005/h each | $0.015 |
| S3 requests during load | PUT/LIST $0.005 per 1k, GET $0.0004 per 1k. Assumes ≤ 30 PUT/s (local disk showed about 23/s at 20 writes/s) and ≤ 300 GET/s | ≤ $0.97 (measure with `io`) |
| S3 storage | $0.023/GB-month. Seeds and clones of tens of GB | < $0.01 |
| CloudWatch request metrics | 3 filters × ~16 metrics × $0.30/metric-month | ~$0.02 |
| **Total, all running, under load** | | **≈ $3.3–4.3/h** (compute + EBS + IPv4 + metrics ≈ $3.28/h) |
| Idle, instances stopped | EBS and metrics (public IPv4 is released while stopped) | ≈ $0.43/h (~$10/day) |

Planning figures (hours × $3.3–4.3/h, plus idle EBS for the stack's lifetime):

- **Screening pass**: bootstrap, native builds, image suites and fixture
  preparation take about 3 h. Seeding a verified 50K subset for both layouts
  takes about 1 h. Seven screens × 2 layouts at about 20 min each (10-min
  window plus restore, startup and drain) take about 4.7 h. Total about 9 h,
  or **≈ $30–40**.
- **Reduced final matrix**: combined family, one fixture, below/near/above,
  warm and cold, 3 repetitions, both layouts. That is 36 runs: 18 warm at
  about 55 min (10-min warm-up, 30-min window, about 15 min overhead) and 18
  cold at about 45 min, about 30 h in total. With about 2 h to seed the 100K
  fixture twice, the total is about 32 h, or **≈ $105–140**, plus a few days of idle
  EBS (~$10/day).
- The three-family version of the same matrix is 108 runs and about 90 h of
  load, **≈ $300–390** plus seeding. `matrix.py final` prints the exact
  `planned_load_hours` for any selection.

## Fixtures

- **100K DBpedia, paired vectors and text (benchmark default):** the first
  100,000 rows of the pinned dataset revision.
  `../prepare-dbpedia-vector-fixture.py FILE --rows 100000 --with-text`, then
  `--verify`. The full 1M fixture (`--rows 1000000`) is out of scope for now. The manifest is written last and carries hashes of the source
  shards, vectors and text.
- **Verified screening subset:** either the pinned 50K subset (`--rows 50000
  --with-text`, whose vector hash is pinned), or any prefix of a verified
  fixture without downloading (`--prefix-of FULL --prefix-rows N`). A prefix
  records the parent's hashes and verifies like any fixture.
- **Small payload:** `small_fixture.py --output DIR` (100K rows by default, 8-dim vectors
  of 32 bytes, 32-byte text). 250,000 pending members are about 8 MB of input
  per index, so the 250,000-member limit is reached well before the 1 GB byte
  limit. Use it for the backlog screen and backpressure (429) at the member
  limit.

## Screens and the final matrix

`matrix.py screens` defaults to 20 writes/s, 5+5 searches/s and 60 s phases.
The mix is 20% insert, 10% index-property removal, 10% whole-entity delete and
60% update, with 90% of updates on 64 hot entities. Top-k is 10, capped at the
product's 800-result limit.

| Screen | Workload |
| --- | --- |
| `vector`, `text` | One family isolated on a combined seed: mutations touch only that property, and deletes become removals (`mutation_scope: indexed_property`). |
| `combined` | Both families. Deletes drop whole entities. |
| `searches` | Four times the strong and eventual search rates, concurrent with writes. |
| `hot-updates` | Updates only, all on 8 hot entities (coalescing and merge-chain stress). |
| `removal-deletion` | 40% index-property removal, 40% whole-entity delete, 10% insert, 10% update. Updates re-add removed properties. |
| `backlog` | Consecutive phases at 1×, 2× and 4× the write rate (sustained load, backlog growth). |

`matrix.py final --calibration FILE` expands reviewed rates into the final
matrix. Rates are per fixture and family: below, one or more near, and above
the drain capacity, plus the strong and eventual rates. Profiles are sustained
below/near/above and growing (1×/2×/4× above). Each profile runs warm (a
`--warmup-s 600` warm-up, then a `--measure-s 1800` window) and cold (window
only, in a fresh process with an empty cache). It uses `--repetitions 3` with
seeds 1827–1829, and every layout replays the same trace. The layout order
alternates by repetition. `plan.json` reports `planned_load_hours`. Select
cells from screening before running a Cartesian product. Calibrate rates on
EC2 from `published_ops_per_s` and the pending trajectory, never from local
runs.

## Metrics

The measurement window is every non-warm-up phase (override with
`--from-ns/--until-ns`). Each phase and the post-load drain are also
reported. Client figures use the load generator's clock. Server figures
difference the samples that bracket the window on the writer's `unix_ms`,
measured with its monotonic `elapsed_ns`. `boundary` gives the gaps, which are
at most one sample interval.

| Metric | Source | Meaning and caveats |
| --- | --- | --- |
| Write ACK p50/p95/p99 by outcome | ledger | Exact nearest rank over offers scheduled in the window. Late completions count. |
| Strong and eventual search p50/p95/p99 | ledger | Same method. The endpoint (writer or reader) comes from `--strong-on/--eventual-on`. |
| `scheduled_latency`, `dispatch_lag` | ledger | Include client scheduling delay. If dispatch lag grows, the load generator is saturated. |
| Outcomes | ledger | `backpressure_429`, `conflict_409`, `client_overload` (never sent), `timeouts`, `uncertain_writes` (5xx, transport failure or timeout; the commit may have happened), `rejected_writes` (other 4xx), `failed_searches`. |
| Acknowledged ingestion | ledger | HTTP write ACKs completed inside the window, per second. These are requests, not index operations. |
| Publication throughput | `queue.acknowledged_operations` Δ/s | Exact operations released: published or discarded. `published_entities` Δ/s counts collapsed per-entity effects, i.e. member drain. `committed_operations` Δ/s is the enqueue rate. |
| Distinct-member drain | `pending_members` gauge | Start, end, min and max per window. The full trajectory is in `timeseries.jsonl`. `drain.seconds_to_empty` covers the time after the trace ends. |
| Publication lag p50/p95/p99 | `lag` histogram diff | Exact per operation ID, from observed commit to durable ACK. Values are **bucket lower bounds** (log-linear, at most 12.5% wide, exact below 16 µs). The mean is exact and the max is the process lifetime maximum. |
| Censored and unfinished | `censored_acknowledgements`, gauges at window end | Censored ACKs have no commit observed by this process (restart or reconciliation, or an ACK that raced the producer's commit return; a few per run happen even without restarts). They are never in the lag histogram. Operations still pending at the window end have no lag yet; `oldest_pending_micros` is a lower bound on theirs. |
| Conflicts and retries by phase | `commit_conflicts`, `uncertain_commits`, `publication_retries`, `publication_error_retries`, `output_retries`, `blocked_attempts`, `deferred_attempts` | Server-side publication retries, kept separate from client 409s. |
| Queue read cost | `queue_reads`, `queue_read_bytes`, `queue_read_micros` | Queues read and decoded by publication, including retired-generation discards and uncertain-commit reconciliation. The time is wall clock: storage I/O (object-store GETs on a cache miss), the map layout's read-time merge resolution (also in `merge.resolved.nanos`) and decoding. |
| Merge cost per kind | `merge.{partial,resolved}` | Process-wide queue merges: count, operands, input/output bytes and wall time. SlateDB folds raw operands without a base in batches of at most 100 (`partial`: every read, flush and compaction), then resolves only the batch results against the base (`resolved`: reads and bottom compaction). So `partial.max_operands_lifetime` stops at 100, `resolved` operands count batches, and `resolved` input bytes repeat `partial` output bytes; no field is the merge-chain length. `rows` should be ~0. Reader merges are reported separately. |
| S3 connector I/O | `io` per `service method` | Attempts below the object_store retry loop (retries included), status headers, transport errors, and **connector body bytes** (request bytes offered, response bytes delivered). These are not wire bytes: no TLS, HTTP or TCP overhead. Empty on local disk. |
| SlateDB storage | `storage` | Changed metrics only. Scalars carry `start/end/delta`, because counters and gauges look alike (a negative delta means a gauge). Histograms carry `count/sum` deltas. Examples are compaction bytes, WAL flush bytes, and object-store requests by API. `merge_operator_operands` includes non-queue merges. |
| CPU and peak memory | `resources.py` (cgroup) or `docker-stats.jsonl` | cgroup peaks are exact. `docker stats` samples about every 2 s, so its peak and mean are approximate. |

Integrity checks: ledgers must hold exactly one outcome per offer, with a
footer that matches. Sample streams must be contiguous, from one process and
one role. Regressed counters and `lag.count != acknowledged - censored` are
listed as `anomalies`. Mid-run samples can be off by in-flight ACKs, because
the queue stats and the lag histogram are read at slightly different moments.
Final samples must agree exactly.

## Local usage (secondary: diagnostics on local disk)

A writer and a reader share one Docker named volume. MinIO images are no
longer pullable. For S3 locally, run the digest-pinned VersityGW gateway used
by `docker-image/tests/index_queue_contracts.py`, then pass `node.py start
--s3-env FILE --db-path PREFIX --network NET`. `run.py` itself is disk-only.

```sh
cd scripts/async-index-benchmark
IMAGE=helix-async-index-bench:<tag>   # benchmark image, loaded locally
OUT=/tmp/async-screen                 # any new directory
python3 small_fixture.py --output $OUT/fixture --rows 4000
for layout in map rows; do
  python3 run.py seed --image $IMAGE --layout $layout --fixture $OUT/fixture/fixture.fbin \
    --small-payload --family combined --output $OUT/seed-$layout
done
python3 matrix.py screens --output $OUT/specs --write-rps 20 --search-rps 5 --duration-s 45 --warmup-s 15
python3 workload.py --fixture $OUT/fixture/fixture.fbin --small-payload --seed $OUT/seed-map/seed \
  --spec $OUT/specs/combined.json --workload-family combined --output $OUT/trace-combined
for layout in map rows; do
  python3 run.py run --image $IMAGE --layout $layout --seed $OUT/seed-$layout \
    --trace $OUT/trace-combined --screen combined --cache warm \
    --strong-on reader --eventual-on writer --docker-stats --output $OUT/run-combined-$layout
done
python3 summarize.py $OUT/run-combined-* --compare $OUT/compare
```

A run directory holds `run.json` (exact configuration: image ID, layout,
redacted env, seed and trace hashes, harness file hashes, host),
`trace-manifest.json`, `replay/requests.jsonl` (every offer's outcome), and
for each role `stdout.log`, `stderr.log`, `samples.jsonl` (raw), `timeseries.jsonl`
(compact gauges per sample), `inspect.json` and `stop.json`. It also holds
`docker-stats.jsonl`, `summary.json` and `summary.md`. Failed runs keep
whatever evidence exists, with `status: incomplete`. Output directories are
never reused.

## Caveats

- Local runs share one laptop-class host with other workloads. They validate
  the harness and the layout switch. They are not performance evidence.
- Cross-host windows map the load generator's clock onto the writer's
  `unix_ms`. Keep chrony evidence. The server uses its own monotonic clock
  for rates.
- A trace depends on the seed's entity IDs. Seed every layout from the same
  fixture in the same order.
- A database whose queues are empty reopens under either layout (there is
  nothing to detect). The server rejects a mismatch only while queue entries
  exist, so keep layouts matched through the seed records.
