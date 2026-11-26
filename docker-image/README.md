# HelixDB Docker image

This directory owns the build and test surface for the standalone HelixDB image. The build context is this repository; it does not require another checkout or sibling source directory.

The canonical image repository is `ghcr.io/helixdb/helixdb`. The scripts require an explicit platform and image tag so local and CI runs exercise the same artifact.

The next release is `ghcr.io/helixdb/helixdb:v0.0.19`, targeting Linux amd64
and arm64. The published release remains v0.0.17 until the release workflow succeeds. See the [local server guide](../docs/database/helix-db/start-here/local-development/local-server.mdx)
for release-image commands. The `local-amd64` and `local-arm64` tags below refer
to images built from your checkout.

## Container contract

- `/bin/helix-server` is PID 1 and runs as the distroless `nonroot` user (`65532:65532`).
- HTTP listens on `0.0.0.0:8080`; internal gRPC listens on `127.0.0.1:8081` and is not exposed.
- `GET /healthz`, `GET /readyz`, and `POST /v2/query` are the supported container probes and query endpoint. `GET /v2/diagnostics` reports read-only setup checks, which `helix doctor` renders.
- A writer another container fenced, by opening the same storage as writer, turns `/readyz` 503 (`"storage": "fenced"`) within about a second, with or without traffic, answers every query with `writer_fenced_commit_outcome_unknown`, and stops its background work. `/healthz` stays 200 and the process keeps running: restarted, it would fence the new writer in turn. Stop it, or restart it once the other writer is gone. `/healthz` turns 503 only when storage failed, when a restart helps.
- Storage is in memory unless local-disk or S3-compatible configuration is supplied.
- `/var/lib/helix` (data) and `/var/cache/helix` (disk cache, always used with S3 storage) are owned by the runtime user, so new named volumes mounted there are writable.
- Docker sends `SIGTERM`; the server drains both listeners and closes storage before exiting.

## Build

Docker Buildx is required. Build and load a native image with one of:

```bash
docker-image/build.sh \
  --platform linux/amd64 \
  --image ghcr.io/helixdb/helixdb:local-amd64 \
  --load

docker-image/build.sh \
  --platform linux/arm64 \
  --image ghcr.io/helixdb/helixdb:local-arm64 \
  --load
```

To produce a Docker archive instead of loading the image:

```bash
docker-image/build.sh \
  --platform linux/amd64 \
  --image ghcr.io/helixdb/helixdb:local-amd64 \
  --output /tmp/helixdb-amd64.tar
```

The output path must not already exist.

## Run

Memory storage is the default:

```bash
docker run --rm -p 8080:8080 ghcr.io/helixdb/helixdb:local-amd64
```

Use `HELIX_DATA_DIR` with a volume for native persistent storage:

```bash
docker volume create helixdb-data
docker run --rm -p 8080:8080 \
  -e HELIX_DATA_DIR=/var/lib/helix \
  --mount type=volume,source=helixdb-data,target=/var/lib/helix \
  ghcr.io/helixdb/helixdb:local-amd64
```

For S3 or an S3-compatible service, set `S3_BUCKET`, credentials through the standard AWS environment variables, and these optional settings:

| Variable | Purpose |
| --- | --- |
| `S3_REGION` | Bucket region; falls back to `AWS_REGION`, `AWS_DEFAULT_REGION`, then `us-east-1`. |
| `AWS_ENDPOINT` | Custom S3 endpoint; `AWS_ENDPOINT_URL_S3` is also accepted. |
| `AWS_ALLOW_HTTP` | Set to `true` or `1` only for a trusted plain-HTTP endpoint. |
| `DB_PATH` | Logical database prefix inside the selected store; defaults to `db/`. |

For Google Cloud Storage, set `GCS_BUCKET`. Credentials come from
`GOOGLE_SERVICE_ACCOUNT` (a key file), `GOOGLE_SERVICE_ACCOUNT_KEY` (the key's JSON) or
`GOOGLE_APPLICATION_CREDENTIALS`, else from the GCE metadata server, which serves GKE
Workload Identity. The server talks to GCS through its own API, not its S3-compatible
endpoint, whose conditional writes do not give SlateDB the fencing it needs.

For Azure Blob Storage, set `AZURE_CONTAINER_NAME` and `AZURE_STORAGE_ACCOUNT_NAME`.
Credentials come from, in order, `AZURE_STORAGE_TOKEN`, `AZURE_STORAGE_ACCOUNT_KEY`,
workload identity (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_FEDERATED_TOKEN_FILE`,
which AKS Workload Identity injects), a service principal (`AZURE_CLIENT_SECRET` with the
client and tenant IDs), `AZURE_STORAGE_SAS_KEY`, else the managed identity of the VM or node.

`HELIX_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES` sets the most unpublished vector work,
in bytes, that one `strong` vector search scores exactly before it fails with the
retryable `index_backpressure` (`pending_vector_bytes`). It defaults to each index's
retained-byte limit (1000000000) with any storage, so writes reach backpressure first;
set it lower to cap the CPU time of each strong vector search. The bound counts every
tenant partition of an index. Startup fails naming it when it is not a positive integer.

`HELIX_STRONG_TEXT_SEARCH_MAX_ANALYSIS_BYTES` sets the most unpublished text, in analysis
bytes (text plus about 280 bytes per token), that one `strong` text search includes per
tenant partition before it fails with the retryable `index_backpressure`
(`pending_text_analysis_bytes`). It defaults to 512 MiB (536870912) with any storage and
also caps the analyses the server keeps for reuse. Startup fails naming it when it is not
a positive integer.

`HELIX_WAL_FLUSH_INTERVAL_MS` sets the milliseconds between WAL uploads, from 1 to 60000,
and defaults to 100. A durable write waits for the next upload, so a lower value cuts
write latency, and each interval with new writes costs one object-store PUT. Startup
fails naming it when it is out of range.

`HELIX_SST_COMPRESSION` sets the block codec of the SSTs the server writes, WAL SSTs
included: `none` (the default) or `zstd`, in any case, with any storage. Every SST
records its codec, so changing it needs no migration: existing SSTs stay readable and
compaction rewrites them with the current codec. Images released before zstd support
cannot read zstd SSTs. Startup fails naming it for any other value.

`HELIX_VECTOR_BULK_BUILD_MEMORY_BYTES` turns in-memory vector index builds on and sets
the memory they may hold together. Unset or `0`, they are off, and every vector index is
built one vector at a time. `auto` gives them what the memory limit leaves beside the
configured caches and write buffers and an eighth of it kept free. A byte count gives
them exactly that much, whatever else the server holds: one larger than the memory limit
leaves free gets the server OOM-killed during the build, up to three times for each
index before the build continues serially. A build that fits links its whole graph in
memory, many times faster than inserting one vector at a time; one that does not is
built serially. Startup fails naming the variable when it is neither `auto` nor a byte
count. Turn it on only as [below](#turning-in-memory-vector-builds-on).

The status of a vector build (`get_index_operation`) reports its path in
`vector_build_path`: `{"path": "bulk"}` for an in-memory build, or
`{"path": "serial", "reason": "..."}` with why it is serial (`bulk_builds_disabled`,
`over_memory_budget`, `too_few_vectors`, `tenant_partitioned`,
`earlier_attempts_failed`, `bulk_attempt_failed`, `too_many_rows_changed`,
`resumed_serial_scan`). `bulk` at stage
`scan` is stored with
the operation and reported by every server; every other value is known only to the
server running the build and is absent after it restarts.

While a build is on the in-memory path its status also carries `vector_bulk_build`:

| Field | Meaning |
| --- | --- |
| `bulk_attempt`, `bulk_attempt_limit` | Which in-memory attempt this is, of three. Stored with the operation, so every server reports it. Not the operation's own `attempt`, which counts retried steps |
| `phase` | What the attempt is doing, from the server running it: `waiting`, `reading`, `linking`, `writing` or `discarding` |
| `waiting_for` | With `waiting`: `turn` while another build reads or links (one does at a time), `memory` while running in-memory builds hold what this one needs |
| `members_read`, `members` | With `reading`: source rows read, of the label's members |
| `vectors_linked`, `vectors` | With `linking`: vectors inserted into the graph, of those the read found |
| `nodes_written`, `nodes`, `paused_for_write_buffer` | With `writing`: graph nodes stored, and whether the build is holding back because the write buffer is full, of its own rows or of other writes |
| `rows_deleted` | With `discarding`: rows of an attempt that did not finish, deleted before the next one starts |

The `progress` counters of the operation move only while the build writes. A build that
waits takes no step until what it waits for is released, so it does not slow other
vector index operations, and the server log says once what it waits for. The log also
names each step of a build (`reserved its memory`, with the budget it was admitted
under and what the rest of the server was taken to hold, `read its source`,
`linked its graph`, `stored its graph`, with the rows it took out of the graph
(`nodes_unlinked`), the time that took (`unlink_ms`) and its longest step
(`longest_step_ms`), `deleting a partially written bulk vector graph`).

An attempt that a restart interrupts is discarded and the build starts again, as the
next attempt. Every attempt counts that ended without the build finishing its
in-memory stage or leaving it: the server was killed, crashed, stopped, restarted for a
deploy, or lost the writer role. After three, the build continues serially
(`earlier_attempts_failed`). A read of the source that object storage fails for a
moment is repeated in place, up to three times, and costs no attempt. A build that
blocks first deletes what it wrote and returns to the serial stage, so a `blocked` build
reports no `bulk` path, and its retry starts at attempt one. Discarding takes about
three times as long as writing the same rows did (about 2,600 entities a second on S3)
and reports `rows_deleted` meanwhile.

An in-memory attempt that fails in a way a build one vector at a time cannot does not
block the build and is not tried again: the server logs the failure as an error (`bulk
vector build failed where a serial build may not`), deletes what the attempt wrote and
builds the index serially at once (`bulk_attempt_failed`). Please report such a log
line. An attempt that cannot start because object storage refuses the upload of its
count goes the same way, with a warning that says so instead.

A row that changes after an in-memory build read it is taken out of the graph before
the graph is written, and indexed after the build like any write. Taking a row out
costs about 4 ms at 128 dimensions and 21 ms at 1,536, against a fraction of a
millisecond for a row that did not change, so a build is bounded twice. One step of it
spends at most a quarter of a second on such rows before other index operations get
their turn. And once it has taken a thousand rows out, a build stops its in-memory
attempt if the rows it has not written yet would hold, at the share changed so far,
more changed rows than an eighth of the index: it logs `source changed too much to
finish in bulk`, deletes what it wrote and builds serially at once
(`too_many_rows_changed`), which is then the faster way. A build with fewer changed
rows than that finishes in memory and pays for each: a tenth of a million vectors of
1,536 dimensions changed adds about 35 minutes.

The automatic budget is measured against what the rest of the server holds when no
in-memory build runs. Memory a build leaves resident after it ends is counted as the
build's, so the next build gets the same budget. If the rest of the server grows while
a build runs, the growth lowers later budgets as far as the server saw it. What it
sees is memory beyond the larger of two amounts: the most the running builds reserved
at once, and what earlier builds had left resident when they started. Growth below
that, which can be the whole of the larger amount while the builds themselves hold
little, is taken for memory the builds left and makes the next budget too large by as
much, within the eighth of the memory limit the budget keeps free. Growth the server
saw even for a moment keeps lowering budgets until that memory has been returned to
the operating system.

#### Turning in-memory vector builds on

In-memory builds are off unless `HELIX_VECTOR_BULK_BUILD_MEMORY_BYTES` turns them on,
because an image from before them (v0.0.18 and earlier) cannot read the stage they
store. Such an image that opens the database as its writer while an in-memory build is
in progress fails at startup (see below), and by opening it takes the writer role from
the server that was running. That server stays up and answers every request, reads
included, with `detected newer DB client` until it is restarted, and the restarted
build has used one of its three attempts. So set the variable only once every server
that can become the writer (a standby, the rest of a rollout, the target of an automatic
rollback) runs an image that has in-memory builds. While the variable is unset this
image stores nothing an earlier image cannot read.

#### Rolling back during an in-memory vector build

Images from before in-memory builds (v0.0.18 and earlier) do not know the stage such a
build stores. While any operation reports stage `scan` with `vector_build_path` path
`bulk`, they refuse to open the database as its writer: startup fails with
`unknown V2 vector build stage discriminant 0x07` and the build stays as it is. A server
reports the two for as long as they are what a restart would find, so once no operation
reports them an earlier image can open the database, also after a kill. Before rolling
back, do one of:

- Let the build finish. Every stage after `scan` is readable by earlier images.
- Restart the current image with `HELIX_VECTOR_BULK_BUILD_MEMORY_BYTES` unset or `0` and
  poll the operation until `vector_build_path` no longer reports `bulk`. The server
  deletes what the build wrote, at about 2,600 entities a second, and continues it
  serially, which the earlier image can take over.
- Abort it (`abort_index_operation`), wait until it reports `aborted`, and create the
  index again afterwards. On a database below index storage version 8 the abort deletes
  what the build wrote row by row, far more slowly than the way above: 46 to 60 minutes
  for a fifth of a million-vector index, and more than in proportion for more; an
  earlier image can finish that abort itself. From version 8 the abort ends within
  seconds and leaves those rows to compactions, which discard them.

These steps make a database readable by an earlier image only while its index storage
version is below 8. This image moves a database it opens as the writer to version 8,
which images from before in-memory builds refuse whatever its builds report: going back
past that needs a backup taken before the upgrade.

A `blocked` build needs none of these: it left the in-memory path before it blocked.
The one exception still reports `bulk` while blocked, a build whose written rows could
not be deleted: abort it.

### Benchmark images

`docker-image/build.sh --async-index-benchmark` builds a separate benchmark
image; `--rust-image` and `--runtime-image` pin base images by digest. It adds:

| Variable | Purpose |
| --- | --- |
| `HELIX_BENCHMARK_ROLE` | `writer` (default) or `reader`; a reader needs shared disk or S3 storage. |
| `HELIX_BENCHMARK_SAMPLE_MS` | Sample interval in milliseconds; defaults to `1000`. |
| `HELIX_INDEX_QUEUE_LAYOUT` | How pending vector/text index operations are stored: `map` (default, the only layout product images run) or `rows`, the row-per-operation baseline. A database must restart with the layout that wrote it; writers, and readers at open, refuse queues written with the other layout. |

The server prints one JSON line per interval to stdout, marked by
`"helix_benchmark_sample": 1`, with cumulative counters: index-operation
backlog and publication (`queue`), exact-operation publication lag (`lag`),
queue merge costs (`merge`), object-store HTTP attempts and body bytes below
the client's retry loop (`io`), and SlateDB's own metrics (`storage`).
Subtract two samples to measure a window. `io` bytes are request/response
body bytes seen by the client, not network-wire bytes.

Set at most one of `HELIX_DATA_DIR`, `S3_BUCKET`, `GCS_BUCKET` and `AZURE_CONTAINER_NAME`. Credentials are runtime-only and are never baked into the image. Object storage always caches on local disk; see [Disk cache](#disk-cache).

Leave all four variables unset for memory storage. Bind mounts and existing volumes
must be writable by the container's `65532:65532` user and group.

### Disk cache

With S3, GCS or Azure storage the server always caches SlateDB blocks, object-store SST parts and
full-text splits in memory and on local disk, ideally NVMe, in `/var/cache/helix`
unless `HELIX_DISK_CACHE_DIR` names another directory. There is no memory-only S3
mode: vector indexes on S3 need the disk tier, because once the HNSW graph
outgrows the memory cache every graph hop that misses it is a serial S3 GET. With
`HELIX_DATA_DIR` storage the disk cache is optional; set `HELIX_DISK_CACHE_DIR` to
enable it. Images up to v0.0.8 cache S3 in memory only unless `HELIX_DISK_CACHE_DIR`
is set, and v0.0.6 and earlier ignore every cache variable.

Mount a volume at `/var/cache/helix` so the cache survives restarts and container
replacement; a restarted server then reads recently used data from local disk
instead of the object store. Without a volume the cache lives in the container's
writable layer and every new container starts with an empty cache.

```bash
sudo mkdir -p /data/helix-cache
sudo chown 65532:65532 /data/helix-cache
docker run --rm -p 8080:8080 \
  -e S3_BUCKET=my-bucket -e S3_REGION=us-east-1 \
  -e HELIX_DISK_CACHE_BYTES=107374182400 \
  -v /data/helix-cache:/var/cache/helix \
  ghcr.io/helixdb/helixdb:local-amd64
```

A named volume (`--mount type=volume,source=helixdb-cache,target=/var/cache/helix`)
needs no `chown`. When running as another user or outside the image, set
`HELIX_DISK_CACHE_DIR` to a directory that user can write. With a read-only root
filesystem, mount a writable volume (for example a tmpfs or `emptyDir`) at the cache
directory. Use one cache directory per running server and per database: changing
`DB_PATH` on the same directory leaves the old database's full-text cache behind.

Changing `HELIX_DISK_CACHE_BYTES` usually changes the block cache's block size, and
then the whole block tier (`slate/`) is discarded at startup and refills from the
object store; only budgets that keep the block size keep it. The object-store tier
keeps its files and evicts down to a smaller budget as it admits new data; the
full-text tier evicts down to its new share at startup.

The full-text tier fills on demand: a split is copied into `fts/` once searches have
used it twice, and startup downloads nothing into it. Startup and every admission
trim the tier, evicting the least recently used splits down to its share. A trim
spares splits admitted or recorded as used in the second before it (a split's use is
recorded at most once a minute) and splits still open in running searches or the
64 MiB full-text memory cache, so the tier can stay over its share by those splits
until the next admission. A split larger than the whole share is never copied.
After a restart, the first search to use each split in `fts/` reads and checksums
the whole split, up to 64 MiB, before it answers.

Once storage opens, the server warms the cache in the background: the index, filter
and stats blocks of the newest SSTs go into `slate/`, and with S3 the search rows of
every Active vector index go into `object-store/`, reading at most half that tier.
Neither warm delays startup or `/readyz`; queries that arrive first read through to
the object store as usual. Set `HELIX_DISK_CACHE_WARM=off` to skip both, for example
to avoid the startup reads from S3; every tier then fills only as queries read.

On a miss the object-store tier fetches and keeps a whole part of an SST: 4 MiB, or
less for budgets under 2 GiB so that the tier always holds at least 256 parts. Budget
at least twice the data the server reads often. Once that data outgrows half the
budget, parts keep evicting each other and cold reads fetch more from the object
store than memory-only caches would.

The budget must also fit on the cache's filesystem: its free space plus what the
cache already occupies. When `HELIX_DISK_CACHE_BYTES` is unset and the 8 GiB
default does not fit, startup fails naming the variable; set a budget that fits or
mount a larger volume. Without a volume the cache shares the filesystem of the
container runtime's writable layers, so an unchecked default could fill it. A
budget that is set and does not fit only logs a warning at startup: the cache can
then fill the filesystem, and if `HELIX_DATA_DIR` shares it, durable writes fail
too.

| Variable | Purpose |
| --- | --- |
| `HELIX_DISK_CACHE_DIR` | Disk cache directory, created with its `slate/`, `object-store/` and `fts/` subdirectories if needed. With S3 it defaults to `/var/cache/helix`. With `HELIX_DATA_DIR`, setting it enables the disk cache and leaving it unset keeps memory-only caches. Rejected with memory storage. |
| `HELIX_DISK_CACHE_BYTES` | Total disk budget in bytes, from 64 MiB to 1 TiB; defaults to 8 GiB. Half goes to object-store SST parts (`object-store/`), 3/8 to the SlateDB block cache (`slate/`), and the rest to full-text splits (`fts/`). With S3, `object-store/` also keeps the SSTs the server writes; with `HELIX_DATA_DIR` those are already on local disk, so it keeps only SSTs the server reads. |
| `HELIX_DISK_CACHE_MEMORY_BYTES` | Memory tier of the SlateDB block cache in bytes; defaults to 640 MiB, the memory-only default. |
| `HELIX_DISK_CACHE_WARM` | `on` (the default) or `off`, in any case: whether the server warms the disk cache in the background at startup. Rejected with memory storage, and with `HELIX_DATA_DIR` unless `HELIX_DISK_CACHE_DIR` is set. |

Size the container's memory for more than `HELIX_DISK_CACHE_MEMORY_BYTES`: the block
cache also indexes everything in `slate/` in memory. Once `slate/` fills, that index
takes roughly 2–9 MiB of RAM per GiB of `HELIX_DISK_CACHE_BYTES`, about 20–70 MiB
at the 8 GiB default and 2–9 GiB at 1 TiB. A restart rebuilds it from disk before
the server listens, briefly using about twice as much memory.

The block cache holds one file open per partition: its 3/8 share divided by a
power-of-two block of 64 KiB to 16 MiB, at most 32,768 files. With 2,024 more for
the object-store tier and the rest of the server, the minimum is 26,600 open files
at the default budget and never more than 34,792; open full-text split files come
on top. The server raises its soft open-file limit to the hard limit at startup. If
the hard limit is below the minimum, startup fails naming `HELIX_DISK_CACHE_BYTES`;
raise the hard limit with `--ulimit nofile=65536:65536`. Lowering the budget is not
a reliable fix above about 3 GiB, where the file count does not fall steadily with
it; budgets of 1 GiB or less need about 8,200 or fewer. Run natively on macOS, the
limit is also capped by `sysctl kern.maxfilesperproc`.

Startup also fails with a message naming the variable when a size is not a positive
integer (including non-UTF-8 text) or is out of range, `HELIX_DISK_CACHE_WARM` is
neither `on` nor `off`, a cache variable is set with `HELIX_DATA_DIR` but without
`HELIX_DISK_CACHE_DIR`, the default budget does not
fit, the directory or a tier subdirectory cannot be created or written, the
directory cannot be locked (some network and FUSE filesystems do not support
locks), or another running server already uses the directory. A server holds a
lock on `.helix-cache.lock` in the directory until its storage closes, so stop the
old container before starting its replacement on the same cache.

#### Upgrading S3 deployments from v0.0.8 and earlier

S3 storage used no local disk before, so an S3 container that started with v0.0.8
or earlier can fail or use more resources after the upgrade:

- The hard open-file limit must be at least 26,600 at the default budget. Budgets
  of 1 GiB or less need about 8,200 or fewer.
- RSS grows by the block cache's index, about 20–70 MiB at the default budget and
  twice that briefly after a restart; size memory limits for it.
- The cache directory must be writable. Mount a volume there with a read-only root
  filesystem, and set `HELIX_DISK_CACHE_DIR` when running as a user other than
  `65532` (for example on platforms that assign arbitrary UIDs).
- Without `HELIX_DISK_CACHE_BYTES`, the 8 GiB default must fit the cache's
  filesystem.

#### Index storage version 5

Images with asynchronous index publication upgrade a database's index storage
version from 4 to 5 the first time a writer opens it. The upgrade rewrites only
the version marker; no index is rebuilt. After it, v0.0.9 and earlier refuse to
open the database, so:

- Upgrade readers before the writer; current readers serve both versions.
- Never start a v0.0.9-or-earlier writer against an upgraded database, and exit
  an embedded process that gets `unsupported_index_storage_version` rather than
  keeping it running.
- Take a backup before upgrading if you may need to roll back.

## Test

After loading a native image, run the full packaging and runtime suite:

```bash
docker-image/test.sh \
  --platform linux/amd64 \
  --image ghcr.io/helixdb/helixdb:local-amd64
```

The suite inspects the saved image metadata and filesystem, scans it for credential material, exercises memory, native-volume, and disk-cache behavior, rejects invalid configuration, checks clean `SIGTERM` shutdown, and verifies S3-compatible persistence with a digest-pinned SeaweedFS image. It creates only `helixdb-image-*` Docker resources and removes them on exit.

The Compose stage runs SeaweedFS `weed mini` with a static S3 identity config
and a startup-created `helix-db` bucket. Before Helix writes anything, it
probes S3 conditional writes, which SlateDB needs to avoid silent data loss:
`If-None-Match: *` on an existing key and `If-Match` with a wrong ETag must both
return HTTP 412 and leave the object unchanged, and the matching create and
replace must succeed. SlateDB's writer and compactor race to advance the same
manifest, so the probe then sends eight concurrent creates of one new key, and
eight concurrent replaces carrying its current ETag, from one parallel curl
process: each race must end with exactly one HTTP 200, HTTP 412 for the rest,
and the winner's body stored. A passing race cannot prove the store atomic, but
a store that checks the condition and then writes without a lock is likely to
fail it. The probe runs against SeaweedFS directly and through the
request-logging proxy Helix uses. The stage then seeds a vector index, reopens
flushed data, and checks that three idle refresh intervals fetch no uncached
vector-data SST ranges (catalog polling is measured separately) while search
remains correct before and after a write. A test-only nginx proxy logs each
S3 request's method, path, and `Range` header for that check. Helix keeps its
disk cache on tmpfs there, with a 1 GiB budget, so each container starts with
a cold cache: reopened data must come from SeaweedFS, and hydration after the
restart reaches the trace. A range read again is served from the warm cache and
never reaches the trace, so this stage cannot see an idle refresh that
re-hydrates cached data; the DB contract `run_idle_refresh_contracts`, which
runs without the object-store cache, guards that.

| Dependency | Pinned reference |
| --- | --- |
| SeaweedFS 4.47 | `ghcr.io/chrislusf/seaweedfs:4.47@sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882` |
| nginx 1.30.5 (request log) | `ghcr.io/nginx/nginx-unprivileged:1.30.5-alpine@sha256:4714e0b1b2577eaa1a6131d07c958b67f0eb68e6d0521e90c6e5287db8cf0bc5` |

Both pins are multi-platform index digests from the projects' official GHCR
repositories and contain Linux amd64 and arm64 images; the SeaweedFS pin is
shared with the CLI disk runtime. SeaweedFS enforces S3 conditional writes from
4.09. MinIO's community images were withdrawn from Quay and Docker Hub, which is
why the suite no longer uses them. The Compose suite explicitly pulls both
dependencies for the requested platform before startup and fails if either pull
fails, even when images are cached. It does not remove or retag cached images.

When updating a pin, keep the Compose fixture, CLI defaults, CLI test fixtures,
and local-server docs aligned. Verify anonymous pulls and run the full suite on
both platforms.

Archive and secret-scanner unit tests can be run without Docker:

```bash
python3 -m unittest discover -s docker-image/tests -p 'test_*.py'
```

Asynchronous vector/text index-queue contracts are long-running and separate
from `test.sh`:

```bash
python3 docker-image/tests/index_queue_contracts.py --image IMAGE --scenario all
python3 docker-image/tests/index_queue_contracts.py --image BENCHMARK_IMAGE --scenario benchmark
```

`correctness` and `s3` check exact strong/eventual search through ingest,
restart, and `SIGKILL` on disk and S3-compatible storage; `member-limit` fills
the 250,000-member limit during a long build (100K existing rows by default);
`benchmark` checks the benchmark image's samples and a reader container. The S3
scenarios use a digest-pinned VersityGW gateway.

Pull requests and main-branch pushes build and run this suite natively for both amd64 and arm64. Automatic CI runs do not log in to GHCR or publish an image.


## Indexed equality benchmark

Build and load the baseline and candidate images, then compare them locally:

```bash
python3 docker-image/tests/indexed_equality_benchmark.py \
  --baseline-image helixdb:baseline \
  --candidate-image helixdb:candidate \
  --rows 50000 --samples 30 --output /absolute/path/results.json
```

This Linux arm64 benchmark tests 1–5 equalities on sparse, skewed, broad, and
small synthetic fixtures. Each response must match an independent source-data
oracle. It also checks bound parameters, nested conjunctions, reordered terms,
unindexed residuals, missing values, and reopened durable data. The JSON records
paired HTTP p50/p95 measurements, raw samples, result hashes, and image IDs.
HTTP timings include planning, execution, and serialization. Reopened reads
have cold process caches, not cold host disks. Disposable loopback containers
and volumes are removed on exit; images remain available to the caller.

## Release

Run the `Docker image` workflow manually from `main` with `release_version`
set to a new version tag matching `DEFAULT_LOCAL_IMAGE_TAG` in the CLI.
An empty version runs tests only. The release waits for workspace quality and
both native image suites, then loads their tested archives without rebuilding.
It publishes both architectures to GHCR, creates the versioned index, checks its
platforms, and updates `latest`. An existing version tag or registry lookup error
stops publication. Only the publication job has package write permission.

```text
native amd64 + arm64 build/test → tested archives
workspace checks + tested archives → versioned image → latest
published image → CLI release → fresh-install verification
```

Publish the Docker image before dispatching `cli.yml` so the new CLI default
is available when binaries are released. Keep the previous version tag for rollback.
