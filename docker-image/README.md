# HelixDB Docker image

This directory owns the build and test surface for the standalone HelixDB image. The build context is this repository; it does not require another checkout or sibling source directory.

The canonical image repository is `ghcr.io/helixdb/helixdb`. The scripts require an explicit platform and image tag so local and CI runs exercise the same artifact.

The published release is `ghcr.io/helixdb/helixdb:v0.0.8`, available for Linux amd64
and arm64. See the [local server guide](../docs/database/helix-db/start-here/local-development/local-server.mdx)
for release-image commands. The `local-amd64` and `local-arm64` tags below refer
to images built from your checkout.

## Container contract

- `/bin/helix-server` is PID 1 and runs as the distroless `nonroot` user (`65532:65532`).
- HTTP listens on `0.0.0.0:8080`; internal gRPC listens on `127.0.0.1:8081` and is not exposed.
- `GET /healthz`, `GET /readyz`, and `POST /v2/query` are the supported container probes and query endpoint.
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

`HELIX_DATA_DIR` and `S3_BUCKET` are mutually exclusive. Credentials are runtime-only and are never baked into the image. S3 storage always caches on local disk; see [Disk cache](#disk-cache).

Leave both variables unset for memory storage. Bind mounts and existing volumes
must be writable by the container's `65532:65532` user and group.

### Disk cache

With S3 storage the server always caches SlateDB blocks, object-store SST parts and
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
needs no `chown`. Run as another user or outside the image, set
`HELIX_DISK_CACHE_DIR` to a directory that user can write. Use one cache directory
per running server and per database: changing `DB_PATH` on the same directory
leaves the old database's full-text cache behind.

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

On a miss the object-store tier fetches and keeps a whole part of an SST: 4 MiB, or
less for budgets under 2 GiB so that the tier always holds at least 256 parts. Budget
at least twice the data the server reads often. Once that data outgrows half the
budget, parts keep evicting each other and cold reads fetch more from the object
store than memory-only caches would.

The budget must also fit on the cache's filesystem. At startup the server logs a
warning when `HELIX_DISK_CACHE_BYTES` exceeds the free space there plus what the
cache already occupies: the cache can then fill the filesystem, and if
`HELIX_DATA_DIR` shares it, durable writes fail too.

| Variable | Purpose |
| --- | --- |
| `HELIX_DISK_CACHE_DIR` | Disk cache directory, created with its `slate/`, `object-store/` and `fts/` subdirectories if needed. With S3 it defaults to `/var/cache/helix`. With `HELIX_DATA_DIR`, setting it enables the disk cache and leaving it unset keeps memory-only caches. Rejected with memory storage. |
| `HELIX_DISK_CACHE_BYTES` | Total disk budget in bytes, from 64 MiB to 1 TiB; defaults to 32 GiB. Half goes to object-store SST parts (`object-store/`), 3/8 to the SlateDB block cache (`slate/`), and the rest to full-text splits (`fts/`). With S3, `object-store/` also keeps the SSTs the server writes; with `HELIX_DATA_DIR` those are already on local disk, so it keeps only SSTs the server reads. |
| `HELIX_DISK_CACHE_MEMORY_BYTES` | Memory tier of the SlateDB block cache in bytes; defaults to 640 MiB, the memory-only default. |

Size the container's memory for more than `HELIX_DISK_CACHE_MEMORY_BYTES`: the block
cache also indexes everything in `slate/` in memory. Once `slate/` fills, that index
takes roughly 2–9 MiB of RAM per GiB of `HELIX_DISK_CACHE_BYTES`, about 70–280 MiB
at the 32 GiB default and 2–9 GiB at 1 TiB. A restart rebuilds it from disk before
the server listens, briefly using about twice as much memory.

The block cache holds one file open per partition: its 3/8 share divided by a
power-of-two block of 64 KiB to 16 MiB, at most 32,768 files. With 2,024 more for
the object-store tier and the rest of the server, the minimum is 26,600 open files
at the default budget and never more than 34,792; open full-text split files come
on top. The server raises its soft open-file limit to the hard limit at startup. If
the hard limit is below the minimum, startup fails naming `HELIX_DISK_CACHE_BYTES`;
raise the hard limit with `--ulimit nofile=65536:65536`. Lowering the budget is not
a reliable fix, since the file count does not fall steadily with it. Run natively on
macOS, the limit is also capped by `sysctl kern.maxfilesperproc`.

Startup also fails with a message naming the variable when a size is not a positive
integer (including non-UTF-8 text) or is out of range, a size is set with
`HELIX_DATA_DIR` but without `HELIX_DISK_CACHE_DIR`, the directory or a tier
subdirectory cannot be created or written, the directory cannot be locked (some
network and FUSE filesystems do not support locks), or another running server
already uses the directory. A server holds a lock on `.helix-cache.lock` in the
directory until its storage closes, so stop the old container before starting its
replacement on the same cache.

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
flushed data, and checks that three idle refresh intervals produce no
vector-data SST GETs (catalog polling is measured separately) while search
remains correct before and after a write. A test-only nginx proxy logs each
S3 request's method, path, and `Range` header for that check. Helix keeps its
default disk cache on tmpfs there, so each container starts with a cold cache:
reopened data must come from SeaweedFS, and hydration after the restart reaches
the trace. A range read again from the warm cache does not.

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
