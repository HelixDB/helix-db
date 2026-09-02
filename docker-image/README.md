# HelixDB Docker image

This directory owns the build and test surface for the standalone HelixDB image. The build context is this repository; it does not require another checkout or sibling source directory.

The canonical image repository is `ghcr.io/helixdb/helixdb`. The scripts require an explicit platform and image tag so local and CI runs exercise the same artifact.

## Container contract

- `/bin/helix-server` is PID 1 and runs as the distroless `nonroot` user (`65532:65532`).
- HTTP listens on `0.0.0.0:8080`; internal gRPC listens on `127.0.0.1:8081` and is not exposed.
- `GET /healthz`, `GET /readyz`, and `POST /v2/query` are the supported container probes and query endpoint.
- Storage is in memory unless local-disk or S3-compatible configuration is supplied.
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

`HELIX_DATA_DIR` and `S3_BUCKET` are mutually exclusive. Credentials are runtime-only and are never baked into the image.

To route only WAL objects through another S3-compatible client, set at least one
of `WAL_S3_BUCKET`, `WAL_AWS_ENDPOINT`, or `WAL_AWS_ENDPOINT_URL_S3`:

| Variable | Purpose |
| --- | --- |
| `WAL_S3_BUCKET` | WAL bucket; defaults to `S3_BUCKET`. |
| `WAL_S3_REGION` | WAL region; defaults to the resolved main S3 region. |
| `WAL_AWS_ENDPOINT` | WAL endpoint; `WAL_AWS_ENDPOINT_URL_S3` is the fallback, then the main endpoint. |
| `WAL_AWS_ALLOW_HTTP` | WAL HTTP policy; defaults to `AWS_ALLOW_HTTP`. |

The WAL client uses the standard AWS credential variables and the same `DB_PATH`.
For a cache or write-through proxy over the main bucket, set only a WAL endpoint;
the main `S3_BUCKET` and path are inherited. The proxy must expose the existing WAL
objects at that path because HelixDB does not migrate them. A WAL endpoint without
`S3_BUCKET` or `WAL_S3_BUCKET` is rejected. WAL region or HTTP overrides without a
WAL bucket or endpoint are also rejected.

## Test

After loading a native image, run the full packaging and runtime suite:

```bash
docker-image/test.sh \
  --platform linux/amd64 \
  --image ghcr.io/helixdb/helixdb:local-amd64
```

The suite inspects the saved image metadata and filesystem, scans it for credential material, exercises memory and native-volume behavior, rejects invalid configuration, checks clean `SIGTERM` shutdown, and verifies S3-compatible persistence with digest-pinned MinIO images. It creates only `helixdb-image-*` Docker resources and removes them on exit.

Archive and secret-scanner unit tests can be run without Docker:

```bash
python3 -m unittest discover -s docker-image/tests -p 'test_*.py'
```

Pull requests and main-branch pushes build and run this suite natively for both amd64 and arm64. CI does not log in to GHCR or publish an image.
