# HelixDB Docker image

This directory owns the build and test surface for the standalone HelixDB image. The build context is this repository; it does not require another checkout or sibling source directory.

The canonical image repository is `ghcr.io/helixdb/helixdb`. The scripts require an explicit platform and image tag so local and CI runs exercise the same artifact.

The published release is `ghcr.io/helixdb/helixdb:v0.0.5`, available for Linux amd64
and arm64. See the [local server guide](../docs/database/helix-db/start-here/local-development/local-server.mdx)
for release-image commands. The `local-amd64` and `local-arm64` tags below refer
to images built from your checkout.

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

Leave both variables unset for memory storage. Bind mounts and existing volumes
must be writable by the container's `65532:65532` user and group.

## Test

After loading a native image, run the full packaging and runtime suite:

```bash
docker-image/test.sh \
  --platform linux/amd64 \
  --image ghcr.io/helixdb/helixdb:local-amd64
```

The suite inspects the saved image metadata and filesystem, scans it for credential material, exercises memory and native-volume behavior, rejects invalid configuration, checks clean `SIGTERM` shutdown, and verifies S3-compatible persistence with digest-pinned MinIO images. It creates only `helixdb-image-*` Docker resources and removes them on exit. The MinIO test also seeds a vector index, reopens flushed data, and checks that three idle refresh intervals produce no vector-data SST GETs (catalog polling is measured separately) while search remains correct before and after a write.

MinIO and `mc` use the upstream `quay.io/minio` repositories. The server
`RELEASE.2025-09-07T16-13-09Z` and client `RELEASE.2025-08-13T08-35-41Z`
are pinned to multi-platform index digests shared with the CLI disk runtime.
These are the same digests previously used through Docker Hub, whose MinIO
repositories no longer allow anonymous pulls. Both pins contain Linux amd64
and arm64 images. The Compose suite explicitly pulls both dependencies for the
requested platform before startup and fails if either pull fails, even when
images are cached. It does not remove or retag cached images.

When updating a pin, keep the Compose fixture, object-check client in
`compose-smoke.sh`, CLI defaults, CLI test fixtures, and local-server docs aligned.
Verify anonymous pulls and run the full suite on both platforms.

Archive and secret-scanner unit tests can be run without Docker:

```bash
python3 -m unittest discover -s docker-image/tests -p 'test_*.py'
```

Pull requests and main-branch pushes build and run this suite natively for both amd64 and arm64. Automatic CI runs do not log in to GHCR or publish an image.


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
