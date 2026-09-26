# Helix CLI

The v3 CLI manages local Helix instances and WorkOS-session-authenticated Helix Cloud resources.

- Local: `init`, `add`, `start`, `stop`, `restart`, `status`, `logs`, `query`, `shell`, `prune`.
- Cloud discovery/resources: `workspace`, `project`, `cluster`, `database`, `service-credential`, `api`.
- Cloud queries: `query` and `shell` execute through the backend query broker.
- Authentication: `auth login|status|logout` stores only a rotating WorkOS session.

The Cloud CLI accepts no API-key login, service-credential login, direct gateway path, or custom
query authorization. Tenant creation returns a default read-write application key once for direct
gateway clients; the CLI displays but never stores or uses it. Additional application keys remain
explicitly managed secrets. `push` and `sync` are removed.

See [the CLI docs](../../docs/cli/command-reference.mdx).

## Local image selection

The CLI defaults to its tested image version. `latest` is opt-in:

```bash
helix start dev --image-version latest
helix start dev --image-version v0.0.6 --persist
helix start dev --pull never
```

`--image-version` accepts a tag or `sha256:<64 lowercase hex digits>`. The
repository remains the instance's configured `image`. `--persist` saves the
resolved image, pull, port, and storage settings to `helix.toml`; otherwise flags
apply only to that invocation.

```toml
[local.dev]
image = "ghcr.io/helixdb/helixdb"
tag = "v0.0.6"
pull = "missing"
```

Flags override configuration. Without a configured policy, `latest` pulls on
every start and other tags/digests pull only when absent. `--pull always` requires
a successful pull, `--pull missing` permits cached images, and `--pull never`
requires cached images. Explicit pull policies also apply to the disk-mode
SeaweedFS image; its default is `missing`.

Start resolves all required images before saving overrides or replacing containers.
A failed image resolution leaves `helix.toml` unchanged. Helix and SeaweedFS
start by their resolved immutable image IDs.

Disk mode runs SeaweedFS (`ghcr.io/chrislusf/seaweedfs:4.47`, pinned by digest)
as a private S3 sidecar with the `helix-db` bucket, stored in the
`helix-<project>-<instance>-seaweedfs-data` volume. Releases before the switch
used MinIO, whose images are no longer published. On `start`, `stop`, and
`prune` the CLI removes an old `-minio` sidecar; it keeps the unreadable
`-minio-data` volume until `helix prune`. See the
[local workflow guide](https://docs.helix-db.com/cli/workflows/local#migrate-minio-disk-data)
to copy that data.
`helix restart dev` restarts the existing container with its existing image and
settings and checks readiness on the container's published port. It fails if no container exists. Use `helix start dev` to apply new
image or configuration settings. Restarting in-memory storage clears its data.
