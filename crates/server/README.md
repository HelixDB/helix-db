# HelixDB server

The server exposes the same `HelixQueryService` contract over HTTP and gRPC.
Both transports accept at most 16 MiB of query JSON, preserve stable index
error codes, reject writer-only routing on reader handles, and can wait for a
writer flush before acknowledging a durable write.

See the [query error-code reference](../../docs/database/helix-db/query-guides/error-handling.mdx)
for the HTTP `error`/`msg` envelope, gRPC metadata contract, and complete catalog.

HTTP endpoints:

- `POST /v2/query` executes a serialized `QueryRequest`.
- `GET /healthz` reports process liveness and index readiness, and counts the
  index entities the writer's index worker holds back
  (`blocked_index_entity_count`); the writer's logs name each one.
- `GET /readyz` reports whether the configured database handle is ready.
- `GET /v2/diagnostics` runs read-only checks of the setup (storage, the S3
  bucket's region, the disk cache's device and room, memory, index
  publication, and recent queries' missing indexes); `helix doctor` renders it.

The `x-helix-warm`, `x-helix-require-writer`, and
`x-helix-await-durable` headers select the corresponding request contract.
The gRPC `QueryJsonRequest` fields have the same behavior. A write waits for
durability unless `x-helix-await-durable` is explicitly `false`.

Memory-backed standalone servers receive one non-forgeable process-local
database token. Disk and object-storage deployments open readers and writers
directly and need no external runtime authority.

The server test suite runs one shared mutation/read corpus through the embedded
database, `HelixQueryService`, HTTP router, and a real loopback gRPC server. It
also covers malformed and oversized messages, request deadlines, connection
churn, reader/writer routing, shutdown, restart, and persisted disk behavior.
