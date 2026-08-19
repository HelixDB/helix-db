# HelixDB server

The server exposes the same `HelixQueryService` contract over HTTP and gRPC.
Both transports accept at most 16 MiB of query JSON, preserve stable index
error codes, reject writer-only routing on reader handles, and can wait for a
writer flush before acknowledging a durable write.

See the [query error-code reference](../../docs/database/helix-db/query-guides/error-handling.mdx)
for the HTTP `error`/`msg` envelope, gRPC metadata contract, and complete catalog.

HTTP endpoints:

- `POST /v2/query` executes a serialized `QueryRequest`.
- `GET /healthz` reports process liveness and index readiness.
- `GET /readyz` reports whether the configured database handle is ready.

The `x-helix-warm`, `x-helix-require-writer`, and
`x-helix-await-durable` headers select the corresponding request contract.
The gRPC `QueryJsonRequest` fields have the same behavior.

Memory-backed standalone servers receive one non-forgeable process-local
database token. Disk and object-storage deployments open readers and writers
directly and need no external runtime authority.

The server test suite runs one shared mutation/read corpus through the embedded
database, `HelixQueryService`, HTTP router, and a real loopback gRPC server. It
also covers malformed and oversized messages, request deadlines, connection
churn, reader/writer routing, shutdown, restart, and persisted disk behavior.

[`scripts/server-production-coverage.sh`](../../scripts/server-production-coverage.sh)
measures that corpus against server production source and `db::query_service`
compiled without DB test-only internals. The exact uncovered-line digest and
covered-line floors make every coverage change an explicit ratchet review. See
the [cloud-launch test guide](../../docs/CLOUD_LAUNCH_TESTING.md) for the
complete cadence and release contract.
