# Helix anonymous telemetry

This crate defines the typed JSON events sent by the Helix CLI, server, and
embedded runtime to:

```text
POST https://telemetry.helix-db.com/v1/events
Content-Type: application/json
```

`HELIX_TELEMETRY_ENDPOINT` overrides the endpoint. The only accepted sources
are `helix-cli`, `helix-server`, and `helix-embedded`.

## Attribution and query counts

Every envelope's `client` object identifies the sender with pseudonymous
values only. `crates/metrics/src/attribution.rs` defines them:

| Field | Value |
|---|---|
| `version` | The sender's own version. The server adds its image build commit, for example `0.1.0+b524727a2c1f`. |
| `installation_id` | Random UUID per Helix home directory. It changes in every fresh container or CI run. |
| `session_id` | Random UUID per telemetry runtime. |
| `project_id` | Salted SHA-256 of the git repository (`host/owner/repo`), taken from CI variables or the working directory's `origin` remote. `helix start` passes it to the container as `HELIX_TELEMETRY_PROJECT_ID`. |
| `deployment_id` | `HELIX_DEPLOYMENT_ID` as set, or a salted SHA-256 of an AWS S3 bucket and `DB_PATH`. Custom S3 endpoints get none, because local S3 servers reuse bucket names. |
| `environment`, `ci_provider` | `ci` (with a provider such as `github_actions`), `kubernetes`, `container` or `host`. |

The same repository gives the same `project_id` on a laptop, in CI and in
local containers, so short-lived installations can be linked back to one user.
No repository name, bucket name or IP address is sent.

Helix's own traffic sets `HELIX_DEPLOYMENT_ID`: `helix-internal-ci` in CI
workflows, `helix-internal-dev` for cargo runs in this repository
(`.cargo/config.toml`) and for test containers, and `helix-internal-cloud` in
Cloud pods. Exclude these values when counting open-source usage.

Server and embedded runtimes count every query they record, whether its
`query.execute` event is delivered or not. While the counts change, they send a
`query.summary` event every 60 seconds and once more at shutdown:

```json
{"queries": 1500, "dropped_invalid": 0, "dropped_queue_full": 500}
```

The counts are cumulative per `session_id`, so the true number of queries is
the sum over sessions of each session's largest `queries`. Events dropped by
the bounded queue or rate-limited by the ingest service are still counted.

## Privacy and limits

Query events include the canonical query AST, latency, outcome, errors or
warnings, planner diagnostics, optional `cluster_id` from `HELIX_CLUSTER_ID`,
and optional `tenant_id` from the query request's `x-helix-tenant-id` header.
Embedded requests have no transport tenant header and omit `tenant_id`. Events
never include returned rows, parameters, embeddings, or email. `full`
telemetry may include `user_id`; `basic` strips it and `off` disables
telemetry.

The client enforces the ingestion contract before sending:

- at most 500 events per envelope;
- at most 1 MiB per envelope;
- at most 16 KiB per event properties object. A larger query event drops
  `planner_diagnostics` first, then replaces `canonical_query` with
  `{"omitted":"too_large"}`, so it is still sent and counted.

Server and embedded recording is bounded and non-blocking. Delivery failures do
not affect query execution or change public query responses.

## CLI durability

The CLI atomically writes complete envelopes to
`~/.helix/metrics/spool/<uuid>.json` before delivery. A file is removed after
any HTTP response, including non-202 responses, because only a request with no
response is safe to retry. Pending files survive timeouts and are pruned after
30 days or when the spool exceeds 16 MiB. Obsolete protobuf, partial, rejected,
and legacy daily telemetry files are discarded.

Authenticated `GatewayWfeService.SendQueryEvents` is not part of this crate.
That cloud-only transport is owned by the gateway in `helix-hyperscale`.
