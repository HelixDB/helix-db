# Migration parity harness

The harness is pinned to legacy Hyperscale
`e5bac15b020c9acac1649c44b58a2cf16dd1f874` and tests the checked-out Helix
Proper revision. The immutable corpus was established against Proper
`31ae01c74a271e322dfbea59210f0ab379a545fb`; every run checks the current
implementation against those same goldens and records its exact revision. It
also verifies raw persisted key families, logical graph streams, physical V2
memberships, searches, CRUD, rollback, crash recovery, compaction, garbage
collection, object-store faults, and scale bounds.

The one intentional persisted-key difference is descending range encoding.
The corpus freezes Hyperscale's legacy bytes and independently derives the
new Proper bytes, while migration parity requires the resulting logical node
and edge memberships and exact query ID sets to remain unchanged. Proper must
continue writing its new canonical descending representation; matching the
legacy physical bytes would fail the contract.

The checked-in `hash_contract_v1.json` is generated only by the diagnostic
`--emit-hash-contract-golden` command. Normal tests never rewrite or approve
goldens. A mismatch is a compatibility failure that requires restoring the
persisted byte contract or designing an explicit migration.

Named profiles are:

- `contracts`: production-linked DB migration contracts.
- `dev`: all migration modes at 1k nodes/4k edges plus hash and corruption
  contracts, with a hard 600-second aggregate limit and a 300-second limit for
  each individual mode.
- `full-correctness`: six distributions, batch sizes 1 and 1,024, every crash
  boundary, and the SeaweedFS operation-fault matrix.
- `scale-local`: the progressive 5k/20k through 2M/8M local ladder.
- `scale-seaweedfs`: the same ladder with isolated SeaweedFS prefixes and
  verified cleanup.
- `full`: full correctness followed by both scale ladders.

For example:

```bash
scripts/run-migration-parity.sh dev

scripts/run-migration-parity.sh scale-local

SEAWEEDFS_ENDPOINT=http://127.0.0.1:8333 \
SEAWEEDFS_BUCKET=helix-migration-parity \
scripts/run-migration-parity.sh full-correctness
```

The SeaweedFS profiles expect an S3 endpoint with the bucket already created
and credentials in `SEAWEEDFS_ACCESS_KEY` and `SEAWEEDFS_SECRET_KEY` (default
`helix` / `helix-local-secret`). SeaweedFS 4.09 or later enforces the S3
conditional writes SlateDB needs. For example:

```bash
docker run -d --name helix-parity-seaweedfs -p 127.0.0.1:8333:8333 \
  -e AWS_ACCESS_KEY_ID=helix -e AWS_SECRET_ACCESS_KEY=helix-local-secret \
  ghcr.io/chrislusf/seaweedfs:4.47@sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882 \
  mini -dir=/data -bucket=helix-migration-parity -master.telemetry=false
```

Official scale runs keep the 1:4 node/edge ratio, power-law distribution,
migration batch 1,024, and seed batch 10,000. All three modes run through the
100k/400k rung. The slowest mode is selected with deterministic tie ordering
for 500k/2M and 2M/8M. Each rung must pass exact parity, fitted and worst-pair
exponents, per-row amplification, and the 1.5× next-rung capacity projection
before the runner advances.

Reports remain under `target/migration-parity-reports` unless
`MIGRATION_PARITY_REPORT_DIR` is set. Large reports are bounded to 16 MiB and
contain counts, SHA-256 digests, first differences, resource evidence, and
projections rather than per-record JSON.
