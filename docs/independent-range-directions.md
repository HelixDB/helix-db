# Independent range directions

A node or edge range index is identified by scope, element kind, label,
property, and direction. Creating ASC does not create DESC. Both consume
separate index storage and write maintenance. Equality (including uniqueness),
vector and text identities keep their current encodings.

## Codec audit

Physical secondary range keys already contain index ID, generation, directional
lane, value and entity ID. These keys and values do not change. Canonical record
keys and values and their retained operation identities need direction. The
ascending identity keeps tag 0x02; descending uses 0x05. Record definitions already
contain direction. The outbox atomically evicts the previous terminal operation
on recreate, so the retained operation belongs to the current definition.
Global queue pointers contain IDs and revisions, not range identity. Text
compaction identities are text-only. Applied-state and build-delta keys use
index ID and generation. None of those layouts change.

## Migration contract

Storage V6 is the new serving format (V5 is a retired experimental format and
remains rejected). The controlled migration target schema is 2.

1. Finish the existing tenant-envelope and V4 equality migration when required.
2. Validate range canonical records, retained operations, pointers and exact cursor
   ownership. Reject missing links, destination conflicts and malformed metadata.
3. Publish V6 before changing any catalog pair. V6 without
   `kv_migration_ready:range_directions` is a migration-in-progress state.
4. For each old descending index, atomically move its canonical key and rewrite
   its record and retained operation. Keep all IDs, generations, revisions,
   progress, execution state and physical data. A transaction contains one pair;
   no database-size transaction or in-memory row collection is needed.
5. Validate all current range catalog pairs, then publish readiness. A restart
   repeats the scan and skips committed pairs. No index becomes Active as part
   of migration. An additional direction still requires normal backfill.

Catalog discovery requires complete keyspace scans because tenant scopes do not
have a separate global registry. Runtime serving keeps point lookups. Migration
cost therefore includes scanning existing keys even though writes are metadata
only.

## Required production rollout

This branch does not deploy or migrate any production database. Before using it:

- Take a durable rollback checkpoint with all writers stopped.
- Drain old readers and wait for in-flight queries before starting the controlled
  migration. Cached old snapshots are not a substitute for reader retirement.
- Update the rollout controller to authorize target schema 2 and the exact new
  revision. Managed `RecoverOnly` refuses old or incomplete storage. Embedded
  writer startup performs the migration, as with its other storage migrations.
- Run the authorized writer migration, verify readiness and the full catalog,
  then start new readers. Reader startup and each catalog refresh validate the
  same serving gate. Old binaries reject V6 by their maximum-version check.
- Resume traffic only after query and DDL acceptance checks. Rollback after V6
  publication requires restoring the pre-migration checkpoint; do not restart
  an old binary on migrated storage.

Both directions can remove an otherwise necessary ordering sort. They do not
change the executor's range-result materialization or prove a latency improvement.
