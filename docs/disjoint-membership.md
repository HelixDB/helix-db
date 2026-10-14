# Shared membership writes

Normal writer startup uses checked disjoint membership merges immediately. There
is no activation API, stored mode, or exclusive fallback. This applies to node
labels, topology, node and edge equality indexes, and search membership rows.

Each existing flush emits one merge per changed physical row. Its conflict tokens
contain every changed member, including removals. Adjacency tokens include the
direction, so incoming and outgoing membership for the same neighbor are distinct.
Checked staging validates the existing values before it stages the batch. It does
not turn those validation reads into ordinary row observations. Explicit query
reads and ranges still conflict, as do overlapping tokens, entity-record writes,
unique ownership checks, and real graph dependencies.

## Storage contract

The current and maximum supported index storage version is **5**. Canonical
values and the existing `HLXRBM2` / `HLXADJ2` membership-delta codecs are unchanged.
WAL replay, partial merging, and compaction must retain support for those operands.
The membership deltas themselves need no index rebuild or storage migration.

**Do not open a database containing these delta operands with a pre-delta binary.**
Versions 4 and 5 do not distinguish those binaries. Mixed-version operation and
rollback to a binary without these decoders are not supported. This is an explicit
development-only compatibility boundary, not an automatic upgrade protocol.

Version 5 marks stores that may hold asynchronous index-operation queues. It
shares version 4's physical layout: an embedded or controlled-migration writer
upgrades a version-4 store by rewriting only its storage marker, with no index
rebuild. Current readers serve version 4 and 5, so upgrade readers before the
writer. Recovery-only managed failover reports `WriterMigrationRequired` for a
version-4 store instead of upgrading it. Binaries that support at most version 4
refuse an upgraded store, so a rollback needs a backup taken before the upgrade.

New databases initialize at version 5. Versions after 5 return the existing
unsupported-version error on reader, embedded-writer, and managed-failover open;
startup must not lower or remove their marker. Managed bootstrap still refuses
any nonempty store before version dispatch. The
retired activation key tag `0x0C` and value tag `0x08` are not reused. Malformed
metadata checks, managed-writer fencing, and existing version-2/3 equality
migrations (which now publish version 5 directly) remain in place.

The change does not alter cascade ordering, intermediate topology flushes,
`drop_nodes`, token containers, or SlateDB.
