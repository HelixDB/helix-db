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

The current and maximum supported index storage version remain **4**. Canonical
values and the existing `HLXRBM2` / `HLXADJ2` membership-delta codecs are unchanged.
WAL replay, partial merging, and compaction must retain support for those operands.
No index rebuild or new storage migration is needed for this unreleased change.

**Do not open a database containing these delta operands with a pre-delta binary.**
Version 4 does not distinguish those binaries. Mixed-version operation and rollback
to a binary without these decoders are not supported. This is an explicit
development-only compatibility boundary, not an automatic upgrade protocol.

New databases initialize at version 4. Existing supported version-4 databases open
without conversion. Experimental version-5 databases return the existing
unsupported-version error on reader, embedded-writer, and managed-failover open;
startup must not lower or remove their marker. Managed bootstrap still refuses
any nonempty store before version dispatch. The
retired activation key tag `0x0C` and value tag `0x08` are not reused. Malformed
metadata checks, managed-writer fencing, and existing version-2/3 migrations to
version 4 remain in place.

The change does not alter cascade ordering, intermediate topology flushes,
`drop_nodes`, token containers, or SlateDB.
