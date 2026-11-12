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

## Edge endpoints

An edge insert needs its endpoints to exist, not to stay unchanged, so a
concurrent property write must not abort it. A read that only learns whether a
node row exists takes no conflict dependency on that row. This is serializable
for four reasons:

1. The insert's outcome depends on existence alone. It uses no byte of the row.
2. Node IDs are never reused, so a row that exists at the snapshot can only stop
   existing through a node delete.
3. A node delete always tombstones the node's adjacency row in the same
   transaction. A tombstone is an exclusive write.
4. The insert stages a disjoint merge on both endpoints' adjacency rows, and an
   exclusive write conflicts with a disjoint merge whatever its members.

So an insert and a delete of either endpoint cannot both commit, in either order.
A property write leaves existence alone, so the insert serializes after it.

The argument needs the merge. At commit, an existence read whose node has no
staged adjacency merge becomes an ordinary dependency, and so does any read that
found no node. A read of a row's content always conflicts, because changing that
row can change what the statement selected. So does a scan. This means a Cypher
`MATCH` keeps conflicting with a `SET` on the nodes it read: the insert only
passes a busy node when it addresses that node by ID.

## Storage contract

The current and maximum supported index storage version is **6**. Canonical
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

Version 6 marks stores whose label index rows are keyed by the exact label (its
big-endian `u16` byte length followed by its UTF-8 bytes) instead of the label's
64-bit hash. An embedded or controlled-migration writer migrates a version-5 store
during open, after the blocking startup migrations and before it serves any graph
write. It copies each node label, edge label, and edge-label neighbor row to its
exact key, leaving every hash row in place, and publishes version 6 in one commit
once every scope is copied. A row is named by its lowest live member whose label
hashes to it; live members whose labels hash elsewhere are stale leftovers of a
label change and are not copied, and a highest such member with a different label
(two labels sharing a hash) fails the open closed. Until publication readers of
any version-5-capable binary keep reading complete hash rows, and a failed or
interrupted copy leaves an ordinary version-5 store that older binaries still
open. Each copy batch records its last row and the open's SlateDB writer epoch:
the next writer open resumes after it when that epoch is its own predecessor's,
and otherwise, since another writer (possibly an older binary) may have changed
the hash rows, clears the partial copy and copies again. After publication
readers read only exact rows, and the background migration worker deletes the
hash rows while the writer serves, one tuned batch per step beside the other
background migrations, resuming after the last deleted row; with the worker
disabled they stay until a step is taken. Recovery-only managed failover reports
`WriterMigrationRequired` for a version-5 store. Binaries that support at most version 5 refuse a version-6
store, so a rollback after publication needs a backup taken before the upgrade.
Labels longer than the label index bound are rejected as invalid queries.

New databases initialize at version 6. Versions after 6 return the existing
unsupported-version error on reader, embedded-writer, and managed-failover open;
startup must not lower or remove their marker. Managed bootstrap still refuses
any nonempty store before version dispatch. The
retired activation key tag `0x0C` and value tag `0x08` are not reused. Malformed
metadata checks, managed-writer fencing, and existing version-2/3 equality
migrations (which publish version 5, after which the same open runs the label
identity migration to version 6) remain in place.

The change does not alter cascade ordering, intermediate topology flushes,
`drop_nodes`, token containers, or SlateDB.
