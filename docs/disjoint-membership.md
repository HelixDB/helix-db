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

The current and maximum supported index storage version is **7**. Canonical
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

Versions 6 and 7 move label index rows from the label's 64-bit hash to the exact
label (its big-endian `u16` byte length followed by its UTF-8 bytes) while the
writer serves. An embedded or controlled-migration writer opening a version-5 store
starts the move in one commit after the blocking startup migrations: version 6, or
version 7 directly when the store has no hash label row. The open copies nothing,
so it takes about as long on a store of any size. At version 6 readers keep reading
the complete hash rows, and every graph write files each label row change under
both its hash and its exact key with the same members. The background migration
worker copies the hash rows written before then: each step walks every scope's
hash rows in key order on one iterator, seeking past other rows, and copies at most
16,384 rows, 8 MiB (or a sixteenth of the writer's `max_unflushed_bytes`, if that is
less), and 262,144 members in one transaction that begins before it reads. A step
whose commit takes over 500 ms halves the next step's limits, and quicker commits
grow them back. Each exact row is a disjoint membership merge whose tokens are the members it
adds, so a graph write that removes or relabels a copied member after the step
began conflicts with it in either order, and the step is taken again with halved
limits; writes that add other members do not conflict. A row with more members
than one step copies is copied a range of members at a time. Each step records its
last row, or the last member of a partly copied row, so any later step resumes
there, a failover writer's included. A row is named by its lowest live member whose
label hashes to it; live members whose labels hash elsewhere, or whose records have
no string `$label`, are stale leftovers of a label change and are not copied, and a
highest such member with a different label (two labels sharing a hash) fails the
copy closed, leaving the store serving at version 6. Neighbor rows take their labels
from the scope's exact edge label rows. Once a step finds no hash row left it
flushes and publishes version 7 in one durable commit, and readers switch to the
exact rows at once. Until its writer has seen publication a graph transaction reads
the storage marker in its serializable snapshot, so one that read version 6
conflicts with the publishing commit rather than filing hash rows after it. After
publication the worker deletes the hash rows while the writer serves, one tuned
batch and at most 256 scopes per step, resuming after the last deleted row or the
scope it stopped before. Every writer takes the copy and cleanup steps, a managed
failover writer included, though it runs no other migration, so recovery-only
managed failover serves a version-6 store; it reports `WriterMigrationRequired` for
a version-5 store. With the worker disabled the store keeps serving at version 6,
reading hash rows and writing both, until a step is taken. Binaries that support at
most version 5 refuse a version-6 store, which they would not keep exact, so a
rollback after the first open needs a backup taken before the upgrade. Labels
longer than the label index bound are rejected as invalid queries.

New databases initialize at version 7. Versions after 7 return the existing
unsupported-version error on reader, embedded-writer, and managed-failover open;
startup must not lower or remove their marker. Managed bootstrap still refuses
any nonempty store before version dispatch. The
retired activation key tag `0x0C` and value tag `0x08` are not reused. Malformed
metadata checks, managed-writer fencing, and existing version-2/3 equality
migrations (which publish version 5, after which the same open starts the label
identity move) remain in place.

The change does not alter cascade ordering, intermediate topology flushes,
`drop_nodes`, token containers, or SlateDB.
