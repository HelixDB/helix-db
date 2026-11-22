//! Source rows of one build's label, read through the label's bitmap.
//!
//! Every index definition names one label, and every node and edge write
//! keeps its label's bitmap in the same transaction as its row (the node
//! label row for nodes, the global edge-label row for edges), as label scans
//! already rely on. A build's source scan therefore reads only the rows of
//! its label's members, rather than every row of its kind.
//!
//! The bitmap is read in the layout of the snapshot it is read from, which
//! the storage marker in that same snapshot names ([`label_layout`]): hash
//! rows until the label identity copy publishes, while the copy runs
//! included, since its exact rows are incomplete until then; exact rows from
//! the publication on. Each step reads its own snapshot, so a build that
//! spans the publication reads hash rows in its steps before it and exact
//! rows after, each complete in its own snapshot.
//!
//! Every step reads and decodes the label's whole bitmap, so a build of `N`
//! members reads `N / max_entities` bitmaps: quadratic in `N`, with a small
//! constant. Decoding one on a c6i.8xlarge took 0.5 ms at 10M members (about
//! 10 s across a build at 512 a step) and 7 to 22 ms at 100M (dense or every
//! third id; 20 to 70 minutes across a build, a few percent to a tenth of its
//! row reads). A label far past that would keep the bitmap across steps.
//!
//! The scan keeps the progress it always had: the cursor is the complete
//! property-by-ID key of the last member taken, whether or not its row still
//! exists, so ascending ids keep the key order the stored cursor and
//! inclusive upper bound are compared in, and a build resumes across binaries
//! either way. A member whose row is gone is skipped. A member that carries
//! another label (a label-hash collision, which only a hash row can hold) is
//! read and left to each driver's own label check, which writes nothing for
//! it.
//!
//! A step reads its ids' rows in one of two ways ([`Reads`]), chosen from
//! the ids it took. When they fill at least half of the key range from the
//! first to the last ([`RANGE_DENSITY`]), one scan of that range reads them
//! and the rows of other labels between them, as the prefix scan before it
//! did, since a point read costs more than a row a scan passes: a dense 1M
//! row label built in 30 s by point reads against the prefix scan's 25 s,
//! and in 25 s this way. Such a step reads at most the rows the prefix scan
//! read for the same ids, and at most twice the label's. Otherwise
//! rows are read with `multi_get`, in chunks of at most [`POINT_CHUNK_IDS`]
//! ids, each read only once the previous one is consumed. Either way no row
//! is read once the step has read its input-byte limit, counting every row
//! read, of the label or not, so a scanning step reads at most one row past
//! its limit and a point step at most [`POINT_CHUNK_IDS`] rows past it,
//! whatever their sizes. Within that bound a chunk takes at most as many
//! ids as the step has read so far ([`FIRST_CHUNK_IDS`] first), and once a
//! row is read, no more ids than reach the input bytes the step has left at
//! the mean bytes per id read so far, so a label of rows of one size reads
//! about its limit and a row or two more per step, rather than a whole chunk
//! of rows the next step reads again. A scan starts at the step's first id,
//! so a row of another label it read is never read again by a later step.
//! Like the prefix scan before them, a build's reads do not fill the block
//! cache.

use bytes::Bytes;
use slatedb::DbReadOps;

use crate::config::{IndexLifecycleScanTuning, SearchIndexBatchLimits};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, EdgePropertyByIdKey, NodePropertyKey};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::repository::{LabelRowLayout, LabelRowWrites};
use crate::index_lifecycle::{IndexCursor, IndexElementKind, IndexEntityId, SourceScanProgress};

/// The most ids one `multi_get` reads. A step reads no chunk once it has
/// read its input-byte limit, so it reads at most this many rows past that
/// limit, whatever their sizes. The bound costs calls: a 50k-member label at
/// every 20th of 1M rows of about 140 B, built by point on an m6id.16xlarge
/// with local NVMe, took 4.0 s, against 2.8 s at 64 ids and 2.45 s at 256,
/// whose chunks read up to 256 rows past the limit.
const POINT_CHUNK_IDS: usize = 16;

/// The ids a step reads before it knows how large the label's rows are: one
/// such chunk of the largest rows a step admits stays near its byte limit.
const FIRST_CHUNK_IDS: usize = 4;
const _: () = assert!(FIRST_CHUNK_IDS <= POINT_CHUNK_IDS);

/// A step scans its ids' key range when the range holds at most this many
/// keys per id it took: a dense label then reads no more than this many
/// rows per member, and a sparse one reads only its members.
const RANGE_DENSITY: u64 = 2;

/// One present source row of the label.
pub(crate) struct SourceRow {
    /// The entity the row belongs to.
    pub(crate) entity_id: IndexEntityId,
    /// The row's complete property-by-ID key, a valid cursor.
    pub(crate) key: Bytes,
    /// The row's stored properties.
    pub(crate) value: Bytes,
}

/// What [`LabelSource::next`] found.
pub(crate) enum LabelRead {
    /// The next present row, in ascending id order.
    Row(SourceRow),
    /// The label holds no further member up to the upper bound: the scan is
    /// complete once the rows already returned are reconciled.
    Exhausted,
    /// The ids this step took are read, and members remain: the scan resumes
    /// after this cursor, the key of the last id taken, once the rows already
    /// returned are reconciled.
    Paused(IndexCursor),
}

/// The label's members after a build's cursor, up to its upper bound and at
/// most one step's entities, read from one snapshot.
pub(crate) struct LabelSource<'a, R> {
    reader: &'a R,
    scope: DataScope,
    kind: IndexElementKind,
    reads: Reads,
    /// Whether the label holds members after the taken ids, up to the bound.
    more: bool,
    read: Read,
}

/// How a step reads the rows of the ids it took (see the module docs).
enum Reads {
    Points(PointReads),
    /// Boxed: a scan's iterator is several times the size of point reads.
    Range(Box<RangeReads>),
}

/// Point reads of the taken ids, a chunk at a time.
struct PointReads {
    /// Ids taken and not yet read, ascending.
    ids: std::vec::IntoIter<u64>,
    /// The chunk being returned: each id, its key and its row if present.
    chunk: std::vec::IntoIter<(u64, Bytes, Option<Bytes>)>,
    /// Ids read so far, present or not.
    ids_read: u64,
}

/// One scan from the first taken id's key to the last's.
struct RangeReads {
    /// Ids taken and not yet passed, ascending.
    ids: std::iter::Peekable<std::vec::IntoIter<u64>>,
    rows: slatedb::DbIterator,
}

/// What a step has read so far.
struct Read {
    /// The last id read or passed.
    last: Option<u64>,
    /// Key and value bytes of the rows read, of the label or not.
    bytes: u64,
    /// The step's input-byte limit: no row is read once `bytes` reaches it.
    limit: u64,
}

impl Read {
    fn add(&mut self, key: &[u8], value: &[u8]) {
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(key.len() + value.len()).unwrap_or(u64::MAX));
    }

    fn spent(&self) -> bool {
        self.bytes >= self.limit
    }
}

impl<'a, R: DbReadOps + Send + Sync> LabelSource<'a, R> {
    /// Takes the members of `label` after `progress`'s cursor, up to its
    /// inclusive upper bound, at most `limits.max_entities()` of them, from
    /// the bitmap `reader` holds in its own layout ([`label_layout`]).
    /// `None` when no member is left: the scan is complete. A step therefore
    /// returns at most `limits.max_entities()` rows, and a driver ends its
    /// step on [`LabelRead::Exhausted`] or [`LabelRead::Paused`] rather than
    /// counting entities itself. `scan` tunes the read-ahead of a step that
    /// scans its ids' key range.
    ///
    /// A cursor is the complete property-by-ID key of its kind. An edge
    /// cursor may also be a retired 16-byte `[from][to]` property key an older
    /// build's prefix scan stopped on: it sorts after the ID key of `from`
    /// and before the next, so the scan resumes after id `from`.
    pub(crate) async fn open(
        reader: &'a R,
        scope: DataScope,
        kind: IndexElementKind,
        label: &str,
        progress: &SourceScanProgress,
        limits: SearchIndexBatchLimits,
        scan: IndexLifecycleScanTuning,
    ) -> Result<Option<Self>> {
        let upper = match source_id(scope, kind, progress.inclusive_upper_bound.as_bytes())? {
            SourceKey::Id(id) => id,
            SourceKey::LegacyEdgePair { .. } => {
                return Err(corruption(
                    "build source upper bound is not an exact property-by-ID key",
                ));
            }
        };
        let after = progress
            .cursor
            .as_ref()
            .map(|cursor| {
                source_id(scope, kind, cursor.as_bytes()).map(|key| match key {
                    SourceKey::Id(id) | SourceKey::LegacyEdgePair { from: id } => id,
                })
            })
            .transpose()?;
        let first = match after {
            Some(after) if after > upper => {
                return Err(corruption(
                    "build source cursor exceeds its inclusive upper bound",
                ));
            }
            Some(after) if after == upper => return Ok(None),
            Some(after) => after + 1,
            None => 0,
        };
        let layout = label_layout(reader).await?;
        let mut members = label_members(reader, layout, kind, label, scope).await?;
        // Not `iter().advance_to(first)`: in roaring 0.11 it advances the
        // next 2^32-id block to `first`'s low bits when `first`'s own block
        // is empty, skipping that block's members below them.
        members.remove_range(..first);
        let mut in_bound = members.iter().take_while(|id| *id <= upper);
        let taken = in_bound
            .by_ref()
            .take(limits.max_entities().get())
            .collect::<Vec<_>>();
        let (Some(&first), Some(&last)) = (taken.first(), taken.last()) else {
            return Ok(None);
        };
        let taken_ids = u64::try_from(taken.len()).unwrap_or(u64::MAX);
        let reads = if last - first < taken_ids.saturating_mul(RANGE_DENSITY) {
            Reads::Range(Box::new(RangeReads {
                ids: taken.into_iter().peekable(),
                rows: reader
                    .scan_with_options(
                        source_key(scope, kind, first)..=source_key(scope, kind, last),
                        &scan.scan_options(),
                    )
                    .await?,
            }))
        } else {
            Reads::Points(PointReads {
                ids: taken.into_iter(),
                chunk: Vec::new().into_iter(),
                ids_read: 0,
            })
        };
        Ok(Some(Self {
            reader,
            scope,
            kind,
            reads,
            more: in_bound.next().is_some(),
            read: Read {
                last: None,
                bytes: 0,
                limit: limits.max_input_bytes().get(),
            },
        }))
    }

    /// The next present row, or how the step's ids ended once every row
    /// returned so far is all there is.
    pub(crate) async fn next(&mut self) -> Result<LabelRead> {
        let row = match &mut self.reads {
            Reads::Points(points) => {
                points
                    .next(self.reader, self.scope, self.kind, &mut self.read)
                    .await?
            }
            Reads::Range(range) => range.next(self.scope, self.kind, &mut self.read).await?,
        };
        let Some(row) = row else {
            return Ok(self.end());
        };
        Ok(LabelRead::Row(row))
    }

    fn end(&mut self) -> LabelRead {
        let last_read = self
            .read
            .last
            .expect("a source with taken ids reads or passes one before it ends");
        let taken_left = match &mut self.reads {
            Reads::Points(points) => points.ids.len() > 0,
            Reads::Range(range) => range.ids.peek().is_some(),
        };
        if !taken_left && !self.more {
            return LabelRead::Exhausted;
        }
        LabelRead::Paused(
            IndexCursor::try_new(source_key(self.scope, self.kind, last_read))
                .expect("a property-by-ID key is a valid cursor"),
        )
    }
}

impl PointReads {
    /// The next present row, or `None` once the ids are read or the step has
    /// read its limit.
    async fn next(
        &mut self,
        reader: &(impl DbReadOps + Send + Sync),
        scope: DataScope,
        kind: IndexElementKind,
        read: &mut Read,
    ) -> Result<Option<SourceRow>> {
        loop {
            let Some((id, key, value)) = self.chunk.next() else {
                if !self.read_chunk(reader, scope, kind, read).await? {
                    return Ok(None);
                }
                continue;
            };
            let Some(value) = value else {
                continue;
            };
            return Ok(Some(SourceRow {
                entity_id: IndexEntityId::new(id),
                key,
                value,
            }));
        }
    }

    /// Reads the next chunk of taken ids, unless none is left or the step has
    /// read its input-byte limit. Returns whether a chunk was read.
    async fn read_chunk(
        &mut self,
        reader: &(impl DbReadOps + Send + Sync),
        scope: DataScope,
        kind: IndexElementKind,
        read: &mut Read,
    ) -> Result<bool> {
        if read.spent() {
            return Ok(false);
        }
        let chunk_ids = self.chunk_ids(read);
        let ids = self.ids.by_ref().take(chunk_ids).collect::<Vec<_>>();
        let Some(&last) = ids.last() else {
            return Ok(false);
        };
        let keys = ids
            .iter()
            .map(|id| source_key(scope, kind, *id))
            .collect::<Vec<_>>();
        // Build reads stay out of the block cache, as the prefix scan's did:
        // a build reads a label once and would evict what queries read.
        let rows = reader
            .multi_get_with_options(
                &keys,
                &slatedb::config::ReadOptions::new().with_cache_blocks(false),
            )
            .await?;
        if rows.len() != keys.len() {
            return Err(HelixDbError::InvariantViolation(
                "build source multi-get must return one row per key".to_string(),
            ));
        }
        keys.iter()
            .zip(&rows)
            .filter_map(|(key, row)| row.as_ref().map(|row| (key, row)))
            .for_each(|(key, row)| read.add(key, row));
        self.ids_read = self
            .ids_read
            .saturating_add(u64::try_from(ids.len()).unwrap_or(u64::MAX));
        read.last = Some(last);
        self.chunk = ids
            .into_iter()
            .zip(keys)
            .zip(rows)
            .map(|((id, key), row)| (id, key, row))
            .collect::<Vec<_>>()
            .into_iter();
        Ok(true)
    }

    /// How many ids the next chunk reads: at most [`POINT_CHUNK_IDS`] and as
    /// many as were read so far (starting at [`FIRST_CHUNK_IDS`]), and once
    /// a row was read, at most enough to reach the input-byte limit at the
    /// mean bytes per id read so far. A mean taken over small rows can still
    /// size a chunk of large ones, which [`POINT_CHUNK_IDS`] bounds.
    fn chunk_ids(&self, read: &Read) -> usize {
        let grown = u128::from(self.ids_read).max(FIRST_CHUNK_IDS as u128);
        let ids = match read.bytes {
            0 => grown,
            bytes => {
                let left = u128::from(read.limit - bytes);
                (left * u128::from(self.ids_read))
                    .div_ceil(u128::from(bytes))
                    .min(grown)
            }
        };
        usize::try_from(ids)
            .unwrap_or(usize::MAX)
            .clamp(1, POINT_CHUNK_IDS)
    }
}

impl RangeReads {
    /// The next present row of the taken ids, passing the ids without a row
    /// before it and the rows of other labels, or `None` once the range is
    /// read or the step has read its limit.
    ///
    /// A row passes every taken id whose key sorts before it: the ID key of
    /// `id` passes the ids below `id`, and a retired `[from][to]` edge row,
    /// which sorts after the ID key of `from`, passes the ids up to `from`.
    /// The range starts at the first taken id's key, so the first row read
    /// reads or passes it, and `read.last` is set once any row is read. A
    /// row read past the limit therefore lies before the next taken id,
    /// where the next step's range starts, and is never read again.
    async fn next(
        &mut self,
        scope: DataScope,
        kind: IndexElementKind,
        read: &mut Read,
    ) -> Result<Option<SourceRow>> {
        loop {
            if read.spent() {
                return Ok(None);
            }
            let Some(row) = self.rows.next().await? else {
                // No taken id left has a row.
                read.last = self.ids.by_ref().last().or(read.last);
                return Ok(None);
            };
            read.add(&row.key, &row.value);
            let id = match source_id(scope, kind, &row.key)? {
                SourceKey::Id(id) => id,
                // A retired `[from][to]` edge row is no entity.
                SourceKey::LegacyEdgePair { from } => {
                    while let Some(passed) = self.ids.next_if(|taken| *taken <= from) {
                        read.last = Some(passed);
                    }
                    continue;
                }
            };
            while let Some(absent) = self.ids.next_if(|taken| *taken < id) {
                read.last = Some(absent);
            }
            if self.ids.next_if_eq(&id).is_none() {
                continue;
            }
            read.last = Some(id);
            return Ok(Some(SourceRow {
                entity_id: IndexEntityId::new(id),
                key: row.key,
                value: row.value,
            }));
        }
    }
}

/// Where `reader`'s snapshot files label rows, by the storage marker that
/// snapshot holds: hash rows until the label identity copy publishes (a
/// store no V2 writer has opened included), exact rows from then on. The
/// marker and the bitmap come from one snapshot, so the layout is the one
/// whose rows are complete there, however far the copy or its cleanup has
/// gone since.
pub(crate) async fn label_layout(reader: &(impl DbReadOps + Sync)) -> Result<LabelRowLayout> {
    Ok(LabelRowWrites::of_stored_version(
        crate::migrations::startup::stored_storage_version(reader).await?,
    )
    .reads()
    .label_rows())
}

/// Every member of `label`'s bitmap of `kind`, read in `layout`, the
/// [`label_layout`] of `reader`.
pub(crate) async fn label_members(
    reader: &(impl DbReadOps + Send + Sync),
    layout: LabelRowLayout,
    kind: IndexElementKind,
    label: &str,
    scope: DataScope,
) -> Result<roaring::RoaringTreemap> {
    let members = match kind {
        IndexElementKind::Node => {
            crate::search::read_node_label(reader, layout, label, scope, None).await?
        }
        IndexElementKind::Edge => {
            crate::search::read_global_edge_label(reader, layout, label, scope, None).await?
        }
    };
    Ok(members.into_unbudgeted())
}

/// A build source key, decoded to where its scan stands.
enum SourceKey {
    /// The property-by-ID key of this id.
    Id(u64),
    /// A retired `[from][to]` edge property key.
    LegacyEdgePair { from: u64 },
}

fn source_id(scope: DataScope, kind: IndexElementKind, key: &[u8]) -> Result<SourceKey> {
    let parsed = DataKey::parse_from_slice(scope, key)
        .map_err(|_| corruption("build source cursor is not a property key of its scope"))?;
    match (kind, parsed) {
        (
            IndexElementKind::Node,
            DataKey::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            },
        ) => Ok(SourceKey::Id(key.node_id())),
        (
            IndexElementKind::Edge,
            DataKey::Data {
                kind: DataKeyKind::EdgePropertyById(key),
                ..
            },
        ) => Ok(SourceKey::Id(key.edge_id())),
        (
            IndexElementKind::Edge,
            DataKey::Data {
                kind: DataKeyKind::EdgePropertyPair(key),
                ..
            },
        ) => Ok(SourceKey::LegacyEdgePair { from: key.from() }),
        _ => Err(corruption(
            "build source cursor is not a property key of its entity kind",
        )),
    }
}

/// The property-by-ID key of `id`.
fn source_key(scope: DataScope, kind: IndexElementKind, id: u64) -> Bytes {
    let kind = match kind {
        IndexElementKind::Node => DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
        IndexElementKind::Edge => DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(id)),
    };
    DataKey::Data { scope, kind }.to_bytes()
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

/// Graph-row writes for tests that write source rows directly: each keeps
/// the row's label bitmap as a graph write does, in the same transaction and
/// under the keys the store's marker in its snapshot names (hash keys before
/// the label identity copy, both while it runs, exact keys once published).
#[cfg(any(
    test,
    feature = "index-lifecycle-testing",
    feature = "production-scale"
))]
pub(crate) mod testing {
    use slatedb::DbTransaction;

    use crate::encoding::property::Property;
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::index_lifecycle::repository::LabelRowWrites;
    use crate::index_lifecycle::IndexElementKind;

    fn label_of(properties: &[Property]) -> Option<&str> {
        properties
            .iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str())
    }

    /// Stages `id` joining (`present`) or leaving `label`'s bitmap.
    async fn stage_membership(
        transaction: &DbTransaction,
        scope: DataScope,
        kind: IndexElementKind,
        label: &str,
        id: u64,
        present: bool,
    ) {
        let writes = LabelRowWrites::of_stored_version(
            crate::migrations::startup::stored_storage_version(transaction)
                .await
                .expect("the storage marker reads"),
        );
        if writes.hash() {
            let row = match kind {
                IndexElementKind::Node => crate::search::HashLabelRow::Node(label),
                IndexElementKind::Edge => crate::search::HashLabelRow::Edge(label),
            };
            crate::search::stage_hash_label_membership(transaction, row, id, present, scope)
                .expect("hash label membership stages");
        }
        if !writes.exact() {
            return;
        }
        match (kind, present) {
            (IndexElementKind::Node, true) => {
                crate::search::add_to_equality_index_scoped(transaction, "$label", label, id, scope)
            }
            (IndexElementKind::Node, false) => crate::search::remove_from_equality_index_scoped(
                transaction,
                "$label",
                label,
                id,
                scope,
            ),
            (IndexElementKind::Edge, true) => {
                crate::search::add_to_global_edge_label_index_scoped(transaction, label, id, scope)
            }
            (IndexElementKind::Edge, false) => {
                crate::search::remove_from_global_edge_label_index_scoped(
                    transaction,
                    label,
                    id,
                    scope,
                )
            }
        }
        .expect("exact label membership stages");
    }

    /// Stages `id` joining `label`'s bitmap.
    #[cfg(any(test, feature = "index-lifecycle-testing"))]
    pub(crate) async fn stage_member(
        transaction: &DbTransaction,
        scope: DataScope,
        kind: IndexElementKind,
        label: &str,
        id: u64,
    ) {
        stage_membership(transaction, scope, kind, label, id, true).await;
    }

    /// Stages `id` leaving `label`'s bitmap.
    #[cfg(test)]
    pub(crate) async fn stage_departure(
        transaction: &DbTransaction,
        scope: DataScope,
        kind: IndexElementKind,
        label: &str,
        id: u64,
    ) {
        stage_membership(transaction, scope, kind, label, id, false).await;
    }

    /// Stages the label-bitmap change of `id`'s row going from `before` to
    /// `after` (an empty side is no row), as a graph write does.
    pub(crate) async fn stage_labels(
        transaction: &DbTransaction,
        scope: DataScope,
        kind: IndexElementKind,
        id: u64,
        before: &[Property],
        after: &[Property],
    ) {
        let (before, after) = (label_of(before), label_of(after));
        if before == after {
            return;
        }
        let changes = before
            .map(|label| (label, false))
            .into_iter()
            .chain(after.map(|label| (label, true)));
        for (label, present) in changes {
            stage_membership(transaction, scope, kind, label, id, present).await;
        }
    }

    /// Writes `properties` as `id`'s source row and adds `id` to its label's
    /// bitmap in one commit.
    #[cfg(any(test, feature = "index-lifecycle-testing"))]
    pub(crate) async fn put_source_row(
        db: &slatedb::Db,
        scope: DataScope,
        kind: IndexElementKind,
        id: u64,
        properties: &[Property],
    ) {
        let transaction = db
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .expect("source write begins");
        for label in label_of(properties).into_iter() {
            stage_member(&transaction, scope, kind, label, id).await;
        }
        transaction
            .put(
                super::source_key(scope, kind, id),
                crate::encoding::v2::values::property::encode_properties(properties),
            )
            .expect("source row stages");
        transaction.commit().await.expect("source write commits");
    }

    /// Writes raw `value` as `id`'s source row, a member of `label`, in one
    /// commit: a row that may no longer decode keeps the membership it had.
    #[cfg(test)]
    pub(crate) async fn put_raw_source_row(
        db: &slatedb::Db,
        scope: DataScope,
        kind: IndexElementKind,
        id: u64,
        label: &str,
        value: bytes::Bytes,
    ) {
        let transaction = db
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .expect("source write begins");
        stage_member(&transaction, scope, kind, label, id).await;
        transaction
            .put(super::source_key(scope, kind, id), value)
            .expect("source row stages");
        transaction.commit().await.expect("source write commits");
    }
}

#[cfg(test)]
#[path = "../../tests/unit/index_lifecycle_label_source.rs"]
mod tests;
