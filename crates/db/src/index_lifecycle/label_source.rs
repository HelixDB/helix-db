//! Source rows of one build's label, read through the label's bitmap.
//!
//! Every index definition names one label, and every node and edge write
//! keeps its label's bitmap in the same transaction as its row (the `$label`
//! equality bitmap for nodes, the global edge-label bitmap for edges), as
//! label scans already rely on. A build's source scan therefore reads only
//! the rows of its label's members, rather than every row of its kind.
//!
//! The scan keeps the progress it always had: the cursor is the complete
//! property-by-ID key of the last member taken, whether or not its row still
//! exists, so ascending ids keep the key order the stored cursor and
//! inclusive upper bound are compared in, and a build resumes across binaries
//! either way. A member whose row is gone is skipped. A member that carries
//! another label (a label-hash collision) is read and left to each driver's
//! own label check, which writes nothing for it.
//!
//! Rows are read with `multi_get`, [`helix_planner::cost::RECORD_BATCH_ROWS`]
//! ids at a time and only once the previous chunk is consumed, and no chunk
//! is read once the step has read its input-byte limit, so a step holds at
//! most one chunk of rows beyond that limit. Like the prefix scan before it,
//! a build's reads do not fill the block cache.

use bytes::Bytes;
use slatedb::DbReadOps;

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, EdgePropertyByIdKey, NodePropertyKey};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexCursor, IndexElementKind, IndexEntityId, SourceScanProgress};

/// Ids read by one `multi_get`.
const CHUNK_IDS: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;

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
    /// Ids taken for this step and not yet read, ascending.
    ids: std::vec::IntoIter<u64>,
    /// Whether the label holds members after the taken ids, up to the bound.
    more: bool,
    /// The chunk being returned: each id, its key and its row if present.
    chunk: std::vec::IntoIter<(u64, Bytes, Option<Bytes>)>,
    /// The last id read.
    last_read: Option<u64>,
    read_bytes: u64,
    max_input_bytes: u64,
}

impl<'a, R: DbReadOps + Send + Sync> LabelSource<'a, R> {
    /// Takes the members of `label` after `progress`'s cursor, up to its
    /// inclusive upper bound, at most `limits.max_entities()` of them, from
    /// the bitmap `reader` holds. `None` when no member is left: the scan is
    /// complete.
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
        let members = match kind {
            IndexElementKind::Node => {
                crate::search::lookup_node_label_index_scoped(reader, label, scope).await?
            }
            IndexElementKind::Edge => {
                crate::search::lookup_global_edge_label_index_scoped(reader, label, scope).await?
            }
        };
        let mut members = members.iter();
        members.advance_to(first);
        let mut in_bound = members.take_while(|id| *id <= upper);
        let taken = in_bound
            .by_ref()
            .take(limits.max_entities().get())
            .collect::<Vec<_>>();
        if taken.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            reader,
            scope,
            kind,
            ids: taken.into_iter(),
            more: in_bound.next().is_some(),
            chunk: Vec::new().into_iter(),
            last_read: None,
            read_bytes: 0,
            max_input_bytes: limits.max_input_bytes().get(),
        }))
    }

    /// The next present row, or how the step's ids ended once every row
    /// returned so far is all there is.
    pub(crate) async fn next(&mut self) -> Result<LabelRead> {
        loop {
            let Some((id, key, value)) = self.chunk.next() else {
                if !self.read_chunk().await? {
                    return Ok(self.end());
                }
                continue;
            };
            let Some(value) = value else {
                continue;
            };
            return Ok(LabelRead::Row(SourceRow {
                entity_id: IndexEntityId::new(id),
                key,
                value,
            }));
        }
    }

    /// Reads the next chunk of taken ids, unless none is left or the step has
    /// read its input-byte limit. Returns whether a chunk was read.
    async fn read_chunk(&mut self) -> Result<bool> {
        if self.read_bytes >= self.max_input_bytes {
            return Ok(false);
        }
        let ids = self.ids.by_ref().take(CHUNK_IDS).collect::<Vec<_>>();
        let Some(&last) = ids.last() else {
            return Ok(false);
        };
        let keys = ids
            .iter()
            .map(|id| source_key(self.scope, self.kind, *id))
            .collect::<Vec<_>>();
        // Build reads stay out of the block cache, as the prefix scan's did:
        // a build reads a label once and would evict what queries read.
        let rows = self
            .reader
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
        self.read_bytes = keys
            .iter()
            .zip(&rows)
            .filter_map(|(key, row)| row.as_ref().map(|row| key.len() + row.len()))
            .fold(self.read_bytes, |bytes, row| {
                bytes.saturating_add(u64::try_from(row).unwrap_or(u64::MAX))
            });
        self.last_read = Some(last);
        self.chunk = ids
            .into_iter()
            .zip(keys)
            .zip(rows)
            .map(|((id, key), row)| (id, key, row))
            .collect::<Vec<_>>()
            .into_iter();
        Ok(true)
    }

    fn end(&self) -> LabelRead {
        let last_read = self
            .last_read
            .expect("a source with taken ids reads its first chunk before it ends");
        if self.ids.len() == 0 && !self.more {
            return LabelRead::Exhausted;
        }
        LabelRead::Paused(
            IndexCursor::try_new(source_key(self.scope, self.kind, last_read))
                .expect("a property-by-ID key is a valid cursor"),
        )
    }
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
/// the row's label bitmap as a graph write does, in the same transaction.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) mod testing {
    use slatedb::{Db, DbTransaction, IsolationLevel};

    use crate::encoding::property::Property;
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::encoding::v2::values::property::encode_properties;
    use crate::index_lifecycle::IndexElementKind;

    fn label_of(properties: &[Property]) -> Option<&str> {
        properties
            .iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str())
    }

    /// Stages `id` joining `label`'s bitmap.
    pub(crate) fn stage_member(
        transaction: &DbTransaction,
        scope: DataScope,
        kind: IndexElementKind,
        label: &str,
        id: u64,
    ) {
        match kind {
            IndexElementKind::Node => {
                crate::search::add_to_equality_index_scoped(transaction, "$label", label, id, scope)
            }
            IndexElementKind::Edge => {
                crate::search::add_to_global_edge_label_index_scoped(transaction, label, id, scope)
            }
        }
        .expect("label membership stages");
    }

    /// Stages `id` leaving `label`'s bitmap.
    pub(crate) fn stage_departure(
        transaction: &DbTransaction,
        scope: DataScope,
        kind: IndexElementKind,
        label: &str,
        id: u64,
    ) {
        match kind {
            IndexElementKind::Node => crate::search::remove_from_equality_index_scoped(
                transaction,
                "$label",
                label,
                id,
                scope,
            ),
            IndexElementKind::Edge => crate::search::remove_from_global_edge_label_index_scoped(
                transaction,
                label,
                id,
                scope,
            ),
        }
        .expect("label departure stages");
    }

    /// Stages the label-bitmap change of `id`'s row going from `before` to
    /// `after` (an empty side is no row), as a graph write does.
    pub(crate) fn stage_labels(
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
        before
            .into_iter()
            .for_each(|label| stage_departure(transaction, scope, kind, label, id));
        after
            .into_iter()
            .for_each(|label| stage_member(transaction, scope, kind, label, id));
    }

    /// Writes `properties` as `id`'s source row and adds `id` to its label's
    /// bitmap in one commit.
    pub(crate) async fn put_source_row(
        db: &Db,
        scope: DataScope,
        kind: IndexElementKind,
        id: u64,
        properties: &[Property],
    ) {
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("source write begins");
        label_of(properties)
            .into_iter()
            .for_each(|label| stage_member(&transaction, scope, kind, label, id));
        transaction
            .put(
                super::source_key(scope, kind, id),
                encode_properties(properties),
            )
            .expect("source row stages");
        transaction.commit().await.expect("source write commits");
    }

    /// Writes raw `value` as `id`'s source row, a member of `label`, in one
    /// commit: a row that may no longer decode keeps the membership it had.
    #[cfg(test)]
    pub(crate) async fn put_raw_source_row(
        db: &Db,
        scope: DataScope,
        kind: IndexElementKind,
        id: u64,
        label: &str,
        value: bytes::Bytes,
    ) {
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("source write begins");
        stage_member(&transaction, scope, kind, label, id);
        transaction
            .put(super::source_key(scope, kind, id), value)
            .expect("source row stages");
        transaction.commit().await.expect("source write commits");
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, DbTransaction, IsolationLevel};

    use super::*;
    use crate::encoding::property::Property;
    use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;
    use crate::index_lifecycle::OperationCounters;

    async fn test_db(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("label source test database opens")
    }

    fn row(label: &str, filler: usize) -> Vec<Property> {
        vec![
            Property::string("$label", label),
            Property::string("pad", "x".repeat(filler)),
        ]
    }

    fn limits(entities: usize, input_bytes: u64) -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(entities).expect("entity limit is positive"),
            NonZeroU64::new(input_bytes).expect("input limit is positive"),
            NonZeroU64::new(1 << 20).expect("operation limit is positive"),
            NonZeroU64::new(1 << 30).expect("output limit is positive"),
            NonZeroU64::new(1 << 30).expect("output limit is positive"),
        )
        .expect("test limits are consistent")
    }

    fn cursor(scope: DataScope, kind: IndexElementKind, id: u64) -> IndexCursor {
        IndexCursor::try_new(source_key(scope, kind, id)).expect("a source key is a cursor")
    }

    fn progress(
        scope: DataScope,
        kind: IndexElementKind,
        after: Option<u64>,
        upper: u64,
    ) -> SourceScanProgress {
        SourceScanProgress {
            inclusive_upper_bound: cursor(scope, kind, upper),
            cursor: after.map(|after| cursor(scope, kind, after)),
            counters: OperationCounters::default(),
        }
    }

    /// Ids of every row one step returns, and how the step's ids ended.
    async fn step(
        snapshot: &DbTransaction,
        kind: IndexElementKind,
        label: &str,
        progress: &SourceScanProgress,
        limits: SearchIndexBatchLimits,
    ) -> Option<(Vec<u64>, Option<u64>)> {
        let scope = DataScope::LegacyUnscoped;
        let mut source = LabelSource::open(snapshot, scope, kind, label, progress, limits)
            .await
            .expect("the label source opens")?;
        let mut ids = Vec::new();
        loop {
            match source.next().await.expect("the label source reads") {
                LabelRead::Row(row) => {
                    assert_eq!(row.key, source_key(scope, kind, row.entity_id.get()));
                    ids.push(row.entity_id.get());
                }
                LabelRead::Exhausted => return Some((ids, None)),
                LabelRead::Paused(paused) => {
                    let SourceKey::Id(paused) = source_id(scope, kind, paused.as_bytes())
                        .expect("a paused cursor is a source key")
                    else {
                        panic!("a paused cursor is an id key");
                    };
                    return Some((ids, Some(paused)));
                }
            }
        }
    }

    async fn snapshot(db: &Db) -> DbTransaction {
        db.begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot opens")
    }

    #[tokio::test]
    async fn a_label_without_members_or_ids_left_has_nothing_to_read() {
        let db = test_db("label-source-empty").await;
        let scope = DataScope::LegacyUnscoped;
        let node = IndexElementKind::Node;
        for id in 0..300 {
            testing::put_source_row(&db, scope, node, id, &row("Other", 0)).await;
        }
        testing::put_source_row(&db, scope, node, 5, &row("User", 0)).await;
        let read = snapshot(&db).await;
        let limits = limits(1024, 1 << 20);
        assert!(step(
            &read,
            node,
            "Absent",
            &progress(scope, node, None, 299),
            limits
        )
        .await
        .is_none());
        // Its only member is at or before the cursor, or above the bound.
        assert!(step(
            &read,
            node,
            "User",
            &progress(scope, node, Some(5), 299),
            limits
        )
        .await
        .is_none());
        assert!(
            step(&read, node, "User", &progress(scope, node, None, 4), limits)
                .await
                .is_none()
        );
        // A cursor at the bound is done without reading the bitmap.
        assert!(step(
            &read,
            node,
            "User",
            &progress(scope, node, Some(299), 299),
            limits
        )
        .await
        .is_none());
        assert!(matches!(
            LabelSource::open(&read, scope, node, "User", &progress(scope, node, Some(300), 299), limits).await,
            Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("exceeds")
        ));
    }

    #[tokio::test]
    async fn members_are_read_in_id_order_skipping_absent_rows_and_ids_above_the_bound() {
        let db = test_db("label-source-members").await;
        let scope = DataScope::LegacyUnscoped;
        let node = IndexElementKind::Node;
        for id in [3, 9, 600, 601, 2000] {
            testing::put_source_row(&db, scope, node, id, &row("User", 0)).await;
        }
        testing::put_source_row(&db, scope, node, 4, &row("Other", 0)).await;
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("write begins");
        // A long run of members without rows, and one row of another label.
        (10..1500).for_each(|id| testing::stage_member(&transaction, scope, node, "User", id));
        testing::stage_member(&transaction, scope, node, "User", 4);
        transaction.commit().await.expect("members commit");
        let read = snapshot(&db).await;

        let all = step(
            &read,
            node,
            "User",
            &progress(scope, node, None, 1999),
            limits(4096, 1 << 20),
        )
        .await
        .expect("members remain");
        assert_eq!(all, (vec![3, 4, 9, 600, 601], None));

        // Absent ids count towards a step's entities and move its cursor.
        let mut after = None;
        let mut read_ids = Vec::new();
        let mut steps = 0;
        while let Some((ids, paused)) = step(
            &read,
            node,
            "User",
            &progress(scope, node, after, 1999),
            limits(100, 1 << 20),
        )
        .await
        {
            steps += 1;
            read_ids.extend(ids);
            let Some(paused) = paused else {
                break;
            };
            assert!(
                after.is_none_or(|after| paused > after),
                "every step advances"
            );
            after = Some(paused);
        }
        assert_eq!(read_ids, [3, 4, 9, 600, 601]);
        // 1,494 members up to the bound, 100 a step.
        assert_eq!(steps, 15);
    }

    #[tokio::test]
    async fn no_chunk_is_read_past_the_input_byte_limit() {
        let db = test_db("label-source-bytes").await;
        let scope = DataScope::LegacyUnscoped;
        let node = IndexElementKind::Node;
        for id in 0..1000 {
            testing::put_source_row(&db, scope, node, id, &row("User", 100)).await;
        }
        let read = snapshot(&db).await;
        // The first chunk of 256 rows already passes 4 KiB: the step returns
        // that chunk and pauses after its last id.
        let (ids, paused) = step(
            &read,
            node,
            "User",
            &progress(scope, node, None, 999),
            limits(1024, 4096),
        )
        .await
        .expect("members remain");
        assert_eq!(ids, (0..256).collect::<Vec<_>>());
        assert_eq!(paused, Some(255));
        // A limit two chunks fit in reads both and the start of a third.
        let (ids, paused) = step(
            &read,
            node,
            "User",
            &progress(scope, node, Some(255), 999),
            limits(1024, 2 * 256 * 140),
        )
        .await
        .expect("members remain");
        assert_eq!(ids.first(), Some(&256));
        assert_eq!(paused, ids.last().copied());
        assert_eq!(ids.len() % CHUNK_IDS, 0);
    }

    #[tokio::test]
    async fn edge_builds_resume_after_a_retired_pair_key_cursor() {
        let db = test_db("label-source-legacy-edge-cursor").await;
        let scope = DataScope::LegacyUnscoped;
        let edge = IndexElementKind::Edge;
        for id in 3..9 {
            testing::put_source_row(&db, scope, edge, id, &row("FOLLOWS", 0)).await;
        }
        let read = snapshot(&db).await;
        // An older build's prefix scan stopped on the retired row `[5][9]`,
        // which sorts after edge 5 and before edge 6.
        let legacy = IndexCursor::try_new(
            DataKey::Data {
                scope,
                kind: DataKeyKind::EdgePropertyPair(LegacyEdgePropertyPairKey::new(5, 9)),
            }
            .to_bytes(),
        )
        .expect("a legacy key is a cursor");
        let resumed = SourceScanProgress {
            cursor: Some(legacy.clone()),
            ..progress(scope, edge, None, 8)
        };
        assert_eq!(
            step(&read, edge, "FOLLOWS", &resumed, limits(1024, 1 << 20)).await,
            Some((vec![6, 7, 8], None))
        );
        // A node build has no pair keys, and no bound is one.
        let node = IndexElementKind::Node;
        assert!(LabelSource::open(
            &read,
            scope,
            node,
            "FOLLOWS",
            &SourceScanProgress {
                cursor: Some(legacy.clone()),
                ..progress(scope, node, None, 8)
            },
            limits(1024, 1 << 20)
        )
        .await
        .is_err());
        assert!(LabelSource::open(
            &read,
            scope,
            edge,
            "FOLLOWS",
            &SourceScanProgress {
                inclusive_upper_bound: legacy,
                cursor: None,
                counters: OperationCounters::default(),
            },
            limits(1024, 1 << 20)
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn a_corrupt_label_bitmap_fails_the_step() {
        let db = test_db("label-source-corrupt-bitmap").await;
        let scope = DataScope::LegacyUnscoped;
        let node = IndexElementKind::Node;
        testing::put_source_row(&db, scope, node, 1, &row("User", 0)).await;
        db.put(
            DataKey::Data {
                scope,
                kind: DataKeyKind::PropertyIndex(
                    crate::encoding::v2::keys::indexes::PropertyIndexKey::Equality(
                        crate::encoding::indexes::equality::EqualityIndexKey::new(
                            crate::encoding::indexes::hash_property_name("$label"),
                            crate::encoding::indexes::hash_property_value("User"),
                        ),
                    ),
                ),
            }
            .to_bytes(),
            b"not a bitmap".as_slice(),
        )
        .await
        .expect("corrupt bitmap writes");
        let read = snapshot(&db).await;
        assert!(LabelSource::open(
            &read,
            scope,
            node,
            "User",
            &progress(scope, node, None, 1),
            limits(1024, 1 << 20)
        )
        .await
        .is_err());
    }
}
