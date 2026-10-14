//! Row observers and damage for text generations.
//!
//! Coverage contracts drive a real text build with explicit scheduling, pause
//! it at one exact stage or validation lane, and damage one generation-owned
//! row or split object through the deployed codecs. The next production step
//! then decides the outcome; nothing here classifies or repairs lifecycle
//! state. The observers decode manifest roots and compaction pointers so
//! contracts can see what publication and compaction committed. Every helper
//! addresses the legacy unscoped namespace, the only scope the contracts use.

use std::ops::Bound;

use bytes::Bytes;
use slatedb::object_store::{ObjectStoreExt, PutPayload};

use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::index_lifecycle::{work, IndexGenerationId, IndexId, TextLogicalVersion};
use crate::{HelixDB, HelixStorage};

/// Bytes that no text value codec accepts.
const UNDECODABLE: &[u8] = &[0xFF, 0x00, 0xFF];

/// One exact damage to the rows of a building text generation.
///
/// "First" means first in key order within the generation's rows of that
/// kind, which is the row validation reads first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextBuildDamage {
    /// Replace the first manifest page with bytes no codec accepts.
    PageUndecodable,
    /// Delete the first manifest root.
    RootMissing,
    /// Replace the first manifest root with bytes no codec accepts.
    RootUndecodable,
    /// Reset the first manifest root to the canonical empty partition.
    RootEmptied,
    /// Rebind the first manifest root to a tenant partition its key does not name.
    RootRepartitioned,
    /// Count every page the first manifest root can address, so appending
    /// its next page overflows the page count.
    RootPagesExhausted,
    /// Delete page zero of the first manifest root.
    PageZeroMissing,
    /// Repeat page zero's first split without counting it in the root.
    PageSplitUncounted,
    /// Repeat page zero's first split and count it in the root.
    PageSplitDuplicated,
    /// Delete the first partition's corpus statistics.
    CorpusMissing,
    /// Replace the first entity state with bytes no codec accepts.
    EntityStateUndecodable,
    /// Advance the first entity state past every manifest revision.
    EntityStateAhead,
    /// Delete the first entity's statistics marker.
    MarkerMissing,
    /// Replace the first entity's statistics marker with bytes no codec accepts.
    MarkerUndecodable,
    /// Store the second entity's statistics marker under the first entity's key.
    MarkerForeign,
    /// Record no accounted document for the first entity.
    MarkerAbsent,
    /// Stage one build delta for the first entity, as a build started before
    /// text operations were queued would have left.
    PreQueueDelta,
}

/// Damage to the immutable object behind page zero's first split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextSplitObjectDamage {
    /// Delete the object.
    Missing,
    /// Append one byte, so its size disagrees with the manifest.
    Resized,
}

/// Applies `damage` to generation `generation` of index `index_id`.
pub async fn damage_text_build(
    db: &HelixDB,
    index_id: IndexId,
    generation: IndexGenerationId,
    damage: TextBuildDamage,
) {
    let rows = |kind| generation_rows(db, kind, index_id, generation);
    match damage {
        TextBuildDamage::PageUndecodable => {
            let (key, _) = first(rows(index_keys::RecordKind::TextManifestPage).await);
            put(db, key, Bytes::from_static(UNDECODABLE)).await;
        }
        TextBuildDamage::RootMissing => {
            let (key, _) = first(rows(index_keys::RecordKind::TextManifestRoot).await);
            delete(db, key).await;
        }
        TextBuildDamage::RootUndecodable => {
            let (key, _) = first(rows(index_keys::RecordKind::TextManifestRoot).await);
            put(db, key, Bytes::from_static(UNDECODABLE)).await;
        }
        TextBuildDamage::RootEmptied => {
            let (key, value) = first(rows(index_keys::RecordKind::TextManifestRoot).await);
            let root = decode_root(&value);
            let emptied = work::TextManifestRootValue::empty(
                root.index_id(),
                root.generation(),
                root.partition().clone(),
            );
            put(db, key, index_values::encode_manifest_root(&emptied)).await;
        }
        TextBuildDamage::RootRepartitioned => {
            let (key, value) = first(rows(index_keys::RecordKind::TextManifestRoot).await);
            let root = decode_root(&value);
            let foreign = work::TextPartition::try_tenant_value(Bytes::from_static(
                b"foreign-tenant-partition",
            ))
            .expect("fixture tenant partition is valid");
            let repartitioned = work::TextManifestRootValue::try_new(
                root.index_id(),
                root.generation(),
                foreign,
                root.revision(),
                root.page_count(),
                root.split_count(),
            )
            .expect("repartitioned root keeps valid counts");
            put(db, key, index_values::encode_manifest_root(&repartitioned)).await;
        }
        TextBuildDamage::RootPagesExhausted => {
            let (key, value) = first(rows(index_keys::RecordKind::TextManifestRoot).await);
            let root = decode_root(&value);
            let exhausted = work::TextManifestRootValue::try_new(
                root.index_id(),
                root.generation(),
                root.partition().clone(),
                root.revision(),
                u32::MAX,
                u64::from(u32::MAX),
            )
            .expect("one split per addressable page is a valid root");
            put(db, key, index_values::encode_manifest_root(&exhausted)).await;
        }
        TextBuildDamage::PageZeroMissing => {
            let (key, _) = page_zero(rows(index_keys::RecordKind::TextManifestPage).await);
            delete(db, key).await;
        }
        TextBuildDamage::PageSplitUncounted => {
            let (key, value) = page_zero(rows(index_keys::RecordKind::TextManifestPage).await);
            put(
                db,
                key,
                index_values::encode_manifest_page(&repeated(&value)),
            )
            .await;
        }
        TextBuildDamage::PageSplitDuplicated => {
            let (page_key, page_value) =
                page_zero(rows(index_keys::RecordKind::TextManifestPage).await);
            let page = repeated(&page_value);
            let (root_key, root_value) =
                root_of(rows(index_keys::RecordKind::TextManifestRoot).await, &page);
            let root = decode_root(&root_value);
            let counted = work::TextManifestRootValue::try_new(
                root.index_id(),
                root.generation(),
                root.partition().clone(),
                root.revision(),
                root.page_count(),
                root.split_count() + 1,
            )
            .expect("one more split keeps valid root counts");
            put(db, page_key, index_values::encode_manifest_page(&page)).await;
            put(db, root_key, index_values::encode_manifest_root(&counted)).await;
        }
        TextBuildDamage::CorpusMissing => {
            let (key, _) = first(rows(index_keys::RecordKind::TextCorpusStatistics).await);
            delete(db, key).await;
        }
        TextBuildDamage::EntityStateUndecodable => {
            let (key, _) = first(rows(index_keys::RecordKind::TextEntityState).await);
            put(db, key, Bytes::from_static(UNDECODABLE)).await;
        }
        TextBuildDamage::EntityStateAhead => {
            let (key, value) = first(rows(index_keys::RecordKind::TextEntityState).await);
            let mut state = index_values::decode_text_entity_state(&value)
                .expect("fixture entity state decodes");
            state.logical_version =
                TextLogicalVersion::new(1 << 40).expect("fixture logical version is nonzero");
            put(db, key, index_values::encode_text_entity_state(&state)).await;
        }
        TextBuildDamage::MarkerMissing => {
            let (key, _) = first(rows(index_keys::RecordKind::TextStatisticsEntity).await);
            delete(db, key).await;
        }
        TextBuildDamage::MarkerUndecodable => {
            let (key, _) = first(rows(index_keys::RecordKind::TextStatisticsEntity).await);
            put(db, key, Bytes::from_static(UNDECODABLE)).await;
        }
        TextBuildDamage::MarkerForeign => {
            let markers = rows(index_keys::RecordKind::TextStatisticsEntity).await;
            let [(first_key, _), (_, second_value), ..] = markers.as_slice() else {
                panic!("foreign marker damage needs two entities, found {markers:?}");
            };
            put(db, first_key.clone(), second_value.clone()).await;
        }
        TextBuildDamage::MarkerAbsent => {
            let (key, value) = first(rows(index_keys::RecordKind::TextStatisticsEntity).await);
            let mut marker =
                index_values::decode_statistics_entity(&value).expect("fixture marker decodes");
            marker.contribution = work::TextStatisticsContribution::Absent;
            put(db, key, index_values::encode_statistics_entity(&marker)).await;
        }
        TextBuildDamage::PreQueueDelta => {
            let (_, value) = first(rows(index_keys::RecordKind::TextEntityState).await);
            let state = index_values::decode_text_entity_state(&value)
                .expect("fixture entity state decodes");
            let key = ManagedIndexKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: index_keys::ScopedKey::BuildDelta(index_keys::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: index_keys::IndexEntity {
                        kind: state.entity_kind,
                        id: state.entity_id,
                    },
                }),
            }
            .to_bytes();
            let delta = work::CoalescedBuildDeltaValue {
                index_id,
                generation,
                entity_kind: state.entity_kind,
                entity_id: state.entity_id,
                state: work::CoalescedBuildDeltaState::Marker,
            };
            put(db, key, index_values::encode_build_delta(&delta)).await;
        }
    }
}

/// Damages the object behind page zero's first split, returning its exact
/// original bytes so a contract can restore it.
pub async fn damage_text_split_object(
    db: &HelixDB,
    index_id: IndexId,
    generation: IndexGenerationId,
    damage: TextSplitObjectDamage,
) -> Bytes {
    let location = split_object_location(db, index_id, generation).await;
    let original = db
        .object_store()
        .get(&location)
        .await
        .expect("fixture split object exists")
        .bytes()
        .await
        .expect("fixture split object reads");
    match damage {
        TextSplitObjectDamage::Missing => db
            .object_store()
            .delete(&location)
            .await
            .expect("fixture split object deletes"),
        TextSplitObjectDamage::Resized => {
            let mut resized = original.to_vec();
            resized.push(0);
            db.object_store()
                .put(&location, PutPayload::from_bytes(Bytes::from(resized)))
                .await
                .expect("fixture split object is replaced");
        }
    }
    original
}

/// Restores the object behind page zero's first split to `original`.
pub async fn restore_text_split_object(
    db: &HelixDB,
    index_id: IndexId,
    generation: IndexGenerationId,
    original: Bytes,
) {
    let location = split_object_location(db, index_id, generation).await;
    db.object_store()
        .put(&location, PutPayload::from_bytes(original))
        .await
        .expect("fixture split object is restored");
}

/// Returns the manifest root and page rows a generation holds, in that order.
pub async fn text_manifest_row_counts(
    db: &HelixDB,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> (usize, usize) {
    (
        generation_rows(
            db,
            index_keys::RecordKind::TextManifestRoot,
            index_id,
            generation,
        )
        .await
        .len(),
        generation_rows(
            db,
            index_keys::RecordKind::TextManifestPage,
            index_id,
            generation,
        )
        .await
        .len(),
    )
}

/// Returns each manifest root's split count, in partition key order.
pub async fn text_manifest_split_counts(
    db: &HelixDB,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Vec<u64> {
    generation_rows(
        db,
        index_keys::RecordKind::TextManifestRoot,
        index_id,
        generation,
    )
    .await
    .iter()
    .map(|(_, value)| decode_root(value).split_count())
    .collect()
}

/// Returns the Active text pages scheduled for compaction across all indexes.
pub async fn text_compaction_pointer_count(db: &HelixDB) -> usize {
    let prefix =
        index_keys::GlobalKey::logical_prefix(index_keys::GlobalKind::TextCompactionPointer);
    let mut rows = writer(db)
        .scan_prefix(&prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await
        .expect("compaction pointers are readable");
    let mut count = 0;
    while rows
        .next()
        .await
        .expect("compaction pointer scan succeeds")
        .is_some()
    {
        count += 1;
    }
    count
}

async fn split_object_location(
    db: &HelixDB,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> slatedb::object_store::path::Path {
    let (_, value) = page_zero(
        generation_rows(
            db,
            index_keys::RecordKind::TextManifestPage,
            index_id,
            generation,
        )
        .await,
    );
    let page = index_values::decode_manifest_page(&value).expect("fixture page decodes");
    let split = page.entries()[0];
    crate::search::text::blob_object_store_path(db.path(), *split.blob().hash())
}

/// Every row of `kind` owned by one generation, in key order.
async fn generation_rows(
    db: &HelixDB,
    kind: index_keys::RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Vec<(Bytes, Bytes)> {
    let prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        index_keys::ScopedKey::generation_prefix(kind, index_id, generation),
    );
    let mut rows = writer(db)
        .scan_prefix(&prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await
        .expect("generation rows are readable");
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await.expect("generation row scan succeeds") {
        collected.push((row.key, row.value));
    }
    collected
}

fn first(rows: Vec<(Bytes, Bytes)>) -> (Bytes, Bytes) {
    rows.into_iter()
        .next()
        .expect("the damaged generation holds a row of this kind")
}

/// The first page-zero row, in key order.
fn page_zero(rows: Vec<(Bytes, Bytes)>) -> (Bytes, Bytes) {
    rows.into_iter()
        .find(|(_, value)| {
            index_values::decode_manifest_page(value)
                .expect("fixture page decodes")
                .page()
                == 0
        })
        .expect("the damaged generation holds page zero")
}

/// The root row describing `page`'s partition.
fn root_of(rows: Vec<(Bytes, Bytes)>, page: &work::TextManifestPageValue) -> (Bytes, Bytes) {
    rows.into_iter()
        .find(|(_, value)| decode_root(value).partition() == page.partition())
        .expect("the damaged page has a root")
}

/// Page zero with its first split repeated at the end.
fn repeated(value: &[u8]) -> work::TextManifestPageValue {
    let page = index_values::decode_manifest_page(value).expect("fixture page decodes");
    let mut entries = page.entries().to_vec();
    entries.push(entries[0]);
    work::TextManifestPageValue::try_new(
        page.index_id(),
        page.generation(),
        page.partition().clone(),
        page.page(),
        entries,
    )
    .expect("a repeated split keeps a bounded page")
}

fn decode_root(value: &[u8]) -> work::TextManifestRootValue {
    index_values::decode_manifest_root(value).expect("fixture root decodes")
}

async fn put(db: &HelixDB, key: Bytes, value: Bytes) {
    writer(db)
        .put(key, value)
        .await
        .expect("damaged row is written");
}

async fn delete(db: &HelixDB, key: Bytes) {
    writer(db)
        .delete(key)
        .await
        .expect("damaged row is deleted");
}

fn writer(db: &HelixDB) -> &slatedb::Db {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("text build damage requires writer storage");
    };
    writer.db()
}
