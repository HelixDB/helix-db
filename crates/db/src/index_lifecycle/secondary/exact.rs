//! Literal secondary-index primitives selected by executable plans.

use super::*;
use crate::encoding::v2::values::property::equality_index_value as equality;
use crate::query_resources::{self, bitmap};

mod dynamic;
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(super) use dynamic::lookup_active_equality_generation_admitted;
pub(crate) use dynamic::lookup_active_equality_generations_admitted;

mod ordered;
pub(crate) use ordered::{
    scan_active_range_generation_ordered, ExactRangeScanProgress, UnobservedRangeScan,
};

#[async_trait]
trait ExactRangeRows {
    async fn next_exact(
        &mut self,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error>;
}

#[async_trait]
impl ExactRangeRows for slatedb::DbIterator {
    async fn next_exact(
        &mut self,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        self.next().await
    }
}

/// Records one authoritative graph read requested by an exact equality plan.
#[inline]
pub(crate) fn record_equality_graph_read() {
    #[cfg(any(test, feature = "production-coverage"))]
    record(ReadKind::Graph);
}

/// Executes one planner-selected indexed equality point read without choosing
/// or performing authoritative verification.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn lookup_active_equality_point_literal(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
) -> Result<roaring::RoaringTreemap> {
    lookup_active_equality_point_literal_with_compatibility(
        reader,
        handle,
        value,
        ReaderStorageCompatibility::Current,
    )
    .await
}

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn lookup_active_equality_point_literal_with_compatibility(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
    compatibility: ReaderStorageCompatibility,
) -> Result<roaring::RoaringTreemap> {
    lookup_active_equality_point_admitted(reader, handle, value, compatibility, None)
        .await
        .map(bitmap::Bitmap::into_unbudgeted)
}

pub(crate) async fn lookup_active_equality_point_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "literal equality point read received a non-secondary Active handle",
        ));
    };
    if !matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeEquality { .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }
    ) {
        return Err(corruption(
            "literal equality point read received a range definition",
        ));
    }
    let prepared = match equality::prepare_equality_value(value) {
        EqualityValueProjection::Indexed(value) => value,
        EqualityValueProjection::Oversized {
            encoded_len,
            maximum,
        } => {
            return Err(SecondaryIndexValueError::EncodedKeyTooLarge {
                encoded_len,
                maximum,
            }
            .into());
        }
        EqualityValueProjection::AuthoritativeNull
        | EqualityValueProjection::NonReflexive
        | EqualityValueProjection::Unsupported(_) => {
            return Err(corruption(
                "literal equality point read received a non-indexed value",
            ));
        }
    };
    // Canonical payload and Bytes sharing metadata remain owned across legacy
    // compatibility reads. Admission precedes both encoding and the first clone.
    let _canonical_memory = budget
        .map(|budget| {
            budget.reserve(
                prepared
                    .encoded_len()
                    .saturating_add(2 * size_of::<equality::CanonicalEqualityValue>()),
            )
        })
        .transpose()?;
    let value = prepared.encode();
    let lane = definition_lane(definition);
    let legacy_value = value.clone();
    let key = prepare_secondary_entry_key(
        handle.scope(),
        handle.index_id(),
        handle.generation(),
        definition,
        CanonicalSecondaryValue::equality(value),
        IndexEntityId::initial(),
    )
    .expect("validated indexed equality values always fit their physical key");
    let _key_memory = budget
        .map(|budget| budget.reserve(key.encoded_len()))
        .transpose()?;
    let key = key.to_bytes();
    record_equality_point_read();
    if let Some(budget) = budget {
        budget.record_reads(query_resources::StorageReadUsage {
            point_gets: 1,
            ..Default::default()
        });
    }
    if lane.is_unique() {
        let Some(bytes) = reader.get(key).await? else {
            return bitmap::Bitmap::empty(budget);
        };
        let _raw = budget
            .map(|budget| budget.reserve(bytes.len()))
            .transpose()?;
        let owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &bytes)?;
        return bitmap::Bitmap::singleton(owner.get(), budget);
    }
    let owners = reader
        .get(key)
        .await?
        .map(|bytes| {
            let _raw = budget
                .map(|budget| budget.reserve(bytes.len()))
                .transpose()?;
            bitmap::Bitmap::decode(&bytes, budget)
        })
        .transpose()?;
    let mut owners = match owners {
        Some(owners) => owners,
        None => bitmap::Bitmap::empty(budget)?,
    };
    if compatibility == ReaderStorageCompatibility::LegacyEqualityUnion {
        owners = owners.union(
            lookup_legacy_equality_entries_admitted(reader, handle, lane, &legacy_value, budget)
                .await?,
        )?;
    }
    Ok(owners)
}

/// Executes one planner-selected literal bitmap multi-get.
///
/// Duplicate physical keys are preserved and the primitive always issues one
/// `multi_get`; executable validation owns the at-least-two invariant.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn lookup_active_equality_literal_batch(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
) -> Result<roaring::RoaringTreemap> {
    lookup_active_equality_literal_batch_with_compatibility(
        reader,
        handle,
        values,
        ReaderStorageCompatibility::Current,
    )
    .await
}

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn lookup_active_equality_literal_batch_with_compatibility(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
    compatibility: ReaderStorageCompatibility,
) -> Result<roaring::RoaringTreemap> {
    lookup_active_equality_batch_admitted(reader, handle, values, compatibility, None)
        .await
        .map(bitmap::Bitmap::into_unbudgeted)
}

pub(crate) async fn lookup_active_equality_batch_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
    compatibility: ReaderStorageCompatibility,
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    if values.len() < 2 {
        return Err(corruption(
            "literal equality bitmap batch contained fewer than two values",
        ));
    }
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "literal equality bitmap batch received a non-secondary Active handle",
        ));
    };
    if !definition_uses_equality_bitmap(definition) {
        return Err(corruption(
            "literal equality bitmap batch received a non-bitmap definition",
        ));
    }
    if compatibility == ReaderStorageCompatibility::LegacyEqualityUnion {
        let mut owners = bitmap::Bitmap::empty(budget)?;
        for value in values {
            owners = owners.union(
                lookup_active_equality_point_admitted(reader, handle, value, compatibility, budget)
                    .await?,
            )?;
        }
        return Ok(owners);
    }
    lookup_equality_keys_admitted(
        reader,
        handle,
        definition,
        values,
        budget,
        EqualityRead::LiteralBatch,
    )
    .await
}

#[derive(Clone, Copy)]
enum EqualityRead {
    LiteralBatch,
    DistinctSet,
}

async fn lookup_equality_keys_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    definition: &ValidatedSecondaryIndexDefinition,
    values: &[PropertyValue],
    budget: Option<&query_resources::Budget>,
    primitive: EqualityRead,
) -> Result<bitmap::Bitmap> {
    let mut key_bytes = values.len().saturating_mul(size_of::<Bytes>());
    let mut key_memory = budget.map(|budget| budget.reserve(key_bytes)).transpose()?;
    let mut keys = Vec::with_capacity(values.len());
    for value in values {
        let prepared = match equality::prepare_equality_value(value) {
            EqualityValueProjection::Indexed(value) => value,
            EqualityValueProjection::Oversized {
                encoded_len,
                maximum,
            } => {
                return Err(SecondaryIndexValueError::EncodedKeyTooLarge {
                    encoded_len,
                    maximum,
                }
                .into());
            }
            EqualityValueProjection::AuthoritativeNull
            | EqualityValueProjection::NonReflexive
            | EqualityValueProjection::Unsupported(_) => {
                return Err(corruption(
                    "literal equality bitmap batch received a non-indexed value",
                ));
            }
        };
        let _canonical_memory = budget
            .map(|budget| {
                budget.reserve(
                    prepared
                        .encoded_len()
                        .saturating_add(2 * size_of::<equality::CanonicalEqualityValue>()),
                )
            })
            .transpose()?;
        let key = prepare_secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::equality(prepared.encode()),
            IndexEntityId::initial(),
        )?;
        key_bytes = key_bytes.saturating_add(key.encoded_len());
        key_memory
            .as_mut()
            .map(|memory| memory.resize(key_bytes))
            .transpose()?;
        keys.push(key.to_bytes());
    }
    if matches!(primitive, EqualityRead::DistinctSet) {
        keys.sort_unstable();
        keys.dedup();
    }
    keys.iter().for_each(|_| record_equality_point_read());
    if matches!(primitive, EqualityRead::DistinctSet) && keys.len() == 1 {
        if let Some(budget) = budget {
            budget.record_reads(query_resources::StorageReadUsage {
                point_gets: 1,
                ..Default::default()
            });
        }
        let Some(bytes) = reader.get(&keys[0]).await? else {
            return bitmap::Bitmap::empty(budget);
        };
        let _raw = budget
            .map(|budget| budget.reserve(bytes.len()))
            .transpose()?;
        return bitmap::Bitmap::decode(&bytes, budget);
    }
    #[cfg(any(test, feature = "production-coverage"))]
    record(ReadKind::MultiGet);
    if let Some(budget) = budget {
        budget.record_reads(query_resources::StorageReadUsage {
            multi_get_batches: 1,
            multi_get_keys: keys.len(),
            ..Default::default()
        });
    }
    let values = reader.multi_get(&keys).await?;
    let _raw = budget
        .map(|budget| {
            budget.reserve(values.iter().flatten().fold(
                values.capacity().saturating_mul(size_of::<Option<Bytes>>()),
                |total, bytes| total.saturating_add(bytes.len()),
            ))
        })
        .transpose()?;
    let mut owners = bitmap::Bitmap::empty(budget)?;
    for bytes in values.into_iter().flatten() {
        owners = owners.union(bitmap::Bitmap::decode(&bytes, budget)?)?;
    }
    Ok(owners)
}

async fn lookup_legacy_equality_entries_admitted(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    lane: SecondaryEntryLane,
    value: &crate::encoding::v2::values::property::equality_index_value::CanonicalEqualityValue,
    budget: Option<&query_resources::Budget>,
) -> Result<bitmap::Bitmap> {
    let logical_prefix = ScopedKey::secondary_equality_entry_value_prefix(
        handle.index_id(),
        handle.generation(),
        lane,
        value,
    )?;
    let copies = if handle.scope().is_unscoped() { 1 } else { 2 };
    let _prefix_memory = budget
        .map(|budget| {
            budget.reserve(
                logical_prefix
                    .encoded_len()
                    .saturating_mul(copies)
                    .saturating_add(handle.scope().encoded_len()),
            )
        })
        .transpose()?;
    let prefix = IndexKey::data_prefix(handle.scope(), logical_prefix.to_bytes());
    let mut rows = reader.scan_prefix(prefix, ..).await?;
    if let Some(budget) = budget {
        budget.record_reads(query_resources::StorageReadUsage {
            scans: 1,
            ..Default::default()
        });
    }
    let mut owners = bitmap::SortedBuilder::new(budget)?;
    while let Some(row) = rows.next().await? {
        let _raw = budget
            .map(|budget| {
                budget.record_reads(query_resources::StorageReadUsage {
                    scan_rows: 1,
                    ..Default::default()
                });
                budget.reserve(row.key.len().saturating_add(row.value.len()))
            })
            .transpose()?;
        let key_owner = logical_prefix.parse_owner(handle.scope(), &row.key)?;
        let value_owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if key_owner != value_owner {
            return Err(corruption(
                "V3 non-unique equality key and value owners disagree",
            ));
        }
        owners.push(key_owner.get())?;
    }
    Ok(owners.finish())
}

trait ExactRangeAccumulator {
    type Output;

    fn accepted(&self) -> usize;
    fn accept(&mut self, owner: u64);
    fn finish(self) -> Self::Output;
}

#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
#[derive(Default)]
struct ExactRangeOwners(Vec<u64>);

#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
impl ExactRangeAccumulator for ExactRangeOwners {
    type Output = Vec<u64>;

    fn accepted(&self) -> usize {
        self.0.len()
    }

    fn accept(&mut self, owner: u64) {
        self.0.push(owner);
    }

    fn finish(self) -> Self::Output {
        self.0
    }
}

#[derive(Default)]
struct ExactRangeCount(usize);

impl ExactRangeAccumulator for ExactRangeCount {
    type Output = usize;

    fn accepted(&self) -> usize {
        self.0
    }

    fn accept(&mut self, _owner: u64) {
        self.0 = self.0.saturating_add(1);
    }

    fn finish(self) -> Self::Output {
        self.0
    }
}

/// Scans an exact range generation and returns planner-accepted owners.
///
/// Bitmap membership is evaluated in executable-plan order and `limit` is an
/// accepted-owner threshold, not a storage-row limit.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn scan_active_range_generation_with_membership(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
    limit: Option<usize>,
    membership: &[&roaring::RoaringTreemap],
) -> Result<Vec<u64>> {
    execute_active_range_generation_with_membership(
        reader,
        handle,
        query,
        limit,
        membership,
        ExactRangeOwners::default(),
    )
    .await
}

/// Counts an exact range generation without materializing accepted owners.
///
/// Bitmap membership is evaluated in executable-plan order and `limit` is an
/// accepted-owner threshold, so a bounded physical count stops as soon as the
/// planner-selected count window has enough verified matches.
pub(crate) async fn count_active_range_generation_with_membership(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
    limit: Option<usize>,
    membership: &[&roaring::RoaringTreemap],
) -> Result<usize> {
    execute_active_range_generation_with_membership(
        reader,
        handle,
        query,
        limit,
        membership,
        ExactRangeCount::default(),
    )
    .await
}

async fn execute_active_range_generation_with_membership<A: ExactRangeAccumulator>(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
    limit: Option<usize>,
    membership: &[&roaring::RoaringTreemap],
    accumulator: A,
) -> Result<A::Output> {
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary range serving received a non-secondary Active handle",
        ));
    };
    if !matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeRange { .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { .. }
    ) {
        return Err(corruption(
            "secondary range serving received an equality definition",
        ));
    }

    let direction = match definition.direction() {
        RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
        RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
    };
    let lane = definition_lane(definition);
    let bounds = match query {
        Some(query) => match secondary_range_scan_bounds(direction, query)? {
            Some(bounds) => bounds,
            None => return Ok(accumulator.finish()),
        },
        None => (Bound::Unbounded, Bound::Unbounded),
    };
    if limit == Some(0) {
        return Ok(accumulator.finish());
    }
    let prefix = IndexKey::data_prefix(
        handle.scope(),
        ScopedKey::secondary_lane_prefix(handle.index_id(), handle.generation(), lane),
    );
    let rows = reader.scan_prefix(&prefix, bounds).await?;
    consume_active_range_rows(
        reader,
        handle,
        definition,
        direction,
        lane,
        rows,
        query,
        limit,
        membership,
        accumulator,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn consume_active_range_rows<A: ExactRangeAccumulator>(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    definition: &ValidatedSecondaryIndexDefinition,
    direction: StorageRangeIndexDirection,
    lane: SecondaryEntryLane,
    mut rows: impl ExactRangeRows,
    query: Option<&SecondaryRangeQuery>,
    limit: Option<usize>,
    membership: &[&roaring::RoaringTreemap],
    mut accumulator: A,
) -> Result<A::Output> {
    while let Some(row) = rows.next_exact().await? {
        let IndexKey::Data {
            kind: ScopedKey::SecondaryEntry(key),
            ..
        } = IndexKey::parse_from_slice(handle.scope(), &row.key)?
        else {
            return Err(corruption(
                "secondary range prefix yielded another key kind",
            ));
        };
        if key.index_id() != handle.index_id()
            || key.generation() != handle.generation()
            || key.lane() != lane
        {
            return Err(corruption(
                "secondary range entry escaped its exact serving prefix",
            ));
        }
        let key_owner = key
            .entity_id()
            .expect("the validated range lane always carries its key owner");
        let value_owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if key_owner != value_owner {
            return Err(corruption(
                "secondary range entry key/value owners disagree",
            ));
        }
        let key_value = key
            .range_value()
            .expect("the validated range lane always carries a range value");
        if !membership
            .iter()
            .all(|bitmap| bitmap.contains(value_owner.get()))
        {
            continue;
        }
        if !authoritative_range_matches(
            reader,
            handle.scope(),
            definition,
            value_owner,
            direction,
            key_value,
            query,
            &UnobservedRangeScan,
        )
        .await?
        {
            continue;
        }
        accumulator.accept(value_owner.get());
        if limit.is_some_and(|limit| accumulator.accepted() >= limit) {
            break;
        }
    }
    Ok(accumulator.finish())
}

#[cfg(all(feature = "production-coverage", not(test)))]
pub(crate) async fn run_production_contracts() {
    use slatedb::object_store::memory::InMemory;

    fn active_handle(
        definition: crate::index_lifecycle::ValidatedDynamicIndexDefinition,
        physical: crate::index_lifecycle::PhysicalGeneration,
    ) -> ActiveIndexHandle {
        let building = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::initial(),
            definition,
            crate::index_lifecycle::IndexRevision::initial(),
            physical,
            crate::index_lifecycle::IndexOperationId::new_v4(),
        )
        .expect("exact serving fixture starts building");
        let active = building
            .transition(crate::index_lifecycle::IndexStateTransition::Activate)
            .expect("exact serving fixture activates");
        ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("exact serving fixture projects an Active handle")
    }

    fn secondary_handle(definition: crate::config::SecondaryIndexDefinition) -> ActiveIndexHandle {
        active_handle(
            crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(definition)
                .expect("exact secondary fixture validates"),
            crate::index_lifecycle::PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
        )
    }

    async fn put_entry(db: &slatedb::Db, handle: &ActiveIndexHandle, value: &str, entity_id: u64) {
        let definition = handle
            .secondary_definition()
            .expect("exact entry fixture uses a secondary handle");
        let direction = match definition.direction() {
            RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
            RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
        };
        let canonical = if definition_uses_equality_bitmap(definition)
            || matches!(
                definition,
                ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. }
            ) {
            let EqualityValueProjection::Indexed(value) =
                project_equality_value(&PropertyValue::String(value.to_owned()))
            else {
                panic!("string equality fixtures are always indexable")
            };
            CanonicalSecondaryValue::equality(value)
        } else {
            let RangeValueProjection::Indexed(value) =
                project_range_value(&PropertyValue::String(value.to_owned()), direction)
            else {
                panic!("string range fixtures are always indexable")
            };
            CanonicalSecondaryValue::range(value)
        };
        let entity_id = IndexEntityId::new(entity_id);
        let lane = definition_lane(definition);
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            canonical,
            entity_id,
        )
        .expect("exact entry key validates");
        let value_bytes = if definition_uses_equality_bitmap(definition) {
            SecondaryEqualityBitmapValue::new(roaring::RoaringTreemap::from_iter([entity_id.get()]))
                .encode()
        } else {
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane,
                entity_id,
            })
        };
        db.put(key, value_bytes)
            .await
            .expect("exact entry persists");
        db.put(
            authoritative_property_key(
                handle.scope(),
                IndexEntity {
                    kind: definition.element_kind(),
                    id: entity_id,
                },
            ),
            crate::encoding::v2::values::property::encode_properties(&[
                Property::string("$label", definition.label().as_str()),
                Property::string(definition.property().as_str(), value),
            ]),
        )
        .await
        .expect("exact authoritative row persists");
    }

    let db = slatedb::Db::builder(
        "secondary-exact-production-contracts",
        Arc::new(InMemory::new()),
    )
    .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
    .build()
    .await
    .expect("exact serving database opens");
    let equality = secondary_handle(
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
    );
    let unique = secondary_handle(
        crate::config::SecondaryIndexDefinition::node_unique_equality("User", "value").unwrap(),
    );
    let range = secondary_handle(
        crate::config::SecondaryIndexDefinition::node_range_desc("User", "value").unwrap(),
    );
    let text_definition = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
        crate::config::TextIndexDefinition::new_node("User", "value").unwrap(),
    )
    .unwrap();
    let text = active_handle(
        text_definition,
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
    );

    assert!(lookup_active_equality_point_literal(
        &db,
        &text,
        &PropertyValue::String("value".to_string()),
    )
    .await
    .is_err());
    assert!(lookup_active_equality_point_literal(
        &db,
        &range,
        &PropertyValue::String("value".to_string()),
    )
    .await
    .is_err());
    assert!(
        lookup_active_equality_point_literal(&db, &equality, &PropertyValue::Null)
            .await
            .is_err()
    );
    assert!(lookup_active_equality_point_literal(
        &db,
        &equality,
        &PropertyValue::String("missing".to_string()),
    )
    .await
    .unwrap()
    .is_empty());
    let oversized = PropertyValue::String("x".repeat(
        crate::encoding::v2::values::property::equality_index_value::MAX_EQUALITY_CANONICAL_LEN + 1,
    ));
    assert!(matches!(
        lookup_active_equality_point_literal(&db, &equality, &oversized).await,
        Err(HelixDbError::SecondaryIndexValue(
            SecondaryIndexValueError::EncodedKeyTooLarge { .. }
        ))
    ));

    put_entry(&db, &equality, "same", 3).await;
    put_entry(&db, &unique, "owner", 7).await;
    assert_eq!(
        lookup_active_equality_point_literal(
            &db,
            &unique,
            &PropertyValue::String("owner".to_string()),
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>(),
        vec![7]
    );
    assert!(lookup_active_equality_literal_batch(
        &db,
        &equality,
        &[PropertyValue::String("same".to_string())],
    )
    .await
    .is_err());
    assert!(matches!(
        lookup_active_equality_literal_batch(
            &db,
            &equality,
            &[PropertyValue::String("same".to_string()), oversized],
        )
        .await,
        Err(HelixDbError::SecondaryIndexValue(
            SecondaryIndexValueError::EncodedKeyTooLarge { .. }
        ))
    ));
    assert!(lookup_active_equality_literal_batch(
        &db,
        &text,
        &[
            PropertyValue::String("same".to_string()),
            PropertyValue::String("other".to_string()),
        ],
    )
    .await
    .is_err());
    assert!(lookup_active_equality_literal_batch(
        &db,
        &unique,
        &[
            PropertyValue::String("same".to_string()),
            PropertyValue::String("other".to_string()),
        ],
    )
    .await
    .is_err());
    assert!(lookup_active_equality_literal_batch(
        &db,
        &equality,
        &[
            PropertyValue::String("same".to_string()),
            PropertyValue::Null,
        ],
    )
    .await
    .is_err());
    assert_eq!(
        lookup_active_equality_literal_batch(
            &db,
            &equality,
            &[
                PropertyValue::String("same".to_string()),
                PropertyValue::String("missing".to_string()),
            ],
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>(),
        vec![3]
    );

    assert!(
        scan_active_range_generation_with_membership(&db, &text, None, None, &[])
            .await
            .is_err()
    );
    assert!(
        scan_active_range_generation_with_membership(&db, &equality, None, None, &[])
            .await
            .is_err()
    );
    assert!(scan_active_range_generation_with_membership(
        &db,
        &range,
        Some(&SecondaryRangeQuery::Between {
            lower: PropertyValue::String("z".to_string()),
            lower_inclusive: true,
            upper: PropertyValue::String("a".to_string()),
            upper_inclusive: true,
        }),
        None,
        &[],
    )
    .await
    .unwrap()
    .is_empty());

    put_entry(&db, &range, "a", 1).await;
    put_entry(&db, &range, "b", 2).await;
    let rejects_all = roaring::RoaringTreemap::new();
    assert!(
        scan_active_range_generation_with_membership(&db, &range, None, None, &[&rejects_all],)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        scan_active_range_generation_with_membership(&db, &range, None, Some(1), &[])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        count_active_range_generation_with_membership(&db, &range, None, None, &[])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        count_active_range_generation_with_membership(&db, &range, None, Some(1), &[])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        count_active_range_generation_with_membership(&db, &range, None, Some(0), &[])
            .await
            .unwrap(),
        0
    );
    db.close().await.expect("exact serving database closes");

    // These calls are linked into the production library (not cfg(test)), so
    // the production coverage gate exercises reverse serving and stale recovery.
    for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
        for definition in [
            crate::config::SecondaryIndexDefinition::node_range_with_direction(
                "Item", "value", direction,
            )
            .unwrap(),
            crate::config::SecondaryIndexDefinition::edge_range_with_direction(
                "LINK", "value", direction,
            )
            .unwrap(),
        ] {
            let db = slatedb::Db::builder("ordered-range-production", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap();
            let handle = secondary_handle(definition);
            for (id, value) in [(1, "a"), (2, "b"), (3, "b"), (4, "b"), (5, "c")] {
                put_entry(&db, &handle, value, id).await;
            }
            for iteration in [
                helix_planner::ir::RangeScanIteration::Forward,
                helix_planner::ir::RangeScanIteration::Reverse,
            ] {
                let expected = if iteration.effective_direction(match direction {
                    RangeIndexDirection::Asc => helix_ast::index::RangeIndexDirection::Asc,
                    RangeIndexDirection::Desc => helix_ast::index::RangeIndexDirection::Desc,
                }) == helix_ast::index::RangeIndexDirection::Asc
                {
                    vec![1, 2, 3, 4, 5]
                } else {
                    vec![5, 2, 3, 4, 1]
                };
                for limit in [None, Some(0), Some(1), Some(3), Some(20)] {
                    assert_eq!(
                        scan_active_range_generation_ordered(
                            &db,
                            &handle,
                            None,
                            iteration,
                            limit,
                            &[],
                            &UnobservedRangeScan
                        )
                        .await
                        .unwrap(),
                        expected
                            .iter()
                            .copied()
                            .take(limit.unwrap_or(usize::MAX))
                            .collect::<Vec<_>>()
                    );
                }
                let membership = roaring::RoaringTreemap::from_iter([2, 4]);
                assert_eq!(
                    scan_active_range_generation_ordered(
                        &db,
                        &handle,
                        None,
                        iteration,
                        Some(2),
                        &[&membership],
                        &UnobservedRangeScan
                    )
                    .await
                    .unwrap(),
                    vec![2, 4]
                );
            }
            let query = SecondaryRangeQuery::Between {
                lower: PropertyValue::String("b".into()),
                lower_inclusive: true,
                upper: PropertyValue::String("b".into()),
                upper_inclusive: true,
            };
            // Retained IDs 2 and 3 include a stale row; ID 4 was discarded.
            // Recovery must replace the provisional prefix and return [2, 4] once.
            let property_key = |id| {
                authoritative_property_key(
                    handle.scope(),
                    IndexEntity {
                        kind: handle.secondary_definition().unwrap().element_kind(),
                        id: IndexEntityId::new(id),
                    },
                )
            };
            db.delete(property_key(3)).await.unwrap();
            for (limit, expected) in [
                (Some(2), vec![2, 4]),
                (None, vec![2, 4]),
                (Some(4), vec![2, 4]),
            ] {
                assert_eq!(
                    scan_active_range_generation_ordered(
                        &db,
                        &handle,
                        Some(&query),
                        helix_planner::ir::RangeScanIteration::Reverse,
                        limit,
                        &[],
                        &UnobservedRangeScan
                    )
                    .await
                    .unwrap(),
                    expected
                );
            }
            // Exhausted recovery: every discarded candidate is stale too.
            db.delete(property_key(4)).await.unwrap();
            assert_eq!(
                scan_active_range_generation_ordered(
                    &db,
                    &handle,
                    Some(&query),
                    helix_planner::ir::RangeScanIteration::Reverse,
                    Some(2),
                    &[],
                    &UnobservedRangeScan
                )
                .await
                .unwrap(),
                vec![2]
            );
            // Skipped blobs are not audited, but an attempted decode must fail.
            db.put(property_key(2), vec![255]).await.unwrap();
            assert!(scan_active_range_generation_ordered(
                &db,
                &handle,
                Some(&query),
                helix_planner::ir::RangeScanIteration::Reverse,
                Some(2),
                &[],
                &UnobservedRangeScan
            )
            .await
            .is_err());
            assert!(scan_active_range_generation_ordered(
                &db,
                &handle,
                Some(&query),
                helix_planner::ir::RangeScanIteration::Reverse,
                Some(2),
                &[&roaring::RoaringTreemap::new()],
                &UnobservedRangeScan
            )
            .await
            .unwrap()
            .is_empty());
            db.close().await.unwrap();
        }
    }
}

#[cfg(test)]
pub(crate) use ordered::RangeScanCounters;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn equality_key_admission_precedes_large_allocations_and_storage_reads() {
        let db = super::super::tests::test_db("secondary-key-admission").await;
        let value = PropertyValue::String("x".repeat(64 * 1024));
        let values = [value.clone(), value.clone()];
        let definitions = [
            crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
            crate::config::SecondaryIndexDefinition::node_unique_equality("User", "unique")
                .unwrap(),
            crate::config::SecondaryIndexDefinition::edge_equality("KNOWS", "value").unwrap(),
        ];
        for definition in definitions {
            let handle = super::super::tests::active_read_handle(&db, definition).await;
            for compatibility in [
                ReaderStorageCompatibility::Current,
                ReaderStorageCompatibility::LegacyEqualityUnion,
            ] {
                let budget = query_resources::Budget::new(1024);
                let mut future = std::pin::pin!(lookup_active_equality_point_admitted(
                    &db,
                    &handle,
                    &value,
                    compatibility,
                    Some(&budget)
                ));
                let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
                let (result, allocations) = crate::allocation_testing::observe(|| {
                    std::future::Future::poll(future.as_mut(), &mut context)
                });
                assert!(matches!(
                    result,
                    std::task::Poll::Ready(Err(HelixDbError::QueryMemoryLimitExceeded))
                ));
                assert_eq!(
                    allocations.allocations, 0,
                    "the rejected canonical buffer was never constructed"
                );
                assert_eq!(budget.available(), 1024);
                assert_eq!(budget.reads(), query_resources::StorageReadUsage::default());

                if !definition_uses_equality_bitmap(handle.secondary_definition().unwrap()) {
                    continue;
                }
                let mut future = std::pin::pin!(lookup_active_equality_batch_admitted(
                    &db,
                    &handle,
                    &values,
                    compatibility,
                    Some(&budget)
                ));
                let (result, allocations) = crate::allocation_testing::observe(|| {
                    std::future::Future::poll(future.as_mut(), &mut context)
                });
                assert!(matches!(
                    result,
                    std::task::Poll::Ready(Err(HelixDbError::QueryMemoryLimitExceeded))
                ));
                // Current batches may admit their small output vector before
                // encountering a rejected canonical member; legacy uses points.
                assert!(allocations.bytes <= values.len() * size_of::<Bytes>());
                assert_eq!(budget.available(), 1024);
                assert_eq!(budget.reads(), query_resources::StorageReadUsage::default());
            }
            let budget = query_resources::Budget::new(512 * 1024);
            let ids = lookup_active_equality_point_admitted(
                &db,
                &handle,
                &value,
                ReaderStorageCompatibility::Current,
                Some(&budget),
            )
            .await
            .unwrap();
            assert!(ids.is_empty());
            assert_eq!(budget.reads().point_gets, 1);
            drop(ids);
            assert_eq!(budget.available(), 512 * 1024);
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn admitted_reads_bound_decoding_and_keep_guards_through_compatibility_and_iteration() {
        let db = super::super::tests::test_db("secondary-admitted-bitmap").await;
        let handle = super::super::tests::active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        )
        .await;
        put_v4_equality_bitmap(&db, &handle, "dense", 0..20000).await;
        put_v4_equality_bitmap(&db, &handle, "small", [1, 3]).await;
        put_v3_equality_entry(&db, &handle, "small", 0).await;
        put_v3_equality_entry(&db, &handle, "small", 2).await;
        let dense = PropertyValue::String("dense".into());
        let small = PropertyValue::String("small".into());
        let missing = PropertyValue::String("missing".into());
        for limit in [1, 9000] {
            let budget = query_resources::Budget::new(limit);
            assert!(matches!(
                lookup_active_equality_point_admitted(
                    &db,
                    &handle,
                    &dense,
                    ReaderStorageCompatibility::Current,
                    Some(&budget)
                )
                .await,
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
            assert_eq!(budget.available(), limit);
            // A one-byte budget now rejects canonical/key construction before
            // storage I/O; the larger budget reaches bitmap decoding.
            assert_eq!(budget.reads().point_gets, usize::from(limit > 1));
            assert!(matches!(
                lookup_active_equality_batch_admitted(
                    &db,
                    &handle,
                    &[small.clone(), dense.clone()],
                    ReaderStorageCompatibility::Current,
                    Some(&budget)
                )
                .await,
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
            assert_eq!(budget.available(), limit);
            assert_eq!(budget.reads().multi_get_batches, usize::from(limit > 1));
            assert_eq!(budget.reads().multi_get_keys, if limit > 1 { 2 } else { 0 });
        }
        for compatibility in [
            ReaderStorageCompatibility::Current,
            ReaderStorageCompatibility::LegacyEqualityUnion,
        ] {
            let budget = query_resources::Budget::new(1_000_000);
            let expected = if compatibility == ReaderStorageCompatibility::Current {
                vec![1, 3]
            } else {
                vec![0, 1, 2, 3]
            };
            let ids = lookup_active_equality_point_admitted(
                &db,
                &handle,
                &small,
                compatibility,
                Some(&budget),
            )
            .await
            .unwrap();
            let retained = budget.available();
            assert!(retained < 1_000_000);
            let mut cursor = ids.into_iter();
            assert_eq!(cursor.next(), Some(expected[0]));
            assert_eq!(budget.available(), retained);
            drop(cursor);
            assert_eq!(budget.available(), 1_000_000);
            let ids = lookup_active_equality_batch_admitted(
                &db,
                &handle,
                &[small.clone(), missing.clone()],
                compatibility,
                Some(&budget),
            )
            .await
            .unwrap();
            assert_eq!(ids.into_iter().collect::<Vec<_>>(), expected);
            assert_eq!(budget.available(), 1_000_000);
            if compatibility == ReaderStorageCompatibility::LegacyEqualityUnion {
                assert_eq!(budget.reads().scans, 3);
                assert_eq!(budget.reads().scan_rows, 4);
            }
        }
        let unique = super::super::tests::active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_unique_equality("User", "unique")
                .unwrap(),
        )
        .await;
        super::super::tests::put_read_entry(&db, &unique, "small", 42).await;
        let budget = query_resources::Budget::new(1_000_000);
        for (value, expected) in [(small, vec![42]), (missing, vec![])] {
            let ids = lookup_active_equality_point_admitted(
                &db,
                &unique,
                &value,
                ReaderStorageCompatibility::Current,
                Some(&budget),
            )
            .await
            .unwrap();
            assert_eq!(ids.into_iter().collect::<Vec<_>>(), expected);
            assert_eq!(budget.available(), 1_000_000);
        }
        db.close().await.unwrap();
    }

    async fn put_v3_equality_entry(
        db: &slatedb::Db,
        handle: &ActiveIndexHandle,
        value: &str,
        entity_id: u64,
    ) {
        let definition = handle.secondary_definition().unwrap();
        let lane = definition_lane(definition);
        let entity_id = IndexEntityId::new(entity_id);
        let EqualityValueProjection::Indexed(value) =
            project_equality_value(&PropertyValue::String(value.to_string()))
        else {
            unreachable!("string fixture is indexable")
        };
        let key = IndexKey::Data {
            scope: handle.scope(),
            kind: ScopedKey::SecondaryEntry(
                SecondaryEntryKey::try_new(
                    handle.index_id(),
                    handle.generation(),
                    lane,
                    CanonicalSecondaryValue::equality(value),
                    Some(entity_id),
                )
                .unwrap(),
            ),
        }
        .to_bytes();
        db.put(
            key,
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane,
                entity_id,
            }),
        )
        .await
        .unwrap();
    }

    async fn put_v4_equality_bitmap(
        db: &slatedb::Db,
        handle: &ActiveIndexHandle,
        value: &str,
        entity_ids: impl IntoIterator<Item = u64>,
    ) {
        let definition = handle.secondary_definition().unwrap();
        let EqualityValueProjection::Indexed(value) =
            project_equality_value(&PropertyValue::String(value.to_string()))
        else {
            unreachable!("string fixture is indexable")
        };
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::equality(value),
            IndexEntityId::initial(),
        )
        .unwrap();
        db.put(
            key,
            SecondaryEqualityBitmapValue::new(roaring::RoaringTreemap::from_iter(entity_ids))
                .encode(),
        )
        .await
        .unwrap();
    }

    struct FailingRows;

    #[tokio::test]
    async fn legacy_equality_reads_union_v3_entries_and_v4_bitmaps_without_duplicates() {
        let db = super::super::tests::test_db("secondary-exact-legacy-equality-union").await;
        let handle = super::super::tests::active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .await;
        put_v3_equality_entry(&db, &handle, "shared", 1).await;
        put_v3_equality_entry(&db, &handle, "shared", 2).await;
        put_v3_equality_entry(&db, &handle, "other", 4).await;
        put_v4_equality_bitmap(&db, &handle, "shared", [2, 3]).await;

        let shared = PropertyValue::String("shared".to_string());
        assert_eq!(
            lookup_active_equality_point_literal_with_compatibility(
                &db,
                &handle,
                &shared,
                ReaderStorageCompatibility::LegacyEqualityUnion,
            )
            .await
            .unwrap(),
            roaring::RoaringTreemap::from_iter([1, 2, 3])
        );
        assert_eq!(
            lookup_active_equality_point_literal_with_compatibility(
                &db,
                &handle,
                &shared,
                ReaderStorageCompatibility::Current,
            )
            .await
            .unwrap(),
            roaring::RoaringTreemap::from_iter([2, 3])
        );
        assert_eq!(
            lookup_active_equality_literal_batch_with_compatibility(
                &db,
                &handle,
                &[shared, PropertyValue::String("other".to_string())],
                ReaderStorageCompatibility::LegacyEqualityUnion,
            )
            .await
            .unwrap(),
            roaring::RoaringTreemap::from_iter([1, 2, 3, 4])
        );
        db.close().await.unwrap();
    }

    #[async_trait]
    impl ExactRangeRows for FailingRows {
        async fn next_exact(
            &mut self,
        ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
            Err(slatedb::Error::unavailable(
                "injected exact iterator failure".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn exact_range_row_contract_propagates_iterator_failure() {
        let db = super::super::tests::test_db("secondary-exact-iterator-error").await;
        let handle = super::super::tests::active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        )
        .await;
        let definition = handle.secondary_definition().unwrap();
        assert!(consume_active_range_rows(
            &db,
            &handle,
            definition,
            StorageRangeIndexDirection::Asc,
            definition_lane(definition),
            FailingRows,
            None,
            None,
            &[],
            ExactRangeOwners::default(),
        )
        .await
        .is_err());
        assert!(consume_active_range_rows(
            &db,
            &handle,
            definition,
            StorageRangeIndexDirection::Asc,
            definition_lane(definition),
            FailingRows,
            None,
            None,
            &[],
            ExactRangeCount::default(),
        )
        .await
        .is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn exact_equality_literals_reject_non_indexed_values_and_preserve_size_errors() {
        let db = super::super::tests::test_db("secondary-exact-equality-values").await;
        let handle = super::super::tests::active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        )
        .await;

        for value in [
            PropertyValue::Null,
            PropertyValue::F64(f64::NAN),
            PropertyValue::Array(Vec::new()),
        ] {
            assert!(matches!(
                lookup_active_equality_point_literal(&db, &handle, &value).await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
            assert!(matches!(
                lookup_active_equality_literal_batch(
                    &db,
                    &handle,
                    &[PropertyValue::String("indexed".to_string()), value],
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }

        let oversized = PropertyValue::String("x".repeat(
            crate::encoding::v2::values::property::equality_index_value::MAX_EQUALITY_CANONICAL_LEN
                + 1,
        ));
        assert!(matches!(
            lookup_active_equality_point_literal(&db, &handle, &oversized).await,
            Err(HelixDbError::SecondaryIndexValue(
                SecondaryIndexValueError::EncodedKeyTooLarge { .. }
            ))
        ));
        assert!(matches!(
            lookup_active_equality_literal_batch(
                &db,
                &handle,
                &[PropertyValue::String("indexed".to_string()), oversized],
            )
            .await,
            Err(HelixDbError::SecondaryIndexValue(
                SecondaryIndexValueError::EncodedKeyTooLarge { .. }
            ))
        ));
        db.close().await.unwrap();
    }
}
