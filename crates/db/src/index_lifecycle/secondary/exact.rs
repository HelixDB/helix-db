//! Literal secondary-index primitives selected by executable plans.

use super::*;

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
    BENCHMARK_GRAPH_READS.fetch_add(1, AtomicOrdering::Relaxed);
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

pub(crate) async fn lookup_active_equality_point_literal_with_compatibility(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
    compatibility: ReaderStorageCompatibility,
) -> Result<roaring::RoaringTreemap> {
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
    let value = match project_equality_value(value) {
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
    let lane = definition_lane(definition);
    let legacy_value = value.clone();
    let key = secondary_entry_key(
        handle.scope(),
        handle.index_id(),
        handle.generation(),
        definition,
        CanonicalSecondaryValue::equality(value),
        IndexEntityId::initial(),
    )
    .expect("validated indexed equality values always fit their physical key");
    record_equality_point_read();
    if lane.is_unique() {
        let Some(bytes) = reader.get(key).await? else {
            return Ok(roaring::RoaringTreemap::new());
        };
        let owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &bytes)?;
        return Ok(roaring::RoaringTreemap::from_iter([owner.get()]));
    }
    let mut owners = reader
        .get(key)
        .await?
        .map(|bytes| {
            SecondaryEqualityBitmapValue::decode(&bytes)
                .map(SecondaryEqualityBitmapValue::into_ids)
                .map_err(HelixDbError::from)
        })
        .transpose()?
        .unwrap_or_default();
    if compatibility == ReaderStorageCompatibility::LegacyEqualityUnion {
        owners |= lookup_legacy_equality_entries(reader, handle, lane, &legacy_value).await?;
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

pub(crate) async fn lookup_active_equality_literal_batch_with_compatibility(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
    compatibility: ReaderStorageCompatibility,
) -> Result<roaring::RoaringTreemap> {
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
        let mut owners = roaring::RoaringTreemap::new();
        for value in values {
            owners |= lookup_active_equality_point_literal_with_compatibility(
                reader,
                handle,
                value,
                compatibility,
            )
            .await?;
        }
        return Ok(owners);
    }
    let keys = values
        .iter()
        .map(|value| {
            let value = match project_equality_value(value) {
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
            secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                definition,
                CanonicalSecondaryValue::equality(value),
                IndexEntityId::initial(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    keys.iter().for_each(|_| record_equality_point_read());
    #[cfg(any(test, feature = "production-coverage"))]
    BENCHMARK_MULTI_GETS.fetch_add(1, AtomicOrdering::Relaxed);
    let mut owners = roaring::RoaringTreemap::new();
    for bytes in reader.multi_get(&keys).await?.into_iter().flatten() {
        owners |= SecondaryEqualityBitmapValue::decode(&bytes)?.into_ids();
    }
    Ok(owners)
}

async fn lookup_legacy_equality_entries(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    lane: SecondaryEntryLane,
    value: &crate::encoding::v2::values::property::equality_index_value::CanonicalEqualityValue,
) -> Result<roaring::RoaringTreemap> {
    let logical_prefix = ScopedKey::secondary_equality_entry_value_prefix(
        handle.index_id(),
        handle.generation(),
        lane,
        value,
    )?;
    let prefix = IndexKey::data_prefix(handle.scope(), logical_prefix);
    let mut rows = reader.scan_prefix(prefix, ..).await?;
    let mut owners = roaring::RoaringTreemap::new();
    while let Some(row) = rows.next().await? {
        let IndexKey::Data {
            kind: ScopedKey::SecondaryEntry(entry),
            ..
        } = IndexKey::parse_from_slice(handle.scope(), &row.key)?
        else {
            return Err(corruption(
                "V3 equality value prefix resolved a different record kind",
            ));
        };
        let Some(key_owner) = entry.entity_id() else {
            return Err(corruption(
                "V3 non-unique equality entry omitted its entity ID",
            ));
        };
        let value_owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if key_owner != value_owner {
            return Err(corruption(
                "V3 non-unique equality key and value owners disagree",
            ));
        }
        owners.insert(key_owner.get());
    }
    Ok(owners)
}

trait ExactRangeAccumulator {
    type Output;

    fn accepted(&self) -> usize;
    fn accept(&mut self, owner: u64);
    fn finish(self) -> Self::Output;
}

#[cfg(any(test, feature = "production-coverage"))]
#[derive(Default)]
struct ExactRangeOwners(Vec<u64>);

#[cfg(any(test, feature = "production-coverage"))]
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
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn scan_active_range_generation_with_membership(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
    limit: Option<usize>,
    membership: &[roaring::RoaringTreemap],
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
    membership: &[roaring::RoaringTreemap],
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
    membership: &[roaring::RoaringTreemap],
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
    membership: &[roaring::RoaringTreemap],
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
        if !authoritative_range_matches(
            reader,
            handle.scope(),
            definition,
            value_owner,
            direction,
            key_value,
            query,
        )
        .await?
        {
            continue;
        }
        if !membership
            .iter()
            .all(|bitmap| bitmap.contains(value_owner.get()))
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
        scan_active_range_generation_with_membership(&db, &range, None, None, &[rejects_all],)
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
