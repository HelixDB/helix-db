//! Production-linked contracts for unique equality batches. The reader is an
//! immutable request view with real encoded rows and explicit storage faults.
//! Harness code stays outside the measured production source tree.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::config;
use crate::encoding::v2::keys::scope;
use crate::encoding::v2::values::property;
use crate::index_lifecycle as lifecycle;

#[derive(Clone, Copy, Default)]
enum ReadFault {
    #[default]
    None,
    MultiGet,
    ShortMultiGet,
    Get,
    PendingGet,
}

#[derive(Default)]
struct Reader {
    rows: BTreeMap<Bytes, Bytes>,
    fault: ReadFault,
    batches: AtomicUsize,
    gets: AtomicUsize,
}

#[async_trait]
impl DbReadOps for Reader {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        _: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if matches!(self.fault, ReadFault::Get) {
            return Err(slatedb::Error::unavailable(
                "injected row read failure".into(),
            ));
        }
        if matches!(self.fault, ReadFault::PendingGet) {
            std::future::pending::<()>().await;
        }
        Ok(self.rows.get(key.as_ref()).cloned())
    }

    async fn multi_get_with_options<K: AsRef<[u8]> + Send + Sync>(
        &self,
        keys: &[K],
        _: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error> {
        self.batches.fetch_add(1, Ordering::Relaxed);
        match self.fault {
            ReadFault::MultiGet => Err(slatedb::Error::unavailable(
                "injected owner read failure".into(),
            )),
            ReadFault::ShortMultiGet => Ok(Vec::new()),
            ReadFault::None | ReadFault::Get | ReadFault::PendingGet => Ok(keys
                .iter()
                .map(|key| self.rows.get(key.as_ref()).cloned())
                .collect()),
        }
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        _: K,
        _: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        panic!("unique batch must use the bounded multi-get and authoritative get paths")
    }

    async fn scan_with_options<T: slatedb::ByteRangeBounds + Send>(
        &self,
        _: T,
        _: &slatedb::config::ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error> {
        panic!("unique batch must not scan")
    }
}

fn handle(
    definition: config::SecondaryIndexDefinition,
    scope: DataScope,
    generation: IndexGenerationId,
) -> ActiveIndexHandle {
    let active = IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::try_from(definition).unwrap(),
        lifecycle::IndexRevision::initial(),
        lifecycle::PhysicalGeneration::Secondary { generation },
        lifecycle::IndexOperationId::new_v4(),
    )
    .unwrap()
    .transition(lifecycle::IndexStateTransition::Activate)
    .unwrap();
    ActiveIndexHandle::try_from_record(scope, &active).unwrap()
}

fn seed(
    reader: &mut Reader,
    handle: &ActiveIndexHandle,
    value: PropertyValue,
    owner: u64,
) -> (Bytes, Bytes) {
    let definition = handle.secondary_definition().unwrap();
    let EqualityValueProjection::Indexed(canonical) = project_equality_value(&value) else {
        panic!("fixture value must be indexable")
    };
    let entity_id = IndexEntityId::new(owner);
    let key = secondary_entry_key(
        handle.scope(),
        handle.index_id(),
        handle.generation(),
        definition,
        CanonicalSecondaryValue::equality(canonical),
        entity_id,
    )
    .unwrap();
    reader.rows.insert(
        key.clone(),
        encode_secondary_entry(&SecondaryEntryValue {
            index_id: handle.index_id(),
            generation: handle.generation(),
            lane: definition_lane(definition),
            entity_id,
        }),
    );
    let row = authoritative_property_key(
        handle.scope(),
        IndexEntity {
            kind: IndexElementKind::Node,
            id: entity_id,
        },
    );
    reader.rows.insert(
        row.clone(),
        property::encode_properties(&[
            Property::string("$label", definition.label().as_str()),
            Property {
                name: definition.property().as_str().into(),
                value,
            },
        ]),
    );
    (key, row)
}

pub(crate) async fn run() {
    admission_contracts().await;
    let unique = handle(
        config::SecondaryIndexDefinition::node_unique_equality("Fixture", "value").unwrap(),
        DataScope::LegacyUnscoped,
        IndexGenerationId::initial(),
    );
    let values = [PropertyValue::I64(7), PropertyValue::I64(8)];
    let reader = Reader::default();
    for values in [&[][..], &values[..1]] {
        assert!(matches!(
            lookup_active_unique_equality_batch(&reader, &unique, values).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
    for definition in [
        config::SecondaryIndexDefinition::node_equality("Fixture", "value").unwrap(),
        config::SecondaryIndexDefinition::node_range("Fixture", "value").unwrap(),
    ] {
        let wrong = handle(definition, unique.scope(), unique.generation());
        assert!(matches!(
            lookup_active_unique_equality_batch(&reader, &wrong, &values).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
    for invalid in [
        PropertyValue::Null,
        PropertyValue::F64(f64::NAN),
        PropertyValue::Array(vec![]),
    ] {
        assert!(matches!(
            lookup_active_unique_equality_batch(&reader, &unique, &[values[0].clone(), invalid])
                .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
    let oversized = PropertyValue::String(
        "x".repeat(property::equality_index_value::MAX_EQUALITY_CANONICAL_LEN + 1),
    );
    assert!(matches!(
        lookup_active_unique_equality_batch(&reader, &unique, &[values[0].clone(), oversized])
            .await,
        Err(HelixDbError::SecondaryIndexValue(
            SecondaryIndexValueError::EncodedKeyTooLarge { .. }
        ))
    ));
    assert_eq!(
        reader.batches.load(Ordering::Relaxed),
        0,
        "invalid inputs never reach storage"
    );
    assert_eq!(reader.gets.load(Ordering::Relaxed), 0);
    assert!(
        lookup_active_unique_equality_batch(&reader, &unique, &values)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(reader.batches.load(Ordering::Relaxed), 1);
    assert_eq!(
        reader.gets.load(Ordering::Relaxed),
        0,
        "absent owners need no row reads"
    );

    // Identical literals in different tenants/generations must select only the
    // handle's owner rows. Numeric aliases must verify and deduplicate correctly.
    let mut reader = Reader::default();
    let scopes = [
        (DataScope::LegacyUnscoped, 1, 11),
        (DataScope::Tenant(scope::TenantId::from_u128(1)), 1, 21),
        (DataScope::Tenant(scope::TenantId::from_u128(2)), 1, 31),
        (DataScope::Tenant(scope::TenantId::from_u128(1)), 2, 41),
    ];
    for (scope, generation, owner) in scopes {
        let handle = handle(
            config::SecondaryIndexDefinition::node_unique_equality("Fixture", "value").unwrap(),
            scope,
            IndexGenerationId::new(generation).unwrap(),
        );
        seed(&mut reader, &handle, PropertyValue::I64(7), owner);
        seed(&mut reader, &handle, PropertyValue::I64(8), owner + 1);
    }
    for (scope, generation, owner) in scopes {
        let handle = handle(
            config::SecondaryIndexDefinition::node_unique_equality("Fixture", "value").unwrap(),
            scope,
            IndexGenerationId::new(generation).unwrap(),
        );
        reader.batches.store(0, Ordering::Relaxed);
        assert_eq!(
            lookup_active_unique_equality_batch(
                &reader,
                &handle,
                &[
                    PropertyValue::I64(7),
                    PropertyValue::F64(7.0),
                    PropertyValue::I64(8),
                    PropertyValue::I64(99),
                ]
            )
            .await
            .unwrap(),
            roaring::RoaringTreemap::from_iter([owner, owner + 1])
        );
        assert_eq!(reader.batches.load(Ordering::Relaxed), 1);
    }

    for fault in [
        ReadFault::MultiGet,
        ReadFault::ShortMultiGet,
        ReadFault::Get,
    ] {
        reader.fault = fault;
        let error = lookup_active_unique_equality_batch(&reader, &unique, &values)
            .await
            .unwrap_err();
        match fault {
            ReadFault::ShortMultiGet => {
                assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)))
            }
            ReadFault::MultiGet | ReadFault::Get => assert!(error.to_string().contains("injected")),
            ReadFault::None | ReadFault::PendingGet => {
                unreachable!("only injected faults are tested here")
            }
        }
    }
    reader.fault = ReadFault::None;

    // A valid first result must never escape as partial success when the next
    // owner or authoritative row is corrupt, missing, or no longer matches.
    let (key, row) = seed(&mut reader, &unique, PropertyValue::I64(8), 12);
    let valid_owner = reader.rows[&key].clone();
    for bytes in [
        Bytes::from_static(&[255]),
        encode_secondary_entry(&SecondaryEntryValue {
            index_id: unique.index_id(),
            generation: IndexGenerationId::new(2).unwrap(),
            lane: definition_lane(unique.secondary_definition().unwrap()),
            entity_id: IndexEntityId::new(12),
        }),
    ] {
        reader.rows.insert(key.clone(), bytes);
        assert!(
            lookup_active_unique_equality_batch(&reader, &unique, &values)
                .await
                .is_err()
        );
    }
    reader.rows.insert(key, valid_owner);
    for properties in [
        vec![
            Property::string("$label", "Other"),
            Property {
                name: "value".into(),
                value: PropertyValue::I64(8),
            },
        ],
        vec![Property::string("$label", "Fixture")],
        vec![
            Property::string("$label", "Fixture"),
            Property {
                name: "value".into(),
                value: PropertyValue::I64(9),
            },
        ],
    ] {
        reader
            .rows
            .insert(row.clone(), property::encode_properties(&properties));
        assert!(matches!(
            lookup_active_unique_equality_batch(&reader, &unique, &values).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
    reader.rows.insert(row.clone(), Bytes::from_static(&[255]));
    assert!(
        lookup_active_unique_equality_batch(&reader, &unique, &values)
            .await
            .is_err()
    );
    reader.rows.remove(&row);
    assert!(matches!(
        lookup_active_unique_equality_batch(&reader, &unique, &values).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}

/// Every byte limit through the successful high-water mark must either return
/// the exact owners or fail atomically and release all temporary reservations.
async fn admission_contracts() {
    use crate::query_resources as resources;
    let unique = handle(
        config::SecondaryIndexDefinition::node_unique_equality("Fixture", "value").unwrap(),
        DataScope::LegacyUnscoped,
        IndexGenerationId::initial(),
    );
    let values = [PropertyValue::I64(7), PropertyValue::I64(8)];
    let mut reader = Reader::default();
    seed(&mut reader, &unique, values[0].clone(), 11);
    seed(&mut reader, &unique, values[1].clone(), 12);
    let budget = resources::Budget::new(1024 * 1024);
    let owners =
        lookup_active_unique_equality_batch_admitted(&reader, &unique, &values, Some(&budget))
            .await
            .unwrap();
    let peak = budget.peak();
    assert_eq!(owners.iter().collect::<Vec<_>>(), [11, 12]);
    assert!(budget.available() < 1024 * 1024);
    let retained = budget.available();
    let mut cursor = owners.into_iter();
    assert_eq!(cursor.next(), Some(11));
    assert_eq!(
        budget.available(),
        retained,
        "consuming cursor retains admission"
    );
    drop(cursor);
    assert_eq!(budget.available(), 1024 * 1024);
    let reads = budget.reads();
    assert_eq!(reads.multi_get_batches, 1);
    assert_eq!(reads.multi_get_keys, 2);
    assert_eq!(reads.point_gets, 2);
    assert_eq!(reads.scans, 0);
    let mut boundaries = std::collections::BTreeSet::new();
    for limit in 0..=peak {
        reader.batches.store(0, Ordering::Relaxed);
        reader.gets.store(0, Ordering::Relaxed);
        let budget = resources::Budget::new(limit);
        match lookup_active_unique_equality_batch_admitted(&reader, &unique, &values, Some(&budget))
            .await
        {
            Ok(owners) => {
                assert_eq!(limit, peak, "successful high-water mark is the boundary");
                assert_eq!(owners.iter().collect::<Vec<_>>(), [11, 12]);
            }
            Err(error) => assert!(
                matches!(error, HelixDbError::QueryMemoryLimitExceeded),
                "{error}"
            ),
        }
        assert_eq!(
            budget.available(),
            limit,
            "all owners release at limit {limit}"
        );
        boundaries.insert((
            reader.batches.load(Ordering::Relaxed),
            reader.gets.load(Ordering::Relaxed),
        ));
    }
    assert!(
        boundaries.contains(&(0, 0)),
        "headers/keys are rejected before I/O"
    );
    assert!(
        boundaries.contains(&(1, 0)),
        "raw owner rows are admitted before verification"
    );
    assert!(
        boundaries.contains(&(1, 1)),
        "first decoded row/output can be rejected"
    );
    assert!(
        boundaries.contains(&(1, 2)),
        "later verification can be rejected atomically"
    );

    for fault in [
        ReadFault::MultiGet,
        ReadFault::ShortMultiGet,
        ReadFault::Get,
    ] {
        reader.fault = fault;
        let budget = resources::Budget::new(1024 * 1024);
        assert!(lookup_active_unique_equality_batch_admitted(
            &reader,
            &unique,
            &values,
            Some(&budget),
        )
        .await
        .is_err());
        assert_eq!(budget.available(), 1024 * 1024);
    }
    reader.fault = ReadFault::PendingGet;
    let budget = resources::Budget::new(1024 * 1024);
    let mut pending = Box::pin(lookup_active_unique_equality_batch_admitted(
        &reader,
        &unique,
        &values,
        Some(&budget),
    ));
    assert!(futures::poll!(&mut pending).is_pending());
    assert!(budget.available() < 1024 * 1024);
    drop(pending);
    assert_eq!(
        budget.available(),
        1024 * 1024,
        "cancellation releases pending owners"
    );
}

#[cfg(test)]
#[tokio::test]
async fn unique_batch_contracts_cover_faults_scope_and_admission() {
    run().await;
}
