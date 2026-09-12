use super::*;
use crate::encoding::v2::values::property::encode_properties;
use crate::index_lifecycle::secondary::tests as fixture;

#[tokio::test]
async fn runtime_domains_fold_keys_and_retain_admission_through_iteration() {
    for definition in [
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        crate::config::SecondaryIndexDefinition::edge_equality("KNOWS", "value").unwrap(),
    ] {
        let db = fixture::test_db("dynamic-equality-admission").await;
        let handle = fixture::active_read_handle(&db, definition).await;
        for (value, id) in [("a", 2), ("a", 9), ("b", 4)] {
            fixture::put_read_entry(&db, &handle, value, id).await;
        }
        for (names, expected, points, batches, keys) in [
            (vec![], vec![], 0, 0, 0),
            (vec!["a", "a"], vec![2, 9], 1, 0, 0),
            (vec!["missing"], vec![], 1, 0, 0),
            (vec!["a", "b", "a"], vec![2, 4, 9], 0, 1, 2),
        ] {
            let values = names
                .into_iter()
                .map(|name| PropertyValue::String(name.into()))
                .collect::<Vec<_>>();
            let budget = query_resources::Budget::new(1024 * 1024);
            let ids = lookup_active_equality_generations_admitted(
                &db,
                &handle,
                &values,
                ReaderStorageCompatibility::Current,
                Some(&budget),
            )
            .await
            .unwrap();
            let retained = budget.available();
            assert!(retained < 1024 * 1024);
            assert_eq!(
                budget.reads(),
                query_resources::StorageReadUsage {
                    point_gets: points,
                    multi_get_batches: batches,
                    multi_get_keys: keys,
                    ..Default::default()
                }
            );
            let mut iter = ids.into_iter();
            assert_eq!(iter.next(), expected.first().copied());
            assert_eq!(budget.available(), retained);
            assert_eq!(
                iter.by_ref().collect::<Vec<_>>(),
                expected.into_iter().skip(1).collect::<Vec<_>>()
            );
            drop(iter);
            assert_eq!(budget.available(), 1024 * 1024);
        }
        let values = [PropertyValue::String("x".repeat(65536))];
        let budget = query_resources::Budget::new(1024);
        let mut future = std::pin::pin!(lookup_active_equality_generations_admitted(
            &db,
            &handle,
            &values,
            ReaderStorageCompatibility::Current,
            Some(&budget)
        ));
        let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
        let (result, allocation) = crate::allocation_testing::observe(|| {
            std::future::Future::poll(future.as_mut(), &mut context)
        });
        assert!(matches!(
            result,
            std::task::Poll::Ready(Err(HelixDbError::QueryMemoryLimitExceeded))
        ));
        assert!(allocation.bytes <= size_of::<Bytes>());
        assert_eq!(budget.reads(), query_resources::StorageReadUsage::default());
        assert_eq!(budget.available(), 1024);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn runtime_unique_hits_verify_graph_and_native_null_preserves_missing_values() {
    for definition in [
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        crate::config::SecondaryIndexDefinition::node_unique_equality("User", "value").unwrap(),
        crate::config::SecondaryIndexDefinition::edge_equality("KNOWS", "value").unwrap(),
    ] {
        let db = fixture::test_db("dynamic-equality-authority").await;
        let handle = fixture::active_read_handle(&db, definition).await;
        fixture::put_read_entry(&db, &handle, "a", 9).await;
        let definition = handle.secondary_definition().unwrap();
        for (id, properties) in [
            (
                1,
                vec![Property::string("$label", definition.label().as_str())],
            ),
            (
                2,
                vec![
                    Property::string("$label", definition.label().as_str()),
                    Property::new("value", PropertyValue::Null),
                ],
            ),
            (3, vec![Property::string("$label", "Unrelated")]),
        ] {
            db.put(
                authoritative_property_key(
                    handle.scope(),
                    IndexEntity {
                        kind: definition.element_kind(),
                        id: IndexEntityId::new(id),
                    },
                ),
                encode_properties(&properties),
            )
            .await
            .unwrap();
        }
        for compatibility in [
            ReaderStorageCompatibility::Current,
            ReaderStorageCompatibility::LegacyEqualityUnion,
        ] {
            let budget = query_resources::Budget::new(1024 * 1024);
            let values = [
                PropertyValue::Null,
                PropertyValue::F64(f64::NAN),
                PropertyValue::String("a".into()),
            ];
            let ids = lookup_active_equality_generations_admitted(
                &db,
                &handle,
                &values,
                compatibility,
                Some(&budget),
            )
            .await
            .unwrap();
            assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![1, 2, 9]);
            assert_eq!(budget.available(), 1024 * 1024);
            assert_eq!(budget.reads().scan_rows, 4);
            assert_eq!(
                budget.reads().point_gets,
                if definition_lane(definition).is_unique() {
                    2
                } else {
                    1
                }
            );
            for value in [PropertyValue::Object(Default::default()), PropertyValue::String("x".repeat(crate::encoding::v2::values::property::equality_index_value::MAX_EQUALITY_CANONICAL_LEN))] {
                assert!(lookup_active_equality_generations_admitted(&db, &handle, &[value], compatibility, Some(&budget)).await.is_err());
                assert_eq!(budget.available(), 1024 * 1024);
            }
        }
        if definition_lane(definition).is_unique() {
            for properties in [
                vec![
                    Property::string("$label", "Wrong"),
                    Property::string("value", "a"),
                ],
                vec![
                    Property::string("$label", definition.label().as_str()),
                    Property::string("value", "wrong"),
                ],
                vec![Property::string("$label", definition.label().as_str())],
            ] {
                db.put(
                    authoritative_property_key(
                        handle.scope(),
                        IndexEntity {
                            kind: definition.element_kind(),
                            id: IndexEntityId::new(9),
                        },
                    ),
                    encode_properties(&properties),
                )
                .await
                .unwrap();
                let budget = query_resources::Budget::new(1024 * 1024);
                assert!(matches!(
                    lookup_active_equality_generations_admitted(
                        &db,
                        &handle,
                        &[PropertyValue::String("a".into())],
                        ReaderStorageCompatibility::Current,
                        Some(&budget)
                    )
                    .await,
                    Err(HelixDbError::IndexCatalogCorruption(_))
                ));
                assert_eq!(budget.available(), 1024 * 1024);
            }
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn folded_numeric_keys_and_null_scans_stay_inside_the_authorized_tenant() {
    let db = fixture::test_db("dynamic-equality-tenants").await;
    let template = fixture::active_read_handle(
        &db,
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
    )
    .await;
    let mut handles = Vec::new();
    for (scope, first) in [
        (DataScope::LegacyUnscoped, 100),
        (
            DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(1)),
            200,
        ),
        (
            DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(2)),
            300,
        ),
    ] {
        let mut handle = template.clone();
        let ActiveIndexHandle::Secondary {
            scope: handle_scope,
            ..
        } = &mut handle
        else {
            unreachable!()
        };
        *handle_scope = scope;
        let definition = handle.secondary_definition().unwrap();
        let EqualityValueProjection::Indexed(value) =
            project_equality_value(&PropertyValue::I64(7))
        else {
            unreachable!()
        };
        db.put(
            secondary_entry_key(
                scope,
                handle.index_id(),
                handle.generation(),
                definition,
                CanonicalSecondaryValue::equality(value),
                IndexEntityId::initial(),
            )
            .unwrap(),
            SecondaryEqualityBitmapValue::new([first].into_iter().collect()).encode(),
        )
        .await
        .unwrap();
        for (id, properties) in [
            (
                first,
                vec![
                    Property::string("$label", "User"),
                    Property::new("value", PropertyValue::I64(7)),
                ],
            ),
            (first + 1, vec![Property::string("$label", "User")]),
        ] {
            db.put(
                authoritative_property_key(
                    scope,
                    IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(id),
                    },
                ),
                encode_properties(&properties),
            )
            .await
            .unwrap();
        }
        handles.push((handle, first));
    }
    for (handle, first) in handles {
        for (values, expected, reads) in [
            (
                vec![PropertyValue::I64(7), PropertyValue::F64(7.0)],
                vec![first],
                query_resources::StorageReadUsage {
                    point_gets: 1,
                    ..Default::default()
                },
            ),
            (
                vec![PropertyValue::Null, PropertyValue::F64(7.0)],
                vec![first, first + 1],
                query_resources::StorageReadUsage {
                    point_gets: 1,
                    scans: 1,
                    scan_rows: 2,
                    ..Default::default()
                },
            ),
        ] {
            let budget = query_resources::Budget::new(64 * 1024);
            let ids = lookup_active_equality_generations_admitted(
                &db,
                &handle,
                &values,
                ReaderStorageCompatibility::Current,
                Some(&budget),
            )
            .await
            .unwrap();
            assert_eq!(ids.into_iter().collect::<Vec<_>>(), expected);
            assert_eq!(budget.reads(), reads);
            assert_eq!(budget.available(), 64 * 1024);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn runtime_bitmap_decode_union_and_property_failures_release_all_admission() {
    let db = fixture::test_db("dynamic-equality-failures").await;
    let handle = fixture::active_read_handle(
        &db,
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
    )
    .await;
    let definition = handle.secondary_definition().unwrap();
    for (value, ids) in [
        ("a", (0..20_000).collect()),
        ("b", ((1_u64 << 32)..(1_u64 << 32) + 20_000).collect()),
    ] {
        db.put(
            secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                definition,
                CanonicalSecondaryValue::equality_string(value),
                IndexEntityId::initial(),
            )
            .unwrap(),
            SecondaryEqualityBitmapValue::new(ids).encode(),
        )
        .await
        .unwrap();
    }
    for limit in [9000, 32 * 1024] {
        let budget = query_resources::Budget::new(limit);
        assert!(matches!(
            lookup_active_equality_generations_admitted(
                &db,
                &handle,
                &[
                    PropertyValue::String("a".into()),
                    PropertyValue::String("b".into())
                ],
                ReaderStorageCompatibility::Current,
                Some(&budget)
            )
            .await,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(budget.reads().multi_get_batches, 1);
        assert_eq!(budget.reads().multi_get_keys, 2);
        assert_eq!(budget.available(), limit);
    }
    let budget = query_resources::Budget::new(1024 * 1024);
    let ids = lookup_active_equality_generations_admitted(
        &db,
        &handle,
        &[
            PropertyValue::String("a".into()),
            PropertyValue::String("b".into()),
        ],
        ReaderStorageCompatibility::Current,
        Some(&budget),
    )
    .await
    .unwrap();
    assert_eq!(ids.len(), 40_000);
    drop(ids);
    assert_eq!(budget.available(), 1024 * 1024);
    db.put(
        secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::equality_string("bad"),
            IndexEntityId::initial(),
        )
        .unwrap(),
        Bytes::from_static(b"invalid bitmap"),
    )
    .await
    .unwrap();
    for values in [
        vec![PropertyValue::String("bad".into())],
        vec![
            PropertyValue::String("a".into()),
            PropertyValue::String("bad".into()),
        ],
    ] {
        assert!(lookup_active_equality_generations_admitted(
            &db,
            &handle,
            &values,
            ReaderStorageCompatibility::Current,
            Some(&budget)
        )
        .await
        .is_err());
        assert_eq!(budget.available(), 1024 * 1024);
    }
    let property_key = authoritative_property_key(
        handle.scope(),
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        },
    );
    db.put(
        &property_key,
        encode_properties(&[
            Property::string("$label", "User"),
            Property::string("payload", "x".repeat(65536)),
        ]),
    )
    .await
    .unwrap();
    let small = query_resources::Budget::new(8192);
    assert!(matches!(
        lookup_active_equality_generations_admitted(
            &db,
            &handle,
            &[PropertyValue::Null],
            ReaderStorageCompatibility::Current,
            Some(&small)
        )
        .await,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(small.reads().scan_rows, 1);
    assert_eq!(small.available(), 8192);
    db.put(&property_key, Bytes::from_static(b"invalid properties"))
        .await
        .unwrap();
    assert!(lookup_active_equality_generations_admitted(
        &db,
        &handle,
        &[PropertyValue::Null],
        ReaderStorageCompatibility::Current,
        Some(&budget)
    )
    .await
    .is_err());
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}

#[derive(Clone, Copy)]
enum InterruptedRead {
    Point,
    Batch,
    Scan,
}
struct InterruptedReader<'a> {
    db: &'a slatedb::Db,
    operation: InterruptedRead,
    fail: bool,
}
#[async_trait::async_trait]
impl slatedb::DbReadOps for InterruptedReader<'_> {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        if matches!(self.operation, InterruptedRead::Point) {
            return if self.fail {
                Err(slatedb::Error::unavailable("injected point failure".into()))
            } else {
                std::future::pending().await
            };
        }
        self.db.get_with_options(key, options).await
    }
    async fn multi_get_with_options<K: AsRef<[u8]> + Send + Sync>(
        &self,
        keys: &[K],
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error> {
        if matches!(self.operation, InterruptedRead::Batch) {
            return if self.fail {
                Err(slatedb::Error::unavailable("injected batch failure".into()))
            } else {
                std::future::pending().await
            };
        }
        self.db.multi_get_with_options(keys, options).await
    }
    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        self.db.get_key_value_with_options(key, options).await
    }
    async fn scan_with_options<T: slatedb::ByteRangeBounds + Send>(
        &self,
        range: T,
        options: &slatedb::config::ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error> {
        if matches!(self.operation, InterruptedRead::Scan) {
            return if self.fail {
                Err(slatedb::Error::unavailable("injected scan failure".into()))
            } else {
                std::future::pending().await
            };
        }
        self.db.scan_with_options(range, options).await
    }
}

#[tokio::test]
async fn pending_and_failed_storage_reads_release_prepared_keys_and_domain_state() {
    let db = fixture::test_db("dynamic-equality-interrupted").await;
    let handle = fixture::active_read_handle(
        &db,
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
    )
    .await;
    for (operation, values) in [
        (
            InterruptedRead::Point,
            vec![PropertyValue::String("a".repeat(65536))],
        ),
        (
            InterruptedRead::Batch,
            vec![
                PropertyValue::String("a".repeat(65536)),
                PropertyValue::String("b".repeat(65536)),
            ],
        ),
        (InterruptedRead::Scan, vec![PropertyValue::Null]),
    ] {
        for fail in [false, true] {
            let reader = InterruptedReader {
                db: &db,
                operation,
                fail,
            };
            let budget = query_resources::Budget::new(1024 * 1024);
            let mut future = Box::pin(lookup_active_equality_generations_admitted(
                &reader,
                &handle,
                &values,
                ReaderStorageCompatibility::Current,
                Some(&budget),
            ));
            let polled = futures::poll!(future.as_mut());
            if fail {
                assert!(matches!(polled, std::task::Poll::Ready(Err(_))));
                assert_eq!(budget.available(), 1024 * 1024);
            } else {
                assert!(polled.is_pending());
                assert!(budget.available() < 1024 * 1024);
            }
            drop(future);
            assert_eq!(budget.available(), 1024 * 1024);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_cached_null_scan_yields_for_request_cancellation_and_releases_its_state() {
    let db = fixture::test_db("dynamic-null-cooperative-cancel").await;
    let handle = fixture::active_read_handle(
        &db,
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
    )
    .await;
    let properties = encode_properties(&[Property::string("$label", "User")]);
    let transaction = db
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    for id in 0..4096 {
        transaction
            .put(
                authoritative_property_key(
                    handle.scope(),
                    IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(id),
                    },
                ),
                properties.clone(),
            )
            .unwrap();
    }
    transaction.commit().await.unwrap();
    let cancellation = crate::execution_control::ReaderRetirementCancellation::new();
    let control = crate::execution_control::ExecutionControl::unlimited()
        .with_reader_retirement_cancellation(cancellation.clone());
    let budget = query_resources::Budget::new(64 * 1024);
    let values = [PropertyValue::Null];
    let mut read = Box::pin(control.run(lookup_active_equality_generations_admitted(
        &db,
        &handle,
        &values,
        ReaderStorageCompatibility::Current,
        Some(&budget),
    )));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            assert!(
                futures::poll!(read.as_mut()).is_pending(),
                "the scan must yield before processing the entire graph"
            );
            if budget.reads().scan_rows >= 512 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(budget.reads().scan_rows, 512);
    assert!(budget.available() < 64 * 1024);
    cancellation.cancel();
    assert!(matches!(
        read.await,
        Err(HelixDbError::QueryCancelledByReaderRetirement)
    ));
    assert_eq!(budget.reads().scan_rows, 512);
    assert_eq!(budget.available(), 64 * 1024);
    db.close().await.unwrap();
}

#[tokio::test]
async fn invalid_serving_handles_fail_before_reading_and_release_domain_admission() {
    let db = fixture::test_db("dynamic-equality-invalid-handles").await;
    let handles = [
        fixture::active_vector_read_handle(&db).await,
        fixture::active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_range("User", "value").unwrap(),
        )
        .await,
    ];
    let value = PropertyValue::I64(7);
    for handle in handles {
        let budget = query_resources::Budget::new(32 * 1024);
        assert!(matches!(
            lookup_active_equality_generation_admitted(
                &db,
                &handle,
                &value,
                ReaderStorageCompatibility::Current,
                Some(&budget)
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(budget.available(), 32 * 1024);
        assert!(matches!(
            lookup_active_equality_generations_admitted(
                &db,
                &handle,
                std::slice::from_ref(&value),
                ReaderStorageCompatibility::Current,
                Some(&budget)
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(budget.available(), 32 * 1024);
        assert_eq!(budget.reads(), query_resources::StorageReadUsage::default());
    }
    db.close().await.unwrap();
}
