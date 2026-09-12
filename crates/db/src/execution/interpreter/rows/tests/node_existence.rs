use super::super::*;
use crate::encoding::v2::keys;
use crate::query_resources::bitmap;
use futures::{FutureExt, TryStreamExt};
use helix_planner::context;

#[tokio::test]
async fn existence_batches_preserve_sparse_owner_order_tenants_and_missing_prefixes() {
    use keys::scope::{DataScope, TenantId};
    let db = crate::execution::interpreter::test_support::open_db("cypher-existence-batches").await;
    let owners = (0..1025_u64)
        .chain([u32::MAX as u64, u64::MAX])
        .collect::<Vec<_>>();
    let scopes = [
        DataScope::Tenant(TenantId::from_u128(1)),
        DataScope::Tenant(TenantId::from_u128(2)),
    ];
    for (index, scope) in scopes.into_iter().enumerate() {
        let context = ExecutionContext::new_scoped(&db, context::ParamBindings::default(), scope);
        for id in owners
            .iter()
            .copied()
            .filter(|id| (*id >= 512) == (index == 0))
        {
            // Existence does not decode properties. Malformed-but-present bytes
            // must stay visible here; property consumers report their own error.
            db.inner_db()
                .put(
                    context.storage_key(keys::DataKeyKind::NodeProperty(
                        keys::NodePropertyKey::new(id),
                    )),
                    bytes::Bytes::from_static(&[0xff]),
                )
                .await
                .unwrap();
        }
    }
    for (index, scope) in scopes.into_iter().enumerate() {
        let expected = owners
            .iter()
            .copied()
            .filter(|id| (*id >= 512) == (index == 0))
            .collect::<Vec<_>>();
        for batch_rows in [1, 17, 512, 2048] {
            let limits = Limits {
                batch_rows,
                memory_bytes: 1024 * 1024,
                ..Default::default()
            };
            let mut context =
                ExecutionContext::new_scoped(&db, context::ParamBindings::default(), scope);
            context.row_memory = Some(memory::Budget::new(limits.memory_bytes));
            context.enable_request_read_view().await.unwrap();
            let source = bitmap::Bitmap::retain_legacy(
                owners.iter().copied().collect(),
                Some(context.row_budget()),
            )
            .unwrap();
            let cursor = scan::NodeCursor::Indexed {
                ids: source.into_iter(),
                verify_existence: true,
            };
            let mut batches = Box::pin(context.node_id_batches(cursor, 3, r::Slot(1), limits));
            let mut actual = Vec::new();
            while let Some(batch) = batches.try_next().await.unwrap() {
                assert!(!batch.is_empty());
                assert!(batch.len() <= batch_rows.min(512));
                for row in batch {
                    let [r::Value::Null, r::Value::Entity(r::Entity::Node(id)), r::Value::Null] =
                        row.as_slice()
                    else {
                        panic!("slot-indexed node row")
                    };
                    actual.push(*id);
                }
            }
            assert_eq!(actual, expected);
            assert_eq!(
                context.row_budget().reads(),
                crate::cypher::StorageReadUsage {
                    multi_get_batches: owners.len().div_ceil(batch_rows.min(512)),
                    multi_get_keys: owners.len(),
                    ..Default::default()
                }
            );
            drop(batches);
            assert_eq!(context.row_budget().available(), limits.memory_bytes);
            context.close_request_read_view().unwrap();
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn existence_admission_precedes_key_allocation_and_storage_reads() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-existence-admission").await;
    let limits = Limits {
        batch_rows: 1,
        memory_bytes: 64 * 1024,
        ..Default::default()
    };
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    context.enable_request_read_view().await.unwrap();
    let source = bitmap::Bitmap::singleton(7, Some(context.row_budget())).unwrap();
    let cursor = scan::NodeCursor::Indexed {
        ids: source.into_iter(),
        verify_existence: true,
    };
    let held = context
        .row_budget()
        .reserve(context.row_budget().available() - size_of::<u64>())
        .unwrap();
    let (_, error_allocation) = crate::allocation_testing::observe(|| {
        Error::from(crate::HelixDbError::QueryMemoryLimitExceeded)
    });
    let (result, allocation) = crate::allocation_testing::observe(|| {
        cursor
            .next_batch(&context, 1, r::Slot(0), limits)
            .now_or_never()
            .expect("admission fails before polling storage")
    });
    assert!(matches!(result, Err(Error::Query(ref error)) if error.detail == "MemoryLimit"));
    // Apart from the structured error, only the one-ID buffer may allocate.
    assert_eq!(allocation.allocations, error_allocation.allocations + 1);
    assert_eq!(allocation.bytes, error_allocation.bytes + size_of::<u64>());
    assert_eq!(context.row_budget().reads(), Default::default());
    drop(held);
    assert_eq!(context.row_budget().available(), limits.memory_bytes);
    for poll in [false, true] {
        let source = bitmap::Bitmap::singleton(7, Some(context.row_budget())).unwrap();
        let cursor = scan::NodeCursor::Indexed {
            ids: source.into_iter(),
            verify_existence: true,
        };
        context.fail_deadline_after(0);
        let future = cursor.next_batch(&context, 1, r::Slot(0), limits);
        if poll {
            assert!(matches!(
                future.await,
                Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
            ));
        } else {
            drop(future);
        }
        assert_eq!(context.row_budget().reads(), Default::default());
        assert_eq!(context.row_budget().available(), limits.memory_bytes);
    }
    context.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn existence_reads_use_the_request_snapshot_and_active_write_transaction() {
    use crate::encoding::v2::values::property;
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-existence-snapshot").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context.row_memory = Some(memory::Budget::new(1024 * 1024));
    context.enable_request_read_view().await.unwrap();
    db.inner_db()
        .put(
            context.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                7,
            ))),
            property::encode_properties(&[property::Property::string("$label", "N")]),
        )
        .await
        .unwrap();
    for visible in [false, true] {
        let source = bitmap::Bitmap::singleton(7, Some(context.row_budget())).unwrap();
        let cursor = scan::NodeCursor::Indexed {
            ids: source.into_iter(),
            verify_existence: true,
        };
        let result = context
            .node_id_batches(cursor, 1, r::Slot(0), Limits::default())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            result.iter().map(|batch| batch.len()).sum::<usize>(),
            usize::from(visible)
        );
        drop(result);
        assert_eq!(context.row_budget().available(), 1024 * 1024);
        context.close_request_read_view().unwrap();
        if !visible {
            context.enable_request_read_view().await.unwrap();
        }
    }
    context.enable_request_write_scope().await.unwrap();
    let id = context.row_create_node("Created", vec![]).await.unwrap();
    let source = bitmap::Bitmap::singleton(id, Some(context.row_budget())).unwrap();
    let cursor = scan::NodeCursor::Indexed {
        ids: source.into_iter(),
        verify_existence: true,
    };
    let result = context
        .node_id_batches(cursor, 1, r::Slot(0), Limits::default())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(result[0][0], vec![r::Value::Entity(r::Entity::Node(id))]);
    drop(result);
    context.abort_request_write_scope();
    let response = db.cypher(crate::cypher::Request::new(
        "CREATE (n:Written {key:1}) WITH n MATCH (again:Written) WHERE n=again RETURN again.key AS key"
    )).await.unwrap();
    assert_eq!(response.rows, vec![vec![serde_json::json!(1)]]);
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Created) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![serde_json::json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn limited_products_and_hash_joins_keep_source_build_reads_batched() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-source-build-width").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,1027) AS key CREATE (:Large {key:key}) WITH key WHERE key<=17 CREATE (:Small {key:key})"
    )).await.unwrap();
    for (query, max_batches, max_keys) in [
        ("MATCH (a:Large),(b:Small) RETURN 1 AS found LIMIT 1", 3, 4),
        (
            "MATCH (a:Large),(b:Small) WHERE a.key=b.key RETURN 1 AS found LIMIT 1",
            10,
            // WHERE retains candidate validation for observable errors. The
            // entire 17-row probe side may be checked before projection limits.
            2 * 1027 + 4 * 17,
        ),
    ] {
        let response = db.cypher(crate::cypher::Request::new(query)).await.unwrap();
        assert_eq!(response.rows, vec![vec![serde_json::json!(1)]]);
        assert_eq!(response.resources.reads.point_gets, 2);
        assert_eq!(response.resources.reads.scans, 0);
        assert!(
            response.resources.reads.multi_get_batches <= max_batches,
            "{query}: {:?}",
            response.resources.reads
        );
        assert!(
            response.resources.reads.multi_get_keys <= max_keys,
            "{query}: {:?}",
            response.resources.reads
        );
    }
    db.close().await.unwrap();
}
