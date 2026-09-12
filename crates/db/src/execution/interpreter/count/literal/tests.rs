use super::*;
use crate::{allocation_testing, HelixDbError};
use helix_ast::value::PropertyValue as P;
use helix_planner::ir;

fn indexed(value: P) -> exec::ExecIndexedEqualityValue {
    ir::SecondaryIndexLiteral::new(value)
        .unwrap()
        .try_into()
        .unwrap()
}

#[test]
fn index_identity_validation_borrows_large_unicode_names_without_allocation() {
    use helix_planner::catalog;
    let key =
        catalog::ScopedPropertyKey::try_new("Graph:名前".repeat(1024), "属性:key".repeat(1024))
            .unwrap();
    for prefix in ["node_eq:", "edge_eq:"] {
        let identity = ir::NonEmptyString::from_prefixed_display(prefix, &key);
        let (result, allocation) = allocation_testing::observe(|| {
            super::super::validate_index_id(prefix, &identity, &key)
        });
        result.unwrap();
        assert_eq!(allocation.allocations, 0);
        for invalid in [
            "wrong".to_owned(),
            format!("{prefix}{}", key.label),
            format!("{identity}:extra"),
        ] {
            let invalid = ir::NonEmptyString::new(invalid).unwrap();
            assert!(matches!(
                super::super::validate_index_id(prefix, &invalid, &key),
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }
    }
    for direction in [
        helix_ast::index::RangeIndexDirection::Asc,
        helix_ast::index::RangeIndexDirection::Desc,
    ] {
        let key = catalog::ScopedPropertyDirectionKey::new(
            key.label.clone(),
            key.property.clone(),
            direction,
        );
        for prefix in ["node_range:", "edge_range:"] {
            let identity = ir::NonEmptyString::from_prefixed_display(prefix, &key);
            let (result, allocation) = allocation_testing::observe(|| {
                super::super::validate_index_id(prefix, &identity, &key)
            });
            result.unwrap();
            assert_eq!(allocation.allocations, 0);
        }
    }
}

#[tokio::test]
async fn node_and_edge_bitmap_admission_precedes_boxing_copying_and_storage() {
    use crate::execution::interpreter::{test_support, ExecutionContext};
    use futures::FutureExt;
    use helix_planner::{catalog, context};
    let db = test_support::open_db_with_config(test_support::in_memory_config(
        "bitmap-future-admission",
    ))
    .await;
    let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
    execution.row_memory = Some(query_resources::Budget::new(1));
    execution.enable_request_read_view().await.unwrap();
    let node = exec::ExecNodeBitmapExpr::PointRead {
        index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:A:key"))
            .try_into()
            .unwrap(),
        key: catalog::ScopedPropertyKey::try_new("A", "key").unwrap(),
        value: indexed(P::String("value".repeat(8192))),
    };
    let edge = exec::ExecEdgeBitmapExpr::PointRead {
        index: exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
            test_support::name("edge_eq:R:key"),
        )),
        key: catalog::ScopedPropertyKey::try_new("R", "key").unwrap(),
        value: indexed(P::String("value".repeat(8192))),
    };
    let (futures, allocation) = allocation_testing::observe(|| {
        [execution.node_bitmap(&node), execution.edge_bitmap(&edge)]
    });
    assert_eq!(allocation.allocations, 0);
    for future in futures {
        let (result, allocation) = allocation_testing::observe(|| {
            future
                .now_or_never()
                .expect("rejection precedes storage awaits")
        });
        assert!(matches!(
            result,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(allocation.allocations, 0);
    }
    let budget = execution.row_memory.as_ref().unwrap();
    assert_eq!(budget.available(), 1);
    assert_eq!(budget.reads(), query_resources::StorageReadUsage::default());
    drop(execution);
    db.close().await.unwrap();
}

#[test]
fn literal_copy_admission_covers_all_flat_types_and_spare_source_capacity() {
    let mut text = String::with_capacity(8192);
    text.push_str("héllo");
    let mut bytes = Vec::with_capacity(8192);
    bytes.extend([0, 1, 255]);
    let mut integers = Vec::with_capacity(8192);
    integers.extend([i64::MIN, i64::MAX]);
    let mut doubles = Vec::with_capacity(8192);
    doubles.extend([f64::NEG_INFINITY, -0.0, f64::INFINITY]);
    let mut floats = Vec::with_capacity(8192);
    floats.extend([f32::NEG_INFINITY, -0.0, f32::INFINITY]);
    let mut strings = Vec::with_capacity(8192);
    strings.push(text.clone());
    strings[0].reserve(8192);
    strings.push(String::with_capacity(8192));
    let values = [
        P::Bool(false),
        P::I64(i64::MIN),
        P::DateTime(i64::MAX),
        P::F64(f64::INFINITY),
        P::F32(f32::NEG_INFINITY),
        P::String(text),
        P::Bytes(bytes),
        P::I64Array(integers),
        P::F64Array(doubles),
        P::F32Array(floats),
        P::StringArray(strings),
        P::String(String::with_capacity(8192)),
        P::Bytes(Vec::with_capacity(8192)),
        P::I64Array(Vec::with_capacity(8192)),
        P::F64Array(Vec::with_capacity(8192)),
        P::F32Array(Vec::with_capacity(8192)),
        P::StringArray(Vec::with_capacity(8192)),
    ]
    .map(indexed);
    for value in &values {
        let expected = stream::ast_to_db_value(value.literal().as_property_value().clone());
        let (copy, allocation) = allocation_testing::observe(|| Value::new(value, None));
        assert_eq!(*copy.unwrap(), expected);
        // Compare the estimate to independent allocation requests, including
        // zero-length owners whose original buffers have spare capacity.
        assert_eq!(copied_bytes(value), allocation.bytes);
        let budget = query_resources::Budget::new(allocation.bytes);
        let (copy, admitted_allocation) =
            allocation_testing::observe(|| Value::new(value, Some(&budget)));
        let copy = copy.unwrap();
        assert_eq!(*copy, expected);
        assert_eq!(admitted_allocation.bytes, allocation.bytes);
        assert_eq!(budget.available(), 0);
        drop(copy);
        assert_eq!(budget.available(), allocation.bytes);
        if allocation.bytes == 0 {
            continue;
        }
        let small = query_resources::Budget::new(allocation.bytes - 1);
        let (denied, denied_allocation) =
            allocation_testing::observe(|| Value::new(value, Some(&small)));
        assert!(matches!(
            denied,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(denied_allocation.allocations, 0);
        assert_eq!(small.available(), allocation.bytes - 1);
    }
    let expected = values
        .iter()
        .map(|value| stream::ast_to_db_value(value.literal().as_property_value().clone()))
        .collect::<Vec<_>>();
    let (batch, allocation) = allocation_testing::observe(|| Batch::new(&values, None));
    assert_eq!(&*batch.unwrap(), expected.as_slice());
    let budget = query_resources::Budget::new(allocation.bytes);
    let (batch, admitted_allocation) =
        allocation_testing::observe(|| Batch::new(&values, Some(&budget)));
    let batch = batch.unwrap();
    assert_eq!(&*batch, expected.as_slice());
    assert_eq!(admitted_allocation.bytes, allocation.bytes);
    assert_eq!(budget.available(), 0);
    // The borrowed batch remains admitted in an unpolled async continuation.
    let pending = async move {
        std::future::pending::<()>().await;
        std::hint::black_box(batch);
    };
    drop(pending);
    assert_eq!(budget.available(), allocation.bytes);
    let small = query_resources::Budget::new(allocation.bytes - 1);
    let (denied, denied_allocation) =
        allocation_testing::observe(|| Batch::new(&values, Some(&small)));
    assert!(matches!(
        denied,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(denied_allocation.allocations, 0);
    assert_eq!(small.available(), allocation.bytes - 1);
    let empty = query_resources::Budget::new(0);
    let (batch, allocation) = allocation_testing::observe(|| Batch::new(&[], Some(&empty)));
    assert!(batch.unwrap().is_empty());
    assert_eq!(allocation.allocations, 0);
}
