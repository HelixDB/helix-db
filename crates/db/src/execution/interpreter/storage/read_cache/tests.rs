use super::*;
use crate::encoding::v2::keys;
use crate::execution::interpreter::{test_support, ExecutionContext};
use helix_planner::context;

#[test]
fn reuse_copies_small_values_without_pinning_storage_blocks() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Block {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Block {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Block {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let block = Bytes::from_owner(Block {
        bytes: vec![4; 1024 * 1024],
        dropped: dropped.clone(),
    });
    let slice = block.slice(..8);
    drop(block);
    let budget = Budget::new(1024 * 1024);
    let cache = Cache::new(&budget, 128 * 1024).unwrap();
    cache.insert(b"small", &Some(slice));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(cache.get(b"small").unwrap().unwrap().as_ref(), &[4; 8]);
    assert!(budget.peak() < 4096);
    drop(cache);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn reuse_copy_admission_precedes_buffer_allocation() {
    let bytes = [9; 4096];
    let budget = Budget::new(1);
    let (result, observed) = crate::allocation_testing::observe(|| budget.copy_read(&bytes));
    assert!(result.is_err());
    assert_eq!(observed.allocations, 0);
    let budget = Budget::new(16 * 1024);
    let (result, observed) =
        crate::allocation_testing::observe(|| budget.copy_read(&bytes).unwrap());
    assert_eq!(result.as_ref(), bytes);
    assert!(observed.peak_bytes <= budget.peak());
    drop(result);
    assert_eq!(budget.available(), 16 * 1024);
}

#[test]
fn reuse_distinguishes_absence_and_preserves_first_snapshot_value() {
    let budget = Budget::new(1024 * 1024);
    assert!(Cache::new(&budget, 100).is_none());
    assert!(Cache::new(&Budget::new(0), 128 * 1024).is_none());
    let cache = Cache::new(&budget, 128 * 1024).unwrap();
    assert!(cache.get(b"missing").is_none());
    cache.insert(b"missing", &None);
    cache.insert(b"empty", &Some(budget.retain_read(Bytes::new()).unwrap()));
    cache.insert(
        b"value",
        &Some(budget.retain_read(Bytes::from_static(b"first")).unwrap()),
    );
    cache.insert(b"value", &Some(Bytes::from_static(b"second")));
    assert_eq!(cache.get(b"missing"), Some(None));
    assert_eq!(cache.get(b"empty"), Some(Some(Bytes::new())));
    assert_eq!(
        cache.get(b"value"),
        Some(Some(Bytes::from_static(b"first")))
    );
    let (_, observed) = crate::allocation_testing::observe(|| {
        for _ in 0..10_000 {
            std::hint::black_box(cache.get(b"value"));
        }
    });
    assert_eq!(observed.allocations, 0);
    drop(cache);
    assert_eq!(budget.available(), 1024 * 1024);
    assert!(budget.reserve(1024 * 1024 + 1).is_err());
}

#[test]
fn reuse_fifo_and_allocator_peaks_stay_inside_admitted_bounds() {
    let budget = Budget::new(1024 * 1024);
    let (_, observed) = crate::allocation_testing::observe(|| {
        let cache = Cache::new(&budget, 128 * 1024).unwrap();
        for key in 0_u64..10_000 {
            cache.insert(&key.to_be_bytes(), &None);
            assert_eq!(cache.get(&key.to_be_bytes()), Some(None));
        }
        assert!(cache.get(&0_u64.to_be_bytes()).is_none());
        assert!(budget.peak() <= 128 * 1024 + size_of::<Cache>() + 16);
        let state = cache.state.lock();
        assert_eq!(state.entries.len(), state.order.len());
        assert!(state
            .order
            .iter()
            .all(|key| state.entries.contains_key(key)));
    });
    assert!(
        observed.peak_bytes <= budget.peak(),
        "actual {}, admitted {}",
        observed.peak_bytes,
        budget.peak()
    );
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn reuse_pressure_reclaims_optional_owners_and_avoids_a_budget_cycle() {
    let limit = 1024 * 1024;
    let budget = Budget::new(limit);
    let cache = Cache::new(&budget, 128 * 1024).unwrap();
    let weak = Arc::downgrade(&cache);
    let header = limit - budget.available();
    let value = Some(budget.retain_read(Bytes::from(vec![7; 16 * 1024])).unwrap());
    cache.insert(b"value", &value);
    drop(value);
    let memory = budget.reserve(limit - header - 1).unwrap();
    assert_eq!(budget.available(), 1);
    assert!(cache.get(b"value").is_none());
    // Insertion fails while its own lock is held. Reclaim must not deadlock.
    cache.insert(b"cannot-admit", &None);
    assert!(cache.get(b"cannot-admit").is_none());
    drop(memory);
    drop(cache);
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.available(), limit);
    assert!(Cache::new(&budget, 128 * 1024).is_some());
}

#[test]
fn reuse_eviction_keeps_outstanding_values_charged_and_skips_oversized_values() {
    let limit = 1024 * 1024;
    let budget = Budget::new(limit);
    let cache = Cache::new(&budget, 128 * 1024).unwrap();
    let header = limit - budget.available();
    cache.insert(
        b"value",
        &Some(budget.retain_read(Bytes::from(vec![9; 4096])).unwrap()),
    );
    let value = cache.get(b"value").unwrap().unwrap();
    assert!(budget.reserve(limit - header).is_err());
    assert!(cache.get(b"value").is_none());
    assert_eq!(value.as_ref(), &[9; 4096]);
    drop(value);
    let memory = budget.reserve(limit - header).unwrap();
    drop(memory);
    cache.insert(
        b"oversized",
        &Some(budget.retain_read(Bytes::from(vec![1; 64 * 1024])).unwrap()),
    );
    assert!(cache.get(b"oversized").is_none());
    drop(cache);
    assert_eq!(budget.available(), limit);
}

#[tokio::test]
async fn reuse_raw_reads_preserve_order_duplicates_absence_and_cancellation() {
    let db = test_support::open_db("read-reuse-contract").await;
    let a = test_support::add_user(&db, "a").await;
    let b = test_support::add_user(&db, "b").await;
    let budget = Budget::new(2 * 1024 * 1024);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(budget.clone());
    ctx.enable_request_read_view().await.unwrap();
    ctx.enable_request_read_cache(256 * 1024);
    let key = |id| {
        ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            id,
        )))
    };
    let (a, b, missing) = (key(a), key(b), key(u64::MAX));
    let first = ctx.get_raw(&a).await.unwrap();
    assert!(first.is_some());
    let request = [&a, &b, &missing, &b];
    let values = ctx.multi_get_raw(&request).await.unwrap();
    assert_eq!(values[0], first);
    assert!(values[1].is_some());
    assert_eq!(values[1], values[3]);
    assert!(values[2].is_none());
    assert_eq!(budget.reads().point_gets, 1);
    assert_eq!(budget.reads().multi_get_keys, 3);
    let before = budget.reads();
    assert_eq!(ctx.multi_get_raw(&request).await.unwrap(), values);
    assert!(ctx.multi_get_raw::<Bytes>(&[]).await.unwrap().is_empty());
    assert_eq!(ctx.get_raw(&missing).await.unwrap(), None);
    assert_eq!(budget.reads().multi_get_keys, before.multi_get_keys);
    assert_eq!(budget.reads().point_gets, before.point_gets);
    ctx.execution_control =
        crate::execution_control::ExecutionControl::from_timeout(std::time::Duration::ZERO);
    assert!(ctx.get_raw(&a).await.is_err());
    assert!(ctx.multi_get_raw(&request).await.is_err());
    drop((values, first));
    ctx.close_request_read_view().unwrap();
    assert_eq!(budget.available(), 2 * 1024 * 1024);
    db.close().await.unwrap();
}

#[tokio::test]
async fn reuse_does_not_cross_snapshot_boundaries_or_hide_concurrent_changes() {
    let db = test_support::open_db("read-reuse-snapshot").await;
    let id = test_support::add_user(&db, "before").await;
    let budget = Budget::new(2 * 1024 * 1024);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(budget.clone());
    let key = ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
        id,
    )));
    ctx.enable_request_read_view().await.unwrap();
    ctx.enable_request_read_cache(256 * 1024);
    let old = ctx.get_raw(&key).await.unwrap();
    db.cypher(crate::cypher::Request::new(
        "MATCH (n:User) SET n.name = 'after'",
    ))
    .await
    .unwrap();
    assert_eq!(ctx.get_raw(&key).await.unwrap(), old);
    ctx.enable_request_write_scope().await.unwrap();
    assert!(ctx.request_read_cache().is_none());
    let updated = ctx.get_raw(&key).await.unwrap();
    assert_ne!(updated, old);
    let before = budget.reads().point_gets;
    assert_eq!(ctx.get_raw(&key).await.unwrap(), updated);
    assert_eq!(budget.reads().point_gets, before + 1);
    drop(updated);
    ctx.abort_request_write_scope();
    assert_eq!(ctx.get_raw(&key).await.unwrap(), old);
    ctx.close_request_read_view().unwrap();
    ctx.enable_request_read_view().await.unwrap();
    ctx.enable_request_read_cache(256 * 1024);
    let new = ctx.get_raw(&key).await.unwrap();
    assert_ne!(new, old);
    drop((new, old));
    ctx.close_request_read_view().unwrap();
    assert_eq!(budget.available(), 2 * 1024 * 1024);
    db.close().await.unwrap();
}
