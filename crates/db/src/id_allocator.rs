//! Lease-based ID allocator for sequential node ID allocation
//!
//! Instead of hitting storage for every ID, we "lease" a range of IDs upfront
//! and allocate from memory. Storage is only touched when:
//! - First allocation (read persisted counter, write new lease)
//! - Lease exhausted (write extended lease)
//!
//! Fast path is lock-free (atomic fetch_add). Lock only protects lease extension.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::Db;
use tokio::sync::Mutex;

use super::error::Result;
use crate::encoding::v1::keys::metadata;
use crate::encoding::v1::values::id_allocation::IdAllocationWatermarkValue;

/// Default number of IDs to lease at a time
pub const DEFAULT_LEASE_SIZE: u64 = 1000;

/// Lease-based ID allocator
///
/// State machine:
/// - `lease_end == 0`: Uninitialized (must read from storage)
/// - `lease_end > 0`: Initialized, can allocate IDs in range `[0, lease_end)`
/// - `next_id`: Counter for next ID to allocate (may exceed lease_end temporarily)
pub struct IdAllocator {
    /// Next ID to hand out. May temporarily exceed lease_end during extension.
    next_id: AtomicU64,

    /// Upper bound of current lease (exclusive). Zero means uninitialized.
    lease_end: AtomicU64,

    /// How many IDs to acquire per lease extension.
    lease_size: u64,

    /// Storage backend for persisting the high watermark.
    db: Arc<Db>,

    /// Storage key for the persisted high watermark.
    watermark_key: Bytes,

    /// Lock protecting lease extension. Only held during storage I/O.
    extend_lock: Mutex<()>,
}

impl IdAllocator {
    pub fn new(db: Arc<Db>, watermark_key: Bytes, lease_size: u64) -> Self {
        Self {
            next_id: AtomicU64::new(0),
            lease_end: AtomicU64::new(0),
            lease_size,
            db,
            watermark_key,
            extend_lock: Mutex::new(()),
        }
    }

    /// Allocate a single ID.
    ///
    /// Fast path (99.9% of calls): Single atomic increment, no I/O.
    /// Slow path: Lock + storage write when lease exhausted.
    pub async fn allocate(&self) -> Result<u64> {
        // Check if initialized
        if self.lease_end.load(Ordering::Acquire) == 0 {
            return self.initialize_and_allocate(1).await.map(|r| r.start);
        }

        // Fast path: atomically claim an ID
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);

        // Check if claimed ID is within our lease
        if id < self.lease_end.load(Ordering::Acquire) {
            return Ok(id);
        }

        // Claimed past lease boundary - extend to cover it
        self.ensure_lease_covers_claimed_ids().await?;
        Ok(id)
    }

    /// Allocate a contiguous batch of IDs.
    ///
    /// More efficient than calling `allocate()` in a loop.
    pub async fn allocate_batch(&self, count: u64) -> Result<std::ops::Range<u64>> {
        if count == 0 {
            return Ok(0..0);
        }

        // Check if initialized
        if self.lease_end.load(Ordering::Acquire) == 0 {
            return self.initialize_and_allocate(count).await;
        }

        // Fast path: atomically claim a range
        let start = self.next_id.fetch_add(count, Ordering::AcqRel);
        let end = start + count;

        // Check if entire range is within our lease
        if end <= self.lease_end.load(Ordering::Acquire) {
            return Ok(start..end);
        }

        // Claimed past lease boundary - extend to cover it
        self.ensure_lease_covers_claimed_ids().await?;
        Ok(start..end)
    }

    /// Cold start: read persisted counter from storage, then allocate.
    ///
    /// Called when `lease_end == 0` (uninitialized). Takes lock to ensure
    /// only one thread initializes.
    async fn initialize_and_allocate(&self, count: u64) -> Result<std::ops::Range<u64>> {
        let _guard = self.extend_lock.lock().await;

        // Double-check: another thread may have initialized while we waited
        let lease_end = self.lease_end.load(Ordering::Acquire);
        if lease_end > 0 {
            // Already initialized - do normal allocation under lock
            let start = self.next_id.fetch_add(count, Ordering::AcqRel);
            let end = start + count;
            if end <= lease_end {
                return Ok(start..end);
            }
            // Still need to extend
            self.persist_lease_extension().await?;
            return Ok(start..end);
        }

        // We're the initializing thread - read persisted counter
        let persisted_counter = self.load_persisted_watermark().await?;
        self.next_id.store(persisted_counter, Ordering::Release);

        // Claim IDs and extend lease to cover them
        let start = self.next_id.fetch_add(count, Ordering::AcqRel);
        self.persist_lease_extension().await?;

        Ok(start..start + count)
    }

    /// Ensure lease covers all claimed IDs.
    ///
    /// Called when a thread claimed an ID past lease_end. Takes lock to
    /// serialize extensions and avoid redundant storage writes.
    async fn ensure_lease_covers_claimed_ids(&self) -> Result<()> {
        let _guard = self.extend_lock.lock().await;

        // Double-check: another thread may have extended while we waited
        let next = self.next_id.load(Ordering::Acquire);
        let lease_end = self.lease_end.load(Ordering::Acquire);
        if next <= lease_end {
            return Ok(()); // Already covered
        }

        self.persist_lease_extension().await
    }

    /// Write extended lease to storage. Caller must hold extend_lock.
    async fn persist_lease_extension(&self) -> Result<()> {
        let next = self.next_id.load(Ordering::Acquire);
        let new_lease_end = Self::round_up_to_multiple(next, self.lease_size);

        // Persist new high watermark to storage
        let encoded = IdAllocationWatermarkValue::new(new_lease_end).encode();
        self.db.put(&self.watermark_key, &encoded).await?;

        // Update in-memory lease boundary
        self.lease_end.store(new_lease_end, Ordering::Release);

        Ok(())
    }

    /// Load the persisted watermark from storage.
    ///
    /// Returns 0 if not found (fresh database).
    async fn load_persisted_watermark(&self) -> Result<u64> {
        let value = self
            .db
            .get(&self.watermark_key)
            .await?
            .map(|bytes| {
                IdAllocationWatermarkValue::decode(&bytes).map(|value| value.exclusive_id())
            })
            .transpose()?
            .unwrap_or(0);
        Ok(value)
    }

    /// Round up to the next multiple of `multiple`.
    fn round_up_to_multiple(value: u64, multiple: u64) -> u64 {
        value.div_ceil(multiple) * multiple
    }

    /// Get remaining IDs in current lease.
    pub fn remaining_in_lease(&self) -> u64 {
        let next = self.next_id.load(Ordering::Acquire);
        let end = self.lease_end.load(Ordering::Acquire);
        end.saturating_sub(next)
    }
}

/// Node ID allocator (type-safe wrapper)
pub struct NodeIdAllocator(IdAllocator);

impl NodeIdAllocator {
    pub fn new(db: Arc<Db>, lease_size: u64) -> Self {
        Self(IdAllocator::new(
            db,
            metadata::MetadataKey::next_node_id_key().to_bytes(),
            lease_size,
        ))
    }

    pub async fn allocate(&self) -> Result<u64> {
        self.0.allocate().await
    }

    pub async fn allocate_batch(&self, count: u64) -> Result<std::ops::Range<u64>> {
        self.0.allocate_batch(count).await
    }

    pub fn remaining_in_lease(&self) -> u64 {
        self.0.remaining_in_lease()
    }
}

/// Edge ID allocator (type-safe wrapper)
pub struct EdgeIdAllocator(IdAllocator);

impl EdgeIdAllocator {
    pub fn new(db: Arc<Db>, lease_size: u64) -> Self {
        Self(IdAllocator::new(
            db,
            metadata::MetadataKey::next_edge_id_key().to_bytes(),
            lease_size,
        ))
    }

    pub async fn allocate(&self) -> Result<u64> {
        self.0.allocate().await
    }

    pub async fn allocate_batch(&self, count: u64) -> Result<std::ops::Range<u64>> {
        self.0.allocate_batch(count).await
    }

    pub fn remaining_in_lease(&self) -> u64 {
        self.0.remaining_in_lease()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Barrier;

    async fn test_db() -> Arc<Db> {
        Arc::new(
            Db::builder("test", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        )
    }

    /// Read the persisted HWM(High Water Mark) directly from storage
    async fn read_stored_hwm(db: &Db, key: &Bytes) -> u64 {
        db.get(key)
            .await
            .unwrap()
            .map(|d| u64::from_be_bytes(d.as_ref().try_into().expect("u64 field is 8 bytes")))
            .unwrap_or(0)
    }

    // =========================================================================
    // Basic Functionality
    // =========================================================================

    #[tokio::test]
    async fn test_sequential_allocation() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 100);

        assert_eq!(alloc.allocate().await.unwrap(), 0);
        assert_eq!(alloc.allocate().await.unwrap(), 1);
        assert_eq!(alloc.allocate().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_batch_allocation() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 100);

        assert_eq!(alloc.allocate_batch(5).await.unwrap(), 0..5);
        assert_eq!(alloc.allocate_batch(10).await.unwrap(), 5..15);
        assert_eq!(alloc.allocate().await.unwrap(), 15);
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 100);

        assert_eq!(alloc.allocate_batch(0).await.unwrap(), 0..0);
        assert_eq!(alloc.allocate().await.unwrap(), 0);
    }

    // =========================================================================
    // Lease Extension
    // =========================================================================

    #[tokio::test]
    async fn test_lease_extension() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 10);

        // Exhaust first lease
        for i in 0..10 {
            assert_eq!(alloc.allocate().await.unwrap(), i);
        }

        // Triggers extension
        assert_eq!(alloc.allocate().await.unwrap(), 10);
        assert!(alloc.remaining_in_lease() > 0);
    }

    #[tokio::test]
    async fn test_large_batch_extends_lease() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 10);

        assert_eq!(alloc.allocate_batch(25).await.unwrap(), 0..25);
        // Lease extended to 30 (rounded up)
        assert_eq!(alloc.remaining_in_lease(), 5);
    }

    #[tokio::test]
    async fn test_initialized_batch_extends_existing_lease() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 5);

        assert_eq!(alloc.allocate_batch(3).await.unwrap(), 0..3);
        assert_eq!(alloc.allocate_batch(4).await.unwrap(), 3..7);
        assert_eq!(alloc.remaining_in_lease(), 3);
    }

    #[tokio::test]
    async fn typed_allocators_delegate_all_public_operations() {
        let db = test_db().await;
        let nodes = NodeIdAllocator::new(Arc::clone(&db), 5);
        let edges = EdgeIdAllocator::new(db, 5);

        assert_eq!(nodes.allocate().await.unwrap(), 0);
        assert_eq!(nodes.allocate_batch(2).await.unwrap(), 1..3);
        assert_eq!(nodes.remaining_in_lease(), 2);

        assert_eq!(edges.allocate().await.unwrap(), 0);
        assert_eq!(edges.allocate_batch(2).await.unwrap(), 1..3);
        assert_eq!(edges.remaining_in_lease(), 2);
    }

    #[tokio::test]
    async fn test_hwm_is_minimal_after_extension() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = IdAllocator::new(Arc::clone(&db), key.clone(), 10);

        // Allocate 15 IDs (exhausts first lease of 10, extends to 20)
        for _ in 0..15 {
            alloc.allocate().await.unwrap();
        }

        // HWM should be exactly 20 (not 30)
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 20, "HWM should be minimal: 20, not {}", hwm);
    }

    // =========================================================================
    // Persistence
    // =========================================================================

    #[tokio::test]
    async fn test_persistence() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");

        {
            let alloc = IdAllocator::new(Arc::clone(&db), key.clone(), 100);
            for _ in 0..50 {
                alloc.allocate().await.unwrap();
            }
        }

        // New allocator continues from persisted HWM
        let alloc = IdAllocator::new(db, key, 100);
        assert_eq!(alloc.allocate().await.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_persistence_after_multiple_extensions() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");

        {
            let alloc = IdAllocator::new(Arc::clone(&db), key.clone(), 10);
            // Allocate 35 IDs, causing 4 lease extensions (10, 20, 30, 40)
            for _ in 0..35 {
                alloc.allocate().await.unwrap();
            }
        }

        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 40);

        let alloc = IdAllocator::new(db, key, 10);
        assert_eq!(alloc.allocate().await.unwrap(), 40);
    }

    // =========================================================================
    // Concurrent Allocation
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_allocation() {
        let alloc = Arc::new(IdAllocator::new(
            test_db().await,
            Bytes::from_static(b"test"),
            1000,
        ));

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                tokio::spawn(async move {
                    let mut ids = Vec::new();
                    for _ in 0..100 {
                        ids.push(alloc.allocate().await.unwrap());
                    }
                    ids
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect();

        all_ids.sort();
        assert_eq!(all_ids, (0..1000).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_batch_allocation() {
        let alloc = Arc::new(IdAllocator::new(
            test_db().await,
            Bytes::from_static(b"test"),
            100,
        ));

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                tokio::spawn(async move { alloc.allocate_batch(50).await.unwrap() })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect();

        all_ids.sort();
        assert_eq!(all_ids, (0..500).collect::<Vec<_>>());
    }

    // =========================================================================
    // Racing Initialization - Multiple tasks see lease_end == 0
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_racing_initialization_single_extend() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(IdAllocator::new(Arc::clone(&db), key.clone(), 100));

        let num_tasks = 20;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    alloc.allocate().await.unwrap()
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        // All IDs should be unique and sequential
        assert_eq!(all_ids, (0..num_tasks as u64).collect::<Vec<_>>());

        // HWM should be exactly 100 (one lease), not 100 * num_tasks
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(
            hwm, 100,
            "HWM should be 100, got {} (indicates multiple extends)",
            hwm
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_racing_initialization_batch_single_extend() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(IdAllocator::new(Arc::clone(&db), key.clone(), 100));

        let num_tasks = 10;
        let batch_size = 5u64;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    alloc.allocate_batch(batch_size).await.unwrap()
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        let expected_count = num_tasks * batch_size as usize;
        assert_eq!(all_ids, (0..expected_count as u64).collect::<Vec<_>>());

        // HWM should be exactly 100
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 100, "HWM should be 100, got {}", hwm);
    }

    // =========================================================================
    // Racing Extension - Multiple tasks claim past lease boundary
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_racing_extension_single_extend() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(IdAllocator::new(Arc::clone(&db), key.clone(), 10));

        // First, allocate 9 IDs (one away from lease boundary)
        for _ in 0..9 {
            alloc.allocate().await.unwrap();
        }

        // Now 20 tasks will race to get the remaining IDs
        let num_tasks = 20;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    alloc.allocate().await.unwrap()
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        // All IDs should be unique: 9..29
        assert_eq!(all_ids, (9..29).collect::<Vec<_>>());

        // HWM should be 30 (next_id is 29, rounded up to 30)
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 30, "HWM should be 30, got {}", hwm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_racing_extension_with_small_lease() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(IdAllocator::new(Arc::clone(&db), key.clone(), 5));

        let num_tasks = 50;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    alloc.allocate().await.unwrap()
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        assert_eq!(all_ids, (0..50).collect::<Vec<_>>());

        // HWM should be 50 (exactly 10 leases of 5)
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 50, "HWM should be 50, got {}", hwm);
    }

    // =========================================================================
    // Extension Count Verification
    // =========================================================================

    /// Wrapper that counts how many times persist_lease_extension is called
    struct CountingAllocator {
        inner: IdAllocator,
        extend_count: AtomicUsize,
    }

    impl CountingAllocator {
        fn new(db: Arc<Db>, key: Bytes, lease_size: u64) -> Self {
            Self {
                inner: IdAllocator::new(db, key, lease_size),
                extend_count: AtomicUsize::new(0),
            }
        }

        async fn allocate(&self) -> Result<u64> {
            let lease_end = self.inner.lease_end.load(Ordering::Acquire);

            if lease_end == 0 {
                let _guard = self.inner.extend_lock.lock().await;
                let lease_end = self.inner.lease_end.load(Ordering::Acquire);
                if lease_end > 0 {
                    let id = self.inner.next_id.fetch_add(1, Ordering::AcqRel);
                    if id < lease_end {
                        return Ok(id);
                    }
                    self.counted_persist().await?;
                    return Ok(id);
                }
                let watermark = self.inner.load_persisted_watermark().await?;
                self.inner.next_id.store(watermark, Ordering::Release);
                let id = self.inner.next_id.fetch_add(1, Ordering::AcqRel);
                self.counted_persist().await?;
                return Ok(id);
            }

            let id = self.inner.next_id.fetch_add(1, Ordering::AcqRel);
            if id < lease_end {
                return Ok(id);
            }

            let _guard = self.inner.extend_lock.lock().await;
            if self.inner.next_id.load(Ordering::Acquire)
                <= self.inner.lease_end.load(Ordering::Acquire)
            {
                return Ok(id);
            }
            self.counted_persist().await?;
            Ok(id)
        }

        async fn counted_persist(&self) -> Result<()> {
            self.extend_count.fetch_add(1, Ordering::SeqCst);
            self.inner.persist_lease_extension().await
        }

        fn extend_count(&self) -> usize {
            self.extend_count.load(Ordering::SeqCst)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_extension_count_minimal() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(CountingAllocator::new(Arc::clone(&db), key, 10));

        // 50 concurrent allocations with lease_size=10
        // Should need exactly 5 extensions (10, 20, 30, 40, 50)
        let num_tasks = 50;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    alloc.allocate().await.unwrap()
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        assert_eq!(all_ids, (0..50).collect::<Vec<_>>());

        // Should have exactly 5 extensions
        let count = alloc.extend_count();
        assert_eq!(count, 5, "Expected exactly 5 extensions, got {}", count);
    }

    #[tokio::test]
    async fn counting_allocator_covers_sequential_fast_and_extension_paths() {
        let alloc = CountingAllocator::new(test_db().await, Bytes::from_static(b"test"), 3);

        assert_eq!(alloc.allocate().await.unwrap(), 0);
        assert_eq!(alloc.allocate().await.unwrap(), 1);
        assert_eq!(alloc.allocate().await.unwrap(), 2);
        assert_eq!(alloc.allocate().await.unwrap(), 3);
        assert_eq!(alloc.extend_count(), 2);
    }

    #[tokio::test]
    async fn counting_allocator_observes_extension_completed_while_waiting() {
        let alloc = Arc::new(CountingAllocator::new(
            test_db().await,
            Bytes::from_static(b"test"),
            3,
        ));
        alloc.inner.next_id.store(1, Ordering::Release);
        alloc.inner.lease_end.store(1, Ordering::Release);
        let extension_guard = alloc.inner.extend_lock.lock().await;

        let waiting = {
            let alloc = Arc::clone(&alloc);
            tokio::spawn(async move { alloc.allocate().await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while alloc.inner.next_id.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("allocation should claim an id before waiting for the lock");

        alloc.inner.lease_end.store(3, Ordering::Release);
        drop(extension_guard);

        assert_eq!(waiting.await.unwrap().unwrap(), 1);
        assert_eq!(alloc.extend_count(), 0);
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    #[tokio::test]
    async fn test_lease_size_one() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 1);

        // Every allocation triggers an extension
        for i in 0..10 {
            assert_eq!(alloc.allocate().await.unwrap(), i);
        }
    }

    #[tokio::test]
    async fn test_batch_larger_than_lease() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = IdAllocator::new(Arc::clone(&db), key.clone(), 10);

        // Batch of 100 with lease_size=10
        let range = alloc.allocate_batch(100).await.unwrap();
        assert_eq!(range, 0..100);

        // HWM should be exactly 100
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 100);
    }

    #[tokio::test]
    async fn test_mixed_single_and_batch() {
        let alloc = IdAllocator::new(test_db().await, Bytes::from_static(b"test"), 20);

        assert_eq!(alloc.allocate().await.unwrap(), 0);
        assert_eq!(alloc.allocate_batch(5).await.unwrap(), 1..6);
        assert_eq!(alloc.allocate().await.unwrap(), 6);
        assert_eq!(alloc.allocate_batch(10).await.unwrap(), 7..17);
        assert_eq!(alloc.allocate().await.unwrap(), 17);
    }

    // =========================================================================
    // Stress Tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_stress_many_extensions() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(IdAllocator::new(Arc::clone(&db), key.clone(), 3));

        let num_tasks = 100;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    alloc.allocate().await.unwrap()
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        assert_eq!(all_ids, (0..100).collect::<Vec<_>>());

        // HWM should be 102 (ceil(100/3) * 3 = 34 * 3 = 102)
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 102);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_stress_mixed_operations() {
        let alloc = Arc::new(IdAllocator::new(
            test_db().await,
            Bytes::from_static(b"test"),
            7,
        ));

        let num_tasks = 50;
        let barrier = Arc::new(Barrier::new(num_tasks));

        let handles: Vec<_> = (0..num_tasks)
            .map(|i| {
                let alloc = Arc::clone(&alloc);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    if i % 2 == 0 {
                        vec![alloc.allocate().await.unwrap()]
                    } else {
                        alloc.allocate_batch(3).await.unwrap().collect()
                    }
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        // 25 single + 25 * 3 batch = 25 + 75 = 100 IDs
        assert_eq!(all_ids, (0..100).collect::<Vec<_>>());
    }
}
