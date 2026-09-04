//! Lease-based ID allocator for sequential node ID allocation
//!
//! Instead of hitting storage for every ID, we "lease" a range of IDs upfront
//! and allocate from memory. Storage is only touched when:
//! - First allocation (read persisted counter, write new lease)
//! - Lease exhausted (write extended lease)
//!
//! Fast path is lock-free. Lease extension is serialized, and proactive
//! extension runs in one background task per allocator.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::Db;
use tokio::sync::{Mutex, Notify};

use super::error::Result;
use crate::encoding::v2::keys::metadata;
use crate::encoding::v2::values::id_allocation::IdAllocationWatermarkValue;

/// Default number of IDs to lease at a time.
pub const DEFAULT_LEASE_SIZE: u64 = 10_000;

/// Default remaining-ID threshold for starting a proactive lease refill.
pub const DEFAULT_LEASE_REFILL_THRESHOLD: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RefillLifecycle {
    Idle,
    InFlight,
    ClosingInFlight,
    Closed,
}

impl RefillLifecycle {
    fn load(state: &AtomicU8) -> Self {
        match state.load(Ordering::Acquire) {
            value if value == Self::Idle as u8 => Self::Idle,
            value if value == Self::InFlight as u8 => Self::InFlight,
            value if value == Self::ClosingInFlight as u8 => Self::ClosingInFlight,
            value if value == Self::Closed as u8 => Self::Closed,
            value => unreachable!("invalid ID allocator refill lifecycle {value}"),
        }
    }
}

struct RefillCompletion {
    lifecycle: Arc<AtomicU8>,
    finished: Arc<Notify>,
}

impl Drop for RefillCompletion {
    fn drop(&mut self) {
        loop {
            let lifecycle = RefillLifecycle::load(&self.lifecycle);
            let completed = match lifecycle {
                RefillLifecycle::InFlight => RefillLifecycle::Idle,
                RefillLifecycle::ClosingInFlight => RefillLifecycle::Closed,
                RefillLifecycle::Idle | RefillLifecycle::Closed => {
                    unreachable!("completed ID allocator refill was not in flight")
                }
            };
            if self
                .lifecycle
                .compare_exchange(
                    lifecycle as u8,
                    completed as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.finished.notify_waiters();
                return;
            }
        }
    }
}

/// Lease-based ID allocator
///
/// State machine:
/// - `lease_end == 0`: Uninitialized (must read from storage)
/// - `lease_end > 0`: Initialized, can allocate IDs in range `[0, lease_end)`
/// - `next_id`: Counter for next ID to allocate (may exceed lease_end temporarily)
#[derive(Clone)]
pub struct IdAllocator {
    /// Next ID to hand out. May temporarily exceed lease_end during extension.
    next_id: Arc<AtomicU64>,

    /// Upper bound of current lease (exclusive). Zero means uninitialized.
    lease_end: Arc<AtomicU64>,

    /// How many IDs to acquire per lease extension.
    lease_size: u64,

    /// Remaining durable IDs at which a proactive refill should begin.
    refill_threshold: u64,

    /// Storage backend for persisting the high watermark.
    db: Arc<Db>,

    /// Storage key for the persisted high watermark.
    watermark_key: Bytes,

    /// Lock protecting lease extension. Only held during storage I/O.
    extend_lock: Arc<Mutex<()>>,

    /// Lock-free lifecycle gate for the single proactive refill task.
    refill_lifecycle: Arc<AtomicU8>,

    /// Wakes database close after an in-flight refill finishes.
    refill_finished: Arc<Notify>,
}

impl IdAllocator {
    pub fn new(db: Arc<Db>, watermark_key: Bytes, lease_size: u64) -> Self {
        Self::new_with_refill_threshold(
            db,
            watermark_key,
            lease_size,
            DEFAULT_LEASE_REFILL_THRESHOLD,
        )
    }

    pub(crate) fn new_with_refill_threshold(
        db: Arc<Db>,
        watermark_key: Bytes,
        lease_size: u64,
        refill_threshold: u64,
    ) -> Self {
        assert_ne!(lease_size, 0, "ID allocator lease size must be nonzero");
        Self {
            next_id: Arc::new(AtomicU64::new(0)),
            lease_end: Arc::new(AtomicU64::new(0)),
            lease_size,
            refill_threshold,
            db,
            watermark_key,
            extend_lock: Arc::new(Mutex::new(())),
            refill_lifecycle: Arc::new(AtomicU8::new(RefillLifecycle::Idle as u8)),
            refill_finished: Arc::new(Notify::new()),
        }
    }

    /// Allocate a single ID.
    ///
    /// Fast path (99.9% of calls): Single atomic increment, no I/O.
    /// Slow path: Lock + storage write when lease exhausted.
    pub async fn allocate(&self) -> Result<u64> {
        let range = if self.lease_end.load(Ordering::Acquire) == 0 {
            self.initialize_and_allocate(1).await?
        } else {
            let range = self.claim_ids(1)?;
            if range.end > self.lease_end.load(Ordering::Acquire) {
                self.ensure_lease_covers_claimed_ids().await?;
            }
            range
        };
        self.start_refill_if_needed();
        Ok(range.start)
    }

    /// Allocate a contiguous batch of IDs.
    ///
    /// More efficient than calling `allocate()` in a loop.
    pub async fn allocate_batch(&self, count: u64) -> Result<std::ops::Range<u64>> {
        if count == 0 {
            return Ok(0..0);
        }

        let range = if self.lease_end.load(Ordering::Acquire) == 0 {
            self.initialize_and_allocate(count).await?
        } else {
            let range = self.claim_ids(count)?;
            if range.end > self.lease_end.load(Ordering::Acquire) {
                self.ensure_lease_covers_claimed_ids().await?;
            }
            range
        };
        self.start_refill_if_needed();
        Ok(range)
    }

    /// Ensure no future allocation returns an ID below `next_id`.
    pub async fn reserve_at_least(&self, next_id: u64) -> Result<()> {
        let _guard = self.extend_lock.lock().await;
        let persisted_counter = self.load_persisted_watermark().await?;
        let current_next = self.next_id.load(Ordering::Acquire);
        let target = persisted_counter.max(current_next).max(next_id);

        self.next_id.fetch_max(target, Ordering::AcqRel);
        let current_lease_end = self.lease_end.load(Ordering::Acquire);
        let durable_lease_end = current_lease_end.max(persisted_counter);
        let next = self.next_id.load(Ordering::Acquire);
        if current_lease_end < next || persisted_counter < next {
            self.persist_lease_extension(durable_lease_end).await?;
        }

        Ok(())
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
            let range = self.claim_ids(count)?;
            if range.end <= lease_end {
                return Ok(range);
            }
            // Still need to extend
            self.persist_lease_extension(lease_end).await?;
            return Ok(range);
        }

        // We're the initializing thread - read persisted counter
        let persisted_counter = self.load_persisted_watermark().await?;
        self.next_id.store(persisted_counter, Ordering::Release);

        // Claim IDs and extend lease to cover them
        let range = self.claim_ids(count)?;
        self.persist_lease_extension(persisted_counter).await?;

        Ok(range)
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

        self.persist_lease_extension(lease_end).await
    }

    /// Persist enough whole lease chunks to cover claims and refill headroom.
    ///
    /// The caller must hold `extend_lock` and provide the greatest currently
    /// durable watermark observed under that lock.
    async fn persist_lease_extension(&self, current_lease_end: u64) -> Result<()> {
        let next = self.next_id.load(Ordering::Acquire);
        let new_lease_end = Self::extension_target(
            current_lease_end,
            next,
            self.lease_size,
            self.refill_threshold,
        );
        if new_lease_end == current_lease_end {
            return Ok(());
        }

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

    /// Compute the next exclusive watermark without overflowing the ID space.
    fn extension_target(
        current_lease_end: u64,
        next_id: u64,
        lease_size: u64,
        refill_threshold: u64,
    ) -> u64 {
        let desired_lease_end = next_id.saturating_add(refill_threshold);
        if desired_lease_end < current_lease_end {
            return current_lease_end;
        }

        let missing = desired_lease_end - current_lease_end;
        let chunks = if missing == 0 {
            1
        } else {
            missing.div_ceil(lease_size)
        };
        current_lease_end
            .saturating_add(chunks.saturating_mul(lease_size))
            .max(desired_lease_end)
    }

    /// Claim a range without allowing the atomic counter to wrap.
    fn claim_ids(&self, count: u64) -> Result<std::ops::Range<u64>> {
        let start = self
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                next.checked_add(count)
            })
            .map_err(|_| {
                crate::HelixDbError::InvariantViolation(
                    "ID allocator exhausted the u64 ID space".to_string(),
                )
            })?;
        Ok(start..start + count)
    }

    /// Start one best-effort refill without delaying the successful claim.
    fn start_refill_if_needed(&self) {
        if self.remaining_in_lease() > self.refill_threshold
            || self
                .refill_lifecycle
                .compare_exchange(
                    RefillLifecycle::Idle as u8,
                    RefillLifecycle::InFlight as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return;
        }

        let allocator = self.clone();
        let completion = RefillCompletion {
            lifecycle: Arc::clone(&allocator.refill_lifecycle),
            finished: Arc::clone(&allocator.refill_finished),
        };
        tokio::spawn(async move {
            let _completion = completion;
            if let Err(error) = allocator.refill_if_needed().await {
                tracing::warn!(
                    error = %error,
                    "proactive ID allocator lease refill failed"
                );
            }
        });
    }

    /// Recheck the low-water mark under the extension lock and refill it.
    async fn refill_if_needed(&self) -> Result<()> {
        let _guard = self.extend_lock.lock().await;
        let next = self.next_id.load(Ordering::Acquire);
        let lease_end = self.lease_end.load(Ordering::Acquire);
        if lease_end.saturating_sub(next) > self.refill_threshold {
            return Ok(());
        }
        self.persist_lease_extension(lease_end).await
    }

    /// Prevent new refills and wait for the current background write to finish.
    pub(crate) async fn shutdown(&self) {
        loop {
            match RefillLifecycle::load(&self.refill_lifecycle) {
                RefillLifecycle::Idle => {
                    if self
                        .refill_lifecycle
                        .compare_exchange(
                            RefillLifecycle::Idle as u8,
                            RefillLifecycle::Closed as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                RefillLifecycle::InFlight => {
                    let _ = self.refill_lifecycle.compare_exchange(
                        RefillLifecycle::InFlight as u8,
                        RefillLifecycle::ClosingInFlight as u8,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
                RefillLifecycle::ClosingInFlight => {
                    let notified = self.refill_finished.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if RefillLifecycle::load(&self.refill_lifecycle)
                        == RefillLifecycle::ClosingInFlight
                    {
                        notified.await;
                    }
                }
                RefillLifecycle::Closed => return,
            }
        }
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
        Self::new_with_refill_threshold(db, lease_size, DEFAULT_LEASE_REFILL_THRESHOLD)
    }

    pub(crate) fn new_with_refill_threshold(
        db: Arc<Db>,
        lease_size: u64,
        refill_threshold: u64,
    ) -> Self {
        Self(IdAllocator::new_with_refill_threshold(
            db,
            metadata::MetadataKey::next_node_id_key().to_bytes(),
            lease_size,
            refill_threshold,
        ))
    }

    pub async fn allocate(&self) -> Result<u64> {
        self.0.allocate().await
    }

    pub async fn allocate_batch(&self, count: u64) -> Result<std::ops::Range<u64>> {
        self.0.allocate_batch(count).await
    }

    pub async fn reserve_at_least(&self, next_id: u64) -> Result<()> {
        self.0.reserve_at_least(next_id).await
    }

    pub fn remaining_in_lease(&self) -> u64 {
        self.0.remaining_in_lease()
    }

    pub(crate) async fn shutdown(&self) {
        self.0.shutdown().await;
    }
}

/// Edge ID allocator (type-safe wrapper)
pub struct EdgeIdAllocator(IdAllocator);

impl EdgeIdAllocator {
    pub fn new(db: Arc<Db>, lease_size: u64) -> Self {
        Self::new_with_refill_threshold(db, lease_size, DEFAULT_LEASE_REFILL_THRESHOLD)
    }

    pub(crate) fn new_with_refill_threshold(
        db: Arc<Db>,
        lease_size: u64,
        refill_threshold: u64,
    ) -> Self {
        Self(IdAllocator::new_with_refill_threshold(
            db,
            metadata::MetadataKey::next_edge_id_key().to_bytes(),
            lease_size,
            refill_threshold,
        ))
    }

    pub async fn allocate(&self) -> Result<u64> {
        self.0.allocate().await
    }

    pub async fn reserve_at_least(&self, next_id: u64) -> Result<()> {
        self.0.reserve_at_least(next_id).await
    }

    pub async fn allocate_batch(&self, count: u64) -> Result<std::ops::Range<u64>> {
        self.0.allocate_batch(count).await
    }

    pub fn remaining_in_lease(&self) -> u64 {
        self.0.remaining_in_lease()
    }

    pub(crate) async fn shutdown(&self) {
        self.0.shutdown().await;
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

    fn test_allocator(db: Arc<Db>, key: Bytes, lease_size: u64) -> IdAllocator {
        let allocator = IdAllocator::new_with_refill_threshold(db, key, lease_size, 0);
        allocator
            .refill_lifecycle
            .store(RefillLifecycle::Closed as u8, Ordering::Release);
        allocator
    }

    async fn wait_for_refill_lifecycle(allocator: &IdAllocator, expected: RefillLifecycle) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while RefillLifecycle::load(&allocator.refill_lifecycle) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("allocator reaches the expected refill lifecycle");
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
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 100);

        assert_eq!(alloc.allocate().await.unwrap(), 0);
        assert_eq!(alloc.allocate().await.unwrap(), 1);
        assert_eq!(alloc.allocate().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_batch_allocation() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 100);

        assert_eq!(alloc.allocate_batch(5).await.unwrap(), 0..5);
        assert_eq!(alloc.allocate_batch(10).await.unwrap(), 5..15);
        assert_eq!(alloc.allocate().await.unwrap(), 15);
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 100);

        assert_eq!(alloc.allocate_batch(0).await.unwrap(), 0..0);
        assert_eq!(alloc.allocate().await.unwrap(), 0);
    }

    // =========================================================================
    // Lease Extension
    // =========================================================================

    #[tokio::test]
    async fn test_lease_extension() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 10);

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
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 10);

        assert_eq!(alloc.allocate_batch(25).await.unwrap(), 0..25);
        // Lease extended to 30 (rounded up)
        assert_eq!(alloc.remaining_in_lease(), 5);
    }

    #[tokio::test]
    async fn test_initialized_batch_extends_existing_lease() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 5);

        assert_eq!(alloc.allocate_batch(3).await.unwrap(), 0..3);
        assert_eq!(alloc.allocate_batch(4).await.unwrap(), 3..7);
        assert_eq!(alloc.remaining_in_lease(), 3);
    }

    #[tokio::test]
    async fn typed_allocators_delegate_all_public_operations() {
        let db = test_db().await;
        let nodes = NodeIdAllocator::new_with_refill_threshold(Arc::clone(&db), 5, 0);
        let edges = EdgeIdAllocator::new_with_refill_threshold(db, 5, 0);

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
        let alloc = test_allocator(Arc::clone(&db), key.clone(), 10);

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
            let alloc = test_allocator(Arc::clone(&db), key.clone(), 100);
            for _ in 0..50 {
                alloc.allocate().await.unwrap();
            }
        }

        // New allocator continues from persisted HWM
        let alloc = test_allocator(db, key, 100);
        assert_eq!(alloc.allocate().await.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_persistence_after_multiple_extensions() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");

        {
            let alloc = test_allocator(Arc::clone(&db), key.clone(), 10);
            // Allocate 35 IDs, causing 4 lease extensions (10, 20, 30, 40)
            for _ in 0..35 {
                alloc.allocate().await.unwrap();
            }
        }

        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 40);

        let alloc = test_allocator(db, key, 10);
        assert_eq!(alloc.allocate().await.unwrap(), 40);
    }

    // =========================================================================
    // Concurrent Allocation
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_allocation() {
        let alloc = Arc::new(test_allocator(
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
        let alloc = Arc::new(test_allocator(
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
        let alloc = Arc::new(test_allocator(Arc::clone(&db), key.clone(), 100));

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
        let alloc = Arc::new(test_allocator(Arc::clone(&db), key.clone(), 100));

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
        let alloc = Arc::new(test_allocator(Arc::clone(&db), key.clone(), 10));

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
        let alloc = Arc::new(test_allocator(Arc::clone(&db), key.clone(), 5));

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
                inner: IdAllocator::new_with_refill_threshold(db, key, lease_size, 0),
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
            let lease_end = self.inner.lease_end.load(Ordering::Acquire);
            self.inner.persist_lease_extension(lease_end).await
        }

        fn extend_count(&self) -> usize {
            self.extend_count.load(Ordering::SeqCst)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_extension_count_minimal() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = Arc::new(CountingAllocator::new(Arc::clone(&db), key.clone(), 10));

        // Fill the initial lease before forcing all remaining claims to wait for
        // the same extension. The first waiter must cover every claimed ID with
        // one write; the remaining waiters must observe that completed write.
        for expected_id in 0..10 {
            assert_eq!(alloc.allocate().await.unwrap(), expected_id);
        }
        assert_eq!(alloc.extend_count(), 1);

        let extension_guard = alloc.inner.extend_lock.lock().await;
        let num_tasks = 40;

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                tokio::spawn(async move { alloc.allocate().await.unwrap() })
            })
            .collect();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while alloc.inner.next_id.load(Ordering::Acquire) != 50 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all allocations should claim an id before the extension lock is released");
        drop(extension_guard);

        let mut all_ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        all_ids.sort();

        assert_eq!(all_ids, (10..50).collect::<Vec<_>>());
        assert_eq!(read_stored_hwm(&db, &key).await, 50);
        assert_eq!(alloc.extend_count(), 2);
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
    // Proactive Refill
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exact_threshold_starts_one_nonblocking_refill() {
        let db = test_db().await;
        let key = Bytes::from_static(b"proactive");
        let alloc = Arc::new(IdAllocator::new_with_refill_threshold(
            Arc::clone(&db),
            key.clone(),
            10,
            5,
        ));

        assert_eq!(alloc.allocate().await.unwrap(), 0);
        for expected in 1..4 {
            assert_eq!(alloc.allocate().await.unwrap(), expected);
        }
        assert_eq!(alloc.remaining_in_lease(), 6);

        let extension_guard = alloc.extend_lock.lock().await;
        let threshold_allocation =
            tokio::time::timeout(std::time::Duration::from_secs(1), alloc.allocate())
                .await
                .expect("threshold allocation must not wait for the extension lock")
                .unwrap();
        assert_eq!(threshold_allocation, 4);
        wait_for_refill_lifecycle(&alloc, RefillLifecycle::InFlight).await;
        assert_eq!(read_stored_hwm(&db, &key).await, 10);

        let concurrent = (0..5)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                tokio::spawn(async move { alloc.allocate().await.unwrap() })
            })
            .collect::<Vec<_>>();
        let mut concurrent_ids = futures::future::join_all(concurrent)
            .await
            .into_iter()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();
        concurrent_ids.sort_unstable();
        assert_eq!(concurrent_ids, (5..10).collect::<Vec<_>>());
        assert_eq!(
            RefillLifecycle::load(&alloc.refill_lifecycle),
            RefillLifecycle::InFlight
        );

        drop(extension_guard);
        alloc.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &key).await, 20);
        assert_eq!(
            RefillLifecycle::load(&alloc.refill_lifecycle),
            RefillLifecycle::Closed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_can_skip_past_threshold_without_waiting() {
        let db = test_db().await;
        let key = Bytes::from_static(b"batch-proactive");
        let alloc = IdAllocator::new_with_refill_threshold(Arc::clone(&db), key.clone(), 10, 5);

        assert_eq!(alloc.allocate_batch(4).await.unwrap(), 0..4);
        let extension_guard = alloc.extend_lock.lock().await;
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), alloc.allocate_batch(3),)
                .await
                .expect("in-lease batch must not wait for proactive refill")
                .unwrap(),
            4..7
        );
        wait_for_refill_lifecycle(&alloc, RefillLifecycle::InFlight).await;

        drop(extension_guard);
        alloc.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &key).await, 20);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn allocation_past_durable_boundary_waits_for_refill() {
        let db = test_db().await;
        let key = Bytes::from_static(b"blocking-fallback");
        let alloc = Arc::new(IdAllocator::new_with_refill_threshold(
            Arc::clone(&db),
            key.clone(),
            10,
            5,
        ));

        assert_eq!(alloc.allocate_batch(4).await.unwrap(), 0..4);
        let extension_guard = alloc.extend_lock.lock().await;
        assert_eq!(alloc.allocate_batch(6).await.unwrap(), 4..10);
        wait_for_refill_lifecycle(&alloc, RefillLifecycle::InFlight).await;

        let waiting = {
            let alloc = Arc::clone(&alloc);
            tokio::spawn(async move { alloc.allocate().await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while alloc.next_id.load(Ordering::Acquire) != 11 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("boundary allocation claims its ID before waiting");
        assert!(!waiting.is_finished());

        drop(extension_guard);
        assert_eq!(waiting.await.unwrap().unwrap(), 10);
        alloc.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &key).await, 20);
    }

    #[tokio::test]
    async fn zero_and_large_thresholds_have_defined_headroom() {
        let db = test_db().await;
        let zero_key = Bytes::from_static(b"zero-threshold");
        let zero = IdAllocator::new_with_refill_threshold(Arc::clone(&db), zero_key.clone(), 3, 0);
        assert_eq!(zero.allocate_batch(3).await.unwrap(), 0..3);
        zero.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &zero_key).await, 6);

        let large_key = Bytes::from_static(b"large-threshold");
        let large =
            IdAllocator::new_with_refill_threshold(Arc::clone(&db), large_key.clone(), 10, 25);
        assert_eq!(large.allocate().await.unwrap(), 0);
        assert_eq!(large.remaining_in_lease(), 29);
        assert_eq!(large.allocate_batch(4).await.unwrap(), 1..5);
        large.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &large_key).await, 40);
    }

    #[tokio::test]
    async fn reopening_can_change_lease_and_threshold_tuning() {
        let db = test_db().await;
        let key = Bytes::from_static(b"retuned");
        let first = IdAllocator::new_with_refill_threshold(Arc::clone(&db), key.clone(), 10, 5);
        assert_eq!(first.allocate().await.unwrap(), 0);
        first.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &key).await, 10);

        let second = IdAllocator::new_with_refill_threshold(Arc::clone(&db), key.clone(), 6, 3);
        assert_eq!(second.allocate().await.unwrap(), 10);
        second.shutdown().await;
        assert_eq!(read_stored_hwm(&db, &key).await, 16);
    }

    #[tokio::test]
    async fn failed_background_refill_retries_and_exhaustion_fails_closed() {
        let db = test_db().await;
        let alloc = IdAllocator::new_with_refill_threshold(
            Arc::clone(&db),
            Bytes::from_static(b"closed-db"),
            4,
            2,
        );
        assert_eq!(alloc.allocate().await.unwrap(), 0);
        db.close().await.unwrap();

        assert_eq!(alloc.allocate().await.unwrap(), 1);
        wait_for_refill_lifecycle(&alloc, RefillLifecycle::Idle).await;
        assert_eq!(alloc.allocate().await.unwrap(), 2);
        wait_for_refill_lifecycle(&alloc, RefillLifecycle::Idle).await;
        assert_eq!(alloc.allocate().await.unwrap(), 3);
        wait_for_refill_lifecycle(&alloc, RefillLifecycle::Idle).await;
        assert!(alloc.allocate().await.is_err());
        alloc.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reserve_and_allocate_concurrently_preserve_monotonic_ids() {
        let db = test_db().await;
        let alloc = Arc::new(IdAllocator::new_with_refill_threshold(
            db,
            Bytes::from_static(b"concurrent-reserve"),
            10,
            5,
        ));
        assert_eq!(alloc.allocate().await.unwrap(), 0);

        let reservation = {
            let alloc = Arc::clone(&alloc);
            tokio::spawn(async move { alloc.reserve_at_least(50).await })
        };
        let allocation = {
            let alloc = Arc::clone(&alloc);
            tokio::spawn(async move { alloc.allocate_batch(3).await })
        };
        reservation.await.unwrap().unwrap();
        let allocated = allocation.await.unwrap().unwrap();
        let next = alloc.allocate().await.unwrap();
        assert!(allocated.end <= 50 || allocated.start >= 50);
        assert!(next >= 50);
        alloc.shutdown().await;
    }

    #[test]
    fn extension_target_clamps_at_the_end_of_the_id_space() {
        assert_eq!(IdAllocator::extension_target(10, 5, 10, 5), 20);
        assert_eq!(IdAllocator::extension_target(0, 1, 10, 25), 30);
        assert_eq!(
            IdAllocator::extension_target(u64::MAX - 5, u64::MAX - 1, 10, 5),
            u64::MAX
        );
    }

    #[tokio::test]
    async fn allocator_exhaustion_does_not_wrap() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"exhaustion"), 10);
        alloc.next_id.store(u64::MAX - 1, Ordering::Release);
        alloc.lease_end.store(u64::MAX, Ordering::Release);

        assert_eq!(alloc.allocate().await.unwrap(), u64::MAX - 1);
        assert!(alloc.allocate().await.is_err());
        assert_eq!(alloc.next_id.load(Ordering::Acquire), u64::MAX);
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    #[tokio::test]
    async fn test_lease_size_one() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 1);

        // Every allocation triggers an extension
        for i in 0..10 {
            assert_eq!(alloc.allocate().await.unwrap(), i);
        }
    }

    #[tokio::test]
    async fn test_batch_larger_than_lease() {
        let db = test_db().await;
        let key = Bytes::from_static(b"test");
        let alloc = test_allocator(Arc::clone(&db), key.clone(), 10);

        // Batch of 100 with lease_size=10
        let range = alloc.allocate_batch(100).await.unwrap();
        assert_eq!(range, 0..100);

        // HWM should be exactly 100
        let hwm = read_stored_hwm(&db, &key).await;
        assert_eq!(hwm, 100);
    }

    #[tokio::test]
    async fn test_mixed_single_and_batch() {
        let alloc = test_allocator(test_db().await, Bytes::from_static(b"test"), 20);

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
        let alloc = Arc::new(test_allocator(Arc::clone(&db), key.clone(), 3));

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
        let alloc = Arc::new(test_allocator(
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
