//! Shared request-owned memory admission and logical storage-read accounting.
//! Resource guards follow buffer ownership and release on cancellation/errors.
//! This layer depends on storage errors, not either query frontend.
use crate::error::{HelixDbError, Result};

pub(crate) mod adjacency;
pub(crate) mod bitmap;
pub(crate) mod future;
pub(crate) mod properties;

/// Logical storage calls made through a request. Object-store reads are separate:
/// storage caches and transactions can satisfy these calls without external I/O.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct StorageReadUsage {
    pub point_gets: usize,
    pub multi_get_batches: usize,
    pub multi_get_keys: usize,
    pub scans: usize,
    pub scan_rows: usize,
}

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
pub(crate) struct Budget(Arc<State>);
struct State {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    point_gets: AtomicUsize,
    multi_get_batches: AtomicUsize,
    multi_get_keys: AtomicUsize,
    scans: AtomicUsize,
    scan_rows: AtomicUsize,
}
impl Budget {
    pub fn new(limit: usize) -> Self {
        Self(Arc::new(State {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            point_gets: AtomicUsize::new(0),
            multi_get_batches: AtomicUsize::new(0),
            multi_get_keys: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_rows: AtomicUsize::new(0),
        }))
    }
    pub(crate) fn reserve(&self, bytes: usize) -> Result<Reservation> {
        let mut reservation = Reservation {
            budget: self.clone(),
            bytes: 0,
        };
        reservation.resize(bytes)?;
        Ok(reservation)
    }
    /// Admit a boxed async operation before allocation. Its state exists only
    /// while that operation is pending, instead of inflating every suspended
    /// outer cursor. Dropping an unpolled or pending future releases the charge.
    pub(crate) fn admitted_future<F: std::future::Future>(
        &self,
        future: F,
    ) -> Result<impl std::future::Future<Output = F::Output> + use<F>> {
        let memory = self.reserve(size_of::<F>())?;
        let future = Box::pin(future);
        Ok(async move {
            let result = future.await;
            drop(memory);
            result
        })
    }
    pub(crate) fn available(&self) -> usize {
        self.0
            .limit
            .saturating_sub(self.0.used.load(Ordering::Relaxed))
    }
    pub(crate) fn peak(&self) -> usize {
        self.0.peak.load(Ordering::Relaxed)
    }

    pub(crate) fn record_reads(&self, reads: StorageReadUsage) {
        for (counter, amount) in [
            (&self.0.point_gets, reads.point_gets),
            (&self.0.multi_get_batches, reads.multi_get_batches),
            (&self.0.multi_get_keys, reads.multi_get_keys),
            (&self.0.scans, reads.scans),
            (&self.0.scan_rows, reads.scan_rows),
        ] {
            if amount > 0 {
                let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(amount))
                });
            }
        }
    }

    pub(crate) fn reads(&self) -> StorageReadUsage {
        StorageReadUsage {
            point_gets: self.0.point_gets.load(Ordering::Relaxed),
            multi_get_batches: self.0.multi_get_batches.load(Ordering::Relaxed),
            multi_get_keys: self.0.multi_get_keys.load(Ordering::Relaxed),
            scans: self.0.scans.load(Ordering::Relaxed),
            scan_rows: self.0.scan_rows.load(Ordering::Relaxed),
        }
    }
}

/// A pinned stream whose allocation remains charged until its final owner drops.
/// The wrapper can move; its potentially immovable stream stays in the same box.
struct AdmittedStream<S> {
    stream: std::pin::Pin<Box<S>>,
    _memory: Reservation,
}
impl<S: futures::Stream> futures::Stream for AdmittedStream<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
impl Budget {
    /// Admit stream state before pinning it on the heap. The returned owner
    /// carries admission through pending polls, exhaustion and cancellation.
    pub(crate) fn admitted_stream<S: futures::Stream>(
        &self,
        stream: S,
    ) -> Result<impl futures::Stream<Item = S::Item> + use<S>> {
        let memory = self.reserve(size_of::<S>())?;
        Ok(AdmittedStream {
            stream: Box::pin(stream),
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod stream_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_transfer_preserves_exact_admission_without_allocating() {
        let budget = Budget::new(100);
        let mut output = budget.reserve(60).unwrap();
        let input = budget.reserve(40).unwrap();
        let (_, allocations) = crate::allocation_testing::observe(|| output.absorb(input));
        assert_eq!(allocations.allocations, 0);
        assert_eq!(budget.available(), 0);
        assert_eq!(budget.peak(), 100);
        output.absorb(budget.reserve(0).unwrap());
        output.resize(10).unwrap();
        assert_eq!(budget.available(), 90);
        let unrelated = Budget::new(100);
        let foreign = unrelated.reserve(70).unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            output.absorb(foreign);
        }))
        .is_err());
        assert_eq!(unrelated.available(), 100);
        assert_eq!(budget.available(), 90);
        drop(output);
        assert_eq!(budget.available(), 100);
        let budget = Budget::new(usize::MAX);
        let mut output = budget.reserve(usize::MAX - 1).unwrap();
        output.absorb(budget.reserve(1).unwrap());
        assert_eq!(budget.available(), 0);
        drop(output);
        assert_eq!(budget.available(), usize::MAX);
    }

    #[test]
    fn admitted_futures_release_before_poll_on_cancel_success_error_and_panic() {
        use futures::{executor::block_on, FutureExt};
        let budget = Budget::new(4096);
        let completed = budget.admitted_future(std::future::ready(17)).unwrap();
        assert!(budget.available() < 4096);
        assert_eq!(block_on(completed), 17);
        assert_eq!(budget.available(), 4096);
        let failed = budget
            .admitted_future(std::future::ready(Err::<(), _>("failure")))
            .unwrap();
        assert_eq!(block_on(failed), Err("failure"));
        assert_eq!(budget.available(), 4096);
        for poll in [false, true] {
            let state = [3_u8; 1024];
            let pending = budget
                .admitted_future(async move {
                    std::future::pending::<()>().await;
                    std::hint::black_box(state)
                })
                .unwrap();
            assert!(budget.available() <= 4096 - 1024);
            if poll {
                assert!(pending.now_or_never().is_none());
            } else {
                drop(pending);
            }
            assert_eq!(budget.available(), 4096);
        }
        let panics = budget
            .admitted_future(async { panic!("future failure") })
            .unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(panics))).is_err()
        );
        assert_eq!(budget.available(), 4096);
        let polled = std::cell::Cell::new(false);
        let was_polled = &polled;
        let state = [3_u8; 8192];
        assert!(budget
            .admitted_future(async move {
                was_polled.set(true);
                std::future::pending::<()>().await;
                std::hint::black_box(state)
            })
            .is_err());
        assert!(!polled.get());
        assert_eq!(budget.available(), 4096);
    }

    #[test]
    fn shared_reservations_release_on_failure_shrink_move_and_overflow() {
        let budget = Budget::new(100);
        let mut first = budget.reserve(60).unwrap();
        assert!(matches!(
            budget.reserve(41),
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        let second = budget.reserve(40).unwrap();
        assert_eq!(budget.peak(), 100);
        assert!(first.resize(61).is_err());
        drop(second);
        first.resize(100).unwrap();
        first.resize(0).unwrap();
        let moved = budget.reserve(100).unwrap();
        drop(moved);
        assert!(budget.reserve(usize::MAX).is_err());
        assert_eq!(budget.available(), 100);
        let mut partial = budget.reserve(60).unwrap();
        partial.release(0);
        partial.release(20);
        assert_eq!(budget.available(), 60);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| partial.release(41))).is_err()
        );
        assert_eq!(budget.available(), 60);
        drop(partial);
        assert_eq!(budget.available(), 100);
        let budget = Budget::new(usize::MAX);
        let full = budget.reserve(usize::MAX).unwrap();
        assert!(budget.reserve(1).is_err());
        drop(full);
        assert_eq!(budget.available(), usize::MAX);
    }

    #[test]
    fn concurrent_admission_cannot_oversubscribe_a_shared_budget() {
        let budget = Budget::new(100);
        let rendezvous = std::sync::Barrier::new(8);
        std::thread::scope(|threads| {
            for _ in 0..8 {
                threads.spawn(|| {
                    rendezvous.wait();
                    let reservation = budget.reserve(30);
                    // Every attempted admission completes before any successful
                    // thread drops its guard. Exactly three can hold 30 bytes.
                    rendezvous.wait();
                    let available = budget.available();
                    rendezvous.wait();
                    assert_eq!(available, 10);
                    drop(reservation);
                });
            }
        });
        assert_eq!(budget.peak(), 90);
        assert_eq!(budget.available(), 100);
    }

    #[test]
    fn counters_saturate_and_shared_byte_owners_keep_their_charge() {
        let budget = Budget::new(4096);
        budget.record_reads(StorageReadUsage {
            point_gets: usize::MAX,
            multi_get_batches: usize::MAX,
            multi_get_keys: usize::MAX,
            scans: usize::MAX,
            scan_rows: usize::MAX,
        });
        let before = budget.reads();
        budget.record_reads(StorageReadUsage::default());
        budget.record_reads(StorageReadUsage {
            point_gets: 1,
            multi_get_batches: 1,
            multi_get_keys: 1,
            scans: 1,
            scan_rows: 1,
        });
        assert_eq!(budget.reads(), before);
        let bytes = Bytes::from(vec![7; 2048]);
        let pointer = bytes.as_ptr();
        let retained = budget.retain_read(bytes).unwrap();
        assert_eq!(retained.as_ptr(), pointer);
        let available = budget.available();
        let cloned = retained.clone();
        let slice = retained.slice(1..2);
        assert_eq!(budget.available(), available);
        assert!(budget.retain_read(Bytes::from(vec![0; 2048])).is_err());
        assert_eq!(budget.available(), available);
        drop(retained);
        drop(cloned);
        assert_eq!(budget.available(), available);
        assert_eq!(&slice[..], &[7]);
        drop(slice);
        assert_eq!(budget.available(), 4096);
        let empty = budget.retain_read(Bytes::new()).unwrap();
        assert!(empty.is_empty());
        assert!(budget.available() < 4096);
        drop(empty);
        assert_eq!(budget.available(), 4096);
    }
}

pub(crate) struct Reservation {
    budget: Budget,
    bytes: usize,
}
impl Reservation {
    /// Transfer an already-admitted owner in the same query without briefly
    /// charging both reservations for the same output. Callers release surplus
    /// construction allowance before transferring the retained allocation.
    pub(crate) fn absorb(&mut self, mut other: Self) {
        assert!(
            Arc::ptr_eq(&self.budget.0, &other.budget.0),
            "reservation transfer crosses query budgets"
        );
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .expect("same-budget reservations fit its admitted total");
        other.bytes = 0;
    }

    /// Release an already-dropped portion without recounting the retained owner.
    /// The owner must never release more than it previously admitted.
    pub(crate) fn release(&mut self, bytes: usize) {
        assert!(
            bytes <= self.bytes,
            "released bytes exceed their reservation"
        );
        self.budget.0.used.fetch_sub(bytes, Ordering::Relaxed);
        self.bytes -= bytes;
    }

    pub fn resize(&mut self, bytes: usize) -> Result<()> {
        if bytes <= self.bytes {
            self.budget
                .0
                .used
                .fetch_sub(self.bytes - bytes, Ordering::Relaxed);
            self.bytes = bytes;
            return Ok(());
        }
        let additional = bytes - self.bytes;
        let previous = self
            .budget
            .0
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(additional)
                    .filter(|total| *total <= self.budget.0.limit)
            })
            .map_err(|_| HelixDbError::QueryMemoryLimitExceeded)?;
        self.budget
            .0
            .peak
            .fetch_max(previous + additional, Ordering::Relaxed);
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.0.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

use bytes::Bytes;

struct ReadOwner {
    bytes: Bytes,
    _reservation: Reservation,
}

impl AsRef<[u8]> for ReadOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Budget {
    /// Retain a storage-returned value without copying its contents. Admission
    /// occurs before decoding or retaining it in an interpreter read result.
    /// The storage engine's shared caches and internal I/O buffers are separate
    /// from these request-owned references.
    pub(crate) fn retain_read(&self, bytes: Bytes) -> Result<Bytes> {
        // Besides the owner/refcount, reserve room for byte handles in a growing
        // read-result vector. Sharing or slicing the admitted Bytes keeps the
        // entire original read charged until its final reference is released.
        let reservation = self.reserve(
            bytes
                .len()
                .saturating_add(size_of::<ReadOwner>())
                .saturating_add(size_of::<std::sync::atomic::AtomicUsize>())
                .saturating_add(2 * size_of::<Bytes>()),
        )?;
        Ok(Bytes::from_owner(ReadOwner {
            bytes,
            _reservation: reservation,
        }))
    }
}

pub(crate) use crate::encoding::v2::values::indexes::equality::retained_allocation_estimate as bitmap_bytes;
