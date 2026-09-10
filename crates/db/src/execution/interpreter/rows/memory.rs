//! Query-owned admission accounting. Reservations follow buffers across moves
//! and release on errors, cancellation and normal destruction.
mod raw;

use super::{resource, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
pub(in crate::execution::interpreter) struct Budget(Arc<State>);
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
    pub(in crate::execution::interpreter) fn reserve(&self, bytes: usize) -> Result<Reservation> {
        let mut reservation = Reservation {
            budget: self.clone(),
            bytes: 0,
        };
        reservation.resize(bytes)?;
        Ok(reservation)
    }
    pub(super) fn available(&self) -> usize {
        self.0
            .limit
            .saturating_sub(self.0.used.load(Ordering::Relaxed))
    }
    pub(super) fn peak(&self) -> usize {
        self.0.peak.load(Ordering::Relaxed)
    }

    pub(in crate::execution::interpreter) fn record_reads(
        &self,
        reads: crate::cypher::StorageReadUsage,
    ) {
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

    pub(in crate::execution::interpreter) fn reads(&self) -> crate::cypher::StorageReadUsage {
        crate::cypher::StorageReadUsage {
            point_gets: self.0.point_gets.load(Ordering::Relaxed),
            multi_get_batches: self.0.multi_get_batches.load(Ordering::Relaxed),
            multi_get_keys: self.0.multi_get_keys.load(Ordering::Relaxed),
            scans: self.0.scans.load(Ordering::Relaxed),
            scan_rows: self.0.scan_rows.load(Ordering::Relaxed),
        }
    }
}

pub(in crate::execution::interpreter) struct Reservation {
    budget: Budget,
    bytes: usize,
}
impl Reservation {
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
            .map_err(|_| resource("MemoryLimit", "query live buffers exceed the memory budget"))?;
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

/// A relation and its admission reservation are moved together. Consumers may
/// mutate rows, but must call `refresh` before retaining an enlarged relation.
pub(super) struct Rows {
    pub data: Vec<super::r::Row>,
    pub(super) reservation: Reservation,
}
impl Rows {
    pub fn new(data: Vec<super::r::Row>, budget: &Budget) -> Result<Self> {
        let mut rows = Self {
            data,
            reservation: budget.reserve(0)?,
        };
        rows.refresh()?;
        Ok(rows)
    }
    pub fn refresh(&mut self) -> Result<()> {
        self.reservation.resize(
            super::rows_bytes(&self.data).saturating_add(
                self.data
                    .capacity()
                    .saturating_sub(self.data.len())
                    .saturating_mul(size_of::<super::r::Row>()),
            ),
        )
    }
}
impl std::ops::Deref for Rows {
    type Target = Vec<super::r::Row>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl std::ops::DerefMut for Rows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_admission_releases_on_error_and_move() {
        let budget = Budget::new(100);
        let mut first = budget.reserve(60).unwrap();
        assert!(budget.reserve(41).is_err());
        let second = budget.reserve(40).unwrap();
        assert_eq!(budget.peak(), 100);
        assert!(first.resize(61).is_err());
        drop(second);
        first.resize(100).unwrap();
        first.resize(0).unwrap();
        let moved = budget.reserve(100).unwrap();
        drop(moved);
        assert!(budget.reserve(usize::MAX).is_err());
        assert_eq!(budget.0.used.load(Ordering::Relaxed), 0);
    }
}

pub(super) struct IntoRows {
    iter: std::vec::IntoIter<super::r::Row>,
    _reservation: Reservation,
}
impl IntoRows {
    /// Borrow the remaining admitted input for property hydration before moving
    /// individual rows into an expansion cursor.
    pub fn as_slice(&self) -> &[super::r::Row] {
        self.iter.as_slice()
    }
}
impl Iterator for IntoRows {
    type Item = super::r::Row;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}
impl IntoIterator for Rows {
    type Item = super::r::Row;
    type IntoIter = IntoRows;
    fn into_iter(self) -> IntoRows {
        IntoRows {
            iter: self.data.into_iter(),
            _reservation: self.reservation,
        }
    }
}
impl<'a> IntoIterator for &'a Rows {
    type Item = &'a super::r::Row;
    type IntoIter = std::slice::Iter<'a, super::r::Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}
impl<'a> IntoIterator for &'a mut Rows {
    type Item = &'a mut super::r::Row;
    type IntoIter = std::slice::IterMut<'a, super::r::Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.data.iter_mut()
    }
}

/// Conservative retained allocation of a compressed ID bitmap, including
/// container capacity and treemap/vector headers, shared by graph cursors.
pub(super) fn bitmap_bytes(bitmap: &roaring::RoaringTreemap) -> usize {
    bitmap.bitmaps().fold(
        size_of::<roaring::RoaringTreemap>(),
        |bytes, (_, bitmap)| {
            let statistics = bitmap.statistics();
            let payload = statistics
                .n_bytes_array_containers
                .saturating_add(statistics.n_bytes_bitset_containers)
                .saturating_add(statistics.n_bytes_run_containers);
            bytes
                .saturating_add(usize::try_from(payload).unwrap_or(usize::MAX))
                .saturating_add((statistics.n_containers as usize).saturating_mul(256))
                .saturating_add(512)
        },
    )
}
