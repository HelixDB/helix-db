//! Bounds for the pinned backend's serializable read dependencies.
//!
//! SlateDB retains unique point keys, but extending its hash set may reserve
//! before deduplicating. Track that capacity high-water mark separately from
//! unique payload. Every requested scan range survives, including empty scans.
//! Bounds include the backend's table/range clones at commit; shared storage
//! caches, recent-commit history and iterator working buffers are separate.

use std::{collections::HashSet, ops::Bound};

use bytes::Bytes;
use helix_planner::relational::allocation;
use parking_lot::Mutex;

use super::AdmissionFailure;

type Result<T> = std::result::Result<T, AdmissionFailure>;
use crate::query_resources::{Budget, Reservation};

#[cfg(test)]
mod tests;

pub(super) struct Tracker {
    pub(super) budget: Budget,
    state: Mutex<State>,
}

struct State {
    keys: HashSet<Box<[u8]>>,
    key_payload: usize,
    backend_table: usize,
    ranges: usize,
    range_payload: usize,
    memory: Reservation,
}

impl Tracker {
    /// Admit the owning box before it is allocated by the transaction owner.
    pub(super) fn new(budget: &Budget) -> Result<Self> {
        let memory = admit(budget.reserve(size_of::<Self>()))?;
        Ok(Self {
            budget: budget.clone(),
            state: Mutex::new(State {
                keys: HashSet::new(),
                key_payload: 0,
                backend_table: 0,
                ranges: 0,
                range_payload: 0,
                memory,
            }),
        })
    }

    fn point(&self, key: &[u8]) -> Result<Reservation> {
        let temporary = admit(
            self.budget.reserve(
                key.len()
                    .saturating_add(allocation::hash_table_bytes::<Bytes, ()>(1)),
            ),
        )?;
        self.retain_keys(std::iter::once(key), 1)?;
        Ok(temporary)
    }

    pub(super) fn keys<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<Reservation> {
        let [key] = keys else {
            // Both our borrowed deduplication table and the backend's temporary
            // key set are admitted before construction. The backend may reserve
            // from the original batch length even when every key is identical.
            let payload = keys.iter().try_fold(0usize, |bytes, key| {
                bytes
                    .checked_add(key.as_ref().len())
                    .ok_or(AdmissionFailure)
            })?;
            let temporary = admit(
                self.budget.reserve(
                    payload
                        .saturating_add(allocation::hash_table_bytes::<&[u8], ()>(keys.len()))
                        .saturating_add(allocation::hash_table_bytes::<Bytes, ()>(keys.len())),
                ),
            )?;
            let unique: HashSet<&[u8]> = keys.iter().map(AsRef::as_ref).collect();
            self.retain_keys(unique.iter().copied(), unique.len())?;
            return Ok(temporary);
        };
        self.point(key.as_ref())
    }

    fn retain_keys<'a>(
        &self,
        unique: impl Iterator<Item = &'a [u8]> + Clone,
        incoming: usize,
    ) -> Result<()> {
        let mut state = self.state.lock();
        let (mut count, mut payload) = (state.keys.len(), state.key_payload);
        for key in unique.clone().filter(|key| !state.keys.contains(*key)) {
            count = count.checked_add(1).ok_or(AdmissionFailure)?;
            payload = payload.checked_add(key.len()).ok_or(AdmissionFailure)?;
        }
        // HashSet::extend reserves against the pre-insertion length and the
        // incoming unique batch, even when those keys already exist.
        let proposed = state
            .keys
            .len()
            .checked_add(incoming)
            .ok_or(AdmissionFailure)?;
        let backend_table = state
            .backend_table
            .max(allocation::hash_table_retained_bytes::<Bytes, ()>(proposed));
        let bytes = State::bytes(
            count,
            payload,
            backend_table,
            state.ranges,
            state.range_payload,
        )?;
        admit(state.memory.resize(bytes))?;
        for key in unique {
            if !state.keys.contains(key) {
                state.keys.insert(key.into());
            }
        }
        state.key_payload = payload;
        state.backend_table = backend_table;
        Ok(())
    }

    pub(super) fn range<T: slatedb::ByteRangeBounds>(
        &self,
        range: &T,
        prefix_len: Option<usize>,
    ) -> Result<Reservation> {
        let suffix_payload = [range.start_bound(), range.end_bound()]
            .into_iter()
            .try_fold(0usize, |bytes, bound| {
                let length = match bound {
                    Bound::Included(bytes) | Bound::Excluded(bytes) => bytes.len(),
                    Bound::Unbounded => 0,
                };
                bytes.checked_add(length).ok_or(AdmissionFailure)
            })?;
        // Prefix scans construct full bounds internally. Counting two copies
        // of the prefix also bounds an unbounded suffix's successor endpoint;
        // an all-0xff prefix may need less, but never more.
        let prefix = prefix_len.unwrap_or(0);
        let payload = prefix
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(suffix_payload))
            .ok_or(AdmissionFailure)?;
        let temporary = admit(
            self.budget
                .reserve(payload.saturating_mul(3).saturating_add(prefix)),
        )?;
        let mut state = self.state.lock();
        let ranges = state.ranges.checked_add(1).ok_or(AdmissionFailure)?;
        let range_payload = state
            .range_payload
            .checked_add(payload)
            .ok_or(AdmissionFailure)?;
        let bytes = State::bytes(
            state.keys.len(),
            state.key_payload,
            state.backend_table,
            ranges,
            range_payload,
        )?;
        admit(state.memory.resize(bytes))?;
        state.ranges = ranges;
        state.range_payload = range_payload;
        Ok(temporary)
    }

    pub(super) fn mark_read<K: AsRef<[u8]>, I: IntoIterator<Item = K>>(
        &self,
        raw: &slatedb::DbTransaction,
        keys: I,
    ) -> std::result::Result<(), slatedb::Error> {
        let mut keys = keys.into_iter();
        let mut failed = None;
        let mut memory = None;
        // from_fn has no lower size hint. The backend cannot reserve a batch
        // table before these per-key admissions. It still takes one manager
        // lock and performs the same explicit dependency registration.
        let admitted = std::iter::from_fn(|| {
            if failed.is_some() {
                return None;
            }
            memory = None;
            let key = keys.next()?;
            match self.point(key.as_ref()) {
                Ok(reservation) => {
                    memory = Some(reservation);
                    Some(key)
                }
                Err(error) => {
                    failed = Some(error);
                    None
                }
            }
        });
        raw.mark_read(admitted)?;
        match failed {
            Some(error) => Err(super::storage_error(error)),
            None => Ok(()),
        }
    }
}

impl State {
    fn bytes(
        keys: usize,
        payload: usize,
        backend_table: usize,
        ranges: usize,
        range_payload: usize,
    ) -> Result<usize> {
        // Two shadow tables cover rehash overlap. Two backend tables cover
        // either rehash or TransactionState's commit-time clone. Byte payload
        // is copied once by each owner; backend Bytes may acquire shared owners
        // when the commit clone promotes them.
        let shadow = allocation::hash_table_bytes::<Box<[u8]>, ()>(keys);
        let range_capacity = if ranges == 0 {
            0
        } else {
            ranges
                .checked_next_power_of_two()
                .ok_or(AdmissionFailure)?
                .max(4)
        };
        let tracker_and_commit_ranges = range_capacity
            .checked_mul(3 * 2 * size_of::<Bound<Bytes>>())
            .ok_or(AdmissionFailure)?;
        [
            Some(size_of::<Tracker>()),
            Some(shadow),
            backend_table.checked_mul(2),
            payload.checked_mul(2),
            keys.checked_mul(3 * size_of::<usize>()),
            Some(range_payload),
            ranges.checked_mul(6 * size_of::<usize>()),
            Some(tracker_and_commit_ranges),
        ]
        .into_iter()
        .try_fold(0usize, |total, part| {
            total.checked_add(part?).filter(|bytes| *bytes < usize::MAX)
        })
        .ok_or(AdmissionFailure)
    }
}

/// The shared allocator currently has one refusal type. Keep that contract
/// explicit instead of carrying unrelated database errors through this ledger.
fn admit<T>(result: crate::error::Result<T>) -> Result<T> {
    result.map_err(|error| {
        assert!(
            matches!(error, crate::HelixDbError::QueryMemoryLimitExceeded),
            "budget admission returned an unrelated database error"
        );
        AdmissionFailure
    })
}
