//! Bounded raw-value reuse for one immutable request view. Values keep their
//! own compact copy and admission; eviction never invalidates an outstanding clone.
//! No decoded values, errors, database handles, or write transactions are held.
use crate::query_resources::{self, Budget, Reservation};
use bytes::Bytes;
use helix_planner::relational::allocation;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

#[cfg(test)]
mod tests;

pub(in crate::execution::interpreter) struct Cache {
    state: parking_lot::Mutex<State>,
    _header: Reservation,
}

struct State {
    entries: BTreeMap<Bytes, Option<Bytes>>,
    order: VecDeque<Bytes>,
    payload_bytes: usize,
    value_weight: usize,
    limit: usize,
    memory: Reservation,
    budget: Budget,
}

impl Cache {
    pub(in crate::execution::interpreter) fn new(
        budget: &Budget,
        limit: usize,
    ) -> Option<Arc<Self>> {
        // Tiny queries should spend their allowance on required execution state.
        if limit < 128 * 1024 {
            return None;
        }
        let header = budget
            .reserve(size_of::<Self>() + 2 * size_of::<usize>())
            .ok()?;
        let cache = Arc::new(Self {
            state: parking_lot::Mutex::new(State {
                entries: BTreeMap::new(),
                order: VecDeque::new(),
                payload_bytes: 0,
                value_weight: 0,
                limit,
                memory: budget.reserve(0).expect("empty reservation fits"),
                budget: budget.clone(),
            }),
            _header: header,
        });
        let reclaim: Arc<dyn query_resources::Reclaim> = cache.clone();
        budget.set_reclaimer(Arc::downgrade(&reclaim));
        Some(cache)
    }

    /// Outer None means a miss; inner None is a cached absent storage value.
    pub(in crate::execution::interpreter) fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        self.state.lock().entries.get(key).cloned()
    }

    pub(in crate::execution::interpreter) fn insert(&self, key: &[u8], value: &Option<Bytes>) {
        let mut state = self.state.lock();
        if state.entries.contains_key(key) {
            return;
        }
        // A cache admission failure cannot turn a successful storage read into
        // a query failure. Release optional state; the caller owns its result.
        if state.insert(key, value).is_err() {
            state.clear();
        }
    }
}

impl query_resources::Reclaim for Cache {
    fn reclaim(&self) {
        // Admission may originate inside insert while this lock is held.
        // That insertion falls back to clear on failure instead of deadlocking.
        let Some(mut state) = self.state.try_lock() else {
            return;
        };
        state.clear();
    }
}

impl State {
    fn clear(&mut self) {
        self.entries = BTreeMap::new();
        self.order = VecDeque::new();
        self.payload_bytes = 0;
        self.value_weight = 0;
        self.memory.shrink_to(0);
    }

    fn insert(&mut self, key: &[u8], value: &Option<Bytes>) -> crate::Result<()> {
        // Keys and values own compact copies. The eviction weight includes the
        // independently admitted value owner without charging that owner twice.
        let key_bytes = key.len().saturating_add(64);
        let value_weight = value.as_ref().map_or(0, |v| v.len().saturating_add(256));
        if key_bytes.saturating_add(value_weight) > self.limit / 4 {
            return Ok(());
        }
        loop {
            let count = self.entries.len().saturating_add(1);
            let capacity = if self.order.len() == self.order.capacity() {
                self.order.capacity().saturating_mul(2).max(4)
            } else {
                self.order.capacity()
            };
            let containers = allocation::btree_bytes::<Bytes, Option<Bytes>>(count)
                .saturating_add(capacity.saturating_mul(2 * size_of::<Bytes>()));
            let owned = containers
                .saturating_add(self.payload_bytes)
                .saturating_add(key_bytes);
            if owned
                .saturating_add(self.value_weight)
                .saturating_add(value_weight)
                <= self.limit
            {
                self.memory.resize(owned)?;
                let value = value
                    .as_deref()
                    .map(|value| self.budget.copy_read(value))
                    .transpose()?;
                self.order.reserve(1);
                assert!(self.order.capacity() <= capacity, "admitted queue growth");
                let key = Bytes::copy_from_slice(key);
                self.order.push_back(key.clone());
                self.entries.insert(key, value);
                self.payload_bytes += key_bytes;
                self.value_weight += value_weight;
                return Ok(());
            }
            // FIFO bounds bookkeeping and avoids allocating/reordering on hits.
            // Each key appears once in the queue and the lookup tree.
            let Some(oldest) = self.order.pop_front() else {
                return Ok(());
            };
            let removed = self.entries.remove(&oldest).expect("queued key exists");
            self.payload_bytes -= oldest.len() + 64;
            self.value_weight -= removed.as_ref().map_or(0, |v| v.len() + 256);
        }
    }
}
