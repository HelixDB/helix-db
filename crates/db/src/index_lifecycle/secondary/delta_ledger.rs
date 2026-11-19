//! Process-local ledger of where each building secondary generation's
//! pending build deltas are.
//!
//! Delta rows are keyed by entity, and every reconciliation deletes its row,
//! so the delta prefix of a long build fills with tombstones that SlateDB
//! keeps until compaction. A scan from the start of the prefix walks every
//! one of them, and catch-up, validation and activation observe the backlog
//! on every step. A resume cursor alone cannot skip them: a writer records
//! the delta of an entity before any position a scan has passed.
//!
//! A ledger therefore holds the exact keys that may hold a pending delta in
//! the part of the prefix observed so far, and where the unobserved rest
//! begins. Observations verify those keys by point reads and scan only the
//! unobserved rest, so no tombstone an observation passed is scanned again.
//!
//! # Contract
//!
//! Every writer of a secondary build delta records its key here
//! ([`DeltaLedgers::record`]) after its commit has applied and before it
//! releases the scope's mutation permit: [`super::super::IndexScopeMutationPermit`]
//! records the deltas staged on it when dropped. Recording after the commit,
//! never before, is what lets an observation forget a key it saw absent (see
//! [`DeltaLedgers::observe`]). A delta committed before its generation's
//! ledger exists needs no record: the ledger begins with the whole prefix
//! unobserved. Writers record nothing unless a ledger exists, and build steps
//! only delete deltas, so they record nothing either.
//!
//! Under that contract, every pending delta of a generation is either among
//! its ledger's keys or in its unobserved rest, except a delta whose writer
//! still holds its mutation permit. A step holding the write fence or the
//! lifecycle permit excludes every such writer, so a complete observation
//! taken under either sees every pending delta.
//!
//! The ledger lives only in memory. A restarted process, or a generation
//! whose ledger was dropped, starts again from one scan of the whole prefix.
//! That includes an observation whose ledger is dropped while it verifies
//! the ledger's keys: the keys it had not verified left with the ledger, so
//! it starts again from one scan of the whole prefix rather than return part
//! of the backlog.
//!
//! # Memory
//!
//! A ledger lives from its generation's first observation until activation,
//! a block, an abort or cleanup drops it, and each key costs about two
//! hundred bytes. One ledger is dropped once it holds more than
//! [`LEDGER_CAPACITY`] keys, or an observation would take it past that, and
//! the largest are dropped while all of them together hold more than
//! [`LEDGERS_CAPACITY`], so a database handle never holds more than about
//! 50 MB of ledgers. Dropping one only costs its generation one scan of its
//! prefix. Dropped ledgers are freed after the lock is released, so writers
//! of other scopes never wait on freeing one.

use std::collections::{btree_map, BTreeMap, HashMap};
use std::ops::Bound;

use bytes::Bytes;

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::RecordKind;
use crate::error::Result;
use crate::index_lifecycle::{IndexGenerationId, IndexId};

/// Keys one generation's ledger may hold before it is dropped and the
/// generation falls back to one scan of its whole prefix.
///
/// Observations read at most a few batches past the reach they count to, and
/// verified keys leave the ledger once a drain deletes them, so a ledger
/// grows past this only while writes add deltas much faster than the build
/// drains them, or while its build waits for a worker.
pub(super) const LEDGER_CAPACITY: usize = 1 << 16;

/// Keys every ledger of one database handle may hold together before the
/// largest are dropped, about 50 MB.
pub(super) const LEDGERS_CAPACITY: usize = 4 * LEDGER_CAPACITY;

/// Most deltas any reach counts to, whatever the batch limits.
///
/// An observation adds the keys it scans to its ledger, so a reach past
/// [`LEDGER_CAPACITY`] would drop the ledger on every observation and make
/// each one rescan the prefix from its start. Large configured batches would
/// otherwise reach that far: eight batches of 8,192 entities.
const REACH_ROWS: usize = LEDGER_CAPACITY / 4;

/// Delta keys one multi-get verifies, and the most keys an observation
/// copies out of the ledgers per lock: writers of every scope record under
/// the same lock, so a large ledger is never copied in one hold.
const VERIFY_CHUNK: usize = 1024;

/// The building generation one ledger covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DeltaGeneration {
    pub(crate) scope: DataScope,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
}

/// Delta keys a writer staged in one transaction, recorded once it commits.
#[must_use = "staged build deltas must be recorded after their transaction commits"]
#[derive(Debug, Default)]
pub(crate) struct StagedBuildDeltas(pub(super) Vec<(DeltaGeneration, Bytes)>);

impl StagedBuildDeltas {
    /// Adds one staged delta key of `generation`.
    pub(crate) fn push(&mut self, generation: DeltaGeneration, key: Bytes) {
        self.0.push((generation, key));
    }

    /// Moves every key of `other` into this set.
    pub(crate) fn append(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }

    /// Whether no delta was staged.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Where the part of a delta prefix no observation has read begins.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Unobserved {
    /// Keys within this start bound are unobserved; `Unbounded` is the
    /// whole prefix.
    From(Bound<Bytes>),
    /// An observation read the prefix to its end.
    Nothing,
}

/// One generation's ledger.
#[derive(Debug)]
struct Ledger {
    /// Distinguishes this ledger from a later one of the same generation, so
    /// an observation begun on a dropped ledger changes nothing.
    id: u64,
    unobserved: Unobserved,
    /// Keys that may hold a pending delta, each with the sequence it was
    /// last recorded at.
    keys: BTreeMap<Bytes, u64>,
}

/// One observation between reading the ledger and applying what it read.
#[derive(Debug)]
struct Observation {
    generation: DeltaGeneration,
    ledger: u64,
    /// Keys recorded at or after this sequence were recorded after the
    /// observation began.
    began: u64,
    unobserved: Unobserved,
}

#[derive(Debug, Default)]
struct LedgerState {
    /// Orders records against observations; also numbers ledgers.
    next_sequence: u64,
    ledgers: HashMap<DeltaGeneration, Ledger>,
    /// Keys across every ledger; at most [`LEDGERS_CAPACITY`] between calls.
    keys: usize,
}

impl LedgerState {
    /// Takes the next sequence. A sequence is never handed out twice, so a
    /// ledger id never names two ledgers.
    fn sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .expect("a process never takes 2^64 ledger sequences");
        sequence
    }

    /// Takes `generation`'s ledger, if any, out of the state; the caller
    /// frees it once the lock is released.
    fn remove(&mut self, generation: &DeltaGeneration) -> Option<Ledger> {
        let ledger = self.ledgers.remove(generation)?;
        self.keys -= ledger.keys.len();
        Some(ledger)
    }

    /// Takes `generation`'s ledger out if it outgrew [`LEDGER_CAPACITY`],
    /// then the largest ledgers until all fit [`LEDGERS_CAPACITY`] together,
    /// into `dropped`, which the caller frees once the lock is released.
    fn bound(&mut self, generation: &DeltaGeneration, dropped: &mut Vec<Ledger>) {
        if self
            .ledgers
            .get(generation)
            .is_some_and(|ledger| ledger.keys.len() > LEDGER_CAPACITY)
        {
            dropped.extend(self.remove(generation));
        }
        while self.keys > LEDGERS_CAPACITY {
            let largest = *self
                .ledgers
                .iter()
                .max_by_key(|(_, ledger)| ledger.keys.len())
                .map(|(generation, _)| generation)
                .expect("ledgers hold every counted key");
            dropped.extend(self.remove(&largest));
        }
    }
}

/// Every building secondary generation's ledger in one process.
#[derive(Debug, Default)]
pub(crate) struct DeltaLedgers {
    state: parking_lot::Mutex<LedgerState>,
    /// Tests set a generation here to evict its ledger, by filling the
    /// handle's capacity with other ledgers, once an observation of it has
    /// verified one chunk of its keys.
    #[cfg(test)]
    pub(super) evict_after_chunk: parking_lot::Mutex<Option<DeltaGeneration>>,
    /// The most rows any observation returned, so tests can bound what an
    /// observation holds in memory.
    #[cfg(test)]
    pub(super) largest_observation: std::sync::atomic::AtomicUsize,
}

/// What [`DeltaLedgers::candidates`] found in an observation's ledger.
#[derive(Debug, PartialEq, Eq)]
enum Candidates {
    /// The next keys to verify, in key order; never empty.
    Chunk(Vec<Bytes>),
    /// The observation has verified every key of its ledger.
    Exhausted,
    /// The observation's ledger was dropped, and with it every key the
    /// observation had not verified yet.
    Dropped,
}

/// How much of the backlog an observation reads before it stops counting.
///
/// Production observations always stop at a reach, so one never holds more
/// than about that many batches of deltas in memory, however large the
/// backlog; a caller that needs every delta observes again after draining
/// what it read.
#[derive(Debug, Clone, Copy)]
pub(super) enum Reach {
    /// Stops once the deltas read exceed this many batches.
    Batches(usize),
    /// Reads every pending delta.
    #[cfg(test)]
    Complete,
}

/// The pending deltas one observation read, keyed by delta key.
#[derive(Debug, Default)]
pub(super) struct ObservedBacklog {
    pub(super) rows: BTreeMap<Bytes, Bytes>,
    /// Key and value bytes of `rows`.
    pub(super) input_bytes: u64,
}

impl ObservedBacklog {
    /// Whether the backlog is beyond `batches` batches of `limits`, counting
    /// at most [`REACH_ROWS`] deltas; exact for any `batches` up to the reach
    /// it was observed to, since a reach stops by this same measure.
    pub(super) fn exceeds(&self, limits: SearchIndexBatchLimits, batches: usize) -> bool {
        self.rows.len()
            > limits
                .max_entities()
                .get()
                .saturating_mul(batches)
                .min(REACH_ROWS)
            || self.input_bytes
                > limits
                    .max_input_bytes()
                    .get()
                    .saturating_mul(u64::try_from(batches).unwrap_or(u64::MAX))
    }

    fn insert(&mut self, key: Bytes, value: Bytes) {
        let bytes = u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX);
        if self.rows.insert(key, value).is_none() {
            self.input_bytes = self.input_bytes.saturating_add(bytes);
        }
    }

    fn reached(&self, limits: SearchIndexBatchLimits, reach: Reach) -> bool {
        match reach {
            Reach::Batches(batches) => self.exceeds(limits, batches),
            #[cfg(test)]
            Reach::Complete => false,
        }
    }
}

impl DeltaLedgers {
    /// Records committed delta keys in the ledgers of their generations.
    ///
    /// Call only once the transaction that staged them has committed or
    /// failed, while its mutation permit is still held (see the module
    /// contract). A key of a transaction that did not commit is harmless: an
    /// observation finds it absent and forgets it.
    pub(crate) fn record(&self, staged: StagedBuildDeltas) {
        if staged.is_empty() {
            return;
        }
        let mut dropped = Vec::new();
        let mut state = self.state.lock();
        let sequence = state.sequence();
        for (generation, key) in staged.0 {
            let LedgerState { ledgers, keys, .. } = &mut *state;
            let Some(ledger) = ledgers.get_mut(&generation) else {
                continue;
            };
            if ledger.keys.insert(key, sequence).is_none() {
                *keys += 1;
            }
            state.bound(&generation, &mut dropped);
        }
        drop(state);
        drop(dropped);
    }

    /// Drops `generation`'s ledger; its next observation scans the whole
    /// prefix again, and so does one under way (see [`Self::observe`]).
    pub(super) fn forget(&self, generation: DeltaGeneration) {
        let dropped = self.state.lock().remove(&generation);
        drop(dropped);
    }

    /// Observes `generation`'s pending deltas outside any transaction, until
    /// they exceed `reach` or all are read, and advances its ledger.
    ///
    /// Creates the ledger on first use. The ledger's keys are verified by
    /// point reads in key order, a chunk at a time, and the unobserved rest
    /// is scanned; deltas the scan reads join the ledger, which then covers
    /// the prefix through the last of them, or all of it once the scan
    /// reaches the end.
    ///
    /// The returned backlog is never cut short by its ledger being dropped
    /// (by [`Self::forget`] or by the capacity bounds) while the observation
    /// runs. Once verification has passed every key, a drop changes nothing
    /// the observation returns. Before that, the keys it had not verified are
    /// gone, so it discards what it read and starts again on a new ledger
    /// from one scan of the whole prefix, which needs no ledger to be
    /// complete. The restart happens at most once.
    pub(super) async fn observe(
        &self,
        db: &slatedb::Db,
        generation: DeltaGeneration,
        limits: SearchIndexBatchLimits,
        reach: Reach,
    ) -> Result<ObservedBacklog> {
        let mut observation = self.begin(generation);
        let mut observed = ObservedBacklog::default();
        let mut absent = Vec::new();
        let mut verified = None::<Bytes>;
        // Where the scan starts: the observation's unobserved rest, or the
        // whole prefix once its ledger was dropped.
        let mut unobserved = observation.unobserved.clone();
        while !observed.reached(limits, reach) {
            let chunk = match self.candidates(
                &observation,
                verified.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
            ) {
                Candidates::Chunk(chunk) => chunk,
                Candidates::Exhausted => break,
                Candidates::Dropped => {
                    observation = self.begin(generation);
                    observed = ObservedBacklog::default();
                    absent.clear();
                    unobserved = Unobserved::From(Bound::Unbounded);
                    break;
                }
            };
            let values = db.multi_get(&chunk).await?;
            verified = chunk.last().cloned();
            // Stops at the reach mid-chunk, so a chunk never takes the
            // observation past it by more than one delta.
            for (key, value) in chunk.into_iter().zip(values) {
                if observed.reached(limits, reach) {
                    break;
                }
                match value {
                    Some(value) => observed.insert(key, value),
                    None => absent.push(key),
                }
            }
            #[cfg(test)]
            {
                let evicted = self
                    .evict_after_chunk
                    .lock()
                    .take_if(|evicted| *evicted == generation);
                evicted
                    .into_iter()
                    .for_each(|evicted| self.crowd_out(evicted));
            }
        }
        let mut scanned = Vec::new();
        let mut advanced = observation.unobserved.clone();
        // Ledger keys verified to the reach make the scan unnecessary.
        if let Unobserved::From(start) = &unobserved
            && !observed.reached(limits, reach)
        {
            let prefix = super::generation_prefix(
                generation.scope,
                RecordKind::BuildDelta,
                generation.index_id,
                generation.generation,
            );
            // Scan bounds are suffixes of the prefix; every bound is a key a
            // scan of it returned.
            let suffix = |key: &Bytes| key.slice(prefix.len()..);
            let start = match start {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(key) => Bound::Included(suffix(key)),
                Bound::Excluded(key) => Bound::Excluded(suffix(key)),
            };
            let mut rows = db
                .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
                .await?;
            advanced = loop {
                let Some(row) = rows.next().await? else {
                    break Unobserved::Nothing;
                };
                scanned.push(row.key.clone());
                observed.insert(row.key.clone(), row.value);
                if observed.reached(limits, reach) {
                    break Unobserved::From(Bound::Excluded(row.key));
                }
            };
        }
        self.finish(observation, absent, scanned, advanced);
        #[cfg(test)]
        self.largest_observation
            .fetch_max(observed.rows.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(observed)
    }

    /// Opens an observation of `generation`, creating its ledger on first
    /// use, and notes where the scan of the unobserved rest starts.
    fn begin(&self, generation: DeltaGeneration) -> Observation {
        let mut state = self.state.lock();
        if !state.ledgers.contains_key(&generation) {
            let id = state.sequence();
            state.ledgers.insert(
                generation,
                Ledger {
                    id,
                    unobserved: Unobserved::From(Bound::Unbounded),
                    keys: BTreeMap::new(),
                },
            );
        }
        let began = state.sequence();
        let ledger = &state.ledgers[&generation];
        Observation {
            generation,
            ledger: ledger.id,
            began,
            unobserved: ledger.unobserved.clone(),
        }
    }

    /// The next [`VERIFY_CHUNK`] keys of `observation`'s ledger after
    /// `after`, in key order, or whether there are none left or the ledger
    /// was dropped since the observation began.
    ///
    /// The ledger is read as it stands, so keys recorded since the
    /// observation began are verified too, and keys another observation
    /// forgot are skipped: those were read absent after their last record.
    fn candidates(&self, observation: &Observation, after: Bound<&Bytes>) -> Candidates {
        let state = self.state.lock();
        let Some(ledger) = state
            .ledgers
            .get(&observation.generation)
            .filter(|ledger| ledger.id == observation.ledger)
        else {
            return Candidates::Dropped;
        };
        let chunk = ledger
            .keys
            .range::<Bytes, _>((after, Bound::Unbounded))
            .take(VERIFY_CHUNK)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            Candidates::Exhausted
        } else {
            Candidates::Chunk(chunk)
        }
    }

    /// Applies what `observation` read: the keys it read `absent`, the delta
    /// keys its scan returned, and where the unobserved rest now begins.
    ///
    /// A key read absent leaves the ledger unless it was recorded after the
    /// observation began. A writer records only after its commit applied, so
    /// a key recorded before the observation began was committed before the
    /// read: a delta read absent was deleted, and a write after the read is
    /// recorded again, after it. Concurrent observations stay sound: only one
    /// of them advances the unobserved rest from where it started, and the
    /// others add only keys that were present. An observation of a ledger
    /// since dropped changes nothing. A ledger the scanned keys could take
    /// past [`LEDGER_CAPACITY`] is dropped instead of grown.
    fn finish(
        &self,
        observation: Observation,
        absent: Vec<Bytes>,
        scanned: Vec<Bytes>,
        advanced: Unobserved,
    ) {
        let mut dropped = Vec::new();
        let mut state = self.state.lock();
        let LedgerState { ledgers, keys, .. } = &mut *state;
        let Some(current) = ledgers
            .get_mut(&observation.generation)
            .filter(|current| current.id == observation.ledger)
        else {
            return;
        };
        for key in absent {
            if current
                .keys
                .get(&key)
                .is_some_and(|recorded| *recorded < observation.began)
            {
                current.keys.remove(&key);
                *keys -= 1;
            }
        }
        // Counts scanned keys the ledger already holds too, so it may drop a
        // ledger that would just fit; that costs one scan of the prefix.
        if current.keys.len().saturating_add(scanned.len()) > LEDGER_CAPACITY {
            dropped.extend(state.remove(&observation.generation));
            drop(state);
            drop(dropped);
            return;
        }
        for key in scanned {
            let btree_map::Entry::Vacant(vacant) = current.keys.entry(key) else {
                continue;
            };
            vacant.insert(observation.began);
            *keys += 1;
        }
        if current.unobserved == observation.unobserved {
            current.unobserved = advanced;
        }
        state.bound(&observation.generation, &mut dropped);
        drop(state);
        drop(dropped);
    }

    /// Evicts `generation`'s ledger the way a busy handle does: records
    /// into other generations' ledgers, each smaller than it, until the
    /// handle's capacity bound drops it as the largest.
    #[cfg(test)]
    pub(super) fn crowd_out(&self, generation: DeltaGeneration) {
        let size = self
            .state
            .lock()
            .ledgers
            .get(&generation)
            .map(|ledger| ledger.keys.len())
            .expect("the crowded-out ledger exists");
        assert!(size > 1, "a crowded-out ledger holds more than one key");
        let filler = u64::try_from(size - 1).expect("fits");
        for other in 1_u64.. {
            if !self.state.lock().ledgers.contains_key(&generation) {
                return;
            }
            assert!(
                other <= u64::try_from(LEDGERS_CAPACITY).expect("fits"),
                "crowding out a ledger takes at most one ledger per key"
            );
            let other = DeltaGeneration {
                index_id: IndexId::new((1 << 40) + other).expect("positive index id"),
                ..generation
            };
            let observation = self.begin(other);
            self.finish(observation, Vec::new(), Vec::new(), Unobserved::Nothing);
            let mut staged = StagedBuildDeltas::default();
            (0..filler).for_each(|key| {
                staged.push(other, Bytes::copy_from_slice(&key.to_be_bytes()));
            });
            self.record(staged);
        }
    }

    /// The keys `generation`'s ledger holds and where its unobserved rest
    /// begins, or `None` without a ledger.
    #[cfg(test)]
    pub(super) fn snapshot(
        &self,
        generation: DeltaGeneration,
    ) -> Option<(Vec<Bytes>, Option<Bound<Bytes>>)> {
        self.state.lock().ledgers.get(&generation).map(|ledger| {
            (
                ledger.keys.keys().cloned().collect(),
                match &ledger.unobserved {
                    Unobserved::From(bound) => Some(bound.clone()),
                    Unobserved::Nothing => None,
                },
            )
        })
    }

    /// The keys every ledger holds, checked against the running count.
    #[cfg(test)]
    fn key_count(&self) -> usize {
        let state = self.state.lock();
        assert_eq!(
            state.keys,
            state
                .ledgers
                .values()
                .map(|ledger| ledger.keys.len())
                .sum::<usize>(),
            "the running key count matches the ledgers"
        );
        state.keys
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;
    use crate::config::SearchIndexBackfillLimits;
    use crate::encoding::v2::keys::{IndexEntity, IndexEntityStateKey, ScopedKey};
    use crate::index_lifecycle::{IndexElementKind, IndexEntityId};

    fn generation() -> DeltaGeneration {
        DeltaGeneration {
            scope: DataScope::LegacyUnscoped,
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
        }
    }

    fn delta_key(entity_id: u64) -> Bytes {
        let generation = generation();
        super::super::scoped_index_key(
            generation.scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id: generation.index_id,
                generation: generation.generation,
                entity: IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(entity_id),
                },
            }),
        )
    }

    async fn put_delta(db: &slatedb::Db, entity_id: u64) {
        db.put(delta_key(entity_id), Bytes::from_static(b"delta"))
            .await
            .expect("delta row is written");
    }

    async fn delete_delta(db: &slatedb::Db, entity_id: u64) {
        db.delete(delta_key(entity_id))
            .await
            .expect("delta row is deleted");
    }

    /// Writes (`Some`) or deletes (`None`) many delta rows in one batch, so
    /// a large backlog costs one commit rather than one per row.
    async fn write_deltas(
        db: &slatedb::Db,
        entity_ids: impl IntoIterator<Item = u64>,
        value: Option<&'static [u8]>,
    ) {
        let mut batch = slatedb::WriteBatch::new();
        for entity_id in entity_ids {
            match value {
                Some(value) => batch.put(delta_key(entity_id), value),
                None => batch.delete(delta_key(entity_id)),
            }
        }
        db.write(batch).await.expect("delta rows are written");
    }

    fn record(ledgers: &DeltaLedgers, entity_ids: impl IntoIterator<Item = u64>) {
        let mut staged = StagedBuildDeltas::default();
        for entity_id in entity_ids {
            staged.push(generation(), delta_key(entity_id));
        }
        ledgers.record(staged);
    }

    async fn observe(
        ledgers: &DeltaLedgers,
        db: &slatedb::Db,
        limits: SearchIndexBatchLimits,
        reach: Reach,
    ) -> Vec<Bytes> {
        ledgers
            .observe(db, generation(), limits, reach)
            .await
            .expect("backlog is observed")
            .rows
            .into_keys()
            .collect()
    }

    fn keys(entity_ids: impl IntoIterator<Item = u64>) -> Vec<Bytes> {
        entity_ids.into_iter().map(delta_key).collect()
    }

    fn one_entity_limits() -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).expect("positive"),
            NonZeroU64::new(32).expect("positive"),
            NonZeroU64::new(1024 * 1024).expect("positive"),
            NonZeroU64::new(1024 * 1024).expect("positive"),
        )
        .expect("single-entity limits are consistent")
    }

    /// Once an observation has read the prefix, later ones read only the
    /// ledger's keys: a delta committed behind it without a record (which
    /// the contract forbids) stays unseen, which shows no rescan, until the
    /// ledger is dropped and one full scan finds it.
    #[tokio::test]
    async fn observations_never_rescan_what_an_earlier_one_read() {
        let db = super::super::tests::test_db("delta-ledger-no-rescan").await;
        let ledgers = DeltaLedgers::default();
        let limits = SearchIndexBackfillLimits::default().batch();
        for entity_id in 1..=5 {
            put_delta(&db, entity_id).await;
        }
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys(1..=5)
        );
        assert_eq!(ledgers.snapshot(generation()), Some((keys(1..=5), None)));

        for entity_id in 1..=3 {
            delete_delta(&db, entity_id).await;
        }
        put_delta(&db, 0).await;
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys(4..=5),
            "reconciled deltas leave the ledger and nothing is rescanned"
        );
        assert_eq!(ledgers.snapshot(generation()), Some((keys(4..=5), None)));

        record(&ledgers, [0]);
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys([0, 4, 5]),
            "a recorded delta is observed wherever it lies"
        );

        put_delta(&db, 9).await;
        ledgers.forget(generation());
        assert_eq!(ledgers.snapshot(generation()), None);
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys([0, 4, 5, 9]),
            "a dropped ledger starts again from one full scan"
        );
        db.close().await.expect("test database closes");
    }

    /// An observation that stops at its reach covers the prefix only
    /// through the last delta it read; the next one verifies those and
    /// resumes the scan after them.
    #[tokio::test]
    async fn a_bounded_observation_resumes_after_the_last_delta_it_read() {
        let db = super::super::tests::test_db("delta-ledger-bounded").await;
        let ledgers = DeltaLedgers::default();
        let limits = one_entity_limits();
        for entity_id in 0..10 {
            put_delta(&db, entity_id).await;
        }
        let observed = ledgers
            .observe(&db, generation(), limits, Reach::Batches(2))
            .await
            .expect("backlog is observed");
        assert!(observed.exceeds(limits, 2));
        assert!(!observed.exceeds(limits, 3));
        assert_eq!(observed.rows.into_keys().collect::<Vec<_>>(), keys(0..3));
        assert_eq!(
            ledgers.snapshot(generation()),
            Some((keys(0..3), Some(Bound::Excluded(delta_key(2)))))
        );

        for entity_id in 0..3 {
            delete_delta(&db, entity_id).await;
        }
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Batches(2)).await,
            keys(3..6)
        );
        assert_eq!(
            ledgers.snapshot(generation()),
            Some((keys(3..6), Some(Bound::Excluded(delta_key(5)))))
        );
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys(3..10)
        );
        assert_eq!(ledgers.snapshot(generation()), Some((keys(3..10), None)));
        db.close().await.expect("test database closes");
    }

    /// A key recorded after an observation began may have been committed
    /// after that observation read it absent, so the observation keeps it;
    /// one recorded before is forgotten once read absent.
    #[tokio::test]
    async fn an_absent_read_forgets_only_keys_recorded_before_it_began() {
        let db = super::super::tests::test_db("delta-ledger-record-race").await;
        let ledgers = DeltaLedgers::default();
        let limits = SearchIndexBackfillLimits::default().batch();
        put_delta(&db, 1).await;
        put_delta(&db, 2).await;
        observe(&ledgers, &db, limits, Reach::Complete).await;

        let observation = ledgers.begin(generation());
        record(&ledgers, [2]);
        ledgers.finish(observation, keys([1, 2]), Vec::new(), Unobserved::Nothing);
        assert_eq!(
            ledgers.snapshot(generation()),
            Some((keys([2]), None)),
            "the key recorded mid-observation survives its absent read"
        );
        let observation = ledgers.begin(generation());
        ledgers.finish(observation, keys([2]), Vec::new(), Unobserved::Nothing);
        assert_eq!(ledgers.snapshot(generation()), Some((Vec::new(), None)));
        db.close().await.expect("test database closes");
    }

    /// Only one of two overlapping observations advances the unobserved
    /// rest, and an observation of a dropped ledger changes nothing, even
    /// when a new ledger of the same generation exists.
    #[test]
    fn overlapping_and_stale_observations_stay_sound() {
        let ledgers = DeltaLedgers::default();
        let first = ledgers.begin(generation());
        let second = ledgers.begin(generation());
        ledgers.finish(second, Vec::new(), keys([1]), Unobserved::Nothing);
        ledgers.finish(
            first,
            Vec::new(),
            keys([0]),
            Unobserved::From(Bound::Excluded(delta_key(0))),
        );
        assert_eq!(
            ledgers.snapshot(generation()),
            Some((keys([0, 1]), None)),
            "the later finish adds what it read but keeps the advanced rest"
        );

        let stale = ledgers.begin(generation());
        ledgers.forget(generation());
        let fresh = ledgers.begin(generation());
        ledgers.finish(stale, Vec::new(), keys([7]), Unobserved::Nothing);
        assert_eq!(
            ledgers.snapshot(generation()),
            Some((Vec::new(), Some(Bound::Unbounded)))
        );
        ledgers.finish(fresh, Vec::new(), Vec::new(), Unobserved::Nothing);
        assert_eq!(ledgers.snapshot(generation()), Some((Vec::new(), None)));
    }

    /// A writer's mutation permit records the deltas staged on it when it is
    /// released, and not before, so a step that takes the write fence finds
    /// every committed delta recorded.
    #[tokio::test]
    async fn a_mutation_permit_records_its_deltas_before_a_fence_is_granted() {
        let gates = std::sync::Arc::new(crate::index_lifecycle::IndexScopeGates::default());
        let ledgers = gates.delta_ledgers();
        let observation = ledgers.begin(generation());
        ledgers.finish(observation, Vec::new(), Vec::new(), Unobserved::Nothing);
        let mut permit = gates.mutation_permit(generation().scope).await;
        let mut staged = StagedBuildDeltas::default();
        staged.push(generation(), delta_key(3));
        permit.stage_build_deltas(staged);
        assert_eq!(ledgers.snapshot(generation()), Some((Vec::new(), None)));
        let fenced = tokio::spawn({
            let gates = std::sync::Arc::clone(&gates);
            async move {
                let _fence = gates.write_fence_permit(generation().scope).await;
                gates.delta_ledgers().snapshot(generation())
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!fenced.is_finished(), "the fence waits for the writer");
        drop(permit);
        assert_eq!(
            fenced.await.expect("fenced task joins"),
            Some((keys([3]), None))
        );
    }

    /// Records need a ledger: before the first observation the generation's
    /// first scan finds every delta. A ledger grown past its capacity is
    /// dropped instead of growing without bound.
    #[test]
    fn records_need_a_ledger_and_an_overfull_one_is_dropped() {
        let ledgers = DeltaLedgers::default();
        record(&ledgers, [1]);
        assert_eq!(ledgers.snapshot(generation()), None);

        let observation = ledgers.begin(generation());
        ledgers.finish(observation, Vec::new(), Vec::new(), Unobserved::Nothing);
        record(&ledgers, 0..u64::try_from(LEDGER_CAPACITY).expect("fits"));
        assert_eq!(
            ledgers.snapshot(generation()).map(|(keys, _)| keys.len()),
            Some(LEDGER_CAPACITY)
        );
        record(&ledgers, [u64::MAX]);
        assert_eq!(ledgers.snapshot(generation()), None);
        record(&ledgers, [2]);
        assert_eq!(ledgers.snapshot(generation()), None);
    }

    /// Observations copy ledger keys a chunk at a time, so writers of other
    /// scopes never wait on a copy of a whole ledger, and they verify every
    /// key of a ledger far larger than one chunk, including keys recorded
    /// after they began.
    #[tokio::test]
    async fn observations_copy_and_verify_large_ledgers_a_chunk_at_a_time() {
        let db = super::super::tests::test_db("delta-ledger-chunks").await;
        let ledgers = DeltaLedgers::default();
        let limits = SearchIndexBackfillLimits::default().batch();
        let entities = u64::try_from(2 * VERIFY_CHUNK + 5).expect("fits");
        let chunk = u64::try_from(VERIFY_CHUNK).expect("fits");
        write_deltas(&db, 0..entities, Some(b"delta")).await;
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys(0..entities)
        );

        let observation = ledgers.begin(generation());
        let Candidates::Chunk(first) = ledgers.candidates(&observation, Bound::Unbounded) else {
            panic!("a ledger with keys yields a chunk");
        };
        assert_eq!(first, keys(0..chunk), "one lock copies one chunk");
        record(&ledgers, [entities + 1]);
        let rest = std::iter::successors(Some(first), |chunk| {
            match ledgers.candidates(&observation, Bound::Excluded(chunk.last()?)) {
                Candidates::Chunk(next) => Some(next),
                Candidates::Exhausted => None,
                Candidates::Dropped => panic!("the ledger stays"),
            }
        })
        .skip(1)
        .collect::<Vec<_>>();
        assert!(rest.iter().all(|chunk| chunk.len() <= VERIFY_CHUNK));
        assert_eq!(
            rest.concat(),
            keys((chunk..entities).chain([entities + 1])),
            "a key recorded after the observation began is verified too"
        );
        ledgers.finish(observation, Vec::new(), Vec::new(), Unobserved::Nothing);

        write_deltas(&db, (0..entities).step_by(2), None).await;
        let pending = keys((1..entities).step_by(2));
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            pending
        );
        assert_eq!(ledgers.snapshot(generation()), Some((pending, None)));
        assert_eq!(ledgers.key_count(), VERIFY_CHUNK + 2);
        db.close().await.expect("test database closes");
    }

    /// An observation whose ledger is dropped before it has verified every
    /// key sees the drop, whether the ledger was crowded out, forgotten, or
    /// forgotten and created again, and never mistakes the drop for the end
    /// of its keys. Ledger ids are never reused.
    #[test]
    fn an_observation_sees_its_ledger_dropped() {
        let ledgers = DeltaLedgers::default();
        let observation = ledgers.begin(generation());
        ledgers.finish(observation, Vec::new(), Vec::new(), Unobserved::Nothing);
        let entities = u64::try_from(VERIFY_CHUNK + 1).expect("fits");
        record(&ledgers, 0..entities);
        let next = |observation: &Observation| {
            ledgers.candidates(observation, Bound::Excluded(&delta_key(0)))
        };

        let observation = ledgers.begin(generation());
        assert!(matches!(next(&observation), Candidates::Chunk(_)));
        ledgers.crowd_out(generation());
        assert_eq!(next(&observation), Candidates::Dropped);

        let observation = ledgers.begin(generation());
        assert_eq!(next(&observation), Candidates::Exhausted);
        ledgers.forget(generation());
        assert_eq!(next(&observation), Candidates::Dropped);

        let stale = ledgers.begin(generation());
        ledgers.forget(generation());
        let fresh = ledgers.begin(generation());
        assert_ne!(stale.ledger, fresh.ledger, "a new ledger gets a new id");
        assert_eq!(next(&stale), Candidates::Dropped);
        assert_eq!(next(&fresh), Candidates::Exhausted);
    }

    /// A complete observation whose ledger is evicted after it verified one
    /// chunk, of a ledger that had read the whole prefix, still returns
    /// every pending delta: it starts again from one scan of the prefix.
    /// Before, it returned the one chunk as the whole backlog.
    #[tokio::test]
    async fn a_complete_observation_survives_its_ledger_being_evicted() {
        let db = super::super::tests::test_db("delta-ledger-evicted").await;
        let ledgers = DeltaLedgers::default();
        let limits = SearchIndexBackfillLimits::default().batch();
        let entities = u64::try_from(2 * VERIFY_CHUNK + 5).expect("fits");
        write_deltas(&db, 0..entities, Some(b"delta")).await;
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys(0..entities)
        );
        assert_eq!(
            ledgers.snapshot(generation()),
            Some((keys(0..entities), None)),
            "the ledger read the whole prefix"
        );

        *ledgers.evict_after_chunk.lock() = Some(generation());
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await,
            keys(0..entities),
            "an evicted ledger never cuts the backlog short"
        );
        assert_eq!(
            *ledgers.evict_after_chunk.lock(),
            None,
            "the ledger was evicted"
        );

        // A bounded observation restarts the same way, from the start of
        // the prefix. A new handle holds no ledger the eviction filled.
        let one_entity = one_entity_limits();
        let ledgers = DeltaLedgers::default();
        observe(&ledgers, &db, limits, Reach::Complete).await;
        *ledgers.evict_after_chunk.lock() = Some(generation());
        let observed = ledgers
            .observe(
                &db,
                generation(),
                one_entity,
                Reach::Batches(VERIFY_CHUNK + 1),
            )
            .await
            .expect("backlog is observed");
        assert_eq!(
            *ledgers.evict_after_chunk.lock(),
            None,
            "the ledger was evicted"
        );
        assert!(observed.exceeds(one_entity, VERIFY_CHUNK + 1));
        assert_eq!(
            observed.rows.into_keys().collect::<Vec<_>>(),
            keys(0..=u64::try_from(VERIFY_CHUNK + 1).expect("fits"))
        );
        db.close().await.expect("test database closes");
    }

    /// An observation that would take its ledger past [`LEDGER_CAPACITY`]
    /// drops it rather than grow it, and still returns what it read.
    #[tokio::test]
    async fn an_observation_drops_a_ledger_it_would_overfill() {
        let db = super::super::tests::test_db("delta-ledger-overfill").await;
        let ledgers = DeltaLedgers::default();
        let limits = SearchIndexBackfillLimits::default().batch();
        let entities = u64::try_from(LEDGER_CAPACITY + 1).expect("fits");
        write_deltas(&db, 0..entities, Some(b"delta")).await;
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await.len(),
            LEDGER_CAPACITY + 1
        );
        assert_eq!(ledgers.snapshot(generation()), None);
        assert_eq!(ledgers.key_count(), 0);

        write_deltas(&db, [entities - 1], None).await;
        assert_eq!(
            observe(&ledgers, &db, limits, Reach::Complete).await.len(),
            LEDGER_CAPACITY
        );
        assert_eq!(ledgers.key_count(), LEDGER_CAPACITY);
        db.close().await.expect("test database closes");
    }

    /// Batches large enough that a reach would pass [`LEDGER_CAPACITY`]
    /// count to [`REACH_ROWS`] instead, so repeated bounded observations of
    /// a large backlog keep their ledger rather than drop it and rescan.
    #[tokio::test]
    async fn a_reach_of_large_batches_keeps_its_ledger() {
        let db = super::super::tests::test_db("delta-ledger-large-reach").await;
        let ledgers = DeltaLedgers::default();
        let limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(LEDGER_CAPACITY).expect("positive"),
            NonZeroU64::MAX,
            NonZeroU64::MAX,
            NonZeroU64::MAX,
            NonZeroU64::MAX,
        )
        .expect("large-batch limits are consistent");
        let entities = u64::try_from(LEDGER_CAPACITY + 1).expect("fits");
        write_deltas(&db, 0..entities, Some(b"delta")).await;
        let observed = ledgers
            .observe(&db, generation(), limits, Reach::Batches(8))
            .await
            .expect("backlog is observed");
        assert_eq!(observed.rows.len(), REACH_ROWS + 1);
        assert!(observed.exceeds(limits, 8));
        assert!(observed.exceeds(limits, 1));
        assert_eq!(
            ledgers.key_count(),
            REACH_ROWS + 1,
            "the ledger keeps what one reach read"
        );
        db.close().await.expect("test database closes");
    }

    /// All ledgers of one handle together stay within [`LEDGERS_CAPACITY`]:
    /// growing past it drops the largest ledger, whatever generation grew.
    #[test]
    fn every_ledger_together_stays_within_the_handle_capacity() {
        let ledgers = DeltaLedgers::default();
        let generations = (1..=5)
            .map(|index| DeltaGeneration {
                index_id: IndexId::new(index).expect("positive index id"),
                ..generation()
            })
            .collect::<Vec<_>>();
        for generation in &generations {
            let observation = ledgers.begin(*generation);
            ledgers.finish(observation, Vec::new(), Vec::new(), Unobserved::Nothing);
        }
        let fill = |generation: DeltaGeneration, entity_ids: std::ops::Range<u64>| {
            let mut staged = StagedBuildDeltas::default();
            entity_ids.for_each(|entity_id| staged.push(generation, delta_key(entity_id)));
            ledgers.record(staged);
        };
        let full = u64::try_from(LEDGER_CAPACITY).expect("fits");
        fill(generations[0], 0..full);
        fill(generations[1], 0..full - 1);
        fill(generations[2], 0..full - 1);
        fill(generations[3], 0..full - 1);
        fill(generations[4], 0..3);
        assert_eq!(ledgers.key_count(), LEDGERS_CAPACITY);
        assert!(generations
            .iter()
            .all(|generation| ledgers.snapshot(*generation).is_some()));

        fill(generations[4], 3..4);
        assert_eq!(
            ledgers.snapshot(generations[0]),
            None,
            "the largest ledger is dropped"
        );
        assert!(generations[1..]
            .iter()
            .all(|generation| ledgers.snapshot(*generation).is_some()));
        assert_eq!(ledgers.key_count(), LEDGERS_CAPACITY + 1 - LEDGER_CAPACITY);

        // Forgetting and observing keep the running count exact.
        ledgers.forget(generations[1]);
        let observation = ledgers.begin(generations[4]);
        ledgers.finish(observation, keys(0..2), Vec::new(), Unobserved::Nothing);
        assert_eq!(
            ledgers.key_count(),
            LEDGERS_CAPACITY + 1 - 2 * LEDGER_CAPACITY + 1 - 2
        );
    }
}
