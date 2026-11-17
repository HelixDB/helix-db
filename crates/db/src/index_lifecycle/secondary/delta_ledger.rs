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

use std::collections::{BTreeMap, HashMap};
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
/// drains them. Each key costs about a hundred bytes.
pub(super) const LEDGER_CAPACITY: usize = 1 << 16;

/// Delta keys one multi-get verifies.
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
    candidates: Vec<Bytes>,
    unobserved: Unobserved,
}

#[derive(Debug, Default)]
struct LedgerState {
    /// Orders records against observations; also numbers ledgers.
    next_sequence: u64,
    ledgers: HashMap<DeltaGeneration, Ledger>,
}

/// Every building secondary generation's ledger in one process.
#[derive(Debug, Default)]
pub(crate) struct DeltaLedgers {
    state: parking_lot::Mutex<LedgerState>,
}

/// How much of the backlog an observation reads before it stops counting.
#[derive(Debug, Clone, Copy)]
pub(super) enum Reach {
    /// Stops once the deltas read exceed this many batches.
    Batches(usize),
    /// Reads every pending delta.
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
    /// Whether the backlog is beyond `batches` batches of `limits`; exact for
    /// any `batches` up to the reach it was observed to.
    pub(super) fn exceeds(&self, limits: SearchIndexBatchLimits, batches: usize) -> bool {
        self.rows.len() > limits.max_entities().get().saturating_mul(batches)
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
        let mut state = self.state.lock();
        let LedgerState {
            next_sequence,
            ledgers,
        } = &mut *state;
        let sequence = *next_sequence;
        *next_sequence = next_sequence.saturating_add(1);
        for (generation, key) in staged.0 {
            let Some(ledger) = ledgers.get_mut(&generation) else {
                continue;
            };
            ledger.keys.insert(key, sequence);
            if ledger.keys.len() > LEDGER_CAPACITY {
                ledgers.remove(&generation);
            }
        }
    }

    /// Drops `generation`'s ledger; its next observation scans the whole
    /// prefix again.
    pub(super) fn forget(&self, generation: DeltaGeneration) {
        self.state.lock().ledgers.remove(&generation);
    }

    /// Observes `generation`'s pending deltas outside any transaction, until
    /// they exceed `reach` or all are read, and advances its ledger.
    ///
    /// Creates the ledger on first use. The ledger's keys are verified by
    /// point reads in key order, and the unobserved rest is scanned; deltas
    /// the scan reads join the ledger, which then covers the prefix through
    /// the last of them, or all of it once the scan reaches the end.
    pub(super) async fn observe(
        &self,
        db: &slatedb::Db,
        generation: DeltaGeneration,
        limits: SearchIndexBatchLimits,
        reach: Reach,
    ) -> Result<ObservedBacklog> {
        let observation = self.begin(generation);
        let mut observed = ObservedBacklog::default();
        let mut absent = Vec::new();
        for chunk in observation.candidates.chunks(VERIFY_CHUNK) {
            if observed.reached(limits, reach) {
                break;
            }
            let values = db.multi_get(chunk).await?;
            for (key, value) in chunk.iter().zip(values) {
                match value {
                    Some(value) => observed.insert(key.clone(), value),
                    None => absent.push(key.clone()),
                }
            }
        }
        let mut scanned = Vec::new();
        let mut advanced = observation.unobserved.clone();
        // Ledger keys verified to the reach make the scan unnecessary.
        if let Unobserved::From(start) = &observation.unobserved
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
        Ok(observed)
    }

    /// Opens an observation of `generation`, creating its ledger on first
    /// use: the keys to verify, and where the scan of the rest starts.
    fn begin(&self, generation: DeltaGeneration) -> Observation {
        let mut state = self.state.lock();
        let LedgerState {
            next_sequence,
            ledgers,
        } = &mut *state;
        let ledger = ledgers.entry(generation).or_insert_with(|| {
            let id = *next_sequence;
            *next_sequence = next_sequence.saturating_add(1);
            Ledger {
                id,
                unobserved: Unobserved::From(Bound::Unbounded),
                keys: BTreeMap::new(),
            }
        });
        let began = *next_sequence;
        *next_sequence = next_sequence.saturating_add(1);
        Observation {
            generation,
            ledger: ledger.id,
            began,
            candidates: ledger.keys.keys().cloned().collect(),
            unobserved: ledger.unobserved.clone(),
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
    /// since dropped changes nothing.
    fn finish(
        &self,
        observation: Observation,
        absent: Vec<Bytes>,
        scanned: Vec<Bytes>,
        advanced: Unobserved,
    ) {
        let mut state = self.state.lock();
        let LedgerState { ledgers, .. } = &mut *state;
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
            }
        }
        for key in scanned {
            current.keys.entry(key).or_insert(observation.began);
        }
        if current.unobserved == observation.unobserved {
            current.unobserved = advanced;
        }
        if current.keys.len() > LEDGER_CAPACITY {
            ledgers.remove(&observation.generation);
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
}
