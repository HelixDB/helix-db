//! Equivalence and read-cost contracts for lazily resolved BM25 live state.
//!
//! A manifest search must return exactly what an exhaustive search of every
//! split returns after dropping each candidate whose version is not live, for
//! legacy and V2 live state alike, while reading state only for candidates
//! that can still reach the top `k` and statistics markers only for the hits
//! it serves.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use proptest::prelude::*;
use slatedb::object_store::memory::InMemory;

use super::*;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::values as index_values;
use crate::index_lifecycle::text::serving;
use crate::index_lifecycle::text::statistics::TextBm25Statistics;

const QUERY: &str = "needle";
const DB_PATH: &str = "lazy-live-state";
/// Document bodies. Equal bodies score equally under the fixed corpus
/// statistics every search uses, so exact ties across splits are common.
/// The last body never matches the query.
const BODIES: [&str; 6] = [
    "needle",
    "needle needle",
    "needle pad",
    "needle pad pad pad",
    "needle needle pad pad pad pad pad",
    "pad",
];
const NON_MATCHING_BODY: usize = BODIES.len() - 1;
/// Entity IDs of filler documents that keep a split non-empty; they never
/// match the query, so they need no live state.
const FILLER_ENTITY: u64 = 1_000_000;
const NO_MARKER: &str = "live Active text entity has no statistics marker";
const FOREIGN_MARKER: &str = "live Active text entity disagrees with its statistics marker";

/// Live state of one entity in a generated model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelState {
    /// No legacy state row, so every version is live. V2 stores `Live(1)`.
    Missing,
    Live(u64),
    Dead(u64),
}

/// One document: entity ID, logical version, and index into [`BODIES`].
type ModelDocument = (u64, u64, usize);

#[derive(Debug, Clone)]
struct Model {
    splits: Vec<Vec<ModelDocument>>,
    states: BTreeMap<u64, ModelState>,
    restricted: Option<BTreeSet<u64>>,
}

impl Model {
    /// The state V2 stores for each entity: V2 has no stateless entities.
    fn v2_states(&self) -> BTreeMap<u64, ModelState> {
        self.states
            .iter()
            .map(|(entity_id, state)| {
                let state = match state {
                    ModelState::Missing => ModelState::Live(1),
                    ModelState::Live(version) => ModelState::Live(*version),
                    ModelState::Dead(version) => ModelState::Dead(*version),
                };
                (*entity_id, state)
            })
            .collect()
    }

    fn scope(&self) -> TextSearchScope {
        self.restricted
            .as_ref()
            .map_or(TextSearchScope::Unrestricted, |ids| {
                TextSearchScope::restricted(Arc::new(
                    RestrictedTextCandidates::from_ids(ids.iter().copied()).unwrap(),
                ))
            })
    }
}

/// Generates multi-split manifests with deletes, updates, stale and
/// unindexed versions, entities copied into several splits, splits without a
/// matching document, and optional traversal restriction.
fn model_strategy() -> impl Strategy<Value = Model> {
    let entity = (
        prop::collection::vec((0..BODIES.len(), any::<u8>()), 1..=3),
        0_u8..3,
        any::<u8>(),
        any::<bool>(),
    );
    (
        1_usize..=4,
        any::<bool>(),
        prop::collection::vec(entity, 1..=24),
        any::<bool>(),
    )
        .prop_map(|(split_count, empty_split, entities, restricted)| {
            let mut splits = vec![Vec::new(); split_count];
            let mut states = BTreeMap::new();
            let mut in_scope = BTreeSet::new();
            for (index, (versions, state_kind, state_version, scoped)) in
                entities.into_iter().enumerate()
            {
                let entity_id = index as u64 + 1;
                let state = match state_kind {
                    // One past the newest version is live but unindexed, so
                    // every indexed copy is stale.
                    0 => {
                        ModelState::Live(u64::from(state_version) % (versions.len() as u64 + 1) + 1)
                    }
                    1 => ModelState::Dead(u64::from(state_version) % versions.len() as u64 + 1),
                    _ => ModelState::Missing,
                };
                for (version_index, (body, placement)) in versions.iter().enumerate() {
                    // Every version of a stateless entity is live, so its
                    // copies must score equally to be consistent.
                    let body = if state == ModelState::Missing {
                        versions[0].0
                    } else {
                        *body
                    };
                    let mask = usize::from(*placement) % ((1 << split_count) - 1) + 1;
                    for (split_index, split) in splits.iter_mut().enumerate() {
                        if mask & (1 << split_index) != 0 {
                            split.push((entity_id, version_index as u64 + 1, body));
                        }
                    }
                }
                states.insert(entity_id, state);
                if scoped {
                    in_scope.insert(entity_id);
                }
            }
            if empty_split {
                splits.push(Vec::new());
            }
            for (split_index, split) in splits.iter_mut().enumerate() {
                if split.is_empty() {
                    split.push((FILLER_ENTITY + split_index as u64, 1, NON_MATCHING_BODY));
                }
            }
            Model {
                splits,
                states,
                restricted: restricted.then_some(in_scope),
            }
        })
}

/// Persisted splits, legacy and V2 state rows, and the V2 root of a model.
struct Fixture {
    store: Arc<dyn ObjectStore>,
    db: Db,
    index_name: String,
    manifest: TextIndexGenerationManifest,
    statistics: TextBm25Statistics,
    authority: serving::ActiveTextServingAuthority,
    root: serving::ValidatedActiveTextManifestRoot,
}

impl Fixture {
    async fn new(model: &Model) -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(DB_PATH, Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let index_name = resolve_physical_index_name(&definition, None).unwrap();
        let mut split_refs = Vec::new();
        for documents in &model.splits {
            let documents = documents
                .iter()
                .map(|(entity_id, version, body)| {
                    TextDocumentInput::new(*entity_id, BODIES[*body]).with_logical_version(*version)
                })
                .collect::<Vec<_>>();
            let manifest = persist_documents_as_manifest(
                &store,
                DB_PATH,
                &definition,
                &index_name,
                &documents,
            )
            .await
            .unwrap()
            .expect("every model split has a document");
            split_refs.push(manifest.primary_split_ref().clone());
        }
        let mut manifest = TextIndexGenerationManifest::new_split(
            index_name.clone(),
            "lazy-live-state",
            definition.analyzer(),
            definition.positions_enabled(),
            split_refs[0].clone(),
        );
        manifest.splits = split_refs;
        let term = analyze_text(definition.analyzer(), QUERY).unique_terms;
        let statistics = TextBm25Statistics::for_benchmark(
            1_000,
            3_000,
            BTreeMap::from([(term[0].clone(), 100)]),
        );

        let authority = active_authority();
        let partition = work::TextPartition::Unpartitioned;
        let root_key = index_keys::TextManifestRootKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.fingerprint(),
        };
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (entity_id, state) in &model.states {
            let state = match state {
                ModelState::Missing => continue,
                ModelState::Live(version) => TextIndexLiveState::live(*version),
                ModelState::Dead(version) => TextIndexLiveState::dead(*version),
            };
            transaction
                .put(
                    make_text_index_live_state_key_scoped(
                        DataScope::LegacyUnscoped,
                        &index_name,
                        *entity_id,
                    ),
                    Bytes::from(encode_live_state_bytes(&state).unwrap()),
                )
                .unwrap();
        }
        transaction
            .put(
                scoped(
                    &authority,
                    index_keys::ScopedKey::TextManifestRoot(root_key),
                ),
                index_values::encode_manifest_root(
                    &work::TextManifestRootValue::try_new(
                        authority.index_id(),
                        authority.generation(),
                        partition.clone(),
                        crate::index_lifecycle::TextManifestRevision::new(16).unwrap(),
                        1,
                        model.splits.len() as u64,
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        transaction
            .put(
                scoped(
                    &authority,
                    index_keys::ScopedKey::TextCorpusStatistics(
                        index_keys::TextCorpusStatisticsKey {
                            index_id: authority.index_id(),
                            generation: authority.generation(),
                            partition: partition.fingerprint(),
                        },
                    ),
                ),
                index_values::encode_corpus_statistics(
                    &work::TextCorpusStatisticsValue::try_new(
                        authority.index_id(),
                        authority.generation(),
                        partition.clone(),
                        1_000,
                        3_000,
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        for (entity_id, state) in model.v2_states() {
            let (version, live) = match state {
                ModelState::Live(version) => (version, true),
                ModelState::Dead(version) => (version, false),
                ModelState::Missing => unreachable!("V2 states are never missing"),
            };
            let entity = node_entity(entity_id);
            transaction
                .put(
                    scoped(
                        &authority,
                        index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                            root: root_key,
                            entity,
                        }),
                    ),
                    index_values::encode_text_entity_state(&work::TextEntityStateValue {
                        index_id: authority.index_id(),
                        generation: authority.generation(),
                        partition: partition.clone(),
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        logical_version: crate::index_lifecycle::TextLogicalVersion::new(version)
                            .unwrap(),
                        live,
                    }),
                )
                .unwrap();
            if live {
                transaction
                    .put(
                        marker_key(&authority, entity_id),
                        marker_value(
                            &authority,
                            entity_id,
                            present_contribution(partition.clone()),
                        ),
                    )
                    .unwrap();
            }
        }
        transaction.commit().await.unwrap();
        let root = serving::load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .expect("the unpartitioned root was written");
        Self {
            store,
            db,
            index_name,
            manifest,
            statistics,
            authority,
            root,
        }
    }

    /// Searches through `reader` with legacy (`v2 == false`) or V2 state.
    async fn search(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
        v2: bool,
        k: usize,
        scope: TextSearchScope,
    ) -> Result<Vec<TextSearchHit>, HelixDbError> {
        let state_source = if v2 {
            TextLiveStateSource::V2(&self.root)
        } else {
            TextLiveStateSource::Legacy {
                scope: DataScope::LegacyUnscoped,
                index_name: &self.index_name,
            }
        };
        search_manifest_with_state_source(
            reader,
            TextSearchRuntime::new(&self.store, DB_PATH, None),
            &self.manifest,
            state_source,
            Some(&self.statistics),
            TextSearchRequest::new(QUERY, k, scope),
        )
        .await
    }

    /// Reference result: every split searched exhaustively, every candidate
    /// checked against `states`, the best `k` live hits by [`HitRank`].
    async fn exhaustive(
        &self,
        states: &BTreeMap<u64, ModelState>,
        k: usize,
        scope: &TextSearchScope,
    ) -> Vec<TextSearchHit> {
        let mut live = BTreeMap::<u64, HitRank>::new();
        for split_ref in self.manifest.split_refs() {
            let runtime = TextSearchRuntime::new(&self.store, DB_PATH, None);
            let split = SplitSearchReader::open(&runtime, split_ref).await.unwrap();
            split.warm(self.manifest.analyzer, QUERY).await.unwrap();
            let candidates = split
                .search_candidates(
                    self.manifest.analyzer,
                    QUERY,
                    split.total_docs(),
                    Some(&self.statistics),
                    scope,
                )
                .unwrap();
            let ranks = candidates
                .iter()
                .map(|candidate| HitRank::new(candidate.score, candidate.entity_id))
                .collect::<Vec<_>>();
            assert!(
                ranks.windows(2).all(|pair| pair[0] >= pair[1]),
                "a split serves candidates in descending HitRank"
            );
            for (candidate, rank) in candidates.iter().zip(ranks) {
                let accepted = match states[&candidate.entity_id] {
                    ModelState::Missing => true,
                    ModelState::Live(version) => version == candidate.logical_version,
                    ModelState::Dead(_) => false,
                };
                if accepted && let Some(existing) = live.insert(candidate.entity_id, rank) {
                    assert_eq!(
                        existing, rank,
                        "model copies of a live version score equally"
                    );
                }
            }
        }
        let mut ranks = live.into_values().collect::<Vec<_>>();
        ranks.sort_unstable_by(|left, right| right.cmp(left));
        ranks
            .into_iter()
            .take(k)
            .map(|rank| TextSearchHit {
                entity_id: rank.entity_id.0,
                score: f32::from_bits(rank.score_bits),
            })
            .collect()
    }
}

/// Constructs family-refined authority for one Active node text definition.
fn active_authority() -> serving::ActiveTextServingAuthority {
    let definition = crate::index_lifecycle::ValidatedTextIndexDefinition::try_new(
        crate::index_lifecycle::IndexElementKind::Node,
        "Doc",
        "body",
        None::<String>,
        crate::config::TextAnalyzerKind::Standard,
        false,
    )
    .unwrap();
    let record = crate::index_lifecycle::IndexRecordV2::building(
        crate::index_lifecycle::IndexId::initial(),
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(definition),
        crate::index_lifecycle::IndexRevision::initial(),
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: crate::index_lifecycle::IndexGenerationId::initial(),
        },
        crate::index_lifecycle::IndexOperationId::new_v4(),
    )
    .unwrap()
    .transition(crate::index_lifecycle::IndexStateTransition::Activate)
    .unwrap();
    let active = crate::index_lifecycle::ActiveIndexHandle::try_from_record(
        DataScope::LegacyUnscoped,
        &record,
    )
    .unwrap();
    serving::ActiveTextServingAuthority::try_from_active(&active).unwrap()
}

fn scoped(authority: &serving::ActiveTextServingAuthority, key: index_keys::ScopedKey) -> Bytes {
    index_keys::ManagedIndexKey::Data {
        scope: authority.scope(),
        kind: key,
    }
    .to_bytes()
}

fn node_entity(entity_id: u64) -> index_keys::IndexEntity {
    index_keys::IndexEntity {
        kind: crate::index_lifecycle::IndexElementKind::Node,
        id: crate::index_lifecycle::IndexEntityId::new(entity_id),
    }
}

fn marker_key(authority: &serving::ActiveTextServingAuthority, entity_id: u64) -> Bytes {
    scoped(
        authority,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            entity: node_entity(entity_id),
        }),
    )
}

fn marker_value(
    authority: &serving::ActiveTextServingAuthority,
    entity_id: u64,
    contribution: work::TextStatisticsContribution,
) -> Bytes {
    let entity = node_entity(entity_id);
    index_values::encode_statistics_entity(&work::TextStatisticsEntityValue {
        index_id: authority.index_id(),
        generation: authority.generation(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        contribution,
    })
}

fn present_contribution(partition: work::TextPartition) -> work::TextStatisticsContribution {
    work::TextStatisticsContribution::try_present(
        partition,
        [0x31; 32],
        1,
        vec![Bytes::from_static(b"needl")],
    )
    .unwrap()
}

/// Reader that records the keys of every live-state `multi_get` and counts
/// point reads, which a search issues only for statistics markers.
struct CountingReader<'a> {
    inner: &'a Db,
    state_batches: Mutex<Vec<Vec<Vec<u8>>>>,
    point_reads: AtomicUsize,
}

impl<'a> CountingReader<'a> {
    fn new(inner: &'a Db) -> Self {
        Self {
            inner,
            state_batches: Mutex::new(Vec::new()),
            point_reads: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl DbReadOps for CountingReader<'_> {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_with_options(key, options).await
    }

    async fn multi_get_with_options<K>(
        &self,
        keys: &[K],
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error>
    where
        K: AsRef<[u8]> + Send + Sync,
    {
        self.state_batches
            .lock()
            .unwrap()
            .push(keys.iter().map(|key| key.as_ref().to_vec()).collect());
        self.inner.multi_get_with_options(keys, options).await
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        self.inner.get_key_value_with_options(key, options).await
    }

    async fn scan_with_options<T>(
        &self,
        range: T,
        options: &slatedb::config::ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error>
    where
        T: slatedb::ByteRangeBounds + Send,
    {
        self.inner.scan_with_options(range, options).await
    }
}

/// Runs one search through a fresh [`CountingReader`] and checks the read
/// invariants every search keeps: a state batch never exceeds the V2
/// loader's 512 bound, no entity's state is read twice, and a V2 search
/// point-reads exactly one statistics marker per served hit.
async fn counted_search(
    fixture: &Fixture,
    v2: bool,
    k: usize,
    scope: TextSearchScope,
) -> (Result<Vec<TextSearchHit>, HelixDbError>, Vec<usize>) {
    let reader = CountingReader::new(&fixture.db);
    let result = fixture.search(&reader, v2, k, scope).await;
    let batches = reader.state_batches.into_inner().unwrap();
    assert!(batches.iter().all(|batch| (1..=512).contains(&batch.len())));
    let keys = batches.iter().flatten().collect::<Vec<_>>();
    assert_eq!(
        keys.iter().collect::<BTreeSet<_>>().len(),
        keys.len(),
        "a search reads each entity's live state at most once"
    );
    if let Ok(hits) = &result {
        let expected_marker_reads = if v2 { hits.len() } else { 0 };
        assert_eq!(
            reader.point_reads.load(Ordering::Relaxed),
            expected_marker_reads
        );
    }
    (result, batches.iter().map(Vec::len).collect())
}

#[test]
fn hit_rank_orders_by_score_then_lower_entity() {
    assert!(HitRank::new(2.0, 9) > HitRank::new(1.0, 1));
    assert!(HitRank::new(1.0, 1) > HitRank::new(1.0, 2));
    assert_eq!(HitRank::new(1.5, 3), HitRank::new(1.5, 3));
    let scores = [
        0.0_f32,
        f32::MIN_POSITIVE,
        0.5,
        1.0,
        1.000_001,
        7.25,
        1e30,
        f32::MAX,
    ];
    for pair in scores.windows(2) {
        assert!(
            HitRank::new(pair[1], u64::MAX) > HitRank::new(pair[0], 0),
            "score bits order like non-negative scores"
        );
    }
}

/// Lazy resolution equals an exhaustive search for every `k` from 1 to past
/// the candidate count, under both live-state sources, and a V2 hit without
/// a statistics marker fails exactly when it is served.
#[test]
fn lazy_search_matches_an_exhaustive_search_of_every_split() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 48,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(&(model_strategy(), any::<prop::sample::Index>()), |(model, victim)| {
            runtime.block_on(async {
                let fixture = Fixture::new(&model).await;
                let scope = model.scope();
                let candidate_count = model.splits.iter().map(Vec::len).sum::<usize>();
                let v2_states = model.v2_states();
                for k in 1..=candidate_count + 2 {
                    let (legacy, _) = counted_search(&fixture, false, k, scope.clone()).await;
                    assert_eq!(
                        legacy.unwrap(),
                        fixture.exhaustive(&model.states, k, &scope).await,
                        "legacy k={k}"
                    );
                    let (v2, _) = counted_search(&fixture, true, k, scope.clone()).await;
                    assert_eq!(
                        v2.unwrap(),
                        fixture.exhaustive(&v2_states, k, &scope).await,
                        "V2 k={k}"
                    );
                }

                let live = v2_states
                    .iter()
                    .filter(|(_, state)| matches!(state, ModelState::Live(_)))
                    .map(|(entity_id, _)| *entity_id)
                    .collect::<Vec<_>>();
                if live.is_empty() {
                    return;
                }
                let victim = live[victim.index(live.len())];
                fixture
                    .db
                    .delete(marker_key(&fixture.authority, victim))
                    .await
                    .unwrap();
                for k in 1..=candidate_count + 2 {
                    let expected = fixture.exhaustive(&v2_states, k, &scope).await;
                    let (result, _) = counted_search(&fixture, true, k, scope.clone()).await;
                    if expected.iter().any(|hit| hit.entity_id == victim) {
                        assert!(
                            matches!(
                                &result,
                                Err(HelixDbError::IndexCatalogCorruption(reason)) if reason == NO_MARKER
                            ),
                            "a served hit without a marker fails: {result:?}"
                        );
                    } else {
                        assert_eq!(result.unwrap(), expected, "unserved victim k={k}");
                    }
                }
            });
            Ok(())
        })
        .unwrap();
}

/// One live document per entity: `split_sizes[i]` documents in split `i`.
fn all_live_model(split_sizes: &[usize]) -> Model {
    let mut next_entity = 0_u64;
    let splits = split_sizes
        .iter()
        .map(|size| {
            (0..*size)
                .map(|_| {
                    next_entity += 1;
                    (next_entity, 1, next_entity as usize % NON_MATCHING_BODY)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Model {
        states: (1..=next_entity)
            .map(|entity_id| (entity_id, ModelState::Live(1)))
            .collect(),
        splits,
        restricted: None,
    }
}

/// Twelve splits of twenty live hits each and `k = 10`: an eager search
/// read 120 states and 120 markers; the lazy one reads one minimum batch of
/// 64 states and only the 10 served hits' markers.
#[tokio::test]
async fn many_splits_resolve_one_batch_and_validate_only_served_markers() {
    let model = all_live_model(&[20; 12]);
    let fixture = Fixture::new(&model).await;
    for v2 in [false, true] {
        let (hits, batches) = counted_search(&fixture, v2, 10, TextSearchScope::Unrestricted).await;
        assert_eq!(
            hits.unwrap(),
            fixture
                .exhaustive(&model.states, 10, &TextSearchScope::Unrestricted)
                .await
        );
        assert_eq!(batches, [64], "v2={v2}");
    }
}

/// A batch resolves `k` minus the known hits, clamped to [64, 512], or every
/// remaining candidate when fewer are queued.
#[tokio::test]
async fn state_batches_are_clamped_between_64_and_512() {
    let model = all_live_model(&[700]);
    let fixture = Fixture::new(&model).await;
    for (k, expected) in [
        (1, vec![1]),
        (63, vec![63]),
        (64, vec![64]),
        (65, vec![65]),
        (512, vec![512]),
        (513, vec![512, 1]),
        (600, vec![512, 88]),
        (1_000, vec![512, 188]),
    ] {
        let reference = fixture
            .exhaustive(&model.states, k, &TextSearchScope::Unrestricted)
            .await;
        assert_eq!(reference.len(), k.min(700));
        for v2 in [false, true] {
            let (hits, batches) =
                counted_search(&fixture, v2, k, TextSearchScope::Unrestricted).await;
            assert_eq!(hits.unwrap(), reference, "k={k} v2={v2}");
            assert_eq!(batches, expected, "k={k} v2={v2}");
        }
    }
}

/// Dead and stale candidates above the live ones are resolved in rounds
/// until `k` live hits are known; when no candidate is live every candidate
/// is resolved once and nothing is served.
#[tokio::test]
async fn dead_and_stale_candidates_are_resolved_until_k_live_hits_exist() {
    let mut model = all_live_model(&[150, 150]);
    for (entity_id, state) in &mut model.states {
        *state = match entity_id % 3 {
            0 => ModelState::Live(1),
            1 => ModelState::Dead(1),
            _ => ModelState::Live(2),
        };
    }
    let fixture = Fixture::new(&model).await;
    let v2_states = model.v2_states();
    for k in [1, 5, 64, 99, 100, 101, 300] {
        for v2 in [false, true] {
            let states = if v2 { &v2_states } else { &model.states };
            let (hits, _) = counted_search(&fixture, v2, k, TextSearchScope::Unrestricted).await;
            assert_eq!(
                hits.unwrap(),
                fixture
                    .exhaustive(states, k, &TextSearchScope::Unrestricted)
                    .await,
                "k={k} v2={v2}"
            );
        }
    }

    let mut dead = all_live_model(&[40, 40, 40]);
    for state in dead.states.values_mut() {
        *state = ModelState::Dead(1);
    }
    let fixture = Fixture::new(&dead).await;
    for k in [1, 10, 120, 500] {
        for v2 in [false, true] {
            let (hits, batches) =
                counted_search(&fixture, v2, k, TextSearchScope::Unrestricted).await;
            assert!(hits.unwrap().is_empty(), "k={k} v2={v2}");
            assert_eq!(batches.iter().sum::<usize>(), 120, "k={k} v2={v2}");
        }
    }
}

/// A served hit's marker is validated with main's errors; an unserved live
/// candidate's marker is not read.
#[tokio::test]
async fn served_hit_markers_fail_closed_with_the_eager_errors() {
    let model = all_live_model(&[8, 8]);
    let fixture = Fixture::new(&model).await;
    let hits = fixture
        .exhaustive(&model.states, 16, &TextSearchScope::Unrestricted)
        .await;
    let worst = hits[15].entity_id;
    let tenant = work::TextPartition::try_tenant_value(Bytes::from_static(b"other")).unwrap();
    for (damage, expected) in [
        (None, Some(NO_MARKER)),
        (
            Some(marker_value(
                &fixture.authority,
                worst,
                work::TextStatisticsContribution::Absent,
            )),
            Some(FOREIGN_MARKER),
        ),
        (
            Some(marker_value(
                &fixture.authority,
                worst,
                present_contribution(tenant),
            )),
            Some(FOREIGN_MARKER),
        ),
        (Some(Bytes::from_static(b"\xff")), None),
    ] {
        let key = marker_key(&fixture.authority, worst);
        match &damage {
            Some(value) => fixture.db.put(&key, value.clone()).await.map(drop),
            None => fixture.db.delete(&key).await.map(drop),
        }
        .unwrap();
        let (served, _) = counted_search(&fixture, true, 16, TextSearchScope::Unrestricted).await;
        let served = served.expect_err("the damaged marker belongs to a served hit");
        let point = serving::load_active_entity_state(&fixture.db, &fixture.root, worst)
            .await
            .expect_err("a point load validates the same marker");
        assert_eq!(served.to_string(), point.to_string());
        if let Some(expected) = expected {
            assert!(matches!(
                served,
                HelixDbError::IndexCatalogCorruption(reason) if reason == expected
            ));
        }
        let (unserved, _) = counted_search(&fixture, true, 15, TextSearchScope::Unrestricted).await;
        assert_eq!(unserved.unwrap(), hits[..15]);
        let (legacy, _) = counted_search(&fixture, false, 16, TextSearchScope::Unrestricted).await;
        assert_eq!(legacy.unwrap(), hits);
        fixture
            .db
            .put(
                &key,
                marker_value(
                    &fixture.authority,
                    worst,
                    present_contribution(work::TextPartition::Unpartitioned),
                ),
            )
            .await
            .unwrap();
    }
}

/// Live copies of one version in several splits merge into one hit, and two
/// live copies with different scores are corruption once both can reach the
/// top `k`. A conflicting copy ranked below the `k`-th hit is never resolved,
/// so `k = 1` succeeds here; the eager search re-searched splits whose
/// frontier tied the `k`-th score and reported it at `k = 1` too.
#[tokio::test]
async fn live_copies_of_an_entity_merge_or_fail_on_different_scores() {
    let model = Model {
        splits: vec![vec![(1, 1, 0), (2, 1, 2)], vec![(1, 1, 0), (2, 2, 3)]],
        states: BTreeMap::from([(1, ModelState::Live(1)), (2, ModelState::Missing)]),
        restricted: None,
    };
    let fixture = Fixture::new(&model).await;
    let (merged, batches) = counted_search(&fixture, false, 1, TextSearchScope::Unrestricted).await;
    assert_eq!(
        merged
            .unwrap()
            .into_iter()
            .map(|hit| hit.entity_id)
            .collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(batches, [1], "both copies of entity 1 share one state read");
    let (conflict, _) = counted_search(&fixture, false, 2, TextSearchScope::Unrestricted).await;
    assert!(matches!(
        conflict,
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason == "duplicate live text versions have different BM25 score bits"
    ));
}
