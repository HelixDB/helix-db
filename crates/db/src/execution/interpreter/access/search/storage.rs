//! Physical search storage call contracts.

#[cfg(test)]
use std::cmp::Ordering;
#[cfg(test)]
use std::collections::BTreeMap;

use futures::StreamExt;

use super::generation::{
    ResolvedTextGenerationHandle, ResolvedVectorGenerationHandle, TextSearchAuthority,
    VectorSearchAuthority,
};
use super::*;
use crate::search;
#[cfg(test)]
use crate::search::vector::ValidatedVectorGenerationHandle;
use crate::search::vector::{
    Distance, RestrictedVectorCandidates, SearchParams, ValidatedVectorReadIndex, VectorReadView,
    VectorReadVisibility,
};
#[cfg(test)]
use crate::HelixStorage;
use slatedb::DbReadOps;

/// A checked V2 text root inseparably paired with its resolved generation.
///
/// Private fields prevent a root loaded under one Active generation from being
/// searched with another generation's canonical definition.
pub(super) struct ResolvedTextManifestRoot<'generation> {
    generation: &'generation ResolvedTextGenerationHandle,
    root: crate::index_lifecycle::text::serving::ValidatedActiveTextManifestRoot,
}

impl<'db> ExecutionContext<'db> {
    /// Searches one physical vector index through its validated generation.
    ///
    /// Managed callers must supply a handle so descriptor validation and the
    /// complete physical metadata contract check happen before any vector row is read.
    /// The closed authority distinguishes an absent managed tenant partition
    /// from descriptor-bound managed access. It cannot represent a legacy or
    /// display-name-derived vector read.
    pub(in crate::execution::interpreter::access::search) async fn search_vector_index<
        D: Distance,
    >(
        &self,
        query: &[f32],
        k: usize,
        authority: VectorSearchAuthority<&ResolvedVectorGenerationHandle>,
    ) -> Result<Vec<search::vector::SearchResult>> {
        self.search_vector_index_with_candidates::<D>(query, k, authority, None)
            .await
    }

    /// Searches one generation while enforcing exact upstream traversal membership.
    pub(in crate::execution::interpreter::access::search) async fn search_vector_index_restricted<
        D: Distance,
    >(
        &self,
        query: &[f32],
        k: usize,
        authority: VectorSearchAuthority<&ResolvedVectorGenerationHandle>,
        candidates: &RestrictedVectorCandidates,
    ) -> Result<Vec<search::vector::SearchResult>> {
        self.search_vector_index_with_candidates::<D>(query, k, authority, Some(candidates))
            .await
    }

    async fn search_vector_index_with_candidates<D: Distance>(
        &self,
        query: &[f32],
        k: usize,
        authority: VectorSearchAuthority<&ResolvedVectorGenerationHandle>,
        candidates: Option<&RestrictedVectorCandidates>,
    ) -> Result<Vec<search::vector::SearchResult>> {
        let generation = match authority {
            VectorSearchAuthority::AbsentManagedPartition => return Ok(Vec::new()),
            VectorSearchAuthority::Managed(generation) => generation,
        };
        let generation = generation.physical();
        let visibility = if self.active_write_tx().is_some() {
            // Write requests need VS-08B's transaction-local dirty set before
            // shared cache rows can be observed safely.
            VectorReadVisibility::Unavailable
        } else if let Some(view) = self.request_read_view() {
            view.comparable_sequence().map_or(
                VectorReadVisibility::Unavailable,
                VectorReadVisibility::Comparable,
            )
        } else {
            VectorReadVisibility::Unavailable
        };
        let index = ValidatedVectorReadIndex::<D>::managed(
            generation,
            self.db.vector_cache_registry(),
            std::sync::Arc::clone(self.db.simhasher_registry()),
            visibility,
        )
        .map_err(|error| {
            HelixDbError::InvariantViolation(format!(
                "validated vector read factory rejected generation: {error}"
            ))
        })?;
        let metadata = if let Some(active) = self.active_write_tx() {
            let view = VectorReadView::<
                crate::execution::interpreter::read_view::StableRequestReadView,
            >::transaction(&active.txn);
            index.get_metadata(&view).await
        } else if let Some(view) = self.request_read_view() {
            index.get_metadata(&VectorReadView::snapshot(view)).await
        } else {
            #[cfg(test)]
            {
                match self.db.storage() {
                    HelixStorage::Reader(reader) => index.get_metadata(reader.as_ref()).await,
                    HelixStorage::Writer(writer) => index.get_metadata(writer.db()).await,
                }
            }
            #[cfg(not(test))]
            {
                Err(HelixDbError::InvariantViolation(
                    "vector metadata read escaped its request read view".to_string(),
                ))
            }
        }?;
        let Some(metadata) = metadata else {
            return Err(HelixDbError::InvariantViolation(
                "managed vector ownership references missing physical metadata".to_string(),
            ));
        };
        let expected = search::vector::VectorIndexConfig::from_v2_definition(
            generation.definition(),
            generation.physical_name(),
        );
        if !metadata.config.has_same_physical_contract(&expected) {
            return Err(HelixDbError::InvariantViolation(
                "managed vector descriptor and physical metadata contract mismatch".to_string(),
            ));
        }
        let params = SearchParams::new(k)
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?;
        let results = if let Some(active) = self.active_write_tx() {
            let view = VectorReadView::<
                crate::execution::interpreter::read_view::StableRequestReadView,
            >::transaction(&active.txn);
            match candidates {
                Some(candidates) => {
                    index
                        .search_restricted(&view, query, &params, candidates)
                        .await
                }
                None => index.search(&view, query, &params).await,
            }
        } else if let Some(view) = self.request_read_view() {
            let view = VectorReadView::snapshot(view);
            match candidates {
                Some(candidates) => {
                    index
                        .search_restricted(&view, query, &params, candidates)
                        .await
                }
                None => index.search(&view, query, &params).await,
            }
        } else {
            #[cfg(test)]
            {
                match self.db.storage() {
                    HelixStorage::Reader(reader) => match candidates {
                        Some(candidates) => {
                            index
                                .search_restricted(reader.as_ref(), query, &params, candidates)
                                .await
                        }
                        None => index.search(reader.as_ref(), query, &params).await,
                    },
                    HelixStorage::Writer(writer) => match candidates {
                        Some(candidates) => {
                            index
                                .search_restricted(writer.db(), query, &params, candidates)
                                .await
                        }
                        None => index.search(writer.db(), query, &params).await,
                    },
                }
            }
            #[cfg(not(test))]
            {
                Err(HelixDbError::InvariantViolation(
                    "vector traversal escaped its request read view".to_string(),
                ))
            }
        };
        let results = match results {
            Ok(results) => results,
            Err(HelixDbError::IndexNotFound(_)) => {
                return Err(HelixDbError::InvariantViolation(
                    "managed vector ownership references missing physical rows".to_string(),
                ));
            }
            Err(err) => return Err(err),
        };
        Ok(results)
    }

    /// Loads one text manifest root only after managed ownership is resolved.
    ///
    /// The authority returns an absent normalized tenant partition without a
    /// physical read. A present root is point-loaded through the stable request
    /// view and cross-checked before any page, cache, or blob is accessed.
    pub(in crate::execution::interpreter::access::search) async fn load_text_manifest_root<
        'generation,
    >(
        &self,
        authority: TextSearchAuthority<&'generation ResolvedTextGenerationHandle>,
    ) -> Result<Option<ResolvedTextManifestRoot<'generation>>> {
        let generation = match authority {
            TextSearchAuthority::AbsentManagedPartition => return Ok(None),
            TextSearchAuthority::Managed(generation) => generation,
        };
        if let Some(active) = self.active_write_tx() {
            return load_text_root_in_view(&active.txn, generation).await;
        }
        if let Some(view) = self.request_read_view() {
            return load_text_root_in_view(view, generation).await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    load_text_root_in_view(reader.as_ref(), generation).await
                }
                HelixStorage::Writer(writer) => {
                    load_text_root_in_view(writer.db(), generation).await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "text manifest root read escaped its request read view".to_string(),
        ))
    }

    /// Loads bounded pages concurrently and searches their pruned splits.
    ///
    /// The page, split, and batched V2 candidate-state reads all use the same
    /// admitted request view. Query-level overfetch retains only live global
    /// candidates while opening and searching independent splits concurrently.
    pub(in crate::execution::interpreter::access::search) async fn search_text_manifest_with_scope(
        &self,
        manifest: &ResolvedTextManifestRoot<'_>,
        query: &str,
        k: usize,
        scope: search::text::TextSearchScope,
    ) -> Result<Vec<search::text::TextSearchHit>> {
        if let Some(active) = self.active_write_tx() {
            return search_text_manifest_in_view(self, &active.txn, manifest, query, k, scope)
                .await;
        }
        if let Some(view) = self.request_read_view() {
            return search_text_manifest_in_view(self, view, manifest, query, k, scope).await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    search_text_manifest_in_view(self, reader.as_ref(), manifest, query, k, scope)
                        .await
                }
                HelixStorage::Writer(writer) => {
                    search_text_manifest_in_view(self, writer.db(), manifest, query, k, scope).await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "text manifest search escaped its request read view".to_string(),
        ))
    }
}

/// Point-loads one V2 root through the stable request view.
async fn load_text_root_in_view<'generation>(
    reader: &(impl DbReadOps + Sync),
    generation: &'generation ResolvedTextGenerationHandle,
) -> Result<Option<ResolvedTextManifestRoot<'generation>>> {
    let root = crate::index_lifecycle::text::serving::load_active_manifest_root(
        reader,
        generation.physical(),
        generation.partition(),
    )
    .await?;
    Ok(root.map(|root| ResolvedTextManifestRoot { generation, root }))
}

/// Searches a checked root through bounded page/blob/state batches.
async fn search_text_manifest_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    manifest: &ResolvedTextManifestRoot<'_>,
    query: &str,
    k: usize,
    scope: search::text::TextSearchScope,
) -> Result<Vec<search::text::TextSearchHit>> {
    let generation = manifest.generation;
    let root = &manifest.root;
    let definition = generation.physical().definition();
    let statistics = match crate::index_lifecycle::text::statistics::load_query_statistics(
        reader,
        generation.physical().scope(),
        root.index_id(),
        root.generation(),
        root.partition(),
        definition.analyzer(),
        query,
    )
    .await?
    {
        crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::EmptyQuery => {
            return Ok(Vec::new());
        }
        crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::EmptyCorpus => {
            return Ok(Vec::new());
        }
        crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::Ready(statistics) => {
            statistics
        }
    };
    const PAGE_READ_CONCURRENCY: usize = 4;
    let loaded_pages = futures::stream::iter(0..root.page_count())
        .map(|page| async move {
            #[cfg(feature = "index-lifecycle-testing")]
            crate::index_lifecycle_testing::pause_text_search_before_manifest_page(page).await;
            crate::index_lifecycle::text::serving::load_active_manifest_page(reader, root, page)
                .await
        })
        .buffered(PAGE_READ_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let observed_splits = loaded_pages.iter().try_fold(0_u64, |total, entries| {
        total
            .checked_add(u64::try_from(entries.len()).map_err(|_| {
                HelixDbError::IndexCatalogCorruption(
                    "text manifest page split count exceeds u64".to_string(),
                )
            })?)
            .ok_or_else(|| {
                HelixDbError::IndexCatalogCorruption(
                    "text manifest observed split count overflowed".to_string(),
                )
            })
    })?;
    if observed_splits > root.split_count() {
        return Err(HelixDbError::IndexCatalogCorruption(
            "text manifest pages exceed their root split count".to_string(),
        ));
    }
    if observed_splits != root.split_count() {
        return Err(HelixDbError::IndexCatalogCorruption(
            "text manifest pages disagree with their root split count".to_string(),
        ));
    }
    let query_terms = search::text::analyze_text(definition.analyzer(), query).unique_terms;
    let splits = loaded_pages
        .into_iter()
        .flatten()
        .filter(|split| split.pruning().may_match_any(query_terms.iter()))
        .map(|split| search::text::TextSplitRef {
            blob: search::text::TextBlobRef {
                sha256: *split.blob().hash(),
                size_bytes: split.blob().size(),
            },
            footer_offset: split.footer_offset(),
            footer_len: split.footer_length(),
            hotcache_len: split.hot_cache_length(),
            total_size_bytes: split.total_size(),
        })
        .collect::<Vec<_>>();
    let Some(primary) = splits.first().cloned() else {
        return Ok(Vec::new());
    };
    let mut generation_manifest = search::text::TextIndexGenerationManifest::new_split(
        format!(
            "index-v2-text-{}-{}",
            root.index_id().get(),
            root.generation().get(),
        ),
        format!("{}", root.generation().get()),
        definition.analyzer(),
        definition.positions_enabled(),
        primary,
    );
    generation_manifest.splits = splits;
    search::text::search_manifest_with_v2_live_state_scoped_and_scope(
        reader,
        search::text::TextSearchRuntime::new(
            context.db.object_store(),
            context.db.path(),
            context.db.fts_cache(),
        ),
        root,
        &generation_manifest,
        &statistics,
        search::text::TextSearchRequest::new(query, k, scope),
    )
    .await
}

/// Merges one page's top hits while retaining at most the global top `k`.
#[cfg(test)]
fn retain_best_text_hits(
    retained: &mut Vec<search::text::TextSearchHit>,
    page_hits: Vec<search::text::TextSearchHit>,
    k: usize,
) -> Result<()> {
    let mut by_entity = BTreeMap::new();
    for hit in retained.drain(..).chain(page_hits) {
        if let Some(existing) = by_entity.insert(hit.entity_id, hit.clone())
            && existing.score.to_bits() != hit.score.to_bits()
        {
            return Err(HelixDbError::IndexCatalogCorruption(
                "duplicate live text versions have different cross-page BM25 score bits"
                    .to_string(),
            ));
        }
    }
    retained.extend(by_entity.into_values());
    retained.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    retained.truncate(k);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use bytes::Bytes;
    use helix_planner::context::ParamBindings;
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::TextIndexDefinition;
    use crate::encoding::keys::scope::DataScope;
    use crate::encoding::v2::keys::ManagedIndexKey;
    use crate::encoding::v2::keys::{
        IndexEntity, ScopedKey, TextEntityStateKey, TextManifestPageKey, TextManifestRootKey,
    };
    use crate::encoding::v2::values::{
        encode_index_record, encode_manifest_page, encode_manifest_root, encode_text_entity_state,
    };
    use crate::index_lifecycle::work::{
        BlobRef, SplitRef, TextEntityStateValue, TextManifestPageValue, TextManifestRootValue,
        TextPartition,
    };
    use crate::index_lifecycle::{
        IndexEntityId, IndexGenerationId, IndexOperationId, IndexRecordV2, IndexRevision,
        IndexStateTransition, PhysicalGeneration, TextLogicalVersion, TextManifestRevision,
        ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
    };
    use crate::search::text::TextDocumentInput;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity, VectorIndex};

    fn resolved_vector_generation(
        generation: ValidatedVectorGenerationHandle,
    ) -> ResolvedVectorGenerationHandle {
        ResolvedVectorGenerationHandle::for_storage_test(generation)
    }

    #[test]
    fn paged_text_hit_merge_deduplicates_and_retains_global_top_k() {
        let mut retained = vec![
            search::text::TextSearchHit {
                entity_id: 1,
                score: 0.8,
            },
            search::text::TextSearchHit {
                entity_id: 2,
                score: 0.4,
            },
        ];
        retain_best_text_hits(
            &mut retained,
            vec![
                search::text::TextSearchHit {
                    entity_id: 1,
                    score: 0.8,
                },
                search::text::TextSearchHit {
                    entity_id: 3,
                    score: 0.7,
                },
            ],
            2,
        )
        .unwrap();

        assert_eq!(
            retained,
            vec![
                search::text::TextSearchHit {
                    entity_id: 1,
                    score: 0.8,
                },
                search::text::TextSearchHit {
                    entity_id: 3,
                    score: 0.7,
                },
            ]
        );
    }

    #[test]
    fn paged_text_hit_merge_rejects_divergent_duplicate_score_bits() {
        let mut retained = vec![search::text::TextSearchHit {
            entity_id: 1,
            score: 0.5,
        }];
        assert!(matches!(
            retain_best_text_hits(
                &mut retained,
                vec![search::text::TextSearchHit {
                    entity_id: 1,
                    score: 0.8,
                }],
                1,
            ),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("different cross-page BM25 score bits")
        ));
    }

    #[tokio::test]
    async fn absent_managed_partition_returns_empty_without_physical_access() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let database = HelixDB::open_with_object_store_for_tests(
            "search-storage-absent-managed-partition",
            object_store,
        )
        .await
        .unwrap();
        let context = ExecutionContext::new(&database, ParamBindings::default());

        assert!(context
            .search_vector_index::<crate::search::vector::distance::Cosine>(
                &[1.0, 0.0],
                1,
                VectorSearchAuthority::AbsentManagedPartition,
            )
            .await
            .unwrap()
            .is_empty());
        assert!(context
            .load_text_manifest_root(TextSearchAuthority::AbsentManagedPartition)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn managed_text_search_streams_v2_pages_and_filters_state() {
        let runtime = TextIndexDefinition::new_node("Document", "body").unwrap();
        let canonical = ValidatedTextIndexDefinition::try_from_runtime(&runtime).unwrap();
        let database_name = "search-storage-managed-text";
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let database = HelixDB::open_with_object_store_and_config(
            database_name,
            Arc::clone(&object_store),
            crate::DbConfig::new(),
        )
        .await
        .unwrap();
        let documents = [(7, "rust planner"), (9, "rust storage")];
        let mut splits = Vec::new();
        for (entity_id, text) in documents {
            let documents = [TextDocumentInput::new(entity_id, text)];
            let unpublished = search::text::build_documents_as_split(&runtime, &documents)
                .unwrap()
                .unwrap();
            let (payload, split, pruning) = unpublished.into_parts();
            let uploaded =
                search::text::upload_blob(database.object_store(), database.path(), &payload)
                    .await
                    .unwrap();
            assert_eq!(uploaded, split.blob);
            splits.push(
                SplitRef::try_new(
                    BlobRef::new(split.blob.sha256, split.blob.size_bytes),
                    split.footer_offset,
                    split.footer_len,
                    split.hotcache_len,
                    split.total_size_bytes,
                    pruning,
                )
                .unwrap(),
            );
        }

        let transaction = database
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let index_id = crate::index_lifecycle::repository::allocate_index_id(&transaction)
            .await
            .unwrap();
        let generation = IndexGenerationId::initial();
        let active = IndexRecordV2::building(
            index_id,
            ValidatedDynamicIndexDefinition::Text(canonical),
            IndexRevision::initial(),
            PhysicalGeneration::Text { generation },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        transaction
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::index_record(active.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&active),
            )
            .unwrap();
        let partition = TextPartition::Unpartitioned;
        let root = TextManifestRootKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        };
        transaction
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::TextManifestRoot(root),
                }
                .to_bytes(),
                encode_manifest_root(
                    &TextManifestRootValue::try_new(
                        index_id,
                        generation,
                        partition.clone(),
                        TextManifestRevision::new(3).unwrap(),
                        2,
                        2,
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        for (page, split) in splits.iter().cloned().enumerate() {
            let page = u32::try_from(page).unwrap();
            transaction
                .put(
                    ManagedIndexKey::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: ScopedKey::TextManifestPage(TextManifestPageKey { root, page }),
                    }
                    .to_bytes(),
                    encode_manifest_page(
                        &TextManifestPageValue::try_new(
                            index_id,
                            generation,
                            partition.clone(),
                            page,
                            vec![split],
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
        }
        let mut statistics =
            crate::index_lifecycle::text::statistics::PreparedTextStatisticsBatch::default();
        for (entity_id, text) in documents {
            let entity = IndexEntity {
                kind: crate::index_lifecycle::IndexElementKind::Node,
                id: IndexEntityId::new(entity_id),
            };
            let contribution = crate::index_lifecycle::text::statistics::present_contribution(
                runtime.analyzer(),
                partition.clone(),
                text,
            )
            .unwrap();
            let transition =
                crate::index_lifecycle::text::statistics::prepare_source_scan_in_batch(
                    &transaction,
                    &statistics,
                    DataScope::LegacyUnscoped,
                    index_id,
                    generation,
                    entity,
                    contribution,
                )
                .await
                .unwrap()
                .unwrap();
            statistics.push(transition).unwrap();
            transaction
                .put(
                    ManagedIndexKey::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: ScopedKey::TextEntityState(TextEntityStateKey { root, entity }),
                    }
                    .to_bytes(),
                    encode_text_entity_state(&TextEntityStateValue {
                        index_id,
                        generation,
                        partition: partition.clone(),
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        logical_version: TextLogicalVersion::initial(),
                        live: true,
                    }),
                )
                .unwrap();
        }
        statistics.validate(&transaction).await.unwrap();
        statistics.stage_validated(&transaction).unwrap();
        transaction.commit().await.unwrap();
        database
            .refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .unwrap();

        let context = ExecutionContext::new(&database, ParamBindings::default());
        let authority = context
            .managed_text_generation(&runtime, None)
            .await
            .unwrap();
        let TextSearchAuthority::Managed(generation_handle) = authority else {
            panic!("unpartitioned Active text generation must resolve");
        };
        let manifest = context
            .load_text_manifest_root(TextSearchAuthority::Managed(&generation_handle))
            .await
            .unwrap()
            .unwrap();
        let storage_hits = context
            .search_text_manifest_with_scope(
                &manifest,
                "storage",
                1,
                search::text::TextSearchScope::Unrestricted,
            )
            .await
            .unwrap();
        assert_eq!(
            storage_hits
                .iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>(),
            [9]
        );
        assert!(storage_hits.iter().all(|hit| hit.score.is_finite()));

        let planner_hits = context
            .search_text_manifest_with_scope(
                &manifest,
                "planner",
                1,
                search::text::TextSearchScope::Unrestricted,
            )
            .await
            .unwrap();
        assert_eq!(
            planner_hits
                .iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>(),
            [7]
        );
        assert!(planner_hits.iter().all(|hit| hit.score.is_finite()));

        database.inner_db().flush().await.unwrap();
        let reader = HelixDB::open_reader_with_object_store_and_config(
            database_name,
            object_store,
            crate::DbConfig::new(),
        )
        .await
        .unwrap();
        let reader_context = ExecutionContext::new(&reader, ParamBindings::default());
        let reader_generation = reader_context
            .managed_text_generation(&runtime, None)
            .await
            .unwrap();
        let TextSearchAuthority::Managed(reader_generation) = reader_generation else {
            panic!("reader resolves the unpartitioned Active text generation");
        };
        let reader_manifest = reader_context
            .load_text_manifest_root(TextSearchAuthority::Managed(&reader_generation))
            .await
            .unwrap()
            .unwrap();
        let reader_hits = reader_context
            .search_text_manifest_with_scope(
                &reader_manifest,
                "planner",
                1,
                search::text::TextSearchScope::Unrestricted,
            )
            .await
            .unwrap();
        assert_eq!(
            reader_hits
                .iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>(),
            [7]
        );
        assert!(reader_hits.iter().all(|hit| hit.score.is_finite()));

        database
            .inner_db()
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::TextEntityState(TextEntityStateKey {
                        root,
                        entity: IndexEntity {
                            kind: crate::index_lifecycle::IndexElementKind::Node,
                            id: IndexEntityId::new(9),
                        },
                    }),
                }
                .to_bytes(),
                encode_text_entity_state(&TextEntityStateValue {
                    index_id,
                    generation,
                    partition: partition.clone(),
                    entity_kind: crate::index_lifecycle::IndexElementKind::Node,
                    entity_id: IndexEntityId::new(9),
                    logical_version: TextLogicalVersion::initial(),
                    live: false,
                }),
            )
            .await
            .unwrap();

        let mut filtered = ExecutionContext::new(&database, ParamBindings::default());
        filtered.enable_request_write_scope().await.unwrap();
        let filtered_generation = filtered
            .managed_text_generation(&runtime, None)
            .await
            .unwrap();
        let TextSearchAuthority::Managed(filtered_generation) = filtered_generation else {
            panic!("unpartitioned Active text generation must remain available");
        };
        let filtered_manifest = filtered
            .load_text_manifest_root(TextSearchAuthority::Managed(&filtered_generation))
            .await
            .unwrap()
            .unwrap();
        assert!(filtered
            .search_text_manifest_with_scope(
                &filtered_manifest,
                "storage",
                1,
                search::text::TextSearchScope::Unrestricted,
            )
            .await
            .unwrap()
            .is_empty());

        let corrupt_manifest = database
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        corrupt_manifest
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::TextManifestRoot(root),
                }
                .to_bytes(),
                encode_manifest_root(
                    &TextManifestRootValue::try_new(
                        index_id,
                        generation,
                        partition.clone(),
                        TextManifestRevision::new(3).unwrap(),
                        1,
                        1,
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        corrupt_manifest
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::TextManifestPage(TextManifestPageKey { root, page: 0 }),
                }
                .to_bytes(),
                encode_manifest_page(
                    &TextManifestPageValue::try_new(index_id, generation, partition, 0, splits)
                        .unwrap(),
                ),
            )
            .unwrap();
        corrupt_manifest.commit().await.unwrap();

        let corrupt_context = ExecutionContext::new(&database, ParamBindings::default());
        let corrupt_authority = corrupt_context
            .managed_text_generation(&runtime, None)
            .await
            .unwrap();
        let corrupt_root = corrupt_context
            .load_text_manifest_root(corrupt_authority.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            corrupt_context
                .search_text_manifest_with_scope(
                    &corrupt_root,
                    "storage",
                    1,
                    search::text::TextSearchScope::Unrestricted,
                )
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("pages exceed their root split count")
        ));
        filtered.abort_request_write_scope();
    }

    #[tokio::test]
    async fn managed_vector_search_rejects_missing_physical_metadata() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = HelixDB::open_with_object_store_for_tests(
            "search-storage-missing-vector-metadata",
            object_store,
        )
        .await
        .unwrap();
        let physical_name = "missing-vector-physical";
        let identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            0x32,
            physical_name.to_string(),
            crate::search::vector::index_id_from_name(physical_name),
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let context = ExecutionContext::new(&writer, ParamBindings::default());
        let resolved = resolved_vector_generation(generation);

        assert!(matches!(
            context
                .search_vector_index::<crate::search::vector::distance::Cosine>(
                    &[1.0, 0.0],
                    1,
                    VectorSearchAuthority::Managed(&resolved),
                )
                .await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("missing physical metadata")
        ));
    }

    #[tokio::test]
    async fn managed_vector_search_rejects_generation_distance_mismatch_before_storage() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = HelixDB::open_with_object_store_for_tests(
            "search-storage-managed-distance-mismatch",
            object_store,
        )
        .await
        .unwrap();
        let physical_name = "managed-euclidean-physical";
        let identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            0x35,
            physical_name.to_string(),
            crate::search::vector::index_id_from_name(physical_name),
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Euclidean,
        >(identity)
        .unwrap();
        let context = ExecutionContext::new(&writer, ParamBindings::default());
        let resolved = resolved_vector_generation(generation);

        assert!(matches!(
            context
                .search_vector_index::<crate::search::vector::distance::Cosine>(
                    &[1.0, 0.0],
                    1,
                    VectorSearchAuthority::Managed(&resolved),
                )
                .await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("validated vector read factory rejected generation")
        ));
    }

    #[tokio::test]
    async fn managed_vector_search_rejects_descriptor_metadata_dimension_mismatch() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = HelixDB::open_with_object_store_for_tests(
            "search-storage-managed-dimension",
            object_store,
        )
        .await
        .unwrap();
        let index = VectorIndex::<crate::search::vector::distance::Cosine>::new("managed-vector");
        let txn = writer
            .inner_db()
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .unwrap();
        index
            .create(
                &txn,
                crate::search::vector::VectorIndexConfig::new("managed-vector", "embedding", 2),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();
        let identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            0x33,
            "managed-vector".to_string(),
            crate::search::vector::index_id_from_name("managed-vector"),
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(3).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let context = ExecutionContext::new(&writer, ParamBindings::default());
        let resolved = resolved_vector_generation(generation);
        assert!(matches!(
            context
                .search_vector_index::<crate::search::vector::distance::Cosine>(
                    &[1.0, 0.0],
                    1,
                    VectorSearchAuthority::Managed(&resolved),
                )
                .await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("metadata contract mismatch")
        ));
    }

    #[tokio::test]
    async fn managed_vector_search_covers_direct_writer_and_reader_views() {
        let token = crate::ProcessLocalDatabaseToken::new("search-storage-direct-vector-views")
            .expect("process-local token validates");
        let writer = HelixDB::open_with_process_local_token_for_tests(token.clone())
            .await
            .unwrap();
        let physical_name = "direct-vector-physical";
        let index = VectorIndex::<crate::search::vector::distance::Cosine>::new(physical_name)
            .with_scripted_layers(vec![1])
            .unwrap();
        let transaction = writer
            .inner_db()
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .unwrap();
        index
            .create(
                &transaction,
                crate::search::vector::VectorIndexConfig::new(physical_name, "embedding", 2),
            )
            .await
            .unwrap();
        index.insert(&transaction, 7, &[1.0, 0.0]).await.unwrap();
        transaction.commit().await.unwrap();

        let identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            0x34,
            physical_name.to_string(),
            crate::search::vector::index_id_from_name(physical_name),
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();

        let writer_context = ExecutionContext::new(&writer, ParamBindings::default());
        let writer_generation = resolved_vector_generation(generation.clone());
        let writer_results = writer_context
            .search_vector_index::<crate::search::vector::distance::Cosine>(
                &[1.0, 0.0],
                1,
                VectorSearchAuthority::Managed(&writer_generation),
            )
            .await
            .unwrap();
        assert_eq!(writer_results[0].entity_id(), 7);

        writer.inner_db().flush().await.unwrap();
        let reader = HelixDB::open_reader(crate::HelixDbSource::InMemoryToken { token })
            .await
            .unwrap();
        let reader_context = ExecutionContext::new(&reader, ParamBindings::default());
        let reader_generation = resolved_vector_generation(generation);
        let reader_results = reader_context
            .search_vector_index::<crate::search::vector::distance::Cosine>(
                &[1.0, 0.0],
                1,
                VectorSearchAuthority::Managed(&reader_generation),
            )
            .await
            .unwrap();
        assert_eq!(reader_results[0].entity_id(), 7);
    }

    #[tokio::test]
    async fn managed_read_plan_uses_only_exact_sequence_descriptor_cache() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = HelixDB::open_with_object_store_for_tests(
            "search-storage-managed-cache-factory",
            object_store,
        )
        .await
        .unwrap();
        let physical_name = "managed-cache-physical";
        let index = VectorIndex::<crate::search::vector::distance::Cosine>::new(physical_name)
            .with_scripted_layers(vec![1])
            .unwrap();
        let txn = writer
            .inner_db()
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .unwrap();
        index
            .create(
                &txn,
                crate::search::vector::VectorIndexConfig::new(physical_name, "embedding", 2),
            )
            .await
            .unwrap();
        index.insert(&txn, 7, &[1.0, 0.0]).await.unwrap();
        txn.commit().await.unwrap();

        let identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            0x44,
            physical_name.to_string(),
            crate::search::vector::index_id_from_name(physical_name),
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let snapshot = writer.inner_db().snapshot().await.unwrap();
        let store = Arc::new(crate::search::vector::VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            generation.physical_index_id(),
            snapshot.seq(),
        ));
        store.insert_upper_vector(7, Bytes::from_static(b"invalid cached vector row"));
        let (entry, owns_hydration) = writer.vector_cache_registry().entry_for(&generation);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(store));
        drop(snapshot);

        let mut context = ExecutionContext::new(&writer, ParamBindings::default());
        context.enable_request_read_view().await.unwrap();
        let resolved = resolved_vector_generation(generation);
        assert!(matches!(
            context
                .search_vector_index::<crate::search::vector::distance::Cosine>(
                    &[1.0, 0.0],
                    1,
                    VectorSearchAuthority::Managed(&resolved),
                )
                .await,
            Err(HelixDbError::InvalidVectorItem(_))
        ));
    }

    #[tokio::test]
    async fn write_request_vector_search_uses_eager_transaction_and_aborts_its_rows() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let database = HelixDB::open_with_object_store_for_tests(
            "search-storage-write-request-transaction",
            object_store,
        )
        .await
        .unwrap();
        let index = VectorIndex::<crate::search::vector::distance::Cosine>::new("request-vector");
        let create = database
            .inner_db()
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .unwrap();
        index
            .create(
                &create,
                crate::search::vector::VectorIndexConfig::new("request-vector", "embedding", 2),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();

        let mut context = ExecutionContext::new(&database, ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let active = context
            .active_write_tx()
            .expect("write scope starts its transaction eagerly");
        index.insert(&active.txn, 7, &[1.0, 0.0]).await.unwrap();

        let identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            0x55,
            "request-vector".to_string(),
            crate::search::vector::index_id_from_name("request-vector"),
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let resolved = resolved_vector_generation(generation);

        let results = context
            .search_vector_index::<crate::search::vector::distance::Cosine>(
                &[1.0, 0.0],
                1,
                VectorSearchAuthority::Managed(&resolved),
            )
            .await
            .unwrap();
        assert_eq!(
            results
                .iter()
                .map(|result| result.entity_id())
                .collect::<Vec<_>>(),
            vec![7]
        );

        context.abort_request_write_scope();
        let read = database.inner_db().snapshot().await.unwrap();
        assert!(index.get_item(read.as_ref(), 7).await.unwrap().is_none());
    }
}
