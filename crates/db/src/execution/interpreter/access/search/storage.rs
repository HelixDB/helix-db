//! Physical search storage call contracts.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::generation::{
    LeasedTextGenerationHandle, LeasedVectorGenerationHandle, TextSearchAuthority,
    VectorSearchAuthority,
};
use super::*;
use crate::search;
#[cfg(test)]
use crate::search::vector::ValidatedVectorGenerationHandle;
use crate::search::vector::{
    Distance, SearchParams, ValidatedVectorReadIndex, VectorReadView, VectorReadVisibility,
};
#[cfg(test)]
use crate::HelixStorage;
use slatedb::DbReadOps;

/// A checked V2 text root inseparably paired with its request lease.
///
/// Private fields prevent a root loaded under one Active generation from being
/// searched with another generation's lease or canonical definition.
pub(super) struct LeasedTextManifestRoot<'generation> {
    generation: &'generation LeasedTextGenerationHandle,
    root: crate::index_v2::text::serving::ValidatedActiveTextManifestRoot,
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
        authority: VectorSearchAuthority<&LeasedVectorGenerationHandle>,
    ) -> Result<Vec<search::vector::SearchResult>> {
        let leased = match authority {
            VectorSearchAuthority::AbsentManagedPartition => return Ok(Vec::new()),
            VectorSearchAuthority::Managed(generation) => generation,
        };
        let generation = leased.physical();
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
        let metadata = self
            .run_index_read_batch(leased.lease_generation(), async {
                if let Some(active) = self.active_write_tx() {
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
                            HelixStorage::Reader(reader) => {
                                index.get_metadata(reader.as_ref()).await
                            }
                            HelixStorage::Writer(writer) => index.get_metadata(writer.db()).await,
                        }
                    }
                    #[cfg(not(test))]
                    {
                        Err(HelixDbError::InvariantViolation(
                            "vector metadata read escaped its request read view".to_string(),
                        ))
                    }
                }
            })
            .await?;
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
        let results = self
            .run_index_read_batch(leased.lease_generation(), async {
                if let Some(active) = self.active_write_tx() {
                    let view = VectorReadView::<
                        crate::execution::interpreter::read_view::StableRequestReadView,
                    >::transaction(&active.txn);
                    index.search(&view, query, &params).await
                } else if let Some(view) = self.request_read_view() {
                    index
                        .search(&VectorReadView::snapshot(view), query, &params)
                        .await
                } else {
                    #[cfg(test)]
                    {
                        match self.db.storage() {
                            HelixStorage::Reader(reader) => {
                                index.search(reader.as_ref(), query, &params).await
                            }
                            HelixStorage::Writer(writer) => {
                                index.search(writer.db(), query, &params).await
                            }
                        }
                    }
                    #[cfg(not(test))]
                    {
                        Err(HelixDbError::InvariantViolation(
                            "vector traversal escaped its request read view".to_string(),
                        ))
                    }
                }
            })
            .await;
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

    /// Loads one text manifest root only after managed ownership is leased.
    ///
    /// The authority returns an absent normalized tenant partition without a
    /// physical read. A present root is point-loaded through the stable request
    /// view and cross-checked before any page, cache, or blob is accessed.
    pub(in crate::execution::interpreter::access::search) async fn load_text_manifest_root<
        'generation,
    >(
        &self,
        authority: TextSearchAuthority<&'generation LeasedTextGenerationHandle>,
    ) -> Result<Option<LeasedTextManifestRoot<'generation>>> {
        let generation = match authority {
            TextSearchAuthority::AbsentManagedPartition => return Ok(None),
            TextSearchAuthority::Managed(generation) => generation,
        };
        if let Some(active) = self.active_write_tx() {
            return load_text_root_in_view(self, &active.txn, generation).await;
        }
        if let Some(view) = self.request_read_view() {
            return load_text_root_in_view(self, view, generation).await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    load_text_root_in_view(self, reader.as_ref(), generation).await
                }
                HelixStorage::Writer(writer) => {
                    load_text_root_in_view(self, writer.db(), generation).await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "text manifest root read escaped its request read view".to_string(),
        ))
    }

    /// Streams and searches every bounded page under the same generation lease.
    ///
    /// Each page—including its split cache/blob reads and V2 candidate-state
    /// point reads—is one admitted physical batch. Only the best `k` hits are
    /// retained between pages, so serving memory does not grow with page count.
    pub(in crate::execution::interpreter::access::search) async fn search_text_manifest(
        &self,
        manifest: &LeasedTextManifestRoot<'_>,
        query: &str,
        k: usize,
    ) -> Result<Vec<u64>> {
        if let Some(active) = self.active_write_tx() {
            return search_text_manifest_in_view(self, &active.txn, manifest, query, k).await;
        }
        if let Some(view) = self.request_read_view() {
            return search_text_manifest_in_view(self, view, manifest, query, k).await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    search_text_manifest_in_view(self, reader.as_ref(), manifest, query, k).await
                }
                HelixStorage::Writer(writer) => {
                    search_text_manifest_in_view(self, writer.db(), manifest, query, k).await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "text manifest search escaped its request read view".to_string(),
        ))
    }
}

/// Point-loads one V2 root inside an admitted reader-lease batch.
async fn load_text_root_in_view<'generation>(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Sync),
    generation: &'generation LeasedTextGenerationHandle,
) -> Result<Option<LeasedTextManifestRoot<'generation>>> {
    let root = context
        .run_index_read_batch(
            generation.lease_generation(),
            crate::index_v2::text::serving::load_active_manifest_root(
                reader,
                generation.physical(),
                generation.partition(),
            ),
        )
        .await?;
    Ok(root.map(|root| LeasedTextManifestRoot { generation, root }))
}

/// Searches a checked root through bounded page/blob/state batches.
async fn search_text_manifest_in_view(
    context: &ExecutionContext<'_>,
    reader: &(impl DbReadOps + Send + Sync),
    manifest: &LeasedTextManifestRoot<'_>,
    query: &str,
    k: usize,
) -> Result<Vec<u64>> {
    let generation = manifest.generation;
    let root = &manifest.root;
    let definition = generation.physical().definition();
    let mut observed_splits = 0_u64;
    let mut retained_hits = Vec::new();
    for page in 0..root.page_count() {
        let page_hits = context
            .run_index_read_batch(generation.lease_generation(), async {
                let entries =
                    crate::index_v2::text::serving::load_active_manifest_page(reader, root, page)
                        .await?;
                observed_splits = observed_splits
                    .checked_add(u64::try_from(entries.len()).map_err(|_| {
                        HelixDbError::IndexCatalogCorruption(
                            "text manifest page split count exceeds u64".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        HelixDbError::IndexCatalogCorruption(
                            "text manifest observed split count overflowed".to_string(),
                        )
                    })?;
                if observed_splits > root.split_count() {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "text manifest pages exceed their root split count".to_string(),
                    ));
                }
                let splits = entries
                    .into_iter()
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
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "validated text manifest page became empty".to_string(),
                    ));
                };
                let mut manifest = search::text::TextIndexGenerationManifest::new_split(
                    format!(
                        "index-v2-text-{}-{}-page-{page}",
                        root.index_id().get(),
                        root.generation().get(),
                    ),
                    format!("{}", root.generation().get()),
                    definition.analyzer(),
                    definition.positions_enabled(),
                    primary,
                );
                manifest.splits = splits;
                search::text::search_manifest_with_v2_live_state_scoped(
                    reader,
                    context.db.object_store(),
                    context.db.path(),
                    root,
                    &manifest,
                    query,
                    k,
                )
                .await
            })
            .await?;
        retain_best_text_hits(&mut retained_hits, page_hits, k);
    }
    if observed_splits != root.split_count() {
        return Err(HelixDbError::IndexCatalogCorruption(
            "text manifest pages disagree with their root split count".to_string(),
        ));
    }
    Ok(retained_hits.into_iter().map(|hit| hit.entity_id).collect())
}

/// Merges one page's top hits while retaining at most the global top `k`.
fn retain_best_text_hits(
    retained: &mut Vec<search::text::TextSearchHit>,
    page_hits: Vec<search::text::TextSearchHit>,
    k: usize,
) {
    let mut by_entity = BTreeMap::new();
    for hit in retained.drain(..).chain(page_hits) {
        by_entity
            .entry(hit.entity_id)
            .and_modify(|existing: &mut search::text::TextSearchHit| {
                if hit.score > existing.score {
                    existing.score = hit.score;
                }
            })
            .or_insert(hit);
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
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use bytes::Bytes;
    use helix_planner::context::ParamBindings;
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::{IndexConfig, TextIndexDefinition};
    use crate::encoding::keys::tenant::DataScope;
    use crate::encoding::v1::keys::index_v2::{
        IndexEntity, IndexV2Key, TextEntityStateKey, TextManifestPageKey, TextManifestRootKey,
    };
    use crate::encoding::v1::keys::{DataKeyKind, Key};
    use crate::encoding::v1::values::index_v2::{encode_index_record, encode_work_value};
    use crate::index_v2::work::{
        BlobRef, SplitRef, TextEntityStateValue, TextManifestPageValue, TextManifestRootValue,
        TextPartition,
    };
    use crate::index_v2::{
        IndexEntityId, IndexGenerationId, IndexOperationId, IndexRecordV2, IndexRevision,
        IndexStateTransition, PhysicalGeneration, TextLogicalVersion, TextManifestRevision,
        ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
    };
    use crate::search::text::TextDocumentInput;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity, VectorIndex};

    async fn leased_vector_generation(
        context: &ExecutionContext<'_>,
        generation: ValidatedVectorGenerationHandle,
    ) -> LeasedVectorGenerationHandle {
        let lease_generation = crate::index_v2::reader_lease::LeaseGenerationKey::new(
            generation.identity().scope(),
            generation.identity().index_id(),
            generation.identity().generation(),
        );
        context
            .index_read_leases
            .install_registered_for_storage_test(
                lease_generation,
                crate::error::IndexFamily::Vector,
                context.db.reader_lease_holder(),
            )
            .await
            .expect("physical storage fixture installs an explicit reader lease");
        LeasedVectorGenerationHandle::for_storage_test(lease_generation, generation)
    }

    #[test]
    fn paged_text_hit_merge_deduplicates_and_retains_global_top_k() {
        let mut retained = vec![
            search::text::TextSearchHit {
                entity_id: 1,
                score: 0.5,
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
        );

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

    #[tokio::test]
    async fn absent_managed_partition_returns_empty_without_physical_access() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let database = HelixDB::open_with_object_store_and_index_config_for_tests(
            "search-storage-absent-managed-partition",
            object_store,
            IndexConfig::new(),
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
    async fn managed_text_search_streams_v2_pages_filters_state_and_holds_lease() {
        let runtime = TextIndexDefinition::new_node("Document", "body").unwrap();
        let canonical = ValidatedTextIndexDefinition::try_from_runtime(&runtime).unwrap();
        let token = crate::ProcessLocalDatabaseToken::new("search-storage-managed-text").unwrap();
        let database = HelixDB::open_with_process_local_token_and_index_config_for_tests(
            token,
            IndexConfig::new().with_text_index(runtime.clone()),
        )
        .await
        .unwrap();
        let mut splits = Vec::new();
        for documents in [
            vec![TextDocumentInput::new(7, "rust planner")],
            vec![TextDocumentInput::new(9, "rust storage")],
        ] {
            let unpublished = search::text::build_documents_as_split(&runtime, &documents)
                .unwrap()
                .unwrap();
            let (payload, split) = unpublished.into_parts();
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
                )
                .unwrap(),
            );
        }

        let transaction = database
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let index_id = crate::index_v2::repository::allocate_index_id(&transaction)
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
        let lease_generation = crate::index_v2::reader_lease::LeaseGenerationKey::new(
            DataScope::LegacyUnscoped,
            index_id,
            generation,
        );
        database
            .reader_lease_coordinator()
            .unwrap()
            .register_generation(lease_generation)
            .await
            .unwrap();
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::index_record(active.identity().clone())),
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
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextManifestRoot(root)),
                }
                .to_bytes(),
                encode_work_value(
                    &crate::encoding::v1::values::index_v2::IndexV2WorkValue::TextManifestRoot(
                        TextManifestRootValue::try_new(
                            index_id,
                            generation,
                            partition.clone(),
                            TextManifestRevision::new(2).unwrap(),
                            2,
                            2,
                        )
                        .unwrap(),
                    ),
                ),
            )
            .unwrap();
        for (page, split) in splits.into_iter().enumerate() {
            let page = u32::try_from(page).unwrap();
            transaction
                .put(
                    Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: DataKeyKind::IndexV2(IndexV2Key::TextManifestPage(
                            TextManifestPageKey { root, page },
                        )),
                    }
                    .to_bytes(),
                    encode_work_value(
                        &crate::encoding::v1::values::index_v2::IndexV2WorkValue::TextManifestPage(
                            TextManifestPageValue::try_new(
                                index_id,
                                generation,
                                partition.clone(),
                                page,
                                vec![split],
                            )
                            .unwrap(),
                        ),
                    ),
                )
                .unwrap();
        }
        for entity_id in [7, 9] {
            let entity = IndexEntity {
                kind: crate::index_v2::IndexElementKind::Node,
                id: IndexEntityId::new(entity_id),
            };
            transaction
                .put(
                    Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: DataKeyKind::IndexV2(IndexV2Key::TextEntityState(
                            TextEntityStateKey { root, entity },
                        )),
                    }
                    .to_bytes(),
                    encode_work_value(
                        &crate::encoding::v1::values::index_v2::IndexV2WorkValue::TextEntityState(
                            TextEntityStateValue {
                                index_id,
                                generation,
                                partition: partition.clone(),
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                                logical_version: TextLogicalVersion::initial(),
                                live: true,
                            },
                        ),
                    ),
                )
                .unwrap();
        }
        transaction.commit().await.unwrap();

        let mut context = ExecutionContext::new(&database, ParamBindings::default());
        context.enable_request_read_view().await.unwrap();
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
        assert_eq!(
            context
                .search_text_manifest(&manifest, "storage", 1)
                .await
                .unwrap(),
            vec![9]
        );
        context
            .validate_and_release_index_read_leases()
            .await
            .unwrap();

        database
            .inner_db()
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextEntityState(TextEntityStateKey {
                        root,
                        entity: IndexEntity {
                            kind: crate::index_v2::IndexElementKind::Node,
                            id: IndexEntityId::new(9),
                        },
                    })),
                }
                .to_bytes(),
                encode_work_value(
                    &crate::encoding::v1::values::index_v2::IndexV2WorkValue::TextEntityState(
                        TextEntityStateValue {
                            index_id,
                            generation,
                            partition,
                            entity_kind: crate::index_v2::IndexElementKind::Node,
                            entity_id: IndexEntityId::new(9),
                            logical_version: TextLogicalVersion::initial(),
                            live: false,
                        },
                    ),
                ),
            )
            .await
            .unwrap();

        let mut filtered = ExecutionContext::new(&database, ParamBindings::default());
        filtered.enable_request_read_view().await.unwrap();
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
            .search_text_manifest(&filtered_manifest, "storage", 1)
            .await
            .unwrap()
            .is_empty());

        let coordinator = database.reader_lease_coordinator().unwrap();
        let fence = coordinator
            .begin_drain(lease_generation, None)
            .await
            .unwrap();
        assert!(!coordinator.check_drained(&fence).await.unwrap());
        filtered
            .validate_and_release_index_read_leases()
            .await
            .unwrap();
        assert!(coordinator.check_drained(&fence).await.unwrap());
    }

    #[tokio::test]
    async fn managed_vector_search_rejects_descriptor_metadata_dimension_mismatch() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = HelixDB::open_with_object_store_and_index_config_for_tests(
            "search-storage-managed-dimension",
            object_store,
            IndexConfig::new(),
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
            crate::index_v2::IndexElementKind::Node,
            VectorDimension::try_new(3).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let context = ExecutionContext::new(&writer, ParamBindings::default());
        let leased = leased_vector_generation(&context, generation).await;
        assert!(matches!(
            context
                .search_vector_index::<crate::search::vector::distance::Cosine>(
                    &[1.0, 0.0],
                    1,
                    VectorSearchAuthority::Managed(&leased),
                )
                .await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("metadata contract mismatch")
        ));
        context.release_index_read_leases().await;
    }

    #[tokio::test]
    async fn managed_read_plan_uses_only_exact_sequence_descriptor_cache() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = HelixDB::open_with_object_store_and_index_config_for_tests(
            "search-storage-managed-cache-factory",
            object_store,
            IndexConfig::new(),
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
            crate::index_v2::IndexElementKind::Node,
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
        let leased = leased_vector_generation(&context, generation).await;
        assert!(matches!(
            context
                .search_vector_index::<crate::search::vector::distance::Cosine>(
                    &[1.0, 0.0],
                    1,
                    VectorSearchAuthority::Managed(&leased),
                )
                .await,
            Err(HelixDbError::InvalidVectorItem(_))
        ));
        context.release_index_read_leases().await;
    }

    #[tokio::test]
    async fn write_request_vector_search_uses_eager_transaction_and_aborts_its_rows() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let database = HelixDB::open_with_object_store_and_index_config_for_tests(
            "search-storage-write-request-transaction",
            object_store,
            IndexConfig::new(),
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
            crate::index_v2::IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let leased = leased_vector_generation(&context, generation).await;

        let results = context
            .search_vector_index::<crate::search::vector::distance::Cosine>(
                &[1.0, 0.0],
                1,
                VectorSearchAuthority::Managed(&leased),
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
        context.release_index_read_leases().await;
        let read = database.inner_db().snapshot().await.unwrap();
        assert!(index.get_item(read.as_ref(), 7).await.unwrap().is_none());
    }
}
