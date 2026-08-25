//! Restart-safe compaction for immutable splits on Active manifest pages.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};

use crate::config::TextBackfillCompactionLimits;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{self, work};

const STATE_BATCH_SIZE: usize = 512;

#[derive(Debug)]
struct SelectedPage {
    pointer_key: Bytes,
    observations: Vec<(Bytes, Option<Bytes>)>,
    target: index_keys::TextCompactionTarget,
    root: work::TextManifestRootValue,
    page: work::TextManifestPageValue,
    definition: index_lifecycle::ValidatedTextIndexDefinition,
    selected_indices: Vec<usize>,
}

/// Compacts at most one durable Active-page pointer.
pub(crate) async fn compact_once(
    db: &Db,
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    limits: TextBackfillCompactionLimits,
) -> Result<bool> {
    let prefix =
        index_keys::GlobalKey::logical_prefix(index_keys::GlobalKind::TextCompactionPointer);
    let mut pointers = db.scan_prefix(prefix, ..).await?;
    let Some(pointer) = pointers.next().await? else {
        return Ok(false);
    };
    let index_keys::GlobalKey::TextCompactionPointer(target) =
        index_keys::GlobalKey::parse_from_slice(&pointer.key)?
    else {
        return Err(corruption(
            "text compaction prefix yielded another global key kind",
        ));
    };
    let index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(pointer_value) =
        index_values::decode_metadata_value(&pointer.value)?
    else {
        return Err(corruption(
            "text compaction pointer key contains another metadata value kind",
        ));
    };

    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let record_key = scoped_key(
        target.scope(),
        index_keys::ScopedKey::index_record(target.identity().clone()),
    );
    let Some(record_bytes) = snapshot.get(&record_key).await? else {
        drop(snapshot);
        delete_exact_pointer(db, &pointer.key, &pointer.value).await?;
        return Ok(true);
    };
    let record = index_values::decode_index_record(&record_bytes)?;
    if record.identity() != target.identity() {
        return Err(corruption(
            "text compaction record key/value identity mismatch",
        ));
    }
    let index_lifecycle::IndexStateV2::Active { physical, .. } = record.state() else {
        drop(snapshot);
        delete_exact_pointer(db, &pointer.key, &pointer.value).await?;
        return Ok(true);
    };
    let index_lifecycle::PhysicalGeneration::Text { generation } = physical else {
        return Err(corruption(
            "text compaction pointer resolves to another physical family",
        ));
    };
    let index_lifecycle::ValidatedDynamicIndexDefinition::Text(definition) = record.definition()
    else {
        return Err(corruption(
            "text compaction pointer resolves to another definition family",
        ));
    };
    if record.index_id() != target.index_id() || *generation != target.generation() {
        drop(snapshot);
        delete_exact_pointer(db, &pointer.key, &pointer.value).await?;
        return Ok(true);
    }

    let root_typed = index_keys::TextManifestRootKey {
        index_id: target.index_id(),
        generation: target.generation(),
        partition: target.partition(),
    };
    let root_key = scoped_key(
        target.scope(),
        index_keys::ScopedKey::TextManifestRoot(root_typed),
    );
    let page_key = scoped_key(
        target.scope(),
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root_typed,
            page: target.page(),
        }),
    );
    let root_bytes = snapshot.get(&root_key).await?;
    let page_bytes = snapshot.get(&page_key).await?;
    let (Some(root_bytes), Some(page_bytes)) = (root_bytes, page_bytes) else {
        drop(snapshot);
        delete_exact_pointer(db, &pointer.key, &pointer.value).await?;
        return Ok(true);
    };
    let root = index_values::decode_manifest_root(&root_bytes)?;
    let page = index_values::decode_manifest_page(&page_bytes)?;
    if root.index_id() != target.index_id()
        || root.generation() != target.generation()
        || root.partition().fingerprint() != target.partition()
        || root.revision() != pointer_value.revision
        || target.page() >= root.page_count()
        || page.index_id() != target.index_id()
        || page.generation() != target.generation()
        || page.partition() != root.partition()
        || page.page() != target.page()
    {
        drop(snapshot);
        delete_exact_pointer(db, &pointer.key, &pointer.value).await?;
        return Ok(true);
    }
    let Some(selected_indices) = select_tier(page.entries(), limits) else {
        drop(snapshot);
        delete_exact_pointer(db, &pointer.key, &pointer.value).await?;
        return Ok(true);
    };
    let selected = SelectedPage {
        pointer_key: pointer.key.clone(),
        observations: vec![
            (pointer.key.clone(), Some(pointer.value.clone())),
            (record_key, Some(record_bytes)),
            (root_key, Some(root_bytes)),
            (page_key, Some(page_bytes)),
        ],
        target,
        root,
        page,
        definition: definition.clone(),
        selected_indices,
    };
    drop(snapshot);

    let selected_refs = selected
        .selected_indices
        .iter()
        .map(|index| selected.page.entries()[*index])
        .collect::<Vec<_>>();
    let pruning = selected_refs
        .iter()
        .map(|split| split.pruning())
        .reduce(work::SplitPruning::union)
        .expect("a selected compaction tier contains at least two splits");
    let runtime_refs = selected_refs
        .iter()
        .copied()
        .map(runtime_split)
        .collect::<Vec<_>>();
    let runtime_definition = selected.definition.to_runtime();
    let physical_name = format!(
        "v3-active-text-{}-{}-{}",
        selected.target.index_id().get(),
        selected.target.generation().get(),
        selected.target.page(),
    );
    let prepared = crate::search::text::compaction::prepare_text_build_compaction(
        object_store,
        db_path,
        &runtime_definition,
        &physical_name,
        &runtime_refs,
        pruning,
        limits,
    )
    .await
    .map_err(compaction_error)?;

    let state_snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let entity_ids = prepared
        .document_versions()
        .iter()
        .map(|(entity_id, _)| *entity_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut state_observations = Vec::new();
    let mut live_versions = HashMap::new();
    for entity_ids in entity_ids.chunks(STATE_BATCH_SIZE) {
        let keys = entity_ids
            .iter()
            .copied()
            .map(|entity_id| {
                scoped_key(
                    selected.target.scope(),
                    index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                        root: root_typed,
                        entity: index_keys::IndexEntity {
                            kind: selected.definition.element_kind(),
                            id: index_lifecycle::IndexEntityId::new(entity_id),
                        },
                    }),
                )
            })
            .collect::<Vec<_>>();
        let values = state_snapshot.multi_get(&keys).await?;
        for ((entity_id, key), value) in entity_ids.iter().copied().zip(keys).zip(values) {
            let Some(value) = value else {
                return Err(corruption(
                    "Active text compaction document has no entity state",
                ));
            };
            let state = index_values::decode_text_entity_state(&value)?;
            if state.index_id != selected.target.index_id()
                || state.generation != selected.target.generation()
                || state.partition != *selected.root.partition()
                || state.entity_kind != selected.definition.element_kind()
                || state.entity_id.get() != entity_id
                || state.logical_version.get() > selected.root.revision().get()
            {
                return Err(corruption(
                    "Active text compaction state ownership or revision mismatch",
                ));
            }
            if state.live {
                live_versions.insert(entity_id, state.logical_version.get());
            }
            state_observations.push((key, Some(value)));
        }
    }
    drop(state_snapshot);

    let unpublished = prepared
        .finish(live_versions)
        .await
        .map_err(compaction_error)?;
    let output = match unpublished {
        Some(unpublished) => {
            let (payload, runtime_ref, pruning) = unpublished.into_parts();
            let uploaded =
                crate::search::text::upload_blob(object_store, db_path, &payload).await?;
            if uploaded != runtime_ref.blob {
                return Err(corruption(
                    "Active text compaction upload disagrees with prepared content",
                ));
            }
            Some(
                work::SplitRef::try_new(
                    work::BlobRef::new(uploaded.sha256, uploaded.size_bytes),
                    runtime_ref.footer_offset,
                    runtime_ref.footer_len,
                    runtime_ref.hotcache_len,
                    runtime_ref.total_size_bytes,
                    pruning,
                )
                .map_err(|error| corruption(error.to_string()))?,
            )
        }
        None => None,
    };

    commit_replacement(db, selected, state_observations, output, limits).await?;
    Ok(true)
}

async fn commit_replacement(
    db: &Db,
    selected: SelectedPage,
    state_observations: Vec<(Bytes, Option<Bytes>)>,
    output: Option<work::SplitRef>,
    limits: TextBackfillCompactionLimits,
) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    for (key, value) in selected.observations.iter().chain(&state_observations) {
        if transaction.get(key).await? != *value {
            return Ok(());
        }
    }
    let selected_indices = selected
        .selected_indices
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let first_selected = *selected
        .selected_indices
        .first()
        .expect("a compaction replacement has selected inputs");
    let mut entries = Vec::with_capacity(
        selected
            .page
            .entries()
            .len()
            .saturating_sub(selected_indices.len())
            .saturating_add(usize::from(output.is_some())),
    );
    for (index, split) in selected.page.entries().iter().copied().enumerate() {
        if index == first_selected
            && let Some(output) = output
        {
            entries.push(output);
        }
        if !selected_indices.contains(&index) {
            entries.push(split);
        }
    }
    let retained_placeholder = entries.is_empty();
    if retained_placeholder {
        entries.push(selected.page.entries()[first_selected]);
    }
    let replacement_count = u64::from(output.is_some() || retained_placeholder);
    let split_count = selected
        .root
        .split_count()
        .checked_sub(u64::try_from(selected_indices.len()).unwrap_or(u64::MAX))
        .and_then(|count| count.checked_add(replacement_count))
        .ok_or_else(|| corruption("Active text compaction split count overflowed"))?;
    let revision = selected
        .root
        .revision()
        .checked_next()
        .map_err(|error| corruption(error.to_string()))?;
    let root = work::TextManifestRootValue::try_new(
        selected.target.index_id(),
        selected.target.generation(),
        selected.root.partition().clone(),
        revision,
        selected.root.page_count(),
        split_count,
    )
    .map_err(|error| corruption(error.to_string()))?;
    let page = work::TextManifestPageValue::try_new(
        selected.target.index_id(),
        selected.target.generation(),
        selected.root.partition().clone(),
        selected.target.page(),
        entries,
    )
    .map_err(|error| corruption(error.to_string()))?;
    let root_key = scoped_key(
        selected.target.scope(),
        index_keys::ScopedKey::TextManifestRoot(index_keys::TextManifestRootKey {
            index_id: selected.target.index_id(),
            generation: selected.target.generation(),
            partition: selected.target.partition(),
        }),
    );
    let page_key = scoped_key(
        selected.target.scope(),
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: index_keys::TextManifestRootKey {
                index_id: selected.target.index_id(),
                generation: selected.target.generation(),
                partition: selected.target.partition(),
            },
            page: selected.target.page(),
        }),
    );
    let root_value = index_values::encode_manifest_root(&root);
    let page_value = index_values::encode_manifest_page(&page);
    if u64::try_from(page_value.len()).unwrap_or(u64::MAX) > limits.max_manifest_bytes().get() {
        return Err(corruption(
            "Active text compaction replacement exceeds the manifest page limit",
        ));
    }
    transaction.put(root_key, root_value)?;
    transaction.put(page_key, page_value)?;
    if select_tier(page.entries(), limits).is_some() {
        transaction.put(
            &selected.pointer_key,
            index_values::encode_metadata_value(
                &index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(
                    index_lifecycle::TextCompactionPointerValue { revision },
                ),
            ),
        )?;
    } else {
        transaction.delete(&selected.pointer_key)?;
    }
    transaction.commit().await?;
    Ok(())
}

fn select_tier(
    entries: &[work::SplitRef],
    limits: TextBackfillCompactionLimits,
) -> Option<Vec<usize>> {
    let fan_in = limits.max_fan_in().get();
    if fan_in < 2 {
        return None;
    }
    let mut tiers = BTreeMap::<u32, Vec<usize>>::new();
    for (index, split) in entries.iter().enumerate() {
        let size_class = u64::BITS - split.total_size().saturating_sub(1).leading_zeros();
        tiers.entry(size_class).or_default().push(index);
    }
    let temporary_input_limit = limits
        .max_temporary_disk_bytes()
        .get()
        .saturating_sub(limits.max_output_blob_bytes().get());
    let input_limit = limits.max_input_bytes().get().min(temporary_input_limit);
    tiers.into_values().find_map(|indices| {
        if indices.len() < fan_in {
            return None;
        }
        let selected = indices.into_iter().take(fan_in).collect::<Vec<_>>();
        let input_bytes = selected.iter().try_fold(0_u64, |total, index| {
            total.checked_add(entries[*index].total_size())
        })?;
        (input_bytes <= input_limit).then_some(selected)
    })
}

async fn delete_exact_pointer(db: &Db, key: &[u8], value: &[u8]) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    if transaction.get(key).await?.as_deref() == Some(value) {
        transaction.delete(key)?;
        transaction.commit().await?;
    }
    Ok(())
}

fn runtime_split(split: work::SplitRef) -> crate::search::text::TextSplitRef {
    crate::search::text::TextSplitRef {
        blob: crate::search::text::TextBlobRef {
            sha256: *split.blob().hash(),
            size_bytes: split.blob().size(),
        },
        footer_offset: split.footer_offset(),
        footer_len: split.footer_length(),
        hotcache_len: split.hot_cache_length(),
        total_size_bytes: split.total_size(),
    }
}

fn scoped_key(
    scope: crate::encoding::v2::keys::scope::DataScope,
    key: index_keys::ScopedKey,
) -> Bytes {
    ManagedIndexKey::Data { scope, kind: key }.to_bytes()
}

fn compaction_error(
    error: crate::search::text::compaction::TextBuildCompactionError,
) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(format!("Active text compaction failed: {error}"))
}

fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};

    use slatedb::object_store::memory::InMemory;

    fn limits(fan_in: usize) -> TextBackfillCompactionLimits {
        TextBackfillCompactionLimits::new(
            NonZeroUsize::new(fan_in).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            NonZeroU64::new(128 * 1024 * 1024).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            NonZeroU64::new(4 * 1024 * 1024).unwrap(),
        )
    }

    fn split(seed: u8, size: u64) -> work::SplitRef {
        work::SplitRef::try_new(
            work::BlobRef::new([seed; 32], size),
            0,
            0,
            0,
            size,
            work::SplitPruning::Unavailable,
        )
        .unwrap()
    }

    #[test]
    fn tier_selection_is_deterministic_and_never_mix_sizes() {
        let entries = vec![
            split(1, 8),
            split(2, 9),
            split(3, 8),
            split(4, 8),
            split(5, 8),
        ];
        assert_eq!(select_tier(&entries, limits(3)), Some(vec![0, 2, 3]));
        assert_eq!(select_tier(&entries, limits(5)), None);
    }

    #[tokio::test]
    async fn active_page_compaction_replaces_inputs_and_consumes_pointer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("active-page-compaction", Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let runtime = crate::config::TextIndexDefinition::new_node("Doc", "body").unwrap();
        let definition =
            index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(&runtime).unwrap();
        let index_id = index_lifecycle::IndexId::initial();
        let generation = index_lifecycle::IndexGenerationId::initial();
        let record = index_lifecycle::IndexRecordV2::building(
            index_id,
            index_lifecycle::ValidatedDynamicIndexDefinition::Text(definition.clone()),
            index_lifecycle::IndexRevision::initial(),
            index_lifecycle::PhysicalGeneration::Text { generation },
            index_lifecycle::IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(index_lifecycle::IndexStateTransition::Activate)
        .unwrap();
        let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
        db.put(
            scoped_key(
                scope,
                index_keys::ScopedKey::index_record(record.identity().clone()),
            ),
            index_values::encode_index_record(&record),
        )
        .await
        .unwrap();

        let mut splits = Vec::new();
        for (entity_id, text) in [(1_u64, "alpha shared"), (2_u64, "beta shared")] {
            let unpublished = crate::search::text::build_documents_as_split(
                &runtime,
                &[crate::search::text::TextDocumentInput::new(entity_id, text)
                    .with_logical_version(entity_id)],
            )
            .unwrap()
            .unwrap();
            let (payload, runtime_split, pruning) = unpublished.into_parts();
            let uploaded = crate::search::text::upload_blob(&store, "db", &payload)
                .await
                .unwrap();
            splits.push(
                work::SplitRef::try_new(
                    work::BlobRef::new(uploaded.sha256, uploaded.size_bytes),
                    runtime_split.footer_offset,
                    runtime_split.footer_len,
                    runtime_split.hotcache_len,
                    runtime_split.total_size_bytes,
                    pruning,
                )
                .unwrap(),
            );
        }
        let partition = work::TextPartition::Unpartitioned;
        let root_typed = index_keys::TextManifestRootKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        };
        let revision = index_lifecycle::TextManifestRevision::new(3).unwrap();
        db.put(
            scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(root_typed)),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    index_id,
                    generation,
                    partition.clone(),
                    revision,
                    1,
                    2,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let page_key = scoped_key(
            scope,
            index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                root: root_typed,
                page: 0,
            }),
        );
        db.put(
            &page_key,
            index_values::encode_manifest_page(
                &work::TextManifestPageValue::try_new(
                    index_id,
                    generation,
                    partition.clone(),
                    0,
                    splits,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        for entity_id in [1_u64, 2_u64] {
            db.put(
                scoped_key(
                    scope,
                    index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                        root: root_typed,
                        entity: index_keys::IndexEntity {
                            kind: index_lifecycle::IndexElementKind::Node,
                            id: index_lifecycle::IndexEntityId::new(entity_id),
                        },
                    }),
                ),
                index_values::encode_text_entity_state(&work::TextEntityStateValue {
                    index_id,
                    generation,
                    partition: partition.clone(),
                    entity_kind: index_lifecycle::IndexElementKind::Node,
                    entity_id: index_lifecycle::IndexEntityId::new(entity_id),
                    logical_version: index_lifecycle::TextLogicalVersion::new(entity_id).unwrap(),
                    live: true,
                }),
            )
            .await
            .unwrap();
        }
        let target = index_keys::TextCompactionTarget::try_new(
            scope,
            record.identity().clone(),
            index_id,
            generation,
            partition.fingerprint(),
            0,
        )
        .unwrap();
        let pointer_key = ManagedIndexKey::Global {
            kind: index_keys::GlobalKey::TextCompactionPointer(target),
        }
        .to_bytes();
        db.put(
            &pointer_key,
            index_values::encode_metadata_value(
                &index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(
                    index_lifecycle::TextCompactionPointerValue { revision },
                ),
            ),
        )
        .await
        .unwrap();

        assert!(compact_once(&db, &store, "db", limits(2)).await.unwrap());

        let page =
            index_values::decode_manifest_page(&db.get(page_key).await.unwrap().unwrap()).unwrap();
        assert_eq!(page.entries().len(), 1);
        assert!(db.get(pointer_key).await.unwrap().is_none());
    }
}
