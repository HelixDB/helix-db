//! Transactional application of configured text-index changes.

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::TextIndexDefinition;
use crate::error::{HelixDbError, Result};
use crate::execution::interpreter::runtime_context::ExecutionContext;
use crate::search;

use super::change::TextIndexChange;
use super::document::TextIndexedDocument;
use super::outcome::TextIndexMaintenanceOutcome;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn apply_text_index_change(
        &self,
        txn: &DbTransaction,
        definition: &TextIndexDefinition,
        change: TextIndexChange,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let mut outcome = TextIndexMaintenanceOutcome::default();
        match change {
            TextIndexChange::None => {}
            TextIndexChange::Remove { old } => {
                let index_name = old.index_name.clone();
                if self.mark_text_document_dead(txn, old).await? {
                    outcome.record(index_name);
                }
            }
            TextIndexChange::Upsert { new } => {
                if let Some(index_name) = self.append_text_document(txn, definition, new).await? {
                    outcome.record(index_name);
                }
            }
            TextIndexChange::Replace { old, new } => {
                let old_index_name = old.index_name.clone();
                if old.index_name.as_str() != new.index_name.as_str()
                    && self.mark_text_document_dead(txn, old).await?
                {
                    outcome.record(old_index_name);
                }
                if let Some(index_name) = self.append_text_document(txn, definition, new).await? {
                    outcome.record(index_name);
                }
            }
        }
        Ok(outcome)
    }

    async fn append_text_document(
        &self,
        txn: &DbTransaction,
        definition: &TextIndexDefinition,
        document: TextIndexedDocument,
    ) -> Result<Option<String>> {
        let existing_manifest =
            search::text::load_manifest_scoped(txn, self.tenant_scope, &document.index_name)
                .await?;
        let logical_version = self
            .next_text_logical_version(txn, &document.index_name, existing_manifest.is_some())
            .await?;
        let index_name = document.index_name.clone();
        let entity_id = document.input.entity_id;
        let input = document.input.with_logical_version(logical_version);
        let Some(new_manifest) = search::text::persist_documents_as_manifest(
            self.db.object_store(),
            self.db.path(),
            definition,
            &document.index_name,
            &[input],
        )
        .await?
        else {
            return Ok(None);
        };

        let manifest = match existing_manifest {
            Some(existing) => append_text_manifest_split(existing, &new_manifest),
            None => new_manifest,
        };
        let encoded_manifest = serde_json::to_vec(&manifest).map_err(|err| {
            HelixDbError::Config(format!("failed to encode updated text manifest: {err}"))
        })?;
        txn.put(
            search::make_text_index_manifest_key_scoped(self.tenant_scope, &document.index_name),
            Bytes::from(encoded_manifest),
        )?;
        self.write_text_live_state(
            txn,
            &document.index_name,
            entity_id,
            search::text::TextIndexLiveState::live(logical_version),
        )
        .await?;
        Ok(Some(index_name))
    }

    async fn mark_text_document_dead(
        &self,
        txn: &DbTransaction,
        document: TextIndexedDocument,
    ) -> Result<bool> {
        let manifest_exists =
            search::text::load_manifest_scoped(txn, self.tenant_scope, &document.index_name)
                .await?
                .is_some();
        let Some(logical_version) = self
            .next_existing_text_logical_version(txn, &document.index_name, manifest_exists)
            .await?
        else {
            return Ok(false);
        };
        self.write_text_live_state(
            txn,
            &document.index_name,
            document.input.entity_id,
            search::text::TextIndexLiveState::dead(logical_version),
        )
        .await?;
        Ok(true)
    }

    async fn next_existing_text_logical_version(
        &self,
        txn: &DbTransaction,
        index_name: &str,
        manifest_exists: bool,
    ) -> Result<Option<u64>> {
        let Some(current) = text_version_counter(txn, self.tenant_scope, index_name).await? else {
            if manifest_exists {
                return Err(unsupported_text_manifest_error(index_name));
            }
            return Ok(None);
        };
        Ok(Some(increment_text_version(index_name, current)?))
    }

    async fn next_text_logical_version(
        &self,
        txn: &DbTransaction,
        index_name: &str,
        manifest_exists: bool,
    ) -> Result<u64> {
        match text_version_counter(txn, self.tenant_scope, index_name).await? {
            Some(current) => increment_text_version(index_name, current),
            None if manifest_exists => Err(unsupported_text_manifest_error(index_name)),
            None => Ok(1),
        }
    }

    async fn write_text_live_state(
        &self,
        txn: &DbTransaction,
        index_name: &str,
        entity_id: u64,
        state: search::text::TextIndexLiveState,
    ) -> Result<()> {
        let encoded_version = serde_json::to_vec(&state.logical_version).map_err(|err| {
            HelixDbError::Config(format!("failed to encode text version counter: {err}"))
        })?;
        let encoded_state = search::text::encode_live_state_bytes(&state)?;
        txn.put(
            search::make_text_index_version_counter_key_scoped(self.tenant_scope, index_name),
            Bytes::from(encoded_version),
        )?;
        txn.put(
            search::make_text_index_live_state_key_scoped(self.tenant_scope, index_name, entity_id),
            Bytes::from(encoded_state),
        )?;
        Ok(())
    }

    pub(in crate::execution::interpreter) async fn compact_text_indexes_after_commit(
        &self,
        outcome: TextIndexMaintenanceOutcome,
        _indexes: &crate::config::RuntimeIndexCatalog,
    ) {
        if outcome.is_empty() {
            return;
        }
        let Ok(writer) = self.writer() else {
            tracing::warn!(
                "text compaction skipped after mutation because writer storage is unavailable"
            );
            return;
        };

        for index_name in outcome.indexes() {
            let manifest = match search::text::load_manifest_scoped(
                writer.db(),
                self.tenant_scope,
                index_name,
            )
            .await
            {
                Ok(Some(manifest)) => manifest,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(
                        index_name,
                        error = %err,
                        "text compaction skipped after mutation because manifest load failed"
                    );
                    continue;
                }
            };
            if let Err(err) = search::text::compact_manifest_merge_only_scoped(
                writer.db(),
                self.tenant_scope,
                self.db.object_store(),
                self.db.path(),
                &manifest,
            )
            .await
            {
                tracing::warn!(
                    index_name,
                    error = %err,
                    "text compaction failed after mutation"
                );
            }
        }
    }
}

fn append_text_manifest_split(
    existing: search::text::TextIndexGenerationManifest,
    new_manifest: &search::text::TextIndexGenerationManifest,
) -> search::text::TextIndexGenerationManifest {
    let mut manifest = existing;
    manifest.generation_id = new_manifest.generation_id.clone();
    manifest
        .splits
        .push(new_manifest.primary_split_ref().clone());
    manifest
}

async fn text_version_counter(
    txn: &DbTransaction,
    scope: crate::encoding::keys::tenant::DataScope,
    index_name: &str,
) -> Result<Option<u64>> {
    let Some(bytes) = txn
        .get(&search::make_text_index_version_counter_key_scoped(
            scope, index_name,
        ))
        .await?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to decode text version counter for '{index_name}': {err}"
        ))
    })
}

fn increment_text_version(index_name: &str, current: u64) -> Result<u64> {
    current.checked_add(1).ok_or_else(|| {
        HelixDbError::InvariantViolation(format!(
            "text version counter overflowed for '{index_name}'"
        ))
    })
}

fn unsupported_text_manifest_error(index_name: &str) -> HelixDbError {
    HelixDbError::Config(format!(
        "text index '{index_name}' uses an unsupported manifest without live-state metadata; migrate the database before retrying"
    ))
}

#[cfg(test)]
mod tests {
    use helix_planner::context;
    use slatedb::IsolationLevel;

    use super::super::super::super::test_support;
    use super::*;

    fn definition() -> TextIndexDefinition {
        TextIndexDefinition::new_node("Doc", "body").expect("valid text definition")
    }

    fn document(index_name: &str, entity_id: u64) -> TextIndexedDocument {
        TextIndexedDocument {
            index_name: index_name.to_string(),
            input: search::text::TextDocumentInput::new(entity_id, "indexed text".to_string()),
            logical_partition_identity: vec![0],
        }
    }

    #[tokio::test]
    async fn text_apply_version_contracts_cover_missing_unsupported_malformed_and_overflow_states()
    {
        let db = test_support::open_db("text-apply-version-contracts").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");

        assert!(context
            .apply_text_index_change(&txn, &definition(), TextIndexChange::None,)
            .await
            .unwrap()
            .is_empty());
        assert!(!context
            .mark_text_document_dead(&txn, document("missing", 1))
            .await
            .unwrap());
        assert_eq!(
            context
                .next_existing_text_logical_version(&txn, "missing", false)
                .await
                .unwrap(),
            None
        );

        let unsupported = context
            .next_existing_text_logical_version(&txn, "unsupported", true)
            .await
            .unwrap_err();
        assert!(unsupported
            .to_string()
            .contains("migrate the database before retrying"));
        let unsupported = context
            .next_text_logical_version(&txn, "unsupported", true)
            .await
            .unwrap_err();
        assert!(unsupported
            .to_string()
            .contains("migrate the database before retrying"));

        txn.put(
            search::make_text_index_version_counter_key("malformed"),
            Bytes::from_static(b"not-json"),
        )
        .unwrap();
        let malformed = text_version_counter(
            &txn,
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
            "malformed",
        )
        .await
        .unwrap_err();
        assert!(malformed
            .to_string()
            .contains("failed to decode text version counter"));

        assert!(matches!(
            increment_text_version("overflow", u64::MAX),
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("counter overflowed")
        ));
        assert!(unsupported_text_manifest_error("unsupported")
            .to_string()
            .contains("migrate the database before retrying"));
    }

    #[tokio::test]
    async fn post_commit_compaction_skips_unavailable_missing_and_malformed_manifests() {
        let reader_config = test_support::in_memory_config("text-apply-reader-compaction");
        let writer = test_support::open_db_with_config(reader_config.clone()).await;
        drop(writer);
        let reader = test_support::open_reader_with_config(reader_config).await;
        let reader_context = ExecutionContext::new(&reader, context::ParamBindings::default());
        let mut reader_outcome = TextIndexMaintenanceOutcome::default();
        reader_outcome.record("missing");
        reader_context
            .compact_text_indexes_after_commit(
                reader_outcome,
                &crate::config::RuntimeIndexCatalog::default(),
            )
            .await;

        let db = test_support::open_db("text-apply-writer-compaction").await;
        let writer_context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut missing_outcome = TextIndexMaintenanceOutcome::default();
        missing_outcome.record("missing");
        writer_context
            .compact_text_indexes_after_commit(
                missing_outcome,
                &crate::config::RuntimeIndexCatalog::default(),
            )
            .await;

        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        txn.put(
            search::make_text_index_manifest_key("malformed"),
            Bytes::from_static(b"not-json"),
        )
        .unwrap();
        txn.commit().await.expect("malformed manifest commits");
        let mut malformed_outcome = TextIndexMaintenanceOutcome::default();
        malformed_outcome.record("malformed");
        writer_context
            .compact_text_indexes_after_commit(
                malformed_outcome,
                &crate::config::RuntimeIndexCatalog::default(),
            )
            .await;

        let mut missing_blob_manifest = search::text::TextIndexGenerationManifest::new_split(
            "missing-blob",
            "generation",
            crate::config::TextAnalyzerKind::Standard,
            false,
            search::text::TextSplitRef {
                blob: search::text::TextBlobRef {
                    sha256: [7; 32],
                    size_bytes: 1,
                },
                footer_offset: 0,
                footer_len: 0,
                hotcache_len: 0,
                total_size_bytes: 1,
            },
        );
        missing_blob_manifest
            .splits
            .push(search::text::TextSplitRef {
                blob: search::text::TextBlobRef {
                    sha256: [8; 32],
                    size_bytes: 1,
                },
                footer_offset: 0,
                footer_len: 0,
                hotcache_len: 0,
                total_size_bytes: 1,
            });
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        txn.put(
            search::make_text_index_manifest_key("missing-blob"),
            Bytes::from(serde_json::to_vec(&missing_blob_manifest).unwrap()),
        )
        .unwrap();
        txn.commit().await.expect("missing-blob manifest commits");
        let mut missing_blob_outcome = TextIndexMaintenanceOutcome::default();
        missing_blob_outcome.record("missing-blob");
        writer_context
            .compact_text_indexes_after_commit(
                missing_blob_outcome,
                &crate::config::RuntimeIndexCatalog::default(),
            )
            .await;
    }
}
