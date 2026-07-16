//! Production Active-text request composition for graph mutations.
//!
//! This module is the sole executable-mutation bridge into the V2 text
//! request boundary. It prepares the authoritative graph property row, every
//! hidden-build delta, and all Active effects before publication or SlateDB
//! staging. Upload-bearing requests require the transaction-captured
//! coordinator/writer authority; upload-free requests remain valid without it.
//! The resulting publication and exact proof set stay owned by the transaction
//! context until its physical commit outcome is resolved.

use slatedb::DbTransaction;

use super::*;

impl<'db> ExecutionContext<'db> {
    /// Publishes and stages one complete text-aware graph-row transition.
    ///
    /// Callers must invoke this before buffering any other write for the same
    /// entity mutation. All fallible preparation and object publication then
    /// precede the first graph/index write, and a returned error leaves the
    /// caller-owned graph transaction safe to abort without partial staging.
    pub(super) async fn stage_text_graph_mutation(
        &self,
        transaction: &DbTransaction,
        index_context: &mut MutationIndexContext,
        graph: crate::index_v2::text::active_request::ActiveTextGraphMutation,
    ) -> Result<()> {
        let has_active_handles = index_context.text().has_active_handles();
        let prepared = crate::index_v2::text::active_request::prepare_active_text_mutation(
            transaction,
            graph,
            index_context.text(),
            self.db
                .config()
                .db()
                .search_index_backfill()
                .active_text_mutation(),
        )
        .await?;

        if !has_active_handles {
            let staged = crate::index_v2::text::active_request::stage_active_text_mutation(
                transaction,
                &prepared,
                &[],
            )
            .await?;
            if !staged.is_empty() {
                return Err(HelixDbError::InvariantViolation(
                    "text request without Active handles staged an Active proof".to_string(),
                ));
            }
            return Ok(());
        }

        let publication = if prepared.requires_publication() {
            let Some((coordinator, writer_epoch)) = index_context.active_text_runtime().ready()
            else {
                return Err(HelixDbError::IndexLifecycleUnavailable {
                    family: crate::error::IndexFamily::Text,
                    reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
                });
            };
            crate::index_v2::text::active_publication::publish_active_text_mutation(
                self.writer()?.db(),
                coordinator,
                self.db.blob_gc_gate(),
                self.db.active_text_mutations(),
                writer_epoch,
                crate::index_v2::MutationId::new_v4(),
                &prepared,
            )
            .await
            .map_err(
                crate::index_v2::text::active_publication::ActiveTextPublicationError::into_database_error,
            )?
        } else {
            crate::index_v2::text::active_publication::ActiveTextPublication::without_uploads()
        };
        let staged = crate::index_v2::text::active_request::stage_active_text_mutation(
            transaction,
            &prepared,
            publication.uploaded(),
        )
        .await?;
        index_context
            .active_text_outbox_mut()
            .retain(publication, staged)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use helix_ast::value::PropertyValue;
    use helix_planner::context;
    use slatedb::object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
    use slatedb::{Db, IsolationLevel};

    use super::super::super::test_support;
    use super::*;
    use crate::config::{IndexConfig, TextIndexDefinition};
    use crate::encoding::v1::keys::index_v2 as index_keys;
    use crate::encoding::v1::keys::tenant::DataScope;
    use crate::encoding::v1::keys::{DataKeyKind, Key, NodePropertyKey};
    use crate::encoding::v1::values::index_v2 as index_values;
    use crate::index_v2::{
        IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexOperationId,
        IndexRecordV2, IndexRevision, IndexStateTransition, PhysicalGeneration,
        ValidatedDynamicIndexDefinition,
    };

    /// Canonical identity needed to verify one production Active attachment.
    struct ActiveTextSeed {
        index_id: IndexId,
        root: index_keys::TextManifestRootKey,
    }

    /// Encodes one unscoped V2 logical key through the canonical V1 boundary.
    fn scoped_key(logical: index_keys::IndexV2Key) -> Bytes {
        Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::IndexV2(logical),
        }
        .to_bytes()
    }

    /// Seeds one canonical Active text generation before the runtime opens.
    async fn seed_active_text_generation(
        database: &str,
        object_store: Arc<dyn ObjectStore>,
        definition: &TextIndexDefinition,
    ) -> ActiveTextSeed {
        let raw = Db::builder(database, object_store).build().await.unwrap();
        crate::index_v2::repository::bootstrap_writer(&raw)
            .await
            .unwrap();
        let validated = ValidatedDynamicIndexDefinition::try_from(definition.clone()).unwrap();
        let index_id = IndexId::initial();
        let active = IndexRecordV2::building(
            index_id,
            validated,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x41; 16]).unwrap(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let root = index_keys::TextManifestRootKey {
            index_id,
            generation: IndexGenerationId::initial(),
            partition: crate::index_v2::work::TextPartition::Unpartitioned.fingerprint(),
        };
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction
            .put(
                scoped_key(index_keys::IndexV2Key::index_record(
                    active.identity().clone(),
                )),
                index_values::encode_index_record(&active),
            )
            .unwrap();
        transaction
            .put(
                scoped_key(index_keys::IndexV2Key::TextManifestRoot(root)),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    crate::index_v2::work::TextManifestRootValue::empty(
                        index_id,
                        IndexGenerationId::initial(),
                        crate::index_v2::work::TextPartition::Unpartitioned,
                    ),
                )),
            )
            .unwrap();
        transaction.commit().await.unwrap();
        raw.close().await.unwrap();
        ActiveTextSeed { index_id, root }
    }

    /// Returns whether one scoped V2 record-kind lane contains no rows.
    async fn v2_lane_is_empty(db: &Db, kind: index_keys::IndexV2RecordKind) -> bool {
        let prefix = Key::data_prefix(
            DataScope::LegacyUnscoped,
            index_keys::IndexV2Key::logical_prefix(kind),
        );
        let mut rows = db.scan_prefix(&prefix, ..).await.unwrap();
        rows.next().await.unwrap().is_none()
    }

    #[tokio::test]
    async fn production_graph_write_publishes_and_attaches_active_text_atomically() {
        let database = "mutation-active-text-production-entry";
        let token = crate::ProcessLocalDatabaseToken::new(database).unwrap();
        let object_store = token.object_store();
        let definition = TextIndexDefinition::new_node("Document", "body").unwrap();
        let seed =
            seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
        let db = crate::HelixDB::open_with_process_local_token_and_index_config_for_tests(
            token,
            IndexConfig::new().with_text_index(definition),
        )
        .await
        .unwrap();

        let node_id = test_support::add_node_with_properties(
            &db,
            "Document",
            vec![("body", PropertyValue::from("request-owned active text"))],
        )
        .await;
        let graph_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
        }
        .to_bytes();
        assert!(db.inner_db().get(&graph_key).await.unwrap().is_some());

        let root_value = db
            .inner_db()
            .get(scoped_key(index_keys::IndexV2Key::TextManifestRoot(
                seed.root,
            )))
            .await
            .unwrap()
            .unwrap();
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&root_value).unwrap()
        else {
            panic!("Active root key must retain a manifest root value");
        };
        assert_eq!(root.index_id(), seed.index_id);
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 1);

        let page_value = db
            .inner_db()
            .get(scoped_key(index_keys::IndexV2Key::TextManifestPage(
                index_keys::TextManifestPageKey {
                    root: seed.root,
                    page: 0,
                },
            )))
            .await
            .unwrap()
            .unwrap();
        let index_values::IndexV2WorkValue::TextManifestPage(page) =
            index_values::decode_work_value(&page_value).unwrap()
        else {
            panic!("Active page key must retain a manifest page value");
        };
        assert_eq!(page.entries().len(), 1);
        let blob = page.entries()[0].blob();
        let blob_path = crate::search::text::blob_object_store_path(database, *blob.hash());
        let blob_metadata = object_store.head(&blob_path).await.unwrap();
        assert_eq!(blob_metadata.size, blob.size());

        let state_value = db
            .inner_db()
            .get(scoped_key(index_keys::IndexV2Key::TextEntityState(
                index_keys::TextEntityStateKey {
                    root: seed.root,
                    entity: index_keys::IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(node_id),
                    },
                },
            )))
            .await
            .unwrap()
            .unwrap();
        let index_values::IndexV2WorkValue::TextEntityState(state) =
            index_values::decode_work_value(&state_value).unwrap()
        else {
            panic!("Active entity key must retain a live-state value");
        };
        assert!(state.live);
        assert_eq!(state.entity_id, IndexEntityId::new(node_id));
        assert!(
            v2_lane_is_empty(
                db.inner_db().as_ref(),
                index_keys::IndexV2RecordKind::ActiveMutationCommitProof,
            )
            .await
        );
        assert!(
            v2_lane_is_empty(
                db.inner_db().as_ref(),
                index_keys::IndexV2RecordKind::TextUploadIntent,
            )
            .await
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn multi_entity_request_resolves_one_transaction_wide_proof_union() {
        let database = "mutation-active-text-production-proof-union";
        let token = crate::ProcessLocalDatabaseToken::new(database).unwrap();
        let object_store = token.object_store();
        let definition = TextIndexDefinition::new_node("Document", "body").unwrap();
        let seed =
            seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
        let raw = Db::builder(database, Arc::clone(&object_store))
            .build()
            .await
            .unwrap();
        let seed_nodes = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        for node_id in [10, 11] {
            seed_nodes
                .put(
                    Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                    }
                    .to_bytes(),
                    crate::encoding::v1::property::encode_properties(&[Property::string(
                        "$label", "Document",
                    )]),
                )
                .unwrap();
        }
        seed_nodes.commit().await.unwrap();
        raw.close().await.unwrap();
        let db = crate::HelixDB::open_with_process_local_token_and_index_config_for_tests(
            token,
            IndexConfig::new().with_text_index(definition),
        )
        .await
        .unwrap();
        let node_param = test_support::name("nodes");
        let access_id = helix_planner::exec::ExecStepId::new(1).unwrap();
        let plan = test_support::executable(
            helix_planner::ir::PlanKind::Write,
            vec![
                test_support::step(
                    1,
                    Vec::new(),
                    helix_planner::exec::ExecOp::Access {
                        plan: Box::new(helix_planner::exec::ExecAccessPlan::Node(
                            helix_planner::exec::ExecNodeAccessPlan::FromParam {
                                param: node_param.clone(),
                            },
                        )),
                    },
                ),
                test_support::step(
                    2,
                    vec![access_id],
                    helix_planner::exec::ExecOp::Mutation {
                        plan: helix_planner::exec::ExecMutationPlan::SetProperty {
                            name: test_support::name("body"),
                            value: helix_planner::ir::PropertyInputPlan::Value(
                                PropertyValue::from("shared request body"),
                            ),
                        },
                    },
                ),
            ],
            2,
        );
        db.execute(
            &plan,
            context::ParamBindings::default()
                .with_value(node_param, PropertyValue::I64Array(vec![10, 11])),
        )
        .await
        .expect("both Active attachments commit in one graph transaction");

        let root_value = db
            .inner_db()
            .get(scoped_key(index_keys::IndexV2Key::TextManifestRoot(
                seed.root,
            )))
            .await
            .unwrap()
            .unwrap();
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&root_value).unwrap()
        else {
            panic!("Active root key must retain a manifest root value");
        };
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 2);
        let page_value = db
            .inner_db()
            .get(scoped_key(index_keys::IndexV2Key::TextManifestPage(
                index_keys::TextManifestPageKey {
                    root: seed.root,
                    page: 0,
                },
            )))
            .await
            .unwrap()
            .unwrap();
        let index_values::IndexV2WorkValue::TextManifestPage(page) =
            index_values::decode_work_value(&page_value).unwrap()
        else {
            panic!("Active page key must retain a manifest page value");
        };
        assert_eq!(page.entries().len(), 2);
        for node_id in [10, 11] {
            let state_key = scoped_key(index_keys::IndexV2Key::TextEntityState(
                index_keys::TextEntityStateKey {
                    root: seed.root,
                    entity: index_keys::IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(node_id),
                    },
                },
            ));
            assert!(db.inner_db().get(state_key).await.unwrap().is_some());
        }
        assert!(
            v2_lane_is_empty(
                db.inner_db().as_ref(),
                index_keys::IndexV2RecordKind::ActiveMutationCommitProof,
            )
            .await
        );
        assert!(
            v2_lane_is_empty(
                db.inner_db().as_ref(),
                index_keys::IndexV2RecordKind::TextUploadIntent,
            )
            .await
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn shared_topology_rejects_active_upload_before_graph_staging() {
        let database = "mutation-active-text-shared-rejection";
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let definition = TextIndexDefinition::new_node("Document", "body").unwrap();
        seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
        let db = crate::HelixDB::open_with_object_store_and_index_config_for_tests(
            database,
            object_store,
            IndexConfig::new().with_text_index(definition),
        )
        .await
        .unwrap();
        let plan = test_support::executable(
            helix_planner::ir::PlanKind::Write,
            vec![test_support::step(
                1,
                Vec::new(),
                helix_planner::exec::ExecOp::Mutation {
                    plan: helix_planner::exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("Document"),
                        properties: test_support::assignments(vec![(
                            "body",
                            PropertyValue::from("must not stage"),
                        )]),
                    },
                },
            )],
            1,
        );

        assert!(matches!(
            db.execute(&plan, context::ParamBindings::default()).await,
            Err(HelixDbError::IndexLifecycleUnavailable {
                family: crate::error::IndexFamily::Text,
                reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
            })
        ));
        let graph_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(0)),
        }
        .to_bytes();
        assert!(db.inner_db().get(graph_key).await.unwrap().is_none());
        db.close().await.unwrap();
    }
}
