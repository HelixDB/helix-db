//! Fail-closed physical generation authority during the V2 lifecycle cutover.
//!
//! Text and vector search may only open physical rows after the durable V2
//! repository has resolved an active generation. Keeping this boundary typed
//! prevents configured catalog entries from accidentally acting as physical
//! ownership while the replacement repository is being installed.

use super::*;
use crate::config::{TextIndexDefinition, VectorIndexDefinition};
use crate::encoding::v2::values::property::encode_index_partition_value;
use crate::error::{IndexFamily, IndexLifecycleUnavailableReason};
use crate::index_lifecycle::work::{TextPartition, VectorTenantPartition};
use crate::index_lifecycle::{
    ActiveIndexHandle, ValidatedTextIndexDefinition, ValidatedVectorIndexDefinition,
    VectorPhysicalLayout,
};
use crate::search::vector::{Distance, ValidatedVectorGenerationHandle};
#[cfg(test)]
use crate::HelixStorage;

/// Exhaustive V2 authority for one requested vector partition.
///
/// Unlike the temporary text authority, this type has no legacy/display-name
/// variant, so production vector search cannot represent a descriptorless read.
pub(super) enum VectorSearchAuthority<T> {
    /// The active partitioned generation has no mapping for this tenant.
    AbsentManagedPartition,
    /// The V2 repository validated the exact physical generation.
    Managed(T),
}

/// Normalized vector partition selection before canonical Active resolution.
enum RequestedVectorPartition {
    /// The definition owns one physical namespace directly.
    Unpartitioned,
    /// The request selected one canonical tenant mapping.
    Tenant(VectorTenantPartition),
    /// A nullable tenant expression selected no physical partition.
    Absent,
}

/// Physical vector namespace resolved from canonical state in the request view.
pub(super) struct ResolvedVectorGenerationHandle {
    physical: ValidatedVectorGenerationHandle,
}

impl ResolvedVectorGenerationHandle {
    /// Borrows the descriptor-bound physical vector generation.
    pub(super) const fn physical(&self) -> &ValidatedVectorGenerationHandle {
        &self.physical
    }

    #[cfg(test)]
    fn physical_index_id(&self) -> u64 {
        self.physical.physical_index_id()
    }

    /// Pairs a lower-level storage fixture with its validated generation.
    #[cfg(test)]
    pub(super) const fn for_storage_test(physical: ValidatedVectorGenerationHandle) -> Self {
        Self { physical }
    }
}

impl<T> VectorSearchAuthority<T> {
    /// Borrows a validated handle while preserving its authority state.
    pub(super) const fn as_ref(&self) -> VectorSearchAuthority<&T> {
        match self {
            Self::AbsentManagedPartition => VectorSearchAuthority::AbsentManagedPartition,
            Self::Managed(handle) => VectorSearchAuthority::Managed(handle),
        }
    }
}

/// Exhaustive V2 authority for one requested text partition.
///
/// There is deliberately no display-name or legacy variant: only an Active
/// canonical record may authorize V2 manifest, state, cache, or blob access.
pub(super) enum TextSearchAuthority<T> {
    /// The Active partitioned definition has no normalized tenant here.
    AbsentManagedPartition,
    /// The V2 repository validated the exact physical generation.
    Managed(T),
}

/// Normalized text partition selection before canonical Active resolution.
enum RequestedTextPartition {
    /// The request selected one canonical physical partition.
    Present(TextPartition),
    /// A nullable tenant expression selected no physical partition.
    Absent,
}

impl<T> TextSearchAuthority<T> {
    /// Borrows a validated handle while preserving its authority state.
    pub(super) const fn as_ref(&self) -> TextSearchAuthority<&T> {
        match self {
            Self::AbsentManagedPartition => TextSearchAuthority::AbsentManagedPartition,
            Self::Managed(handle) => TextSearchAuthority::Managed(handle),
        }
    }
}

/// Active text generation and partition resolved in the stable request view.
pub(super) struct ResolvedTextGenerationHandle {
    physical: crate::index_lifecycle::text::serving::ActiveTextServingAuthority,
    partition: TextPartition,
}

impl ResolvedTextGenerationHandle {
    /// Borrows the family-refined authority used for root ownership checks.
    pub(super) const fn physical(
        &self,
    ) -> &crate::index_lifecycle::text::serving::ActiveTextServingAuthority {
        &self.physical
    }

    /// Borrows the normalized text partition selected by the request.
    pub(super) const fn partition(&self) -> &TextPartition {
        &self.partition
    }
}

impl<'db> ExecutionContext<'db> {
    /// Resolves one vector generation from active canonical state.
    ///
    /// The active record and optional tenant mapping are read through the same
    /// request view used by the physical search. Missing partition mappings are
    /// empty results; reads never allocate a mapping.
    pub(super) async fn managed_vector_generation<D: Distance>(
        &self,
        definition: &VectorIndexDefinition,
        tenant_value: Option<&DbPropertyValue>,
    ) -> Result<VectorSearchAuthority<ResolvedVectorGenerationHandle>> {
        let requested = ValidatedVectorIndexDefinition::try_from_runtime(definition)?;
        let partition = match (requested.tenant_property(), tenant_value) {
            (None, None) => RequestedVectorPartition::Unpartitioned,
            (Some(_), Some(value)) => match crate::search::text::normalize_tenant_value(value) {
                Some(value) => RequestedVectorPartition::Tenant(
                    VectorTenantPartition::try_new(encode_index_partition_value(value))
                        .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?,
                ),
                None => RequestedVectorPartition::Absent,
            },
            (None, Some(_)) | (Some(_), None) => {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "vector request tenant shape disagrees with its canonical definition"
                        .to_string(),
                ));
            }
        };

        if let Some(active_write) = self.active_write_tx() {
            return resolve_vector_generation_in_view::<D>(
                self,
                &active_write.txn,
                self.tenant_scope,
                &requested,
                partition,
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return resolve_vector_generation_in_view::<D>(
                self,
                view,
                self.tenant_scope,
                &requested,
                partition,
            )
            .await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    resolve_vector_generation_in_view::<D>(
                        self,
                        reader.as_ref(),
                        self.tenant_scope,
                        &requested,
                        partition,
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    resolve_vector_generation_in_view::<D>(
                        self,
                        writer.db(),
                        self.tenant_scope,
                        &requested,
                        partition,
                    )
                    .await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "vector generation resolution escaped its request read view".to_string(),
        ))
    }

    /// Resolves one text generation from active canonical state.
    ///
    /// Tenant normalization selects a partition without creating any durable
    /// row. A missing normalized tenant is an empty managed partition; every
    /// present partition remains generation-owned.
    pub(super) async fn managed_text_generation(
        &self,
        definition: &TextIndexDefinition,
        tenant_value: Option<&DbPropertyValue>,
    ) -> Result<TextSearchAuthority<ResolvedTextGenerationHandle>> {
        let requested = ValidatedTextIndexDefinition::try_from_runtime(definition)?;
        let partition = match (requested.tenant_property(), tenant_value) {
            (None, None) => RequestedTextPartition::Present(TextPartition::Unpartitioned),
            (Some(_), Some(value)) => match crate::search::text::normalize_tenant_value(value) {
                Some(value) => RequestedTextPartition::Present(
                    TextPartition::try_tenant_value(encode_index_partition_value(value))
                        .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?,
                ),
                None => RequestedTextPartition::Absent,
            },
            (None, Some(_)) | (Some(_), None) => {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "text request tenant shape disagrees with its canonical definition".to_string(),
                ));
            }
        };

        self.resolve_text_generation(&requested, partition).await
    }

    /// Resolves a normalized text partition through the request's stable view.
    async fn resolve_text_generation(
        &self,
        requested: &ValidatedTextIndexDefinition,
        partition: RequestedTextPartition,
    ) -> Result<TextSearchAuthority<ResolvedTextGenerationHandle>> {
        if let Some(active_write) = self.active_write_tx() {
            return resolve_text_generation_in_view(
                self,
                &active_write.txn,
                self.tenant_scope,
                requested,
                partition,
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            return resolve_text_generation_in_view(
                self,
                view,
                self.tenant_scope,
                requested,
                partition,
            )
            .await;
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => {
                    resolve_text_generation_in_view(
                        self,
                        reader.as_ref(),
                        self.tenant_scope,
                        requested,
                        partition,
                    )
                    .await
                }
                HelixStorage::Writer(writer) => {
                    resolve_text_generation_in_view(
                        self,
                        writer.db(),
                        self.tenant_scope,
                        requested,
                        partition,
                    )
                    .await
                }
            }
        }
        #[cfg(not(test))]
        Err(HelixDbError::InvariantViolation(
            "text generation resolution escaped its request read view".to_string(),
        ))
    }
}

/// Revalidates one Active text record and binds its exact request partition.
async fn resolve_text_generation_in_view(
    _context: &ExecutionContext<'_>,
    reader: &(impl slatedb::DbReadOps + Sync),
    scope: crate::encoding::v2::keys::scope::DataScope,
    requested: &ValidatedTextIndexDefinition,
    partition: RequestedTextPartition,
) -> Result<TextSearchAuthority<ResolvedTextGenerationHandle>> {
    let Some(active) = crate::index_lifecycle::repository::load_active_handle(
        reader,
        scope,
        &requested.identity(),
    )
    .await?
    else {
        return Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Text,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        });
    };
    let ActiveIndexHandle::Text { definition, .. } = &active else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "text generation resolver received another active index family".to_string(),
        ));
    };
    if definition.as_ref() != requested {
        return Err(HelixDbError::IndexCatalogCorruption(
            "runtime text definition disagrees with its active canonical record".to_string(),
        ));
    }
    let RequestedTextPartition::Present(partition) = partition else {
        return Ok(TextSearchAuthority::AbsentManagedPartition);
    };
    let physical =
        crate::index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
            &active,
        )?;
    Ok(TextSearchAuthority::Managed(ResolvedTextGenerationHandle {
        physical,
        partition,
    }))
}

/// Revalidates one active record and resolves its exact physical namespace.
async fn resolve_vector_generation_in_view<D: Distance>(
    _context: &ExecutionContext<'_>,
    reader: &(impl slatedb::DbReadOps + Sync),
    scope: crate::encoding::v2::keys::scope::DataScope,
    requested: &ValidatedVectorIndexDefinition,
    partition: RequestedVectorPartition,
) -> Result<VectorSearchAuthority<ResolvedVectorGenerationHandle>> {
    let Some(active) = crate::index_lifecycle::repository::load_active_handle(
        reader,
        scope,
        &requested.identity(),
    )
    .await?
    else {
        return Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Vector,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        });
    };
    let ActiveIndexHandle::Vector {
        scope,
        index_id,
        generation,
        definition,
        layout,
        ..
    } = &active
    else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "vector generation resolver received another active index family".to_string(),
        ));
    };
    if definition.as_ref() != requested {
        return Err(HelixDbError::IndexCatalogCorruption(
            "runtime vector definition disagrees with its active canonical record".to_string(),
        ));
    }
    let physical_index_id = match (layout, &partition) {
        (
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
            RequestedVectorPartition::Unpartitioned,
        ) => Ok(Some(*physical_index_id)),
        (VectorPhysicalLayout::Partitioned, RequestedVectorPartition::Tenant(partition)) => {
            crate::index_lifecycle::repository::load_vector_partition_mapping(
                reader,
                *scope,
                *index_id,
                *generation,
                *layout,
                partition,
            )
            .await
        }
        (VectorPhysicalLayout::Partitioned, RequestedVectorPartition::Absent) => Ok(None),
        (
            VectorPhysicalLayout::Unpartitioned { .. },
            RequestedVectorPartition::Tenant(_) | RequestedVectorPartition::Absent,
        )
        | (VectorPhysicalLayout::Partitioned, RequestedVectorPartition::Unpartitioned) => {
            Err(HelixDbError::IndexCatalogCorruption(
                "vector partition authority disagrees with its active physical layout".to_string(),
            ))
        }
    }?;
    let Some(physical_index_id) = physical_index_id else {
        return Ok(VectorSearchAuthority::AbsentManagedPartition);
    };
    let physical =
        ValidatedVectorGenerationHandle::try_from_active::<D>(&active, physical_index_id)
            .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?;
    Ok(VectorSearchAuthority::Managed(
        ResolvedVectorGenerationHandle { physical },
    ))
}

#[cfg(test)]
mod tests {
    use helix_planner::context::ParamBindings;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::config::TextAnalyzerKind;
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::encoding::v2::keys::ManagedIndexKey;
    use crate::encoding::v2::keys::ScopedKey;
    use crate::encoding::v2::values::encode_index_record;
    use crate::index_lifecycle::{
        IndexGenerationId, IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition,
        PhysicalGeneration, ValidatedDynamicIndexDefinition, VectorGenerationDescriptor,
    };
    use crate::search::vector::VectorDistanceMetric;

    /// Persists one active vector record and its optional tenant mapping.
    async fn seed_active_vector(
        db: &HelixDB,
        definition: &ValidatedVectorIndexDefinition,
        tenant: Option<&VectorTenantPartition>,
    ) -> (IndexRecordV2, crate::index_lifecycle::VectorPhysicalIndexId) {
        let transaction = db
            .inner_db()
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let index_id = crate::index_lifecycle::repository::allocate_index_id(&transaction)
            .await
            .unwrap();
        let generation = IndexGenerationId::initial();
        let (layout, physical_index_id) = match tenant {
            None => {
                let physical_index_id =
                    crate::index_lifecycle::repository::allocate_vector_physical_id(&transaction)
                        .await
                        .unwrap();
                (
                    VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    physical_index_id,
                )
            }
            Some(tenant) => {
                let layout = VectorPhysicalLayout::Partitioned;
                let physical_index_id =
                    crate::index_lifecycle::repository::stage_vector_partition_mapping(
                        &transaction,
                        DataScope::LegacyUnscoped,
                        index_id,
                        generation,
                        layout,
                        tenant,
                    )
                    .await
                    .unwrap();
                (layout, physical_index_id)
            }
        };
        let record = IndexRecordV2::building(
            index_id,
            ValidatedDynamicIndexDefinition::Vector(definition.clone()),
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation,
                layout,
                descriptor: VectorGenerationDescriptor::for_definition(definition),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        transaction
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::index_record(record.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&record),
            )
            .unwrap();
        transaction.commit().await.unwrap();
        (record, physical_index_id)
    }

    /// Persists and registers one Active text generation.
    async fn seed_active_text(
        db: &HelixDB,
        definition: &ValidatedTextIndexDefinition,
    ) -> IndexRecordV2 {
        let transaction = db
            .inner_db()
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let index_id = crate::index_lifecycle::repository::allocate_index_id(&transaction)
            .await
            .unwrap();
        let generation = IndexGenerationId::initial();
        let record = IndexRecordV2::building(
            index_id,
            ValidatedDynamicIndexDefinition::Text(definition.clone()),
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
                    kind: ScopedKey::index_record(record.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&record),
            )
            .unwrap();
        transaction.commit().await.unwrap();
        record
    }

    #[test]
    fn absent_authority_borrows_without_inventing_a_generation() {
        assert!(matches!(
            VectorSearchAuthority::<ResolvedVectorGenerationHandle>::AbsentManagedPartition
                .as_ref(),
            VectorSearchAuthority::AbsentManagedPartition
        ));
        assert!(matches!(
            TextSearchAuthority::<ResolvedTextGenerationHandle>::AbsentManagedPartition.as_ref(),
            TextSearchAuthority::AbsentManagedPartition
        ));
    }

    #[tokio::test]
    async fn generation_resolution_covers_direct_and_transaction_owned_views() {
        let vector = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            2,
            VectorDistanceMetric::Cosine,
        )
        .unwrap();
        let text = TextIndexDefinition::new_node("Document", "body").unwrap();
        let validated_vector = ValidatedVectorIndexDefinition::try_from_runtime(&vector).unwrap();
        let validated_text = ValidatedTextIndexDefinition::try_from_runtime(&text).unwrap();
        let token = crate::ProcessLocalDatabaseToken::new("generation-direct-runtime-views")
            .expect("process-local token validates");
        let writer = HelixDB::open_with_process_local_token_for_tests(token.clone())
            .await
            .unwrap();
        seed_active_vector(&writer, &validated_vector, None).await;
        seed_active_text(&writer, &validated_text).await;
        writer
            .refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .unwrap();

        let direct_writer = ExecutionContext::new(&writer, ParamBindings::default());
        assert!(matches!(
            direct_writer
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &vector, None,
                )
                .await
                .unwrap(),
            VectorSearchAuthority::Managed(_)
        ));
        assert!(matches!(
            direct_writer
                .managed_text_generation(&text, None)
                .await
                .unwrap(),
            TextSearchAuthority::Managed(_)
        ));
        let mut transaction = ExecutionContext::new(&writer, ParamBindings::default());
        transaction.enable_request_write_scope().await.unwrap();
        assert!(matches!(
            transaction
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &vector, None,
                )
                .await
                .unwrap(),
            VectorSearchAuthority::Managed(_)
        ));
        assert!(matches!(
            transaction
                .managed_text_generation(&text, None)
                .await
                .unwrap(),
            TextSearchAuthority::Managed(_)
        ));
        transaction.abort_request_write_scope();

        let unexpected_tenant = DbPropertyValue::String("unexpected".to_string());
        assert!(matches!(
            direct_writer
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &vector,
                    Some(&unexpected_tenant),
                )
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("tenant shape disagrees")
        ));
        assert!(matches!(
            direct_writer
                .managed_text_generation(&text, Some(&unexpected_tenant))
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("tenant shape disagrees")
        ));

        writer.inner_db().flush().await.unwrap();
        let reader = HelixDB::open_reader(crate::HelixDbSource::InMemoryToken { token })
            .await
            .unwrap();
        let direct_reader = ExecutionContext::new(&reader, ParamBindings::default());
        assert!(matches!(
            direct_reader
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &vector, None,
                )
                .await
                .unwrap(),
            VectorSearchAuthority::Managed(_)
        ));
        assert!(matches!(
            direct_reader
                .managed_text_generation(&text, None)
                .await
                .unwrap(),
            TextSearchAuthority::Managed(_)
        ));
    }

    #[tokio::test]
    async fn partitioned_resolution_is_exact_read_only_and_definition_bound() {
        let runtime = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            2,
            VectorDistanceMetric::Cosine,
        )
        .unwrap()
        .with_tenant_property("tenant_id")
        .unwrap();
        let canonical = ValidatedVectorIndexDefinition::try_from_runtime(&runtime).unwrap();
        let token =
            crate::ProcessLocalDatabaseToken::new("vector-generation-partitioned-resolution")
                .unwrap();
        let db = HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .unwrap();
        let tenant_value = DbPropertyValue::String("acme".to_string());
        let tenant =
            VectorTenantPartition::try_new(encode_index_partition_value(&tenant_value)).unwrap();
        let (_, physical_index_id) = seed_active_vector(&db, &canonical, Some(&tenant)).await;
        db.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .unwrap();
        let next_physical_id =
            crate::index_lifecycle::repository::peek_vector_physical_id(db.inner_db().as_ref())
                .await
                .unwrap();

        let mut context = ExecutionContext::new(&db, ParamBindings::default());
        context.enable_request_read_view().await.unwrap();
        let resolved = context
            .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                &runtime,
                Some(&tenant_value),
            )
            .await
            .unwrap();
        let VectorSearchAuthority::Managed(resolved) = resolved else {
            panic!("existing tenant mapping must resolve");
        };
        assert_eq!(resolved.physical_index_id(), physical_index_id.get());

        let absent = context
            .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                &runtime,
                Some(&DbPropertyValue::String("missing".to_string())),
            )
            .await
            .unwrap();
        assert!(matches!(
            absent,
            VectorSearchAuthority::AbsentManagedPartition
        ));
        assert_eq!(
            crate::index_lifecycle::repository::peek_vector_physical_id(db.inner_db().as_ref())
                .await
                .unwrap(),
            next_physical_id
        );

        assert!(matches!(
            context
                .managed_vector_generation::<crate::search::vector::distance::Euclidean>(
                    &runtime,
                    Some(&tenant_value),
                )
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("semantics")
        ));
        let conflicting = runtime.clone().with_m(8).unwrap();
        assert!(matches!(
            context
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &conflicting,
                    Some(&tenant_value),
                )
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("definition disagrees")
        ));
    }

    #[tokio::test]
    async fn generation_resolution_is_request_stable_across_drop_publication() {
        let runtime = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            2,
            VectorDistanceMetric::Cosine,
        )
        .unwrap();
        let canonical = ValidatedVectorIndexDefinition::try_from_runtime(&runtime).unwrap();
        let token = crate::ProcessLocalDatabaseToken::new("vector-generation-stable-drop").unwrap();
        let db = HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .unwrap();
        let (active, _) = seed_active_vector(&db, &canonical, None).await;
        db.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .unwrap();
        let mut stable = ExecutionContext::new(&db, ParamBindings::default());
        stable.enable_request_read_view().await.unwrap();
        assert!(matches!(
            stable
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &runtime, None,
                )
                .await
                .unwrap(),
            VectorSearchAuthority::Managed(_)
        ));

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::new_v4(),
            })
            .unwrap();
        db.inner_db()
            .put(
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::index_record(dropping.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&dropping),
            )
            .await
            .unwrap();

        assert!(matches!(
            stable
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &runtime, None,
                )
                .await
                .unwrap(),
            VectorSearchAuthority::Managed(_)
        ));
        let current = ExecutionContext::new(&db, ParamBindings::default());
        assert!(matches!(
            current
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &runtime, None,
                )
                .await,
            Err(HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Vector,
                reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            })
        ));
    }

    #[tokio::test]
    async fn text_resolution_is_definition_and_partition_bound() {
        let runtime = TextIndexDefinition::new_node("Document", "body")
            .unwrap()
            .with_tenant_property("tenant_id")
            .unwrap();
        let canonical = ValidatedTextIndexDefinition::try_from_runtime(&runtime).unwrap();
        let token = crate::ProcessLocalDatabaseToken::new("text-generation-resolution").unwrap();
        let db = HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .unwrap();
        let active = seed_active_text(&db, &canonical).await;
        db.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .unwrap();
        let tenant_value = DbPropertyValue::String("acme".to_string());
        let expected_partition =
            TextPartition::try_tenant_value(encode_index_partition_value(&tenant_value)).unwrap();
        let mut context = ExecutionContext::new(&db, ParamBindings::default());
        context.enable_request_read_view().await.unwrap();

        let resolved = context
            .managed_text_generation(&runtime, Some(&tenant_value))
            .await
            .unwrap();
        let TextSearchAuthority::Managed(resolved) = resolved else {
            panic!("normalized text tenant must resolve");
        };
        assert_eq!(resolved.partition(), &expected_partition);
        assert_eq!(resolved.physical().index_id(), active.index_id());
        assert_eq!(
            resolved.physical().generation(),
            active.state().physical().unwrap().generation()
        );

        assert!(matches!(
            context
                .managed_text_generation(&runtime, Some(&DbPropertyValue::Null))
                .await
                .unwrap(),
            TextSearchAuthority::AbsentManagedPartition
        ));
        let conflicting = runtime
            .clone()
            .with_analyzer(TextAnalyzerKind::WhitespaceLowercase);
        assert!(matches!(
            context
                .managed_text_generation(&conflicting, Some(&tenant_value))
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("definition disagrees")
        ));
    }

    #[tokio::test]
    async fn absent_text_tenant_still_requires_active_canonical_authority() {
        let runtime = TextIndexDefinition::new_node("Document", "body")
            .unwrap()
            .with_tenant_property("tenant_id")
            .unwrap();
        let token =
            crate::ProcessLocalDatabaseToken::new("text-generation-null-authority").unwrap();
        let db = HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .unwrap();
        let mut context = ExecutionContext::new(&db, ParamBindings::default());
        context.enable_request_read_view().await.unwrap();

        assert!(matches!(
            context
                .managed_text_generation(&runtime, Some(&DbPropertyValue::Null))
                .await,
            Err(HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Text,
                reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            })
        ));
    }

    #[tokio::test]
    async fn absent_vector_tenant_requires_active_canonical_authority() {
        let runtime = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            2,
            VectorDistanceMetric::Cosine,
        )
        .unwrap()
        .with_tenant_property("tenant_id")
        .unwrap();
        let canonical = ValidatedVectorIndexDefinition::try_from_runtime(&runtime).unwrap();
        let missing_token =
            crate::ProcessLocalDatabaseToken::new("vector-generation-null-missing").unwrap();
        let missing = HelixDB::open_with_process_local_token_for_tests(missing_token)
            .await
            .unwrap();
        let mut missing_context = ExecutionContext::new(&missing, ParamBindings::default());
        missing_context.enable_request_read_view().await.unwrap();
        assert!(matches!(
            missing_context
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &runtime,
                    Some(&DbPropertyValue::Null),
                )
                .await,
            Err(HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Vector,
                reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            })
        ));

        let token = crate::ProcessLocalDatabaseToken::new("vector-generation-null-active").unwrap();
        let db = HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .unwrap();
        let seeded_tenant = VectorTenantPartition::try_new(encode_index_partition_value(
            &DbPropertyValue::String("seed".to_string()),
        ))
        .unwrap();
        seed_active_vector(&db, &canonical, Some(&seeded_tenant)).await;
        db.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .unwrap();
        let mut context = ExecutionContext::new(&db, ParamBindings::default());
        context.enable_request_read_view().await.unwrap();
        assert!(matches!(
            context
                .managed_vector_generation::<crate::search::vector::distance::Cosine>(
                    &runtime,
                    Some(&DbPropertyValue::Null),
                )
                .await
                .unwrap(),
            VectorSearchAuthority::AbsentManagedPartition
        ));
    }
}
