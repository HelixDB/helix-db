//! Atomic canonical classification and operation enqueue transactions.
//!
//! Public DDL reaches this repository only after the runtime-only family
//! capability is `FullyReady`. The transaction owns duplicate classification,
//! ID watermark allocation, terminal-operation eviction, canonical state,
//! operation state, and runnable-pointer publication as one commit.

use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::config::NonEmptyDefinitionDifferences;
use crate::encoding::v2::keys::metadata::MetadataKey;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::GlobalKey;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{
    DataKey, DataKeyKind, EdgePropertyByIdKey, GlobalKeyKind, NodePropertyKey,
};
use crate::encoding::v2::values::id_allocation::IdAllocationWatermarkValue;
use crate::encoding::v2::values::{
    decode_index_record, decode_metadata_value, decode_operation_record, encode_index_record,
    encode_metadata_value, encode_operation_record,
};
use crate::error::{HelixDbError, Result};

use super::failpoints::{self, IndexOutboxFailpoint};
use super::outbox::{self, ExpectedCanonicalRevision};
use super::{
    IndexCursor, IndexDdlReceipt, IndexDefinitionFamily, IndexElementKind, IndexGenerationId,
    IndexOperationExecutionState, IndexOperationFamily, IndexOperationKind, IndexOperationProgress,
    IndexOperationRecord, IndexOperationRevision, IndexRecordV2, IndexRevision,
    IndexStateTransition, IndexStateV2, IndexV2MetadataValue, NoCursorProgress, OperationCounters,
    PhysicalGeneration, PrefixScanProgress, SecondaryBuildProgress, SecondaryBuildStage,
    SecondaryCleanupProgress, SourceScanProgress, TextBuildProgress, TextBuildStage,
    TextCleanupProgress, ValidatedDynamicIndexDefinition, VectorBuildProgress, VectorBuildStage,
    VectorCleanupProgress, VectorGenerationDescriptor, VectorPhysicalIndexId,
};

/// Valid initial BUILD checkpoint supplied by a family source scanner.
#[derive(Debug, Clone)]
pub(crate) struct InitialBuildProgress(IndexOperationProgress);

impl InitialBuildProgress {
    /// Starts a secondary build at its authoritative bounded source scan.
    pub(crate) fn secondary(inclusive_upper_bound: IndexCursor) -> Self {
        Self(IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Scan(SourceScanProgress {
                inclusive_upper_bound,
                cursor: None,
                counters: OperationCounters::default(),
            })),
        ))
    }

    /// Starts a vector build at its authoritative bounded source scan.
    pub(crate) fn vector(inclusive_upper_bound: IndexCursor) -> Self {
        Self(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::Scan(SourceScanProgress {
                inclusive_upper_bound,
                cursor: None,
                counters: OperationCounters::default(),
            })),
        ))
    }

    /// Starts a vector build by validating an unchanged legacy namespace.
    pub(crate) const fn legacy_vector() -> Self {
        Self(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::AdoptLegacy(
                super::LegacyVectorValidationProgress::initial(),
            )),
        ))
    }

    /// Starts a text build at its authoritative bounded source scan.
    pub(crate) fn text(inclusive_upper_bound: IndexCursor) -> Self {
        Self(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::ScanSource(SourceScanProgress {
                inclusive_upper_bound,
                cursor: None,
                counters: OperationCounters::default(),
            })),
        ))
    }

    fn family(&self) -> IndexOperationFamily {
        self.0.family()
    }
}

/// Creates or converges on one canonical BUILD operation.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn create_index_operation(
    db: &Db,
    scope: DataScope,
    definition: ValidatedDynamicIndexDefinition,
    mode: helix_planner::ir::IndexCreateMode,
    initial_progress: InitialBuildProgress,
) -> Result<IndexDdlReceipt> {
    failpoints::trip(IndexOutboxFailpoint::DdlEnqueueBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    create_index_operation_in_transaction(transaction, scope, definition, mode, initial_progress)
        .await
}

/// Captures the graph source cut and enqueues BUILD in one transaction.
///
/// Conflict tracking on the authoritative allocator watermark closes the
/// otherwise possible gap where a new leased ID range could appear after the
/// upper bound was read but before the canonical `Building` record committed.
pub(crate) async fn create_index_operation_from_current_source(
    db: &Db,
    scope: DataScope,
    definition: ValidatedDynamicIndexDefinition,
    mode: helix_planner::ir::IndexCreateMode,
) -> Result<IndexDdlReceipt> {
    failpoints::trip(IndexOutboxFailpoint::DdlEnqueueBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let initial_progress =
        initial_progress_from_current_source(&transaction, scope, &definition).await?;
    create_index_operation_in_transaction_with_physical(
        transaction,
        scope,
        definition,
        mode,
        initial_progress,
        VectorPhysicalSelection::Allocate,
    )
    .await
}

/// Enqueues one legacy vector adoption and claims its reservation atomically.
pub(crate) async fn create_legacy_vector_adoption_operation(
    db: &Db,
    scope: DataScope,
    definition: ValidatedDynamicIndexDefinition,
    physical_index_id: VectorPhysicalIndexId,
) -> Result<IndexDdlReceipt> {
    failpoints::trip(IndexOutboxFailpoint::DdlEnqueueBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    create_index_operation_in_transaction_with_physical(
        transaction,
        scope,
        definition,
        helix_planner::ir::IndexCreateMode::IfNotExists,
        InitialBuildProgress::legacy_vector(),
        VectorPhysicalSelection::AdoptLegacy(physical_index_id),
    )
    .await
}

/// Runs canonical duplicate classification inside an already selected source cut.
#[cfg(any(test, feature = "production-coverage"))]
async fn create_index_operation_in_transaction(
    transaction: DbTransaction,
    scope: DataScope,
    definition: ValidatedDynamicIndexDefinition,
    mode: helix_planner::ir::IndexCreateMode,
    initial_progress: InitialBuildProgress,
) -> Result<IndexDdlReceipt> {
    create_index_operation_in_transaction_with_physical(
        transaction,
        scope,
        definition,
        mode,
        initial_progress,
        VectorPhysicalSelection::Allocate,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum VectorPhysicalSelection {
    Allocate,
    AdoptLegacy(VectorPhysicalIndexId),
}

async fn create_index_operation_in_transaction_with_physical(
    transaction: DbTransaction,
    scope: DataScope,
    definition: ValidatedDynamicIndexDefinition,
    mode: helix_planner::ir::IndexCreateMode,
    initial_progress: InitialBuildProgress,
    vector_physical: VectorPhysicalSelection,
) -> Result<IndexDdlReceipt> {
    let family = operation_family(definition.family());
    if initial_progress.family() != family {
        return Err(HelixDbError::InvariantViolation(
            "initial index build progress belongs to another family".to_string(),
        ));
    }
    if !super::repository::operation_cursors_are_valid(scope, &initial_progress.0) {
        return Err(HelixDbError::InvariantViolation(
            "initial index build cursor is not an exact typed key".to_string(),
        ));
    }

    let identity = definition.identity();
    let index_key = outbox::scoped_index_key_for_identity(scope, &identity);
    let current = transaction
        .get(&index_key)
        .await?
        .map(|value| decode_index_record(&value))
        .transpose()?;

    let (expected, index_id, generation, revision) = match current.as_ref() {
        None => (
            ExpectedCanonicalRevision::Absent,
            super::repository::allocate_index_id(&transaction).await?,
            IndexGenerationId::initial(),
            IndexRevision::initial(),
        ),
        Some(index) => match index.state() {
            IndexStateV2::Aborting { .. } | IndexStateV2::Dropping { .. } => {
                return Err(HelixDbError::IndexBusy {
                    state: index.state().name(),
                });
            }
            IndexStateV2::Building {
                build_operation_id, ..
            } => {
                classify_existing_definition(index, &definition)?;
                return match mode {
                    helix_planner::ir::IndexCreateMode::ErrorIfExists => Err(
                        HelixDbError::IndexAlreadyExists(format!("{:?}", index.identity())),
                    ),
                    helix_planner::ir::IndexCreateMode::IfNotExists => {
                        let operation = linked_operation(&transaction, scope, index).await?;
                        if operation.operation_id() != *build_operation_id {
                            return Err(corruption("building operation link changed"));
                        }
                        Ok(IndexDdlReceipt::ExistingOperation {
                            operation_id: *build_operation_id,
                        })
                    }
                };
            }
            IndexStateV2::Active { physical, .. } => {
                classify_existing_definition(index, &definition)?;
                return match mode {
                    helix_planner::ir::IndexCreateMode::ErrorIfExists => Err(
                        HelixDbError::IndexAlreadyExists(format!("{:?}", index.identity())),
                    ),
                    helix_planner::ir::IndexCreateMode::IfNotExists => {
                        linked_operation(&transaction, scope, index).await?;
                        Ok(IndexDdlReceipt::AlreadyActive {
                            index_id: index.index_id(),
                            generation: physical.generation(),
                        })
                    }
                };
            }
            IndexStateV2::Dropped {
                last_generation, ..
            } => (
                ExpectedCanonicalRevision::Exact(index.revision()),
                index.index_id(),
                last_generation.checked_next()?,
                index.revision().checked_next()?,
            ),
        },
    };

    let operation_id = super::repository::allocate_operation_id(&transaction, scope).await?;
    let physical =
        physical_generation(&transaction, &definition, generation, vector_physical).await?;
    if let VectorPhysicalSelection::AdoptLegacy(physical_index_id) = vector_physical {
        let Some(reservation) = super::repository::load_legacy_vector_physical_reservation(
            &transaction,
            physical_index_id,
        )
        .await?
        else {
            return Err(corruption(
                "legacy vector adoption has no preflight reservation",
            ));
        };
        let Some(reservation) = reservation.begin_adoption(index_id, generation, operation_id)
        else {
            return Err(corruption(
                "legacy vector reservation cannot begin another adoption",
            ));
        };
        transaction.put(
            IndexKey::Global {
                kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                reservation,
            )),
        )?;
    }
    let next_index =
        IndexRecordV2::building(index_id, definition, revision, physical, operation_id)?;
    let operation = IndexOperationRecord::try_new(
        operation_id,
        index_id,
        identity,
        generation,
        revision,
        IndexOperationRevision::initial(),
        IndexOperationKind::Build,
        family,
        initial_progress.0,
        0,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    outbox::stage_operation(&transaction, scope, expected, &next_index, &operation).await?;
    failpoints::trip(IndexOutboxFailpoint::DdlEnqueueAfterStaging)?;
    transaction.commit().await?;
    Ok(IndexDdlReceipt::Accepted {
        operation_id,
        index_id,
        generation,
    })
}

/// Selects the family checkpoint from one transactionally captured source cut.
async fn initial_progress_from_current_source(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
) -> Result<InitialBuildProgress> {
    let upper_bound =
        capture_source_upper_bound(reader, scope, definition.identity().element_kind()).await?;
    Ok(match definition.family() {
        IndexDefinitionFamily::Secondary => InitialBuildProgress::secondary(upper_bound),
        IndexDefinitionFamily::Vector => InitialBuildProgress::vector(upper_bound),
        IndexDefinitionFamily::Text => InitialBuildProgress::text(upper_bound),
    })
}

/// Captures the inclusive authoritative property-row boundary for a BUILD.
pub(crate) async fn capture_source_upper_bound(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    element_kind: IndexElementKind,
) -> Result<IndexCursor> {
    let metadata = match element_kind {
        IndexElementKind::Node => MetadataKey::next_node_id_key(),
        IndexElementKind::Edge => MetadataKey::next_edge_id_key(),
    };
    let watermark_key = DataKey::Global {
        kind: GlobalKeyKind::Metadata(metadata),
    }
    .to_bytes();
    let exclusive_id = reader
        .get(watermark_key)
        .await?
        .map(|bytes| IdAllocationWatermarkValue::decode(&bytes))
        .transpose()?
        .map_or(0, IdAllocationWatermarkValue::exclusive_id);
    let inclusive_id = exclusive_id.saturating_sub(1);
    let key = match element_kind {
        IndexElementKind::Node => DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(inclusive_id)),
        }
        .to_bytes(),
        IndexElementKind::Edge => DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(inclusive_id)),
        }
        .to_bytes(),
    };
    IndexCursor::try_new(key).map_err(|error| HelixDbError::InvariantViolation(error.to_string()))
}

/// Drops an active index, aborts a building index, or converges on the exact
/// existing cleanup operation.
pub(crate) async fn drop_index_operation(
    db: &Db,
    scope: DataScope,
    expected_definition: &ValidatedDynamicIndexDefinition,
) -> Result<IndexDdlReceipt> {
    let identity = expected_definition.identity();
    failpoints::trip(IndexOutboxFailpoint::DdlEnqueueBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let index_key = outbox::scoped_index_key_for_identity(scope, &identity);
    let Some(index_value) = transaction.get(&index_key).await? else {
        return Err(HelixDbError::IndexNotFound(format!("{identity:?}")));
    };
    let index = decode_index_record(&index_value)?;
    if index.identity() != &identity {
        return Err(corruption("canonical index key/value identity mismatch"));
    }
    classify_existing_definition(&index, expected_definition)?;
    match index.state() {
        IndexStateV2::Building {
            build_operation_id, ..
        } => {
            let operation = linked_operation(&transaction, scope, &index).await?;
            if operation.operation_id() != *build_operation_id {
                return Err(corruption("building operation link changed"));
            }
            let next_index = index.transition(IndexStateTransition::BeginAbort)?;
            let next_operation = operation
                .begin_abort(next_index.revision())
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
            let pointer = outbox::pointer_for(scope, &next_operation);
            outbox::validate_link(scope, &next_index, &next_operation, Some(&pointer))?;
            transaction.put(index_key, encode_index_record(&next_index))?;
            transaction.put(
                outbox::scoped_operation_key(scope, operation.operation_id()),
                encode_operation_record(&next_operation),
            )?;
            transaction.put(
                outbox::global_operation_key(operation.operation_id()),
                encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(pointer)),
            )?;
            failpoints::trip(IndexOutboxFailpoint::DdlEnqueueAfterStaging)?;
            transaction.commit().await?;
            Ok(IndexDdlReceipt::ExistingOperation {
                operation_id: *build_operation_id,
            })
        }
        IndexStateV2::Active { physical, .. } => {
            linked_operation(&transaction, scope, &index).await?;
            let family = operation_family(index.definition().family());
            let operation_id =
                super::repository::allocate_operation_id(&transaction, scope).await?;
            let next_index = index.transition(IndexStateTransition::BeginDrop {
                drop_operation_id: operation_id,
            })?;
            let operation = IndexOperationRecord::try_new(
                operation_id,
                index.index_id(),
                identity.clone(),
                physical.generation(),
                next_index.revision(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Drop,
                family,
                initial_cleanup(family),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
            outbox::stage_operation(
                &transaction,
                scope,
                ExpectedCanonicalRevision::Exact(index.revision()),
                &next_index,
                &operation,
            )
            .await?;
            failpoints::trip(IndexOutboxFailpoint::DdlEnqueueAfterStaging)?;
            transaction.commit().await?;
            Ok(IndexDdlReceipt::Accepted {
                operation_id,
                index_id: index.index_id(),
                generation: physical.generation(),
            })
        }
        IndexStateV2::Aborting {
            build_operation_id, ..
        } => {
            linked_operation(&transaction, scope, &index).await?;
            Ok(IndexDdlReceipt::ExistingOperation {
                operation_id: *build_operation_id,
            })
        }
        IndexStateV2::Dropping {
            drop_operation_id, ..
        } => {
            linked_operation(&transaction, scope, &index).await?;
            Ok(IndexDdlReceipt::ExistingOperation {
                operation_id: *drop_operation_id,
            })
        }
        IndexStateV2::Dropped { .. } => Err(HelixDbError::IndexNotFound(format!("{identity:?}"))),
    }
}

fn classify_existing_definition(
    existing: &IndexRecordV2,
    requested: &ValidatedDynamicIndexDefinition,
) -> Result<()> {
    let Some(differing_fields) =
        NonEmptyDefinitionDifferences::between(existing.definition(), requested)
    else {
        return Ok(());
    };
    Err(HelixDbError::IndexDefinitionConflict {
        existing: Box::new(existing.definition().clone()),
        requested: Box::new(requested.clone()),
        differing_fields,
    })
}

async fn linked_operation(
    transaction: &DbTransaction,
    scope: DataScope,
    index: &IndexRecordV2,
) -> Result<IndexOperationRecord> {
    let operation_id = match index.state() {
        IndexStateV2::Building {
            build_operation_id, ..
        }
        | IndexStateV2::Aborting {
            build_operation_id, ..
        } => *build_operation_id,
        IndexStateV2::Active {
            completed_build_operation_id,
            ..
        } => *completed_build_operation_id,
        IndexStateV2::Dropping {
            drop_operation_id, ..
        } => *drop_operation_id,
        IndexStateV2::Dropped {
            completed_operation_id,
            ..
        } => *completed_operation_id,
    };
    let operation_key = outbox::scoped_operation_key(scope, operation_id);
    let Some(operation_value) = transaction.get(operation_key).await? else {
        return Err(corruption("canonical index retained operation is missing"));
    };
    let operation = decode_operation_record(&operation_value)?;
    let pointer = transaction
        .get(outbox::global_operation_key(operation_id))
        .await?
        .map(|value| decode_metadata_value(&value))
        .transpose()?
        .map(|value| match value {
            IndexV2MetadataValue::OperationQueuePointer(pointer) => Ok(pointer),
            IndexV2MetadataValue::StorageVersion(_)
            | IndexV2MetadataValue::LogicalIndexIdWatermark(_)
            | IndexV2MetadataValue::VectorPhysicalIdWatermark(_)
            | IndexV2MetadataValue::LegacyVectorPhysicalReservation(_)
            | IndexV2MetadataValue::TextCompactionPointer(_) => Err(corruption(
                "operation pointer key contains the wrong value kind",
            )),
        })
        .transpose()?;
    outbox::validate_link(scope, index, &operation, pointer.as_ref())?;
    Ok(operation)
}

async fn physical_generation(
    transaction: &DbTransaction,
    definition: &ValidatedDynamicIndexDefinition,
    generation: IndexGenerationId,
    vector_physical: VectorPhysicalSelection,
) -> Result<PhysicalGeneration> {
    Ok(match definition {
        ValidatedDynamicIndexDefinition::Secondary(_) => {
            PhysicalGeneration::Secondary { generation }
        }
        ValidatedDynamicIndexDefinition::Vector(definition) => {
            let layout = match (definition.tenant_property(), vector_physical) {
                (Some(_), VectorPhysicalSelection::Allocate) => {
                    super::VectorPhysicalLayout::Partitioned
                }
                (None, VectorPhysicalSelection::Allocate) => {
                    super::VectorPhysicalLayout::Unpartitioned {
                        physical_index_id: super::repository::allocate_vector_physical_id(
                            transaction,
                        )
                        .await?,
                    }
                }
                (None, VectorPhysicalSelection::AdoptLegacy(physical_index_id)) => {
                    super::VectorPhysicalLayout::Unpartitioned { physical_index_id }
                }
                (Some(_), VectorPhysicalSelection::AdoptLegacy(_)) => {
                    return Err(HelixDbError::InvariantViolation(
                        "tenant-partitioned vector definition cannot adopt one legacy namespace"
                            .to_string(),
                    ));
                }
            };
            PhysicalGeneration::Vector {
                generation,
                layout,
                descriptor: match vector_physical {
                    VectorPhysicalSelection::Allocate | VectorPhysicalSelection::AdoptLegacy(_) => {
                        VectorGenerationDescriptor::for_definition(definition)
                    }
                },
            }
        }
        ValidatedDynamicIndexDefinition::Text(_) => PhysicalGeneration::Text { generation },
    })
}

const fn operation_family(family: IndexDefinitionFamily) -> IndexOperationFamily {
    match family {
        IndexDefinitionFamily::Secondary => IndexOperationFamily::Secondary,
        IndexDefinitionFamily::Vector => IndexOperationFamily::Vector,
        IndexDefinitionFamily::Text => IndexOperationFamily::Text,
    }
}

fn initial_cleanup(family: IndexOperationFamily) -> IndexOperationProgress {
    match family {
        IndexOperationFamily::Secondary => IndexOperationProgress::SecondaryCleanup(
            SecondaryCleanupProgress::DeleteEntries(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
        ),
        IndexOperationFamily::Vector => IndexOperationProgress::VectorCleanup(
            VectorCleanupProgress::RetireCache(NoCursorProgress::default()),
        ),
        IndexOperationFamily::Text => IndexOperationProgress::TextCleanup(
            TextCleanupProgress::DeleteMetadata(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
        ),
    }
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use slatedb::object_store::memory::InMemory;

    use super::*;

    /// Keeps the typed corruption adapter covered independently from any one
    /// durable-row corruption fixture.
    #[test]
    fn corruption_adapter_preserves_the_catalog_error_category() {
        assert!(matches!(
            corruption("fixture"),
            HelixDbError::IndexCatalogCorruption(reason) if reason == "fixture"
        ));
    }
    use crate::config::{
        SearchIndexBackfillLimits, SecondaryIndexDefinition, TextIndexDefinition,
        VectorIndexDefinition,
    };
    use crate::encoding::v2::keys::metadata::MetadataKey;
    use crate::encoding::v2::keys::{DataKey, DataKeyKind, GlobalKeyKind, NodePropertyKey};
    use crate::encoding::v2::values::id_allocation::IdAllocationWatermarkValue;
    use crate::index_lifecycle::outbox::{
        ClaimPermission, CommittedOperationStep, IndexOperationDriver, IndexOperationStepExecution,
        IndexOperationStepResult, OperationPointerObservation,
    };
    use crate::index_lifecycle::{
        BuildOperationOutcome, ClaimSequence, IndexOperationId, IndexOperationOutcome, WriterEpoch,
    };
    use crate::search::vector::VectorDistanceMetric;

    struct CompleteDriver {
        family: IndexOperationFamily,
        outcome: IndexOperationOutcome,
    }

    #[async_trait]
    impl IndexOperationDriver for CompleteDriver {
        fn family(&self) -> IndexOperationFamily {
            self.family
        }

        async fn step(
            &self,
            _db: &Db,
            _transaction: &DbTransaction,
            _scope: DataScope,
            _operation: &IndexOperationRecord,
            _limits: crate::config::SearchIndexBatchLimits,
        ) -> Result<IndexOperationStepExecution> {
            Ok(IndexOperationStepExecution::new(
                IndexOperationStepResult::Completed(self.outcome),
            ))
        }
    }

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("in-memory lifecycle database opens");
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("empty writer bootstraps V2 metadata");
        db
    }

    fn source_upper_bound(scope: DataScope) -> IndexCursor {
        IndexCursor::try_new(
            DataKey::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(42)),
            }
            .to_bytes(),
        )
        .expect("typed node source key is a valid cursor")
    }

    fn initial_progress(
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> InitialBuildProgress {
        let upper_bound = source_upper_bound(scope);
        match definition.family() {
            IndexDefinitionFamily::Secondary => InitialBuildProgress::secondary(upper_bound),
            IndexDefinitionFamily::Vector => InitialBuildProgress::vector(upper_bound),
            IndexDefinitionFamily::Text => InitialBuildProgress::text(upper_bound),
        }
    }

    async fn complete_operation(
        db: &Db,
        operation_id: IndexOperationId,
        family: IndexOperationFamily,
        outcome: IndexOperationOutcome,
    ) {
        let writer_epoch = WriterEpoch::from_bytes([91; 16]).expect("non-nil writer epoch");
        let observation = outbox::observe_operation_pointer(db, operation_id, writer_epoch, 0)
            .await
            .expect("operation pointer is readable");
        let OperationPointerObservation::Eligible(eligible) = observation else {
            panic!("queued operation must be eligible");
        };
        let claimed = outbox::claim_operation(
            db,
            &eligible,
            writer_epoch,
            ClaimSequence::new(1).expect("non-zero claim sequence"),
            0,
            ClaimPermission::Normal,
        )
        .await
        .expect("claim succeeds")
        .expect("exact queued revision is claimable");
        assert_eq!(
            outbox::execute_claimed_step(
                db,
                &claimed,
                &CompleteDriver { family, outcome },
                SearchIndexBackfillLimits::default().batch(),
                0,
            )
            .await
            .expect("terminal family step commits"),
            CommittedOperationStep::Completed
        );
    }

    #[tokio::test]
    async fn all_families_create_one_atomic_canonical_operation_link() {
        let db = test_db("lifecycle-create-all-families").await;
        let scope = DataScope::LegacyUnscoped;
        let definitions = [
            ValidatedDynamicIndexDefinition::try_from(
                SecondaryIndexDefinition::node_equality("User", "email")
                    .expect("secondary definition"),
            )
            .expect("validated secondary definition"),
            ValidatedDynamicIndexDefinition::try_from(
                VectorIndexDefinition::new_node(
                    "Document",
                    "embedding",
                    3,
                    VectorDistanceMetric::Cosine,
                )
                .expect("vector definition"),
            )
            .expect("validated vector definition"),
            ValidatedDynamicIndexDefinition::try_from(
                TextIndexDefinition::new_node("Document", "body").expect("text definition"),
            )
            .expect("validated text definition"),
        ];

        for definition in definitions {
            let expected_family = operation_family(definition.family());
            let receipt = create_index_operation(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
                initial_progress(scope, &definition),
            )
            .await
            .expect("new family operation is accepted");
            let IndexDdlReceipt::Accepted {
                operation_id,
                index_id,
                generation,
            } = receipt
            else {
                panic!("new definition must return an accepted receipt");
            };
            assert_eq!(generation, IndexGenerationId::initial());
            let operation = outbox::read_operation(&db, scope, operation_id)
                .await
                .expect("operation link validates")
                .expect("accepted operation exists");
            assert_eq!(operation.index_id(), index_id);
            assert_eq!(operation.identity(), &definition.identity());
            assert_eq!(operation.family(), expected_family);

            complete_operation(
                &db,
                operation_id,
                expected_family,
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
            )
            .await;
            let drop_receipt = drop_index_operation(&db, scope, &definition)
                .await
                .expect("every active family accepts one drop operation");
            let IndexDdlReceipt::Accepted {
                operation_id: drop_operation_id,
                index_id: drop_index_id,
                generation: drop_generation,
            } = drop_receipt
            else {
                panic!("first active drop must return an accepted receipt");
            };
            assert_eq!(drop_index_id, index_id);
            assert_eq!(drop_generation, generation);
            let drop_operation = outbox::read_operation(&db, scope, drop_operation_id)
                .await
                .expect("drop operation link validates")
                .expect("accepted drop operation exists");
            assert_eq!(drop_operation.kind(), IndexOperationKind::Drop);
            assert_eq!(drop_operation.family(), expected_family);
            assert!(matches!(
                drop_operation.progress(),
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                    _
                )) | IndexOperationProgress::VectorCleanup(VectorCleanupProgress::RetireCache(_))
                    | IndexOperationProgress::TextCleanup(TextCleanupProgress::DeleteMetadata(_))
            ));
            complete_operation(
                &db,
                drop_operation_id,
                expected_family,
                IndexOperationOutcome::DropSucceeded,
            )
            .await;
        }
        db.close().await.expect("database closes");
    }

    #[tokio::test]
    async fn public_create_captures_the_source_watermark_in_its_enqueue_transaction() {
        let db = test_db("lifecycle-current-source-create").await;
        let scope = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(
            0xC0FFEE,
        ));
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").expect("secondary definition"),
        )
        .expect("validated secondary definition");
        db.put(
            DataKey::Global {
                kind: GlobalKeyKind::Metadata(MetadataKey::next_node_id_key()),
            }
            .to_bytes(),
            Bytes::copy_from_slice(&IdAllocationWatermarkValue::new(8).encode()),
        )
        .await
        .expect("exclusive node watermark is written");

        let receipt = create_index_operation_from_current_source(
            &db,
            scope,
            definition,
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("public create captures and enqueues one source cut");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new definition must enqueue a build");
        };
        let operation = outbox::read_operation(&db, scope, operation_id)
            .await
            .expect("operation is readable")
            .expect("accepted operation exists");
        let IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
            SecondaryBuildStage::Scan(SourceScanProgress {
                inclusive_upper_bound,
                ..
            }),
        )) = operation.progress()
        else {
            panic!("secondary create must start at its source scan");
        };
        assert_eq!(
            inclusive_upper_bound,
            &IndexCursor::try_new(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(7)),
                }
                .to_bytes(),
            )
            .expect("typed source key is a valid cursor")
        );
        db.close().await.expect("database closes");
    }

    #[tokio::test]
    async fn duplicate_create_drop_abort_and_recreate_follow_the_frozen_matrix() {
        let db = test_db("lifecycle-duplicate-matrix").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").expect("secondary definition"),
        )
        .expect("validated secondary definition");
        let changed_definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique secondary definition"),
        )
        .expect("validated unique secondary definition");

        assert!(matches!(
            drop_index_operation(&db, scope, &definition).await,
            Err(HelixDbError::IndexNotFound(_))
        ));

        let first = create_index_operation(
            &db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            initial_progress(scope, &definition),
        )
        .await
        .expect("initial build is accepted");
        let IndexDdlReceipt::Accepted {
            operation_id: first_build_id,
            index_id,
            generation,
        } = first
        else {
            panic!("initial build must be accepted");
        };
        assert_eq!(generation, IndexGenerationId::initial());

        assert_eq!(
            create_index_operation(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
                initial_progress(scope, &definition),
            )
            .await
            .expect("if-not-exists converges on the build"),
            IndexDdlReceipt::ExistingOperation {
                operation_id: first_build_id,
            }
        );
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
                initial_progress(scope, &definition),
            )
            .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                changed_definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
                initial_progress(scope, &changed_definition),
            )
            .await,
            Err(HelixDbError::IndexDefinitionConflict { .. })
        ));
        assert!(matches!(
            drop_index_operation(&db, scope, &changed_definition).await,
            Err(HelixDbError::IndexDefinitionConflict { .. })
        ));

        assert_eq!(
            drop_index_operation(&db, scope, &definition)
                .await
                .expect("dropping a build begins abort cleanup"),
            IndexDdlReceipt::ExistingOperation {
                operation_id: first_build_id,
            }
        );
        let aborting = outbox::abort_operation(&db, scope, first_build_id)
            .await
            .expect("abort converges on cleanup started by DROP");
        assert_eq!(aborting.operation_id(), first_build_id);
        assert_eq!(
            outbox::abort_operation(&db, scope, first_build_id)
                .await
                .expect("duplicate abort converges"),
            aborting
        );
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                changed_definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
                initial_progress(scope, &changed_definition),
            )
            .await,
            Err(HelixDbError::IndexBusy { state: "aborting" })
        ));
        assert_eq!(
            drop_index_operation(&db, scope, &definition)
                .await
                .expect("duplicate drop converges on abort cleanup"),
            IndexDdlReceipt::ExistingOperation {
                operation_id: first_build_id,
            }
        );
        complete_operation(
            &db,
            first_build_id,
            IndexOperationFamily::Secondary,
            IndexOperationOutcome::Build(BuildOperationOutcome::Aborted),
        )
        .await;

        let second = create_index_operation(
            &db,
            scope,
            changed_definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            initial_progress(scope, &changed_definition),
        )
        .await
        .expect("dropped logical index is recreated");
        let IndexDdlReceipt::Accepted {
            operation_id: second_build_id,
            index_id: recreated_index_id,
            generation: second_generation,
        } = second
        else {
            panic!("recreate must accept a new build");
        };
        assert_eq!(recreated_index_id, index_id);
        assert_eq!(second_generation.get(), generation.get() + 1);
        assert!(outbox::read_operation(&db, scope, first_build_id)
            .await
            .expect("old operation lookup is valid")
            .is_none());

        complete_operation(
            &db,
            second_build_id,
            IndexOperationFamily::Secondary,
            IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
        )
        .await;
        assert_eq!(
            create_index_operation(
                &db,
                scope,
                changed_definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
                initial_progress(scope, &changed_definition),
            )
            .await
            .expect("if-not-exists converges on active generation"),
            IndexDdlReceipt::AlreadyActive {
                index_id,
                generation: second_generation,
            }
        );
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                changed_definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
                initial_progress(scope, &changed_definition),
            )
            .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
                initial_progress(scope, &definition),
            )
            .await,
            Err(HelixDbError::IndexDefinitionConflict { .. })
        ));
        assert!(matches!(
            drop_index_operation(&db, scope, &definition).await,
            Err(HelixDbError::IndexDefinitionConflict { .. })
        ));

        let drop = drop_index_operation(&db, scope, &changed_definition)
            .await
            .expect("active generation begins drop cleanup");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            index_id: dropped_index_id,
            generation: dropped_generation,
        } = drop
        else {
            panic!("active drop must accept a new operation");
        };
        assert_eq!(dropped_index_id, index_id);
        assert_eq!(dropped_generation, second_generation);
        assert!(outbox::read_operation(&db, scope, second_build_id)
            .await
            .expect("evicted build lookup is valid")
            .is_none());
        assert_eq!(
            drop_index_operation(&db, scope, &changed_definition)
                .await
                .expect("duplicate drop converges"),
            IndexDdlReceipt::ExistingOperation {
                operation_id: drop_id,
            }
        );
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
                initial_progress(scope, &definition),
            )
            .await,
            Err(HelixDbError::IndexBusy { state: "dropping" })
        ));

        complete_operation(
            &db,
            drop_id,
            IndexOperationFamily::Secondary,
            IndexOperationOutcome::DropSucceeded,
        )
        .await;
        assert!(matches!(
            drop_index_operation(&db, scope, &changed_definition).await,
            Err(HelixDbError::IndexNotFound(_))
        ));
        db.close().await.expect("database closes");
    }

    #[tokio::test]
    async fn scope_lookup_is_exact_and_invalid_source_cursor_stages_nothing() {
        let db = test_db("lifecycle-scope-and-cursor").await;
        let unscoped = DataScope::LegacyUnscoped;
        let tenant_scope = DataScope::Tenant(
            crate::encoding::v2::keys::scope::TenantId::from_ulid_str("00000000000000000000000001")
                .expect("canonical tenant ULID"),
        );
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").expect("secondary definition"),
        )
        .expect("validated secondary definition");
        let invalid_progress = InitialBuildProgress::secondary(
            IndexCursor::try_new(Bytes::from_static(b"not-a-v1-key"))
                .expect("cursor length alone is valid"),
        );
        assert!(matches!(
            create_index_operation(
                &db,
                unscoped,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
                invalid_progress,
            )
            .await,
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(matches!(
            create_index_operation(
                &db,
                unscoped,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
                InitialBuildProgress::vector(source_upper_bound(unscoped)),
            )
            .await,
            Err(HelixDbError::InvariantViolation(_))
        ));

        let receipt = create_index_operation(
            &db,
            unscoped,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            initial_progress(unscoped, &definition),
        )
        .await
        .expect("valid source cursor accepts build");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("first valid create must be accepted");
        };
        assert!(outbox::read_operation(&db, tenant_scope, operation_id)
            .await
            .expect("wrong-scope lookup is valid")
            .is_none());

        let tenant_receipt = create_index_operation(
            &db,
            tenant_scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            initial_progress(tenant_scope, &definition),
        )
        .await
        .expect("same identity is independent in another scope");
        let IndexDdlReceipt::Accepted {
            operation_id: tenant_operation_id,
            ..
        } = tenant_receipt
        else {
            panic!("tenant create must be accepted");
        };
        assert_ne!(tenant_operation_id, operation_id);
        assert!(outbox::read_operation(&db, unscoped, tenant_operation_id)
            .await
            .expect("cross-scope lookup is valid")
            .is_none());
        db.close().await.expect("database closes");
    }

    /// Covers retained-operation lookup for the terminal Dropped state and
    /// proves missing or mistyped durable links fail closed.
    #[tokio::test]
    async fn dropped_operation_links_are_exact_and_corruption_is_typed() {
        let db = test_db("lifecycle-dropped-operation-links").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        let receipt = create_index_operation(
            &db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            initial_progress(scope, &definition),
        )
        .await
        .unwrap();
        let IndexDdlReceipt::Accepted {
            operation_id: build_id,
            ..
        } = receipt
        else {
            unreachable!("new lifecycle fixture is accepted")
        };
        complete_operation(
            &db,
            build_id,
            IndexOperationFamily::Secondary,
            IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
        )
        .await;
        let drop_receipt = drop_index_operation(&db, scope, &definition).await.unwrap();
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_receipt
        else {
            unreachable!("first Active drop is accepted")
        };
        complete_operation(
            &db,
            drop_id,
            IndexOperationFamily::Secondary,
            IndexOperationOutcome::DropSucceeded,
        )
        .await;

        let index_key = outbox::scoped_index_key_for_identity(scope, &definition.identity());
        let index = decode_index_record(&db.get(&index_key).await.unwrap().unwrap()).unwrap();
        assert!(matches!(index.state(), IndexStateV2::Dropped { .. }));
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert_eq!(
            linked_operation(&transaction, scope, &index)
                .await
                .unwrap()
                .operation_id(),
            drop_id
        );
        drop(transaction);

        let operation_key = outbox::scoped_operation_key(scope, drop_id);
        let operation_value = db.get(&operation_key).await.unwrap().unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction.delete(operation_key.clone()).unwrap();
        transaction.commit().await.unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            linked_operation(&transaction, scope, &index).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("retained operation is missing")
        ));
        drop(transaction);

        db.put(operation_key, operation_value).await.unwrap();
        db.put(
            outbox::global_operation_key(drop_id),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                crate::index_lifecycle::IndexStorageVersion::CURRENT,
            )),
        )
        .await
        .unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            linked_operation(&transaction, scope, &index).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("wrong value kind")
        ));
        drop(transaction);
        db.close().await.expect("database closes");
    }

    /// Proves DROP rejects a canonical row stored under another logical
    /// identity before it can classify or enqueue any operation.
    #[tokio::test]
    async fn drop_rejects_canonical_key_value_identity_disagreement() {
        let db = test_db("lifecycle-drop-identity-mismatch").await;
        let scope = DataScope::LegacyUnscoped;
        let expected = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        let other = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("Account", "email").unwrap(),
        )
        .unwrap();
        let mismatched = IndexRecordV2::building(
            crate::index_lifecycle::IndexId::initial(),
            other,
            crate::index_lifecycle::IndexRevision::initial(),
            crate::index_lifecycle::PhysicalGeneration::Secondary {
                generation: crate::index_lifecycle::IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap();
        db.put(
            outbox::scoped_index_key_for_identity(scope, &expected.identity()),
            encode_index_record(&mismatched),
        )
        .await
        .unwrap();
        assert!(matches!(
            drop_index_operation(&db, scope, &expected).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("key/value identity mismatch")
        ));
        db.close().await.expect("database closes");
    }

    /// Proves vector CREATE reports physical-ID exhaustion through the public
    /// enqueue transaction without staging a canonical row or operation.
    #[tokio::test]
    async fn vector_create_propagates_physical_id_exhaustion() {
        let db = test_db("lifecycle-vector-id-exhaustion").await;
        let scope = DataScope::LegacyUnscoped;
        db.put(
            IndexKey::Global {
                kind: crate::encoding::v2::keys::GlobalKey::VectorPhysicalIdWatermark,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                crate::index_lifecycle::VectorPhysicalIdWatermark {
                    next_id: crate::index_lifecycle::VectorPhysicalIndexId::new(u64::MAX).unwrap(),
                },
            )),
        )
        .await
        .unwrap();
        let definition = ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            create_index_operation(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
                initial_progress(scope, &definition),
            )
            .await,
            Err(HelixDbError::IdentifierExhausted(
                "vector physical index ID"
            ))
        ));
        assert!(db
            .get(outbox::scoped_index_key_for_identity(
                scope,
                &definition.identity()
            ))
            .await
            .unwrap()
            .is_none());
        db.close().await.expect("database closes");
    }
}
