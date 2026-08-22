//! Production-path crash-boundary contracts for the V2 index outbox.
//!
//! This harness lives outside the measured production tree but drives the real
//! canonical records, scoped operation rows, and global queue pointers. It
//! deliberately uses no alternate lifecycle codec or mock repository.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::{
    SearchIndexBackfillLimits, SearchIndexBatchLimits, SecondaryIndexDefinition,
    TextIndexDefinition, VectorIndexDefinition,
};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{DataKey, DataKeyKind, NodePropertyKey};
use crate::encoding::v2::keys::{RecordKind, ScopedKey};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::failpoints::{self, IndexOutboxFailpoint};
use crate::index_lifecycle::outbox::{
    self, ClaimPermission, CommittedOperationStep, ExpectedCanonicalRevision, IndexOperationDriver,
    IndexOperationStepExecution, IndexOperationStepResult, OperationPointerObservation,
};
use crate::index_lifecycle::secondary::{
    load_mutation_set, lookup_active_equality_generation, maintain_entity, SecondaryIndexDriver,
};
use crate::index_lifecycle::{
    BuildOperationOutcome, ClaimSequence, IndexCursor, IndexDdlReceipt, IndexElementKind,
    IndexGenerationId, IndexId, IndexOperationExecutionState, IndexOperationFamily,
    IndexOperationId, IndexOperationOutcome, IndexOperationProgress, IndexOperationRecord,
    IndexOperationRevision, IndexRecordV2, IndexRevision, IndexStateV2, NoCursorProgress,
    OperationCounters, PhysicalGeneration, SecondaryBuildProgress, SecondaryBuildStage,
    TextBuildProgress, TextBuildStage, ValidatedDynamicIndexDefinition, VectorBuildProgress,
    VectorBuildStage, VectorGenerationDescriptor, VectorPhysicalIndexId, VectorPhysicalLayout,
    WriterEpoch,
};
use crate::search::vector::VectorDistanceMetric;

/// Serializes contracts that share the process-local one-shot failpoint slot.
static ACCEPTANCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Closed family cases used by the generic outbox transition contract.
#[derive(Debug, Clone, Copy)]
enum FamilyCase {
    Secondary,
    Vector,
    Text,
}

impl FamilyCase {
    const ALL: [Self; 3] = [Self::Secondary, Self::Vector, Self::Text];

    const fn name(self) -> &'static str {
        match self {
            Self::Secondary => "secondary",
            Self::Vector => "vector",
            Self::Text => "text",
        }
    }

    const fn family(self) -> IndexOperationFamily {
        match self {
            Self::Secondary => IndexOperationFamily::Secondary,
            Self::Vector => IndexOperationFamily::Vector,
            Self::Text => IndexOperationFamily::Text,
        }
    }

    fn definition(self) -> ValidatedDynamicIndexDefinition {
        match self {
            Self::Secondary => SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary fixture definition validates")
                .try_into()
                .expect("secondary fixture converts to V2"),
            Self::Vector => VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("vector fixture definition validates")
            .try_into()
            .expect("vector fixture converts to V2"),
            Self::Text => TextIndexDefinition::new_node("Document", "body")
                .expect("text fixture definition validates")
                .try_into()
                .expect("text fixture converts to V2"),
        }
    }

    fn physical(self, definition: &ValidatedDynamicIndexDefinition) -> PhysicalGeneration {
        match (self, definition) {
            (Self::Secondary, ValidatedDynamicIndexDefinition::Secondary(_)) => {
                PhysicalGeneration::Secondary {
                    generation: IndexGenerationId::initial(),
                }
            }
            (Self::Vector, ValidatedDynamicIndexDefinition::Vector(definition)) => {
                PhysicalGeneration::Vector {
                    generation: IndexGenerationId::initial(),
                    layout: VectorPhysicalLayout::Unpartitioned {
                        physical_index_id: VectorPhysicalIndexId::initial(),
                    },
                    descriptor: VectorGenerationDescriptor::for_definition(definition),
                }
            }
            (Self::Text, ValidatedDynamicIndexDefinition::Text(_)) => PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            _ => panic!("family fixture definition and physical generation disagree"),
        }
    }

    fn progress(self) -> IndexOperationProgress {
        let progress = NoCursorProgress {
            counters: OperationCounters::default(),
        };
        match self {
            Self::Secondary => IndexOperationProgress::SecondaryBuild(
                SecondaryBuildProgress::Constructing(SecondaryBuildStage::Activate(progress)),
            ),
            Self::Vector => IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Activate(progress),
            )),
            Self::Text => IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Activate(progress),
            )),
        }
    }

    fn initial_build_progress(self) -> crate::index_lifecycle::lifecycle::InitialBuildProgress {
        let cursor = IndexCursor::try_new(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(0)),
            }
            .to_bytes(),
        )
        .expect("DDL failpoint source cursor is a typed node-property key");
        match self {
            Self::Secondary => {
                crate::index_lifecycle::lifecycle::InitialBuildProgress::secondary(cursor)
            }
            Self::Vector => crate::index_lifecycle::lifecycle::InitialBuildProgress::vector(cursor),
            Self::Text => crate::index_lifecycle::lifecycle::InitialBuildProgress::text(cursor),
        }
    }
}

/// One deterministic family driver that completes an already-validated build.
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

/// Runs the complete operation crash-boundary matrix.
pub(super) async fn run_outbox_failpoint_contracts() {
    let _serial = ACCEPTANCE_LOCK.lock().await;
    for family in FamilyCase::ALL {
        for failpoint in [
            IndexOutboxFailpoint::DdlEnqueueBefore,
            IndexOutboxFailpoint::DdlEnqueueAfterStaging,
        ] {
            for repetition in 0_u8..2 {
                exercise_create_ddl_failpoint(family, failpoint, repetition).await;
                exercise_drop_ddl_failpoint(family, failpoint, repetition).await;
            }
        }
        for failpoint in [
            IndexOutboxFailpoint::ClaimBefore,
            IndexOutboxFailpoint::ClaimAfter,
        ] {
            for repetition in 0_u8..2 {
                exercise_operation_claim_failpoint(family, failpoint, repetition).await;
            }
        }
        for failpoint in [
            IndexOutboxFailpoint::BatchReadBefore,
            IndexOutboxFailpoint::BatchReadAfter,
            IndexOutboxFailpoint::PhysicalStagingBefore,
            IndexOutboxFailpoint::PhysicalStagingAfter,
            IndexOutboxFailpoint::CheckpointStagingBefore,
            IndexOutboxFailpoint::CheckpointStagingAfter,
            IndexOutboxFailpoint::CommitBefore,
            IndexOutboxFailpoint::CommitAfter,
            IndexOutboxFailpoint::ActivationBefore,
            IndexOutboxFailpoint::ActivationAfter,
            IndexOutboxFailpoint::QueueRemovalBefore,
            IndexOutboxFailpoint::QueueRemovalAfter,
        ] {
            for repetition in 0_u8..2 {
                exercise_operation_step_failpoint(family, failpoint, repetition).await;
            }
        }
    }

    failpoints::production_contracts::run();
}

/// Reference lifecycle states used by the deterministic state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReferenceLifecycle {
    Absent,
    Active(IndexGenerationId),
    Dropped(IndexGenerationId),
}

/// Minimal semantic oracle independent of persisted V2 rows.
#[derive(Debug)]
struct SecondaryReferenceModel {
    lifecycle: ReferenceLifecycle,
    entities: BTreeMap<u64, String>,
}

impl Default for SecondaryReferenceModel {
    fn default() -> Self {
        Self {
            lifecycle: ReferenceLifecycle::Absent,
            entities: BTreeMap::new(),
        }
    }
}

impl SecondaryReferenceModel {
    fn expected_for(&self, value: &str) -> BTreeSet<u64> {
        self.entities
            .iter()
            .filter_map(|(entity_id, stored)| (stored == value).then_some(*entity_id))
            .collect()
    }
}

/// Closed input alphabet for one complete lifecycle state-machine execution.
#[derive(Debug, Clone, Copy)]
enum SecondaryAction {
    Create,
    Insert { entity_id: u64, value: &'static str },
    Update { entity_id: u64, value: &'static str },
    Delete { entity_id: u64 },
    Search { value: &'static str },
    Reopen,
    Drop,
    Recreate,
    RetryAfterHigherLimit,
    AbortPartialBuild,
}

/// Production state retained across deterministic model actions.
struct SecondaryMachine {
    database: &'static str,
    store: Arc<dyn ObjectStore>,
    db: Db,
    driver: SecondaryIndexDriver,
    definition: ValidatedDynamicIndexDefinition,
    operation_sequence: u64,
    model: SecondaryReferenceModel,
}

impl SecondaryMachine {
    async fn open() -> Self {
        let database = "phase11-secondary-state-machine";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(database, Arc::clone(&store))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("state-machine database opens");
        crate::index_lifecycle::repository::bootstrap_writer(&db)
            .await
            .expect("state-machine database bootstraps V2 metadata");
        Self {
            database,
            store,
            db,
            driver: secondary_driver(),
            definition: SecondaryIndexDefinition::node_equality("User", "email")
                .expect("state-machine definition validates")
                .try_into()
                .expect("state-machine definition converts to V2"),
            operation_sequence: 1,
            model: SecondaryReferenceModel::default(),
        }
    }

    async fn apply(&mut self, action: SecondaryAction) {
        match action {
            SecondaryAction::Create => {
                let operation_id = enqueue_secondary_build(&self.db, &self.definition, 100).await;
                assert_eq!(
                    drive_secondary_to_terminal(
                        &self.db,
                        &self.driver,
                        operation_id,
                        &mut self.operation_sequence,
                        SearchIndexBackfillLimits::default().batch(),
                    )
                    .await,
                    CommittedOperationStep::Completed
                );
                let record = load_canonical(&self.db, &self.definition).await;
                let IndexStateV2::Active { physical, .. } = record.state() else {
                    panic!("successful CREATE must publish Active");
                };
                self.model.lifecycle = ReferenceLifecycle::Active(physical.generation());
            }
            SecondaryAction::Insert { entity_id, value } => {
                assert!(matches!(
                    self.model.lifecycle,
                    ReferenceLifecycle::Active(_)
                ));
                mutate_secondary_entity(&self.db, entity_id, None, Some(value)).await;
                assert_eq!(
                    self.model.entities.insert(entity_id, value.to_string()),
                    None
                );
            }
            SecondaryAction::Update { entity_id, value } => {
                assert!(matches!(
                    self.model.lifecycle,
                    ReferenceLifecycle::Active(_)
                ));
                let previous = self
                    .model
                    .entities
                    .get(&entity_id)
                    .cloned()
                    .expect("model update requires an existing entity");
                mutate_secondary_entity(&self.db, entity_id, Some(&previous), Some(value)).await;
                self.model.entities.insert(entity_id, value.to_string());
            }
            SecondaryAction::Delete { entity_id } => {
                assert!(matches!(
                    self.model.lifecycle,
                    ReferenceLifecycle::Active(_)
                ));
                let previous = self
                    .model
                    .entities
                    .remove(&entity_id)
                    .expect("model delete requires an existing entity");
                mutate_secondary_entity(&self.db, entity_id, Some(&previous), None).await;
            }
            SecondaryAction::Search { value } => self.assert_search(value).await,
            SecondaryAction::Reopen => {
                self.db
                    .close()
                    .await
                    .expect("state-machine writer closes before reopen");
                self.db = Db::builder(self.database, Arc::clone(&self.store))
                    .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
                    .build()
                    .await
                    .expect("state-machine writer reopens");
                self.driver = secondary_driver();
                let ReferenceLifecycle::Active(expected_generation) = self.model.lifecycle else {
                    panic!("reopen action requires an Active reference generation");
                };
                let record = load_canonical(&self.db, &self.definition).await;
                assert_eq!(record.state().generation(), expected_generation);
                assert!(matches!(record.state(), IndexStateV2::Active { .. }));
            }
            SecondaryAction::Drop => {
                let before = load_canonical(&self.db, &self.definition).await;
                let receipt = crate::index_lifecycle::lifecycle::drop_index_operation(
                    &self.db,
                    DataScope::LegacyUnscoped,
                    &self.definition,
                )
                .await
                .expect("state-machine DROP enqueues");
                let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
                    panic!("first state-machine DROP must be accepted");
                };
                assert_eq!(
                    drive_secondary_to_terminal(
                        &self.db,
                        &self.driver,
                        operation_id,
                        &mut self.operation_sequence,
                        SearchIndexBackfillLimits::default().batch(),
                    )
                    .await,
                    CommittedOperationStep::Completed
                );
                let dropped = load_canonical(&self.db, &self.definition).await;
                assert!(matches!(dropped.state(), IndexStateV2::Dropped { .. }));
                self.model.lifecycle = ReferenceLifecycle::Dropped(before.state().generation());
            }
            SecondaryAction::Recreate => {
                let ReferenceLifecycle::Dropped(previous_generation) = self.model.lifecycle else {
                    panic!("recreate action requires a Dropped reference generation");
                };
                let operation_id = enqueue_secondary_build(&self.db, &self.definition, 100).await;
                assert_eq!(
                    drive_secondary_to_terminal(
                        &self.db,
                        &self.driver,
                        operation_id,
                        &mut self.operation_sequence,
                        SearchIndexBackfillLimits::default().batch(),
                    )
                    .await,
                    CommittedOperationStep::Completed
                );
                let record = load_canonical(&self.db, &self.definition).await;
                assert_eq!(
                    record.state().generation().get(),
                    previous_generation.get() + 1
                );
                self.model.lifecycle = ReferenceLifecycle::Active(record.state().generation());
            }
            SecondaryAction::RetryAfterHigherLimit => {
                exercise_typed_limit_retry(
                    &self.db,
                    &mut self.operation_sequence,
                    self.model.entities.keys().copied().max().unwrap_or(0) + 1,
                )
                .await;
            }
            SecondaryAction::AbortPartialBuild => {
                exercise_partial_build_abort(
                    &self.db,
                    &mut self.operation_sequence,
                    self.model.entities.keys().copied().max().unwrap_or(0) + 2,
                )
                .await;
            }
        }
    }

    async fn assert_search(&self, value: &str) {
        let record = load_canonical(&self.db, &self.definition).await;
        let handle = crate::index_lifecycle::ActiveIndexHandle::try_from_record(
            DataScope::LegacyUnscoped,
            &record,
        )
        .expect("reference search requires one exact Active handle");
        let actual = lookup_active_equality_generation(
            &self.db,
            &handle,
            &PropertyValue::String(value.to_string()),
        )
        .await
        .expect("generation-qualified equality lookup succeeds")
        .iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(actual, self.model.expected_for(value));
    }
}

/// Runs a deterministic state machine against the real secondary lifecycle.
pub(super) async fn run_secondary_state_machine_contracts() {
    let _serial = ACCEPTANCE_LOCK.lock().await;
    let mut machine = SecondaryMachine::open().await;
    for action in [
        SecondaryAction::Create,
        SecondaryAction::Search {
            value: "missing@example.com",
        },
        SecondaryAction::Insert {
            entity_id: 1,
            value: "first@example.com",
        },
        SecondaryAction::Search {
            value: "first@example.com",
        },
        SecondaryAction::Update {
            entity_id: 1,
            value: "updated@example.com",
        },
        SecondaryAction::Search {
            value: "first@example.com",
        },
        SecondaryAction::Search {
            value: "updated@example.com",
        },
        SecondaryAction::Delete { entity_id: 1 },
        SecondaryAction::Search {
            value: "updated@example.com",
        },
        SecondaryAction::Insert {
            entity_id: 2,
            value: "reopen@example.com",
        },
        SecondaryAction::Reopen,
        SecondaryAction::Search {
            value: "reopen@example.com",
        },
        SecondaryAction::Drop,
        SecondaryAction::Recreate,
        SecondaryAction::Search {
            value: "reopen@example.com",
        },
        SecondaryAction::RetryAfterHigherLimit,
        SecondaryAction::AbortPartialBuild,
    ] {
        machine.apply(action).await;
    }
    machine
        .db
        .close()
        .await
        .expect("state-machine database closes");
}

/// Proves global operation-pointer scans discover 16 isolated scopes.
pub(super) async fn run_multi_scope_discovery_contracts() {
    let _serial = ACCEPTANCE_LOCK.lock().await;
    const TENANT_COUNT: u8 = 16;
    let db = Db::open("phase11-multi-scope-discovery", Arc::new(InMemory::new()))
        .await
        .expect("multi-scope discovery database opens");
    let definition = FamilyCase::Secondary.definition();

    for tenant in 1..=TENANT_COUNT {
        let scope = DataScope::Tenant(TenantId::from_u128(u128::from(tenant)));
        let operation_id = IndexOperationId::from_bytes([tenant; 16])
            .expect("multi-scope operation ID is non-nil");
        let index = IndexRecordV2::building(
            IndexId::initial(),
            definition.clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            operation_id,
        )
        .expect("multi-scope canonical record validates");
        let operation = IndexOperationRecord::try_new(
            operation_id,
            index.index_id(),
            index.identity().clone(),
            index.state().generation(),
            index.revision(),
            IndexOperationRevision::initial(),
            crate::index_lifecycle::IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            FamilyCase::Secondary.progress(),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("multi-scope operation validates");
        outbox::enqueue_operation(
            &db,
            scope,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .expect("multi-scope operation enqueues atomically");
    }

    let wrong_scope = DataScope::Tenant(TenantId::from_u128(999));
    let first_operation = IndexOperationId::from_bytes([1; 16]).expect("first ID is non-nil");
    assert!(outbox::read_operation(&db, wrong_scope, first_operation)
        .await
        .expect("wrong-scope point read remains valid")
        .is_none());

    let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("discovery epoch is non-nil");
    let operation_page_size =
        outbox::OperationQueuePageSize::new(5).expect("operation discovery page size is positive");
    let mut operation_cursor = None;
    let mut operation_count = 0_u8;
    loop {
        let page = outbox::scan_operation_queue_page(&db, operation_cursor, operation_page_size)
            .await
            .expect("global operation pointer page decodes");
        for operation_id in page.operation_ids {
            let tenant = operation_id.as_bytes()[0];
            let OperationPointerObservation::Eligible(eligible) =
                outbox::observe_operation_pointer(&db, operation_id, writer_epoch, 0)
                    .await
                    .expect("global operation pointer resolves its scoped owner")
            else {
                panic!("fresh multi-scope operation must be eligible");
            };
            assert_eq!(
                eligible.scope,
                DataScope::Tenant(TenantId::from_u128(u128::from(tenant)))
            );
            operation_count = operation_count
                .checked_add(1)
                .expect("multi-scope operation count remains bounded");
        }
        if page.prefix_exhausted {
            break;
        }
        operation_cursor = page.resume_after;
    }
    assert_eq!(operation_count, TENANT_COUNT);

    db.close()
        .await
        .expect("multi-scope discovery database closes");
}

/// Creates a secondary lifecycle driver.
fn secondary_driver() -> SecondaryIndexDriver {
    SecondaryIndexDriver::with_catch_up_delay(
        Arc::new(crate::index_lifecycle::IndexScopeGates::default()),
        crate::config::SecondaryIndexLifecycleCatchUpTailDelayMillis::new(1)
            .expect("test catch-up delay is positive"),
    )
}

/// Builds one typed node-property key for the authoritative graph source.
fn secondary_source_key(entity_id: u64) -> bytes::Bytes {
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
    }
    .to_bytes()
}

/// Returns the complete source properties represented by one model value.
fn secondary_properties(value: &str) -> Vec<Property> {
    vec![
        Property::new("$label", PropertyValue::String("User".to_string())),
        Property::new("email", PropertyValue::String(value.to_string())),
    ]
}

/// Atomically mutates authoritative source data and every applicable V2 row.
async fn mutate_secondary_entity(
    db: &Db,
    entity_id: u64,
    before: Option<&str>,
    after: Option<&str>,
) {
    let before = before.map_or_else(Vec::new, secondary_properties);
    let after = after.map_or_else(Vec::new, secondary_properties);
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("graph mutation transaction opens");
    let mutations = load_mutation_set(&transaction, DataScope::LegacyUnscoped)
        .await
        .expect("secondary mutation set loads from canonical rows");
    maintain_entity(
        &transaction,
        DataScope::LegacyUnscoped,
        &mutations,
        IndexElementKind::Node,
        entity_id,
        &before,
        &after,
    )
    .await
    .expect("secondary mutation stages with graph source data");
    if after.is_empty() {
        transaction
            .delete(secondary_source_key(entity_id))
            .expect("source deletion stages");
    } else {
        transaction
            .put(secondary_source_key(entity_id), encode_properties(&after))
            .expect("source replacement stages");
    }
    transaction
        .commit()
        .await
        .expect("graph source and index mutation commit atomically");
}

/// Enqueues one real secondary build from an explicit durable source cut.
async fn enqueue_secondary_build(
    db: &Db,
    definition: &ValidatedDynamicIndexDefinition,
    source_upper_entity_id: u64,
) -> IndexOperationId {
    let cursor = IndexCursor::try_new(secondary_source_key(source_upper_entity_id))
        .expect("typed source key is a valid lifecycle cursor");
    let receipt = crate::index_lifecycle::lifecycle::create_index_operation(
        db,
        DataScope::LegacyUnscoped,
        definition.clone(),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        crate::index_lifecycle::lifecycle::InitialBuildProgress::secondary(cursor),
    )
    .await
    .expect("secondary CREATE atomically enqueues");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("new or recreated secondary definition must enqueue");
    };
    operation_id
}

/// Drives one real secondary operation until it blocks or terminates.
async fn drive_secondary_to_terminal(
    db: &Db,
    driver: &SecondaryIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    for _ in 0..64 {
        let epoch = WriterEpoch::from_bytes([0x71; 16]).expect("state-machine epoch is non-nil");
        let OperationPointerObservation::Eligible(eligible) =
            outbox::observe_operation_pointer(db, operation_id, epoch, 0)
                .await
                .expect("secondary operation pointer is readable")
        else {
            panic!("queued secondary operation must be eligible");
        };
        let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
        *claim_sequence = claim_sequence
            .checked_add(1)
            .expect("state-machine claim sequence remains bounded");
        let claimed =
            outbox::claim_operation(db, &eligible, epoch, sequence, 0, ClaimPermission::Normal)
                .await
                .expect("secondary operation claim succeeds")
                .expect("exact secondary operation revision is claimable");
        let step = outbox::execute_claimed_step(db, &claimed, driver, limits, 0)
            .await
            .expect("secondary operation step commits");
        if step != CommittedOperationStep::Progressed {
            return step;
        }
    }
    panic!("secondary operation exceeded its bounded state-machine checkpoints")
}

/// Loads the exact canonical row named by one definition.
async fn load_canonical(db: &Db, definition: &ValidatedDynamicIndexDefinition) -> IndexRecordV2 {
    crate::index_lifecycle::repository::load_index_record(
        db,
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("canonical secondary record decodes")
    .expect("canonical secondary record exists")
}

/// Proves a typed resource block resumes at the same checkpoint after a limit increase.
async fn exercise_typed_limit_retry(db: &Db, claim_sequence: &mut u64, entity_id: u64) {
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("Retry", "email")
            .expect("retry definition validates")
            .try_into()
            .expect("retry definition converts to V2");
    let properties = vec![
        Property::new("$label", PropertyValue::String("Retry".to_string())),
        Property::new(
            "email",
            PropertyValue::String("limit-retry@example.com".to_string()),
        ),
    ];
    db.put(
        secondary_source_key(entity_id),
        encode_properties(&properties),
    )
    .await
    .expect("retry source row commits");
    let operation_id = enqueue_secondary_build(db, &definition, entity_id).await;
    let tiny = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::MIN,
        NonZeroU64::new(16).expect("fixture output operation limit is positive"),
        NonZeroU64::new(1_024).expect("fixture output byte limit is positive"),
        NonZeroU64::new(1_024).expect("fixture vector byte limit is positive"),
    )
    .expect("tiny limit set remains internally consistent");
    let driver = secondary_driver();
    assert_eq!(
        drive_secondary_to_terminal(db, &driver, operation_id, claim_sequence, tiny).await,
        CommittedOperationStep::Blocked
    );
    let blocked = outbox::read_operation(db, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("blocked operation decodes")
        .expect("blocked operation remains retained");
    assert!(matches!(
        blocked.execution_state(),
        IndexOperationExecutionState::Blocked(
            crate::index_lifecycle::IndexOperationBlocker::OversizedEntity { .. }
        )
    ));
    let retried = outbox::retry_operation(db, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("blocked operation retries at its exact checkpoint");
    assert_eq!(retried.progress(), blocked.progress());
    assert_eq!(
        drive_secondary_to_terminal(
            db,
            &driver,
            operation_id,
            claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await,
        CommittedOperationStep::Completed
    );
}

/// Proves abort reuses a partially progressed build and removes hidden rows.
async fn exercise_partial_build_abort(db: &Db, claim_sequence: &mut u64, entity_id: u64) {
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("Abort", "email")
            .expect("abort definition validates")
            .try_into()
            .expect("abort definition converts to V2");
    let properties = vec![
        Property::new("$label", PropertyValue::String("Abort".to_string())),
        Property::new(
            "email",
            PropertyValue::String("abort@example.com".to_string()),
        ),
    ];
    db.put(
        secondary_source_key(entity_id),
        encode_properties(&properties),
    )
    .await
    .expect("abort source row commits");
    let operation_id = enqueue_secondary_build(db, &definition, entity_id).await;
    let driver = secondary_driver();
    let first = drive_one_secondary_step(
        db,
        &driver,
        operation_id,
        claim_sequence,
        SearchIndexBackfillLimits::default().batch(),
    )
    .await;
    assert_eq!(first, CommittedOperationStep::Progressed);
    let aborting = outbox::abort_operation(db, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("partially progressed build begins abort cleanup");
    assert_eq!(aborting.operation_id(), operation_id);
    assert_eq!(
        drive_secondary_to_terminal(
            db,
            &driver,
            operation_id,
            claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await,
        CommittedOperationStep::Completed
    );
    let terminal = outbox::read_operation(db, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("aborted operation decodes")
        .expect("aborted operation remains retained");
    assert!(matches!(
        terminal.execution_state(),
        IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
            BuildOperationOutcome::Aborted
        ))
    ));
    assert!(matches!(
        load_canonical(db, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
}

/// Claims and executes exactly one secondary checkpoint.
async fn drive_one_secondary_step(
    db: &Db,
    driver: &SecondaryIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    let epoch = WriterEpoch::from_bytes([0x72; 16]).expect("one-step epoch is non-nil");
    let OperationPointerObservation::Eligible(eligible) =
        outbox::observe_operation_pointer(db, operation_id, epoch, 0)
            .await
            .expect("one-step operation pointer is readable")
    else {
        panic!("one-step secondary operation must be eligible");
    };
    let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
    *claim_sequence = claim_sequence
        .checked_add(1)
        .expect("one-step claim sequence remains bounded");
    let claimed =
        outbox::claim_operation(db, &eligible, epoch, sequence, 0, ClaimPermission::Normal)
            .await
            .expect("one-step claim succeeds")
            .expect("one-step operation revision is claimable");
    outbox::execute_claimed_step(db, &claimed, driver, limits, 0)
        .await
        .expect("one secondary checkpoint commits")
}

/// Counts the exact scoped operation lane without assuming operation IDs.
async fn operation_row_count(db: &Db) -> usize {
    let prefix = DataKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::logical_prefix(RecordKind::Operation),
    );
    let mut rows = db
        .scan_prefix(&prefix, ..)
        .await
        .expect("operation lane scan succeeds");
    let mut count = 0_usize;
    while rows
        .next()
        .await
        .expect("operation lane row decodes")
        .is_some()
    {
        count += 1;
    }
    count
}

/// Proves no globally runnable operation survived an aborted DDL transaction.
async fn assert_operation_queue_empty(db: &Db) {
    assert!(outbox::scan_operation_queue_page(
        db,
        None,
        outbox::OperationQueuePageSize::new(1).expect("one-row queue page validates"),
    )
    .await
    .expect("operation queue scan succeeds")
    .operation_ids
    .is_empty());
}

/// Proves a failed CREATE left no canonical, scoped-operation, or pointer row.
async fn assert_failed_create_is_absent(db: &Db, definition: &ValidatedDynamicIndexDefinition) {
    assert!(db
        .get(
            crate::encoding::v2::keys::ManagedIndexKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: ScopedKey::index_record(definition.identity().clone()),
            }
            .to_bytes(),
        )
        .await
        .expect("canonical absence check succeeds")
        .is_none());
    assert_eq!(operation_row_count(db).await, 0);
    assert_operation_queue_empty(db).await;
}

/// Proves each early CREATE interruption aborts atomically and retries after reopen.
async fn exercise_create_ddl_failpoint(
    family: FamilyCase,
    failpoint: IndexOutboxFailpoint,
    repetition: u8,
) {
    let database = format!(
        "phase11-ddl-create-{}-{}-{repetition}",
        family.name(),
        failpoint.as_str()
    );
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(database.as_str(), Arc::clone(&store))
        .await
        .expect("DDL CREATE database opens");
    crate::index_lifecycle::repository::bootstrap_writer(&db)
        .await
        .expect("DDL CREATE database bootstraps V2 metadata");
    let definition = family.definition();

    failpoints::inject_once(failpoint).expect("DDL CREATE failpoint installs");
    assert!(matches!(
        crate::index_lifecycle::lifecycle::create_index_operation(
            &db,
            DataScope::LegacyUnscoped,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            family.initial_build_progress(),
        )
        .await,
        Err(HelixDbError::InvariantViolation(reason))
            if reason.contains(failpoint.as_str())
    ));
    assert!(failpoints::was_triggered());
    assert_failed_create_is_absent(&db, &definition).await;
    db.close()
        .await
        .expect("failed DDL CREATE database closes before reopen");

    let db = Db::open(database.as_str(), store)
        .await
        .expect("failed DDL CREATE database reopens");
    assert_failed_create_is_absent(&db, &definition).await;
    let receipt = crate::index_lifecycle::lifecycle::create_index_operation(
        &db,
        DataScope::LegacyUnscoped,
        definition,
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        family.initial_build_progress(),
    )
    .await
    .expect("reopened DDL CREATE retries");
    let IndexDdlReceipt::Accepted {
        operation_id,
        index_id,
        generation,
    } = receipt
    else {
        panic!("retried CREATE must enqueue one new operation");
    };
    assert_eq!(index_id, IndexId::initial());
    assert_eq!(generation, IndexGenerationId::initial());
    let epoch_byte = family.family() as u8 * 40 + failpoint as u8 * 2 + repetition + 1;
    let epoch = WriterEpoch::from_bytes([epoch_byte; 16]).expect("CREATE epoch is non-nil");
    let claimed = claim_operation(&db, operation_id, epoch, 1).await;
    complete_operation(
        &db,
        family,
        &claimed,
        IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
    )
    .await;
    assert_terminal_operation(&db, operation_id).await;
    db.close()
        .await
        .expect("recovered DDL CREATE database closes");
}

/// Proves each early DROP interruption preserves Active and retries after reopen.
async fn exercise_drop_ddl_failpoint(
    family: FamilyCase,
    failpoint: IndexOutboxFailpoint,
    repetition: u8,
) {
    let database = format!(
        "phase11-ddl-drop-{}-{}-{repetition}",
        family.name(),
        failpoint.as_str()
    );
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(database.as_str(), Arc::clone(&store))
        .await
        .expect("DDL DROP database opens");
    crate::index_lifecycle::repository::bootstrap_writer(&db)
        .await
        .expect("DDL DROP database bootstraps V2 metadata");
    let definition = family.definition();
    let create = crate::index_lifecycle::lifecycle::create_index_operation(
        &db,
        DataScope::LegacyUnscoped,
        definition.clone(),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        family.initial_build_progress(),
    )
    .await
    .expect("DDL DROP fixture CREATE enqueues");
    let IndexDdlReceipt::Accepted {
        operation_id: build_operation_id,
        ..
    } = create
    else {
        panic!("DDL DROP fixture CREATE must be accepted");
    };
    let build_epoch = WriterEpoch::from_bytes([0xA1; 16]).expect("build epoch is non-nil");
    let claimed = claim_operation(&db, build_operation_id, build_epoch, 1).await;
    complete_operation(
        &db,
        family,
        &claimed,
        IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
    )
    .await;
    let active = load_canonical(&db, &definition).await;
    assert!(matches!(active.state(), IndexStateV2::Active { .. }));
    assert_eq!(operation_row_count(&db).await, 1);
    assert_operation_queue_empty(&db).await;

    failpoints::inject_once(failpoint).expect("DDL DROP failpoint installs");
    assert!(matches!(
        crate::index_lifecycle::lifecycle::drop_index_operation(
            &db,
            DataScope::LegacyUnscoped,
            &definition,
        )
        .await,
        Err(HelixDbError::InvariantViolation(reason))
            if reason.contains(failpoint.as_str())
    ));
    assert!(failpoints::was_triggered());
    assert_eq!(load_canonical(&db, &definition).await, active);
    assert_eq!(operation_row_count(&db).await, 1);
    assert_operation_queue_empty(&db).await;
    db.close()
        .await
        .expect("failed DDL DROP database closes before reopen");

    let db = Db::open(database.as_str(), store)
        .await
        .expect("failed DDL DROP database reopens");
    assert_eq!(load_canonical(&db, &definition).await, active);
    assert_eq!(operation_row_count(&db).await, 1);
    assert_operation_queue_empty(&db).await;
    let drop = crate::index_lifecycle::lifecycle::drop_index_operation(
        &db,
        DataScope::LegacyUnscoped,
        &definition,
    )
    .await
    .expect("reopened DDL DROP retries");
    let IndexDdlReceipt::Accepted {
        operation_id: drop_operation_id,
        ..
    } = drop
    else {
        panic!("retried DROP must enqueue one new operation");
    };
    let drop_epoch = WriterEpoch::from_bytes([0xA2; 16]).expect("drop epoch is non-nil");
    let claimed = claim_operation(&db, drop_operation_id, drop_epoch, 1).await;
    complete_operation(&db, family, &claimed, IndexOperationOutcome::DropSucceeded).await;
    let terminal = outbox::read_operation(&db, DataScope::LegacyUnscoped, drop_operation_id)
        .await
        .expect("terminal DROP operation decodes")
        .expect("terminal DROP operation remains retained");
    assert!(matches!(
        terminal.execution_state(),
        IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded)
    ));
    assert!(matches!(
        load_canonical(&db, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert_operation_queue_empty(&db).await;
    db.close()
        .await
        .expect("recovered DDL DROP database closes");
}

/// Creates one clean durable operation already at the activation boundary.
async fn operation_fixture(family: FamilyCase, id_byte: u8) -> (Db, IndexOperationId, WriterEpoch) {
    let db = Db::open(
        format!("phase11-{}-{id_byte}", family.name()),
        Arc::new(InMemory::new()),
    )
    .await
    .expect("isolated operation database opens");
    let definition = family.definition();
    let operation_id =
        IndexOperationId::from_bytes([id_byte; 16]).expect("operation fixture ID is non-nil");
    let index = IndexRecordV2::building(
        IndexId::initial(),
        definition.clone(),
        IndexRevision::initial(),
        family.physical(&definition),
        operation_id,
    )
    .expect("family fixture starts in Building");
    let operation = IndexOperationRecord::try_new(
        operation_id,
        index.index_id(),
        index.identity().clone(),
        index.state().generation(),
        index.revision(),
        IndexOperationRevision::initial(),
        crate::index_lifecycle::IndexOperationKind::Build,
        family.family(),
        family.progress(),
        0,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .expect("family fixture operation validates");
    outbox::enqueue_operation(
        &db,
        DataScope::LegacyUnscoped,
        ExpectedCanonicalRevision::Absent,
        &index,
        &operation,
    )
    .await
    .expect("canonical record, operation, and pointer enqueue atomically");
    let epoch = WriterEpoch::from_bytes([id_byte.wrapping_add(1); 16])
        .expect("writer fixture epoch is non-nil");
    (db, operation_id, epoch)
}

/// Observes and claims one queued or prior-writer operation.
async fn claim_operation(
    db: &Db,
    operation_id: IndexOperationId,
    epoch: WriterEpoch,
    sequence: u64,
) -> outbox::ClaimedOperation {
    let OperationPointerObservation::Eligible(eligible) =
        outbox::observe_operation_pointer(db, operation_id, epoch, 0)
            .await
            .expect("operation pointer observation succeeds")
    else {
        panic!("operation must be eligible for recovery");
    };
    outbox::claim_operation(
        db,
        &eligible,
        epoch,
        ClaimSequence::new(sequence).expect("claim sequence is non-zero"),
        0,
        ClaimPermission::Normal,
    )
    .await
    .expect("operation claim transaction succeeds")
    .expect("exact eligible operation revision is claimed")
}

/// Proves a claim-boundary failure leaves a legal durable recovery action.
async fn exercise_operation_claim_failpoint(
    family: FamilyCase,
    failpoint: IndexOutboxFailpoint,
    repetition: u8,
) {
    let id_byte = family.family() as u8 * 40 + repetition + failpoint as u8 + 1;
    let (db, operation_id, epoch) = operation_fixture(family, id_byte).await;
    let OperationPointerObservation::Eligible(eligible) =
        outbox::observe_operation_pointer(&db, operation_id, epoch, 0)
            .await
            .expect("fresh operation pointer is readable")
    else {
        panic!("fresh operation must be eligible");
    };
    failpoints::inject_once(failpoint).expect("one operation failpoint installs");
    assert!(outbox::claim_operation(
        &db,
        &eligible,
        epoch,
        ClaimSequence::new(1).expect("first claim sequence is non-zero"),
        0,
        ClaimPermission::Normal,
    )
    .await
    .is_err());
    assert!(failpoints::was_triggered());

    let recovered_epoch =
        WriterEpoch::from_bytes([id_byte.wrapping_add(2); 16]).expect("recovery epoch is non-nil");
    let claimed = claim_operation(&db, operation_id, recovered_epoch, 1).await;
    complete_operation(
        &db,
        family,
        &claimed,
        IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
    )
    .await;
    assert_terminal_operation(&db, operation_id).await;
    db.close().await.expect("operation database closes");
}

/// Proves a step-boundary failure is either resumable or already terminal.
async fn exercise_operation_step_failpoint(
    family: FamilyCase,
    failpoint: IndexOutboxFailpoint,
    repetition: u8,
) {
    let id_byte = family.family() as u8 * 40 + repetition + failpoint as u8 + 1;
    let (db, operation_id, epoch) = operation_fixture(family, id_byte).await;
    let claimed = claim_operation(&db, operation_id, epoch, 1).await;
    failpoints::inject_once(failpoint).expect("one operation failpoint installs");
    assert!(outbox::execute_claimed_step(
        &db,
        &claimed,
        &CompleteDriver {
            family: family.family(),
            outcome: IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
        },
        SearchIndexBackfillLimits::default().batch(),
        0,
    )
    .await
    .is_err());
    assert!(failpoints::was_triggered());

    let durable = outbox::read_operation(&db, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("durable operation remains decodable")
        .expect("terminal history or resumable operation remains retained");
    if matches!(
        durable.execution_state(),
        IndexOperationExecutionState::Completed(_)
    ) {
        assert_eq!(failpoint, IndexOutboxFailpoint::CommitAfter);
    } else {
        assert!(matches!(
            durable.execution_state(),
            IndexOperationExecutionState::Claimed(_)
        ));
        let recovered_epoch = WriterEpoch::from_bytes([id_byte.wrapping_add(2); 16])
            .expect("recovery epoch is non-nil");
        let recovered = claim_operation(&db, operation_id, recovered_epoch, 1).await;
        complete_operation(
            &db,
            family,
            &recovered,
            IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
        )
        .await;
    }
    assert_terminal_operation(&db, operation_id).await;
    db.close().await.expect("operation database closes");
}

/// Commits one terminal build step after an injected failure has been cleared.
async fn complete_operation(
    db: &Db,
    family: FamilyCase,
    claimed: &outbox::ClaimedOperation,
    outcome: IndexOperationOutcome,
) {
    assert_eq!(
        outbox::execute_claimed_step(
            db,
            claimed,
            &CompleteDriver {
                family: family.family(),
                outcome,
            },
            SearchIndexBackfillLimits::default().batch(),
            0,
        )
        .await
        .expect("recovered operation completes"),
        CommittedOperationStep::Completed
    );
}

/// Asserts the exact terminal operation and canonical Active state.
async fn assert_terminal_operation(db: &Db, operation_id: IndexOperationId) {
    let operation = outbox::read_operation(db, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("terminal operation link validates")
        .expect("terminal operation history remains retained");
    assert!(matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
            BuildOperationOutcome::Succeeded
        ))
    ));
    let index = crate::index_lifecycle::repository::load_index_record(
        db,
        DataScope::LegacyUnscoped,
        operation.identity(),
    )
    .await
    .expect("terminal canonical record decodes")
    .expect("terminal canonical record exists");
    assert!(matches!(index.state(), IndexStateV2::Active { .. }));
}
