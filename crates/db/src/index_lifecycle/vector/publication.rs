//! Queued vector publication through the build planner.
//!
//! The publication worker collapses each selected entity's ordered operation
//! prefix into one physical effect: remove the entity from every other
//! partition any selected operation's `previous` routing names, then upsert
//! the last operation's replacement. Effects are planned in order exactly as a
//! build plans source entities ([`super::driver::plan_and_apply`]): in one
//! disposable snapshot transaction through one session checked out of the
//! planning budget builds share ([`super::driver::VectorBuildCache`]), at
//! deterministic layers, each admitted against the publication budget beside
//! the acknowledgement. The first effect that does not fit ends the batch, and
//! the captured writes of the admitted prefix are applied to the serializable
//! publication transaction. No source graph row is read.
//!
//! A generation's physical state for an entity is always one state of its
//! queued chain: the state before the first outstanding operation, or, after
//! an initial build scanned the entity while earlier operations were still
//! queued, a later intermediate state. Consecutive operations are continuous
//! (each `previous` is the prior replacement's partition), so the union of
//! `previous` partitions covers every state the entity can physically occupy.
//! Removal tolerates an absent entity or partition, and HNSW deletion of an
//! absent node stages no semantic change.
//!
//! # Sole writer
//!
//! Planning reads HNSW rows from a snapshot outside the publication
//! transaction, so those reads register no serializable conflicts. That is
//! sound because the publisher is the only writer of an Active generation's
//! physical rows: foreground writes only enqueue operations, a build writes
//! only its hidden generation, cleanup starts only after retirement, which
//! conflicts with the publication transaction's read of the index record, and
//! one attempt per generation runs at a time under its publication permit.
//! Tenant mappings and the physical-ID watermark are still read through the
//! publication transaction, and a physical ID another index allocates before
//! the snapshot opens is a retryable conflict.

use std::num::NonZeroUsize;
use std::sync::Arc;

use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorKey, VectorStorageLane,
};
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::vector::transaction_guard::{
    decode_active_txn_guard, LegacyVectorTxnGuardKey,
};
use crate::encoding::v2::values::indexes::operation_queue::QueuedVectorReplacement;
use crate::error::{HelixDbError, Result};
use crate::search::vector::{
    self, Distance, MeasuredVectorTransaction, ValidatedVectorGenerationHandle,
    VectorCacheWriteSet, VectorDistanceMetric, VectorIndexConfig, VectorWriteRecorder,
};

use super::super::queue::publication::VectorPublicationResources;
use super::super::queue::storage::AcknowledgementOutput;
use super::driver::{plan_and_apply, EntityPlanOutcome, VectorBatchAccounting, VectorPlanTarget};
use super::{
    corruption, ActiveIndexHandle, IndexEntityId, TextPartition, ValidatedVectorIndexDefinition,
    VectorIndexedDocument,
};

/// One entity's collapsed queued effect.
#[derive(Debug, Clone)]
pub(crate) struct QueuedVectorEffect {
    pub(crate) entity_id: IndexEntityId,
    /// Distinct partitions the entity may occupy before this effect, in
    /// chain order; every one other than the replacement's is cleared.
    pub(crate) stale: Vec<TextPartition>,
    /// Final state after the last selected operation; `None` deletes.
    pub(crate) replacement: Option<QueuedVectorReplacement>,
}

/// How many effects one publication staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StagedEffects {
    /// The first `n` effects fit the budget and were staged; no later one was.
    Prefix(NonZeroUsize),
    /// Not even the first effect fits beside the reserved output; nothing was
    /// staged.
    NoneFits,
}

/// Plans `effects` in order and stages the longest prefix that fits `limits`
/// beside `reserved`, the output already staged for the acknowledgement.
///
/// Every admitted write to a resident-cache row is recorded in `cache_writes`,
/// and every tenant partition an admitted removal empties is reclaimed and
/// recorded there for retirement.
#[allow(
    clippy::too_many_arguments,
    reason = "publication binds the exact storage, generation, budget, planner resources, and cache effects"
)]
pub(crate) async fn stage_active_effects(
    db: &Db,
    transaction: &DbTransaction,
    handle: &ActiveIndexHandle,
    effects: &[QueuedVectorEffect],
    limits: SearchIndexBatchLimits,
    reserved: AcknowledgementOutput,
    resources: &VectorPublicationResources,
    cache_writes: &VectorCacheWriteSet,
) -> Result<StagedEffects> {
    let ActiveIndexHandle::Vector { definition, .. } = handle else {
        return Err(corruption(
            "vector publication received another family handle",
        ));
    };
    let target = VectorPlanTarget::publication(handle, cache_writes)?;
    match definition.metric() {
        VectorDistanceMetric::Cosine => {
            stage_with_distance::<vector::distance::Cosine>(
                db,
                transaction,
                &target,
                definition,
                effects,
                limits,
                reserved,
                resources,
            )
            .await
        }
        VectorDistanceMetric::Euclidean => {
            stage_with_distance::<vector::distance::Euclidean>(
                db,
                transaction,
                &target,
                definition,
                effects,
                limits,
                reserved,
                resources,
            )
            .await
        }
        VectorDistanceMetric::Manhattan => {
            stage_with_distance::<vector::distance::Manhattan>(
                db,
                transaction,
                &target,
                definition,
                effects,
                limits,
                reserved,
                resources,
            )
            .await
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "publication binds the exact storage, target, budget, and planner resources"
)]
async fn stage_with_distance<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    target: &VectorPlanTarget<'_>,
    definition: &ValidatedVectorIndexDefinition,
    effects: &[QueuedVectorEffect],
    limits: SearchIndexBatchLimits,
    reserved: AcknowledgementOutput,
    resources: &VectorPublicationResources,
) -> Result<StagedEffects> {
    let planning = db.begin(IsolationLevel::Snapshot).await?;
    let recorder = VectorWriteRecorder::new();
    let mut accounting =
        VectorBatchAccounting::reserving(limits, reserved.operations, reserved.bytes);
    let mut session = resources.planning_cache.checkout_fresh::<D>().await;
    let mut staged = 0_usize;
    for effect in effects {
        let next = effect
            .replacement
            .as_ref()
            .map(|replacement| VectorIndexedDocument {
                partition: replacement.partition().clone(),
                vector: replacement.vector().to_vec(),
            });
        let EntityPlanOutcome::Admitted {
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
            ..
        } = plan_and_apply::<D>(
            &planning,
            &recorder,
            transaction,
            target,
            definition,
            Arc::clone(&resources.simhasher_registry),
            resources.batch_reads,
            effect.entity_id,
            &effect.stale,
            next.as_ref(),
            &accounting,
            &mut session,
        )
        .await?
        else {
            session.discard_entity();
            break;
        };
        // Selection already bounded the batch's input bytes.
        accounting.admit(
            0,
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
        )?;
        staged += 1;
    }
    Ok(NonZeroUsize::new(staged).map_or(StagedEffects::NoneFits, StagedEffects::Prefix))
}

/// Stages the reclamation of a tenant partition `write` emptied, returning
/// whether it did.
///
/// A partition that still holds entities stages nothing. Otherwise every
/// physical lane is proven empty except the metadata and the optional legacy
/// transaction guard, and both are deleted through `write`. The V2 count is an
/// exact fast-path signal for newly allocated generations; every lane is still
/// probed. The planner deletes the mapping and retires the partition's cache
/// only once the entity is admitted.
pub(super) async fn stage_empty_tenant_reclamation<D: Distance>(
    write: &MeasuredVectorTransaction<'_>,
    generation: &ValidatedVectorGenerationHandle,
) -> Result<bool> {
    let metadata = vector::VectorIndex::<D>::from_generation(generation)
        .get_metadata(write)
        .await?
        .ok_or_else(|| corruption("tenant vector partition lost metadata during deletion"))?;
    if metadata.count != 0 {
        return Ok(false);
    }
    if metadata.validated_state()? != vector::VectorIndexState::Empty {
        return Err(HelixDbError::InvariantViolation(
            "zero-count tenant vector partition retains populated metadata state".to_string(),
        ));
    }
    let expected =
        VectorIndexConfig::from_v2_definition(generation.definition(), generation.physical_name());
    if !metadata.config.has_same_physical_contract(&expected) {
        return Err(corruption(
            "empty tenant vector metadata conflicts with its active generation",
        ));
    }
    let scope = generation.scope();
    let physical_index_id = generation.physical_index_id();
    let metadata_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    let guard_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::TxnGuard(LegacyVectorTxnGuardKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    for lane in VectorStorageLane::ALL {
        let prefix = DataKey::data_prefix(scope, lane.prefix_key(physical_index_id).to_bytes());
        let mut rows = write.scan_prefix(prefix, ..).await?;
        while let Some(row) = rows.next().await? {
            if lane == VectorStorageLane::Core && row.key == metadata_key {
                continue;
            }
            if lane == VectorStorageLane::Core && row.key == guard_key {
                decode_active_txn_guard(&row.value).map_err(|error| {
                    HelixDbError::InvariantViolation(format!(
                        "empty tenant vector partition has a malformed transaction guard: {error}"
                    ))
                })?;
                continue;
            }
            return Err(HelixDbError::InvariantViolation(format!(
                "zero-count tenant vector partition {physical_index_id} retains a {lane:?} row"
            )));
        }
    }
    write.delete(metadata_key)?;
    write.delete(guard_key)?;
    Ok(true)
}
