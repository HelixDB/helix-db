//! Atomic manifest attachment for one published Active text mutation.
//!
//! An Active request prepares and publishes its upload through an independent
//! durable intent transaction while retaining the older authoritative graph
//! transaction. This module consumes only the validated in-memory upload value:
//! it never reads or writes the graph row, intent, upload pointer, or
//! intent-owned global reference. The request-level owner composes this
//! attachment with the one authoritative graph write and every other index
//! action before staging anything.
//!
//! Split construction is request-owned: this module derives the manifest's
//! next logical version before building the immutable Tantivy document and
//! retains the resulting payload beside its content-addressed reference. A
//! production caller therefore cannot pair caller-built metadata with unrelated
//! bytes or index a document at a guessed version.
//!
//! The caller must retain its process-local GC permit and coordinator reference
//! guard through the transaction outcome. This module owns only the typed
//! database-row contract; publication and outcome reconciliation remain separate
//! boundaries.

use std::num::NonZeroU32;

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_v2::{self, work};

use super::active_preflight::ActiveTextMutationMeasurements;

/// One exact row observation retained across publication latency.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveAttachmentObservation {
    key: Bytes,
    value: Option<Bytes>,
}

/// Complete admitted manifest/outbox unit prepared before intent creation.
///
/// The capability owns the exact root, page, live-state, reachability, and proof
/// rows that the graph transaction may later stage, plus exact measurements for
/// its independent three-row upload outbox. The request-level owner adds the
/// graph row once and aggregates every unit before publication can begin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedActiveManifestSplit {
    handle: index_v2::ActiveIndexHandle,
    partition: work::TextPartition,
    logical_version: index_v2::TextLogicalVersion,
    payload: Bytes,
    split: work::SplitRef,
    observations: Vec<ActiveAttachmentObservation>,
    root_key: Bytes,
    root_value: Bytes,
    page_typed: index_keys::TextManifestPageKey,
    page_key: Bytes,
    page_value: Bytes,
    state_key: Bytes,
    state_value: Bytes,
    reference_key: Bytes,
    reference_value: Bytes,
    proof_key_len: usize,
    proof_value_len: usize,
    slot: u32,
    measurements: ActiveTextMutationMeasurements,
}

impl PreparedActiveManifestSplit {
    /// Returns the exact Active handle bound into every prepared row.
    pub(crate) const fn handle(&self) -> &index_v2::ActiveIndexHandle {
        &self.handle
    }

    /// Returns the canonical destination partition.
    pub(crate) const fn partition(&self) -> &work::TextPartition {
        &self.partition
    }

    /// Returns the exact immutable bytes bound to the admitted split reference.
    pub(crate) const fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Returns the immutable split that must be published before staging.
    pub(crate) const fn split(&self) -> work::SplitRef {
        self.split
    }

    /// Returns the manifest-derived logical version written for the entity.
    #[cfg(test)]
    pub(crate) const fn logical_version(&self) -> index_v2::TextLogicalVersion {
        self.logical_version
    }

    /// Returns the admitted exact resource measurements.
    pub(super) const fn measurements(&self) -> ActiveTextMutationMeasurements {
        self.measurements
    }
}

/// Validated authority to attach one definitively published Active split.
///
/// Construction binds the exact text handle, upload identity, publication
/// phase, owner, and manifest destination. Private fields make the invalid
/// build-owned, non-text, non-Uploaded, and mismatched forms unrepresentable at
/// the database-staging boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedActiveManifestSplit {
    handle: index_v2::ActiveIndexHandle,
    intent_id: index_v2::TextUploadIntentId,
    partition: work::TextPartition,
    split: work::SplitRef,
    writer_epoch: index_v2::WriterEpoch,
    mutation_id: index_v2::MutationId,
}

impl PublishedActiveManifestSplit {
    /// Validates one in-memory upload result against its exact Active handle.
    pub(crate) fn try_new(
        handle: &index_v2::ActiveIndexHandle,
        intent: &work::TextUploadIntentValue,
    ) -> Result<Self> {
        let index_v2::ActiveIndexHandle::Text {
            identity,
            index_id,
            generation,
            record_revision,
            ..
        } = handle
        else {
            return Err(corruption(
                "active text attachment received a non-text generation handle",
            ));
        };
        let work::TextUploadOwner::ActiveMutation {
            writer_epoch,
            mutation_id,
            active_record_revision,
        } = intent.owner
        else {
            return Err(corruption(
                "active text attachment received a build-owned upload",
            ));
        };
        let work::TextUploadAttachment::ManifestSplit(split) = intent.attachment else {
            return Err(corruption(
                "active text attachment received a build-artifact destination",
            ));
        };
        if !matches!(intent.phase, work::TextUploadPhase::Uploaded) {
            return Err(corruption(
                "active text attachment requires definitive Uploaded publication",
            ));
        }
        if intent.index_id != *index_id
            || intent.identity != *identity
            || intent.generation != *generation
            || active_record_revision != *record_revision
            || intent.blob != split.blob()
        {
            return Err(corruption(
                "active text attachment identity disagrees with its Active generation",
            ));
        }

        Ok(Self {
            handle: handle.clone(),
            intent_id: intent.intent_id,
            partition: intent.partition.clone(),
            split,
            writer_epoch,
            mutation_id,
        })
    }
}

/// Fully revalidated attachment rows ready for infallible transaction staging.
///
/// Request-level orchestration validates every destination before consuming any
/// of these capabilities. This prevents a later invalid destination from
/// leaving earlier writes buffered in the caller's graph transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ValidatedActiveManifestSplit {
    prepared: PreparedActiveManifestSplit,
    proof_key: Bytes,
    proof_value: Bytes,
    authorization: work::UploadDestinationAuthorization,
}

/// Exact proof and historical destination authority staged by one graph commit.
///
/// Private construction binds outcome resolution to the same canonical bytes
/// that were buffered in the authoritative graph transaction. A resolver never
/// reconstructs success from manifest or blob presence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedActiveTextCommitProof {
    proof_key: Bytes,
    proof_value: Bytes,
    authorization: work::UploadDestinationAuthorization,
}

impl StagedActiveTextCommitProof {
    /// Returns the exact scoped proof row key staged with the graph mutation.
    pub(super) const fn proof_key(&self) -> &Bytes {
        &self.proof_key
    }

    /// Returns the exact encoded proof value staged with the graph mutation.
    pub(super) const fn proof_value(&self) -> &Bytes {
        &self.proof_value
    }

    /// Returns the destination and proof identity retained by the upload phase.
    pub(super) const fn authorization(&self) -> &work::UploadDestinationAuthorization {
        &self.authorization
    }
}

/// Source used to construct one immutable split after its version is known.
enum ActiveSplitSource<'a> {
    /// Production path derived from a canonical validated definition.
    Document {
        definition: &'a index_v2::ValidatedTextIndexDefinition,
        text: &'a str,
    },
    /// Storage-focused tests inject exact content-addressed bytes without
    /// paying the Tantivy construction cost for every corruption fixture.
    #[cfg(test)]
    TestPayload {
        payload: Bytes,
        split: work::SplitRef,
    },
}

/// Builds and admits one exact manifest/outbox unit before intent creation.
///
/// Every attachment source/destination row and the future prepared-upload
/// triple is serialized before admission. Request-level code must aggregate
/// this unit with the graph row and every sibling index action before it may
/// reserve or publish. The manifest revision is read before split construction,
/// so the document's logical version cannot disagree with the state row.
pub(crate) async fn prepare_active_manifest_document(
    transaction: &DbTransaction,
    handle: &index_v2::ActiveIndexHandle,
    definition: &index_v2::ValidatedTextIndexDefinition,
    partition: work::TextPartition,
    text: &str,
    entity: index_keys::IndexEntity,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveManifestSplit> {
    prepare_active_manifest_split_from(
        transaction,
        handle,
        partition,
        entity,
        limits,
        ActiveSplitSource::Document { definition, text },
    )
    .await
}

/// Injects one exact payload/reference pair for storage-focused unit tests.
#[cfg(test)]
async fn prepare_active_manifest_split(
    transaction: &DbTransaction,
    handle: &index_v2::ActiveIndexHandle,
    partition: work::TextPartition,
    split: work::SplitRef,
    entity: index_keys::IndexEntity,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveManifestSplit> {
    use sha2::{Digest, Sha256};

    let payload = (0_u8..=u8::MAX)
        .map(|seed| Bytes::copy_from_slice(&[seed; 64]))
        .find(|payload| {
            split.blob().size() == u64::try_from(payload.len()).unwrap_or(u64::MAX)
                && split.blob().hash() == &<[u8; 32]>::from(Sha256::digest(payload))
        })
        .ok_or_else(|| corruption("test Active split has no matching deterministic payload"))?;
    prepare_active_manifest_test_payload(
        transaction,
        handle,
        partition,
        payload,
        split,
        entity,
        limits,
    )
    .await
}

/// Injects an already content-addressed test payload into storage preparation.
#[cfg(test)]
pub(super) async fn prepare_active_manifest_test_payload(
    transaction: &DbTransaction,
    handle: &index_v2::ActiveIndexHandle,
    partition: work::TextPartition,
    payload: Bytes,
    split: work::SplitRef,
    entity: index_keys::IndexEntity,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveManifestSplit> {
    use sha2::{Digest, Sha256};

    if split.blob().size() != u64::try_from(payload.len()).unwrap_or(u64::MAX)
        || split.blob().hash() != &<[u8; 32]>::from(Sha256::digest(&payload))
    {
        return Err(corruption(
            "test Active payload disagrees with its content-addressed split",
        ));
    }
    prepare_active_manifest_split_from(
        transaction,
        handle,
        partition,
        entity,
        limits,
        ActiveSplitSource::TestPayload { payload, split },
    )
    .await
}

/// Shared storage preparation after the split-construction policy is fixed.
async fn prepare_active_manifest_split_from(
    transaction: &DbTransaction,
    handle: &index_v2::ActiveIndexHandle,
    partition: work::TextPartition,
    entity: index_keys::IndexEntity,
    limits: ActiveTextMutationLimits,
    source: ActiveSplitSource<'_>,
) -> Result<PreparedActiveManifestSplit> {
    let index_v2::ActiveIndexHandle::Text { .. } = handle else {
        return Err(corruption(
            "active text preflight received a non-text generation handle",
        ));
    };
    if handle.identity().element_kind() != entity.kind {
        return Err(corruption(
            "Active text attachment entity kind disagrees with its Active index",
        ));
    }
    let scope = handle.scope();
    let index_id = handle.index_id();
    let generation = handle.generation();
    let (record_key, record_value) =
        index_v2::repository::revalidate_active_handle_row(transaction, handle).await?;
    let mut observations = vec![ActiveAttachmentObservation {
        key: record_key,
        value: Some(record_value),
    }];

    let root_typed = index_keys::TextManifestRootKey {
        index_id,
        generation,
        partition: partition.fingerprint(),
    };
    let root_key =
        super::attachment::scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_typed));
    let root_bytes = transaction.get(&root_key).await?;
    observations.push(ActiveAttachmentObservation {
        key: root_key.clone(),
        value: root_bytes.clone(),
    });
    let root = match root_bytes {
        Some(root_bytes) => {
            let index_values::IndexV2WorkValue::TextManifestRoot(root) =
                index_values::decode_work_value(&root_bytes)?
            else {
                return Err(corruption(
                    "active text manifest root key contains another value kind",
                ));
            };
            root
        }
        None => work::TextManifestRootValue::empty(index_id, generation, partition.clone()),
    };
    if root.index_id() != index_id
        || root.generation() != generation
        || root.partition() != &partition
        || root_typed.partition != root.partition().fingerprint()
    {
        return Err(corruption(
            "active text manifest root key/value ownership mismatch",
        ));
    }

    let next_revision = root
        .revision()
        .checked_next()
        .map_err(|_| corruption("active text manifest revision is exhausted"))?;
    let logical_version = index_v2::TextLogicalVersion::new(next_revision.get())
        .expect("a non-zero manifest revision is a non-zero logical version");

    let (payload, split) = match source {
        ActiveSplitSource::Document { definition, text } => {
            if handle.text_definition() != Some(definition) {
                return Err(corruption(
                    "Active text split definition disagrees with its canonical handle",
                ));
            }
            let document = crate::search::text::TextDocumentInput::new(entity.id.get(), text)
                .with_logical_version(logical_version.get());
            let Some(unpublished) = crate::search::text::build_documents_as_split(
                &definition.to_runtime(),
                &[document],
            )?
            else {
                return Err(corruption(
                    "non-empty Active text document produced no immutable split",
                ));
            };
            let (payload, runtime_split) = unpublished.into_parts();
            let split = work::SplitRef::try_new(
                work::BlobRef::new(runtime_split.blob.sha256, runtime_split.blob.size_bytes),
                runtime_split.footer_offset,
                runtime_split.footer_len,
                runtime_split.hotcache_len,
                runtime_split.total_size_bytes,
            )
            .map_err(|error| {
                corruption(format!(
                    "Active text split metadata is invalid after construction: {error}"
                ))
            })?;
            (payload, split)
        }
        #[cfg(test)]
        ActiveSplitSource::TestPayload { payload, split } => (payload, split),
    };

    let (page_typed, page, slot, next_root) = if root.page_count() == 0 {
        let page_typed = index_keys::TextManifestPageKey {
            root: root_typed,
            page: 0,
        };
        let page_key = super::attachment::scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestPage(page_typed),
        );
        let existing_page = transaction.get(&page_key).await?;
        observations.push(ActiveAttachmentObservation {
            key: page_key,
            value: existing_page.clone(),
        });
        if existing_page.is_some() {
            return Err(corruption(
                "empty active text manifest has an occupied first page",
            ));
        }
        let page = work::TextManifestPageValue::try_new(
            index_id,
            generation,
            partition.clone(),
            0,
            vec![split],
        )
        .expect("one typed split always forms a valid first manifest page");
        let next_root = root
            .append_page(0, NonZeroU32::MIN)
            .expect("a validated empty root at its checked next revision accepts page zero");
        (page_typed, page, 0, next_root)
    } else {
        let last_page = root.page_count() - 1;
        let last_page_typed = index_keys::TextManifestPageKey {
            root: root_typed,
            page: last_page,
        };
        let last_page_key = super::attachment::scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestPage(last_page_typed),
        );
        let Some(last_page_bytes) = transaction.get(&last_page_key).await? else {
            return Err(corruption(
                "active text manifest is missing its last contiguous page",
            ));
        };
        observations.push(ActiveAttachmentObservation {
            key: last_page_key,
            value: Some(last_page_bytes.clone()),
        });
        let index_values::IndexV2WorkValue::TextManifestPage(last_page_value) =
            index_values::decode_work_value(&last_page_bytes)?
        else {
            return Err(corruption(
                "active text manifest page key contains another value kind",
            ));
        };
        if last_page_value.index_id() != index_id
            || last_page_value.generation() != generation
            || last_page_value.partition() != &partition
            || last_page_value.page() != last_page
        {
            return Err(corruption(
                "active text manifest page key/value ownership mismatch",
            ));
        }

        if last_page_value.entries().len() < work::TextManifestPageValue::MAX_ENTRIES {
            let slot = u32::try_from(last_page_value.entries().len())
                .expect("bounded manifest page slot fits u32");
            let entries = last_page_value
                .entries()
                .iter()
                .copied()
                .chain(std::iter::once(split))
                .collect();
            let page = work::TextManifestPageValue::try_new(
                index_id,
                generation,
                partition.clone(),
                last_page,
                entries,
            )
            .expect("a typed split appended below the page cap remains valid");
            let split_count = root
                .split_count()
                .checked_add(1)
                .expect("validated u32 page counts cannot contain u64::MAX splits");
            let next_root = work::TextManifestRootValue::try_new(
                index_id,
                generation,
                partition.clone(),
                next_revision,
                root.page_count(),
                split_count,
            )
            .map_err(|error| {
                corruption(format!(
                    "active text manifest root append is invalid: {error}"
                ))
            })?;
            (last_page_typed, page, slot, next_root)
        } else {
            let page_number = root.page_count();
            let page_typed = index_keys::TextManifestPageKey {
                root: root_typed,
                page: page_number,
            };
            let page_key = super::attachment::scoped_key(
                scope,
                index_keys::IndexV2Key::TextManifestPage(page_typed),
            );
            let existing_page = transaction.get(&page_key).await?;
            observations.push(ActiveAttachmentObservation {
                key: page_key,
                value: existing_page.clone(),
            });
            if existing_page.is_some() {
                return Err(corruption(
                    "active text next contiguous manifest page is occupied",
                ));
            }
            let page = work::TextManifestPageValue::try_new(
                index_id,
                generation,
                partition.clone(),
                page_number,
                vec![split],
            )
            .expect("one typed split always forms a valid next manifest page");
            let next_root = root
                .append_page(page_number, NonZeroU32::MIN)
                .map_err(|error| {
                    corruption(format!("active text manifest root is full: {error}"))
                })?;
            (page_typed, page, 0, next_root)
        }
    };

    let page_logical_key = index_keys::IndexV2Key::TextManifestPage(page_typed);
    let page_key = super::attachment::scoped_key(scope, page_logical_key.clone());
    let state_typed = index_keys::TextEntityStateKey {
        root: root_typed,
        entity,
    };
    let state_key =
        super::attachment::scoped_key(scope, index_keys::IndexV2Key::TextEntityState(state_typed));
    let state_bytes = transaction.get(&state_key).await?;
    observations.push(ActiveAttachmentObservation {
        key: state_key.clone(),
        value: state_bytes.clone(),
    });
    if let Some(state_bytes) = state_bytes {
        let index_values::IndexV2WorkValue::TextEntityState(state) =
            index_values::decode_work_value(&state_bytes)?
        else {
            return Err(corruption(
                "active text entity-state key contains another value kind",
            ));
        };
        if state.index_id != index_id
            || state.generation != generation
            || state.partition != partition
            || state.entity_kind != entity.kind
            || state.entity_id != entity.id
            || state.logical_version >= logical_version
        {
            return Err(corruption(
                "active text entity-state ownership or version mismatch",
            ));
        }
    }

    let (reference_key, reference_value) =
        super::attachment::manifest_page_reachability_row(split.blob(), scope, page_typed, slot);
    let existing_reference = transaction.get(&reference_key).await?;
    observations.push(ActiveAttachmentObservation {
        key: reference_key.clone(),
        value: existing_reference.clone(),
    });
    if existing_reference.is_some() {
        return Err(corruption(
            "active text manifest reachability slot is already occupied",
        ));
    }

    let root_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextManifestRoot(next_root),
    );
    let page_value =
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(page));
    let state_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextEntityState(work::TextEntityStateValue {
            index_id,
            generation,
            partition: partition.clone(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version,
            live: true,
        }),
    );
    let sizing_intent_id = index_v2::TextUploadIntentId::from_bytes([u8::MAX; 16])
        .expect("all-ones sizing intent ID is non-nil");
    let sizing_writer_epoch = index_v2::WriterEpoch::from_bytes([u8::MAX; 16])
        .expect("all-ones sizing writer epoch is non-nil");
    let sizing_mutation_id = index_v2::MutationId::from_bytes([u8::MAX; 16])
        .expect("all-ones sizing mutation ID is non-nil");
    let sizing_proof_logical_key =
        index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
            index_id,
            generation,
            intent_id: sizing_intent_id,
        });
    let sizing_proof_key = super::attachment::scoped_key(scope, sizing_proof_logical_key.clone());
    let sizing_proof_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::ActiveMutationCommitProof(
            work::ActiveMutationCommitProofValue {
                intent_id: sizing_intent_id,
                index_id,
                generation,
                partition: partition.clone(),
                writer_epoch: sizing_writer_epoch,
                mutation_id: sizing_mutation_id,
                active_record_revision: handle.record_revision(),
                logical_version,
                destination: work::TextManifestSplitLocation::try_new(page_typed.page, slot)
                    .expect("prepared manifest destination page and slot are bounded"),
                split,
            },
        ),
    );
    let observed_row_bytes = observations.iter().fold(0_u64, |bytes, observation| {
        bytes
            .saturating_add(u64::try_from(observation.key.len()).unwrap_or(u64::MAX))
            .saturating_add(
                observation
                    .value
                    .as_ref()
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            )
    });
    let upload_spec = work::TextUploadSpec::try_new(
        index_id,
        handle.identity().clone(),
        generation,
        partition.clone(),
        split.blob(),
        work::TextUploadOwner::ActiveMutation {
            writer_epoch: sizing_writer_epoch,
            mutation_id: sizing_mutation_id,
            active_record_revision: handle.record_revision(),
        },
        work::TextUploadAttachment::ManifestSplit(split),
    )
    .expect("an exact Active handle, partition, and split form a valid upload specification");
    let upload_measurements = super::upload::measure_prepared_upload_spec(scope, &upload_spec)?;
    let input_bytes = observed_row_bytes
        .saturating_mul(2)
        .saturating_add(u64::try_from(sizing_proof_key.len()).unwrap_or(u64::MAX))
        .saturating_add(upload_measurements.input_bytes());
    let output_rows = [
        (&root_key, &root_value),
        (&page_key, &page_value),
        (&state_key, &state_value),
        (&reference_key, &reference_value),
        (&sizing_proof_key, &sizing_proof_value),
    ];
    let output_operations = u64::try_from(output_rows.len())
        .unwrap_or(u64::MAX)
        .saturating_add(upload_measurements.output_operations());
    let output_bytes = output_rows
        .iter()
        .fold(0_u64, |bytes, (key, value)| {
            bytes
                .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
        })
        .saturating_add(upload_measurements.output_bytes());
    let measurements = ActiveTextMutationMeasurements::try_admit(
        limits,
        input_bytes,
        output_operations,
        output_bytes,
        split.blob().size(),
        u64::try_from(page_value.len()).unwrap_or(u64::MAX),
    )?;

    Ok(PreparedActiveManifestSplit {
        handle: handle.clone(),
        partition,
        logical_version,
        payload,
        split,
        observations,
        root_key,
        root_value,
        page_typed,
        page_key,
        page_value,
        state_key,
        state_value,
        reference_key,
        reference_value,
        proof_key_len: sizing_proof_key.len(),
        proof_value_len: sizing_proof_value.len(),
        slot,
        measurements,
    })
}

/// Revalidates one admitted and definitively published Active manifest split.
///
/// The function performs no staging and touches no intent, pointer, or
/// intent-owned reference row. Its private-field result is the only input the
/// infallible staging half accepts.
pub(super) async fn validate_active_manifest_split(
    transaction: &DbTransaction,
    prepared: &PreparedActiveManifestSplit,
    published: &PublishedActiveManifestSplit,
) -> Result<ValidatedActiveManifestSplit> {
    if published.handle != prepared.handle
        || published.partition != prepared.partition
        || published.split != prepared.split
    {
        return Err(corruption(
            "published Active text split disagrees with its admitted preflight",
        ));
    }
    for observation in &prepared.observations {
        if transaction.get(&observation.key).await? != observation.value {
            return Err(corruption(
                "Active text attachment input changed after serialized preflight",
            ));
        }
    }

    let index_id = prepared.handle.index_id();
    let generation = prepared.handle.generation();
    let proof_owner = index_keys::TextIntentOwnedKey {
        index_id,
        generation,
        intent_id: published.intent_id,
    };
    let proof_logical_key = index_keys::IndexV2Key::ActiveMutationCommitProof(proof_owner);
    let proof_key =
        super::attachment::scoped_key(prepared.handle.scope(), proof_logical_key.clone());
    if transaction.get(&proof_key).await?.is_some() {
        return Err(corruption(
            "active text mutation proof destination is already occupied",
        ));
    }
    let proof_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::ActiveMutationCommitProof(
            work::ActiveMutationCommitProofValue {
                intent_id: published.intent_id,
                index_id,
                generation,
                partition: prepared.partition.clone(),
                writer_epoch: published.writer_epoch,
                mutation_id: published.mutation_id,
                active_record_revision: prepared.handle.record_revision(),
                logical_version: prepared.logical_version,
                destination: work::TextManifestSplitLocation::try_new(
                    prepared.page_typed.page,
                    prepared.slot,
                )
                .expect("prepared manifest destination page and slot are bounded"),
                split: prepared.split,
            },
        ),
    );
    if proof_key.len() != prepared.proof_key_len || proof_value.len() != prepared.proof_value_len {
        return Err(HelixDbError::InvariantViolation(
            "active text proof encoding changed after exact serialized preflight".to_string(),
        ));
    }
    let page_logical_key = index_keys::IndexV2Key::TextManifestPage(prepared.page_typed);
    Ok(ValidatedActiveManifestSplit {
        prepared: prepared.clone(),
        proof_key,
        proof_value,
        authorization: work::UploadDestinationAuthorization::try_new(
            index_keys::BlobReferenceOwnerKind::ManifestPageSplit,
            page_logical_key.to_bytes(),
            prepared.slot,
            Some(proof_logical_key.to_bytes()),
        )
        .expect("canonical bounded manifest and proof keys always authorize their exact slot"),
    })
}

/// Stages rows that were all validated before the request began buffering writes.
pub(super) fn stage_validated_active_manifest_split(
    transaction: &DbTransaction,
    validated: ValidatedActiveManifestSplit,
) -> StagedActiveTextCommitProof {
    transaction
        .put(&validated.prepared.root_key, &validated.prepared.root_value)
        .expect("SlateDB transactional put only buffers a validated key/value pair");
    transaction
        .put(&validated.prepared.page_key, &validated.prepared.page_value)
        .expect("SlateDB transactional put only buffers a validated key/value pair");
    transaction
        .put(
            &validated.prepared.state_key,
            &validated.prepared.state_value,
        )
        .expect("SlateDB transactional put only buffers a validated key/value pair");
    transaction
        .put(
            &validated.prepared.reference_key,
            &validated.prepared.reference_value,
        )
        .expect("SlateDB transactional put only buffers a validated key/value pair");
    transaction
        .put(&validated.proof_key, &validated.proof_value)
        .expect("SlateDB transactional put only buffers a validated key/value pair");
    StagedActiveTextCommitProof {
        proof_key: validated.proof_key,
        proof_value: validated.proof_value,
        authorization: validated.authorization,
    }
}

/// Test-only single-attachment composition of validation and staging.
#[cfg(test)]
async fn stage_active_manifest_split(
    transaction: &DbTransaction,
    prepared: &PreparedActiveManifestSplit,
    published: &PublishedActiveManifestSplit,
) -> Result<StagedActiveTextCommitProof> {
    let validated = validate_active_manifest_split(transaction, prepared, published).await?;
    Ok(stage_validated_active_manifest_split(
        transaction,
        validated,
    ))
}

/// Constructs the stable corruption category for persisted or staged disagreement.
fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{
        ActiveTextMutationLimits, SearchIndexBackfillLimits, SearchIndexBatchLimits,
        SecondaryIndexDefinition, TextBackfillCompactionLimits, TextBuildArtifactLimits,
        TextIndexDefinition,
    };
    use crate::encoding::v1::keys::tenant::DataScope;
    use crate::encoding::v1::keys::{DataKeyKind, Key, NodePropertyKey};
    use crate::encoding::v1::property::{encode_properties, Property};
    use crate::index_v2::blob_publication::BlobPublicationPermit;
    use crate::index_v2::text::upload::{stage_prepared_upload, PreparedTextUploadIntent};
    use crate::index_v2::{
        ClaimSequence, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId,
        IndexOperationId, IndexOperationRevision, IndexRecordV2, IndexRevision,
        IndexStateTransition, MutationId, OperationClaim, PhysicalGeneration, TextManifestRevision,
        TextUploadIntentId, ValidatedDynamicIndexDefinition, WriterEpoch,
    };

    /// Opens one isolated database for the Active attachment transaction contract.
    async fn raw_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new())).await.unwrap()
    }

    /// Returns the canonical text definition and identity used by the contract.
    fn text_definition() -> ValidatedDynamicIndexDefinition {
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").unwrap(),
        )
        .unwrap()
    }

    /// Builds one deterministic single-document split reference.
    fn split(seed: u8) -> work::SplitRef {
        let payload = [seed; 64];
        let blob = work::BlobRef::new(Sha256::digest(payload).into(), payload.len() as u64);
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap()
    }

    /// Builds independent positive Active ceilings for exact boundary tests.
    fn active_limits(
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
        split_bytes: u64,
        manifest_page_bytes: u64,
    ) -> ActiveTextMutationLimits {
        SearchIndexBackfillLimits::try_new(
            SearchIndexBatchLimits::try_new(
                NonZeroUsize::MIN,
                NonZeroU64::new(input_bytes).unwrap(),
                NonZeroU64::new(output_operations).unwrap(),
                NonZeroU64::new(output_bytes).unwrap(),
                NonZeroU64::MIN,
            )
            .unwrap(),
            NonZeroUsize::MIN,
            TextBuildArtifactLimits::new(NonZeroUsize::MIN, NonZeroU64::MIN),
            TextBackfillCompactionLimits::new(
                NonZeroUsize::MIN,
                NonZeroU64::new(input_bytes).unwrap(),
                NonZeroU64::MIN,
                NonZeroU64::new(split_bytes).unwrap(),
                NonZeroU64::new(manifest_page_bytes).unwrap(),
            ),
        )
        .unwrap()
        .active_text_mutation()
    }

    /// Builds one prepared Active upload whose immutable identity matches a handle.
    fn prepared_upload(
        handle: &index_v2::ActiveIndexHandle,
        seed: u8,
        split: work::SplitRef,
    ) -> PreparedTextUploadIntent {
        let intent_id = TextUploadIntentId::from_bytes([seed; 16]).unwrap();
        PreparedTextUploadIntent::try_new(
            intent_id,
            handle.index_id(),
            handle.identity().clone(),
            handle.generation(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermit::from_id(
                index_v2::BlobPublicationPermitId::from_bytes([seed + 1; 16]).unwrap(),
            ),
            work::TextUploadOwner::ActiveMutation {
                writer_epoch: WriterEpoch::from_bytes([seed + 2; 16]).unwrap(),
                mutation_id: MutationId::from_bytes([seed + 3; 16]).unwrap(),
                active_record_revision: handle.record_revision(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap()
    }

    /// Projects the in-memory post-publication value without changing durable intent rows.
    fn uploaded_value(
        prepared: &PreparedTextUploadIntent,
        seed: u8,
    ) -> work::TextUploadIntentValue {
        prepared
            .value()
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([seed + 2; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap()
            .publication_succeeded()
            .unwrap()
    }

    /// Constructs one exact scoped V2 key for post-commit assertions.
    fn scoped_key(scope: DataScope, logical: index_keys::IndexV2Key) -> bytes::Bytes {
        Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(logical),
        }
        .to_bytes()
    }

    /// Shared canonical rows for fail-closed Active attachment tests.
    struct ActiveAttachmentFixture {
        db: Db,
        scope: DataScope,
        handle: index_v2::ActiveIndexHandle,
        root_typed: index_keys::TextManifestRootKey,
        root_key: bytes::Bytes,
    }

    impl ActiveAttachmentFixture {
        /// Opens an isolated database with one canonical Active text record.
        async fn open(name: &str, seed_empty_root: bool) -> Self {
            let db = raw_db(name).await;
            let scope = DataScope::LegacyUnscoped;
            let building = IndexRecordV2::building(
                IndexId::initial(),
                text_definition(),
                IndexRevision::initial(),
                PhysicalGeneration::Text {
                    generation: IndexGenerationId::initial(),
                },
                IndexOperationId::from_bytes([1; 16]).unwrap(),
            )
            .unwrap();
            let active = building.transition(IndexStateTransition::Activate).unwrap();
            let handle = index_v2::ActiveIndexHandle::try_from_record(scope, &active).unwrap();
            db.put(
                scoped_key(
                    scope,
                    index_keys::IndexV2Key::index_record(active.identity().clone()),
                ),
                index_values::encode_index_record(&active),
            )
            .await
            .unwrap();
            let root_typed = index_keys::TextManifestRootKey {
                index_id: handle.index_id(),
                generation: handle.generation(),
                partition: work::TextPartition::Unpartitioned.fingerprint(),
            };
            let root_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_typed));
            if seed_empty_root {
                db.put(
                    root_key.clone(),
                    index_values::encode_work_value(
                        &index_values::IndexV2WorkValue::TextManifestRoot(
                            work::TextManifestRootValue::empty(
                                handle.index_id(),
                                handle.generation(),
                                work::TextPartition::Unpartitioned,
                            ),
                        ),
                    ),
                )
                .await
                .unwrap();
            }
            Self {
                db,
                scope,
                handle,
                root_typed,
                root_key,
            }
        }

        /// Validates one deterministic in-memory Uploaded capability.
        fn published(&self, seed: u8) -> PublishedActiveManifestSplit {
            let split = split(seed);
            let prepared = prepared_upload(&self.handle, seed, split);
            PublishedActiveManifestSplit::try_new(&self.handle, &uploaded_value(&prepared, seed))
                .unwrap()
        }

        /// Runs one expected preflight rejection in a disposable graph transaction.
        async fn prepare_for_rejection(
            &self,
            published: &PublishedActiveManifestSplit,
            entity: index_keys::IndexEntity,
        ) -> Result<PreparedActiveManifestSplit> {
            let transaction = self
                .db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            prepare_active_manifest_split(
                &transaction,
                &self.handle,
                published.partition.clone(),
                published.split,
                entity,
                crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await
        }
    }

    /// Requires the stable corruption category and exact fail-closed reason.
    fn expect_corruption<T: std::fmt::Debug>(result: Result<T>, expected: &str) {
        let Err(HelixDbError::IndexCatalogCorruption(actual)) = result else {
            panic!("expected index catalog corruption, got {result:?}");
        };
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn published_capability_rejects_every_invalid_upload_shape() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-published-capability", false)
                .await;
        let exact_split = split(40);
        let prepared = prepared_upload(&fixture.handle, 40, exact_split);
        let uploaded = uploaded_value(&prepared, 40);

        let secondary_definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("Document", "slug").unwrap(),
        )
        .unwrap();
        let secondary_active = IndexRecordV2::building(
            IndexId::new(2).unwrap(),
            secondary_definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([41; 16]).unwrap(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let secondary_handle =
            index_v2::ActiveIndexHandle::try_from_record(fixture.scope, &secondary_active).unwrap();
        expect_corruption(
            PublishedActiveManifestSplit::try_new(&secondary_handle, &uploaded),
            "active text attachment received a non-text generation handle",
        );
        let non_text_preflight = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        expect_corruption(
            prepare_active_manifest_split(
                &non_text_preflight,
                &secondary_handle,
                work::TextPartition::Unpartitioned,
                exact_split,
                index_keys::IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(40),
                },
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            "active text preflight received a non-text generation handle",
        );
        drop(non_text_preflight);

        let wrong_entity_kind = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        expect_corruption(
            prepare_active_manifest_split(
                &wrong_entity_kind,
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                exact_split,
                index_keys::IndexEntity {
                    kind: IndexElementKind::Edge,
                    id: IndexEntityId::new(40),
                },
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            "Active text attachment entity kind disagrees with its Active index",
        );
        drop(wrong_entity_kind);

        let mut build_owned = uploaded.clone();
        build_owned.owner = work::TextUploadOwner::Build {
            operation_id: IndexOperationId::from_bytes([42; 16]).unwrap(),
            expected_operation_revision: IndexOperationRevision::initial(),
        };
        expect_corruption(
            PublishedActiveManifestSplit::try_new(&fixture.handle, &build_owned),
            "active text attachment received a build-owned upload",
        );

        let mut build_artifact = uploaded.clone();
        build_artifact.attachment = work::TextUploadAttachment::BuildArtifact {
            artifact_ordinal: 0,
            split: exact_split,
        };
        expect_corruption(
            PublishedActiveManifestSplit::try_new(&fixture.handle, &build_artifact),
            "active text attachment received a build-artifact destination",
        );

        let mut prepared_phase = uploaded.clone();
        prepared_phase.phase = work::TextUploadPhase::Prepared;
        expect_corruption(
            PublishedActiveManifestSplit::try_new(&fixture.handle, &prepared_phase),
            "active text attachment requires definitive Uploaded publication",
        );

        let mut wrong_identity = uploaded;
        wrong_identity.index_id = IndexId::new(2).unwrap();
        expect_corruption(
            PublishedActiveManifestSplit::try_new(&fixture.handle, &wrong_identity),
            "active text attachment identity disagrees with its Active generation",
        );

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn manifest_root_contract_creates_missing_and_rejects_invalid_state() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-root-contract", false).await;
        let published = fixture.published(50);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(50),
        };

        let absent_root = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared_absent_root = prepare_active_manifest_split(
            &absent_root,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared_absent_root.logical_version().get(), 2);
        drop(absent_root);

        let wrong_kind = work::TextManifestPageValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            0,
            vec![split(51)],
        )
        .unwrap();
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    wrong_kind,
                )),
            )
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest root key contains another value kind",
        );

        let mismatched_root = work::TextManifestRootValue::empty(
            IndexId::new(2).unwrap(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
        );
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    mismatched_root,
                )),
            )
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest root key/value ownership mismatch",
        );

        let exhausted_root = work::TextManifestRootValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            TextManifestRevision::new(u64::MAX).unwrap(),
            0,
            0,
        )
        .unwrap();
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    exhausted_root,
                )),
            )
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest revision is exhausted",
        );

        let empty_root = work::TextManifestRootValue::empty(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
        );
        let encoded_empty_root = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::TextManifestRoot(empty_root),
        );
        fixture
            .db
            .put(fixture.root_key.clone(), encoded_empty_root.clone())
            .await
            .unwrap();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let admitted = prepare_active_manifest_split(
            &transaction,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(admitted.logical_version().get(), 2);
        assert!(admitted.measurements().input_bytes() > 0);
        assert_eq!(admitted.measurements().output_operations(), 8);
        drop(transaction);

        let first_page_typed = index_keys::TextManifestPageKey {
            root: fixture.root_typed,
            page: 0,
        };
        fixture
            .db
            .put(
                scoped_key(
                    fixture.scope,
                    index_keys::IndexV2Key::TextManifestPage(first_page_typed),
                ),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    work::TextManifestPageValue::try_new(
                        fixture.handle.index_id(),
                        fixture.handle.generation(),
                        work::TextPartition::Unpartitioned,
                        0,
                        vec![split(52)],
                    )
                    .unwrap(),
                )),
            )
            .await
            .unwrap();
        fixture
            .db
            .put(fixture.root_key.clone(), encoded_empty_root)
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "empty active text manifest has an occupied first page",
        );

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn exact_resource_boundaries_reject_without_staging_index_rows() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-exact-limits", true).await;
        let published = fixture.published(55);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(55),
        };
        let baseline_root = fixture.db.get(&fixture.root_key).await.unwrap().unwrap();
        let baseline = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let admitted = prepare_active_manifest_split(
            &baseline,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let measurements = admitted.measurements();
        assert_eq!(admitted.handle(), &fixture.handle);
        assert_eq!(admitted.partition(), &work::TextPartition::Unpartitioned);
        assert_eq!(admitted.split(), published.split);
        drop(baseline);

        let exact = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        prepare_active_manifest_split(
            &exact,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            active_limits(
                measurements.input_bytes(),
                measurements.output_operations(),
                measurements.output_bytes(),
                measurements.split_bytes(),
                measurements.manifest_page_bytes(),
            ),
        )
        .await
        .unwrap();
        exact.commit().await.unwrap();

        let cases = [
            (
                active_limits(
                    measurements.input_bytes() - 1,
                    u64::MAX,
                    u64::MAX,
                    u64::MAX,
                    u64::MAX,
                ),
                crate::error::ActiveTextMutationResource::InputBytes,
                measurements.input_bytes(),
            ),
            (
                active_limits(
                    u64::MAX,
                    measurements.output_operations() - 1,
                    u64::MAX,
                    u64::MAX,
                    u64::MAX,
                ),
                crate::error::ActiveTextMutationResource::OutputOperations,
                measurements.output_operations(),
            ),
            (
                active_limits(
                    u64::MAX,
                    u64::MAX,
                    measurements.output_bytes() - 1,
                    u64::MAX,
                    measurements.manifest_page_bytes(),
                ),
                crate::error::ActiveTextMutationResource::OutputBytes,
                measurements.output_bytes(),
            ),
            (
                active_limits(
                    u64::MAX,
                    u64::MAX,
                    u64::MAX,
                    measurements.split_bytes() - 1,
                    u64::MAX,
                ),
                crate::error::ActiveTextMutationResource::SplitBytes,
                measurements.split_bytes(),
            ),
            (
                active_limits(
                    u64::MAX,
                    u64::MAX,
                    u64::MAX,
                    u64::MAX,
                    measurements.manifest_page_bytes() - 1,
                ),
                crate::error::ActiveTextMutationResource::ManifestPageBytes,
                measurements.manifest_page_bytes(),
            ),
        ];
        for (limits, expected_resource, observed) in cases {
            let transaction = fixture
                .db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(matches!(
                prepare_active_manifest_split(
                    &transaction,
                    &fixture.handle,
                    published.partition.clone(),
                    published.split,
                    entity,
                    limits,
                )
                .await,
                Err(HelixDbError::ActiveTextMutationLimitExceeded {
                    resource,
                    observed: actual,
                    limit,
                }) if resource == expected_resource && actual == observed && limit + 1 == observed
            ));
            transaction.commit().await.unwrap();
        }

        let page_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                root: fixture.root_typed,
                page: 0,
            }),
        );
        let state_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                root: fixture.root_typed,
                entity,
            }),
        );
        assert_eq!(
            fixture.db.get(&fixture.root_key).await.unwrap().unwrap(),
            baseline_root
        );
        assert!(fixture.db.get(page_key).await.unwrap().is_none());
        assert!(fixture.db.get(state_key).await.unwrap().is_none());

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn prepared_attachment_cannot_be_replayed_after_an_observed_row_changes() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-stale-preflight", true).await;
        let published = fixture.published(56);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(56),
        };
        let original = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = prepare_active_manifest_split(
            &original,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        drop(original);

        let mismatch = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut wrong_published = published.clone();
        wrong_published.split = split(57);
        expect_corruption(
            stage_active_manifest_split(&mismatch, &prepared, &wrong_published).await,
            "published Active text split disagrees with its admitted preflight",
        );
        mismatch.commit().await.unwrap();

        let invariant = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut wrong_proof_shape = prepared.clone();
        wrong_proof_shape.proof_key_len += 1;
        assert!(matches!(
            stage_active_manifest_split(&invariant, &wrong_proof_shape, &published).await,
            Err(HelixDbError::InvariantViolation(reason))
                if reason == "active text proof encoding changed after exact serialized preflight"
        ));
        invariant.commit().await.unwrap();

        let replacement_root = work::TextManifestRootValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            TextManifestRevision::new(2).unwrap(),
            0,
            0,
        )
        .unwrap();
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    replacement_root,
                )),
            )
            .await
            .unwrap();

        let replay = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        expect_corruption(
            stage_active_manifest_split(&replay, &prepared, &published).await,
            "Active text attachment input changed after serialized preflight",
        );
        replay.commit().await.unwrap();

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn existing_manifest_state_and_destination_rows_fail_closed_before_commit() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-existing-state", false).await;
        let published = fixture.published(60);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(60),
        };
        let next_version = index_v2::TextLogicalVersion::new(2).unwrap();
        let root = work::TextManifestRootValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            TextManifestRevision::initial(),
            1,
            1,
        )
        .unwrap();
        let encoded_root = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::TextManifestRoot(root.clone()),
        );
        fixture
            .db
            .put(fixture.root_key.clone(), encoded_root.clone())
            .await
            .unwrap();
        let page_typed = index_keys::TextManifestPageKey {
            root: fixture.root_typed,
            page: 0,
        };
        let page_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextManifestPage(page_typed),
        );

        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest is missing its last contiguous page",
        );

        fixture
            .db
            .put(page_key.clone(), encoded_root.clone())
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest page key contains another value kind",
        );

        let mismatched_page = work::TextManifestPageValue::try_new(
            IndexId::new(2).unwrap(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            0,
            vec![split(61)],
        )
        .unwrap();
        fixture
            .db
            .put(
                page_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    mismatched_page,
                )),
            )
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest page key/value ownership mismatch",
        );

        let page = work::TextManifestPageValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            0,
            vec![split(61)],
        )
        .unwrap();
        fixture
            .db
            .put(
                page_key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    page,
                )),
            )
            .await
            .unwrap();
        let state_typed = index_keys::TextEntityStateKey {
            root: fixture.root_typed,
            entity,
        };
        let state_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextEntityState(state_typed),
        );
        fixture
            .db
            .put(state_key.clone(), encoded_root.clone())
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text entity-state key contains another value kind",
        );

        let stale_state = work::TextEntityStateValue {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: work::TextPartition::Unpartitioned,
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version: next_version,
            live: true,
        };
        fixture
            .db
            .put(
                state_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                    stale_state,
                )),
            )
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text entity-state ownership or version mismatch",
        );

        let previous_state = work::TextEntityStateValue {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: work::TextPartition::Unpartitioned,
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version: index_v2::TextLogicalVersion::initial(),
            live: false,
        };
        fixture
            .db
            .put(
                state_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                    previous_state,
                )),
            )
            .await
            .unwrap();
        let proof_logical =
            index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
                index_id: fixture.handle.index_id(),
                generation: fixture.handle.generation(),
                intent_id: published.intent_id,
            });
        let proof_key = scoped_key(fixture.scope, proof_logical);
        fixture
            .db
            .put(proof_key.clone(), encoded_root.clone())
            .await
            .unwrap();
        let proof_transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let proof_preflight = prepare_active_manifest_split(
            &proof_transaction,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        expect_corruption(
            stage_active_manifest_split(&proof_transaction, &proof_preflight, &published).await,
            "active text mutation proof destination is already occupied",
        );
        drop(proof_transaction);

        fixture.db.delete(proof_key).await.unwrap();
        let (reference_key, _) = super::super::attachment::manifest_page_reachability_row(
            published.split.blob(),
            fixture.scope,
            page_typed,
            1,
        );
        fixture
            .db
            .put(reference_key.clone(), encoded_root)
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text manifest reachability slot is already occupied",
        );

        fixture.db.delete(reference_key).await.unwrap();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let preflight = prepare_active_manifest_split(
            &transaction,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(preflight.logical_version(), next_version);
        let staged = stage_active_manifest_split(&transaction, &preflight, &published)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(staged.authorization().owner_slot, 1);
        let index_values::IndexV2WorkValue::TextEntityState(committed_state) =
            index_values::decode_work_value(&fixture.db.get(state_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("entity-state key retains an entity-state value");
        };
        assert_eq!(committed_state.logical_version, next_version);
        assert!(committed_state.live);

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn full_manifest_page_rolls_over_only_into_an_empty_contiguous_slot() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-page-rollover", false).await;
        let published = fixture.published(70);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(70),
        };
        let root = work::TextManifestRootValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            TextManifestRevision::initial(),
            1,
            work::TextManifestPageValue::MAX_ENTRIES as u64,
        )
        .unwrap();
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    root,
                )),
            )
            .await
            .unwrap();
        let full_page = work::TextManifestPageValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            0,
            vec![split(71); work::TextManifestPageValue::MAX_ENTRIES],
        )
        .unwrap();
        fixture
            .db
            .put(
                scoped_key(
                    fixture.scope,
                    index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                        root: fixture.root_typed,
                        page: 0,
                    }),
                ),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    full_page,
                )),
            )
            .await
            .unwrap();
        let next_page_typed = index_keys::TextManifestPageKey {
            root: fixture.root_typed,
            page: 1,
        };
        let next_page_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextManifestPage(next_page_typed),
        );
        let occupied_page = work::TextManifestPageValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            1,
            vec![split(72)],
        )
        .unwrap();
        fixture
            .db
            .put(
                next_page_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    occupied_page,
                )),
            )
            .await
            .unwrap();
        expect_corruption(
            fixture.prepare_for_rejection(&published, entity).await,
            "active text next contiguous manifest page is occupied",
        );

        fixture.db.delete(next_page_key.clone()).await.unwrap();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let preflight = prepare_active_manifest_split(
            &transaction,
            &fixture.handle,
            published.partition.clone(),
            published.split,
            entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(preflight.logical_version().get(), 2);
        let staged = stage_active_manifest_split(&transaction, &preflight, &published)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(staged.authorization().owner_slot, 0);
        let index_values::IndexV2WorkValue::TextManifestRoot(committed_root) =
            index_values::decode_work_value(
                &fixture.db.get(&fixture.root_key).await.unwrap().unwrap(),
            )
            .unwrap()
        else {
            panic!("root key retains a manifest root");
        };
        assert_eq!(committed_root.revision().get(), 2);
        assert_eq!(committed_root.page_count(), 2);
        assert_eq!(
            committed_root.split_count(),
            work::TextManifestPageValue::MAX_ENTRIES as u64 + 1
        );
        let index_values::IndexV2WorkValue::TextManifestPage(committed_page) =
            index_values::decode_work_value(&fixture.db.get(next_page_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("next page key retains a manifest page");
        };
        assert_eq!(committed_page.entries(), &[published.split]);

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn manifest_count_disagreement_and_page_count_exhaustion_fail_closed() {
        let fixture =
            ActiveAttachmentFixture::open("active-text-attachment-count-bounds", false).await;
        let published = fixture.published(80);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(80),
        };
        let inconsistent_root = work::TextManifestRootValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            TextManifestRevision::initial(),
            2,
            (work::TextManifestPageValue::MAX_ENTRIES as u64) * 2,
        )
        .unwrap();
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    inconsistent_root,
                )),
            )
            .await
            .unwrap();
        let second_page_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                root: fixture.root_typed,
                page: 1,
            }),
        );
        fixture
            .db
            .put(
                second_page_key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    work::TextManifestPageValue::try_new(
                        fixture.handle.index_id(),
                        fixture.handle.generation(),
                        work::TextPartition::Unpartitioned,
                        1,
                        vec![split(81)],
                    )
                    .unwrap(),
                )),
            )
            .await
            .unwrap();
        let result = fixture.prepare_for_rejection(&published, entity).await;
        let Err(HelixDbError::IndexCatalogCorruption(reason)) = result else {
            panic!("expected invalid root append corruption, got {result:?}");
        };
        assert!(reason.starts_with("active text manifest root append is invalid:"));

        let exhausted_root = work::TextManifestRootValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            TextManifestRevision::initial(),
            u32::MAX,
            u64::from(u32::MAX),
        )
        .unwrap();
        fixture
            .db
            .put(
                fixture.root_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                    exhausted_root,
                )),
            )
            .await
            .unwrap();
        let last_page_key = scoped_key(
            fixture.scope,
            index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                root: fixture.root_typed,
                page: u32::MAX - 1,
            }),
        );
        let full_last_page = work::TextManifestPageValue::try_new(
            fixture.handle.index_id(),
            fixture.handle.generation(),
            work::TextPartition::Unpartitioned,
            u32::MAX - 1,
            vec![split(82); work::TextManifestPageValue::MAX_ENTRIES],
        )
        .unwrap();
        fixture
            .db
            .put(
                last_page_key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                    full_last_page,
                )),
            )
            .await
            .unwrap();
        let result = fixture.prepare_for_rejection(&published, entity).await;
        let Err(HelixDbError::IndexCatalogCorruption(reason)) = result else {
            panic!("expected page-count exhaustion corruption, got {result:?}");
        };
        assert!(reason.starts_with("active text manifest root is full:"));

        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn disjoint_intent_and_graph_transactions_commit_while_active_read_conflicts_with_ddl() {
        let db = raw_db("active-text-attachment-transaction-contract").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = text_definition();
        let building = IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([1; 16]).unwrap(),
        )
        .unwrap();
        let active = building.transition(IndexStateTransition::Activate).unwrap();
        let handle = index_v2::ActiveIndexHandle::try_from_record(scope, &active).unwrap();
        let record_key = scoped_key(
            scope,
            index_keys::IndexV2Key::index_record(active.identity().clone()),
        );
        db.put(
            record_key.clone(),
            index_values::encode_index_record(&active),
        )
        .await
        .unwrap();
        let root_typed = index_keys::TextManifestRootKey {
            index_id: handle.index_id(),
            generation: handle.generation(),
            partition: work::TextPartition::Unpartitioned.fingerprint(),
        };
        let root_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_typed));
        db.put(
            root_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::empty(
                    handle.index_id(),
                    handle.generation(),
                    work::TextPartition::Unpartitioned,
                ),
            )),
        )
        .await
        .unwrap();

        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(7)),
        }
        .to_bytes();
        let graph_value = encode_properties(&[
            Property::string("$label", "Document"),
            Property::string("body", "committed with the manifest"),
        ]);
        let graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        graph.put(graph_key.clone(), graph_value.clone()).unwrap();

        let first_split = split(10);
        let first_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        };
        let first_attachment = prepare_active_manifest_split(
            &graph,
            &handle,
            work::TextPartition::Unpartitioned,
            first_split,
            first_entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let first_prepared = prepared_upload(&handle, 10, first_split);
        let intent = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage_prepared_upload(&intent, scope, &first_prepared)
            .await
            .unwrap();
        intent.commit().await.unwrap();
        let first_uploaded = uploaded_value(&first_prepared, 10);
        let first_published =
            PublishedActiveManifestSplit::try_new(&handle, &first_uploaded).unwrap();
        let first_authorization =
            stage_active_manifest_split(&graph, &first_attachment, &first_published)
                .await
                .unwrap();
        graph.commit().await.unwrap();

        assert_eq!(
            db.get(&graph_key).await.unwrap().as_deref(),
            Some(graph_value.as_ref())
        );
        let intent_logical =
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: handle.index_id(),
                generation: handle.generation(),
                intent_id: first_prepared.value().intent_id,
            });
        let retained_intent = db
            .get(scoped_key(scope, intent_logical))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            retained_intent,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(first_prepared.value().clone()),
            ))
        );
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&db.get(&root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("root key retains a manifest root");
        };
        assert_eq!(root.revision().get(), 2);
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 1);
        assert_eq!(
            first_authorization.authorization().owner_kind,
            index_keys::BlobReferenceOwnerKind::ManifestPageSplit
        );
        assert_eq!(first_authorization.authorization().owner_slot, 0);
        assert!(first_authorization
            .authorization()
            .proof_logical_key
            .is_some());

        let conflicting_graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(8)),
        }
        .to_bytes();
        let conflicting_graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        conflicting_graph
            .put(
                conflicting_graph_key.clone(),
                encode_properties(&[
                    Property::string("$label", "Document"),
                    Property::string("body", "must conflict with DDL"),
                ]),
            )
            .unwrap();
        let second_split = split(20);
        let second_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(8),
        };
        let second_attachment = prepare_active_manifest_split(
            &conflicting_graph,
            &handle,
            work::TextPartition::Unpartitioned,
            second_split,
            second_entity,
            crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let second_prepared = prepared_upload(&handle, 20, second_split);
        let second_intent = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage_prepared_upload(&second_intent, scope, &second_prepared)
            .await
            .unwrap();
        second_intent.commit().await.unwrap();
        let second_uploaded = uploaded_value(&second_prepared, 20);
        let second_published =
            PublishedActiveManifestSplit::try_new(&handle, &second_uploaded).unwrap();
        stage_active_manifest_split(&conflicting_graph, &second_attachment, &second_published)
            .await
            .unwrap();

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::from_bytes([31; 16]).unwrap(),
            })
            .unwrap();
        db.put(record_key, index_values::encode_index_record(&dropping))
            .await
            .unwrap();
        assert!(conflicting_graph.commit().await.is_err());
        assert!(db.get(&conflicting_graph_key).await.unwrap().is_none());
        let second_proof_key = scoped_key(
            scope,
            index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
                index_id: handle.index_id(),
                generation: handle.generation(),
                intent_id: second_prepared.value().intent_id,
            }),
        );
        assert!(db.get(second_proof_key).await.unwrap().is_none());
        assert!(db
            .get(scoped_key(
                scope,
                index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                    index_id: handle.index_id(),
                    generation: handle.generation(),
                    intent_id: second_prepared.value().intent_id,
                }),
            ))
            .await
            .unwrap()
            .is_some());
        db.close().await.unwrap();
    }
}
