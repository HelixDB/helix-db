//! Request-level preflight and atomic staging for Active text mutations.
//!
//! One graph mutation may feed hidden text builds and several Active text
//! indexes. This module owns the composition boundary: it observes the
//! authoritative graph row once, prepares every coalesced build delta and
//! independent manifest/upload-outbox unit without staging, and admits their
//! aggregate serialized database work. After publication, it revalidates the
//! graph plus every delta and attachment before buffering the one graph write
//! and all index writes. Per-index code therefore cannot duplicate the graph
//! write or undercount a multi-index request.
//!
//! Active append, retirement, and tenant-move effects are derived from the
//! exact validated definitions carried by the complete loaded Active-handle
//! set. Append documents are built only after their manifest-derived logical
//! versions are known, and each payload remains paired with its upload
//! specification. This module changes no persisted key or value format.

#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;

use bytes::Bytes;
#[cfg(test)]
use sha2::{Digest, Sha256};
use slatedb::DbTransaction;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::property::{encode_properties, Property};
use crate::error::{HelixDbError, Result};
use crate::index_v2::{self, work};

use super::active_attachment::{self, PreparedActiveManifestSplit};
use super::active_preflight::ActiveTextMutationMeasurements;
use super::active_retirement::{self, PreparedActiveTextRetirement};
use super::mutation;

/// Closed authoritative graph-row transition for one request entity.
#[derive(Debug, Clone, PartialEq)]
enum ActiveGraphTransition {
    /// The property row must not exist before this request.
    Create { after: Vec<Property> },
    /// The property row must equal `before` and is replaced by `after`.
    Replace {
        before: Vec<Property>,
        after: Vec<Property>,
    },
    /// The property row must equal `before` and is removed without an upload.
    Delete { before: Vec<Property> },
}

impl ActiveGraphTransition {
    /// Returns the authoritative property snapshot expected before mutation.
    fn before(&self) -> &[Property] {
        match self {
            Self::Create { .. } => &[],
            Self::Replace { before, .. } | Self::Delete { before } => before,
        }
    }

    /// Returns the authoritative property snapshot after mutation.
    fn after(&self) -> &[Property] {
        match self {
            Self::Create { after } | Self::Replace { after, .. } => after,
            Self::Delete { .. } => &[],
        }
    }
}

/// Canonically encoded graph mutation supplied to Active text preflight.
///
/// Private fields and distinct constructors exclude an absent `before` value
/// for replacement/deletion and exclude an `after` value for deletion.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ActiveTextGraphMutation {
    scope: DataScope,
    entity: index_keys::IndexEntity,
    transition: ActiveGraphTransition,
}

impl ActiveTextGraphMutation {
    /// Encodes a graph property-row creation through the canonical V1 codec.
    pub(crate) fn create(
        scope: DataScope,
        entity: index_keys::IndexEntity,
        after: &[Property],
    ) -> Self {
        Self {
            scope,
            entity,
            transition: ActiveGraphTransition::Create {
                after: after.to_vec(),
            },
        }
    }

    /// Encodes an exact graph property-row replacement through the V1 codec.
    pub(crate) fn replace(
        scope: DataScope,
        entity: index_keys::IndexEntity,
        before: &[Property],
        after: &[Property],
    ) -> Self {
        Self {
            scope,
            entity,
            transition: ActiveGraphTransition::Replace {
                before: before.to_vec(),
                after: after.to_vec(),
            },
        }
    }

    /// Encodes an exact graph property-row deletion with no replacement value.
    pub(crate) fn delete(
        scope: DataScope,
        entity: index_keys::IndexEntity,
        before: &[Property],
    ) -> Self {
        Self {
            scope,
            entity,
            transition: ActiveGraphTransition::Delete {
                before: before.to_vec(),
            },
        }
    }
}

/// One immutable Active manifest destination known before intent reservation.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveManifestSplitInput {
    handle: index_v2::ActiveIndexHandle,
    partition: work::TextPartition,
    payload: Bytes,
    split: work::SplitRef,
}

/// One indexed document derived from canonical properties and a text definition.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveTextDocument {
    partition: work::TextPartition,
    text: String,
}

/// Complete semantic effect of one Active text index for one graph transition.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ActiveTextEffect {
    /// Neither snapshot is indexed, or indexed content is unchanged.
    None,
    /// The new document adds a split to this partition.
    Append { document: ActiveTextDocument },
    /// The previous document becomes dead without a replacement split.
    Retire { partition: work::TextPartition },
    /// The previous partition becomes dead and the new partition gains a split.
    Move {
        previous: work::TextPartition,
        current: ActiveTextDocument,
    },
}

/// Source effect that requires one immutable Active split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveAppendOrigin {
    Append,
    Move,
}

/// Complete canonical input needed to build one derived append.
struct DerivedActiveTextAppend<'a> {
    handle: &'a index_v2::ActiveIndexHandle,
    definition: &'a index_v2::ValidatedTextIndexDefinition,
    document: ActiveTextDocument,
    origin: ActiveAppendOrigin,
}

#[cfg(test)]
impl ActiveManifestSplitInput {
    /// Binds deterministic content-addressed bytes to one Active generation.
    pub(crate) fn try_new(
        handle: &index_v2::ActiveIndexHandle,
        partition: work::TextPartition,
        split: work::SplitRef,
    ) -> Result<Self> {
        let index_v2::ActiveIndexHandle::Text { .. } = handle else {
            return Err(corruption(
                "Active text request received a non-text manifest destination",
            ));
        };
        let payload = (0_u8..=u8::MAX)
            .map(|seed| Bytes::copy_from_slice(&[seed; 64]))
            .find(|payload| {
                split.blob().size() == u64::try_from(payload.len()).unwrap_or(u64::MAX)
                    && split.blob().hash() == &<[u8; 32]>::from(Sha256::digest(payload))
            })
            .ok_or_else(|| corruption("test Active split has no matching deterministic payload"))?;
        Ok(Self {
            handle: handle.clone(),
            partition,
            payload,
            split,
        })
    }
}

/// Split construction policy fixed before request preparation begins.
enum ActiveSplitMode {
    /// Production construction from canonical graph content and definition.
    Canonical,
    /// Exact synthetic payloads used by storage-focused unit tests.
    #[cfg(test)]
    Supplied(HashMap<(index_v2::IndexId, index_v2::IndexGenerationId), ActiveManifestSplitInput>),
}

/// Graph write retained after exact source observation.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PreparedGraphWrite {
    Put(Bytes),
    Delete,
}

/// Exact graph observation and write shared by all Active destinations.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedActiveGraphMutation {
    key: Bytes,
    observed: Option<Bytes>,
    write: PreparedGraphWrite,
}

/// Fully measured request that may reserve its independent upload intents.
///
/// No constructor is exposed other than [`prepare_active_text_mutation`], so a
/// caller cannot bypass aggregate admission or pair attachments with another
/// graph entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedActiveTextMutation {
    scope: DataScope,
    graph: PreparedActiveGraphMutation,
    build_deltas: mutation::PreparedTextBuildDeltas,
    retirements: Vec<PreparedActiveTextRetirement>,
    attachments: Vec<PreparedActiveManifestSplit>,
    measurements: ActiveTextMutationMeasurements,
}

/// Exact commit-proof set buffered with one authoritative graph mutation.
///
/// Construction is possible only after every request input and publication has
/// revalidated. Outcome resolution consumes these exact staged bytes rather
/// than inferring success from a manifest, split, or object-store observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedActiveTextMutation {
    scope: DataScope,
    proofs: Vec<active_attachment::StagedActiveTextCommitProof>,
}

impl StagedActiveTextMutation {
    /// Returns the number of independently published destinations in the commit.
    pub(crate) const fn len(&self) -> usize {
        self.proofs.len()
    }

    /// Returns whether this graph mutation writes no upload commit proofs.
    pub(crate) const fn is_empty(&self) -> bool {
        self.proofs.is_empty()
    }

    /// Returns the exact database scope shared by every staged proof and upload.
    pub(super) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Borrows exact proof rows in publication order for fresh resolution.
    pub(super) fn proofs(&self) -> &[active_attachment::StagedActiveTextCommitProof] {
        &self.proofs
    }
}

/// One inseparable payload and upload specification owned by a prepared request.
///
/// Private fields prevent orchestration from pairing the bytes built for one
/// manifest destination with another destination's durable upload intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedActiveTextUpload {
    payload: Bytes,
    spec: work::TextUploadSpec,
}

impl PreparedActiveTextUpload {
    /// Transfers the exact bytes and their matching immutable upload specification.
    pub(crate) fn into_parts(self) -> (Bytes, work::TextUploadSpec) {
        (self.payload, self.spec)
    }
}

impl PreparedActiveTextMutation {
    /// Returns the one database scope shared by graph and index work.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Returns whether this request must reserve and publish immutable bytes.
    pub(crate) const fn requires_publication(&self) -> bool {
        !self.attachments.is_empty()
    }

    /// Returns the aggregate exact resource measurements admitted for the request.
    #[cfg(test)]
    pub(super) const fn measurements(&self) -> ActiveTextMutationMeasurements {
        self.measurements
    }

    /// Constructs each exact payload/specification pair that may be reserved next.
    pub(crate) fn uploads(
        &self,
        writer_epoch: index_v2::WriterEpoch,
        mutation_id: index_v2::MutationId,
    ) -> impl ExactSizeIterator<Item = PreparedActiveTextUpload> + '_ {
        self.attachments
            .iter()
            .map(move |attachment| PreparedActiveTextUpload {
                payload: attachment.payload().clone(),
                spec: work::TextUploadSpec::try_new(
                    attachment.handle().index_id(),
                    attachment.handle().identity().clone(),
                    attachment.handle().generation(),
                    attachment.partition().clone(),
                    attachment.split().blob(),
                    work::TextUploadOwner::ActiveMutation {
                        writer_epoch,
                        mutation_id,
                        active_record_revision: attachment.handle().record_revision(),
                    },
                    work::TextUploadAttachment::ManifestSplit(attachment.split()),
                )
                .expect(
                    "an admitted Active attachment always forms its exact upload specification",
                ),
            })
    }
}

/// Prepares one graph transition, hidden-build deltas, and derived Active effects.
///
/// `mutations` must be the complete set returned by the transaction-local text
/// catalog loader. Its private Active-handle field prevents callers from
/// independently inventing retirements: this function projects before/after
/// properties through every canonical definition and derives `None`, `Append`,
/// `Retire`, or `Move`. Graph input is counted once. Hidden-build rows contribute
/// one coalesced write each. Attachment database work includes its five
/// graph-transaction writes plus the independently committed three-row upload
/// outbox. Aggregate input/output limits are checked only after every exact row
/// is known; split and manifest-page ceilings use the maximum individual object
/// because those policies bound one immutable object, not their sum.
pub(crate) async fn prepare_active_text_mutation(
    transaction: &DbTransaction,
    graph: ActiveTextGraphMutation,
    mutations: &mutation::TextMutationSet,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveTextMutation> {
    prepare_active_text_mutation_from(
        transaction,
        graph,
        mutations,
        limits,
        ActiveSplitMode::Canonical,
    )
    .await
}

/// Storage-focused preparation with deterministic injected split payloads.
#[cfg(test)]
async fn prepare_active_text_mutation_with_inputs(
    transaction: &DbTransaction,
    graph: ActiveTextGraphMutation,
    mutations: &mutation::TextMutationSet,
    destinations: Vec<ActiveManifestSplitInput>,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveTextMutation> {
    let mut destinations_by_generation = HashMap::with_capacity(destinations.len());
    for destination in destinations {
        if destinations_by_generation
            .insert(
                (
                    destination.handle.index_id(),
                    destination.handle.generation(),
                ),
                destination,
            )
            .is_some()
        {
            return Err(corruption(
                "Active text request contains a duplicate manifest destination",
            ));
        }
    }
    prepare_active_text_mutation_from(
        transaction,
        graph,
        mutations,
        limits,
        ActiveSplitMode::Supplied(destinations_by_generation),
    )
    .await
}

/// Shared semantic preparation after the split-construction policy is fixed.
async fn prepare_active_text_mutation_from(
    transaction: &DbTransaction,
    graph: ActiveTextGraphMutation,
    mutations: &mutation::TextMutationSet,
    limits: ActiveTextMutationLimits,
    mut split_mode: ActiveSplitMode,
) -> Result<PreparedActiveTextMutation> {
    let graph_kind = match graph.entity.kind {
        index_v2::IndexElementKind::Node => DataKeyKind::NodeProperty(
            crate::encoding::v1::keys::NodePropertyKey::new(graph.entity.id.get()),
        ),
        index_v2::IndexElementKind::Edge => DataKeyKind::EdgePropertyById(
            crate::encoding::v1::keys::EdgePropertyByIdKey::new(graph.entity.id.get()),
        ),
    };
    let graph_key = Key::Data {
        scope: graph.scope,
        kind: graph_kind,
    }
    .to_bytes();
    let graph_observed = transaction.get(&graph_key).await?;
    let (graph_matches, graph_write) = match (&graph.transition, &graph_observed) {
        (ActiveGraphTransition::Create { after }, None) => {
            (true, PreparedGraphWrite::Put(encode_properties(after)))
        }
        (ActiveGraphTransition::Replace { before, after }, Some(actual)) => (
            encode_properties(before) == *actual,
            PreparedGraphWrite::Put(encode_properties(after)),
        ),
        (ActiveGraphTransition::Delete { before }, Some(actual)) => (
            encode_properties(before) == *actual,
            PreparedGraphWrite::Delete,
        ),
        (ActiveGraphTransition::Create { .. }, Some(_))
        | (ActiveGraphTransition::Replace { .. }, None)
        | (ActiveGraphTransition::Delete { .. }, None) => (false, PreparedGraphWrite::Delete),
    };
    if !graph_matches {
        return Err(HelixDbError::InvariantViolation(
            "Active text graph source disagrees with its requested property transition".to_string(),
        ));
    }

    let build_deltas = mutation::prepare_text_build_deltas(
        transaction,
        graph.scope,
        mutations,
        mutation::TextEntityMutation::new(
            graph.entity.kind,
            graph.entity.id.get(),
            graph.transition.before(),
            graph.transition.after(),
        ),
    )
    .await?;

    let mut active_text_handles = mutations
        .active_handles()
        .iter()
        .filter(|handle| handle.text_definition().is_some())
        .collect::<Vec<_>>();
    active_text_handles.sort_by_key(|handle| handle.index_id());
    let mut active_identities = HashSet::with_capacity(active_text_handles.len());
    let mut retirements = Vec::new();
    let mut attachments = Vec::with_capacity(active_text_handles.len());
    for handle in active_text_handles {
        if handle.scope() != graph.scope {
            return Err(corruption(
                "Active text generation scope disagrees with its graph mutation",
            ));
        }
        if !active_identities.insert((handle.index_id(), handle.generation())) {
            return Err(corruption(
                "Active text request contains a duplicate canonical generation",
            ));
        }
        let definition = handle
            .text_definition()
            .expect("the filtered Active handle is text-typed");
        if definition.identity() != *handle.identity() {
            return Err(corruption(
                "Active text handle definition disagrees with its canonical identity",
            ));
        }
        if definition.element_kind() != graph.entity.kind {
            continue;
        }

        let before = active_document(definition, graph.transition.before())?;
        let after = active_document(definition, graph.transition.after())?;
        let effect = derive_active_text_effect(before, after);

        match effect {
            ActiveTextEffect::None => {}
            ActiveTextEffect::Retire { partition } => {
                retirements.push(
                    active_retirement::prepare_active_text_retirement(
                        transaction,
                        handle,
                        partition,
                        graph.entity,
                        limits,
                    )
                    .await?,
                );
            }
            ActiveTextEffect::Append { document } => {
                attachments.push(
                    prepare_derived_attachment(
                        transaction,
                        DerivedActiveTextAppend {
                            handle,
                            definition,
                            document,
                            origin: ActiveAppendOrigin::Append,
                        },
                        graph.entity,
                        limits,
                        &mut split_mode,
                    )
                    .await?,
                );
            }
            ActiveTextEffect::Move { previous, current } => {
                let attachment = prepare_derived_attachment(
                    transaction,
                    DerivedActiveTextAppend {
                        handle,
                        definition,
                        document: current,
                        origin: ActiveAppendOrigin::Move,
                    },
                    graph.entity,
                    limits,
                    &mut split_mode,
                )
                .await?;
                let retirement = active_retirement::prepare_active_text_retirement(
                    transaction,
                    handle,
                    previous,
                    graph.entity,
                    limits,
                )
                .await?;
                retirements.push(retirement);
                attachments.push(attachment);
            }
        }
    }
    #[cfg(test)]
    if matches!(&split_mode, ActiveSplitMode::Supplied(destinations) if !destinations.is_empty()) {
        return Err(corruption(
            "Active text request contains an unexpected manifest destination",
        ));
    }

    let graph_row_bytes = u64::try_from(graph_key.len())
        .unwrap_or(u64::MAX)
        .saturating_add(
            graph_observed
                .as_ref()
                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
        );
    let graph_output_bytes = u64::try_from(graph_key.len())
        .unwrap_or(u64::MAX)
        .saturating_add(match &graph_write {
            PreparedGraphWrite::Put(value) => u64::try_from(value.len()).unwrap_or(u64::MAX),
            PreparedGraphWrite::Delete => 0,
        });
    let build_measurements = build_deltas.measurements();
    let measured_before_attachments = retirements.iter().fold(
        (
            graph_row_bytes
                .saturating_mul(2)
                .saturating_add(build_measurements.input_bytes()),
            1_u64.saturating_add(build_measurements.output_operations()),
            graph_output_bytes.saturating_add(build_measurements.output_bytes()),
            0_u64,
            0_u64,
        ),
        |(input, operations, output, split, page), retirement| {
            let measured = retirement.measurements();
            (
                input.saturating_add(measured.input_bytes()),
                operations.saturating_add(measured.output_operations()),
                output.saturating_add(measured.output_bytes()),
                split.max(measured.split_bytes()),
                page.max(measured.manifest_page_bytes()),
            )
        },
    );
    let (input_bytes, output_operations, output_bytes, split_bytes, manifest_page_bytes) =
        attachments.iter().fold(
            measured_before_attachments,
            |(input, operations, output, split, page), attachment| {
                let measured = attachment.measurements();
                (
                    input.saturating_add(measured.input_bytes()),
                    operations.saturating_add(measured.output_operations()),
                    output.saturating_add(measured.output_bytes()),
                    split.max(measured.split_bytes()),
                    page.max(measured.manifest_page_bytes()),
                )
            },
        );
    let measurements = ActiveTextMutationMeasurements::try_admit(
        limits,
        input_bytes,
        output_operations,
        output_bytes,
        split_bytes,
        manifest_page_bytes,
    )?;

    Ok(PreparedActiveTextMutation {
        scope: graph.scope,
        graph: PreparedActiveGraphMutation {
            key: graph_key,
            observed: graph_observed,
            write: graph_write,
        },
        build_deltas,
        retirements,
        attachments,
        measurements,
    })
}

/// Projects one canonical property snapshot into a definition-scoped document.
fn active_document(
    definition: &index_v2::ValidatedTextIndexDefinition,
    properties: &[Property],
) -> Result<Option<ActiveTextDocument>> {
    let label_matches = properties.iter().any(|property| {
        property.name == "$label" && property.value.as_str() == Some(definition.label().as_str())
    });
    if !label_matches {
        return Ok(None);
    }
    let Some(indexed_property) = properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
    else {
        return Ok(None);
    };
    let Some(text) = crate::search::text::normalize_indexed_text_value(&indexed_property.value)?
    else {
        return Ok(None);
    };
    let partition = match definition.tenant_property() {
        None => work::TextPartition::Unpartitioned,
        Some(tenant_property) => {
            let Some(tenant_value) = properties
                .iter()
                .find(|property| property.name == tenant_property.as_str())
                .and_then(|property| crate::search::text::normalize_tenant_value(&property.value))
            else {
                return Ok(None);
            };
            work::TextPartition::try_tenant_value(
                crate::encoding::v1::property::encode_index_partition_value(tenant_value),
            )
            .map_err(|error| {
                HelixDbError::Query(format!(
                    "text index {}:{} has an invalid tenant partition: {error}",
                    definition.label().as_str(),
                    definition.property().as_str(),
                ))
            })?
        }
    };
    Ok(Some(ActiveTextDocument { partition, text }))
}

/// Reduces two projected snapshots to the only legal Active text effects.
fn derive_active_text_effect(
    before: Option<ActiveTextDocument>,
    after: Option<ActiveTextDocument>,
) -> ActiveTextEffect {
    match (before, after) {
        (None, None) => ActiveTextEffect::None,
        (None, Some(document)) => ActiveTextEffect::Append { document },
        (Some(previous), None) => ActiveTextEffect::Retire {
            partition: previous.partition,
        },
        (Some(previous), Some(current)) if previous == current => ActiveTextEffect::None,
        (Some(previous), Some(current)) if previous.partition == current.partition => {
            ActiveTextEffect::Append { document: current }
        }
        (Some(previous), Some(current)) => ActiveTextEffect::Move {
            previous: previous.partition,
            current,
        },
    }
}

/// Builds one definition-derived append under the fixed construction policy.
async fn prepare_derived_attachment(
    transaction: &DbTransaction,
    append: DerivedActiveTextAppend<'_>,
    entity: index_keys::IndexEntity,
    limits: ActiveTextMutationLimits,
    split_mode: &mut ActiveSplitMode,
) -> Result<PreparedActiveManifestSplit> {
    let DerivedActiveTextAppend {
        handle,
        definition,
        document,
        origin: _origin,
    } = append;
    match split_mode {
        ActiveSplitMode::Canonical => {
            active_attachment::prepare_active_manifest_document(
                transaction,
                handle,
                definition,
                document.partition,
                &document.text,
                entity,
                limits,
            )
            .await
        }
        #[cfg(test)]
        ActiveSplitMode::Supplied(destinations) => {
            let Some(destination) = destinations.remove(&(handle.index_id(), handle.generation()))
            else {
                let reason = match _origin {
                    ActiveAppendOrigin::Append => {
                        "Active text append is missing its derived manifest destination"
                    }
                    ActiveAppendOrigin::Move => {
                        "Active text move is missing its derived manifest destination"
                    }
                };
                return Err(corruption(reason));
            };
            if destination.handle != *handle || destination.partition != document.partition {
                return Err(corruption(
                    "Active text manifest destination disagrees with its derived index partition",
                ));
            }
            active_attachment::prepare_active_manifest_test_payload(
                transaction,
                handle,
                document.partition,
                destination.payload,
                destination.split,
                entity,
                limits,
            )
            .await
        }
    }
}

/// Revalidates all request inputs and stages graph, build deltas, and attachments.
///
/// Every upload must be definitively `Uploaded` and appear in the same order as
/// [`PreparedActiveTextMutation::uploads`]. All fallible reads and shape
/// validation complete before the first buffered put/delete.
pub(crate) async fn stage_active_text_mutation(
    transaction: &DbTransaction,
    prepared: &PreparedActiveTextMutation,
    uploaded: &[work::TextUploadIntentValue],
) -> Result<StagedActiveTextMutation> {
    if uploaded.len() != prepared.attachments.len() {
        return Err(corruption(
            "Active text upload results disagree with the admitted destination count",
        ));
    }
    if transaction.get(&prepared.graph.key).await? != prepared.graph.observed {
        return Err(corruption(
            "Active text graph input changed after serialized preflight",
        ));
    }
    let validated_build_deltas =
        mutation::validate_text_build_deltas(transaction, &prepared.build_deltas).await?;
    let mut validated_retirements = Vec::with_capacity(prepared.retirements.len());
    for retirement in &prepared.retirements {
        validated_retirements.push(
            active_retirement::validate_active_text_retirement(transaction, retirement).await?,
        );
    }

    let mut validated = Vec::with_capacity(prepared.attachments.len());
    for (attachment, uploaded) in prepared.attachments.iter().zip(uploaded) {
        let published = active_attachment::PublishedActiveManifestSplit::try_new(
            attachment.handle(),
            uploaded,
        )?;
        validated.push(
            active_attachment::validate_active_manifest_split(transaction, attachment, &published)
                .await?,
        );
    }

    match &prepared.graph.write {
        PreparedGraphWrite::Put(value) => transaction
            .put(&prepared.graph.key, value)
            .expect("SlateDB transactional put only buffers a validated key/value pair"),
        PreparedGraphWrite::Delete => transaction
            .delete(&prepared.graph.key)
            .expect("SlateDB transactional delete only buffers a validated key"),
    }
    mutation::stage_validated_text_build_deltas(transaction, validated_build_deltas)?;
    for retirement in validated_retirements {
        active_retirement::stage_validated_active_text_retirement(transaction, retirement)?;
    }
    Ok(StagedActiveTextMutation {
        scope: prepared.scope,
        proofs: validated
            .into_iter()
            .map(|attachment| {
                active_attachment::stage_validated_active_manifest_split(transaction, attachment)
            })
            .collect(),
    })
}

/// Constructs the stable corruption category for request-shape disagreement.
fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::{memory::InMemory, ObjectStore};
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SearchIndexBatchLimits, SecondaryIndexDefinition,
        TextAnalyzerKind, TextBackfillCompactionLimits, TextBuildArtifactLimits,
        TextIndexDefinition,
    };
    use crate::encoding::property::property_value::PropertyValue;
    use crate::encoding::v1::keys::tenant::TenantId;
    use crate::encoding::v1::keys::{EdgePropertyByIdKey, GlobalKeyKind, NodePropertyKey};
    use crate::encoding::v1::values::index_v2 as index_values;
    use crate::index_v2::blob_publication::{
        BlobPublicationCoordinator, BlobPublicationPermit, BlobPublicationStatus,
        BlobPublicationTiming, ProcessLocalBlobPublicationCoordinator,
    };
    use crate::index_v2::text::upload::{stage_prepared_upload, PreparedTextUploadIntent};
    use crate::index_v2::{
        BlobPublicationPermitId, ClaimSequence, IndexElementKind, IndexEntityId, IndexGenerationId,
        IndexId, IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition,
        OperationClaim, PhysicalGeneration, TextManifestRevision, TextUploadIntentId,
        ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
    };

    /// One canonical Active text generation and its empty manifest root.
    struct ActiveFixture {
        handle: index_v2::ActiveIndexHandle,
        root_typed: index_keys::TextManifestRootKey,
        root_key: Bytes,
    }

    /// Opens one isolated database for request-level mutation contracts.
    async fn raw_db(name: &str) -> Db {
        let db = Db::open(name, Arc::new(InMemory::new())).await.unwrap();
        index_v2::repository::bootstrap_writer(&db).await.unwrap();
        db
    }

    /// Encodes one scoped V2 key through the canonical key boundary.
    fn scoped_key(scope: DataScope, logical: index_keys::IndexV2Key) -> Bytes {
        Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(logical),
        }
        .to_bytes()
    }

    /// Installs one Active text record and its canonical empty root.
    async fn seed_active(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        property: &str,
        operation_seed: u8,
        kind: IndexElementKind,
    ) -> ActiveFixture {
        let runtime = match kind {
            IndexElementKind::Node => TextIndexDefinition::new_node("Document", property),
            IndexElementKind::Edge => TextIndexDefinition::new_edge("Document", property),
        }
        .unwrap();
        seed_active_definition(
            db,
            scope,
            index_id,
            ValidatedDynamicIndexDefinition::try_from(runtime).unwrap(),
            operation_seed,
            work::TextPartition::Unpartitioned,
        )
        .await
    }

    /// Installs one partitioned Active text record and its initial partition root.
    async fn seed_partitioned_active(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        operation_seed: u8,
        partition: work::TextPartition,
    ) -> ActiveFixture {
        let definition = ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "body",
            Some("tenant"),
            TextAnalyzerKind::Standard,
            false,
        )
        .unwrap();
        seed_active_definition(
            db,
            scope,
            index_id,
            ValidatedDynamicIndexDefinition::Text(definition),
            operation_seed,
            partition,
        )
        .await
    }

    /// Installs one exact Active text definition and canonical initial root.
    async fn seed_active_definition(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        definition: ValidatedDynamicIndexDefinition,
        operation_seed: u8,
        partition: work::TextPartition,
    ) -> ActiveFixture {
        let building = IndexRecordV2::building(
            index_id,
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([operation_seed; 16]).unwrap(),
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
            partition: partition.fingerprint(),
        };
        let root_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestRoot(root_typed));
        db.put(
            root_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::empty(
                    handle.index_id(),
                    handle.generation(),
                    partition,
                ),
            )),
        )
        .await
        .unwrap();
        ActiveFixture {
            handle,
            root_typed,
            root_key,
        }
    }

    /// Seeds one previously published live document in an Active partition.
    async fn seed_live_entity(
        db: &Db,
        fixture: &ActiveFixture,
        partition: work::TextPartition,
        entity: index_keys::IndexEntity,
        split_seed: u8,
    ) -> (Bytes, Bytes) {
        let root_typed = index_keys::TextManifestRootKey {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: partition.fingerprint(),
        };
        let root_key = scoped_key(
            fixture.handle.scope(),
            index_keys::IndexV2Key::TextManifestRoot(root_typed),
        );
        db.put(
            root_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    fixture.handle.index_id(),
                    fixture.handle.generation(),
                    partition.clone(),
                    TextManifestRevision::new(2).unwrap(),
                    1,
                    1,
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
        db.put(
            scoped_key(
                fixture.handle.scope(),
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root: root_typed,
                    page: 0,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                work::TextManifestPageValue::try_new(
                    fixture.handle.index_id(),
                    fixture.handle.generation(),
                    partition.clone(),
                    0,
                    vec![split(split_seed)],
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
        let state_key = scoped_key(
            fixture.handle.scope(),
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                root: root_typed,
                entity,
            }),
        );
        db.put(
            state_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                work::TextEntityStateValue {
                    index_id: fixture.handle.index_id(),
                    generation: fixture.handle.generation(),
                    partition,
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    logical_version: index_v2::TextLogicalVersion::new(2).unwrap(),
                    live: true,
                },
            )),
        )
        .await
        .unwrap();
        (root_key, state_key)
    }

    /// Builds one deterministic single-document split reference.
    fn split(seed: u8) -> work::SplitRef {
        let payload = [seed; 64];
        let blob = work::BlobRef::new(Sha256::digest(payload).into(), payload.len() as u64);
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap()
    }

    /// Builds the complete canonical graph properties used by request tests.
    fn properties(body: &str) -> Vec<Property> {
        vec![
            Property::string("$label", "Document"),
            Property::string("body", body),
            Property::string("title", format!("title for {body}")),
        ]
    }

    /// Builds a partitioned graph document for Active move tests.
    fn partitioned_properties(body: &str, tenant: &str) -> Vec<Property> {
        properties(body)
            .into_iter()
            .chain(std::iter::once(Property::string("tenant", tenant)))
            .collect()
    }

    /// Encodes one tenant partition through the canonical V1 property identity.
    fn tenant_partition(tenant: &str) -> work::TextPartition {
        work::TextPartition::try_tenant_value(
            crate::encoding::v1::property::encode_index_partition_value(&PropertyValue::String(
                tenant.to_string(),
            )),
        )
        .unwrap()
    }

    /// Creates real fixed-width intent values for one admitted request.
    fn uploaded_values(
        prepared: &PreparedActiveTextMutation,
        writer_epoch: index_v2::WriterEpoch,
        mutation_id: index_v2::MutationId,
    ) -> (
        Vec<PreparedTextUploadIntent>,
        Vec<work::TextUploadIntentValue>,
    ) {
        let pairs = prepared
            .uploads(writer_epoch, mutation_id)
            .enumerate()
            .map(|(index, upload)| {
                let seed = u8::try_from(index).unwrap() + 0x40;
                let (_payload, spec) = upload.into_parts();
                let intent = PreparedTextUploadIntent::from_spec(
                    TextUploadIntentId::from_bytes([seed; 16]).unwrap(),
                    BlobPublicationPermit::from_id(
                        BlobPublicationPermitId::from_bytes([seed + 0x10; 16]).unwrap(),
                    ),
                    spec,
                );
                let uploaded = intent
                    .value()
                    .claim(OperationClaim {
                        writer_epoch,
                        sequence: ClaimSequence::new(u64::from(seed)).unwrap(),
                    })
                    .unwrap()
                    .publication_succeeded()
                    .unwrap();
                (intent, uploaded)
            })
            .collect::<Vec<_>>();
        pairs.into_iter().unzip()
    }

    /// Constructs independent positive ceilings for aggregate boundary tests.
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

    /// Constructs one hidden text generation affected by Document body changes.
    fn build_mutations(index_id: IndexId) -> mutation::TextMutationSet {
        mutation::TextMutationSet::one_build_target(
            index_id,
            IndexGenerationId::initial(),
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        )
    }

    /// Prepares a request with no hidden-build generations for focused Active tests.
    async fn prepare_active_text_mutation(
        transaction: &DbTransaction,
        graph: ActiveTextGraphMutation,
        destinations: Vec<ActiveManifestSplitInput>,
        limits: ActiveTextMutationLimits,
    ) -> Result<PreparedActiveTextMutation> {
        let active_generations = destinations
            .iter()
            .map(|destination| destination.handle.clone())
            .collect::<Vec<_>>();
        let mutations = mutation::TextMutationSet::empty().with_active_handles(active_generations);
        super::prepare_active_text_mutation_with_inputs(
            transaction,
            graph,
            &mutations,
            destinations,
            limits,
        )
        .await
    }

    #[test]
    fn canonical_definition_derives_only_none_append_retire_or_move() {
        let definition = ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "body",
            Some("tenant"),
            TextAnalyzerKind::Standard,
            false,
        )
        .unwrap();
        let acme = partitioned_properties("before", "acme");
        let acme_changed = partitioned_properties("after", "acme");
        let globex = partitioned_properties("after", "globex");
        let before = active_document(&definition, &acme).unwrap();
        let unchanged = active_document(&definition, &acme).unwrap();
        let changed = active_document(&definition, &acme_changed).unwrap();
        let moved = active_document(&definition, &globex).unwrap();

        assert_eq!(
            derive_active_text_effect(None, None),
            ActiveTextEffect::None
        );
        assert_eq!(
            derive_active_text_effect(None, before.clone()),
            ActiveTextEffect::Append {
                document: ActiveTextDocument {
                    partition: tenant_partition("acme"),
                    text: "before".to_string(),
                }
            }
        );
        assert_eq!(
            derive_active_text_effect(before.clone(), None),
            ActiveTextEffect::Retire {
                partition: tenant_partition("acme")
            }
        );
        assert_eq!(
            derive_active_text_effect(before.clone(), unchanged),
            ActiveTextEffect::None
        );
        assert_eq!(
            derive_active_text_effect(before.clone(), changed),
            ActiveTextEffect::Append {
                document: ActiveTextDocument {
                    partition: tenant_partition("acme"),
                    text: "after".to_string(),
                }
            }
        );
        assert_eq!(
            derive_active_text_effect(before, moved),
            ActiveTextEffect::Move {
                previous: tenant_partition("acme"),
                current: ActiveTextDocument {
                    partition: tenant_partition("globex"),
                    text: "after".to_string(),
                }
            }
        );

        assert_eq!(
            active_document(
                &definition,
                &[
                    Property::string("$label", "Other"),
                    Property::string("body", "ignored"),
                    Property::string("tenant", "acme"),
                ],
            )
            .unwrap(),
            None
        );
        assert_eq!(
            active_document(
                &definition,
                &[
                    Property::string("$label", "Document"),
                    Property::string("tenant", "acme"),
                ],
            )
            .unwrap(),
            None
        );
        assert_eq!(
            active_document(
                &definition,
                &[
                    Property::string("$label", "Document"),
                    Property::string("body", "missing partition"),
                ],
            )
            .unwrap(),
            None
        );
        assert!(matches!(
            active_document(
                &definition,
                &[
                    Property::string("$label", "Document"),
                    Property::new("body", PropertyValue::I64(7)),
                    Property::string("tenant", "acme"),
                ],
            ),
            Err(HelixDbError::Query(reason))
                if reason == "text indexes only support String and StringArray values"
        ));
        let oversized_tenant = vec![
            Property::string("$label", "Document"),
            Property::string("body", "oversized tenant"),
            Property::new("tenant", PropertyValue::Bytes(vec![0; 16 * 1024 * 1024])),
        ];
        assert!(matches!(
            active_document(&definition, &oversized_tenant),
            Err(HelixDbError::Query(reason))
                if reason.contains("has an invalid tenant partition")
        ));
    }

    #[tokio::test]
    async fn canonical_preparation_builds_and_retains_the_versioned_split_payload() {
        let db = raw_db("active-text-request-canonical-split").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x13,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(13),
        };
        let after = properties("request owned payload");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &transaction,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();

        assert_eq!(prepared.attachments.len(), 1);
        assert_eq!(prepared.attachments[0].logical_version().get(), 2);
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x31; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x32; 16]).unwrap();
        let mut uploads = prepared.uploads(writer_epoch, mutation_id);
        let (payload, spec) = uploads.next().unwrap().into_parts();
        assert!(uploads.next().is_none());
        assert_eq!(u64::try_from(payload.len()).unwrap(), spec.blob().size());
        assert_eq!(
            &<[u8; 32]>::from(Sha256::digest(&payload)),
            spec.blob().hash()
        );
        assert_eq!(prepared.attachments[0].split().blob(), spec.blob());

        let (intents, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        let intent_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage_prepared_upload(&intent_transaction, scope, &intents[0])
            .await
            .unwrap();
        intent_transaction.commit().await.unwrap();
        stage_active_text_mutation(&transaction, &prepared, &uploaded)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let state_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                root: fixture.root_typed,
                entity,
            }),
        );
        let index_values::IndexV2WorkValue::TextEntityState(state) =
            index_values::decode_work_value(&db.get(state_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("canonical split preparation commits a typed entity state");
        };
        assert_eq!(state.logical_version.get(), 2);
        assert!(state.live);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn request_publication_commits_outbox_before_io_and_holds_gc_gate_through_graph_commit() {
        let db = raw_db("active-text-request-publication").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x14,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(14),
        };
        let after = properties("intent before object io");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let object_store = Arc::new(InMemory::new());
        let coordinator: Arc<dyn BlobPublicationCoordinator> =
            Arc::new(ProcessLocalBlobPublicationCoordinator::new(
                object_store,
                "active-text-request-publication-blobs",
                BlobPublicationTiming::default(),
            ));
        let gc_gate = crate::search::text::BlobGcGate::new();
        let registry = crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x41; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x42; 16]).unwrap();

        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &db,
            Arc::clone(&coordinator),
            &gc_gate,
            &registry,
            writer_epoch,
            mutation_id,
            &prepared,
        )
        .await
        .unwrap();
        assert_eq!(publication.uploaded().len(), 1);
        let uploaded = &publication.uploaded()[0];
        assert!(matches!(uploaded.phase, work::TextUploadPhase::Uploaded));
        assert_eq!(uploaded.revision.get(), 2);
        assert_eq!(
            registry.observe(scope, uploaded).unwrap(),
            crate::index_v2::text::active_mutation::ActiveTextMutationOwnerObservation::InFlight
        );
        assert!(matches!(
            coordinator
                .publication_status(&BlobPublicationPermit::from_id(
                    uploaded.publication_permit_id,
                ))
                .await
                .unwrap(),
            BlobPublicationStatus::Succeeded(metadata) if metadata.blob() == uploaded.blob
        ));
        assert_eq!(
            index_v2::repository::load_upload_from_pointer(&db, uploaded.intent_id)
                .await
                .unwrap()
                .unwrap(),
            *uploaded
        );

        let waiting_gate = gc_gate.clone();
        let mut deletion = tokio::spawn(async move { waiting_gate.acquire_deletion().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err()
        );
        let staged = stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        graph.commit().await.unwrap();
        let intent = uploaded.clone();
        let resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_graph_outcome(
                &db,
                publication,
                staged,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Ambiguous(
                    HelixDbError::InvariantViolation(
                        "injected post-commit transport ambiguity".to_string(),
                    ),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            resolution,
            crate::index_v2::text::active_resolution::ActiveTextGraphResolution::Committed(
                crate::index_v2::text::active_resolution::ActiveTextFinalization::Complete
            )
        ));
        assert!(matches!(
            registry.observe(scope, &intent),
            Err(
                crate::index_v2::text::active_mutation::ActiveTextMutationRegistryError::MissingOwner
            )
        ));
        assert_eq!(
            index_v2::repository::load_upload_from_pointer(&db, intent.intent_id)
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            coordinator
                .publication_status(&BlobPublicationPermit::from_id(
                    intent.publication_permit_id,
                ))
                .await,
            Err(crate::index_v2::blob_publication::BlobPublicationError::UnknownPermit)
        ));
        deletion.await.unwrap();

        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encode_properties(&after).as_ref())
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn transaction_proof_union_classifies_upload_free_graph_rows() {
        let db = raw_db("active-text-transaction-proof-union").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x62,
            IndexElementKind::Node,
        )
        .await;
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let indexed_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(620),
        };
        let ignored_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(621),
        };
        let indexed = properties("proof-bearing entity");
        let ignored = vec![
            Property::string("$label", "Other"),
            Property::string("body", "upload-free entity"),
        ];
        let graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let indexed_prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, indexed_entity, &indexed),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let ignored_prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, ignored_entity, &ignored),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let coordinator: Arc<dyn BlobPublicationCoordinator> =
            Arc::new(ProcessLocalBlobPublicationCoordinator::new(
                Arc::new(InMemory::new()),
                "active-text-transaction-proof-union-blobs",
                BlobPublicationTiming::default(),
            ));
        let registry = crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let gc_gate = crate::search::text::BlobGcGate::new();
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x63; 16]).unwrap();
        let indexed_publication =
            crate::index_v2::text::active_publication::publish_active_text_mutation(
                &db,
                Arc::clone(&coordinator),
                &gc_gate,
                &registry,
                writer_epoch,
                index_v2::MutationId::from_bytes([0x64; 16]).unwrap(),
                &indexed_prepared,
            )
            .await
            .unwrap();
        let ignored_publication =
            crate::index_v2::text::active_publication::publish_active_text_mutation(
                &db,
                coordinator,
                &gc_gate,
                &registry,
                writer_epoch,
                index_v2::MutationId::from_bytes([0x65; 16]).unwrap(),
                &ignored_prepared,
            )
            .await
            .unwrap();
        assert_eq!(indexed_publication.uploaded().len(), 1);
        assert!(ignored_publication.uploaded().is_empty());

        let indexed_staged =
            stage_active_text_mutation(&graph, &indexed_prepared, indexed_publication.uploaded())
                .await
                .unwrap();
        let ignored_staged =
            stage_active_text_mutation(&graph, &ignored_prepared, ignored_publication.uploaded())
                .await
                .unwrap();
        let mut outbox =
            crate::index_v2::text::active_resolution::ActiveTextTransactionOutbox::default();
        outbox.retain(indexed_publication, indexed_staged).unwrap();
        outbox.retain(ignored_publication, ignored_staged).unwrap();
        graph.commit().await.unwrap();

        let resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_transaction_outbox(
                &db,
                outbox,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Ambiguous(
                    HelixDbError::TransactionConflict(
                        "injected response loss after transaction commit".to_string(),
                    ),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            resolution,
            crate::index_v2::text::active_resolution::ActiveTextGraphResolution::Committed(
                crate::index_v2::text::active_resolution::ActiveTextFinalization::Complete
            )
        ));
        for (entity, expected) in [(indexed_entity, indexed), (ignored_entity, ignored)] {
            let key = Key::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
            }
            .to_bytes();
            assert_eq!(
                db.get(key).await.unwrap().as_deref(),
                Some(encode_properties(&expected).as_ref())
            );
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn committed_graph_returns_success_when_reference_release_is_deferred() {
        let db = raw_db("active-text-request-release-deferred").await;
        let scope = DataScope::Tenant(TenantId::from_u128(0x15));
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x16,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(16),
        };
        let after = properties("release may finish after graph success");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let concrete = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "active-text-request-release-deferred-blobs",
            BlobPublicationTiming::default(),
        ));
        let coordinator: Arc<dyn BlobPublicationCoordinator> = concrete.clone();
        let registry = crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x47; 16]).unwrap();
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &db,
            coordinator,
            &crate::search::text::BlobGcGate::new(),
            &registry,
            writer_epoch,
            index_v2::MutationId::from_bytes([0x48; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        let uploaded = publication.uploaded()[0].clone();
        let staged = stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        graph.commit().await.unwrap();
        concrete.fail_next_release();

        let resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_graph_outcome(
                &db,
                publication,
                staged,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Committed,
            )
            .await
            .unwrap();
        assert!(matches!(
            resolution,
            crate::index_v2::text::active_resolution::ActiveTextGraphResolution::Committed(
                crate::index_v2::text::active_resolution::ActiveTextFinalization::Deferred
            )
        ));
        let retained = index_v2::repository::load_upload_from_pointer(&db, uploaded.intent_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            retained.phase,
            work::TextUploadPhase::ReferenceCommitted(_)
        ));
        assert_eq!(
            registry.observe(scope, &retained).unwrap(),
            crate::index_v2::text::active_mutation::ActiveTextMutationOwnerObservation::Terminal
        );
        assert!(matches!(
            concrete
                .publication_status(&BlobPublicationPermit::from_id(
                    retained.publication_permit_id,
                ))
                .await
                .unwrap(),
            BlobPublicationStatus::Succeeded(metadata) if metadata.blob() == retained.blob
        ));
        let work::TextUploadPhase::ReferenceCommitted(authorization) = &retained.phase else {
            unreachable!("retained phase was checked above");
        };
        let proof_logical_key = authorization
            .proof_logical_key
            .as_ref()
            .expect("Active reference authorization retains its proof key");
        let proof_key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(
                index_keys::IndexV2Key::parse_from_slice(proof_logical_key).unwrap(),
            ),
        }
        .to_bytes();
        assert!(db.get(&proof_key).await.unwrap().is_some());

        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &db,
            retained.intent_id,
            &registry,
            writer_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("terminal Active reference must be worker-eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &db,
            &eligible,
            &registry,
            writer_epoch,
            ClaimSequence::new(1).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        let driver = crate::index_v2::text::reconciliation::CoordinatorTextUploadDriver::new(
            concrete.clone(),
        );
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ReferenceReleased
        );
        assert_eq!(
            index_v2::repository::load_upload_from_pointer(&db, retained.intent_id)
                .await
                .unwrap(),
            None
        );
        assert!(db.get(proof_key).await.unwrap().is_none());
        assert!(registry.forget_terminal_after_absence(retained.intent_id, writer_epoch));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_worker_recovers_uploaded_active_intent_from_exact_proof_location() {
        let db = raw_db("active-text-request-proof-worker-recovery").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x17,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(17),
        };
        let after = properties("recover exact proof page and slot");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let concrete = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "active-text-request-proof-worker-recovery-blobs",
            BlobPublicationTiming::default(),
        ));
        let coordinator: Arc<dyn BlobPublicationCoordinator> = concrete.clone();
        let registry = crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x49; 16]).unwrap();
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &db,
            coordinator,
            &crate::search::text::BlobGcGate::new(),
            &registry,
            writer_epoch,
            index_v2::MutationId::from_bytes([0x4a; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        let uploaded = publication.uploaded()[0].clone();
        stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        graph.commit().await.unwrap();
        drop(publication);
        assert_eq!(
            registry.observe(scope, &uploaded).unwrap(),
            crate::index_v2::text::active_mutation::ActiveTextMutationOwnerObservation::Terminal
        );

        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &db,
            uploaded.intent_id,
            &registry,
            writer_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("terminal proof-bearing upload must be eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &db,
            &eligible,
            &registry,
            writer_epoch,
            ClaimSequence::new(1).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        let driver = crate::index_v2::text::reconciliation::CoordinatorTextUploadDriver::new(
            concrete.clone(),
        );
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ResolveActiveReference
        );
        let referenced = index_v2::repository::load_upload_from_pointer(&db, uploaded.intent_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            referenced.phase,
            work::TextUploadPhase::ReferenceCommitted(_)
        ));

        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &db,
            uploaded.intent_id,
            &registry,
            writer_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("terminal release outbox must remain eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &db,
            &eligible,
            &registry,
            writer_epoch,
            ClaimSequence::new(2).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ReferenceReleased
        );
        assert_eq!(
            index_v2::repository::load_upload_from_pointer(&db, uploaded.intent_id)
                .await
                .unwrap(),
            None
        );
        assert!(registry.forget_terminal_after_absence(uploaded.intent_id, writer_epoch));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_request_makes_owner_terminal_and_releases_gc_gate() {
        let db = raw_db("active-text-request-publication-drop").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x15,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(15),
        };
        let after = properties("dropped request leaves recovery authority");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let concrete = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "active-text-request-publication-drop-blobs",
            BlobPublicationTiming::default(),
        ));
        let coordinator: Arc<dyn BlobPublicationCoordinator> = concrete.clone();
        let gc_gate = crate::search::text::BlobGcGate::new();
        let registry = crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x43; 16]).unwrap();
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &db,
            coordinator,
            &gc_gate,
            &registry,
            writer_epoch,
            index_v2::MutationId::from_bytes([0x44; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        let uploaded = publication.uploaded()[0].clone();
        assert_eq!(
            registry.observe(scope, &uploaded).unwrap(),
            crate::index_v2::text::active_mutation::ActiveTextMutationOwnerObservation::InFlight
        );
        let waiting_gate = gc_gate.clone();
        let mut deletion = tokio::spawn(async move { waiting_gate.acquire_deletion().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err()
        );

        let staged = stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        drop(graph);
        let resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_graph_outcome(
                &db,
                publication,
                staged,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Ambiguous(
                    HelixDbError::InvariantViolation(
                        "injected graph commit failure before durable commit".to_string(),
                    ),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            resolution,
            crate::index_v2::text::active_resolution::ActiveTextGraphResolution::Aborted { .. }
        ));

        assert_eq!(
            registry.observe(scope, &uploaded).unwrap(),
            crate::index_v2::text::active_mutation::ActiveTextMutationOwnerObservation::Terminal
        );
        tokio::time::timeout(Duration::from_secs(1), deletion)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            index_v2::repository::load_upload_from_pointer(&db, uploaded.intent_id)
                .await
                .unwrap(),
            Some(uploaded.clone())
        );
        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &db,
            uploaded.intent_id,
            &registry,
            writer_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("terminal aborted Active upload must be eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &db,
            &eligible,
            &registry,
            writer_epoch,
            ClaimSequence::new(1).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        let driver =
            crate::index_v2::text::reconciliation::CoordinatorTextUploadDriver::new(concrete);
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ResolveActiveReference
        );
        let reclaimable = index_v2::repository::load_upload_from_pointer(&db, uploaded.intent_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reclaimable.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned)
        ));
        let candidate_key = scoped_key(
            scope,
            index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                index_id: uploaded.index_id,
                generation: uploaded.generation,
                owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(uploaded.intent_id),
                blob_hash: index_keys::BlobHash::new(*uploaded.blob.hash()),
            }),
        );
        assert!(db.get(candidate_key).await.unwrap().is_some());
        let anchor = crate::index_v2::text::upload::upload_anchor_rows(scope, &uploaded).unwrap();
        assert!(db.get(anchor.reachability_key).await.unwrap().is_none());
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        assert!(db.get(graph_key).await.unwrap().is_none());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn newer_writer_reclaims_prior_epoch_uploaded_active_abort() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "active-text-request-prior-writer-abort";
        let old_db = Db::open(database, Arc::clone(&store)).await.unwrap();
        index_v2::repository::bootstrap_writer(&old_db)
            .await
            .unwrap();
        let scope = DataScope::Tenant(TenantId::from_u128(0x24));
        let fixture = seed_active(
            &old_db,
            scope,
            IndexId::initial(),
            "body",
            0x25,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(24),
        };
        let after = properties("new writer reclaims old aborted upload");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = old_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let coordinator = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            Arc::clone(&store),
            database,
            BlobPublicationTiming::default(),
        ));
        let coordinator_dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let old_registry =
            crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let old_epoch = index_v2::WriterEpoch::from_bytes([0x26; 16]).unwrap();
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &old_db,
            coordinator_dependency,
            &crate::search::text::BlobGcGate::new(),
            &old_registry,
            old_epoch,
            index_v2::MutationId::from_bytes([0x27; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        let uploaded = publication.uploaded()[0].clone();
        stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        drop(graph);
        drop(publication);

        let new_db = Db::open(database, Arc::clone(&store)).await.unwrap();
        let new_registry =
            crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let new_epoch = index_v2::WriterEpoch::from_bytes([0x28; 16]).unwrap();
        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &new_db,
            uploaded.intent_id,
            &new_registry,
            new_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("prior-writer Active upload must be eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &new_db,
            &eligible,
            &new_registry,
            new_epoch,
            ClaimSequence::new(1).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        let driver =
            crate::index_v2::text::reconciliation::CoordinatorTextUploadDriver::new(coordinator);
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &new_db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ResolveActiveReference
        );
        let reclaimable =
            index_v2::repository::load_upload_from_pointer(&new_db, uploaded.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            reclaimable.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned)
        ));
        let candidate_key = scoped_key(
            scope,
            index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                index_id: uploaded.index_id,
                generation: uploaded.generation,
                owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(uploaded.intent_id),
                blob_hash: index_keys::BlobHash::new(*uploaded.blob.hash()),
            }),
        );
        assert!(new_db.get(candidate_key).await.unwrap().is_some());
        let anchor = crate::index_v2::text::upload::upload_anchor_rows(scope, &uploaded).unwrap();
        assert!(new_db.get(anchor.reachability_key).await.unwrap().is_none());
        new_db.close().await.unwrap();
        drop(old_db);
    }

    #[tokio::test]
    async fn fenced_old_request_cannot_misreport_cleaned_commit_as_abort() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "active-text-request-fenced-outcome";
        let old_db = Db::open(database, Arc::clone(&store)).await.unwrap();
        index_v2::repository::bootstrap_writer(&old_db)
            .await
            .unwrap();
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &old_db,
            scope,
            IndexId::initial(),
            "body",
            0x29,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(29),
        };
        let after = properties("new writer cleans proof before old request resumes");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = old_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let coordinator = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            Arc::clone(&store),
            database,
            BlobPublicationTiming::default(),
        ));
        let coordinator_dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let old_registry =
            crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let old_epoch = index_v2::WriterEpoch::from_bytes([0x2a; 16]).unwrap();
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &old_db,
            coordinator_dependency,
            &crate::search::text::BlobGcGate::new(),
            &old_registry,
            old_epoch,
            index_v2::MutationId::from_bytes([0x2b; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        let uploaded = publication.uploaded()[0].clone();
        let staged = stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        graph.commit().await.unwrap();

        let new_db = Db::open(database, Arc::clone(&store)).await.unwrap();
        let new_registry =
            crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let new_epoch = index_v2::WriterEpoch::from_bytes([0x2c; 16]).unwrap();
        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &new_db,
            uploaded.intent_id,
            &new_registry,
            new_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("prior-writer proof-bearing upload must be eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &new_db,
            &eligible,
            &new_registry,
            new_epoch,
            ClaimSequence::new(1).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        let driver =
            crate::index_v2::text::reconciliation::CoordinatorTextUploadDriver::new(coordinator);
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &new_db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ResolveActiveReference
        );
        let observation = crate::index_v2::text::upload_queue::observe_upload_pointer(
            &new_db,
            uploaded.intent_id,
            &new_registry,
            new_epoch,
            u64::MAX,
        )
        .await
        .unwrap();
        let crate::index_v2::text::upload_queue::UploadPointerObservation::Eligible(eligible) =
            observation
        else {
            panic!("prior-writer release outbox must be eligible: {observation:?}");
        };
        let claimed = crate::index_v2::text::upload_queue::claim_upload(
            &new_db,
            &eligible,
            &new_registry,
            new_epoch,
            ClaimSequence::new(2).unwrap(),
            u64::MAX,
            crate::index_v2::outbox::ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            crate::index_v2::text::upload_queue::execute_claimed_upload_step(
                &new_db,
                &claimed,
                &driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            crate::index_v2::text::upload_queue::TextUploadStepResult::ReferenceReleased
        );
        assert!(
            index_v2::repository::load_upload_from_pointer(&new_db, uploaded.intent_id)
                .await
                .unwrap()
                .is_none()
        );

        let old_resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_graph_outcome(
                &old_db,
                publication,
                staged,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Ambiguous(
                    HelixDbError::InvariantViolation(
                        "injected old-writer transport ambiguity".to_string(),
                    ),
                ),
            )
            .await;
        assert!(
            matches!(
                old_resolution,
                Ok(
                    crate::index_v2::text::active_resolution::ActiveTextGraphResolution::Committed(
                        _
                    )
                ) | Err(
                    crate::index_v2::text::active_resolution::ActiveTextResolutionError::Database(
                        HelixDbError::WriterFencedCommitOutcomeUnknown
                    )
                )
            ),
            "old writer must not infer abort after proof cleanup: {old_resolution:?}"
        );
        new_db.close().await.unwrap();
        drop(old_db);
    }

    #[tokio::test]
    async fn fenced_old_request_cannot_treat_absent_proof_as_abort() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "active-text-request-fenced-absent-proof";
        let old_db = Db::open(database, Arc::clone(&store)).await.unwrap();
        index_v2::repository::bootstrap_writer(&old_db)
            .await
            .unwrap();
        let scope = DataScope::Tenant(TenantId::from_u128(0x2d));
        let fixture = seed_active(
            &old_db,
            scope,
            IndexId::initial(),
            "body",
            0x2e,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(30),
        };
        let after = properties("fenced proof absence needs a commit barrier");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let graph = old_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation(
            &graph,
            ActiveTextGraphMutation::create(scope, entity, &after),
            &mutations,
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let coordinator: Arc<dyn BlobPublicationCoordinator> =
            Arc::new(ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                database,
                BlobPublicationTiming::default(),
            ));
        let registry = crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &old_db,
            coordinator,
            &crate::search::text::BlobGcGate::new(),
            &registry,
            index_v2::WriterEpoch::from_bytes([0x2f; 16]).unwrap(),
            index_v2::MutationId::from_bytes([0x30; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        let staged = stage_active_text_mutation(&graph, &prepared, publication.uploaded())
            .await
            .unwrap();
        drop(graph);

        let new_db = Db::open(database, Arc::clone(&store)).await.unwrap();
        let fence_transaction = new_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        fence_transaction
            .put(
                Key::Data {
                    scope,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(31)),
                }
                .to_bytes(),
                encode_properties(&properties("new writer fence marker")),
            )
            .unwrap();
        fence_transaction.commit().await.unwrap();
        let old_resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_graph_outcome(
                &old_db,
                publication,
                staged,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Ambiguous(
                    HelixDbError::InvariantViolation(
                        "injected old-writer transport ambiguity".to_string(),
                    ),
                ),
            )
            .await;
        assert!(
            matches!(
                old_resolution,
                Err(
                    crate::index_v2::text::active_resolution::ActiveTextResolutionError::Database(
                        HelixDbError::WriterFencedCommitOutcomeUnknown
                    )
                )
            ),
            "fenced proof absence must remain unknown: {old_resolution:?}"
        );
        new_db.close().await.unwrap();
        drop(old_db);
    }

    #[tokio::test]
    async fn derived_effects_reject_missing_extra_wrong_partition_and_duplicate_inputs() {
        let db = raw_db("active-text-request-derived-effect-shapes").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x14,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(14),
        };
        let after = properties("derived append");
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);

        let missing = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            super::prepare_active_text_mutation_with_inputs(
                &missing,
                ActiveTextGraphMutation::create(scope, entity, &after),
                &mutations,
                vec![],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text append is missing its derived manifest destination"
        ));
        missing.commit().await.unwrap();

        let wrong_partition = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            super::prepare_active_text_mutation_with_inputs(
                &wrong_partition,
                ActiveTextGraphMutation::create(scope, entity, &after),
                &mutations,
                vec![ActiveManifestSplitInput::try_new(
                    &fixture.handle,
                    tenant_partition("wrong"),
                    split(20),
                )
                .unwrap()],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason
                    == "Active text manifest destination disagrees with its derived index partition"
        ));
        wrong_partition.commit().await.unwrap();

        let duplicate = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let duplicate_mutations = mutation::TextMutationSet::empty()
            .with_active_handles(vec![fixture.handle.clone(), fixture.handle.clone()]);
        assert!(matches!(
            super::prepare_active_text_mutation_with_inputs(
                &duplicate,
                ActiveTextGraphMutation::create(scope, entity, &after),
                &duplicate_mutations,
                vec![ActiveManifestSplitInput::try_new(
                    &fixture.handle,
                    work::TextPartition::Unpartitioned,
                    split(21),
                )
                .unwrap()],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text request contains a duplicate canonical generation"
        ));
        duplicate.commit().await.unwrap();

        let old_partition = tenant_partition("acme");
        let partitioned =
            seed_partitioned_active(&db, scope, IndexId::new(2).unwrap(), 0x15, old_partition)
                .await;
        let move_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(15),
        };
        let move_before = partitioned_properties("before move", "acme");
        let move_after = partitioned_properties("after move", "globex");
        let move_graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(move_entity.id.get())),
        }
        .to_bytes();
        db.put(move_graph_key, encode_properties(&move_before))
            .await
            .unwrap();
        let move_mutations = mutation::TextMutationSet::empty()
            .with_active_handles(vec![partitioned.handle.clone()]);
        let missing_move = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            super::prepare_active_text_mutation_with_inputs(
                &missing_move,
                ActiveTextGraphMutation::replace(
                    scope,
                    move_entity,
                    &move_before,
                    &move_after,
                ),
                &move_mutations,
                vec![],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text move is missing its derived manifest destination"
        ));
        missing_move.commit().await.unwrap();

        let before = properties("unchanged text");
        let mut unrelated_after = before.clone();
        unrelated_after.push(Property::new("unrelated", PropertyValue::I64(2)));
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        db.put(graph_key.clone(), encode_properties(&before))
            .await
            .unwrap();
        let extra = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            super::prepare_active_text_mutation_with_inputs(
                &extra,
                ActiveTextGraphMutation::replace(scope, entity, &before, &unrelated_after),
                &mutations,
                vec![ActiveManifestSplitInput::try_new(
                    &fixture.handle,
                    work::TextPartition::Unpartitioned,
                    split(22),
                )
                .unwrap()],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text request contains an unexpected manifest destination"
        ));
        extra.commit().await.unwrap();

        let no_effect = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &no_effect,
            ActiveTextGraphMutation::replace(scope, entity, &before, &unrelated_after),
            &mutations,
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.measurements().output_operations(), 1);
        stage_active_text_mutation(&no_effect, &prepared, &[])
            .await
            .unwrap();
        no_effect.commit().await.unwrap();
        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encode_properties(&unrelated_after).as_ref())
        );

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn multi_index_request_counts_graph_once_and_commits_disjoint_intents() {
        let db = raw_db("active-text-request-multi-index").await;
        let scope = DataScope::LegacyUnscoped;
        let body = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x11,
            IndexElementKind::Node,
        )
        .await;
        let title = seed_active(
            &db,
            scope,
            IndexId::new(2).unwrap(),
            "title",
            0x12,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        };
        let graph_properties = properties("one graph write");
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = prepare_active_text_mutation(
            &transaction,
            ActiveTextGraphMutation::create(scope, entity, &graph_properties),
            vec![
                ActiveManifestSplitInput::try_new(
                    &body.handle,
                    work::TextPartition::Unpartitioned,
                    split(10),
                )
                .unwrap(),
                ActiveManifestSplitInput::try_new(
                    &title.handle,
                    work::TextPartition::Unpartitioned,
                    split(20),
                )
                .unwrap(),
            ],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.measurements().output_operations(), 17);

        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x31; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x32; 16]).unwrap();
        let (intents, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        for intent in &intents {
            let intent_transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            stage_prepared_upload(&intent_transaction, scope, intent)
                .await
                .unwrap();
            intent_transaction.commit().await.unwrap();
        }

        let authorizations = stage_active_text_mutation(&transaction, &prepared, &uploaded)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(authorizations.len(), 2);
        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encode_properties(&graph_properties).as_ref())
        );
        for fixture in [&body, &title] {
            let index_values::IndexV2WorkValue::TextManifestRoot(root) =
                index_values::decode_work_value(&db.get(&fixture.root_key).await.unwrap().unwrap())
                    .unwrap()
            else {
                panic!("manifest root key retains its typed value");
            };
            assert_eq!(root.revision().get(), 2);
            assert_eq!(root.split_count(), 1);
        }
        for intent in intents {
            let pointer_key = Key::Global {
                kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::UploadPointer(
                    intent.value().intent_id,
                )),
            }
            .to_bytes();
            assert!(db.get(pointer_key).await.unwrap().is_some());
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn hidden_build_delta_is_counted_and_staged_with_the_active_request() {
        let db = raw_db("active-text-request-hidden-build-delta").await;
        let scope = DataScope::LegacyUnscoped;
        let active = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x19,
            IndexElementKind::Node,
        )
        .await;
        let build_index = IndexId::new(2).unwrap();
        let mutations =
            build_mutations(build_index).with_active_handles(vec![active.handle.clone()]);
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(15),
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &transaction,
            ActiveTextGraphMutation::create(scope, entity, &properties("hidden build")),
            &mutations,
            vec![ActiveManifestSplitInput::try_new(
                &active.handle,
                work::TextPartition::Unpartitioned,
                split(25),
            )
            .unwrap()],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.measurements().output_operations(), 10);

        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x35; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x36; 16]).unwrap();
        let (_, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        stage_active_text_mutation(&transaction, &prepared, &uploaded)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let delta_key = scoped_key(
            scope,
            index_keys::IndexV2Key::BuildDelta(index_keys::IndexEntityStateKey {
                index_id: build_index,
                generation: IndexGenerationId::initial(),
                entity,
            }),
        );
        let index_values::IndexV2WorkValue::CoalescedBuildDelta(delta) =
            index_values::decode_work_value(&db.get(delta_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("hidden-build delta key retains its typed value");
        };
        assert_eq!(delta.index_id, build_index);
        assert_eq!(delta.entity_id, entity.id);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn state_only_delete_bumps_root_and_commits_an_exact_dead_version() {
        let db = raw_db("active-text-request-state-only-delete").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x1a,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(16),
        };
        let before = properties("retire me");
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        db.put(graph_key.clone(), encode_properties(&before))
            .await
            .unwrap();
        let (root_key, state_key) = seed_live_entity(
            &db,
            &fixture,
            work::TextPartition::Unpartitioned,
            entity,
            26,
        )
        .await;

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &transaction,
            ActiveTextGraphMutation::delete(scope, entity, &before),
            &mutations,
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.measurements().output_operations(), 3);
        let coordinator: Arc<dyn BlobPublicationCoordinator> =
            Arc::new(ProcessLocalBlobPublicationCoordinator::new(
                Arc::new(InMemory::new()),
                "active-text-request-state-only-delete-blobs",
                BlobPublicationTiming::default(),
            ));
        let publication = crate::index_v2::text::active_publication::publish_active_text_mutation(
            &db,
            coordinator,
            &crate::search::text::BlobGcGate::new(),
            &crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new(),
            index_v2::WriterEpoch::from_bytes([0x45; 16]).unwrap(),
            index_v2::MutationId::from_bytes([0x46; 16]).unwrap(),
            &prepared,
        )
        .await
        .unwrap();
        assert!(publication.uploaded().is_empty());
        let staged = stage_active_text_mutation(&transaction, &prepared, publication.uploaded())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let resolution =
            crate::index_v2::text::active_resolution::resolve_active_text_graph_outcome(
                &db,
                publication,
                staged,
                crate::index_v2::text::active_resolution::ActiveTextGraphCommitObservation::Committed,
            )
            .await
            .unwrap();
        assert!(matches!(
            resolution,
            crate::index_v2::text::active_resolution::ActiveTextGraphResolution::Committed(
                crate::index_v2::text::active_resolution::ActiveTextFinalization::Complete
            )
        ));

        assert!(db.get(graph_key).await.unwrap().is_none());
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&db.get(root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("retired partition keeps its manifest root");
        };
        assert_eq!(root.revision().get(), 3);
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 1);
        let index_values::IndexV2WorkValue::TextEntityState(state) =
            index_values::decode_work_value(&db.get(state_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("retired entity keeps its typed state");
        };
        assert!(!state.live);
        assert_eq!(state.logical_version.get(), 3);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_retirement_revalidates_before_staging_graph_or_index_rows() {
        let db = raw_db("active-text-request-stale-retirement").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x26,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(18),
        };
        let before = properties("stale retirement");
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        let encoded_before = encode_properties(&before);
        db.put(graph_key.clone(), encoded_before.clone())
            .await
            .unwrap();
        let (root_key, state_key) = seed_live_entity(
            &db,
            &fixture,
            work::TextPartition::Unpartitioned,
            entity,
            29,
        )
        .await;
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let original = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &original,
            ActiveTextGraphMutation::delete(scope, entity, &before),
            &mutations,
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        drop(original);

        let concurrent_state = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::TextEntityState(work::TextEntityStateValue {
                index_id: fixture.handle.index_id(),
                generation: fixture.handle.generation(),
                partition: work::TextPartition::Unpartitioned,
                entity_kind: entity.kind,
                entity_id: entity.id,
                logical_version: index_v2::TextLogicalVersion::new(2).unwrap(),
                live: false,
            }),
        );
        db.put(state_key.clone(), concurrent_state.clone())
            .await
            .unwrap();
        let replay = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            stage_active_text_mutation(&replay, &prepared, &[]).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text retirement input changed after serialized preflight"
        ));
        replay.commit().await.unwrap();

        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encoded_before.as_ref())
        );
        assert_eq!(
            db.get(state_key).await.unwrap().as_deref(),
            Some(concurrent_state.as_ref())
        );
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&db.get(root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("stale retirement leaves the observed root unchanged");
        };
        assert_eq!(root.revision().get(), 2);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn same_partition_text_update_appends_without_a_retirement() {
        let db = raw_db("active-text-request-same-partition-update").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x27,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(19),
        };
        let before = properties("before same partition");
        let after = properties("after same partition");
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        db.put(graph_key.clone(), encode_properties(&before))
            .await
            .unwrap();
        let (root_key, state_key) = seed_live_entity(
            &db,
            &fixture,
            work::TextPartition::Unpartitioned,
            entity,
            30,
        )
        .await;
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &transaction,
            ActiveTextGraphMutation::replace(scope, entity, &before, &after),
            &mutations,
            vec![ActiveManifestSplitInput::try_new(
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                split(31),
            )
            .unwrap()],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.measurements().output_operations(), 9);
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x39; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x3A; 16]).unwrap();
        let (_, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        stage_active_text_mutation(&transaction, &prepared, &uploaded)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encode_properties(&after).as_ref())
        );
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&db.get(root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("same-partition append keeps its typed root");
        };
        assert_eq!(root.revision().get(), 3);
        let index_values::IndexV2WorkValue::TextEntityState(state) =
            index_values::decode_work_value(&db.get(state_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("same-partition append keeps its typed live state");
        };
        assert!(state.live);
        assert_eq!(state.logical_version.get(), 3);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_move_retires_old_partition_and_creates_the_new_root_atomically() {
        let db = raw_db("active-text-request-tenant-move").await;
        let scope = DataScope::LegacyUnscoped;
        let old_partition = tenant_partition("acme");
        let new_partition = tenant_partition("globex");
        let fixture =
            seed_partitioned_active(&db, scope, IndexId::initial(), 0x1b, old_partition.clone())
                .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(17),
        };
        let before = partitioned_properties("before move", "acme");
        let after = partitioned_properties("after move", "globex");
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        db.put(graph_key.clone(), encode_properties(&before))
            .await
            .unwrap();
        let (old_root_key, old_state_key) =
            seed_live_entity(&db, &fixture, old_partition.clone(), entity, 27).await;

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mutations =
            mutation::TextMutationSet::empty().with_active_handles(vec![fixture.handle.clone()]);
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &transaction,
            ActiveTextGraphMutation::replace(scope, entity, &before, &after),
            &mutations,
            vec![ActiveManifestSplitInput::try_new(
                &fixture.handle,
                new_partition.clone(),
                split(28),
            )
            .unwrap()],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.measurements().output_operations(), 11);
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x37; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x38; 16]).unwrap();
        let (_, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        stage_active_text_mutation(&transaction, &prepared, &uploaded)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encode_properties(&after).as_ref())
        );
        let index_values::IndexV2WorkValue::TextManifestRoot(old_root) =
            index_values::decode_work_value(&db.get(old_root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("moved-from partition keeps its root");
        };
        assert_eq!(old_root.revision().get(), 3);
        let index_values::IndexV2WorkValue::TextEntityState(old_state) =
            index_values::decode_work_value(&db.get(old_state_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("moved-from partition keeps its dead state");
        };
        assert!(!old_state.live);
        assert_eq!(old_state.logical_version.get(), 3);

        let new_root_typed = index_keys::TextManifestRootKey {
            index_id: fixture.handle.index_id(),
            generation: fixture.handle.generation(),
            partition: new_partition.fingerprint(),
        };
        let new_root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(new_root_typed),
        );
        let index_values::IndexV2WorkValue::TextManifestRoot(new_root) =
            index_values::decode_work_value(&db.get(new_root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("moved-to partition creates its root");
        };
        assert_eq!(new_root.revision().get(), 2);
        assert_eq!(new_root.page_count(), 1);
        let new_state_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                root: new_root_typed,
                entity,
            }),
        );
        let index_values::IndexV2WorkValue::TextEntityState(new_state) =
            index_values::decode_work_value(&db.get(new_state_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("moved-to partition creates its live state");
        };
        assert!(new_state.live);
        assert_eq!(new_state.logical_version.get(), 2);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn graph_create_replace_delete_and_edge_keys_are_exact() {
        let db = raw_db("active-text-request-graph-transitions").await;
        let scope = DataScope::LegacyUnscoped;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(8),
        };
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        let before = properties("before");
        let after = properties("after");
        let wrong = properties("wrong");

        let missing = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_mutation(
                &missing,
                ActiveTextGraphMutation::replace(scope, entity, &before, &after),
                vec![],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::InvariantViolation(_))
        ));
        missing.commit().await.unwrap();

        let encoded_before = encode_properties(&before);
        db.put(graph_key.clone(), encoded_before.clone())
            .await
            .unwrap();
        for mutation in [
            ActiveTextGraphMutation::create(scope, entity, &after),
            ActiveTextGraphMutation::replace(scope, entity, &wrong, &after),
            ActiveTextGraphMutation::delete(scope, entity, &wrong),
        ] {
            let rejected = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(matches!(
                prepare_active_text_mutation(
                    &rejected,
                    mutation,
                    vec![],
                    SearchIndexBackfillLimits::default().active_text_mutation(),
                )
                .await,
                Err(HelixDbError::InvariantViolation(_))
            ));
            rejected.commit().await.unwrap();
        }
        assert_eq!(
            db.get(&graph_key).await.unwrap().as_deref(),
            Some(encoded_before.as_ref())
        );

        let replace = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared_replace = prepare_active_text_mutation(
            &replace,
            ActiveTextGraphMutation::replace(scope, entity, &before, &after),
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert_eq!(prepared_replace.measurements().output_operations(), 1);
        stage_active_text_mutation(&replace, &prepared_replace, &[])
            .await
            .unwrap();
        replace.commit().await.unwrap();
        let encoded_after = encode_properties(&after);
        assert_eq!(
            db.get(&graph_key).await.unwrap().as_deref(),
            Some(encoded_after.as_ref())
        );

        let delete = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared_delete = prepare_active_text_mutation(
            &delete,
            ActiveTextGraphMutation::delete(scope, entity, &after),
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        stage_active_text_mutation(&delete, &prepared_delete, &[])
            .await
            .unwrap();
        delete.commit().await.unwrap();
        assert!(db.get(&graph_key).await.unwrap().is_none());

        let edge = index_keys::IndexEntity {
            kind: IndexElementKind::Edge,
            id: IndexEntityId::new(9),
        };
        let edge_properties = properties("edge");
        let edge_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared_edge = prepare_active_text_mutation(
            &edge_transaction,
            ActiveTextGraphMutation::create(scope, edge, &edge_properties),
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        stage_active_text_mutation(&edge_transaction, &prepared_edge, &[])
            .await
            .unwrap();
        edge_transaction.commit().await.unwrap();
        let edge_key = Key::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge.id.get())),
        }
        .to_bytes();
        assert_eq!(
            db.get(edge_key).await.unwrap().as_deref(),
            Some(encode_properties(&edge_properties).as_ref())
        );

        let stale_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(10),
        };
        let stale_properties = properties("stale preflight");
        let original = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let stale = prepare_active_text_mutation(
            &original,
            ActiveTextGraphMutation::create(scope, stale_entity, &stale_properties),
            vec![],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        drop(original);
        let stale_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(stale_entity.id.get())),
        }
        .to_bytes();
        let concurrent = encode_properties(&properties("concurrent graph value"));
        db.put(stale_key.clone(), concurrent.clone()).await.unwrap();
        let replay = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            stage_active_text_mutation(&replay, &stale, &[]).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text graph input changed after serialized preflight"
        ));
        replay.commit().await.unwrap();
        assert_eq!(
            db.get(stale_key).await.unwrap().as_deref(),
            Some(concurrent.as_ref())
        );

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn aggregate_limits_reject_before_graph_or_attachment_staging() {
        let db = raw_db("active-text-request-aggregate-limits").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x21,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(10),
        };
        let graph = ActiveTextGraphMutation::create(scope, entity, &properties("limits"));
        let destination = ActiveManifestSplitInput::try_new(
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            split(30),
        )
        .unwrap();
        let baseline = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let admitted = prepare_active_text_mutation(
            &baseline,
            graph.clone(),
            vec![destination.clone()],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        let measured = admitted.measurements();
        assert_eq!(measured.output_operations(), 9);
        drop(baseline);

        let exact = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        prepare_active_text_mutation(
            &exact,
            graph.clone(),
            vec![destination.clone()],
            active_limits(
                measured.input_bytes(),
                measured.output_operations(),
                measured.output_bytes(),
                measured.split_bytes(),
                measured.manifest_page_bytes(),
            ),
        )
        .await
        .unwrap();
        exact.commit().await.unwrap();

        let cases = [
            active_limits(
                measured.input_bytes() - 1,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
            ),
            active_limits(
                u64::MAX,
                measured.output_operations() - 1,
                u64::MAX,
                u64::MAX,
                u64::MAX,
            ),
            active_limits(
                u64::MAX,
                u64::MAX,
                measured.output_bytes() - 1,
                u64::MAX,
                measured.manifest_page_bytes(),
            ),
        ];
        for limits in cases {
            let rejected = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(matches!(
                prepare_active_text_mutation(
                    &rejected,
                    graph.clone(),
                    vec![destination.clone()],
                    limits,
                )
                .await,
                Err(HelixDbError::ActiveTextMutationLimitExceeded { .. })
            ));
            rejected.commit().await.unwrap();
        }

        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        let page_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                root: fixture.root_typed,
                page: 0,
            }),
        );
        assert!(db.get(graph_key).await.unwrap().is_none());
        assert!(db.get(page_key).await.unwrap().is_none());
        let index_values::IndexV2WorkValue::TextManifestRoot(root) =
            index_values::decode_work_value(&db.get(&fixture.root_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("manifest root key retains its typed value");
        };
        assert_eq!(root.revision().get(), 1);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_second_destination_leaves_every_request_row_unstaged() {
        let db = raw_db("active-text-request-all-validation-before-staging").await;
        let scope = DataScope::LegacyUnscoped;
        let first = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x31,
            IndexElementKind::Node,
        )
        .await;
        let second = seed_active(
            &db,
            scope,
            IndexId::new(2).unwrap(),
            "title",
            0x32,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(11),
        };
        let build_index = IndexId::new(3).unwrap();
        let mutations = build_mutations(build_index)
            .with_active_handles(vec![first.handle.clone(), second.handle.clone()]);
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        let original = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &original,
            ActiveTextGraphMutation::create(scope, entity, &properties("all or none")),
            &mutations,
            vec![
                ActiveManifestSplitInput::try_new(
                    &first.handle,
                    work::TextPartition::Unpartitioned,
                    split(40),
                )
                .unwrap(),
                ActiveManifestSplitInput::try_new(
                    &second.handle,
                    work::TextPartition::Unpartitioned,
                    split(41),
                )
                .unwrap(),
            ],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        drop(original);
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x51; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x52; 16]).unwrap();
        let (_, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        let occupied_proof = scoped_key(
            scope,
            index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
                index_id: second.handle.index_id(),
                generation: second.handle.generation(),
                intent_id: uploaded[1].intent_id,
            }),
        );
        db.put(occupied_proof, Bytes::from_static(b"occupied"))
            .await
            .unwrap();

        let replay = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            stage_active_text_mutation(&replay, &prepared, &uploaded).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "active text mutation proof destination is already occupied"
        ));
        replay.commit().await.unwrap();
        assert!(db.get(graph_key).await.unwrap().is_none());
        let build_delta_key = scoped_key(
            scope,
            index_keys::IndexV2Key::BuildDelta(index_keys::IndexEntityStateKey {
                index_id: build_index,
                generation: IndexGenerationId::initial(),
                entity,
            }),
        );
        assert!(db.get(build_delta_key).await.unwrap().is_none());
        for fixture in [&first, &second] {
            let page_key = scoped_key(
                scope,
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root: fixture.root_typed,
                    page: 0,
                }),
            );
            assert!(db.get(page_key).await.unwrap().is_none());
            let index_values::IndexV2WorkValue::TextManifestRoot(root) =
                index_values::decode_work_value(&db.get(&fixture.root_key).await.unwrap().unwrap())
                    .unwrap()
            else {
                panic!("manifest root key retains its typed value");
            };
            assert_eq!(root.revision().get(), 1);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_later_attachment_leaves_an_earlier_move_retirement_unstaged() {
        let db = raw_db("active-text-request-retirement-before-invalid-attachment").await;
        let scope = DataScope::LegacyUnscoped;
        let old_partition = tenant_partition("acme");
        let new_partition = tenant_partition("globex");
        let moving =
            seed_partitioned_active(&db, scope, IndexId::initial(), 0x53, old_partition.clone())
                .await;
        let second = seed_active(
            &db,
            scope,
            IndexId::new(2).unwrap(),
            "title",
            0x54,
            IndexElementKind::Node,
        )
        .await;
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(20),
        };
        let before = partitioned_properties("before all validation", "acme");
        let after = partitioned_properties("after all validation", "globex");
        let graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes();
        let encoded_before = encode_properties(&before);
        db.put(graph_key.clone(), encoded_before.clone())
            .await
            .unwrap();
        let (old_root_key, old_state_key) =
            seed_live_entity(&db, &moving, old_partition, entity, 42).await;
        let mutations = mutation::TextMutationSet::empty()
            .with_active_handles(vec![moving.handle.clone(), second.handle.clone()]);
        let original = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = super::prepare_active_text_mutation_with_inputs(
            &original,
            ActiveTextGraphMutation::replace(scope, entity, &before, &after),
            &mutations,
            vec![
                ActiveManifestSplitInput::try_new(&moving.handle, new_partition.clone(), split(43))
                    .unwrap(),
                ActiveManifestSplitInput::try_new(
                    &second.handle,
                    work::TextPartition::Unpartitioned,
                    split(44),
                )
                .unwrap(),
            ],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        drop(original);
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x55; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x56; 16]).unwrap();
        let (_, uploaded) = uploaded_values(&prepared, writer_epoch, mutation_id);
        let occupied_proof = scoped_key(
            scope,
            index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
                index_id: second.handle.index_id(),
                generation: second.handle.generation(),
                intent_id: uploaded[1].intent_id,
            }),
        );
        db.put(occupied_proof, Bytes::from_static(b"occupied"))
            .await
            .unwrap();

        let replay = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            stage_active_text_mutation(&replay, &prepared, &uploaded).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "active text mutation proof destination is already occupied"
        ));
        replay.commit().await.unwrap();

        assert_eq!(
            db.get(graph_key).await.unwrap().as_deref(),
            Some(encoded_before.as_ref())
        );
        let index_values::IndexV2WorkValue::TextManifestRoot(old_root) =
            index_values::decode_work_value(&db.get(old_root_key).await.unwrap().unwrap()).unwrap()
        else {
            panic!("failed request leaves the moved-from root unchanged");
        };
        assert_eq!(old_root.revision().get(), 2);
        let index_values::IndexV2WorkValue::TextEntityState(old_state) =
            index_values::decode_work_value(&db.get(old_state_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("failed request leaves the moved-from state typed");
        };
        assert!(old_state.live);
        assert_eq!(old_state.logical_version.get(), 2);
        let new_root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(index_keys::TextManifestRootKey {
                index_id: moving.handle.index_id(),
                generation: moving.handle.generation(),
                partition: new_partition.fingerprint(),
            }),
        );
        assert!(db.get(new_root_key).await.unwrap().is_none());
        let index_values::IndexV2WorkValue::TextManifestRoot(second_root) =
            index_values::decode_work_value(&db.get(second.root_key).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("failed request leaves the later root typed");
        };
        assert_eq!(second_root.revision().get(), 1);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn destination_shape_rejections_are_typed_and_leave_rows_unstaged() {
        let db = raw_db("active-text-request-destination-shapes").await;
        let scope = DataScope::LegacyUnscoped;
        let fixture = seed_active(
            &db,
            scope,
            IndexId::initial(),
            "body",
            0x61,
            IndexElementKind::Node,
        )
        .await;
        let secondary_definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("Document", "slug").unwrap(),
        )
        .unwrap();
        let secondary = IndexRecordV2::building(
            IndexId::new(2).unwrap(),
            secondary_definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x62; 16]).unwrap(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let secondary_handle =
            index_v2::ActiveIndexHandle::try_from_record(scope, &secondary).unwrap();
        assert!(matches!(
            ActiveManifestSplitInput::try_new(
                &secondary_handle,
                work::TextPartition::Unpartitioned,
                split(60),
            ),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text request received a non-text manifest destination"
        ));

        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(12),
        };
        let duplicate = ActiveManifestSplitInput::try_new(
            &fixture.handle,
            work::TextPartition::Unpartitioned,
            split(61),
        )
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_mutation(
                &transaction,
                ActiveTextGraphMutation::create(scope, entity, &properties("duplicate")),
                vec![duplicate.clone(), duplicate],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text request contains a duplicate manifest destination"
        ));
        transaction.commit().await.unwrap();

        let wrong_kind = seed_active(
            &db,
            scope,
            IndexId::new(3).unwrap(),
            "body",
            0x63,
            IndexElementKind::Edge,
        )
        .await;
        let wrong_kind_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_mutation(
                &wrong_kind_transaction,
                ActiveTextGraphMutation::create(scope, entity, &properties("wrong kind")),
                vec![ActiveManifestSplitInput::try_new(
                    &wrong_kind.handle,
                    work::TextPartition::Unpartitioned,
                    split(62),
                )
                .unwrap()],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text request contains an unexpected manifest destination"
        ));
        wrong_kind_transaction.commit().await.unwrap();

        let tenant_scope = DataScope::Tenant(TenantId::from_u128(1));
        let wrong_scope_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepare_active_text_mutation(
                &wrong_scope_transaction,
                ActiveTextGraphMutation::create(
                    tenant_scope,
                    entity,
                    &properties("wrong scope"),
                ),
                vec![ActiveManifestSplitInput::try_new(
                    &fixture.handle,
                    work::TextPartition::Unpartitioned,
                    split(63),
                )
                .unwrap()],
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text generation scope disagrees with its graph mutation"
        ));
        wrong_scope_transaction.commit().await.unwrap();

        let count_entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(13),
        };
        let count_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let count_prepared = prepare_active_text_mutation(
            &count_transaction,
            ActiveTextGraphMutation::create(scope, count_entity, &properties("count")),
            vec![ActiveManifestSplitInput::try_new(
                &fixture.handle,
                work::TextPartition::Unpartitioned,
                split(64),
            )
            .unwrap()],
            SearchIndexBackfillLimits::default().active_text_mutation(),
        )
        .await
        .unwrap();
        assert!(matches!(
            stage_active_text_mutation(&count_transaction, &count_prepared, &[]).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason
                    == "Active text upload results disagree with the admitted destination count"
        ));
        let writer_epoch = index_v2::WriterEpoch::from_bytes([0x71; 16]).unwrap();
        let mutation_id = index_v2::MutationId::from_bytes([0x72; 16]).unwrap();
        let (count_intents, _) = uploaded_values(&count_prepared, writer_epoch, mutation_id);
        assert!(matches!(
            stage_active_text_mutation(
                &count_transaction,
                &count_prepared,
                &[count_intents[0].value().clone()],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "active text attachment requires definitive Uploaded publication"
        ));
        count_transaction.commit().await.unwrap();
        let count_graph_key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(count_entity.id.get())),
        }
        .to_bytes();
        assert!(db.get(count_graph_key).await.unwrap().is_none());

        db.close().await.unwrap();
    }
}
