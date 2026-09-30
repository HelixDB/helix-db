//! Transactional V2 text-index mutation routing.
//!
//! A graph transaction loads canonical text generations in its serializable
//! snapshot. Each routed entity transition becomes one complete queued text
//! operation for every affected Building or Active generation (see
//! [`crate::index_lifecycle::queue::producer`]); no physical text row is read
//! or written by the graph transaction itself.

use crate::encoding::property::Property;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexRecordV2, ValidatedDynamicIndexDefinition,
    ValidatedTextIndexDefinition,
};

/// Transaction-local text generations that accept ordinary mutation work.
///
/// Each generation keeps the canonical record this transaction classified,
/// so queued admission measures the record row publication revalidates.
#[derive(Debug, Clone, Default)]
pub(crate) struct TextMutationSet {
    /// Hidden builds, by build route ordinal.
    building: Vec<IndexRecordV2>,
    /// Active generations, by Active route ordinal.
    active: Vec<IndexRecordV2>,
}

/// One routed text generation selected for queued maintenance.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueuedTextTarget<'a> {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) definition: &'a ValidatedTextIndexDefinition,
    /// Canonical record every publication of this generation revalidates.
    pub(crate) record: &'a IndexRecordV2,
}

/// Family-local target ordinal produced by the canonical catalog classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::index_lifecycle) enum TextMutationTargetOrdinal {
    /// Hidden build target ordinal.
    Building(usize),
    /// Active generation handle ordinal.
    Active(usize),
}

impl TextMutationSet {
    /// Constructs an empty set for focused configured-index tests.
    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self {
            building: Vec::new(),
            active: Vec::new(),
        }
    }

    /// Resolves one routed text target for queued maintenance.
    pub(crate) fn queued_target(
        &self,
        route: crate::index_lifecycle::mutation_catalog::MutationRouteTarget,
    ) -> Result<QueuedTextTarget<'_>> {
        let record = match route {
            crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(
                ordinal,
            ) => self.building.get(ordinal).ok_or_else(|| {
                corruption("text mutation route named a build target outside its catalog")
            })?,
            crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextActive(ordinal) => {
                self.active.get(ordinal).ok_or_else(|| {
                    corruption("text mutation route named an Active target outside its catalog")
                })?
            }
            crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Secondary(_)
            | crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Vector(_) => {
                return Err(corruption(
                    "text queued target resolution received another family route",
                ));
            }
        };
        let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
            return Err(corruption(
                "text mutation route named a non-text generation",
            ));
        };
        Ok(QueuedTextTarget {
            index_id: record.index_id(),
            generation: record.state().generation(),
            definition,
            record,
        })
    }

    /// Counts classified records for the one-scan catalog contract.
    #[cfg(test)]
    pub(in crate::index_lifecycle) const fn catalog_entry_count(&self) -> usize {
        self.building.len() + self.active.len()
    }

    /// Classifies one same-snapshot canonical text record.
    pub(in crate::index_lifecycle) fn include_catalog_entry(
        &mut self,
        entry: crate::index_lifecycle::mutation_catalog::MutationCatalogEntry<'_>,
    ) -> Result<TextMutationTargetOrdinal> {
        match entry {
            crate::index_lifecycle::mutation_catalog::MutationCatalogEntry::Building(record) => {
                let ValidatedDynamicIndexDefinition::Text(_) = record.definition() else {
                    return Err(corruption(
                        "text mutation classifier received another family",
                    ));
                };
                let ordinal = self.building.len();
                self.building.push(record.clone());
                Ok(TextMutationTargetOrdinal::Building(ordinal))
            }
            crate::index_lifecycle::mutation_catalog::MutationCatalogEntry::Active {
                record,
                handle,
            } => {
                if !matches!(
                    handle,
                    crate::index_lifecycle::ActiveIndexHandle::Text { .. }
                ) || !matches!(
                    record.definition(),
                    ValidatedDynamicIndexDefinition::Text(_)
                ) {
                    return Err(corruption(
                        "active text record carried another family handle",
                    ));
                }
                let ordinal = self.active.len();
                self.active.push(record.clone());
                Ok(TextMutationTargetOrdinal::Active(ordinal))
            }
        }
    }
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

/// Projects one complete property row into the exact queued text payload.
///
/// Present documents that cannot satisfy the definition fail the write with
/// the `invalid_index_source_data` contract initial builds also report.
pub(crate) fn queued_document(
    definition: &ValidatedTextIndexDefinition,
    properties: &[Property],
) -> Result<Option<crate::index_lifecycle::queue::producer::QueuedTextDocument>> {
    match super::projection::project(definition, properties).map_err(|error| {
        HelixDbError::InvalidIndexSourceData {
            reason: format!(
                "text index {}:{}: {error}",
                definition.label().as_str(),
                definition.property().as_str(),
            ),
        }
    })? {
        super::projection::TextSourceProjection::NotIndexed => Ok(None),
        super::projection::TextSourceProjection::Indexed { partition, text } => Ok(Some(
            crate::index_lifecycle::queue::producer::QueuedTextDocument {
                partition,
                text: std::sync::Arc::from(text),
            },
        )),
    }
}
