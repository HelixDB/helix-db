//! Loaded V2 catalog and active-generation handles.
//!
//! A loaded catalog is the only planner-facing projection of durable V2 state.
//! It contains configured indexes plus canonical records in `Active`; every
//! physical index access must retain and revalidate its family-typed handle.

use std::collections::HashMap;

use crate::config::RuntimeIndexCatalog;
use crate::encoding::v1::keys::tenant::DataScope;

use super::{
    IndexGenerationId, IndexId, IndexIdentity, IndexRecordV2, IndexRevision, IndexStateV2,
    PhysicalGeneration, ValidatedDynamicIndexDefinition, VectorGenerationDescriptor,
    VectorPhysicalLayout,
};

/// Immutable configured-index authority for retained static maintenance.
///
/// This capability is constructed only while loading a scope. It deliberately
/// excludes canonical V2 `Active` definitions, preventing the retained static
/// secondary/text paths from writing a second, legacy physical representation
/// for lifecycle-owned indexes.
#[derive(Debug, Clone)]
pub(crate) struct ConfiguredIndexCatalog {
    runtime: RuntimeIndexCatalog,
}

#[cfg(test)]
impl Default for ConfiguredIndexCatalog {
    fn default() -> Self {
        Self::new(RuntimeIndexCatalog::new())
    }
}

impl ConfiguredIndexCatalog {
    fn new(runtime: RuntimeIndexCatalog) -> Self {
        Self { runtime }
    }

    /// Borrows the configured-only runtime definitions.
    pub(crate) const fn runtime(&self) -> &RuntimeIndexCatalog {
        &self.runtime
    }
}

/// Exact durable generation authorization retained by runtime index users.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActiveIndexHandle {
    /// Generation-qualified secondary rows.
    Secondary {
        scope: DataScope,
        identity: IndexIdentity,
        index_id: IndexId,
        generation: IndexGenerationId,
        record_revision: IndexRevision,
        definition: Box<super::ValidatedSecondaryIndexDefinition>,
    },
    /// HNSW rows under unpartitioned or mapped partition ownership.
    Vector {
        scope: DataScope,
        identity: IndexIdentity,
        index_id: IndexId,
        generation: IndexGenerationId,
        record_revision: IndexRevision,
        definition: Box<super::ValidatedVectorIndexDefinition>,
        layout: VectorPhysicalLayout,
        descriptor: VectorGenerationDescriptor,
    },
    /// Generation-qualified text manifests and entity state.
    Text {
        scope: DataScope,
        identity: IndexIdentity,
        index_id: IndexId,
        generation: IndexGenerationId,
        record_revision: IndexRevision,
        definition: Box<super::ValidatedTextIndexDefinition>,
    },
}

impl ActiveIndexHandle {
    /// Projects a handle only from an exact canonical `Active` record.
    pub(crate) fn try_from_record(scope: DataScope, record: &IndexRecordV2) -> Option<Self> {
        let IndexStateV2::Active { physical, .. } = record.state() else {
            return None;
        };
        let common = (
            scope,
            record.identity().clone(),
            record.index_id(),
            physical.generation(),
            record.revision(),
        );
        Some(match physical {
            PhysicalGeneration::Secondary { .. } => {
                let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition()
                else {
                    return None;
                };
                Self::Secondary {
                    scope: common.0,
                    identity: common.1,
                    index_id: common.2,
                    generation: common.3,
                    record_revision: common.4,
                    definition: Box::new(definition.clone()),
                }
            }
            PhysicalGeneration::Vector {
                layout, descriptor, ..
            } => {
                let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition()
                else {
                    return None;
                };
                Self::Vector {
                    scope: common.0,
                    identity: common.1,
                    index_id: common.2,
                    generation: common.3,
                    record_revision: common.4,
                    definition: Box::new(definition.clone()),
                    layout: *layout,
                    descriptor: *descriptor,
                }
            }
            PhysicalGeneration::Text { .. } => {
                let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
                    return None;
                };
                Self::Text {
                    scope: common.0,
                    identity: common.1,
                    index_id: common.2,
                    generation: common.3,
                    record_revision: common.4,
                    definition: Box::new(definition.clone()),
                }
            }
        })
    }

    /// Returns the logical data scope whose durable record authorized this handle.
    pub(crate) const fn scope(&self) -> DataScope {
        match self {
            Self::Secondary { scope, .. }
            | Self::Vector { scope, .. }
            | Self::Text { scope, .. } => *scope,
        }
    }

    /// Returns the stable logical index ID bound into the physical authorization.
    pub(crate) const fn index_id(&self) -> IndexId {
        match self {
            Self::Secondary { index_id, .. }
            | Self::Vector { index_id, .. }
            | Self::Text { index_id, .. } => *index_id,
        }
    }

    /// Returns the generation that qualifies every physical row reached through this handle.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        match self {
            Self::Secondary { generation, .. }
            | Self::Vector { generation, .. }
            | Self::Text { generation, .. } => *generation,
        }
    }

    /// Returns the canonical record revision that must still be active before physical access.
    pub(crate) const fn record_revision(&self) -> IndexRevision {
        match self {
            Self::Secondary {
                record_revision, ..
            }
            | Self::Vector {
                record_revision, ..
            }
            | Self::Text {
                record_revision, ..
            } => *record_revision,
        }
    }

    /// Returns the logical identity used to point-read the canonical index record.
    pub(crate) fn identity(&self) -> &IndexIdentity {
        match self {
            Self::Secondary { identity, .. }
            | Self::Vector { identity, .. }
            | Self::Text { identity, .. } => identity,
        }
    }

    /// Returns the validated definition carried by an Active text handle.
    ///
    /// Keeping semantic routing data inside the canonical-generation
    /// capability lets mutation code derive label, property, and tenant
    /// effects without trusting a second caller-supplied definition.
    pub(crate) const fn text_definition(&self) -> Option<&super::ValidatedTextIndexDefinition> {
        match self {
            Self::Text { definition, .. } => Some(definition),
            Self::Secondary { .. } | Self::Vector { .. } => None,
        }
    }

    /// Returns the validated definition carried by an Active secondary handle.
    ///
    /// Equality uniqueness and range direction select different physical lanes,
    /// so serving must retain these canonical settings beside the generation.
    pub(crate) const fn secondary_definition(
        &self,
    ) -> Option<&super::ValidatedSecondaryIndexDefinition> {
        match self {
            Self::Secondary { definition, .. } => Some(definition),
            Self::Vector { .. } | Self::Text { .. } => None,
        }
    }

    /// Returns whether a freshly decoded record grants this exact handle.
    pub(crate) fn matches_record(&self, scope: DataScope, record: &IndexRecordV2) -> bool {
        Self::try_from_record(scope, record).as_ref() == Some(self)
    }

    /// Returns the public family used for fail-closed reader diagnostics.
    pub(crate) const fn family(&self) -> crate::error::IndexFamily {
        match self {
            Self::Secondary { .. } => crate::error::IndexFamily::Secondary,
            Self::Vector { .. } => crate::error::IndexFamily::Vector,
            Self::Text { .. } => crate::error::IndexFamily::Text,
        }
    }
}

/// Planner catalog plus exact active handles loaded for one scope.
#[derive(Debug, Clone)]
pub(crate) struct LoadedV2ScopeCatalog {
    scope: DataScope,
    configured: ConfiguredIndexCatalog,
    runtime: RuntimeIndexCatalog,
    active: HashMap<IndexIdentity, ActiveIndexHandle>,
}

impl LoadedV2ScopeCatalog {
    /// Starts a loaded scope from its non-persisted configured-index projection.
    pub(crate) fn new(scope: DataScope, runtime: RuntimeIndexCatalog) -> Self {
        Self {
            scope,
            configured: ConfiguredIndexCatalog::new(runtime.clone()),
            runtime,
            active: HashMap::new(),
        }
    }

    /// Adds one canonical active record to both runtime projections.
    pub(crate) fn insert_active(
        &mut self,
        record: &IndexRecordV2,
    ) -> Result<(), crate::error::HelixDbError> {
        let Some(handle) = ActiveIndexHandle::try_from_record(self.scope, record) else {
            return Ok(());
        };
        if self.active.contains_key(record.identity()) {
            return Err(crate::error::HelixDbError::IndexCatalogCorruption(
                "duplicate canonical V2 index identity in one scope".to_string(),
            ));
        }
        self.runtime.insert_dynamic_index(record.definition());
        self.active.insert(record.identity().clone(), handle);
        Ok(())
    }

    /// Returns the one scope covered by this complete catalog load.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Borrows the immutable configured/static base used by future refreshes.
    ///
    /// Dynamic definitions are deliberately excluded so a fresh canonical
    /// scan can remove indexes activated or dropped by another runtime.
    pub(crate) const fn configured(&self) -> &ConfiguredIndexCatalog {
        &self.configured
    }

    /// Borrows the planner-facing configured plus active-dynamic projection.
    pub(crate) const fn runtime(&self) -> &RuntimeIndexCatalog {
        &self.runtime
    }

    /// Returns the exact active-generation authorization for one logical identity.
    #[cfg(test)]
    pub(crate) fn handle(&self, identity: &IndexIdentity) -> Option<&ActiveIndexHandle> {
        self.active.get(identity)
    }

    /// Iterates every active handle whose canonical key must join mutation conflict tracking.
    pub(crate) fn active_handles(&self) -> impl Iterator<Item = &ActiveIndexHandle> {
        self.active.values()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{SecondaryIndexDefinition, TextAnalyzerKind};

    use super::*;
    use crate::index_v2::{
        IndexElementKind, IndexOperationId, PhysicalGeneration, ValidatedTextIndexDefinition,
    };

    #[test]
    fn only_active_records_enter_the_loaded_catalog() {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        let operation_id = IndexOperationId::new_v4();
        let building = IndexRecordV2::building(
            IndexId::initial(),
            definition.clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            operation_id,
        )
        .unwrap();
        let active = building
            .transition(crate::index_v2::IndexStateTransition::Activate)
            .unwrap();
        let mut catalog =
            LoadedV2ScopeCatalog::new(DataScope::LegacyUnscoped, RuntimeIndexCatalog::new());

        catalog.insert_active(&building).unwrap();
        assert!(catalog.handle(building.identity()).is_none());
        catalog.insert_active(&active).unwrap();
        let handle = catalog.handle(active.identity()).unwrap();
        assert!(handle.text_definition().is_none());
        let key = crate::config::scoped_secondary_index_property("User", "email");
        assert!(catalog.runtime().contains_node_equality_scoped(&key));
        assert!(
            !catalog
                .configured()
                .runtime()
                .contains_node_equality_scoped(&key),
            "persisted Active rows must not contaminate the configured refresh base"
        );
    }

    #[test]
    fn active_text_handle_carries_its_exact_validated_definition() {
        let definition = ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "body",
            Some("tenant"),
            TextAnalyzerKind::Standard,
            false,
        )
        .unwrap();
        let building = IndexRecordV2::building(
            IndexId::initial(),
            ValidatedDynamicIndexDefinition::Text(definition.clone()),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap();
        let active = building
            .transition(crate::index_v2::IndexStateTransition::Activate)
            .unwrap();
        let handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("an Active text record projects its exact handle");

        assert_eq!(handle.text_definition(), Some(&definition));
        assert_eq!(handle.identity(), &definition.identity());
    }
}
