//! One canonical index-catalog snapshot for a graph mutation transaction.
//!
//! Ordinary graph writes need hidden-build deltas, Active maintenance targets,
//! Active text handles, and generation capabilities for commit-error
//! classification. Loading those views independently would add redundant range
//! reads and could mix runtime state with transaction state. This module scans
//! canonical `IndexRecord` rows exactly once inside the caller's serializable
//! transaction and derives every family view from those same decoded records.

use std::collections::{BTreeMap, HashMap};

use slatedb::DbReadOps;

use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v2::keys::Key;
use crate::encoding::v2::keys::{RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
use crate::error::{HelixDbError, Result};

use super::{
    secondary, text, vector, ActiveIndexHandle, IndexRecordV2, IndexStateV2,
    ValidatedDynamicIndexDefinition,
};

/// One canonical record whose lifecycle state accepts ordinary mutation work.
///
/// Carrying the exact Active handle in the Active variant prevents family
/// classifiers from accepting a record and an unrelated generation capability.
#[derive(Clone, Copy)]
pub(super) enum MutationCatalogEntry<'a> {
    /// A hidden generation that receives a coalesced build delta.
    Building(&'a IndexRecordV2),
    /// A published generation that receives direct physical maintenance.
    Active {
        /// Canonical record read by this transaction.
        record: &'a IndexRecordV2,
        /// Capability projected from that exact record.
        handle: &'a ActiveIndexHandle,
    },
}

/// Closed family projections derived from one serializable catalog range read.
pub(crate) struct MutationIndexCatalog {
    active: ActiveMutationCatalog,
    secondary: secondary::SecondaryMutationSet,
    vector: vector::VectorMutationSet,
    text: text::mutation::TextMutationSet,
    routes: MutationRouteCatalog,
}

/// Exact Active capabilities from one transaction snapshot.
///
/// The private fields keep identity lookup and commit classification backed by
/// the same handles, so callers cannot pair an ordinal map with another scan.
#[derive(Debug, Default)]
pub(crate) struct ActiveMutationCatalog {
    generations: Vec<ActiveIndexHandle>,
    ordinals: HashMap<super::IndexIdentity, usize>,
}

impl ActiveMutationCatalog {
    fn insert(&mut self, handle: ActiveIndexHandle) -> Result<()> {
        let ordinal = self.generations.len();
        if self
            .ordinals
            .insert(handle.identity().clone(), ordinal)
            .is_some()
        {
            return Err(corruption(
                "mutation catalog contained a duplicate active identity",
            ));
        }
        self.generations.push(handle);
        Ok(())
    }

    /// Resolves a capability read from this exact transaction snapshot.
    pub(crate) fn handle(&self, identity: &super::IndexIdentity) -> Option<&ActiveIndexHandle> {
        self.ordinals
            .get(identity)
            .and_then(|ordinal| self.generations.get(*ordinal))
    }

    #[cfg(test)]
    pub(crate) fn generations(&self) -> &[ActiveIndexHandle] {
        &self.generations
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(&mut self, handle: ActiveIndexHandle) {
        self.insert(handle)
            .expect("focused test active handles have unique identities");
    }

    pub(crate) fn into_generations(self) -> Vec<ActiveIndexHandle> {
        self.generations
    }
}

/// One family-owned target selected without rescanning unrelated definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MutationRouteTarget {
    /// Index into the transaction-local secondary target set.
    Secondary(usize),
    /// Index into the transaction-local vector target set.
    Vector(usize),
    /// Index into the transaction-local hidden text-build target set.
    TextBuilding(usize),
    /// Index into the transaction-local Active text handle set.
    TextActive(usize),
}

/// Runtime-only target router derived from the same serializable catalog scan.
///
/// Whole-row changes use `by_label`; replacements use `by_property` unless
/// `$label` changed, in which case both old and new label routes are selected.
/// Target ordinals point into the family sets returned with this catalog, so a
/// route cannot name a generation from another transaction snapshot.
#[derive(Debug, Default)]
pub(crate) struct MutationRouteCatalog {
    by_element_kind: BTreeMap<super::IndexElementKind, BTreeMap<Box<str>, MutationLabelRoutes>>,
}

#[derive(Debug, Default)]
struct MutationLabelRoutes {
    all: Vec<MutationRouteTarget>,
    by_property: BTreeMap<Box<str>, Vec<MutationRouteTarget>>,
}

/// Borrowed target selection for one authoritative graph transition.
///
/// The common create/delete/single-property cases borrow one catalog slice and
/// allocate nothing. `Owned` is reserved for future multi-property edits.
pub(crate) enum RoutedMutationTargets<'a> {
    /// No configured target can observe this transition.
    None,
    /// One label/property route.
    One(&'a [MutationRouteTarget]),
    /// A label move spanning two disjoint label routes.
    Two(&'a [MutationRouteTarget], &'a [MutationRouteTarget]),
    /// A deduplicated union for a multi-property replacement.
    Owned(Vec<MutationRouteTarget>),
}

impl RoutedMutationTargets<'_> {
    /// Iterates selected targets in canonical catalog order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = MutationRouteTarget> + '_ {
        let (first, second) = match self {
            Self::None => (&[][..], &[][..]),
            Self::One(targets) => (*targets, &[][..]),
            Self::Two(first, second) => (*first, *second),
            Self::Owned(targets) => (targets.as_slice(), &[][..]),
        };
        first.iter().chain(second).copied()
    }
}

impl MutationRouteCatalog {
    fn register<'a>(
        &mut self,
        element_kind: super::IndexElementKind,
        label: &str,
        properties: impl IntoIterator<Item = &'a str>,
        target: MutationRouteTarget,
    ) {
        let label_routes = self
            .by_element_kind
            .entry(element_kind)
            .or_default()
            .entry(label.into())
            .or_default();
        label_routes.all.push(target);
        for property in properties {
            let targets = label_routes.by_property.entry(property.into()).or_default();
            if targets.last() != Some(&target) {
                targets.push(target);
            }
        }
    }

    /// Selects exact family target ordinals for one graph-row transition.
    pub(crate) fn targets_for(
        &self,
        transition: &super::graph_mutation::GraphMutationTransition,
    ) -> RoutedMutationTargets<'_> {
        use super::graph_mutation::GraphMutationTransition;

        let element_kind = transition.entity().index_entity().kind;
        match transition {
            GraphMutationTransition::Create { after, .. } => {
                self.targets_for_label(element_kind, graph_label(after.properties()))
            }
            GraphMutationTransition::Delete { before, .. } => {
                self.targets_for_label(element_kind, graph_label(before.properties()))
            }
            GraphMutationTransition::Replace {
                before,
                after,
                changed,
                ..
            } if changed.contains("$label") => {
                let before = self.label_targets(element_kind, graph_label(before.properties()));
                let after = self.label_targets(element_kind, graph_label(after.properties()));
                match (before, after) {
                    (None, None) => RoutedMutationTargets::None,
                    (Some(targets), None) | (None, Some(targets)) => {
                        RoutedMutationTargets::One(targets)
                    }
                    (Some(first), Some(second)) => RoutedMutationTargets::Two(first, second),
                }
            }
            GraphMutationTransition::Replace { after, changed, .. } => {
                let Some(label) = graph_label(after.properties()) else {
                    return RoutedMutationTargets::None;
                };
                let mut changed = changed.iter();
                let Some(first) = changed.next() else {
                    unreachable!("replacement transitions have non-empty changed properties")
                };
                let first = self.property_targets(element_kind, label, first);
                let Some(second_property) = changed.next() else {
                    return first.map_or(RoutedMutationTargets::None, RoutedMutationTargets::One);
                };
                let mut targets = first.map_or_else(Vec::new, ToOwned::to_owned);
                for property in std::iter::once(second_property).chain(changed) {
                    if let Some(more) = self.property_targets(element_kind, label, property) {
                        targets.extend_from_slice(more);
                    }
                }
                targets.sort_unstable();
                targets.dedup();
                if targets.is_empty() {
                    RoutedMutationTargets::None
                } else {
                    RoutedMutationTargets::Owned(targets)
                }
            }
        }
    }

    /// Selects label-scoped targets for a coalesced original/final row pair.
    pub(crate) fn targets_for_states(
        &self,
        element_kind: super::IndexElementKind,
        before: &[crate::encoding::v1::property::Property],
        after: &[crate::encoding::v1::property::Property],
    ) -> RoutedMutationTargets<'_> {
        let before_label = graph_label(before);
        let after_label = graph_label(after);
        if before_label == after_label {
            return self.targets_for_label(element_kind, after_label);
        }
        let before = self.label_targets(element_kind, before_label);
        let after = self.label_targets(element_kind, after_label);
        match (before, after) {
            (None, None) => RoutedMutationTargets::None,
            (Some(targets), None) | (None, Some(targets)) => RoutedMutationTargets::One(targets),
            (Some(first), Some(second)) => RoutedMutationTargets::Two(first, second),
        }
    }

    fn targets_for_label(
        &self,
        element_kind: super::IndexElementKind,
        label: Option<&str>,
    ) -> RoutedMutationTargets<'_> {
        self.label_targets(element_kind, label)
            .map_or(RoutedMutationTargets::None, RoutedMutationTargets::One)
    }

    fn label_targets(
        &self,
        element_kind: super::IndexElementKind,
        label: Option<&str>,
    ) -> Option<&[MutationRouteTarget]> {
        self.by_element_kind
            .get(&element_kind)?
            .get(label?)
            .map(|routes| routes.all.as_slice())
    }

    fn property_targets(
        &self,
        element_kind: super::IndexElementKind,
        label: &str,
        property: &str,
    ) -> Option<&[MutationRouteTarget]> {
        self.by_element_kind
            .get(&element_kind)?
            .get(label)?
            .by_property
            .get(property)
            .map(Vec::as_slice)
    }
}

fn graph_label(properties: &[crate::encoding::v1::property::Property]) -> Option<&str> {
    properties
        .iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str())
}

impl MutationIndexCatalog {
    /// Loads and classifies every canonical record in one transaction-owned scan.
    ///
    /// Retaining this range read in the graph transaction preserves the SSI
    /// conflict with concurrent DDL. Runtime-catalog refresh remains separate
    /// publication work and is never treated as mutation authority here.
    pub(crate) async fn load(
        transaction: &(impl DbReadOps + Sync),
        scope: DataScope,
    ) -> Result<Self> {
        let prefix = Key::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
        let mut rows = transaction.scan_prefix(prefix, ..).await?;
        let mut active = ActiveMutationCatalog::default();
        let mut secondary = secondary::SecondaryMutationSet::default();
        let mut vector = vector::VectorMutationSet::default();
        let mut text = text::mutation::TextMutationSet::default();
        let mut routes = MutationRouteCatalog::default();

        while let Some(row) = rows.next().await? {
            let Key::Data {
                kind: ScopedKey::IndexRecord(key),
                ..
            } = Key::parse_from_slice(scope, &row.key)?
            else {
                return Err(corruption(
                    "mutation catalog prefix yielded another key kind",
                ));
            };
            let record = decode_index_record(&row.value)?;
            if key.identity != *record.identity() {
                return Err(corruption("mutation catalog key/value identity mismatch"));
            }

            let active_handle = match record.state() {
                IndexStateV2::Building { .. } => None,
                IndexStateV2::Active { .. } => Some(
                    ActiveIndexHandle::try_from_record(scope, &record).ok_or_else(|| {
                        corruption("active mutation record did not project an exact handle")
                    })?,
                ),
                IndexStateV2::Aborting { .. }
                | IndexStateV2::Dropping { .. }
                | IndexStateV2::Dropped { .. } => continue,
            };
            let entry = match active_handle.as_ref() {
                Some(handle) => MutationCatalogEntry::Active {
                    record: &record,
                    handle,
                },
                None => MutationCatalogEntry::Building(&record),
            };
            match record.definition() {
                ValidatedDynamicIndexDefinition::Secondary(definition) => {
                    let ordinal = secondary.include_catalog_entry(entry)?;
                    routes.register(
                        definition.element_kind(),
                        definition.label().as_str(),
                        std::iter::once(definition.property().as_str()),
                        MutationRouteTarget::Secondary(ordinal),
                    );
                }
                ValidatedDynamicIndexDefinition::Vector(definition) => {
                    let ordinal = vector.include_catalog_entry(entry)?;
                    routes.register(
                        definition.element_kind(),
                        definition.label().as_str(),
                        std::iter::once(definition.property().as_str()).chain(
                            definition
                                .tenant_property()
                                .map(super::IndexComponent::as_str),
                        ),
                        MutationRouteTarget::Vector(ordinal),
                    );
                }
                ValidatedDynamicIndexDefinition::Text(definition) => {
                    let ordinal = text.include_catalog_entry(entry)?;
                    let target = match ordinal {
                        text::mutation::TextMutationTargetOrdinal::Building(ordinal) => {
                            MutationRouteTarget::TextBuilding(ordinal)
                        }
                        text::mutation::TextMutationTargetOrdinal::Active(ordinal) => {
                            MutationRouteTarget::TextActive(ordinal)
                        }
                    };
                    routes.register(
                        definition.element_kind(),
                        definition.label().as_str(),
                        std::iter::once(definition.property().as_str()).chain(
                            definition
                                .tenant_property()
                                .map(super::IndexComponent::as_str),
                        ),
                        target,
                    );
                }
            }
            if let Some(active_handle) = active_handle {
                active.insert(active_handle)?;
            }
        }

        Ok(Self {
            active,
            secondary,
            vector,
            text,
            routes,
        })
    }

    /// Transfers every family view into the request-owned mutation context.
    pub(crate) fn into_components(
        self,
    ) -> (
        ActiveMutationCatalog,
        secondary::SecondaryMutationSet,
        vector::VectorMutationSet,
        text::mutation::TextMutationSet,
        MutationRouteCatalog,
    ) {
        (
            self.active,
            self.secondary,
            self.vector,
            self.text,
            self.routes,
        )
    }
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use bytes::Bytes;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{ByteRangeBounds, Db, DbIterator, DbReadOps, IsolationLevel, KeyValue};

    use super::*;
    use crate::config::{SecondaryIndexDefinition, TextIndexDefinition, VectorIndexDefinition};
    use crate::encoding::v2::values::encode_index_record;
    use crate::index_lifecycle::{
        IndexGenerationId, IndexId, IndexOperationId, IndexRevision, IndexStateTransition,
        PhysicalGeneration, VectorGenerationDescriptor, VectorPhysicalIndexId,
        VectorPhysicalLayout,
    };
    use crate::search::vector::VectorDistanceMetric;

    struct CountingCatalogRead<'a> {
        transaction: &'a slatedb::DbTransaction,
        scans: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DbReadOps for CountingCatalogRead<'_> {
        async fn get_with_options<K: AsRef<[u8]> + Send>(
            &self,
            key: K,
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
            self.transaction.get_with_options(key, options).await
        }

        async fn multi_get_with_options<K>(
            &self,
            keys: &[K],
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error>
        where
            K: AsRef<[u8]> + Send + Sync,
        {
            self.transaction.multi_get_with_options(keys, options).await
        }

        async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
            &self,
            key: K,
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Option<KeyValue>, slatedb::Error> {
            self.transaction
                .get_key_value_with_options(key, options)
                .await
        }

        async fn scan_with_options<T>(
            &self,
            range: T,
            options: &slatedb::config::ScanOptions,
        ) -> std::result::Result<DbIterator, slatedb::Error>
        where
            T: ByteRangeBounds + Send,
        {
            self.scans.fetch_add(1, Ordering::Relaxed);
            self.transaction.scan_with_options(range, options).await
        }
    }

    fn building_record(
        index_id: u64,
        definition: ValidatedDynamicIndexDefinition,
    ) -> IndexRecordV2 {
        let physical = match &definition {
            ValidatedDynamicIndexDefinition::Secondary(_) => PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            ValidatedDynamicIndexDefinition::Vector(definition) => PhysicalGeneration::Vector {
                generation: IndexGenerationId::initial(),
                layout: VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::new(index_id + 100)
                        .expect("fixture physical ID is nonzero"),
                },
                descriptor: VectorGenerationDescriptor::for_definition(definition),
            },
            ValidatedDynamicIndexDefinition::Text(_) => PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
        };
        IndexRecordV2::building(
            IndexId::new(index_id).expect("fixture logical ID is nonzero"),
            definition,
            IndexRevision::initial(),
            physical,
            IndexOperationId::new_v4(),
        )
        .expect("fixture record builds")
    }

    fn secondary(label: &str) -> ValidatedDynamicIndexDefinition {
        SecondaryIndexDefinition::node_equality(label, "value")
            .expect("secondary fixture validates")
            .try_into()
            .expect("secondary fixture enters V2")
    }

    fn vector(label: &str) -> ValidatedDynamicIndexDefinition {
        VectorIndexDefinition::new_node(label, "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("vector fixture validates")
            .try_into()
            .expect("vector fixture enters V2")
    }

    fn text(label: &str) -> ValidatedDynamicIndexDefinition {
        TextIndexDefinition::new_node(label, "body")
            .expect("text fixture validates")
            .try_into()
            .expect("text fixture enters V2")
    }

    #[tokio::test]
    async fn one_scan_classifies_every_family_and_same_snapshot_active_handle() {
        let scope = DataScope::LegacyUnscoped;
        let db = Db::builder("mutation-catalog-one-scan", Arc::new(InMemory::new()))
            .build()
            .await
            .expect("fixture database opens");
        let records = [
            building_record(1, secondary("SecondaryBuilding")),
            building_record(2, secondary("SecondaryActive"))
                .transition(IndexStateTransition::Activate)
                .expect("secondary fixture activates"),
            building_record(3, vector("VectorBuilding")),
            building_record(4, vector("VectorActive"))
                .transition(IndexStateTransition::Activate)
                .expect("vector fixture activates"),
            building_record(5, text("TextBuilding")),
            building_record(6, text("TextActive"))
                .transition(IndexStateTransition::Activate)
                .expect("text fixture activates"),
        ];
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("seed transaction opens");
        for record in &records {
            seed.put(
                Key::Data {
                    scope,
                    kind: ScopedKey::index_record(record.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(record),
            )
            .expect("canonical record stages");
        }
        seed.commit().await.expect("canonical records commit");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("graph transaction opens");
        let counted = CountingCatalogRead {
            transaction: &transaction,
            scans: AtomicUsize::new(0),
        };
        let catalog = MutationIndexCatalog::load(&counted, scope)
            .await
            .expect("one canonical catalog loads");
        assert_eq!(counted.scans.load(Ordering::Relaxed), 1);
        let (active, secondary, vector, text, routes) = catalog.into_components();
        assert_eq!(active.generations().len(), 3);
        assert_eq!(secondary.catalog_entry_count(), 2);
        assert_eq!(vector.catalog_entry_count(), 2);
        assert_eq!(text.catalog_entry_count(), 2);
        assert!(matches!(
            routes.targets_for(&crate::index_lifecycle::graph_mutation::GraphMutationTransition::create(
                scope,
                crate::index_lifecycle::graph_mutation::GraphEntity::node(1),
                crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::new(vec![
                    crate::encoding::v1::property::Property::string("$label", "TextActive"),
                    crate::encoding::v1::property::Property::string("body", "routed"),
                ]),
            )),
            RoutedMutationTargets::One(targets)
                if targets == [MutationRouteTarget::TextActive(0)]
        ));
        let vector_row = crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::new(vec![
            crate::encoding::v1::property::Property::string("$label", "VectorActive"),
            crate::encoding::v1::property::Property::string("embedding", "before"),
            crate::encoding::v1::property::Property::string("unrelated", "before"),
        ]);
        let crate::index_lifecycle::graph_mutation::PropertyEditOutcome::Changed(unrelated) =
            crate::index_lifecycle::graph_mutation::GraphMutationTransition::edit(
                scope,
                crate::index_lifecycle::graph_mutation::GraphEntity::node(2),
                vector_row.clone(),
                crate::index_lifecycle::graph_mutation::PropertyEdit::set(
                    crate::encoding::v1::property::Property::string("unrelated", "after"),
                ),
            )
        else {
            panic!("unrelated fixture changes")
        };
        assert!(matches!(
            routes.targets_for(&unrelated),
            RoutedMutationTargets::None
        ));
        let crate::index_lifecycle::graph_mutation::PropertyEditOutcome::Changed(vector_change) =
            crate::index_lifecycle::graph_mutation::GraphMutationTransition::edit(
                scope,
                crate::index_lifecycle::graph_mutation::GraphEntity::node(2),
                vector_row,
                crate::index_lifecycle::graph_mutation::PropertyEdit::set(
                    crate::encoding::v1::property::Property::string("embedding", "after"),
                ),
            )
        else {
            panic!("vector fixture changes")
        };
        assert!(matches!(
            routes.targets_for(&vector_change),
            RoutedMutationTargets::One(targets)
                if matches!(targets, [MutationRouteTarget::Vector(_)])
        ));
        assert!(active.generations().iter().all(|handle| records
            .iter()
            .any(|record| handle.matches_record(scope, record))));
        for handle in active.generations() {
            assert_eq!(active.handle(handle.identity()), Some(handle));
        }
        transaction.rollback();
        db.close().await.expect("fixture database closes");
    }
}

#[cfg(test)]
#[path = "../../tests/unit/index_lifecycle_mutation_catalog_contracts.rs"]
mod external_contracts;
