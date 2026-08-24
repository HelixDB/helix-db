#![allow(deprecated)]

//! Feature-gated populated legacy-text migration fixtures.
//!
//! These helpers construct only deployed graph, catalog, text-metadata, and
//! blob representations. Migration itself always runs through the ordinary
//! writer bootstrap and Index V2 lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};

use crate::config::{
    MigrationBatchRows, MigrationTuning, SearchIndexBackfillLimits, SearchIndexBatchLimits,
    SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning, TextAnalyzerKind,
    TextElementType, TextIndexDefinition, ValidatedDynamicIndexDefinition,
};
use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::{self, property_value::PropertyValue, Property};
use crate::encoding::v2::keys::{
    DataKeyKind, EdgeEndpointsKey, EdgePropertyByIdKey, DataKey as Key, NodePropertyKey,
};
use crate::encoding::v2::legacy::text::{live_state, manifest, version_counter};
use crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue;
use crate::search;
use crate::search::text::{persist_documents_as_manifest, TextDocumentInput, TextIndexLiveState};
use crate::{DbConfig, Result};

const BODY_PROPERTY: &str = "body";
const TENANT_PROPERTY: &str = "tenant_id";
const TARGET_TENANT: &str = "tenant-a";
const OTHER_TENANT: &str = "tenant-b";
const MAX_TOKEN_LEN: usize = u16::MAX as usize - 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTextFixtureDocument {
    pub entity_id: u64,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct LegacyTextFixtureCase {
    pub definition: TextIndexDefinition,
    pub target_tenant: Option<String>,
    pub target_partition_bytes: Vec<u8>,
    pub documents: Vec<LegacyTextFixtureDocument>,
    pub other_partition_documents: Vec<LegacyTextFixtureDocument>,
    pub absent_entity_id: u64,
}

pub struct PopulatedLegacyTextFixture {
    pub database: String,
    pub store: Arc<dyn ObjectStore>,
    pub config: DbConfig,
    pub cases: Vec<LegacyTextFixtureCase>,
    pub legacy_manifest_key: Bytes,
    pub legacy_live_state_key: Bytes,
    pub legacy_txn_guard_key: Bytes,
    pub legacy_version_counter_key: Bytes,
    pub legacy_blob_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTextPhysicalEvidence {
    pub manifest_present: bool,
    pub live_state_present: bool,
    pub txn_guard_present: bool,
    pub version_counter_present: bool,
    pub blob_hashes: BTreeSet<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyTextSourceFixtureKind {
    MissingTenant,
    NullTenant,
    OversizedTenant,
    UnsupportedText,
    IntegerTenant,
    BooleanTenant,
    EmptyStringTenant,
    ArrayTenant,
    ObjectTenant,
}

impl LegacyTextSourceFixtureKind {
    pub const fn is_invalid(self) -> bool {
        matches!(
            self,
            Self::MissingTenant | Self::NullTenant | Self::OversizedTenant | Self::UnsupportedText
        )
    }
}

pub struct LegacyTextSourceFixture {
    pub migration: PopulatedLegacyTextFixture,
    pub kind: LegacyTextSourceFixtureKind,
    pub entity_id: u64,
    pub graph_key: Bytes,
    pub graph_value: Bytes,
    pub catalog_key: Bytes,
    pub catalog_value: Bytes,
    pub compatibility_partition_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTextSourceEvidence {
    pub graph_value: Option<Bytes>,
    pub catalog_value: Option<Bytes>,
    pub physical: LegacyTextPhysicalEvidence,
}

fn one_row_config() -> DbConfig {
    DbConfig::new()
        .with_migration_tuning(
            MigrationTuning::default().with_batch_rows(
                MigrationBatchRows::new(1).expect("one migration row is positive"),
            ),
        )
        .with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_batch_rows(
                SecondaryIndexLifecycleBatchRows::new(1).expect("one lifecycle row is positive"),
            ),
        )
}

async fn raw(database: &str, store: Arc<dyn ObjectStore>) -> Db {
    Db::builder(database, store)
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("legacy text fixture database opens")
}

fn graph_key(element_type: TextElementType, entity_id: u64) -> Bytes {
    Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: match element_type {
            TextElementType::Node => DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
            TextElementType::Edge => {
                DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity_id))
            }
        },
    }
    .to_bytes()
}

fn properties(
    definition: &TextIndexDefinition,
    text: Option<&str>,
    tenant: Option<&PropertyValue>,
) -> Bytes {
    let mut properties = vec![Property::string("$label", definition.label())];
    if let Some(text) = text {
        properties.push(Property::string(BODY_PROPERTY, text));
    }
    if let Some(tenant) = tenant {
        properties.push(Property::new(TENANT_PROPERTY, tenant.clone()));
    }
    property::encode_properties(&properties)
}

fn partition_bytes(tenant: Option<&PropertyValue>) -> Vec<u8> {
    let Some(tenant) = tenant else {
        return vec![0x01];
    };
    let encoded = property::encode_index_partition_value(tenant);
    let mut canonical = Vec::with_capacity(1 + core::mem::size_of::<u32>() + encoded.len());
    canonical.push(0x02);
    canonical.extend_from_slice(
        &u32::try_from(encoded.len())
            .expect("fixture tenant encoding fits u32")
            .to_be_bytes(),
    );
    canonical.extend_from_slice(&encoded);
    canonical
}

fn cases() -> Vec<LegacyTextFixtureCase> {
    let long_token = "x".repeat(MAX_TOKEN_LEN);
    let dropped_long_token = "y".repeat(MAX_TOKEN_LEN + 1);
    let mut cases = Vec::new();
    for element_type in [TextElementType::Node, TextElementType::Edge] {
        for partitioned in [false, true] {
            for analyzer in [
                TextAnalyzerKind::Standard,
                TextAnalyzerKind::StandardStemEn,
                TextAnalyzerKind::WhitespaceLowercase,
            ] {
                let element = match element_type {
                    TextElementType::Node => "Node",
                    TextElementType::Edge => "Edge",
                };
                let partition = if partitioned {
                    "Partitioned"
                } else {
                    "Unpartitioned"
                };
                let label = format!(
                    "Legacy{element}{partition}{}",
                    match analyzer {
                        TextAnalyzerKind::Standard => "Standard",
                        TextAnalyzerKind::StandardStemEn => "Stem",
                        TextAnalyzerKind::WhitespaceLowercase => "Whitespace",
                    }
                );
                let base = match element_type {
                    TextElementType::Node => 1_000,
                    TextElementType::Edge => 101_000,
                } + u64::try_from(cases.len()).expect("fixture case count fits u64")
                    * 100;
                let definition = match element_type {
                    TextElementType::Node => TextIndexDefinition::new_node(label, BODY_PROPERTY),
                    TextElementType::Edge => TextIndexDefinition::new_edge(label, BODY_PROPERTY),
                }
                .expect("fixture text definition is valid")
                .with_tenant_property_option(partitioned.then_some(TENANT_PROPERTY))
                .expect("fixture tenant property is valid")
                .with_analyzer(analyzer);
                let documents = vec![
                    LegacyTextFixtureDocument {
                        entity_id: base + 1,
                        text: "alpha alpha common tied".to_string(),
                    },
                    LegacyTextFixtureDocument {
                        entity_id: base + 2,
                        text: "alpha common tied".to_string(),
                    },
                    LegacyTextFixtureDocument {
                        entity_id: base + 3,
                        text: "beta running runners final".to_string(),
                    },
                    LegacyTextFixtureDocument {
                        entity_id: base + 4,
                        text: String::new(),
                    },
                    LegacyTextFixtureDocument {
                        entity_id: base + 5,
                        text: long_token.clone(),
                    },
                    LegacyTextFixtureDocument {
                        entity_id: base + 6,
                        text: dropped_long_token.clone(),
                    },
                ];
                let other_partition_documents = if partitioned {
                    vec![LegacyTextFixtureDocument {
                        entity_id: base + 7,
                        text: "alpha isolated other partition".to_string(),
                    }]
                } else {
                    Vec::new()
                };
                let tenant = partitioned.then(|| PropertyValue::String(TARGET_TENANT.to_string()));
                cases.push(LegacyTextFixtureCase {
                    definition,
                    target_tenant: partitioned.then(|| TARGET_TENANT.to_string()),
                    target_partition_bytes: partition_bytes(tenant.as_ref()),
                    documents,
                    other_partition_documents,
                    absent_entity_id: base + 8,
                });
            }
        }
    }
    cases
}

fn put_entity(
    transaction: &slatedb::DbTransaction,
    definition: &TextIndexDefinition,
    entity_id: u64,
    text: Option<&str>,
    tenant: Option<&PropertyValue>,
) {
    transaction
        .put(
            graph_key(definition.element_type(), entity_id),
            properties(definition, text, tenant),
        )
        .expect("legacy graph row stages");
    if definition.element_type() == TextElementType::Edge {
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(entity_id)),
                }
                .to_bytes(),
                EdgeEndpointsValue::new(entity_id + 1, entity_id + 2)
                    .encode(),
            )
            .expect("legacy edge endpoint stages");
    }
}

async fn seed_legacy_text_fixture(
    database_prefix: &str,
    cases: Vec<LegacyTextFixtureCase>,
) -> Result<PopulatedLegacyTextFixture> {
    let database = format!("{database_prefix}-{}", uuid::Uuid::new_v4());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source = raw(&database, Arc::clone(&store)).await;

    let initial = source.begin(IsolationLevel::SerializableSnapshot).await?;
    for case in &cases {
        let validated: ValidatedDynamicIndexDefinition = case.definition.clone().try_into()?;
        let (key, value) =
            crate::migrations::migration_parity_legacy_catalog_row(&validated, false)?;
        initial.put(key, value)?;
        let target_tenant = case
            .target_tenant
            .as_ref()
            .map(|tenant| PropertyValue::String(tenant.clone()));
        for document in &case.documents {
            let initial_text = if document.entity_id == case.documents[2].entity_id {
                "obsolete mutation history"
            } else {
                &document.text
            };
            let initial_tenant = if case.target_tenant.is_some()
                && document.entity_id == case.documents[2].entity_id
            {
                Some(PropertyValue::String("tenant-before".to_string()))
            } else {
                target_tenant.clone()
            };
            put_entity(
                &initial,
                &case.definition,
                document.entity_id,
                Some(initial_text),
                initial_tenant.as_ref(),
            );
        }
        for document in &case.other_partition_documents {
            put_entity(
                &initial,
                &case.definition,
                document.entity_id,
                Some(&document.text),
                Some(&PropertyValue::String(OTHER_TENANT.to_string())),
            );
        }
        put_entity(
            &initial,
            &case.definition,
            case.absent_entity_id,
            None,
            target_tenant.as_ref(),
        );
    }
    initial.commit().await?;

    let updates = source.begin(IsolationLevel::SerializableSnapshot).await?;
    for case in &cases {
        let target_tenant = case
            .target_tenant
            .as_ref()
            .map(|tenant| PropertyValue::String(tenant.clone()));
        put_entity(
            &updates,
            &case.definition,
            case.documents[2].entity_id,
            Some(&case.documents[2].text),
            target_tenant.as_ref(),
        );
        updates.delete(graph_key(
            case.definition.element_type(),
            case.documents[1].entity_id,
        ))?;
    }
    updates.commit().await?;

    let reinserts = source.begin(IsolationLevel::SerializableSnapshot).await?;
    for case in &cases {
        let target_tenant = case
            .target_tenant
            .as_ref()
            .map(|tenant| PropertyValue::String(tenant.clone()));
        put_entity(
            &reinserts,
            &case.definition,
            case.documents[1].entity_id,
            Some(&case.documents[1].text),
            target_tenant.as_ref(),
        );
    }
    reinserts.commit().await?;

    let legacy_definition = &cases[0].definition;
    let legacy_index_name = match legacy_definition.tenant_property() {
        Some(tenant_property) => search::text_tenant_index_name(
            legacy_definition.element_type(),
            legacy_definition.label(),
            legacy_definition.property(),
            tenant_property,
            &PropertyValue::String(TARGET_TENANT.to_string()),
        ),
        None => search::text_index_name(
            legacy_definition.element_type(),
            legacy_definition.label(),
            legacy_definition.property(),
        ),
    };
    let legacy_manifest = persist_documents_as_manifest(
        &store,
        &database,
        legacy_definition,
        &legacy_index_name,
        &[TextDocumentInput::new(
            cases[0].documents[0].entity_id,
            "legacy-only stale physical text",
        )],
    )
    .await?
    .expect("non-empty legacy fixture produces a manifest");
    let legacy_manifest_key = search::make_text_index_manifest_key(&legacy_index_name);
    let legacy_live_state_key =
        search::make_text_index_live_state_key(&legacy_index_name, cases[0].documents[0].entity_id);
    let legacy_txn_guard_key = search::make_text_index_txn_guard_key(&legacy_index_name);
    let legacy_version_counter_key =
        search::make_text_index_version_counter_key(&legacy_index_name);
    source
        .put(
            &legacy_manifest_key,
            manifest::encode_for_contract(&legacy_manifest).expect("legacy manifest encodes"),
        )
        .await?;
    source
        .put(
            &legacy_live_state_key,
            live_state::encode_for_retained_api(&TextIndexLiveState::live(1))
                .expect("legacy live state encodes"),
        )
        .await?;
    source
        .put(&legacy_txn_guard_key, Bytes::from_static(b"legacy-guard"))
        .await?;
    source
        .put(
            &legacy_version_counter_key,
            version_counter::encode_for_contract(
                NonZeroU64::new(1).expect("legacy version is positive"),
            )
            .expect("legacy version encodes"),
        )
        .await?;
    source.close().await?;

    Ok(PopulatedLegacyTextFixture {
        database,
        store,
        config: one_row_config(),
        cases,
        legacy_manifest_key,
        legacy_live_state_key,
        legacy_txn_guard_key,
        legacy_version_counter_key,
        legacy_blob_hash: legacy_manifest.primary_split_ref().blob.sha256,
    })
}

pub async fn seed_populated_legacy_text_fixture() -> Result<PopulatedLegacyTextFixture> {
    seed_legacy_text_fixture("legacy-populated-text", cases()).await
}

pub async fn seed_recovery_legacy_text_fixture() -> Result<PopulatedLegacyTextFixture> {
    let mut case = cases()
        .into_iter()
        .next()
        .expect("unpartitioned node standard fixture exists");
    case.documents.truncate(3);
    case.other_partition_documents.clear();
    seed_legacy_text_fixture("legacy-recovery-text", vec![case]).await
}

pub async fn seed_legacy_text_source_fixture(
    kind: LegacyTextSourceFixtureKind,
) -> Result<LegacyTextSourceFixture> {
    let mut case = cases()
        .into_iter()
        .find(|case| {
            case.definition.element_type() == TextElementType::Node
                && case.definition.tenant_property().is_some()
                && case.definition.analyzer() == TextAnalyzerKind::Standard
        })
        .expect("partitioned node standard fixture exists");
    case.documents.truncate(3);
    case.other_partition_documents.clear();
    let mut migration = seed_legacy_text_fixture("legacy-source-text", vec![case]).await?;
    if kind == LegacyTextSourceFixtureKind::OversizedTenant {
        let defaults = SearchIndexBackfillLimits::default();
        let batch = defaults.batch();
        let oversized_batch = SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            NonZeroU64::new(32 * 1024 * 1024).expect("oversized fixture input is positive"),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .expect("oversized fixture batch limits are valid");
        let limits = SearchIndexBackfillLimits::try_new(
            oversized_batch,
            defaults.edge_property_read_batch(),
            defaults.text_artifacts(),
            defaults.text_compaction(),
        )
        .expect("oversized fixture backfill limits are valid");
        migration.config = migration
            .config
            .clone()
            .with_search_index_backfill_limits(limits);
    }
    let case = &migration.cases[0];
    let entity_id = case.documents[0].entity_id;
    let graph_key = graph_key(case.definition.element_type(), entity_id);
    let (tenant, text): (Option<PropertyValue>, Option<PropertyValue>) = match kind {
        LegacyTextSourceFixtureKind::MissingTenant => (
            None,
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::NullTenant => (
            Some(PropertyValue::Null),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::OversizedTenant => (
            Some(PropertyValue::String("z".repeat(16 * 1024 * 1024 + 1))),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::UnsupportedText => (
            Some(PropertyValue::String(TARGET_TENANT.to_string())),
            Some(PropertyValue::I64(7)),
        ),
        LegacyTextSourceFixtureKind::IntegerTenant => (
            Some(PropertyValue::I64(7)),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::BooleanTenant => (
            Some(PropertyValue::Bool(true)),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::EmptyStringTenant => (
            Some(PropertyValue::String(String::new())),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::ArrayTenant => (
            Some(PropertyValue::Array(vec![
                PropertyValue::I64(7),
                PropertyValue::String("array".to_string()),
            ])),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
        LegacyTextSourceFixtureKind::ObjectTenant => (
            Some(PropertyValue::Object(BTreeMap::from([(
                "key".to_string(),
                PropertyValue::Bool(true),
            )]))),
            Some(PropertyValue::String(case.documents[0].text.clone())),
        ),
    };
    let mut source_properties = vec![Property::string("$label", case.definition.label())];
    if let Some(text) = text {
        source_properties.push(Property::new(BODY_PROPERTY, text));
    }
    if let Some(tenant) = &tenant {
        source_properties.push(Property::new(TENANT_PROPERTY, tenant.clone()));
    }
    let graph_value = property::encode_properties(&source_properties);
    let validated: ValidatedDynamicIndexDefinition = case.definition.clone().try_into()?;
    let (catalog_key, catalog_value) =
        crate::migrations::migration_parity_legacy_catalog_row(&validated, false)?;
    let source = raw(&migration.database, Arc::clone(&migration.store)).await;
    source.put(&graph_key, &graph_value).await?;
    source.close().await?;
    let compatibility_partition_bytes = tenant
        .as_ref()
        .filter(|_| !kind.is_invalid())
        .map(|tenant| partition_bytes(Some(tenant)));
    Ok(LegacyTextSourceFixture {
        migration,
        kind,
        entity_id,
        graph_key,
        graph_value,
        catalog_key,
        catalog_value,
        compatibility_partition_bytes,
    })
}

pub async fn inspect_legacy_text_source(
    fixture: &LegacyTextSourceFixture,
) -> Result<LegacyTextSourceEvidence> {
    let source = raw(
        &fixture.migration.database,
        Arc::clone(&fixture.migration.store),
    )
    .await;
    let graph_value = source.get(&fixture.graph_key).await?;
    let catalog_value = source.get(&fixture.catalog_key).await?;
    source.close().await?;
    Ok(LegacyTextSourceEvidence {
        graph_value,
        catalog_value,
        physical: inspect_legacy_text_physical_rows(&fixture.migration).await?,
    })
}

pub async fn repair_legacy_text_source(fixture: &LegacyTextSourceFixture) -> Result<()> {
    let case = &fixture.migration.cases[0];
    let source = raw(
        &fixture.migration.database,
        Arc::clone(&fixture.migration.store),
    )
    .await;
    source
        .put(
            &fixture.graph_key,
            properties(
                &case.definition,
                Some(&case.documents[0].text),
                Some(&PropertyValue::String(TARGET_TENANT.to_string())),
            ),
        )
        .await?;
    source.close().await?;
    Ok(())
}

pub async fn inspect_legacy_text_physical_rows(
    fixture: &PopulatedLegacyTextFixture,
) -> Result<LegacyTextPhysicalEvidence> {
    let source = raw(&fixture.database, Arc::clone(&fixture.store)).await;
    let evidence = LegacyTextPhysicalEvidence {
        manifest_present: source.get(&fixture.legacy_manifest_key).await?.is_some(),
        live_state_present: source.get(&fixture.legacy_live_state_key).await?.is_some(),
        txn_guard_present: source.get(&fixture.legacy_txn_guard_key).await?.is_some(),
        version_counter_present: source
            .get(&fixture.legacy_version_counter_key)
            .await?
            .is_some(),
        blob_hashes: search::text::list_blob_hashes(&fixture.store, &fixture.database)
            .await?
            .into_iter()
            .filter_map(|(path, _)| {
                let name = path.filename()?;
                let bytes = (0..32)
                    .map(|offset| u8::from_str_radix(&name[offset * 2..offset * 2 + 2], 16).ok())
                    .collect::<Option<Vec<_>>>()?;
                bytes.try_into().ok()
            })
            .collect(),
    };
    source.close().await?;
    Ok(evidence)
}
