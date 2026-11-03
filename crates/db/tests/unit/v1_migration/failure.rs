//! Fail-closed and recoverable V1 secondary migration fixtures.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::SecondaryIndexDefinition;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, encode_properties, Property};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key, NodePropertyKey};
use crate::index_lifecycle::secondary::lookup_active_equality_generation;
use crate::index_lifecycle::{
    IndexOperationBlockerCode, IndexOperationStatus, IndexStateV2, ValidatedDynamicIndexDefinition,
};
use crate::{HelixDB, HelixDbError};

use super::semantics::{V1ElementKind, V1EqualityQueryObservation, V1OracleValue, V1SemanticRow};

const UNIQUE_LABEL: &str = "UniqueUser";
const UNIQUE_PROPERTY: &str = "key";
const UNIQUE_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Unique migration success, deterministic blockers, and repair evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1UniqueMigrationObservation {
    /// Authoritative distinct-value source rows.
    pub success_rows: Vec<V1SemanticRow>,
    /// Current unique lookups for every distinct value.
    pub success_queries: Vec<V1EqualityQueryObservation>,
    /// Exact numeric duplicates produced a uniqueness blocker.
    pub exact_numeric_duplicate_blocked: bool,
    /// Reopen reproduced the exact operation, index, generation, and error.
    pub exact_numeric_blocker_stable: bool,
    /// The failed source and legacy catalog remained intact without readiness.
    pub exact_numeric_failure_preserved: bool,
    /// Repair plus production retry resumed the same generation to Active.
    pub repaired_same_generation_active: bool,
    /// Signed zero produced a stable uniqueness blocker.
    pub signed_zero_duplicate_blocked: bool,
}

/// One unsupported range source and its recoverability evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1RangeFailureCaseObservation {
    /// Stable property-value shape.
    pub value_type: &'static str,
    /// Reopen retained the same InvalidSourceData operation and generation.
    pub blocker_stable: bool,
    /// Exact graph/catalog source remained and readiness stayed absent.
    pub failure_preserved: bool,
    /// Repair and retry activated the same index generation.
    pub repaired_same_generation_active: bool,
}

/// Missing-property success and every unsupported range failure shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1RangeFailureMigrationObservation {
    /// Missing properties produced no physical row and did not block.
    pub missing_property_active_without_row: bool,
    /// Unsupported, NaN, and oversized source results.
    pub cases: Vec<V1RangeFailureCaseObservation>,
}

/// Malformed legacy catalog preservation and repair evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1MalformedCatalogObservation {
    /// The key/value identity mismatch failed with catalog corruption.
    pub typed_catalog_error: bool,
    /// Reopen reproduced the same typed error text.
    pub blocker_stable: bool,
    /// The exact malformed key/value pair remained and readiness stayed absent.
    pub source_preserved: bool,
    /// Replacing only the malformed value resumed migration successfully.
    pub repaired_to_active: bool,
}

fn definition() -> ValidatedDynamicIndexDefinition {
    SecondaryIndexDefinition::node_unique_equality(UNIQUE_LABEL, UNIQUE_PROPERTY)
        .expect("unique migration definition validates")
        .try_into()
        .expect("unique migration definition converts")
}

async fn seed(
    database: &str,
    store: Arc<dyn ObjectStore>,
    values: &[V1OracleValue],
    node_base: u64,
) -> Vec<V1SemanticRow> {
    let definition = definition();
    let raw = super::raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("unique migration seed transaction opens");
    let mut rows = Vec::with_capacity(values.len());
    for (offset, value) in values.iter().enumerate() {
        let node_id = node_base + u64::try_from(offset).expect("unique fixture offset fits u64");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
                encode_properties(&[
                    Property::string("$label", UNIQUE_LABEL),
                    Property::new(UNIQUE_PROPERTY, value.to_stored()),
                ]),
            )
            .expect("unique source row stages");
        crate::search::add_to_equality_index_scoped(
            &transaction,
            "$label",
            UNIQUE_LABEL,
            node_id,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("unique source label row stages");
        rows.push(V1SemanticRow {
            element_kind: V1ElementKind::Node,
            entity_id: node_id,
            value: Some(value.clone()),
        });
    }
    let (key, value) = crate::migrations::migration_parity_legacy_catalog_row(&definition, false)
        .expect("unique legacy definition encodes");
    transaction
        .put(key, value)
        .expect("unique legacy definition stages");
    transaction
        .commit()
        .await
        .expect("unique migration seed commits");
    raw.close().await.expect("unique migration source closes");
    rows
}

async fn open(database: String, store: Arc<dyn ObjectStore>) -> Result<HelixDB, HelixDbError> {
    tokio::time::timeout(
        UNIQUE_OPEN_TIMEOUT,
        HelixDB::open_with_object_store_for_migration_parity(
            database,
            store,
            super::one_row_config(),
        ),
    )
    .await
    .expect("unique migration writer open must terminate")
}

async fn queries(db: &HelixDB, values: &[V1OracleValue]) -> Vec<V1EqualityQueryObservation> {
    let handle = super::semantics::secondary_handle(db, V1ElementKind::Node, UNIQUE_PROPERTY, None);
    let mut queries = Vec::with_capacity(values.len());
    for value in values {
        let actual_ids =
            lookup_active_equality_generation(db.inner_db().as_ref(), &handle, &value.to_stored())
                .await
                .expect("unique migrated equality lookup succeeds")
                .iter()
                .collect();
        queries.push(V1EqualityQueryObservation {
            element_kind: V1ElementKind::Node,
            value: value.clone(),
            actual_ids,
        });
    }
    queries
}

struct BlockedUnique {
    index_id: u64,
    generation: u64,
    operation_id: crate::index_lifecycle::IndexOperationId,
    source_preserved: bool,
    readiness_absent: bool,
}

async fn inspect_blocked(
    database: &str,
    store: Arc<dyn ObjectStore>,
    expected_values: &[V1OracleValue],
    node_base: u64,
) -> BlockedUnique {
    let definition = definition();
    let raw = super::raw(database, store).await;
    let record = crate::index_lifecycle::repository::load_index_record(
        &raw,
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("blocked unique record reads")
    .expect("blocked unique record exists");
    let IndexStateV2::Building {
        physical,
        build_operation_id,
    } = record.state()
    else {
        panic!("unique conflict retains a Building generation")
    };
    let operation = crate::index_lifecycle::outbox::read_operation(
        &raw,
        DataScope::LegacyUnscoped,
        *build_operation_id,
    )
    .await
    .expect("blocked unique operation reads")
    .expect("blocked unique operation exists");
    assert!(matches!(
        IndexOperationStatus::from_record(&operation),
        IndexOperationStatus::Blocked {
            blocker_code: IndexOperationBlockerCode::UniquenessViolation,
            ..
        }
    ));

    let mut source_preserved = !super::legacy_catalog_empty_raw(&raw).await;
    for (offset, expected) in expected_values.iter().enumerate() {
        let node_id = node_base + u64::try_from(offset).expect("unique fixture offset fits u64");
        let properties = raw
            .get(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
            )
            .await
            .expect("blocked unique source reads")
            .and_then(|value| decode_properties(&value).ok());
        source_preserved &= properties
            == Some(vec![
                Property::string("$label", UNIQUE_LABEL),
                Property::new(UNIQUE_PROPERTY, expected.to_stored()),
            ]);
    }
    let readiness_absent =
        !crate::migrations::index_v2_migration_ready(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("blocked unique index readiness reads")
            && !crate::migrations::storage_schema_complete(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("blocked unique storage readiness reads");
    let blocked = BlockedUnique {
        index_id: record.index_id().get(),
        generation: physical.generation().get(),
        operation_id: *build_operation_id,
        source_preserved,
        readiness_absent,
    };
    raw.close().await.expect("blocked unique inspection closes");
    blocked
}

async fn repair_and_retry(
    database: &str,
    store: Arc<dyn ObjectStore>,
    node_id: u64,
    repaired: &V1OracleValue,
    operation_id: crate::index_lifecycle::IndexOperationId,
) {
    let raw = super::raw(database, store).await;
    raw.put(
        Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
        }
        .to_bytes(),
        encode_properties(&[
            Property::string("$label", UNIQUE_LABEL),
            Property::new(UNIQUE_PROPERTY, repaired.to_stored()),
        ]),
    )
    .await
    .expect("unique repair writes authoritative source");
    crate::index_lifecycle::outbox::retry_operation(&raw, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("unique repair requeues blocked production operation");
    raw.close().await.expect("unique repair database closes");
}

async fn failure_case(
    name: &str,
    values: Vec<V1OracleValue>,
    node_base: u64,
    repair: Option<V1OracleValue>,
) -> (bool, bool, bool, bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!("{name}-{}", uuid::Uuid::new_v4());
    seed(&database, Arc::clone(&store), &values, node_base).await;
    let first_error = open(database.clone(), Arc::clone(&store))
        .await
        .err()
        .expect("semantic duplicate blocks V1 writer open");
    assert!(matches!(
        first_error,
        HelixDbError::MigrationRequired { .. }
    ));
    let first = inspect_blocked(&database, Arc::clone(&store), &values, node_base).await;

    let second_error = open(database.clone(), Arc::clone(&store))
        .await
        .err()
        .expect("blocked unique migration remains fail closed");
    let second = inspect_blocked(&database, Arc::clone(&store), &values, node_base).await;
    let stable = first_error.to_string() == second_error.to_string()
        && first.index_id == second.index_id
        && first.generation == second.generation
        && first.operation_id == second.operation_id;
    let preserved = first.source_preserved
        && second.source_preserved
        && first.readiness_absent
        && second.readiness_absent;

    let Some(repair) = repair else {
        return (true, stable, preserved, false);
    };
    repair_and_retry(
        &database,
        Arc::clone(&store),
        node_base + 1,
        &repair,
        second.operation_id,
    )
    .await;
    let recovered = open(database, store)
        .await
        .expect("repaired unique migration resumes");
    let record = crate::index_lifecycle::repository::load_index_record(
        recovered.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &definition().identity(),
    )
    .await
    .expect("repaired unique record reads")
    .expect("repaired unique record exists");
    let same_generation = record.index_id().get() == first.index_id
        && matches!(
            record.state(),
            IndexStateV2::Active { physical, .. }
                if physical.generation().get() == first.generation
        )
        && super::legacy_catalog_empty(&recovered).await;
    recovered
        .close()
        .await
        .expect("repaired unique writer closes");
    (true, stable, preserved, same_generation)
}

/// Proves typed unique values migrate exactly, semantic duplicates block
/// deterministically, and a repaired source resumes the same generation.
pub async fn v1_unique_failure_preservation_contract() -> V1UniqueMigrationObservation {
    let success_values = vec![
        V1OracleValue::Bool(true),
        V1OracleValue::String("true".to_string()),
        V1OracleValue::I64(42),
        V1OracleValue::String("42".to_string()),
        V1OracleValue::I64Array(vec![1, 2]),
        V1OracleValue::I64Array(vec![8, 9]),
        V1OracleValue::I64(9_007_199_254_740_993),
        V1OracleValue::F64Bits(9_007_199_254_740_992.0_f64.to_bits()),
    ];
    let success_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let success_database = format!("v1-unique-success-{}", uuid::Uuid::new_v4());
    let success_rows = seed(
        &success_database,
        Arc::clone(&success_store),
        &success_values,
        70_000,
    )
    .await;
    let success = open(success_database, success_store)
        .await
        .expect("distinct typed unique migration succeeds");
    let success_queries = queries(&success, &success_values).await;
    success.close().await.expect("unique success writer closes");

    let (exact_blocked, exact_stable, exact_preserved, repaired) = failure_case(
        "v1-unique-exact-numeric-conflict",
        vec![
            V1OracleValue::I64(9_007_199_254_740_992),
            V1OracleValue::F64Bits(9_007_199_254_740_992.0_f64.to_bits()),
        ],
        71_000,
        Some(V1OracleValue::I64(9_007_199_254_740_993)),
    )
    .await;
    let (zero_blocked, zero_stable, zero_preserved, _) = failure_case(
        "v1-unique-signed-zero-conflict",
        vec![
            V1OracleValue::F64Bits(0.0_f64.to_bits()),
            V1OracleValue::F64Bits((-0.0_f64).to_bits()),
        ],
        72_000,
        None,
    )
    .await;

    V1UniqueMigrationObservation {
        success_rows,
        success_queries,
        exact_numeric_duplicate_blocked: exact_blocked,
        exact_numeric_blocker_stable: exact_stable,
        exact_numeric_failure_preserved: exact_preserved,
        repaired_same_generation_active: repaired,
        signed_zero_duplicate_blocked: zero_blocked && zero_stable && zero_preserved,
    }
}

const RANGE_FAILURE_LABEL: &str = "RangeFailure";
const RANGE_FAILURE_PROPERTY: &str = "ordered";

fn range_failure_definition() -> ValidatedDynamicIndexDefinition {
    SecondaryIndexDefinition::node_range(RANGE_FAILURE_LABEL, RANGE_FAILURE_PROPERTY)
        .expect("range failure definition validates")
        .try_into()
        .expect("range failure definition converts")
}

async fn seed_range_failure(
    database: &str,
    store: Arc<dyn ObjectStore>,
    node_id: u64,
    value: Option<&PropertyValue>,
) -> Bytes {
    let raw = super::raw(database, store).await;
    let mut properties = vec![Property::string("$label", RANGE_FAILURE_LABEL)];
    if let Some(value) = value {
        properties.push(Property::new(RANGE_FAILURE_PROPERTY, value.clone()));
    }
    let encoded = encode_properties(&properties);
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("range failure seed transaction opens");
    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
            }
            .to_bytes(),
            encoded.clone(),
        )
        .expect("range failure source row stages");
    crate::search::add_to_equality_index_scoped(
        &transaction,
        "$label",
        RANGE_FAILURE_LABEL,
        node_id,
        DataScope::LegacyUnscoped,
    )
    .await
    .expect("range failure label row stages");
    let (key, value) =
        crate::migrations::migration_parity_legacy_catalog_row(&range_failure_definition(), false)
            .expect("range failure legacy definition encodes");
    transaction
        .put(key, value)
        .expect("range failure legacy definition stages");
    transaction
        .commit()
        .await
        .expect("range failure seed commits");
    raw.close().await.expect("range failure source closes");
    encoded
}

async fn inspect_blocked_range(
    database: &str,
    store: Arc<dyn ObjectStore>,
    node_id: u64,
    expected_source: &Bytes,
) -> BlockedUnique {
    let definition = range_failure_definition();
    let raw = super::raw(database, store).await;
    let record = crate::index_lifecycle::repository::load_index_record(
        &raw,
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("blocked range record reads")
    .expect("blocked range record exists");
    let IndexStateV2::Building {
        physical,
        build_operation_id,
    } = record.state()
    else {
        panic!("unsupported range source retains a Building generation")
    };
    let operation = crate::index_lifecycle::outbox::read_operation(
        &raw,
        DataScope::LegacyUnscoped,
        *build_operation_id,
    )
    .await
    .expect("blocked range operation reads")
    .expect("blocked range operation exists");
    assert!(matches!(
        IndexOperationStatus::from_record(&operation),
        IndexOperationStatus::Blocked {
            blocker_code: IndexOperationBlockerCode::InvalidSourceData,
            ..
        }
    ));
    let source_preserved = raw
        .get(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
            }
            .to_bytes(),
        )
        .await
        .expect("blocked range source reads")
        .as_ref()
        == Some(expected_source)
        && !super::legacy_catalog_empty_raw(&raw).await;
    let readiness_absent =
        !crate::migrations::index_v2_migration_ready(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("blocked range index readiness reads")
            && !crate::migrations::storage_schema_complete(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("blocked range storage readiness reads");
    let blocked = BlockedUnique {
        index_id: record.index_id().get(),
        generation: physical.generation().get(),
        operation_id: *build_operation_id,
        source_preserved,
        readiness_absent,
    };
    raw.close().await.expect("blocked range inspection closes");
    blocked
}

async fn repair_range_and_retry(
    database: &str,
    store: Arc<dyn ObjectStore>,
    node_id: u64,
    operation_id: crate::index_lifecycle::IndexOperationId,
) {
    let raw = super::raw(database, store).await;
    raw.put(
        Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
        }
        .to_bytes(),
        encode_properties(&[
            Property::string("$label", RANGE_FAILURE_LABEL),
            Property::i64(RANGE_FAILURE_PROPERTY, 7),
        ]),
    )
    .await
    .expect("range repair writes authoritative source");
    crate::index_lifecycle::outbox::retry_operation(&raw, DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("range repair requeues blocked production operation");
    raw.close().await.expect("range repair database closes");
}

async fn range_failure_case(
    value_type: &'static str,
    value: PropertyValue,
    node_id: u64,
) -> V1RangeFailureCaseObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!("v1-range-failure-{value_type}-{}", uuid::Uuid::new_v4());
    let encoded_source =
        seed_range_failure(&database, Arc::clone(&store), node_id, Some(&value)).await;
    let first_error = open(database.clone(), Arc::clone(&store))
        .await
        .err()
        .expect("unsupported range source blocks V1 writer open");
    assert!(matches!(
        first_error,
        HelixDbError::MigrationRequired { .. }
    ));
    let first =
        inspect_blocked_range(&database, Arc::clone(&store), node_id, &encoded_source).await;
    let second_error = open(database.clone(), Arc::clone(&store))
        .await
        .err()
        .expect("blocked range migration remains fail closed");
    let second =
        inspect_blocked_range(&database, Arc::clone(&store), node_id, &encoded_source).await;
    let blocker_stable = first_error.to_string() == second_error.to_string()
        && first.index_id == second.index_id
        && first.generation == second.generation
        && first.operation_id == second.operation_id;
    let failure_preserved = first.source_preserved
        && second.source_preserved
        && first.readiness_absent
        && second.readiness_absent;

    repair_range_and_retry(&database, Arc::clone(&store), node_id, second.operation_id).await;
    let recovered = open(database, store)
        .await
        .expect("repaired range migration resumes");
    let record = crate::index_lifecycle::repository::load_index_record(
        recovered.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &range_failure_definition().identity(),
    )
    .await
    .expect("repaired range record reads")
    .expect("repaired range record exists");
    let handle = super::semantics::secondary_handle(
        &recovered,
        V1ElementKind::Node,
        RANGE_FAILURE_PROPERTY,
        Some(false),
    );
    let ids = crate::index_lifecycle::secondary::scan_active_range_generation(
        recovered.inner_db().as_ref(),
        &handle,
        None,
        None,
    )
    .await
    .expect("repaired range generation scans");
    let repaired_same_generation_active = record.index_id().get() == first.index_id
        && matches!(
            record.state(),
            IndexStateV2::Active { physical, .. }
                if physical.generation().get() == first.generation
        )
        && ids == vec![node_id]
        && super::legacy_catalog_empty(&recovered).await;
    recovered
        .close()
        .await
        .expect("repaired range writer closes");

    V1RangeFailureCaseObservation {
        value_type,
        blocker_stable,
        failure_preserved,
        repaired_same_generation_active,
    }
}

/// Proves missing range properties are omitted and every unsupported,
/// non-reflexive, or oversized source fails closed and resumes after repair.
pub async fn v1_range_failure_preservation_contract() -> V1RangeFailureMigrationObservation {
    let missing_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let missing_database = format!("v1-range-missing-{}", uuid::Uuid::new_v4());
    let missing_id = 80_000;
    seed_range_failure(
        &missing_database,
        Arc::clone(&missing_store),
        missing_id,
        None,
    )
    .await;
    let missing = open(missing_database, missing_store)
        .await
        .expect("missing range property does not block migration");
    let missing_handle = super::semantics::secondary_handle(
        &missing,
        V1ElementKind::Node,
        RANGE_FAILURE_PROPERTY,
        Some(false),
    );
    let missing_property_active_without_row =
        crate::index_lifecycle::secondary::scan_active_range_generation(
            missing.inner_db().as_ref(),
            &missing_handle,
            None,
            None,
        )
        .await
        .expect("missing-property range generation scans")
        .is_empty();
    missing.close().await.expect("missing range writer closes");

    let mut object = std::collections::BTreeMap::new();
    object.insert("nested".to_string(), PropertyValue::I64(1));
    let cases = vec![
        ("null", PropertyValue::Null),
        ("bool", PropertyValue::Bool(true)),
        ("bytes", PropertyValue::Bytes(vec![0, 0xFE, 0xFF])),
        ("i64_array", PropertyValue::I64Array(vec![1, 2])),
        ("f64_array", PropertyValue::F64Array(vec![1.0, 2.0])),
        ("f32_array", PropertyValue::F32Array(vec![1.0, 2.0])),
        (
            "string_array",
            PropertyValue::StringArray(vec!["a".to_string(), "b".to_string()]),
        ),
        (
            "array",
            PropertyValue::Array(vec![
                PropertyValue::I64(1),
                PropertyValue::String("two".to_string()),
            ]),
        ),
        ("object", PropertyValue::Object(object)),
        ("nan", PropertyValue::F64(f64::NAN)),
        (
            "oversized",
            PropertyValue::String(
                "x".repeat(crate::encoding::v1::property::range_value::MAX_RANGE_ENCODED_LEN),
            ),
        ),
    ];
    let mut observations = Vec::with_capacity(cases.len());
    for (offset, (value_type, value)) in cases.into_iter().enumerate() {
        observations.push(
            range_failure_case(
                value_type,
                value,
                81_000 + u64::try_from(offset).expect("range case offset fits u64"),
            )
            .await,
        );
    }

    V1RangeFailureMigrationObservation {
        missing_property_active_without_row,
        cases: observations,
    }
}

/// Proves a typed legacy catalog key/value identity mismatch fails closed,
/// survives cold reopen byte-for-byte, and resumes after source repair.
pub async fn v1_malformed_catalog_failure_preservation_contract() -> V1MalformedCatalogObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!("v1-malformed-catalog-{}", uuid::Uuid::new_v4());
    let expected: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("MalformedUser", "email")
            .expect("expected malformed-catalog definition validates")
            .try_into()
            .expect("expected malformed-catalog definition converts");
    let mismatched: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("MalformedUser", "other")
            .expect("mismatched malformed-catalog definition validates")
            .try_into()
            .expect("mismatched malformed-catalog definition converts");
    let (catalog_key, _) = crate::migrations::migration_parity_legacy_catalog_row(&expected, false)
        .expect("expected malformed-catalog row encodes");
    let (_, malformed_value) =
        crate::migrations::migration_parity_legacy_catalog_row(&mismatched, false)
            .expect("mismatched malformed-catalog row encodes");
    let raw = super::raw(&database, Arc::clone(&store)).await;
    raw.put(catalog_key.clone(), malformed_value.clone())
        .await
        .expect("malformed catalog row writes");
    raw.close().await.expect("malformed catalog seed closes");

    let Err(first) = open(database.clone(), Arc::clone(&store)).await else {
        panic!("catalog identity mismatch must fail writer-open");
    };
    let typed_catalog_error = matches!(first, HelixDbError::IndexCatalogCorruption(_));
    let first_text = first.to_string();
    let inspection = super::raw(&database, Arc::clone(&store)).await;
    let exact_source_preserved = inspection
        .get(&catalog_key)
        .await
        .expect("malformed catalog source reads")
        .as_ref()
        == Some(&malformed_value);
    let readiness_absent =
        !crate::migrations::index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("malformed catalog readiness reads");
    inspection
        .close()
        .await
        .expect("malformed catalog inspection closes");

    let Err(second) = open(database.clone(), Arc::clone(&store)).await else {
        panic!("catalog identity mismatch must recur on reopen");
    };
    let blocker_stable = matches!(second, HelixDbError::IndexCatalogCorruption(_))
        && second.to_string() == first_text;

    let (_, repaired_value) =
        crate::migrations::migration_parity_legacy_catalog_row(&expected, false)
            .expect("repaired malformed-catalog row encodes");
    let repair = super::raw(&database, Arc::clone(&store)).await;
    repair
        .put(catalog_key, repaired_value)
        .await
        .expect("malformed catalog repair writes");
    repair
        .close()
        .await
        .expect("malformed catalog repair closes");
    let recovered = open(database, store)
        .await
        .expect("repaired catalog source converges");
    let record = crate::index_lifecycle::repository::load_index_record(
        recovered.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &expected.identity(),
    )
    .await
    .expect("repaired catalog current record reads")
    .expect("repaired catalog current record exists");
    let repaired_to_active = matches!(record.state(), IndexStateV2::Active { .. })
        && super::legacy_catalog_empty(&recovered).await
        && crate::migrations::index_v2_migration_ready(
            recovered.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("repaired catalog readiness reads");
    recovered
        .close()
        .await
        .expect("repaired catalog writer closes");

    V1MalformedCatalogObservation {
        typed_catalog_error,
        blocker_stable,
        source_preserved: exact_source_preserved && readiness_absent,
        repaired_to_active,
    }
}
