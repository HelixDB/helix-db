//! Independent-observation fixtures for V1 secondary semantic migration.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use helix_ast::batch::read_batch;
use helix_ast::expr::Predicate;
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::traversal::{g, Order};

use crate::config::SecondaryIndexDefinition;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{
    DataKeyKind, EdgeEndpointsKey, EdgePropertyByIdKey, Key, NodePropertyKey,
};
use crate::encoding::v1::values::edge_endpoints::EdgeEndpointsValue;
use crate::index_lifecycle::secondary::{
    lookup_active_equality_generation, scan_active_range_generation, SecondaryRangeQuery,
};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexElementKind, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition,
};
use crate::HelixDB;

const SEMANTIC_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// Graph element kind used by independent migration observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum V1ElementKind {
    /// Node-backed source and index.
    Node,
    /// Edge-backed source and index.
    Edge,
}

/// Test-owned property value that exposes no production canonical encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V1OracleValue {
    /// Explicit null.
    Null,
    /// Boolean scalar.
    Bool(bool),
    /// Signed integer scalar.
    I64(i64),
    /// UTC datetime in milliseconds.
    DateTime(i64),
    /// IEEE-754 binary64 bits.
    F64Bits(u64),
    /// IEEE-754 binary32 bits.
    F32Bits(u32),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Signed integer array.
    I64Array(Vec<i64>),
    /// IEEE-754 binary64 array bits.
    F64ArrayBits(Vec<u64>),
    /// IEEE-754 binary32 array bits.
    F32ArrayBits(Vec<u32>),
    /// UTF-8 string array.
    StringArray(Vec<String>),
}

impl V1OracleValue {
    pub(super) fn to_stored(&self) -> PropertyValue {
        match self {
            Self::Null => PropertyValue::Null,
            Self::Bool(value) => PropertyValue::Bool(*value),
            Self::I64(value) => PropertyValue::I64(*value),
            Self::DateTime(value) => PropertyValue::DateTime(*value),
            Self::F64Bits(bits) => PropertyValue::F64(f64::from_bits(*bits)),
            Self::F32Bits(bits) => PropertyValue::F32(f64::from(f32::from_bits(*bits))),
            Self::String(value) => PropertyValue::String(value.clone()),
            Self::Bytes(value) => PropertyValue::Bytes(value.clone()),
            Self::I64Array(values) => PropertyValue::I64Array(values.clone()),
            Self::F64ArrayBits(values) => {
                PropertyValue::F64Array(values.iter().copied().map(f64::from_bits).collect())
            }
            Self::F32ArrayBits(values) => {
                PropertyValue::F32Array(values.iter().copied().map(f32::from_bits).collect())
            }
            Self::StringArray(values) => PropertyValue::StringArray(values.clone()),
        }
    }
}

/// Authoritative graph row consumed by the independent oracle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1SemanticRow {
    /// Node or edge ownership.
    pub element_kind: V1ElementKind,
    /// Stable graph entity ID.
    pub entity_id: u64,
    /// `None` represents a missing indexed property.
    pub value: Option<V1OracleValue>,
}

/// One equality query and the IDs served by the migrated index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1EqualityQueryObservation {
    /// Node or edge index.
    pub element_kind: V1ElementKind,
    /// Typed query value.
    pub value: V1OracleValue,
    /// IDs returned by production serving.
    pub actual_ids: Vec<u64>,
}

/// Equality evidence before and after cold reopen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1EqualityMigrationObservation {
    /// Graph-authoritative source rows.
    pub rows: Vec<V1SemanticRow>,
    /// Every typed equality query.
    pub queries: Vec<V1EqualityQueryObservation>,
    /// Whether cold reopen retained every result.
    pub cold_reopen_identical: bool,
}

/// Inclusive or exclusive independent range bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V1RangeBound {
    /// Includes equal values.
    Inclusive(V1OracleValue),
    /// Excludes equal values.
    Exclusive(V1OracleValue),
}

impl V1RangeBound {
    fn value(&self) -> PropertyValue {
        match self {
            Self::Inclusive(value) | Self::Exclusive(value) => value.to_stored(),
        }
    }

    const fn inclusive(&self) -> bool {
        matches!(self, Self::Inclusive(_))
    }
}

/// Semantic direction requested from a migrated range index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V1RangeDirection {
    /// Smallest admitted value first.
    Ascending,
    /// Largest admitted value first.
    Descending,
}

/// Production serving surface used for one observed range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V1RangeAccess {
    /// Generation-qualified storage scan.
    Direct,
    /// Planner query with literal bounds.
    Literal,
    /// Planner query with runtime parameter bounds.
    Parameter,
}

/// One bounded range query and the IDs served by production.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1RangeCaseObservation {
    /// Node or edge index.
    pub element_kind: V1ElementKind,
    /// Physical and semantic index direction.
    pub direction: V1RangeDirection,
    /// Production serving surface.
    pub access: V1RangeAccess,
    /// Optional lower bound.
    pub lower: Option<V1RangeBound>,
    /// Optional upper bound.
    pub upper: Option<V1RangeBound>,
    /// Optional result cap.
    pub limit: Option<u32>,
    /// IDs returned by production serving.
    pub actual_ids: Vec<u64>,
}

/// Range evidence before and after cold reopen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1RangeMigrationObservation {
    /// Graph-authoritative source rows, including missing properties.
    pub rows: Vec<V1SemanticRow>,
    /// Every bound form, direction, element kind, domain, and limit.
    pub cases: Vec<V1RangeCaseObservation>,
    /// Whether cold reopen retained every result.
    pub cold_reopen_identical: bool,
}

fn equality_values() -> Vec<Option<V1OracleValue>> {
    vec![
        Some(V1OracleValue::Bool(true)),
        Some(V1OracleValue::String("true".to_string())),
        Some(V1OracleValue::I64(42)),
        Some(V1OracleValue::F32Bits(42.0_f32.to_bits())),
        Some(V1OracleValue::String("42".to_string())),
        Some(V1OracleValue::Bytes(vec![1, 2])),
        Some(V1OracleValue::String("[1, 2]".to_string())),
        Some(V1OracleValue::Null),
        None,
        Some(V1OracleValue::String("null".to_string())),
        Some(V1OracleValue::I64Array(vec![1, 2])),
        Some(V1OracleValue::I64Array(vec![8, 9])),
        Some(V1OracleValue::F64Bits(
            9_007_199_254_740_992.0_f64.to_bits(),
        )),
        Some(V1OracleValue::I64(9_007_199_254_740_992)),
        Some(V1OracleValue::I64(9_007_199_254_740_993)),
        Some(V1OracleValue::F64Bits(0.0_f64.to_bits())),
        Some(V1OracleValue::F64Bits((-0.0_f64).to_bits())),
        Some(V1OracleValue::F64Bits(f64::NAN.to_bits())),
        Some(V1OracleValue::F64Bits(f64::INFINITY.to_bits())),
        Some(V1OracleValue::F64Bits(f64::NEG_INFINITY.to_bits())),
        Some(V1OracleValue::DateTime(1_700_000_000_000)),
        Some(V1OracleValue::String("2023-11-14T22:13:20Z".to_string())),
        Some(V1OracleValue::F64ArrayBits(vec![(-0.0_f64).to_bits()])),
        Some(V1OracleValue::F64ArrayBits(vec![0.0_f64.to_bits()])),
        Some(V1OracleValue::F64ArrayBits(vec![f64::NAN.to_bits()])),
        Some(V1OracleValue::F32ArrayBits(vec![1.0_f32.to_bits()])),
        Some(V1OracleValue::StringArray(vec![
            "a".to_string(),
            "b".to_string(),
        ])),
    ]
}

fn range_values() -> Vec<Option<V1OracleValue>> {
    vec![
        Some(V1OracleValue::F64Bits(f64::NEG_INFINITY.to_bits())),
        Some(V1OracleValue::I64(i64::MIN)),
        Some(V1OracleValue::I64(-1024)),
        Some(V1OracleValue::F64Bits((-1.5_f64).to_bits())),
        Some(V1OracleValue::I64(-1)),
        Some(V1OracleValue::F64Bits((-f64::MIN_POSITIVE).to_bits())),
        Some(V1OracleValue::F64Bits((-f64::from_bits(1)).to_bits())),
        Some(V1OracleValue::F64Bits((-0.0_f64).to_bits())),
        Some(V1OracleValue::I64(0)),
        Some(V1OracleValue::F64Bits(0.0_f64.to_bits())),
        Some(V1OracleValue::F64Bits(f64::from_bits(1).to_bits())),
        Some(V1OracleValue::F64Bits(f64::MIN_POSITIVE.to_bits())),
        Some(V1OracleValue::F64Bits(1.0_f64.to_bits())),
        Some(V1OracleValue::F64Bits(
            f64::from_bits(1.0_f64.to_bits() + 1).to_bits(),
        )),
        Some(V1OracleValue::F32Bits(42.0_f32.to_bits())),
        Some(V1OracleValue::F64Bits(
            f64::from_bits(1024.0_f64.to_bits() - 1).to_bits(),
        )),
        Some(V1OracleValue::F64Bits(1024.0_f64.to_bits())),
        Some(V1OracleValue::F64Bits(
            9_007_199_254_740_992.0_f64.to_bits(),
        )),
        Some(V1OracleValue::I64(9_007_199_254_740_992)),
        Some(V1OracleValue::I64(9_007_199_254_740_993)),
        Some(V1OracleValue::I64(i64::MAX)),
        Some(V1OracleValue::F64Bits(f64::INFINITY.to_bits())),
        Some(V1OracleValue::DateTime(i64::MIN)),
        Some(V1OracleValue::DateTime(0)),
        Some(V1OracleValue::DateTime(i64::MAX)),
        Some(V1OracleValue::String(String::new())),
        Some(V1OracleValue::String("\0".to_string())),
        Some(V1OracleValue::String("a".to_string())),
        Some(V1OracleValue::String("a\0".to_string())),
        Some(V1OracleValue::String("aa".to_string())),
        Some(V1OracleValue::String("aaa".to_string())),
        None,
    ]
}

fn definitions(
    label_node: &str,
    label_edge: &str,
    properties: &[(&str, bool)],
) -> Vec<ValidatedDynamicIndexDefinition> {
    let mut definitions = Vec::new();
    for (property, descending) in properties {
        let node = if *descending {
            SecondaryIndexDefinition::node_range_desc(label_node, *property)
        } else {
            SecondaryIndexDefinition::node_range(label_node, *property)
        };
        let edge = if *descending {
            SecondaryIndexDefinition::edge_range_desc(label_edge, *property)
        } else {
            SecondaryIndexDefinition::edge_range(label_edge, *property)
        };
        definitions.push(
            node.expect("node range definition validates")
                .try_into()
                .expect("node range definition converts"),
        );
        definitions.push(
            edge.expect("edge range definition validates")
                .try_into()
                .expect("edge range definition converts"),
        );
    }
    definitions
}

struct SemanticSeed<'a> {
    node_label: &'a str,
    edge_label: &'a str,
    properties: &'a [&'a str],
    values: &'a [Option<V1OracleValue>],
    definitions: &'a [ValidatedDynamicIndexDefinition],
    node_base: u64,
    edge_base: u64,
}

async fn seed_semantic_rows(
    database: &str,
    store: Arc<dyn ObjectStore>,
    seed: SemanticSeed<'_>,
) -> Vec<V1SemanticRow> {
    let raw = super::raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("semantic V1 seed transaction opens");
    let mut rows = Vec::with_capacity(seed.values.len() * 2);
    for (offset, value) in seed.values.iter().enumerate() {
        let offset = u64::try_from(offset).expect("fixture offset fits u64");
        let node_id = seed.node_base + offset;
        let edge_id = seed.edge_base + offset;
        let mut node_properties = vec![Property::string("$label", seed.node_label)];
        let mut edge_properties = vec![Property::string("$label", seed.edge_label)];
        if let Some(value) = value {
            for property in seed.properties {
                node_properties.push(Property::new(*property, value.to_stored()));
                edge_properties.push(Property::new(*property, value.to_stored()));
            }
        }
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
                encode_properties(&node_properties),
            )
            .expect("semantic V1 node row stages");
        crate::search::add_to_equality_index_scoped(
            &transaction,
            "$label",
            seed.node_label,
            node_id,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("semantic V1 node label row stages");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
                }
                .to_bytes(),
                EdgeEndpointsValue::new(seed.node_base, seed.node_base + 1).encode(),
            )
            .expect("semantic V1 edge endpoints stage");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
                }
                .to_bytes(),
                encode_properties(&edge_properties),
            )
            .expect("semantic V1 edge property row stages");
        crate::search::add_to_global_edge_label_index_scoped(
            &transaction,
            seed.edge_label,
            edge_id,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("semantic V1 global edge label row stages");
        crate::search::add_to_edge_label_index_scoped(
            &transaction,
            seed.node_base,
            seed.node_base + 1,
            seed.edge_label,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("semantic V1 edge-neighbor label rows stage");
        rows.push(V1SemanticRow {
            element_kind: V1ElementKind::Node,
            entity_id: node_id,
            value: value.clone(),
        });
        rows.push(V1SemanticRow {
            element_kind: V1ElementKind::Edge,
            entity_id: edge_id,
            value: value.clone(),
        });
    }
    for definition in seed.definitions {
        let (key, value) =
            crate::migrations::migration_parity_legacy_catalog_row(definition, false)
                .expect("semantic V1 catalog row encodes");
        transaction
            .put(key, value)
            .expect("semantic V1 catalog row stages");
    }
    transaction.commit().await.expect("semantic V1 rows commit");
    raw.close().await.expect("semantic V1 raw database closes");
    rows
}

pub(super) fn secondary_handle(
    db: &HelixDB,
    element_kind: V1ElementKind,
    property: &str,
    descending: Option<bool>,
) -> ActiveIndexHandle {
    db.active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| {
            let Some(definition) = handle.secondary_definition() else {
                return false;
            };
            let expected_kind = match element_kind {
                V1ElementKind::Node => IndexElementKind::Node,
                V1ElementKind::Edge => IndexElementKind::Edge,
            };
            if definition.element_kind() != expected_kind
                || definition.property().as_str() != property
            {
                return false;
            }
            match (definition, descending) {
                (
                    ValidatedSecondaryIndexDefinition::NodeEquality { .. }
                    | ValidatedSecondaryIndexDefinition::EdgeEquality { .. },
                    None,
                ) => true,
                (
                    ValidatedSecondaryIndexDefinition::NodeRange { direction, .. }
                    | ValidatedSecondaryIndexDefinition::EdgeRange { direction, .. },
                    Some(descending),
                ) => (*direction == crate::config::RangeIndexDirection::Desc) == descending,
                _ => false,
            }
        })
        .expect("semantic V1 index is Active")
}

async fn equality_queries(
    db: &HelixDB,
    values: &[Option<V1OracleValue>],
) -> Vec<V1EqualityQueryObservation> {
    let mut queries = Vec::new();
    for element_kind in [V1ElementKind::Node, V1ElementKind::Edge] {
        let handle = secondary_handle(db, element_kind, "value", None);
        for value in values.iter().flatten() {
            let actual = lookup_active_equality_generation(
                db.inner_db().as_ref(),
                &handle,
                &value.to_stored(),
            )
            .await
            .expect("migrated equality lookup succeeds")
            .iter()
            .collect();
            queries.push(V1EqualityQueryObservation {
                element_kind,
                value: value.clone(),
                actual_ids: actual,
            });
        }
    }
    queries
}

fn base_range_cases() -> Vec<(Option<V1RangeBound>, Option<V1RangeBound>, Option<u32>)> {
    let i64_value = |value| V1OracleValue::I64(value);
    let datetime = |value| V1OracleValue::DateTime(value);
    let string = |value: &str| V1OracleValue::String(value.to_string());
    vec![
        (None, None, None),
        (None, None, Some(7)),
        (Some(V1RangeBound::Inclusive(i64_value(0))), None, None),
        (Some(V1RangeBound::Exclusive(i64_value(0))), None, None),
        (None, Some(V1RangeBound::Inclusive(i64_value(1))), None),
        (None, Some(V1RangeBound::Exclusive(i64_value(1))), None),
        (
            Some(V1RangeBound::Inclusive(i64_value(-1))),
            Some(V1RangeBound::Inclusive(i64_value(1))),
            None,
        ),
        (
            Some(V1RangeBound::Inclusive(i64_value(-1))),
            Some(V1RangeBound::Exclusive(i64_value(1))),
            None,
        ),
        (
            Some(V1RangeBound::Exclusive(i64_value(-1))),
            Some(V1RangeBound::Inclusive(i64_value(1))),
            None,
        ),
        (
            Some(V1RangeBound::Exclusive(i64_value(-1))),
            Some(V1RangeBound::Exclusive(i64_value(1))),
            None,
        ),
        (
            Some(V1RangeBound::Inclusive(i64_value(-1))),
            Some(V1RangeBound::Inclusive(i64_value(1024))),
            Some(3),
        ),
        (
            Some(V1RangeBound::Inclusive(datetime(i64::MIN))),
            Some(V1RangeBound::Inclusive(datetime(i64::MAX))),
            None,
        ),
        (
            Some(V1RangeBound::Inclusive(string(""))),
            Some(V1RangeBound::Exclusive(string("aaa"))),
            None,
        ),
    ]
}

fn to_secondary_query(
    lower: Option<&V1RangeBound>,
    upper: Option<&V1RangeBound>,
) -> Option<SecondaryRangeQuery> {
    match (lower, upper) {
        (None, None) => None,
        (Some(lower), None) => Some(SecondaryRangeQuery::Lower {
            value: lower.value(),
            inclusive: lower.inclusive(),
        }),
        (None, Some(upper)) => Some(SecondaryRangeQuery::Upper {
            value: upper.value(),
            inclusive: upper.inclusive(),
        }),
        (Some(lower), Some(upper)) => Some(SecondaryRangeQuery::Between {
            lower: lower.value(),
            lower_inclusive: lower.inclusive(),
            upper: upper.value(),
            upper_inclusive: upper.inclusive(),
        }),
    }
}

async fn range_cases(db: &HelixDB) -> Vec<V1RangeCaseObservation> {
    let mut cases = Vec::new();
    for element_kind in [V1ElementKind::Node, V1ElementKind::Edge] {
        for direction in [V1RangeDirection::Ascending, V1RangeDirection::Descending] {
            let descending = direction == V1RangeDirection::Descending;
            let property = if descending { "desc" } else { "asc" };
            let handle = secondary_handle(db, element_kind, property, Some(descending));
            for (lower, upper, limit) in base_range_cases() {
                let query = to_secondary_query(lower.as_ref(), upper.as_ref());
                let actual_ids = scan_active_range_generation(
                    db.inner_db().as_ref(),
                    &handle,
                    query.as_ref(),
                    limit.map(|value| value as usize),
                )
                .await
                .expect("migrated range scan succeeds");
                cases.push(V1RangeCaseObservation {
                    element_kind,
                    direction,
                    access: V1RangeAccess::Direct,
                    lower,
                    upper,
                    limit,
                    actual_ids,
                });
            }
        }
    }
    cases
}

fn returned_ids(response: &serde_json::Value, name: &str) -> Vec<u64> {
    response
        .get(name)
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("planner result `{name}` is an array"))
        .iter()
        .map(|value| {
            value
                .as_u64()
                .unwrap_or_else(|| panic!("planner result `{name}` contains graph IDs"))
        })
        .collect()
}

async fn planner_range_cases(db: &HelixDB) -> Vec<V1RangeCaseObservation> {
    let node_literal = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "ids",
                    g().n_with_label_where("RangeNode", Predicate::between("asc", -1_i64, 1_i64))
                        .order_by("asc", Order::Asc)
                        .limit(3_usize)
                        .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .expect("literal node range query succeeds");
    let node_parameter = db
        .query(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "ids",
                        g().n_with_label_where(
                            "RangeNode",
                            Predicate::gte_param("desc", "minimum"),
                        )
                        .order_by("desc", Order::Desc)
                        .limit(5_usize)
                        .id(),
                    )
                    .returning(["ids"]),
            )
            .with_parameter_value("minimum", QueryValue::I64(0)),
        )
        .await
        .expect("parameter node range query succeeds");
    let edge_literal = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "ids",
                    g().e_with_label_where("RangeEdge", Predicate::lt("asc", 1_i64))
                        .order_by("asc", Order::Asc)
                        .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .expect("literal edge range query succeeds");
    let edge_parameter = db
        .query(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "ids",
                        g().e_with_label_where(
                            "RangeEdge",
                            Predicate::lte_param("desc", "maximum"),
                        )
                        .order_by("desc", Order::Desc)
                        .id(),
                    )
                    .returning(["ids"]),
            )
            .with_parameter_value("maximum", QueryValue::I64(1)),
        )
        .await
        .expect("parameter edge range query succeeds");
    vec![
        V1RangeCaseObservation {
            element_kind: V1ElementKind::Node,
            direction: V1RangeDirection::Ascending,
            access: V1RangeAccess::Literal,
            lower: Some(V1RangeBound::Inclusive(V1OracleValue::I64(-1))),
            upper: Some(V1RangeBound::Inclusive(V1OracleValue::I64(1))),
            limit: Some(3),
            actual_ids: returned_ids(&node_literal, "ids"),
        },
        V1RangeCaseObservation {
            element_kind: V1ElementKind::Node,
            direction: V1RangeDirection::Descending,
            access: V1RangeAccess::Parameter,
            lower: Some(V1RangeBound::Inclusive(V1OracleValue::I64(0))),
            upper: None,
            limit: Some(5),
            actual_ids: returned_ids(&node_parameter, "ids"),
        },
        V1RangeCaseObservation {
            element_kind: V1ElementKind::Edge,
            direction: V1RangeDirection::Ascending,
            access: V1RangeAccess::Literal,
            lower: None,
            upper: Some(V1RangeBound::Exclusive(V1OracleValue::I64(1))),
            limit: None,
            actual_ids: returned_ids(&edge_literal, "ids"),
        },
        V1RangeCaseObservation {
            element_kind: V1ElementKind::Edge,
            direction: V1RangeDirection::Descending,
            access: V1RangeAccess::Parameter,
            lower: None,
            upper: Some(V1RangeBound::Inclusive(V1OracleValue::I64(1))),
            limit: None,
            actual_ids: returned_ids(&edge_parameter, "ids"),
        },
    ]
}

async fn open(database: String, store: Arc<dyn ObjectStore>) -> HelixDB {
    tokio::time::timeout(
        SEMANTIC_OPEN_TIMEOUT,
        HelixDB::open_with_object_store_for_migration_parity(
            database,
            store,
            super::one_row_config(),
        ),
    )
    .await
    .expect("semantic V1 writer open must terminate")
    .expect("semantic V1 writer migration succeeds")
}

/// Migrates typed node and edge equality values and observes exact serving.
pub async fn v1_equality_semantics_migration_contract() -> V1EqualityMigrationObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!("v1-equality-semantics-{}", uuid::Uuid::new_v4());
    let definitions = vec![
        SecondaryIndexDefinition::node_equality("EqNode", "value")
            .expect("node equality definition validates")
            .try_into()
            .expect("node equality definition converts"),
        SecondaryIndexDefinition::edge_equality("EqEdge", "value")
            .expect("edge equality definition validates")
            .try_into()
            .expect("edge equality definition converts"),
    ];
    let values = equality_values();
    let rows = seed_semantic_rows(
        &database,
        Arc::clone(&store),
        SemanticSeed {
            node_label: "EqNode",
            edge_label: "EqEdge",
            properties: &["value"],
            values: &values,
            definitions: &definitions,
            node_base: 10_000,
            edge_base: 20_000,
        },
    )
    .await;
    let migrated = open(database.clone(), Arc::clone(&store)).await;
    let first = equality_queries(&migrated, &values).await;
    migrated.close().await.expect("equality writer closes");
    let reopened = open(database, store).await;
    let second = equality_queries(&reopened, &values).await;
    reopened
        .close()
        .await
        .expect("equality reopened writer closes");
    assert_eq!(first, second);
    V1EqualityMigrationObservation {
        rows,
        queries: second,
        cold_reopen_identical: true,
    }
}

/// Migrates typed node and edge range values and observes every bound form.
pub async fn v1_range_semantics_migration_contract() -> V1RangeMigrationObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!("v1-range-semantics-{}", uuid::Uuid::new_v4());
    let definitions = definitions("RangeNode", "RangeEdge", &[("asc", false), ("desc", true)]);
    let values = range_values();
    let rows = seed_semantic_rows(
        &database,
        Arc::clone(&store),
        SemanticSeed {
            node_label: "RangeNode",
            edge_label: "RangeEdge",
            properties: &["asc", "desc"],
            values: &values,
            definitions: &definitions,
            node_base: 30_000,
            edge_base: 40_000,
        },
    )
    .await;
    let migrated = open(database.clone(), Arc::clone(&store)).await;
    let mut first = range_cases(&migrated).await;
    first.extend(planner_range_cases(&migrated).await);
    migrated.close().await.expect("range writer closes");
    let reopened = open(database, store).await;
    let mut second = range_cases(&reopened).await;
    second.extend(planner_range_cases(&reopened).await);
    reopened
        .close()
        .await
        .expect("range reopened writer closes");
    assert_eq!(first, second);
    V1RangeMigrationObservation {
        rows,
        cases: second,
        cold_reopen_identical: true,
    }
}
