//! Response-encoding regression contracts.
//!
//! [`legacy`] is the `serde_json::Value`-tree encoder production used before
//! results were serialized directly. It is kept only here, as the oracle the
//! production encoder must match: every fixture and every generated result
//! must produce identical transport bytes, an identical embedded `JsonValue`,
//! and an indistinguishable error (message, class and public code).

use std::collections::BTreeMap;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::{PropertyInput, PropertyValue as AstPropertyValue};
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;

use super::*;
use crate::encoding::property::property_value::PropertyValue;
use crate::execution::interpreter::{ExecutionRow, FoldedStream, RowVirtualProperties};
use crate::index_lifecycle as lifecycle;
use crate::HelixDbSource;

/// The pre-direct-serialization encoder, verbatim apart from visibility.
mod legacy {
    use super::*;

    pub(super) fn bytes(
        result: ExecutionResult,
    ) -> std::result::Result<Vec<u8>, QueryServiceError> {
        sonic_rs::to_vec(&returns(result)?).map_err(QueryServiceError::Serialize)
    }

    pub(super) fn value(
        result: ExecutionResult,
    ) -> std::result::Result<JsonValue, QueryServiceError> {
        Ok(JsonValue::Object(returns(result)?.into_iter().collect()))
    }

    fn returns(
        result: ExecutionResult,
    ) -> std::result::Result<BTreeMap<String, JsonValue>, QueryServiceError> {
        result
            .returns
            .into_iter()
            .map(|(name, value)| Ok((name.into_string(), returned_value_to_json(value)?)))
            .collect()
    }

    fn returned_value_to_json(
        value: ReturnedValue,
    ) -> std::result::Result<JsonValue, QueryServiceError> {
        match value {
            ReturnedValue::Present(value) => execution_value_to_json(value),
            ReturnedValue::EmptyList => Ok(JsonValue::Array(Vec::new())),
            ReturnedValue::EmptyObject => Ok(JsonValue::Null),
        }
    }

    fn execution_value_to_json(
        value: ExecutionValue,
    ) -> std::result::Result<JsonValue, QueryServiceError> {
        match value {
            ExecutionValue::Stream(rows) => rows
                .into_iter()
                .map(execution_row_to_json)
                .collect::<std::result::Result<Vec<_>, QueryServiceError>>()
                .map(JsonValue::Array),
            ExecutionValue::FoldedStream(rows) => rows
                .into_rows()
                .into_iter()
                .map(execution_row_to_json)
                .collect::<std::result::Result<Vec<_>, QueryServiceError>>()
                .map(JsonValue::Array),
            ExecutionValue::Count(count) => Ok(JsonValue::from(count)),
            ExecutionValue::Bool(value) => Ok(JsonValue::Bool(value)),
            ExecutionValue::Scalars(values) => values
                .into_iter()
                .map(execution_scalar_to_json)
                .collect::<std::result::Result<Vec<_>, QueryServiceError>>()
                .map(JsonValue::Array),
            ExecutionValue::IndexDdlReceipt(receipt) => {
                serde_json::to_value(receipt).map_err(QueryServiceError::JsonSerialize)
            }
            ExecutionValue::IndexOperationStatus(status) => {
                serde_json::to_value(status).map_err(QueryServiceError::JsonSerialize)
            }
        }
    }

    fn execution_row_to_json(
        row: ExecutionRow,
    ) -> std::result::Result<JsonValue, QueryServiceError> {
        match row.current.as_ref() {
            Some(current)
                if row.bindings.is_empty()
                    && row.binding_virtual_properties.is_empty()
                    && !row.path_visible
                    && !row.sack.is_visible() =>
            {
                let id = match current {
                    ElementRef::Node(id) | ElementRef::Edge(id) => *id,
                };
                let mut object =
                    serde_json::Map::from_iter([("$id".to_string(), JsonValue::from(id))]);
                for property in ["$distance", "$score"] {
                    let property = NonEmptyString::new(property)
                        .expect("public virtual property name is non-empty");
                    if let Some(value) = row.virtual_properties.get(&property) {
                        object.insert(property.into_string(), property_value_to_json(value)?);
                    }
                }
                return Ok(JsonValue::Object(object));
            }
            Some(_) | None => {}
        }

        let path = row.path_visible.then(|| {
            JsonValue::Array(
                row.path
                    .elements()
                    .iter()
                    .cloned()
                    .map(element_ref_to_json)
                    .collect(),
            )
        });
        let sack = if row.sack.is_visible() {
            Some(
                row.sack
                    .value()
                    .cloned()
                    .map(property_value_to_json)
                    .transpose()?
                    .unwrap_or(JsonValue::Null),
            )
        } else {
            None
        };
        let bindings = row
            .bindings
            .into_iter()
            .map(|(name, value)| (name.into_string(), element_ref_to_json(value)))
            .collect::<serde_json::Map<_, _>>();
        let mut object = serde_json::Map::from_iter([
            (
                "current".to_string(),
                row.current.map_or(JsonValue::Null, element_ref_to_json),
            ),
            ("bindings".to_string(), JsonValue::Object(bindings)),
        ]);
        if let Some(path) = path {
            object.insert("path".to_string(), path);
        }
        if let Some(sack) = sack {
            object.insert("sack".to_string(), sack);
        }
        Ok(JsonValue::Object(object))
    }

    fn execution_scalar_to_json(
        value: ExecutionScalar,
    ) -> std::result::Result<JsonValue, QueryServiceError> {
        match value {
            ExecutionScalar::NodeId(id) | ExecutionScalar::EdgeId(id) => Ok(JsonValue::from(id)),
            ExecutionScalar::String(value) => Ok(JsonValue::String(value)),
            ExecutionScalar::Value(value) => property_value_to_json(value),
            ExecutionScalar::Object(values) => values
                .into_iter()
                .map(|(name, value)| Ok((name.to_string(), property_value_to_json(value)?)))
                .collect::<std::result::Result<serde_json::Map<_, _>, QueryServiceError>>()
                .map(JsonValue::Object),
        }
    }

    fn property_value_to_json(
        value: PropertyValue,
    ) -> std::result::Result<JsonValue, QueryServiceError> {
        serde_json::to_value(value).map_err(QueryServiceError::JsonSerialize)
    }

    fn element_ref_to_json(value: ElementRef) -> JsonValue {
        let (kind, id) = match value {
            ElementRef::Node(id) => ("node", id),
            ElementRef::Edge(id) => ("edge", id),
        };
        JsonValue::Object(serde_json::Map::from_iter([(
            kind.to_string(),
            JsonValue::from(id),
        )]))
    }
}

/// Bytes the HTTP/gRPC transports and `HelixDB::query_json` send for `result`.
fn production_bytes(result: ExecutionResult) -> std::result::Result<Vec<u8>, QueryServiceError> {
    QueryResponse::from_execution_result(result).map(QueryResponse::into_json_bytes)
}

/// Value the embedded `HelixDB::query` API returns for `result`.
fn production_value(result: ExecutionResult) -> std::result::Result<JsonValue, QueryServiceError> {
    QueryResponse::<JsonValue>::encode(&result.returns, PlannerDiagnostics::default())
        .map(QueryResponse::into_value)
}

fn object<K: AsRef<str>>(entries: impl IntoIterator<Item = (K, PropertyValue)>) -> ExecutionScalar {
    ExecutionScalar::Object(
        entries
            .into_iter()
            .map(|(name, value)| (name.as_ref().into(), value))
            .collect(),
    )
}

fn name(value: &str) -> NonEmptyString {
    NonEmptyString::new(value).expect("test name is non-empty")
}

fn result(returns: Vec<(&str, ReturnedValue)>) -> ExecutionResult {
    ExecutionResult {
        last: None,
        variables: BTreeMap::new(),
        returns: returns
            .into_iter()
            .map(|(return_name, value)| (name(return_name), value))
            .collect(),
    }
}

fn present(value: ExecutionValue) -> ReturnedValue {
    ReturnedValue::Present(value)
}

fn scalars(values: Vec<ExecutionScalar>) -> ReturnedValue {
    present(ExecutionValue::Scalars(values))
}

fn values(values: Vec<PropertyValue>) -> ReturnedValue {
    scalars(values.into_iter().map(ExecutionScalar::Value).collect())
}

fn plain(current: ElementRef) -> ExecutionRow {
    let mut row = ExecutionRow::empty();
    row.current = Some(current);
    row
}

fn ranked(current: ElementRef, properties: Vec<(&str, PropertyValue)>) -> ExecutionRow {
    let mut row = plain(current);
    let mut virtual_properties = RowVirtualProperties::empty();
    for (property, value) in properties {
        virtual_properties.insert(name(property), value);
    }
    row.virtual_properties = virtual_properties;
    row
}

fn traversed(path: Vec<ElementRef>) -> ExecutionRow {
    let mut row = ExecutionRow::empty();
    for element in path {
        row.set_current(element);
    }
    row
}

fn stream(rows: Vec<ExecutionRow>) -> ReturnedValue {
    present(ExecutionValue::Stream(rows))
}

fn operation_id(byte: u8) -> lifecycle::IndexOperationId {
    lifecycle::IndexOperationId::from_bytes([byte; 16]).expect("operation id is valid")
}

fn status_common() -> lifecycle::IndexOperationStatusCommon {
    lifecycle::IndexOperationStatusCommon {
        operation_id: operation_id(3),
        index_id: lifecycle::IndexId::new(42).expect("index id is valid"),
        generation: lifecycle::IndexGenerationId::new(5).expect("generation is valid"),
        operation_kind: lifecycle::PublicIndexOperationKind::Build,
        family: lifecycle::PublicIndexFamily::Vector,
        stage: lifecycle::IndexOperationStage::CatchUp,
        attempt: 2,
        progress: lifecycle::IndexOperationPublicProgress {
            entities: 1,
            input_bytes: u64::MAX,
            output_operations: 0,
            output_bytes: 9,
        },
    }
}

fn receipts() -> Vec<lifecycle::IndexDdlReceipt> {
    vec![
        lifecycle::IndexDdlReceipt::Accepted {
            operation_id: operation_id(7),
            index_id: lifecycle::IndexId::new(42).expect("index id is valid"),
            generation: lifecycle::IndexGenerationId::new(3).expect("generation is valid"),
        },
        lifecycle::IndexDdlReceipt::ExistingOperation {
            operation_id: operation_id(0xab),
        },
        lifecycle::IndexDdlReceipt::AlreadyActive {
            index_id: lifecycle::IndexId::new(u64::MAX).expect("index id is valid"),
            generation: lifecycle::IndexGenerationId::new(1).expect("generation is valid"),
        },
    ]
}

fn statuses() -> Vec<lifecycle::IndexOperationStatus> {
    vec![
        lifecycle::IndexOperationStatus::Queued {
            common: status_common(),
        },
        lifecycle::IndexOperationStatus::Running {
            common: status_common(),
        },
        lifecycle::IndexOperationStatus::Blocked {
            common: status_common(),
            blocker_code: lifecycle::IndexOperationBlockerCode::UniquenessViolation,
            message: Some("dup \"key\"\n".to_string()),
        },
        lifecycle::IndexOperationStatus::Blocked {
            common: status_common(),
            blocker_code: lifecycle::IndexOperationBlockerCode::InvariantViolation,
            message: None,
        },
        lifecycle::IndexOperationStatus::Succeeded {
            common: status_common(),
        },
        lifecycle::IndexOperationStatus::Aborted {
            common: status_common(),
        },
    ]
}

/// Strings that exercise every JSON escaping class.
fn edge_strings() -> Vec<String> {
    vec![
        String::new(),
        "plain".to_string(),
        "quote\" backslash\\ slash/".to_string(),
        "\u{0}\u{1}\u{8}\u{9}\u{a}\u{c}\u{d}\u{1f}\u{20}\u{7f}".to_string(),
        "é ß 漢字 😀 \u{2028}\u{2029} \u{feff}".to_string(),
        "x".repeat(40),
    ]
}

fn edge_f64s() -> Vec<f64> {
    vec![
        0.0,
        -0.0,
        1.0,
        -1.5,
        0.1,
        0.25,
        1.0 / 3.0,
        1e-7,
        1e-6,
        123_456_789.123_456_79,
        1e15,
        1e16,
        1e17,
        1e21,
        1e22,
        9_007_199_254_740_993.0,
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        5e-324,
        f64::EPSILON,
        f64::NAN,
        -f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]
}

fn edge_f32s() -> Vec<f32> {
    vec![
        0.0,
        -0.0,
        0.1,
        1.0 / 3.0,
        16_777_217.0,
        f32::MAX,
        f32::MIN_POSITIVE,
        1e-45,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
    ]
}

/// One value of every property variant, with numeric and string edge values.
fn edge_properties() -> Vec<PropertyValue> {
    let mut properties = vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::Bool(true),
        PropertyValue::I64(0),
        PropertyValue::I64(-1),
        PropertyValue::I64(i64::MIN),
        PropertyValue::I64(i64::MAX),
        PropertyValue::DateTime(0),
        PropertyValue::DateTime(-1),
        PropertyValue::DateTime(1_700_000_000_123),
        PropertyValue::DateTime(253_402_300_799_999),
        PropertyValue::DateTime(-62_135_596_800_000),
        PropertyValue::Bytes(Vec::new()),
        PropertyValue::Bytes(vec![0, 1, 127, 128, 255]),
        PropertyValue::I64Array(Vec::new()),
        PropertyValue::I64Array(vec![i64::MIN, -1, 0, 1, i64::MAX]),
        PropertyValue::F64Array(edge_f64s()),
        PropertyValue::F32Array(edge_f32s()),
        PropertyValue::StringArray(edge_strings()),
        PropertyValue::Array(Vec::new()),
        PropertyValue::Object(BTreeMap::new()),
    ];
    properties.extend(edge_f64s().into_iter().map(PropertyValue::F64));
    properties.extend(
        edge_f32s()
            .into_iter()
            .map(|value| PropertyValue::F32(f64::from(value))),
    );
    properties.extend(edge_strings().into_iter().map(PropertyValue::String));
    let nested = PropertyValue::Array(vec![
        PropertyValue::F32Array(vec![0.1, f32::NAN]),
        PropertyValue::Object(BTreeMap::from([
            ("z".to_string(), PropertyValue::F32Array(vec![0.1])),
            ("A".to_string(), PropertyValue::Null),
            ("é".to_string(), PropertyValue::DateTime(0)),
            (
                "\"".to_string(),
                PropertyValue::Array(vec![PropertyValue::F64(f64::NAN)]),
            ),
        ])),
        PropertyValue::Array(vec![PropertyValue::Array(Vec::new())]),
    ]);
    properties.push(nested.clone());
    properties.push(PropertyValue::Object(BTreeMap::from([
        ("nested".to_string(), nested),
        (String::new(), PropertyValue::I64(1)),
        (
            "$id".to_string(),
            PropertyValue::String("shadow".to_string()),
        ),
    ])));
    properties
}

/// Named fixtures covering every return, row, scalar and property shape.
fn cases() -> Vec<(&'static str, ExecutionResult)> {
    let invalid_datetime = PropertyValue::DateTime(i64::MAX);
    vec![
        ("no_returns", result(Vec::new())),
        (
            "empty_shapes",
            result(vec![
                ("list", ReturnedValue::EmptyList),
                ("object", ReturnedValue::EmptyObject),
                ("stream", stream(Vec::new())),
                ("scalars", scalars(Vec::new())),
                (
                    "folded",
                    present(ExecutionValue::FoldedStream(FoldedStream::new(Vec::new()))),
                ),
            ]),
        ),
        (
            "counts_and_bools",
            result(vec![
                ("zero", present(ExecutionValue::Count(0))),
                ("max", present(ExecutionValue::Count(usize::MAX))),
                ("yes", present(ExecutionValue::Bool(true))),
                ("no", present(ExecutionValue::Bool(false))),
            ]),
        ),
        (
            "return_name_order_and_escaping",
            result(vec![
                ("b", present(ExecutionValue::Count(2))),
                ("a", present(ExecutionValue::Count(1))),
                ("B", present(ExecutionValue::Count(3))),
                ("$x", present(ExecutionValue::Count(4))),
                ("é\"\n", present(ExecutionValue::Count(5))),
            ]),
        ),
        (
            "plain_and_ranked_rows",
            result(vec![(
                "rows",
                stream(vec![
                    plain(ElementRef::Node(0)),
                    plain(ElementRef::Edge(u64::MAX)),
                    ranked(
                        ElementRef::Node(1),
                        vec![("$distance", PropertyValue::F64(0.25))],
                    ),
                    ranked(
                        ElementRef::Edge(2),
                        vec![("$score", PropertyValue::F32(1.5))],
                    ),
                    ranked(
                        ElementRef::Node(3),
                        vec![
                            ("$score", PropertyValue::F64(f64::NAN)),
                            ("$distance", PropertyValue::I64(-4)),
                            ("hidden", PropertyValue::String("not public".to_string())),
                        ],
                    ),
                    ranked(
                        ElementRef::Node(4),
                        vec![("other", PropertyValue::Bool(true))],
                    ),
                ]),
            )]),
        ),
        (
            "annotated_rows",
            result(vec![(
                "rows",
                stream(vec![
                    ExecutionRow::empty(),
                    {
                        let mut row = plain(ElementRef::Node(7));
                        row.bindings = BTreeMap::from([
                            (name("friend"), ElementRef::Edge(9)),
                            (name("Alpha"), ElementRef::Node(1)),
                        ]);
                        row
                    },
                    {
                        let mut row = ranked(
                            ElementRef::Node(8),
                            vec![("$distance", PropertyValue::F64(0.5))],
                        );
                        row.binding_virtual_properties = BTreeMap::from([(
                            name("seed"),
                            RowVirtualProperties::from_one(name("$score"), PropertyValue::F64(2.0)),
                        )]);
                        row
                    },
                    traversed(vec![
                        ElementRef::Node(1),
                        ElementRef::Edge(2),
                        ElementRef::Node(3),
                    ])
                    .mark_path_visible(),
                    {
                        let mut row = traversed(vec![ElementRef::Node(5)]);
                        row.set_sack(PropertyValue::F32Array(vec![0.1]));
                        row.mark_sack_visible()
                    },
                    plain(ElementRef::Node(6)).mark_sack_visible(),
                    {
                        let mut row = plain(ElementRef::Node(10));
                        row.set_sack(PropertyValue::String("hidden".to_string()));
                        row
                    },
                    {
                        let mut row = ExecutionRow::empty().mark_path_visible();
                        row.set_sack(PropertyValue::Null);
                        row.mark_sack_visible()
                    },
                ]),
            )]),
        ),
        (
            "folded_rows",
            result(vec![(
                "folded",
                present(ExecutionValue::FoldedStream(FoldedStream::new(vec![
                    plain(ElementRef::Edge(11)),
                    traversed(vec![ElementRef::Node(1)]).mark_path_visible(),
                ]))),
            )]),
        ),
        (
            "id_and_string_scalars",
            result(vec![(
                "scalars",
                scalars(
                    [
                        ExecutionScalar::NodeId(0),
                        ExecutionScalar::NodeId(u64::MAX),
                        ExecutionScalar::EdgeId(1),
                    ]
                    .into_iter()
                    .chain(edge_strings().into_iter().map(ExecutionScalar::String))
                    .collect(),
                ),
            )]),
        ),
        (
            "property_values",
            result(vec![("values", values(edge_properties()))]),
        ),
        (
            "objects",
            result(vec![(
                "objects",
                scalars(vec![
                    object(Vec::<(&str, PropertyValue)>::new()),
                    object(vec![
                        ("$id", PropertyValue::I64(7)),
                        ("$from", PropertyValue::I64(1)),
                        ("$to", PropertyValue::I64(2)),
                        ("name", PropertyValue::String("Ada".to_string())),
                        ("Name", PropertyValue::Null),
                        ("é", PropertyValue::F32(f64::from(0.1_f32))),
                        ("\"\\\n", PropertyValue::Bool(true)),
                        ("", PropertyValue::Bytes(vec![255])),
                    ]),
                    object(
                        edge_properties()
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| (format!("p{index:02}"), value)),
                    ),
                ]),
            )]),
        ),
        (
            "index_lifecycle",
            result(
                vec![
                    (
                        "receipts",
                        present(ExecutionValue::IndexDdlReceipt(receipts().remove(0))),
                    ),
                    (
                        "existing",
                        present(ExecutionValue::IndexDdlReceipt(receipts().remove(1))),
                    ),
                    (
                        "active",
                        present(ExecutionValue::IndexDdlReceipt(receipts().remove(2))),
                    ),
                ]
                .into_iter()
                .chain(statuses().into_iter().enumerate().map(|(index, status)| {
                    (
                        [
                            "status_a", "status_b", "status_c", "status_d", "status_e", "status_f",
                        ][index],
                        present(ExecutionValue::IndexOperationStatus(status)),
                    )
                }))
                .collect(),
            ),
        ),
        (
            "invalid_datetime_value",
            result(vec![("values", values(vec![invalid_datetime.clone()]))]),
        ),
        (
            "invalid_datetime_in_object",
            result(vec![(
                "objects",
                scalars(vec![object(vec![
                    ("a", PropertyValue::I64(1)),
                    ("b", PropertyValue::DateTime(i64::MIN)),
                ])]),
            )]),
        ),
        (
            "invalid_datetime_in_nested_array",
            result(vec![(
                "values",
                values(vec![PropertyValue::Array(vec![PropertyValue::Object(
                    BTreeMap::from([("deep".to_string(), invalid_datetime.clone())]),
                )])]),
            )]),
        ),
        (
            "invalid_datetime_in_sack",
            result(vec![(
                "rows",
                stream(vec![{
                    let mut row = plain(ElementRef::Node(1));
                    row.set_sack(invalid_datetime.clone());
                    row.mark_sack_visible()
                }]),
            )]),
        ),
        (
            "invalid_datetime_in_ranked_row",
            result(vec![(
                "rows",
                stream(vec![ranked(
                    ElementRef::Node(1),
                    vec![("$score", invalid_datetime.clone())],
                )]),
            )]),
        ),
        (
            "first_invalid_datetime_wins",
            result(vec![
                ("b", values(vec![PropertyValue::DateTime(i64::MIN)])),
                ("a", values(vec![PropertyValue::DateTime(i64::MAX)])),
            ]),
        ),
        (
            "hidden_invalid_datetime_is_not_encoded",
            result(vec![(
                "rows",
                stream(vec![
                    ranked(
                        ElementRef::Node(1),
                        vec![("hidden", invalid_datetime.clone())],
                    ),
                    {
                        let mut row = plain(ElementRef::Node(2));
                        row.set_sack(invalid_datetime);
                        row
                    },
                ]),
            )]),
        ),
    ]
}

/// Exact public bytes for every fixture, captured from the Value-tree encoder.
const GOLDEN: &[(&str, &str)] = &[
    ("annotated_rows", "{\"rows\":[{\"bindings\":{},\"current\":null},{\"bindings\":{\"Alpha\":{\"node\":1},\"friend\":{\"edge\":9}},\"current\":{\"node\":7}},{\"bindings\":{},\"current\":{\"node\":8}},{\"bindings\":{},\"current\":{\"node\":3},\"path\":[{\"node\":1},{\"edge\":2},{\"node\":3}]},{\"bindings\":{},\"current\":{\"node\":5},\"sack\":[0.10000000149011612]},{\"bindings\":{},\"current\":{\"node\":6},\"sack\":null},{\"$id\":10},{\"bindings\":{},\"current\":null,\"path\":[],\"sack\":null}]}"),
    ("counts_and_bools", "{\"max\":18446744073709551615,\"no\":false,\"yes\":true,\"zero\":0}"),
    ("empty_shapes", "{\"folded\":[],\"list\":[],\"object\":null,\"scalars\":[],\"stream\":[]}"),
    ("first_invalid_datetime_wins", "ERROR Internal ResponseSerializationError json serialization error: datetime millis '9223372036854775807' cannot be rendered as RFC3339"),
    ("folded_rows", "{\"folded\":[{\"$id\":11},{\"bindings\":{},\"current\":{\"node\":1},\"path\":[{\"node\":1}]}]}"),
    ("hidden_invalid_datetime_is_not_encoded", "{\"rows\":[{\"$id\":1},{\"$id\":2}]}"),
    ("id_and_string_scalars", "{\"scalars\":[0,18446744073709551615,1,\"\",\"plain\",\"quote\\\" backslash\\\\ slash/\",\"\\u0000\\u0001\\b\\t\\n\\f\\r\\u001f \u{7f}\",\"é ß 漢字 😀 \u{2028}\u{2029} \u{feff}\",\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"]}"),
    ("index_lifecycle", "{\"active\":{\"generation\":\"1\",\"index_id\":\"18446744073709551615\",\"kind\":\"already_active\"},\"existing\":{\"kind\":\"existing_operation\",\"operation_id\":\"abababab-abab-abab-abab-abababababab\"},\"receipts\":{\"generation\":\"3\",\"index_id\":\"42\",\"kind\":\"accepted\",\"operation_id\":\"07070707-0707-0707-0707-070707070707\"},\"status_a\":{\"attempt\":2,\"family\":\"vector\",\"generation\":\"5\",\"index_id\":\"42\",\"operation_id\":\"03030303-0303-0303-0303-030303030303\",\"operation_kind\":\"build\",\"progress\":{\"entities\":\"1\",\"input_bytes\":\"18446744073709551615\",\"output_bytes\":\"9\",\"output_operations\":\"0\"},\"stage\":\"catch_up\",\"status\":\"queued\"},\"status_b\":{\"attempt\":2,\"family\":\"vector\",\"generation\":\"5\",\"index_id\":\"42\",\"operation_id\":\"03030303-0303-0303-0303-030303030303\",\"operation_kind\":\"build\",\"progress\":{\"entities\":\"1\",\"input_bytes\":\"18446744073709551615\",\"output_bytes\":\"9\",\"output_operations\":\"0\"},\"stage\":\"catch_up\",\"status\":\"running\"},\"status_c\":{\"attempt\":2,\"blocker_code\":\"uniqueness_violation\",\"family\":\"vector\",\"generation\":\"5\",\"index_id\":\"42\",\"message\":\"dup \\\"key\\\"\\n\",\"operation_id\":\"03030303-0303-0303-0303-030303030303\",\"operation_kind\":\"build\",\"progress\":{\"entities\":\"1\",\"input_bytes\":\"18446744073709551615\",\"output_bytes\":\"9\",\"output_operations\":\"0\"},\"stage\":\"catch_up\",\"status\":\"blocked\"},\"status_d\":{\"attempt\":2,\"blocker_code\":\"invariant_violation\",\"family\":\"vector\",\"generation\":\"5\",\"index_id\":\"42\",\"operation_id\":\"03030303-0303-0303-0303-030303030303\",\"operation_kind\":\"build\",\"progress\":{\"entities\":\"1\",\"input_bytes\":\"18446744073709551615\",\"output_bytes\":\"9\",\"output_operations\":\"0\"},\"stage\":\"catch_up\",\"status\":\"blocked\"},\"status_e\":{\"attempt\":2,\"family\":\"vector\",\"generation\":\"5\",\"index_id\":\"42\",\"operation_id\":\"03030303-0303-0303-0303-030303030303\",\"operation_kind\":\"build\",\"progress\":{\"entities\":\"1\",\"input_bytes\":\"18446744073709551615\",\"output_bytes\":\"9\",\"output_operations\":\"0\"},\"stage\":\"catch_up\",\"status\":\"succeeded\"},\"status_f\":{\"attempt\":2,\"family\":\"vector\",\"generation\":\"5\",\"index_id\":\"42\",\"operation_id\":\"03030303-0303-0303-0303-030303030303\",\"operation_kind\":\"build\",\"progress\":{\"entities\":\"1\",\"input_bytes\":\"18446744073709551615\",\"output_bytes\":\"9\",\"output_operations\":\"0\"},\"stage\":\"catch_up\",\"status\":\"aborted\"}}"),
    ("invalid_datetime_in_nested_array", "ERROR Internal ResponseSerializationError json serialization error: datetime millis '9223372036854775807' cannot be rendered as RFC3339"),
    ("invalid_datetime_in_object", "ERROR Internal ResponseSerializationError json serialization error: datetime millis '-9223372036854775808' cannot be rendered as RFC3339"),
    ("invalid_datetime_in_ranked_row", "ERROR Internal ResponseSerializationError json serialization error: datetime millis '9223372036854775807' cannot be rendered as RFC3339"),
    ("invalid_datetime_in_sack", "ERROR Internal ResponseSerializationError json serialization error: datetime millis '9223372036854775807' cannot be rendered as RFC3339"),
    ("invalid_datetime_value", "ERROR Internal ResponseSerializationError json serialization error: datetime millis '9223372036854775807' cannot be rendered as RFC3339"),
    ("no_returns", "{}"),
    ("objects", "{\"objects\":[{},{\"\":[255],\"\\\"\\\\\\n\":true,\"$from\":1,\"$id\":7,\"$to\":2,\"Name\":null,\"name\":\"Ada\",\"é\":0.10000000149011612},{\"p00\":null,\"p01\":false,\"p02\":true,\"p03\":0,\"p04\":-1,\"p05\":-9223372036854775808,\"p06\":9223372036854775807,\"p07\":\"1970-01-01T00:00:00.000Z\",\"p08\":\"1969-12-31T23:59:59.999Z\",\"p09\":\"2023-11-14T22:13:20.123Z\",\"p10\":\"9999-12-31T23:59:59.999Z\",\"p11\":\"0001-01-01T00:00:00.000Z\",\"p12\":[],\"p13\":[0,1,127,128,255],\"p14\":[],\"p15\":[-9223372036854775808,-1,0,1,9223372036854775807],\"p16\":[0.0,-0.0,1.0,-1.5,0.1,0.25,0.3333333333333333,1e-7,1e-6,123456789.12345679,1000000000000000.0,1e+16,1e+17,1e+21,1e+22,9007199254740992.0,1.7976931348623157e+308,-1.7976931348623157e+308,2.2250738585072014e-308,5e-324,2.220446049250313e-16,null,null,null,null],\"p17\":[0.0,-0.0,0.10000000149011612,0.3333333432674408,16777216.0,3.4028234663852886e+38,1.1754943508222875e-38,1.401298464324817e-45,null,null,null],\"p18\":[\"\",\"plain\",\"quote\\\" backslash\\\\ slash/\",\"\\u0000\\u0001\\b\\t\\n\\f\\r\\u001f \u{7f}\",\"é ß 漢字 😀 \u{2028}\u{2029} \u{feff}\",\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"],\"p19\":[],\"p20\":{},\"p21\":0.0,\"p22\":-0.0,\"p23\":1.0,\"p24\":-1.5,\"p25\":0.1,\"p26\":0.25,\"p27\":0.3333333333333333,\"p28\":1e-7,\"p29\":1e-6,\"p30\":123456789.12345679,\"p31\":1000000000000000.0,\"p32\":1e+16,\"p33\":1e+17,\"p34\":1e+21,\"p35\":1e+22,\"p36\":9007199254740992.0,\"p37\":1.7976931348623157e+308,\"p38\":-1.7976931348623157e+308,\"p39\":2.2250738585072014e-308,\"p40\":5e-324,\"p41\":2.220446049250313e-16,\"p42\":null,\"p43\":null,\"p44\":null,\"p45\":null,\"p46\":0.0,\"p47\":-0.0,\"p48\":0.10000000149011612,\"p49\":0.3333333432674408,\"p50\":16777216.0,\"p51\":3.4028234663852886e+38,\"p52\":1.1754943508222875e-38,\"p53\":1.401298464324817e-45,\"p54\":null,\"p55\":null,\"p56\":null,\"p57\":\"\",\"p58\":\"plain\",\"p59\":\"quote\\\" backslash\\\\ slash/\",\"p60\":\"\\u0000\\u0001\\b\\t\\n\\f\\r\\u001f \u{7f}\",\"p61\":\"é ß 漢字 😀 \u{2028}\u{2029} \u{feff}\",\"p62\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\",\"p63\":[[0.10000000149011612,null],{\"\\\"\":[null],\"A\":null,\"z\":[0.10000000149011612],\"é\":\"1970-01-01T00:00:00.000Z\"},[[]]],\"p64\":{\"\":1,\"$id\":\"shadow\",\"nested\":[[0.10000000149011612,null],{\"\\\"\":[null],\"A\":null,\"z\":[0.10000000149011612],\"é\":\"1970-01-01T00:00:00.000Z\"},[[]]]}}]}"),
    ("plain_and_ranked_rows", "{\"rows\":[{\"$id\":0},{\"$id\":18446744073709551615},{\"$distance\":0.25,\"$id\":1},{\"$id\":2,\"$score\":1.5},{\"$distance\":-4,\"$id\":3,\"$score\":null},{\"$id\":4}]}"),
    ("property_values", "{\"values\":[null,false,true,0,-1,-9223372036854775808,9223372036854775807,\"1970-01-01T00:00:00.000Z\",\"1969-12-31T23:59:59.999Z\",\"2023-11-14T22:13:20.123Z\",\"9999-12-31T23:59:59.999Z\",\"0001-01-01T00:00:00.000Z\",[],[0,1,127,128,255],[],[-9223372036854775808,-1,0,1,9223372036854775807],[0.0,-0.0,1.0,-1.5,0.1,0.25,0.3333333333333333,1e-7,1e-6,123456789.12345679,1000000000000000.0,1e+16,1e+17,1e+21,1e+22,9007199254740992.0,1.7976931348623157e+308,-1.7976931348623157e+308,2.2250738585072014e-308,5e-324,2.220446049250313e-16,null,null,null,null],[0.0,-0.0,0.10000000149011612,0.3333333432674408,16777216.0,3.4028234663852886e+38,1.1754943508222875e-38,1.401298464324817e-45,null,null,null],[\"\",\"plain\",\"quote\\\" backslash\\\\ slash/\",\"\\u0000\\u0001\\b\\t\\n\\f\\r\\u001f \u{7f}\",\"é ß 漢字 😀 \u{2028}\u{2029} \u{feff}\",\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"],[],{},0.0,-0.0,1.0,-1.5,0.1,0.25,0.3333333333333333,1e-7,1e-6,123456789.12345679,1000000000000000.0,1e+16,1e+17,1e+21,1e+22,9007199254740992.0,1.7976931348623157e+308,-1.7976931348623157e+308,2.2250738585072014e-308,5e-324,2.220446049250313e-16,null,null,null,null,0.0,-0.0,0.10000000149011612,0.3333333432674408,16777216.0,3.4028234663852886e+38,1.1754943508222875e-38,1.401298464324817e-45,null,null,null,\"\",\"plain\",\"quote\\\" backslash\\\\ slash/\",\"\\u0000\\u0001\\b\\t\\n\\f\\r\\u001f \u{7f}\",\"é ß 漢字 😀 \u{2028}\u{2029} \u{feff}\",\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\",[[0.10000000149011612,null],{\"\\\"\":[null],\"A\":null,\"z\":[0.10000000149011612],\"é\":\"1970-01-01T00:00:00.000Z\"},[[]]],{\"\":1,\"$id\":\"shadow\",\"nested\":[[0.10000000149011612,null],{\"\\\"\":[null],\"A\":null,\"z\":[0.10000000149011612],\"é\":\"1970-01-01T00:00:00.000Z\"},[[]]]}]}"),
    ("return_name_order_and_escaping", "{\"$x\":4,\"B\":3,\"a\":1,\"b\":2,\"é\\\"\\n\":5}"),
];

fn assert_same_error(case: &str, actual: &QueryServiceError, expected: &QueryServiceError) {
    assert_eq!(
        actual.to_string(),
        expected.to_string(),
        "{case}: error message"
    );
    assert_eq!(
        actual.classify(),
        expected.classify(),
        "{case}: error class"
    );
    assert_eq!(
        actual.error_code(),
        expected.error_code(),
        "{case}: error code"
    );
    assert_eq!(
        actual.index_error_code(),
        expected.index_error_code(),
        "{case}: index error code"
    );
}

/// Compares the production encoders with the oracle for one result.
fn assert_matches_oracle(case: &str, result: ExecutionResult) {
    match (
        production_bytes(result.clone()),
        legacy::bytes(result.clone()),
    ) {
        (Ok(actual), Ok(expected)) => assert_eq!(
            String::from_utf8(actual).expect("response is UTF-8"),
            String::from_utf8(expected).expect("oracle is UTF-8"),
            "{case}: transport bytes"
        ),
        (Err(actual), Err(expected)) => assert_same_error(case, &actual, &expected),
        (actual, expected) => panic!("{case}: bytes {actual:?} != oracle {expected:?}"),
    }
    match (production_value(result.clone()), legacy::value(result)) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(actual, expected, "{case}: embedded value");
            // `Value` equality treats every NaN-free float bitwise-equal; also
            // pin the float representation through its exact bits.
            assert_eq!(
                format!("{actual:?}"),
                format!("{expected:?}"),
                "{case}: embedded value representation"
            );
        }
        (Err(actual), Err(expected)) => assert_same_error(case, &actual, &expected),
        (actual, expected) => panic!("{case}: value {actual:?} != oracle {expected:?}"),
    }
}

#[test]
fn every_fixture_matches_the_value_tree_oracle() {
    for (case, result) in cases() {
        assert_matches_oracle(case, result);
    }
}

#[test]
fn every_fixture_matches_its_golden_bytes() {
    let goldens = GOLDEN.iter().copied().collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();
    for (case, result) in cases() {
        let rendered = match production_bytes(result) {
            Ok(bytes) => String::from_utf8(bytes).expect("response is UTF-8"),
            Err(error) => format!(
                "ERROR {:?} {:?} {}",
                error.classify(),
                error.error_code(),
                error
            ),
        };
        actual.insert(case, rendered);
    }
    let mismatches = actual
        .iter()
        .filter(|(case, rendered)| goldens.get(*case) != Some(&rendered.as_str()))
        .map(|(case, rendered)| format!("    ({case:?}, {rendered:?}),"))
        .collect::<Vec<_>>();
    assert!(
        mismatches.is_empty() && goldens.len() == actual.len(),
        "golden mismatch; actual entries:\n{}",
        mismatches.join("\n")
    );
}

#[test]
fn large_results_match_the_oracle() {
    let row_count = 10_000;
    let rows = (0..row_count)
        .map(|index| {
            let index_i64 = i64::try_from(index).expect("row index fits");
            object(vec![
                ("$id", PropertyValue::I64(index_i64)),
                ("label", PropertyValue::String("BenchNode".to_string())),
                (
                    "external_id",
                    PropertyValue::String(format!("node-{index}")),
                ),
                ("attribute_1", PropertyValue::I64(index_i64 * 3)),
                ("weight", PropertyValue::F64(index as f64 / 7.0)),
                ("ratio", PropertyValue::F32(f64::from(index as f32 / 3.0))),
                (
                    "created",
                    PropertyValue::DateTime(1_700_000_000_000 + index_i64),
                ),
            ])
        })
        .collect();
    let stream_rows = (0..row_count)
        .map(|id| {
            ranked(
                ElementRef::Node(id),
                vec![("$distance", PropertyValue::F64(id as f64 * 0.5))],
            )
        })
        .collect();
    assert_matches_oracle(
        "large",
        result(vec![
            ("rows", scalars(rows)),
            ("stream", stream(stream_rows)),
            (
                "huge_string",
                values(vec![PropertyValue::String("é\"".repeat(1 << 19))]),
            ),
        ]),
    );
}

fn arb_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        4 => any::<f64>(),
        1 => proptest::sample::select(edge_f64s()),
        1 => (-1_000_000_i64..1_000_000).prop_map(|value| value as f64 / 100.0),
    ]
}

fn arb_f32() -> impl Strategy<Value = f32> {
    prop_oneof![
        4 => any::<f32>(),
        1 => proptest::sample::select(edge_f32s()),
    ]
}

fn arb_string() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => any::<String>(),
        1 => proptest::sample::select(edge_strings()),
        1 => "[\\x00-\\x1f\"\\\\/a-z]{0,8}",
    ]
}

fn arb_datetime() -> impl Strategy<Value = i64> {
    prop_oneof![
        9 => -8_000_000_000_000_000_i64..8_000_000_000_000_000,
        1 => any::<i64>(),
    ]
}

fn arb_property() -> impl Strategy<Value = PropertyValue> {
    let leaf = prop_oneof![
        Just(PropertyValue::Null),
        any::<bool>().prop_map(PropertyValue::Bool),
        any::<i64>().prop_map(PropertyValue::I64),
        arb_datetime().prop_map(PropertyValue::DateTime),
        arb_f64().prop_map(PropertyValue::F64),
        arb_f32().prop_map(|value| PropertyValue::F32(f64::from(value))),
        arb_f64().prop_map(PropertyValue::F32),
        arb_string().prop_map(PropertyValue::String),
        vec(any::<u8>(), 0..6).prop_map(PropertyValue::Bytes),
        vec(any::<i64>(), 0..5).prop_map(PropertyValue::I64Array),
        vec(arb_f64(), 0..5).prop_map(PropertyValue::F64Array),
        vec(arb_f32(), 0..5).prop_map(PropertyValue::F32Array),
        vec(arb_string(), 0..4).prop_map(PropertyValue::StringArray),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..4).prop_map(PropertyValue::Array),
            btree_map(arb_string(), inner, 0..4).prop_map(PropertyValue::Object),
        ]
    })
}

fn arb_name() -> impl Strategy<Value = NonEmptyString> {
    prop_oneof![
        proptest::sample::select(vec!["$distance", "$score", "$id", "current", "a", "B"])
            .prop_map(name),
        arb_string().prop_filter_map("names are non-empty", NonEmptyString::new),
    ]
}

fn arb_element() -> impl Strategy<Value = ElementRef> {
    prop_oneof![
        any::<u64>().prop_map(ElementRef::Node),
        any::<u64>().prop_map(ElementRef::Edge),
    ]
}

fn arb_virtual_properties() -> impl Strategy<Value = RowVirtualProperties> {
    vec((arb_name(), arb_property()), 0..3).prop_map(|entries| {
        let mut properties = RowVirtualProperties::empty();
        for (property, value) in entries {
            properties.insert(property, value);
        }
        properties
    })
}

fn arb_row() -> impl Strategy<Value = ExecutionRow> {
    let annotations = (
        prop_oneof![3 => Just(BTreeMap::new()), 1 => btree_map(arb_name(), arb_element(), 1..3)],
        prop_oneof![
            4 => Just(BTreeMap::new()),
            1 => btree_map(arb_name(), arb_virtual_properties(), 1..2),
        ],
        prop_oneof![3 => Just(false), 1 => Just(true)],
        proptest::option::of(arb_property()),
        prop_oneof![3 => Just(false), 1 => Just(true)],
    );
    (
        proptest::option::weighted(0.9, arb_element()),
        vec(arb_element(), 0..4),
        arb_virtual_properties(),
        annotations,
    )
        .prop_map(
            |(
                current,
                path,
                virtual_properties,
                (bindings, binding_virtual_properties, path_visible, sack, sack_visible),
            )| {
                let mut row = traversed(path);
                row.current = current;
                row.virtual_properties = virtual_properties;
                row.bindings = bindings;
                row.binding_virtual_properties = binding_virtual_properties;
                if path_visible {
                    row = row.mark_path_visible();
                }
                if let Some(sack) = sack {
                    row.set_sack(sack);
                }
                if sack_visible {
                    row = row.mark_sack_visible();
                }
                row
            },
        )
}

fn arb_scalar() -> impl Strategy<Value = ExecutionScalar> {
    prop_oneof![
        any::<u64>().prop_map(ExecutionScalar::NodeId),
        any::<u64>().prop_map(ExecutionScalar::EdgeId),
        arb_string().prop_map(ExecutionScalar::String),
        arb_property().prop_map(ExecutionScalar::Value),
        btree_map(arb_string(), arb_property(), 0..5).prop_map(object),
    ]
}

fn arb_execution_value() -> impl Strategy<Value = ExecutionValue> {
    prop_oneof![
        3 => vec(arb_row(), 0..5).prop_map(ExecutionValue::Stream),
        1 => vec(arb_row(), 0..3)
            .prop_map(|rows| ExecutionValue::FoldedStream(FoldedStream::new(rows))),
        1 => any::<usize>().prop_map(ExecutionValue::Count),
        1 => any::<bool>().prop_map(ExecutionValue::Bool),
        3 => vec(arb_scalar(), 0..5).prop_map(ExecutionValue::Scalars),
        1 => proptest::sample::select(receipts()).prop_map(ExecutionValue::IndexDdlReceipt),
        1 => proptest::sample::select(statuses()).prop_map(ExecutionValue::IndexOperationStatus),
    ]
}

fn arb_result() -> impl Strategy<Value = ExecutionResult> {
    let returned = prop_oneof![
        8 => arb_execution_value().prop_map(ReturnedValue::Present),
        1 => Just(ReturnedValue::EmptyList),
        1 => Just(ReturnedValue::EmptyObject),
    ];
    btree_map(arb_name(), returned, 0..4).prop_map(|returns| ExecutionResult {
        last: None,
        variables: BTreeMap::new(),
        returns,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_048))]

    #[test]
    fn generated_results_match_the_value_tree_oracle(result in arb_result()) {
        assert_matches_oracle("generated", result);
    }

    #[test]
    fn generated_properties_match_the_value_tree_oracle(value in arb_property()) {
        assert_matches_oracle("generated property", result(vec![("value", values(vec![value]))]));
    }
}

/// Opens an in-memory database holding one node and one edge with every
/// property kind the SDK can write.
async fn embedded_fixture(database: &str) -> HelixDB {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: database.to_string(),
    })
    .await
    .expect("embedded database opens");
    let properties = vec![
        ("s", PropertyInput::from("quote\" é 😀 \n")),
        ("i", PropertyInput::from(i64::MIN)),
        ("f", PropertyInput::from(0.1_f64)),
        ("g", PropertyInput::from(0.1_f32)),
        (
            "dt",
            PropertyInput::Value(AstPropertyValue::DateTime(1_700_000_000_123)),
        ),
        ("b", PropertyInput::from(true)),
        ("bytes", PropertyInput::from(vec![0_u8, 255])),
        ("ia", PropertyInput::from(vec![1_i64, -2])),
        ("fa", PropertyInput::from(vec![0.5_f64, 1e21])),
        ("ga", PropertyInput::from(vec![0.1_f32])),
        (
            "sa",
            PropertyInput::from(vec!["x".to_string(), String::new()]),
        ),
        (
            "obj",
            PropertyInput::from(BTreeMap::from([
                ("z".to_string(), AstPropertyValue::from(1_i64)),
                ("a".to_string(), AstPropertyValue::from(vec![0.1_f32])),
            ])),
        ),
    ];
    let write = write_batch()
        .var_as("doc", g().add_n("Doc", properties.clone()))
        .var_as(
            "other",
            g().add_n("Doc", vec![("s", PropertyInput::from("b"))]),
        )
        .var_as(
            "link",
            g().n(NodeRef::var("doc"))
                .add_e("LINKS", NodeRef::var("other"), properties),
        )
        .returning(["doc", "other", "link"]);
    db.query(QueryRequest::write(write))
        .await
        .expect("fixture writes");
    db
}

fn embedded_reads() -> Vec<(&'static str, QueryRequest)> {
    vec![
        (
            "elements",
            QueryRequest::read(
                read_batch()
                    .var_as("nodes", g().n_with_label("Doc"))
                    .var_as("edges", g().e(EdgeRef::all()))
                    .var_as("folded", g().n_with_label("Doc").fold())
                    .var_as("none", g().n_with_label("Missing"))
                    .returning(["nodes", "edges", "folded", "none"]),
            ),
        ),
        (
            "projections",
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "map",
                        g().n_with_label("Doc").value_map(None::<Vec<String>>),
                    )
                    .var_as(
                        "selected",
                        g().n_with_label("Doc")
                            .value_map(Some(vec!["$id", "g", "dt"])),
                    )
                    .var_as(
                        "values",
                        g().n_with_label("Doc").values(vec!["f", "obj", "s"]),
                    )
                    .var_as("edge_properties", g().e(EdgeRef::all()).edge_properties())
                    .var_as("ids", g().n_with_label("Doc").id())
                    .var_as("labels", g().n_with_label("Doc").label())
                    .var_as("count", g().n_with_label("Doc").count())
                    .var_as("exists", g().n_with_label("Missing").exists())
                    .var_as("groups", g().n_with_label("Doc").group_count("s"))
                    .returning([
                        "map",
                        "selected",
                        "values",
                        "edge_properties",
                        "ids",
                        "labels",
                        "count",
                        "exists",
                        "groups",
                    ]),
            ),
        ),
        (
            "annotated",
            QueryRequest::read(
                read_batch()
                    .var_as("paths", g().n_with_label("Doc").out(Some("LINKS")).path())
                    .var_as(
                        "bound",
                        g().n_with_label("Doc").bind("start").out(Some("LINKS")),
                    )
                    .returning(["paths", "bound"]),
            ),
        ),
    ]
}

/// Exact `HelixDB::query_json` bytes for every embedded read.
const EMBEDDED_GOLDEN: &[(&str, &str)] = &[
    ("elements", "{\"edges\":[{\"$id\":0}],\"folded\":[{\"$id\":0},{\"$id\":1}],\"nodes\":[{\"$id\":0},{\"$id\":1}],\"none\":[]}"),
    ("projections", "{\"count\":2,\"edge_properties\":[{\"$from\":0,\"$id\":0,\"$label\":\"LINKS\",\"$to\":1,\"b\":true,\"bytes\":[0,255],\"dt\":\"2023-11-14T22:13:20.123Z\",\"f\":0.1,\"fa\":[0.5,1e+21],\"g\":0.10000000149011612,\"ga\":[0.10000000149011612],\"i\":-9223372036854775808,\"ia\":[1,-2],\"obj\":{\"a\":[0.10000000149011612],\"z\":1},\"s\":\"quote\\\" é 😀 \\n\",\"sa\":[\"x\",\"\"]}],\"exists\":false,\"groups\":[{\"count\":1,\"s\":\"b\"},{\"count\":1,\"s\":\"quote\\\" é 😀 \\n\"}],\"ids\":[0,1],\"labels\":[\"Doc\",\"Doc\"],\"map\":[{\"$id\":0,\"$label\":\"Doc\",\"b\":true,\"bytes\":[0,255],\"dt\":\"2023-11-14T22:13:20.123Z\",\"f\":0.1,\"fa\":[0.5,1e+21],\"g\":0.10000000149011612,\"ga\":[0.10000000149011612],\"i\":-9223372036854775808,\"ia\":[1,-2],\"obj\":{\"a\":[0.10000000149011612],\"z\":1},\"s\":\"quote\\\" é 😀 \\n\",\"sa\":[\"x\",\"\"]},{\"$id\":1,\"$label\":\"Doc\",\"s\":\"b\"}],\"selected\":[{\"$id\":0,\"dt\":\"2023-11-14T22:13:20.123Z\",\"g\":0.10000000149011612},{\"$id\":1}],\"values\":[{\"f\":0.1,\"obj\":{\"a\":[0.10000000149011612],\"z\":1},\"s\":\"quote\\\" é 😀 \\n\"},{\"s\":\"b\"}]}"),
    ("annotated", "{\"bound\":[{\"bindings\":{\"start\":{\"node\":0}},\"current\":{\"node\":1}}],\"paths\":[{\"bindings\":{},\"current\":{\"node\":1},\"path\":[{\"node\":0},{\"node\":1}]}]}"),
];

#[tokio::test]
async fn embedded_api_results_match_golden_bytes_and_the_json_api() {
    let db = embedded_fixture("response-encoding-embedded").await;
    let goldens = EMBEDDED_GOLDEN.iter().copied().collect::<BTreeMap<_, _>>();
    let mut mismatches = Vec::new();
    for (case, request) in embedded_reads() {
        let bytes = db
            .query_json(&request.to_json_bytes().expect("request serializes"))
            .await
            .expect("embedded JSON query executes");
        let value = db.query(request).await.expect("embedded query executes");
        let rendered = String::from_utf8(bytes).expect("response is UTF-8");
        assert_eq!(
            sonic_rs::from_str::<JsonValue>(&rendered).expect("response parses"),
            value,
            "{case}: embedded value and JSON bytes agree"
        );
        if goldens.get(case) != Some(&rendered.as_str()) {
            mismatches.push(format!("    ({case:?}, {rendered:?}),"));
        }
    }
    assert!(
        mismatches.is_empty() && goldens.len() == embedded_reads().len(),
        "embedded golden mismatch; actual entries:\n{}",
        mismatches.join("\n")
    );
}

#[tokio::test]
async fn embedded_api_reports_unrenderable_datetimes_identically() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "response-encoding-invalid-datetime".to_string(),
    })
    .await
    .expect("embedded database opens");
    let write = write_batch()
        .var_as(
            "doc",
            g().add_n(
                "Doc",
                vec![(
                    "dt",
                    PropertyInput::Value(AstPropertyValue::DateTime(i64::MAX)),
                )],
            ),
        )
        .var_as("count", g().n_with_label("Doc").count())
        .returning(["count"]);
    db.query(QueryRequest::write(write))
        .await
        .expect("unrenderable datetime is stored");
    let read = QueryRequest::read(
        read_batch()
            .var_as("map", g().n_with_label("Doc").value_map(Some(vec!["dt"])))
            .returning(["map"]),
    );
    let json_error = db
        .query_json(&read.to_json_bytes().expect("request serializes"))
        .await
        .expect_err("unrenderable datetime fails JSON encoding");
    let value_error = db
        .query(read)
        .await
        .expect_err("unrenderable datetime fails value encoding");
    let expected = "Query error: json serialization error: datetime millis '9223372036854775807' \
                    cannot be rendered as RFC3339";
    assert_eq!(json_error.to_string(), expected);
    assert_eq!(value_error.to_string(), expected);
}
