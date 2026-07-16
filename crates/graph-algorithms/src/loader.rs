//! Validation boundary for graph-query response bytes.
//!
//! SDKs own query construction. This module owns the shared response aliases
//! and turns the resulting bytes into a graph only after every row and
//! topology invariant has been validated.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::{Edge, ExternalId, Graph, GraphError, GraphKind, IdentitySelection, Node};

/// Prefix reserved for graph loader projection aliases.
pub const PRIVATE_PREFIX: &str = "__helix_graph_";
/// Projected internal node ID alias.
pub const NODE_ID: &str = "__helix_graph_node_id";
/// Projected public external node ID alias.
pub const EXTERNAL_ID: &str = "__helix_graph_external_id";
/// Projected node label alias.
pub const NODE_LABEL: &str = "__helix_graph_node_label";
/// Projected stable edge ID alias.
pub const EDGE_ID: &str = "__helix_graph_edge_id";
/// Projected Graphify multigraph key alias.
pub const EDGE_KEY: &str = "__helix_graph_edge_key";
/// Projected internal source-node ID alias.
pub const EDGE_SOURCE: &str = "__helix_graph_edge_source";
/// Projected internal target-node ID alias.
pub const EDGE_TARGET: &str = "__helix_graph_edge_target";
/// Projected edge label alias.
pub const EDGE_LABEL: &str = "__helix_graph_edge_label";
/// Projected algorithm weight alias.
pub const EDGE_WEIGHT: &str = "__helix_graph_edge_weight";

/// Metadata required to interpret one graph query response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLoadSpec {
    /// Declared topology contract of the constructed graph.
    pub kind: GraphKind,
    /// How projected node identities are decoded.
    pub node_identity: IdentitySelection,
    /// How an optional projected Graphify edge key is decoded.
    pub edge_key_identity: Option<IdentitySelection>,
    /// Maximum complete node result size.
    pub node_limit: Option<usize>,
    /// Maximum complete edge result size.
    pub edge_limit: Option<usize>,
}

/// Failure to validate raw graph response bytes.
#[derive(Debug, Error)]
pub enum GraphLoadError {
    /// The response is not the expected two-array result object.
    #[error("invalid graph query response: {0}")]
    InvalidResponse(String),
    /// A projected row is missing or contains an invalid field.
    #[error("invalid {kind} row {index}: {details}")]
    InvalidRow {
        /// `node` or `edge`.
        kind: &'static str,
        /// Zero-based row index.
        index: usize,
        /// Validation details.
        details: String,
    },
    /// More rows existed than the explicit safety limit permits.
    #[error("graph selection exceeded the {kind} safety limit of {limit}")]
    IncompleteSelection {
        /// `node` or `edge`.
        kind: &'static str,
        /// Configured maximum complete result size.
        limit: usize,
    },
    /// Two internal nodes map to the same external identity.
    #[error("duplicate external node identity: {0}")]
    DuplicateExternalIdentity(ExternalId),
    /// Graph metadata selection returned more than one row.
    #[error("graph metadata selection returned {count} rows; expected at most one")]
    MultipleGraphMetadataRows {
        /// Number of returned rows.
        count: usize,
    },
    /// Graph construction found a topology invariant violation.
    #[error(transparent)]
    Graph(#[from] GraphError),
}

#[derive(Debug, Deserialize)]
struct GraphResponse {
    nodes: Vec<BTreeMap<String, Value>>,
    edges: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    metadata: Vec<BTreeMap<String, Value>>,
}

/// Validate raw `/v1/query` response bytes and construct an immutable graph.
pub fn graph_from_response(spec: GraphLoadSpec, response: &[u8]) -> Result<Graph, GraphLoadError> {
    let response: GraphResponse = serde_json::from_slice(response)
        .map_err(|error| GraphLoadError::InvalidResponse(error.to_string()))?;
    if let Some(limit) = spec.node_limit
        && response.nodes.len() > limit
    {
        return Err(GraphLoadError::IncompleteSelection {
            kind: "node",
            limit,
        });
    }
    if let Some(limit) = spec.edge_limit
        && response.edges.len() > limit
    {
        return Err(GraphLoadError::IncompleteSelection {
            kind: "edge",
            limit,
        });
    }
    let graph_attributes = match response.metadata.len() {
        0 => BTreeMap::new(),
        1 => {
            let attributes = response
                .metadata
                .into_iter()
                .next()
                .expect("metadata length is one");
            if let Some(alias) = attributes
                .keys()
                .find(|alias| alias.starts_with(PRIVATE_PREFIX))
            {
                return Err(GraphLoadError::InvalidRow {
                    kind: "metadata",
                    index: 0,
                    details: format!("reserved projection alias {alias}"),
                });
            }
            attributes
        }
        count => return Err(GraphLoadError::MultipleGraphMetadataRows { count }),
    };

    let mut internal_to_external = BTreeMap::new();
    let mut external_ids = BTreeSet::new();
    let nodes = response
        .nodes
        .into_iter()
        .enumerate()
        .map(|(index, mut row)| {
            let internal = take_identity(&mut row, NODE_ID, "node", index)?;
            let external =
                take_external_id(&mut row, EXTERNAL_ID, "node", index, &spec.node_identity)?;
            if internal_to_external
                .insert(internal.clone(), external.clone())
                .is_some()
            {
                return Err(invalid_row(
                    "node",
                    index,
                    format!("duplicate internal ID {internal}"),
                ));
            }
            if !external_ids.insert(external.clone()) {
                return Err(GraphLoadError::DuplicateExternalIdentity(external));
            }
            let label = take_optional_string(&mut row, NODE_LABEL, "node", index)?;
            let mut node = Node::new(external).with_attributes(row);
            if let Some(label) = label {
                node = node.with_label(label);
            }
            Ok(node)
        })
        .collect::<Result<Vec<_>, GraphLoadError>>()?;

    let edges = response
        .edges
        .into_iter()
        .enumerate()
        .map(|(index, mut row)| {
            let edge_id = take_identity(&mut row, EDGE_ID, "edge", index)?;
            let source_internal = take_identity(&mut row, EDGE_SOURCE, "edge", index)?;
            let target_internal = take_identity(&mut row, EDGE_TARGET, "edge", index)?;
            let source = internal_to_external
                .get(&source_internal)
                .cloned()
                .ok_or_else(|| {
                    invalid_row(
                        "edge",
                        index,
                        format!("source {source_internal} is outside the node selection"),
                    )
                })?;
            let target = internal_to_external
                .get(&target_internal)
                .cloned()
                .ok_or_else(|| {
                    invalid_row(
                        "edge",
                        index,
                        format!("target {target_internal} is outside the node selection"),
                    )
                })?;
            let key = match &spec.edge_key_identity {
                Some(selection) => {
                    take_optional_external_id(&mut row, EDGE_KEY, "edge", index, selection)?
                }
                None => {
                    row.remove(EDGE_KEY);
                    None
                }
            };
            let label = take_optional_string(&mut row, EDGE_LABEL, "edge", index)?;
            let weight = take_optional_weight(&mut row, EDGE_WEIGHT, index)?;
            let mut edge = Edge::new(edge_id, source, target).with_attributes(row);
            if let Some(key) = key {
                edge = edge.with_graphify_key(key);
            }
            if let Some(label) = label {
                edge = edge.with_label(label);
            }
            if let Some(weight) = weight {
                edge = edge.with_weight(weight);
            }
            Ok(edge)
        })
        .collect::<Result<Vec<_>, GraphLoadError>>()?;
    Graph::with_attributes(spec.kind, graph_attributes, nodes, edges).map_err(Into::into)
}

fn take_identity(
    row: &mut BTreeMap<String, Value>,
    field: &str,
    kind: &'static str,
    index: usize,
) -> Result<String, GraphLoadError> {
    let value = row
        .remove(field)
        .ok_or_else(|| invalid_row(kind, index, format!("missing {field}")))?;
    identity(value).ok_or_else(|| invalid_row(kind, index, format!("invalid {field}")))
}

fn take_external_id(
    row: &mut BTreeMap<String, Value>,
    field: &str,
    kind: &'static str,
    index: usize,
    selection: &IdentitySelection,
) -> Result<ExternalId, GraphLoadError> {
    let value = row
        .remove(field)
        .ok_or_else(|| invalid_row(kind, index, format!("missing {field}")))?;
    decode_external_id(value, selection)
        .map_err(|error| invalid_row(kind, index, format!("invalid {field}: {error}")))
}

fn take_optional_external_id(
    row: &mut BTreeMap<String, Value>,
    field: &str,
    kind: &'static str,
    index: usize,
    selection: &IdentitySelection,
) -> Result<Option<ExternalId>, GraphLoadError> {
    match row.remove(field) {
        None => Ok(None),
        Some(value) => decode_external_id(value, selection)
            .map(Some)
            .map_err(|error| invalid_row(kind, index, format!("invalid {field}: {error}"))),
    }
}

fn decode_external_id(
    value: Value,
    selection: &IdentitySelection,
) -> Result<ExternalId, GraphError> {
    match selection {
        IdentitySelection::InternalId | IdentitySelection::ScalarProperty(_) => {
            ExternalId::from_scalar(value)
        }
        IdentitySelection::TaggedProperty(_) => ExternalId::from_tagged_value(value),
    }
}

fn identity(value: Value) -> Option<String> {
    match value {
        Value::String(value) if !value.is_empty() => Some(value),
        Value::Number(value) => Some(value.to_string()),
        Value::Null | Value::Bool(_) | Value::String(_) | Value::Array(_) | Value::Object(_) => {
            None
        }
    }
}

fn take_optional_string(
    row: &mut BTreeMap<String, Value>,
    field: &str,
    kind: &'static str,
    index: usize,
) -> Result<Option<String>, GraphLoadError> {
    match row.remove(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(invalid_row(kind, index, format!("invalid {field}"))),
    }
}

fn take_optional_weight(
    row: &mut BTreeMap<String, Value>,
    field: &str,
    index: usize,
) -> Result<Option<f64>, GraphLoadError> {
    match row.remove(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_f64()
            .filter(|weight| weight.is_finite() && *weight >= 0.0)
            .map(Some)
            .ok_or_else(|| invalid_row("edge", index, format!("invalid {field}"))),
        Some(_) => Err(invalid_row("edge", index, format!("invalid {field}"))),
    }
}

fn invalid_row(kind: &'static str, index: usize, details: String) -> GraphLoadError {
    GraphLoadError::InvalidRow {
        kind,
        index,
        details,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::NodeId;

    fn response(nodes: Value, edges: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({ "nodes": nodes, "edges": edges })).expect("fixture JSON")
    }

    fn response_with_metadata(nodes: Value, edges: Value, metadata: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "nodes": nodes,
            "edges": edges,
            "metadata": metadata,
        }))
        .expect("fixture JSON")
    }

    fn spec() -> GraphLoadSpec {
        GraphLoadSpec {
            kind: GraphKind::DiGraph,
            node_identity: IdentitySelection::ScalarProperty(
                crate::GraphProperty::new("external_id").unwrap(),
            ),
            edge_key_identity: Some(IdentitySelection::ScalarProperty(
                crate::GraphProperty::new("key").unwrap(),
            )),
            node_limit: None,
            edge_limit: None,
        }
    }

    #[test]
    fn loader_preserves_identity_topology_properties_and_weights() {
        let bytes = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): "File", "name": "A" },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): "File", "name": "B" }
            ]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n2",
                (EDGE_KEY): "imports", (EDGE_LABEL): "DEPENDS_ON", (EDGE_WEIGHT): 2.5,
                "relation": "import"
            }]),
        );
        let graph = graph_from_response(spec(), &bytes).expect("valid graph response");
        assert_eq!(graph.node_count(), 2);
        let edge = graph.edge("e1").expect("edge");
        assert_eq!(
            (&edge.source, &edge.target),
            (&NodeId::from("a"), &NodeId::from("b"))
        );
        assert_eq!(edge.weight, Some(2.5));
    }

    #[test]
    fn loader_rejects_duplicate_external_identity_and_missing_endpoint() {
        let duplicate = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "same", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "same", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        assert!(matches!(
            graph_from_response(spec(), &duplicate),
            Err(GraphLoadError::DuplicateExternalIdentity(id)) if id == "same"
        ));

        let outside = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "missing",
                (EDGE_LABEL): null
            }]),
        );
        assert!(matches!(
            graph_from_response(spec(), &outside),
            Err(GraphLoadError::InvalidRow { kind: "edge", .. })
        ));
    }

    #[test]
    fn loader_rejects_malformed_rows_invalid_weights_and_truncation() {
        assert!(matches!(
            graph_from_response(spec(), b"[]"),
            Err(GraphLoadError::InvalidResponse(_))
        ));
        let bad_weight = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n1",
                (EDGE_LABEL): null, (EDGE_WEIGHT): "heavy"
            }]),
        );
        assert!(matches!(
            graph_from_response(spec(), &bad_weight),
            Err(GraphLoadError::InvalidRow { kind: "edge", .. })
        ));

        let nodes = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        assert!(matches!(
            graph_from_response(
                GraphLoadSpec {
                    node_limit: Some(1),
                    ..spec()
                },
                &nodes
            ),
            Err(GraphLoadError::IncompleteSelection {
                kind: "node",
                limit: 1
            })
        ));

        let edges = response(
            json!([{ (NODE_ID): 1, (EXTERNAL_ID): 10, (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): 2, (EDGE_SOURCE): 1, (EDGE_TARGET): 1,
                (EDGE_LABEL): null
            }]),
        );
        let graph = graph_from_response(spec(), &edges).expect("numeric identities are scalar");
        assert!(graph.contains_node(10_i64));
        assert!(matches!(
            graph_from_response(
                GraphLoadSpec {
                    edge_limit: Some(0),
                    ..spec()
                },
                &edges
            ),
            Err(GraphLoadError::IncompleteSelection {
                kind: "edge",
                limit: 0
            })
        ));
    }

    #[test]
    fn loader_rejects_duplicate_internal_ids_missing_sources_and_invalid_optional_fields() {
        let duplicate = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null },
                { (NODE_ID): "n1", (EXTERNAL_ID): "b", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        assert!(matches!(
            graph_from_response(spec(), &duplicate),
            Err(GraphLoadError::InvalidRow { kind: "node", .. })
        ));
        let missing_source = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): "e", (EDGE_SOURCE): "missing", (EDGE_TARGET): "n1",
                (EDGE_LABEL): null
            }]),
        );
        assert!(matches!(
            graph_from_response(spec(), &missing_source),
            Err(GraphLoadError::InvalidRow { kind: "edge", .. })
        ));
        let invalid_key = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): "e", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n1",
                (EDGE_KEY): {}, (EDGE_LABEL): 1
            }]),
        );
        assert!(matches!(
            graph_from_response(spec(), &invalid_key),
            Err(GraphLoadError::InvalidRow { kind: "edge", .. })
        ));
    }

    #[test]
    fn scalar_and_tagged_identities_preserve_types_without_collisions() {
        let scalar = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): 1, (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "1", (NODE_LABEL): null }
            ]),
            json!([]),
        );
        let graph = graph_from_response(spec(), &scalar).unwrap();
        assert!(graph.contains_node(1_i64));
        assert!(graph.contains_node("1"));

        let tuple =
            ExternalId::tuple(vec![ExternalId::from(1_i64), ExternalId::from("1")]).unwrap();
        let bytes = ExternalId::Bytes(vec![0, 255]);
        let tagged = response(
            json!([
                {
                    (NODE_ID): "n1",
                    (EXTERNAL_ID): serde_json::to_value(&tuple).unwrap(),
                    (NODE_LABEL): null
                },
                {
                    (NODE_ID): "n2",
                    (EXTERNAL_ID): serde_json::to_value(&bytes).unwrap(),
                    (NODE_LABEL): null
                }
            ]),
            json!([]),
        );
        let graph = graph_from_response(
            GraphLoadSpec {
                node_identity: IdentitySelection::TaggedProperty(
                    crate::GraphProperty::new("external_id").unwrap(),
                ),
                edge_key_identity: None,
                ..spec()
            },
            &tagged,
        )
        .unwrap();
        assert!(graph.contains_node(tuple));
        assert!(graph.contains_node(bytes));
    }

    #[test]
    fn internal_identity_and_no_edge_key_selection_are_explicit() {
        let bytes = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "n1", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "n2", (NODE_LABEL): null }
            ]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n2",
                (EDGE_KEY): {"ignored": true}, (EDGE_LABEL): null, (EDGE_WEIGHT): null
            }]),
        );
        let graph = graph_from_response(
            GraphLoadSpec {
                node_identity: IdentitySelection::InternalId,
                edge_key_identity: None,
                ..spec()
            },
            &bytes,
        )
        .unwrap();
        assert!(graph.contains_node("n1"));
        assert_eq!(graph.edge("e1").unwrap().graphify_key, None);
        assert!(!graph.edge("e1").unwrap().attributes.contains_key(EDGE_KEY));

        let invalid_label = response(
            json!([{ (NODE_ID): "n1", (EXTERNAL_ID): "n1", (NODE_LABEL): null }]),
            json!([{
                (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n1",
                (EDGE_LABEL): 1
            }]),
        );
        assert!(matches!(
            graph_from_response(spec(), &invalid_label),
            Err(GraphLoadError::InvalidRow { kind: "edge", .. })
        ));
    }

    #[test]
    fn declared_kind_controls_parallel_edges_even_for_small_graphs() {
        let parallel = response(
            json!([
                { (NODE_ID): "n1", (EXTERNAL_ID): "a", (NODE_LABEL): null },
                { (NODE_ID): "n2", (EXTERNAL_ID): "b", (NODE_LABEL): null }
            ]),
            json!([
                {
                    (EDGE_ID): "e1", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n2",
                    (EDGE_LABEL): null
                },
                {
                    (EDGE_ID): "e2", (EDGE_SOURCE): "n1", (EDGE_TARGET): "n2",
                    (EDGE_LABEL): null
                }
            ]),
        );
        assert!(matches!(
            graph_from_response(spec(), &parallel),
            Err(GraphLoadError::Graph(GraphError::ParallelEdge { .. }))
        ));
        let multigraph = graph_from_response(
            GraphLoadSpec {
                kind: GraphKind::MultiDiGraph,
                ..spec()
            },
            &parallel,
        )
        .unwrap();
        assert!(multigraph.is_multigraph());

        let empty = graph_from_response(
            GraphLoadSpec {
                kind: GraphKind::MultiGraph,
                ..spec()
            },
            &response(json!([]), json!([])),
        )
        .unwrap();
        assert!(empty.is_multigraph());
    }

    #[test]
    fn metadata_loads_once_and_rejects_multiple_or_reserved_rows() {
        let graph = graph_from_response(
            spec(),
            &response_with_metadata(json!([]), json!([]), json!([{"name": "demo"}])),
        )
        .unwrap();
        assert_eq!(graph.attributes()["name"], json!("demo"));

        assert!(matches!(
            graph_from_response(
                spec(),
                &response_with_metadata(
                    json!([]),
                    json!([]),
                    json!([{"name": "one"}, {"name": "two"}])
                )
            ),
            Err(GraphLoadError::MultipleGraphMetadataRows { count: 2 })
        ));
        assert!(matches!(
            graph_from_response(
                spec(),
                &response_with_metadata(json!([]), json!([]), json!([{(NODE_ID): "private"}]))
            ),
            Err(GraphLoadError::InvalidRow {
                kind: "metadata",
                ..
            })
        ));
    }
}
