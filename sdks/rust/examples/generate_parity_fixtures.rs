#![recursion_limit = "256"]

//! Generates canonical Rust SDK parity fixtures for cross-language comparison.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use helix_db::dsl::prelude::*;
use helix_db::{Empty, OnNodes, QueryParamType, ReadOnly, Traversal};

struct Fixture {
    bucket: &'static str,
    name: String,
    request: QueryRequest,
}

struct UserProps {
    external_id: &'static str,
    name: &'static str,
    age: i64,
    score: f64,
    status: &'static str,
    city: &'static str,
    bio: &'static str,
    embedding: Vec<f32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = env::args()
        .nth(1)
        .unwrap_or_else(|| "tests/parity/generated/rust".to_string());
    let out = Path::new(&out);
    reset_dir(&out.join("runtime"))?;
    reset_dir(&out.join("json-only"))?;

    let mut fixtures = runtime_fixtures();
    fixtures.extend(node_permutation_fixtures());
    fixtures.extend(json_only_fixtures());
    validate_fixture_coverage(&fixtures);

    match env::var("HELIX_EMBEDDED_PARITY_RESULTS") {
        Ok(results) => execute_embedded_fixtures(&fixtures, Path::new(&results)).await?,
        Err(env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }

    for fixture in fixtures {
        let path = out
            .join(fixture.bucket)
            .join(format!("{}.json", fixture.name));
        fs::write(path, fixture.request.to_json_string()?)?;
    }

    Ok(())
}

#[cfg(feature = "embedded")]
async fn execute_embedded_fixtures(
    fixtures: &[Fixture],
    results: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    const TRANSACTION_CONFLICT_ATTEMPTS: usize = 8;

    use helix_db::{CacheConfig, Client, DbConfig, HelixDbSource, HelixError};

    reset_dir(results)?;
    let database = match env::var("HELIX_EMBEDDED_PARITY_DATABASE") {
        Ok(database) => database,
        Err(env::VarError::NotPresent) => "rust-sdk-embedded-parity".to_string(),
        Err(error) => return Err(error.into()),
    };
    let storage =
        env::var("HELIX_EMBEDDED_PARITY_STORAGE").unwrap_or_else(|_| "memory".to_string());
    let disk_root = match storage.as_str() {
        "memory" => None,
        "disk" => Some(std::path::PathBuf::from(env::var(
            "HELIX_EMBEDDED_PARITY_DISK_ROOT",
        )?)),
        other => return Err(format!("unsupported embedded parity storage {other}").into()),
    };
    let source = || match &disk_root {
        Some(root) => HelixDbSource::Disk {
            root: root.clone(),
            database: database.clone(),
        },
        None => HelixDbSource::InMemory {
            database: database.clone(),
        },
    };
    let config = || DbConfig::new().with_cache(CacheConfig::default());
    let mut client = Client::open_with_config(source(), config()).await?;
    let mut runtime_fixtures = fixtures
        .iter()
        .filter(|fixture| fixture.bucket == "runtime")
        .collect::<Vec<_>>();
    runtime_fixtures.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    for fixture in runtime_fixtures {
        if disk_root.is_some() && fixture.name == "900-write-active-text-items" {
            client.close().await?;
            let reader = Client::open_reader_with_config(source(), config()).await?;
            for search_name in ["025-read-text-search-nodes", "027-read-text-search-edges"] {
                let search = fixtures
                    .iter()
                    .find(|candidate| candidate.name == search_name)
                    .expect("persisted search fixture exists");
                let actual: serde_json::Value = reader.query(search.request.clone()).send().await?;
                let expected: serde_json::Value = serde_json::from_slice(&fs::read(
                    results.join(format!("{}.json", search.name)),
                )?)?;
                if actual != expected {
                    return Err(
                        format!("{} changed after reopening a disk reader", search.name).into(),
                    );
                }
            }
            reader.close().await?;
            client = Client::open_with_config(source(), config()).await?;
        }
        let mut attempt = 0;
        let mut response: serde_json::Value = loop {
            match client.query(fixture.request.clone()).send().await {
                Ok(response) => break response,
                // Embedded errors preserve retry classification in their DB
                // details even though the SDK error surface is transport-neutral.
                Err(HelixError::EmbeddedError { details, .. })
                    if details.contains("Transaction error: transaction conflict")
                        && attempt + 1 < TRANSACTION_CONFLICT_ATTEMPTS =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10_u64 << attempt)).await;
                    attempt += 1;
                }
                Err(error) => return Err(format!("{}: {error}", fixture.name).into()),
            }
        };
        await_embedded_index_operations(&client, &response)
            .await
            .map_err(|error| format!("{}: {error}", fixture.name))?;
        normalize_embedded_operation_ids(&mut response);
        fs::write(
            results.join(format!("{}.json", fixture.name)),
            serde_json::to_vec(&response)?,
        )?;
    }
    for search_name in ["025-read-text-search-nodes", "027-read-text-search-edges"] {
        let search = fixtures
            .iter()
            .find(|candidate| candidate.name == search_name)
            .expect("post-drop search fixture exists");
        let error = client
            .query::<serde_json::Value>(search.request.clone())
            .send()
            .await
            .expect_err("search after index DROP must fail");
        if !error.to_string().contains("index_not_found") {
            return Err(format!(
                "{} returned the wrong post-DROP error: {error}",
                search.name
            )
            .into());
        }
    }
    client.close().await?;
    Ok(())
}

/// Waits for asynchronous embedded DDL receipts before a later fixture uses the index.
#[cfg(feature = "embedded")]
async fn await_embedded_index_operations(
    client: &helix_db::Client,
    response: &serde_json::Value,
) -> Result<(), String> {
    let mut operation_ids = BTreeSet::new();
    collect_embedded_operation_ids(response, &mut operation_ids);
    for operation_id in operation_ids {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let request = QueryRequest::read(
                read_batch()
                    .var_as("status", g().get_index_operation(operation_id.clone()))
                    .returning(["status"]),
            );
            let status: serde_json::Value = client
                .query(request)
                .send()
                .await
                .map_err(|error| format!("operation {operation_id} status failed: {error}"))?;
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => {}
                Some(other) => {
                    return Err(format!(
                        "operation {operation_id} reached unexpected status {other}: {status}"
                    ));
                }
                None => {
                    return Err(format!(
                        "operation {operation_id} returned malformed status: {status}"
                    ));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "operation {operation_id} did not finish within 60s"
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    Ok(())
}

/// Collects operation IDs only from DDL receipts, not from unrelated result objects.
#[cfg(feature = "embedded")]
fn collect_embedded_operation_ids(value: &serde_json::Value, ids: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_embedded_operation_ids(value, ids);
            }
        }
        serde_json::Value::Object(object) => {
            if matches!(
                object.get("kind").and_then(serde_json::Value::as_str),
                Some("accepted" | "existing_operation")
            ) && let Some(operation_id) = object
                .get("operation_id")
                .and_then(serde_json::Value::as_str)
            {
                ids.insert(operation_id.to_string());
            }
            for value in object.values() {
                collect_embedded_operation_ids(value, ids);
            }
        }
        _ => {}
    }
}

/// Removes random operation UUIDs while preserving the receipt shape compared across SDKs.
#[cfg(feature = "embedded")]
fn normalize_embedded_operation_ids(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_embedded_operation_ids(value);
            }
        }
        serde_json::Value::Object(object) => {
            if matches!(
                object.get("kind").and_then(serde_json::Value::as_str),
                Some("accepted" | "existing_operation")
            ) && object.contains_key("operation_id")
            {
                object.insert(
                    "operation_id".to_string(),
                    serde_json::Value::String("<operation-id>".to_string()),
                );
            }
            for value in object.values_mut() {
                normalize_embedded_operation_ids(value);
            }
        }
        _ => {}
    }
}

#[cfg(not(feature = "embedded"))]
async fn execute_embedded_fixtures(
    _fixtures: &[Fixture],
    _results: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("HELIX_EMBEDDED_PARITY_RESULTS requires the embedded feature".into())
}

#[derive(Default)]
struct FixtureCoverage {
    ast_nodes: BTreeSet<&'static str>,
    expressions: BTreeSet<&'static str>,
    predicates: BTreeSet<&'static str>,
    compare_ops: BTreeSet<&'static str>,
    property_values: BTreeSet<&'static str>,
    property_inputs: BTreeSet<&'static str>,
    node_refs: BTreeSet<&'static str>,
    edge_refs: BTreeSet<&'static str>,
    projections: BTreeSet<&'static str>,
    binding_targets: BTreeSet<&'static str>,
    binding_projections: BTreeSet<&'static str>,
    stream_bounds: BTreeSet<&'static str>,
    index_specs: BTreeSet<&'static str>,
    range_directions: BTreeSet<&'static str>,
    vector_metrics: BTreeSet<&'static str>,
    orders: BTreeSet<&'static str>,
    shortest_path_directions: BTreeSet<&'static str>,
    emit_behaviors: BTreeSet<&'static str>,
    aggregate_functions: BTreeSet<&'static str>,
    batch_conditions: BTreeSet<&'static str>,
    batch_entries: BTreeSet<&'static str>,
    batch_queries: BTreeSet<&'static str>,
    query_param_types: BTreeSet<&'static str>,
    query_values: BTreeSet<&'static str>,
}

fn validate_fixture_coverage(fixtures: &[Fixture]) {
    assert_complete_fixture_coverage(&fixture_coverage(fixtures.iter()));
    assert_complete_fixture_coverage(&fixture_coverage(
        fixtures
            .iter()
            .filter(|fixture| fixture.bucket == "json-only" && fixture.name.starts_with('9')),
    ));
}

fn fixture_coverage<'a>(fixtures: impl Iterator<Item = &'a Fixture>) -> FixtureCoverage {
    let mut coverage = FixtureCoverage::default();
    for fixture in fixtures {
        visit_request(&fixture.request, &mut coverage);
    }
    coverage
}

fn assert_complete_fixture_coverage(coverage: &FixtureCoverage) {
    assert_variants(
        "AstNode",
        &coverage.ast_nodes,
        &[
            "Context",
            "Nodes",
            "NodesWhere",
            "Edges",
            "EdgesWhere",
            "VectorSearchNodes",
            "VectorSearchNodesWithin",
            "TextSearchNodes",
            "TextSearchNodesWithin",
            "VectorSearchEdges",
            "VectorSearchEdgesWithin",
            "TextSearchEdges",
            "TextSearchEdgesWithin",
            "Out",
            "In",
            "Both",
            "OutE",
            "InE",
            "BothE",
            "OutN",
            "InN",
            "OtherN",
            "Has",
            "HasLabel",
            "HasKey",
            "Where",
            "Dedup",
            "Within",
            "Without",
            "EdgeHas",
            "EdgeHasLabel",
            "Limit",
            "Skip",
            "Range",
            "As",
            "Store",
            "Select",
            "Bind",
            "Inject",
            "Count",
            "Exists",
            "Id",
            "Label",
            "Values",
            "ValueMap",
            "Project",
            "ProjectBindings",
            "EdgeProperties",
            "CreateIndex",
            "DropIndex",
            "GetIndexOperation",
            "RetryIndexOperation",
            "AbortIndexOperation",
            "AddN",
            "AddE",
            "SetProperty",
            "RemoveProperty",
            "Drop",
            "DropEdge",
            "DropEdgeLabeled",
            "DropEdgeById",
            "OrderBy",
            "OrderByMultiple",
            "Repeat",
            "Union",
            "Choose",
            "Coalesce",
            "Optional",
            "Group",
            "GroupCount",
            "AggregateBy",
            "Fold",
            "Unfold",
            "Path",
            "SimplePath",
            "WithSack",
            "SackSet",
            "SackAdd",
            "SackGet",
            "ShortestPath",
        ],
    );
    assert_variants(
        "Expr",
        &coverage.expressions,
        &[
            "Property",
            "Id",
            "Timestamp",
            "DateTimeNow",
            "Constant",
            "Param",
            "Add",
            "Sub",
            "Mul",
            "Div",
            "Mod",
            "Neg",
            "Case",
        ],
    );
    assert_variants(
        "Predicate",
        &coverage.predicates,
        &[
            "Eq",
            "Neq",
            "Gt",
            "Gte",
            "Lt",
            "Lte",
            "Between",
            "HasKey",
            "IsNull",
            "IsNotNull",
            "StartsWith",
            "EndsWith",
            "Contains",
            "IsIn",
            "And",
            "Or",
            "Not",
            "Compare",
        ],
    );
    assert_variants(
        "CompareOp",
        &coverage.compare_ops,
        &["Eq", "Neq", "Gt", "Gte", "Lt", "Lte"],
    );
    assert_variants(
        "PropertyValue",
        &coverage.property_values,
        &[
            "Null",
            "Bool",
            "I64",
            "DateTime",
            "F64",
            "F32",
            "String",
            "Bytes",
            "I64Array",
            "F64Array",
            "F32Array",
            "StringArray",
            "Array",
            "Object",
        ],
    );
    assert_variants(
        "PropertyInput",
        &coverage.property_inputs,
        &["Value", "Expr"],
    );
    assert_variants(
        "NodeRef",
        &coverage.node_refs,
        &["All", "Ids", "Var", "Param"],
    );
    assert_variants(
        "EdgeRef",
        &coverage.edge_refs,
        &["All", "Ids", "Var", "Param"],
    );
    assert_variants("Projection", &coverage.projections, &["Property", "Expr"]);
    assert_variants(
        "BindingTarget",
        &coverage.binding_targets,
        &["Current", "Binding"],
    );
    assert_variants(
        "BindingProjection",
        &coverage.binding_projections,
        &["Property", "Coalesce"],
    );
    assert_variants("StreamBound", &coverage.stream_bounds, &["Literal", "Expr"]);
    assert_variants(
        "IndexSpec",
        &coverage.index_specs,
        &[
            "NodeEquality",
            "NodeRange",
            "EdgeEquality",
            "EdgeRange",
            "NodeVector",
            "NodeText",
            "EdgeVector",
            "EdgeText",
        ],
    );
    assert_variants(
        "RangeIndexDirection",
        &coverage.range_directions,
        &["Asc", "Desc"],
    );
    assert_variants(
        "VectorDistanceMetric",
        &coverage.vector_metrics,
        &["Cosine", "Euclidean", "Manhattan"],
    );
    assert_variants("Order", &coverage.orders, &["Asc", "Desc"]);
    assert_variants(
        "ShortestPathDirection",
        &coverage.shortest_path_directions,
        &["Out", "In", "Both"],
    );
    assert_variants(
        "EmitBehavior",
        &coverage.emit_behaviors,
        &["None", "Before", "After", "All"],
    );
    assert_variants(
        "AggregateFunction",
        &coverage.aggregate_functions,
        &["Count", "Sum", "Min", "Max", "Mean"],
    );
    assert_variants(
        "BatchCondition",
        &coverage.batch_conditions,
        &["VarNotEmpty", "VarEmpty", "VarMinSize", "PrevNotEmpty"],
    );
    assert_variants("BatchEntry", &coverage.batch_entries, &["Query", "ForEach"]);
    assert_variants("BatchQuery", &coverage.batch_queries, &["Read", "Write"]);
    assert_variants(
        "QueryParamType",
        &coverage.query_param_types,
        &[
            "Bool", "I64", "F64", "F32", "String", "DateTime", "Value", "Object", "Array",
        ],
    );
    assert_variants(
        "QueryValue",
        &coverage.query_values,
        &[
            "Null", "Bool", "I64", "F64", "F32", "String", "Array", "Object",
        ],
    );
}

fn assert_variants(label: &str, actual: &BTreeSet<&str>, expected: &[&'static str]) {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual, &expected, "{label} fixture coverage is incomplete");
}

fn visit_request(request: &QueryRequest, coverage: &mut FixtureCoverage) {
    match request.query() {
        BatchQuery::Read(batch) => {
            coverage.batch_queries.insert("Read");
            visit_batch(batch.entries(), coverage);
        }
        BatchQuery::Write(batch) => {
            coverage.batch_queries.insert("Write");
            visit_batch(&batch.entries, coverage);
        }
    }
    if let Some(parameters) = request.parameters() {
        for value in parameters.values() {
            visit_query_value(value, coverage);
        }
    }
    if let Some(parameter_types) = request.parameter_types() {
        for param_type in parameter_types.values() {
            visit_query_param_type(param_type, coverage);
        }
    }
}

fn visit_batch(entries: &[BatchEntry], coverage: &mut FixtureCoverage) {
    for entry in entries {
        match entry {
            BatchEntry::Query(query) => {
                coverage.batch_entries.insert("Query");
                if let Some(condition) = &query.condition {
                    visit_batch_condition(condition, coverage);
                }
                visit_ast_node(&query.root, coverage);
            }
            BatchEntry::ForEach { body, .. } => {
                coverage.batch_entries.insert("ForEach");
                visit_batch(body, coverage);
            }
        }
    }
}

fn visit_batch_condition(condition: &BatchCondition, coverage: &mut FixtureCoverage) {
    coverage.batch_conditions.insert(match condition {
        BatchCondition::VarNotEmpty(_) => "VarNotEmpty",
        BatchCondition::VarEmpty(_) => "VarEmpty",
        BatchCondition::VarMinSize(_, _) => "VarMinSize",
        BatchCondition::PrevNotEmpty => "PrevNotEmpty",
    });
}

fn visit_ast_node(node: &AstNode, coverage: &mut FixtureCoverage) {
    match node {
        AstNode::Context => {
            coverage.ast_nodes.insert("Context");
        }
        AstNode::Nodes { reference } => {
            coverage.ast_nodes.insert("Nodes");
            visit_node_ref(reference, coverage);
        }
        AstNode::NodesWhere { predicate } => {
            coverage.ast_nodes.insert("NodesWhere");
            visit_predicate(predicate, coverage);
        }
        AstNode::Edges { reference } => {
            coverage.ast_nodes.insert("Edges");
            visit_edge_ref(reference, coverage);
        }
        AstNode::EdgesWhere { predicate } => {
            coverage.ast_nodes.insert("EdgesWhere");
            visit_predicate(predicate, coverage);
        }
        AstNode::VectorSearchNodes {
            tenant_value,
            query_vector,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("VectorSearchNodes");
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_vector, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::VectorSearchNodesWithin {
            input,
            tenant_value,
            query_vector,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("VectorSearchNodesWithin");
            visit_ast_node(input, coverage);
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_vector, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::TextSearchNodes {
            tenant_value,
            query_text,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("TextSearchNodes");
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_text, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::TextSearchNodesWithin {
            input,
            tenant_value,
            query_text,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("TextSearchNodesWithin");
            visit_ast_node(input, coverage);
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_text, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::VectorSearchEdges {
            tenant_value,
            query_vector,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("VectorSearchEdges");
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_vector, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::VectorSearchEdgesWithin {
            input,
            tenant_value,
            query_vector,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("VectorSearchEdgesWithin");
            visit_ast_node(input, coverage);
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_vector, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::TextSearchEdges {
            tenant_value,
            query_text,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("TextSearchEdges");
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_text, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::TextSearchEdgesWithin {
            input,
            tenant_value,
            query_text,
            k,
            ..
        } => {
            coverage.ast_nodes.insert("TextSearchEdgesWithin");
            visit_ast_node(input, coverage);
            if let Some(value) = tenant_value {
                visit_property_input(value, coverage);
            }
            visit_property_input(query_text, coverage);
            visit_stream_bound(k, coverage);
        }
        AstNode::Out { input, .. } => visit_input("Out", input, coverage),
        AstNode::In { input, .. } => visit_input("In", input, coverage),
        AstNode::Both { input, .. } => visit_input("Both", input, coverage),
        AstNode::OutE { input, .. } => visit_input("OutE", input, coverage),
        AstNode::InE { input, .. } => visit_input("InE", input, coverage),
        AstNode::BothE { input, .. } => visit_input("BothE", input, coverage),
        AstNode::OutN { input } => visit_input("OutN", input, coverage),
        AstNode::InN { input } => visit_input("InN", input, coverage),
        AstNode::OtherN { input } => visit_input("OtherN", input, coverage),
        AstNode::Has { input, value, .. } => {
            coverage.ast_nodes.insert("Has");
            visit_ast_node(input, coverage);
            visit_property_value(value, coverage);
        }
        AstNode::HasLabel { input, .. } => visit_input("HasLabel", input, coverage),
        AstNode::HasKey { input, .. } => visit_input("HasKey", input, coverage),
        AstNode::Where {
            input, predicate, ..
        } => {
            coverage.ast_nodes.insert("Where");
            visit_ast_node(input, coverage);
            visit_predicate(predicate, coverage);
        }
        AstNode::Dedup { input } => visit_input("Dedup", input, coverage),
        AstNode::Within { input, .. } => visit_input("Within", input, coverage),
        AstNode::Without { input, .. } => visit_input("Without", input, coverage),
        AstNode::EdgeHas { input, value, .. } => {
            coverage.ast_nodes.insert("EdgeHas");
            visit_ast_node(input, coverage);
            visit_property_input(value, coverage);
        }
        AstNode::EdgeHasLabel { input, .. } => visit_input("EdgeHasLabel", input, coverage),
        AstNode::Limit { input, count } => {
            coverage.ast_nodes.insert("Limit");
            visit_ast_node(input, coverage);
            visit_stream_bound(count, coverage);
        }
        AstNode::Skip { input, count } => {
            coverage.ast_nodes.insert("Skip");
            visit_ast_node(input, coverage);
            visit_stream_bound(count, coverage);
        }
        AstNode::Range { input, start, end } => {
            coverage.ast_nodes.insert("Range");
            visit_ast_node(input, coverage);
            visit_stream_bound(start, coverage);
            visit_stream_bound(end, coverage);
        }
        AstNode::As { input, .. } => visit_input("As", input, coverage),
        AstNode::Store { input, .. } => visit_input("Store", input, coverage),
        AstNode::Select { input, .. } => visit_input("Select", input, coverage),
        AstNode::Bind { input, .. } => visit_input("Bind", input, coverage),
        AstNode::Inject { input, .. } => {
            coverage.ast_nodes.insert("Inject");
            if let Some(input) = input {
                visit_ast_node(input, coverage);
            }
        }
        AstNode::Count { input } => visit_input("Count", input, coverage),
        AstNode::Exists { input } => visit_input("Exists", input, coverage),
        AstNode::Id { input } => visit_input("Id", input, coverage),
        AstNode::Label { input } => visit_input("Label", input, coverage),
        AstNode::Values { input, .. } => visit_input("Values", input, coverage),
        AstNode::ValueMap { input, .. } => visit_input("ValueMap", input, coverage),
        AstNode::Project {
            input, projections, ..
        } => {
            coverage.ast_nodes.insert("Project");
            visit_ast_node(input, coverage);
            for projection in projections {
                visit_projection(projection, coverage);
            }
        }
        AstNode::ProjectBindings {
            input, projections, ..
        } => {
            coverage.ast_nodes.insert("ProjectBindings");
            visit_ast_node(input, coverage);
            for projection in projections {
                visit_binding_projection(projection, coverage);
            }
        }
        AstNode::EdgeProperties { input } => visit_input("EdgeProperties", input, coverage),
        AstNode::CreateIndex { spec, .. } => {
            coverage.ast_nodes.insert("CreateIndex");
            visit_index_spec(spec, coverage);
        }
        AstNode::DropIndex { spec } => {
            coverage.ast_nodes.insert("DropIndex");
            visit_index_spec(spec, coverage);
        }
        AstNode::GetIndexOperation { .. } => {
            coverage.ast_nodes.insert("GetIndexOperation");
        }
        AstNode::RetryIndexOperation { .. } => {
            coverage.ast_nodes.insert("RetryIndexOperation");
        }
        AstNode::AbortIndexOperation { .. } => {
            coverage.ast_nodes.insert("AbortIndexOperation");
        }
        AstNode::AddN {
            input, properties, ..
        } => {
            coverage.ast_nodes.insert("AddN");
            if let Some(input) = input {
                visit_ast_node(input, coverage);
            }
            for (_, value) in properties {
                visit_property_input(value, coverage);
            }
        }
        AstNode::AddE {
            input,
            to,
            properties,
            ..
        } => {
            coverage.ast_nodes.insert("AddE");
            visit_ast_node(input, coverage);
            visit_node_ref(to, coverage);
            for (_, value) in properties {
                visit_property_input(value, coverage);
            }
        }
        AstNode::SetProperty { input, value, .. } => {
            coverage.ast_nodes.insert("SetProperty");
            visit_ast_node(input, coverage);
            visit_property_input(value, coverage);
        }
        AstNode::RemoveProperty { input, .. } => visit_input("RemoveProperty", input, coverage),
        AstNode::Drop { input } => visit_input("Drop", input, coverage),
        AstNode::DropEdge { input, to } => {
            coverage.ast_nodes.insert("DropEdge");
            visit_ast_node(input, coverage);
            visit_node_ref(to, coverage);
        }
        AstNode::DropEdgeLabeled { input, to, .. } => {
            coverage.ast_nodes.insert("DropEdgeLabeled");
            visit_ast_node(input, coverage);
            visit_node_ref(to, coverage);
        }
        AstNode::DropEdgeById { input, edges } => {
            coverage.ast_nodes.insert("DropEdgeById");
            if let Some(input) = input {
                visit_ast_node(input, coverage);
            }
            visit_edge_ref(edges, coverage);
        }
        AstNode::OrderBy { input, order, .. } => {
            coverage.ast_nodes.insert("OrderBy");
            visit_ast_node(input, coverage);
            visit_order(order, coverage);
        }
        AstNode::OrderByMultiple {
            input, orderings, ..
        } => {
            coverage.ast_nodes.insert("OrderByMultiple");
            visit_ast_node(input, coverage);
            for (_, order) in orderings {
                visit_order(order, coverage);
            }
        }
        AstNode::Repeat { input, config } => {
            coverage.ast_nodes.insert("Repeat");
            visit_ast_node(input, coverage);
            visit_repeat_config(config, coverage);
        }
        AstNode::Union { input, traversals } => {
            coverage.ast_nodes.insert("Union");
            visit_ast_node(input, coverage);
            for traversal in traversals {
                visit_ast_node(&traversal.root, coverage);
            }
        }
        AstNode::Choose {
            input,
            condition,
            then_traversal,
            else_traversal,
        } => {
            coverage.ast_nodes.insert("Choose");
            visit_ast_node(input, coverage);
            visit_predicate(condition, coverage);
            visit_ast_node(&then_traversal.root, coverage);
            if let Some(traversal) = else_traversal {
                visit_ast_node(&traversal.root, coverage);
            }
        }
        AstNode::Coalesce { input, traversals } => {
            coverage.ast_nodes.insert("Coalesce");
            visit_ast_node(input, coverage);
            for traversal in traversals {
                visit_ast_node(&traversal.root, coverage);
            }
        }
        AstNode::Optional { input, traversal } => {
            coverage.ast_nodes.insert("Optional");
            visit_ast_node(input, coverage);
            visit_ast_node(&traversal.root, coverage);
        }
        AstNode::Group { input, .. } => visit_input("Group", input, coverage),
        AstNode::GroupCount { input, .. } => visit_input("GroupCount", input, coverage),
        AstNode::AggregateBy {
            input, function, ..
        } => {
            coverage.ast_nodes.insert("AggregateBy");
            visit_ast_node(input, coverage);
            visit_aggregate_function(function, coverage);
        }
        AstNode::Fold { input } => visit_input("Fold", input, coverage),
        AstNode::Unfold { input } => visit_input("Unfold", input, coverage),
        AstNode::Path { input } => visit_input("Path", input, coverage),
        AstNode::SimplePath { input } => visit_input("SimplePath", input, coverage),
        AstNode::WithSack { input, initial } => {
            coverage.ast_nodes.insert("WithSack");
            visit_ast_node(input, coverage);
            visit_property_value(initial, coverage);
        }
        AstNode::SackSet { input, .. } => visit_input("SackSet", input, coverage),
        AstNode::SackAdd { input, .. } => visit_input("SackAdd", input, coverage),
        AstNode::SackGet { input } => visit_input("SackGet", input, coverage),
        AstNode::ShortestPath {
            source,
            target,
            direction,
            ..
        } => {
            coverage.ast_nodes.insert("ShortestPath");
            visit_node_ref(source, coverage);
            visit_node_ref(target, coverage);
            visit_shortest_path_direction(direction, coverage);
        }
    }
}

fn visit_input(name: &'static str, input: &AstNode, coverage: &mut FixtureCoverage) {
    coverage.ast_nodes.insert(name);
    visit_ast_node(input, coverage);
}

fn visit_expr(expr: &Expr, coverage: &mut FixtureCoverage) {
    match expr {
        Expr::Property(_) => {
            coverage.expressions.insert("Property");
        }
        Expr::Id => {
            coverage.expressions.insert("Id");
        }
        Expr::Timestamp => {
            coverage.expressions.insert("Timestamp");
        }
        Expr::DateTimeNow => {
            coverage.expressions.insert("DateTimeNow");
        }
        Expr::Constant(value) => {
            coverage.expressions.insert("Constant");
            visit_property_value(value, coverage);
        }
        Expr::Param(_) => {
            coverage.expressions.insert("Param");
        }
        Expr::Add { left, right } => visit_binary_expr("Add", left, right, coverage),
        Expr::Sub { left, right } => visit_binary_expr("Sub", left, right, coverage),
        Expr::Mul { left, right } => visit_binary_expr("Mul", left, right, coverage),
        Expr::Div { left, right } => visit_binary_expr("Div", left, right, coverage),
        Expr::Mod { left, right } => visit_binary_expr("Mod", left, right, coverage),
        Expr::Neg { expr } => {
            coverage.expressions.insert("Neg");
            visit_expr(expr, coverage);
        }
        Expr::Case {
            when_then,
            else_expr,
        } => {
            coverage.expressions.insert("Case");
            for branch in when_then {
                visit_predicate(&branch.when, coverage);
                visit_expr(&branch.then, coverage);
            }
            if let Some(expr) = else_expr {
                visit_expr(expr, coverage);
            }
        }
    }
}

fn visit_binary_expr(
    name: &'static str,
    left: &Expr,
    right: &Expr,
    coverage: &mut FixtureCoverage,
) {
    coverage.expressions.insert(name);
    visit_expr(left, coverage);
    visit_expr(right, coverage);
}

fn visit_predicate(predicate: &Predicate, coverage: &mut FixtureCoverage) {
    match predicate {
        Predicate::Eq { left, right } => visit_binary_predicate("Eq", left, right, coverage),
        Predicate::Neq { left, right } => visit_binary_predicate("Neq", left, right, coverage),
        Predicate::Gt { left, right } => visit_binary_predicate("Gt", left, right, coverage),
        Predicate::Gte { left, right } => visit_binary_predicate("Gte", left, right, coverage),
        Predicate::Lt { left, right } => visit_binary_predicate("Lt", left, right, coverage),
        Predicate::Lte { left, right } => visit_binary_predicate("Lte", left, right, coverage),
        Predicate::Between { value, min, max } => {
            coverage.predicates.insert("Between");
            visit_expr(value, coverage);
            visit_expr(min, coverage);
            visit_expr(max, coverage);
        }
        Predicate::HasKey { .. } => {
            coverage.predicates.insert("HasKey");
        }
        Predicate::IsNull { .. } => {
            coverage.predicates.insert("IsNull");
        }
        Predicate::IsNotNull { .. } => {
            coverage.predicates.insert("IsNotNull");
        }
        Predicate::StartsWith { value, prefix } => {
            visit_binary_predicate("StartsWith", value, prefix, coverage);
        }
        Predicate::EndsWith { value, suffix } => {
            visit_binary_predicate("EndsWith", value, suffix, coverage);
        }
        Predicate::Contains { value, substring } => {
            visit_binary_predicate("Contains", value, substring, coverage);
        }
        Predicate::IsIn { value, values } => {
            visit_binary_predicate("IsIn", value, values, coverage);
        }
        Predicate::And { predicates } => visit_predicates("And", predicates, coverage),
        Predicate::Or { predicates } => visit_predicates("Or", predicates, coverage),
        Predicate::Not { predicate } => {
            coverage.predicates.insert("Not");
            visit_predicate(predicate, coverage);
        }
        Predicate::Compare { left, op, right } => {
            coverage.predicates.insert("Compare");
            visit_expr(left, coverage);
            visit_compare_op(op, coverage);
            visit_expr(right, coverage);
        }
    }
}

fn visit_binary_predicate(
    name: &'static str,
    left: &Expr,
    right: &Expr,
    coverage: &mut FixtureCoverage,
) {
    coverage.predicates.insert(name);
    visit_expr(left, coverage);
    visit_expr(right, coverage);
}

fn visit_predicates(name: &'static str, predicates: &[Predicate], coverage: &mut FixtureCoverage) {
    coverage.predicates.insert(name);
    for predicate in predicates {
        visit_predicate(predicate, coverage);
    }
}

fn visit_compare_op(op: &CompareOp, coverage: &mut FixtureCoverage) {
    coverage.compare_ops.insert(match op {
        CompareOp::Eq => "Eq",
        CompareOp::Neq => "Neq",
        CompareOp::Gt => "Gt",
        CompareOp::Gte => "Gte",
        CompareOp::Lt => "Lt",
        CompareOp::Lte => "Lte",
    });
}

fn visit_property_input(input: &PropertyInput, coverage: &mut FixtureCoverage) {
    match input {
        PropertyInput::Value(value) => {
            coverage.property_inputs.insert("Value");
            visit_property_value(value, coverage);
        }
        PropertyInput::Expr(expr) => {
            coverage.property_inputs.insert("Expr");
            visit_expr(expr, coverage);
        }
    }
}

fn visit_property_value(value: &PropertyValue, coverage: &mut FixtureCoverage) {
    match value {
        PropertyValue::Null => {
            coverage.property_values.insert("Null");
        }
        PropertyValue::Bool(_) => {
            coverage.property_values.insert("Bool");
        }
        PropertyValue::I64(_) => {
            coverage.property_values.insert("I64");
        }
        PropertyValue::DateTime(_) => {
            coverage.property_values.insert("DateTime");
        }
        PropertyValue::F64(_) => {
            coverage.property_values.insert("F64");
        }
        PropertyValue::F32(_) => {
            coverage.property_values.insert("F32");
        }
        PropertyValue::String(_) => {
            coverage.property_values.insert("String");
        }
        PropertyValue::Bytes(_) => {
            coverage.property_values.insert("Bytes");
        }
        PropertyValue::I64Array(_) => {
            coverage.property_values.insert("I64Array");
        }
        PropertyValue::F64Array(_) => {
            coverage.property_values.insert("F64Array");
        }
        PropertyValue::F32Array(_) => {
            coverage.property_values.insert("F32Array");
        }
        PropertyValue::StringArray(_) => {
            coverage.property_values.insert("StringArray");
        }
        PropertyValue::Array(values) => {
            coverage.property_values.insert("Array");
            for value in values {
                visit_property_value(value, coverage);
            }
        }
        PropertyValue::Object(values) => {
            coverage.property_values.insert("Object");
            for value in values.values() {
                visit_property_value(value, coverage);
            }
        }
    }
}

fn visit_query_value(value: &QueryValue, coverage: &mut FixtureCoverage) {
    match value {
        QueryValue::Null => {
            coverage.query_values.insert("Null");
        }
        QueryValue::Bool(_) => {
            coverage.query_values.insert("Bool");
        }
        QueryValue::I64(_) => {
            coverage.query_values.insert("I64");
        }
        QueryValue::F64(_) => {
            coverage.query_values.insert("F64");
        }
        QueryValue::F32(_) => {
            coverage.query_values.insert("F32");
        }
        QueryValue::String(_) => {
            coverage.query_values.insert("String");
        }
        QueryValue::Array(values) => {
            coverage.query_values.insert("Array");
            for value in values {
                visit_query_value(value, coverage);
            }
        }
        QueryValue::Object(values) => {
            coverage.query_values.insert("Object");
            for value in values.values() {
                visit_query_value(value, coverage);
            }
        }
    }
}

fn visit_node_ref(reference: &NodeRef, coverage: &mut FixtureCoverage) {
    coverage.node_refs.insert(match reference {
        NodeRef::All => "All",
        NodeRef::Ids(_) => "Ids",
        NodeRef::Var(_) => "Var",
        NodeRef::Param(_) => "Param",
    });
}

fn visit_edge_ref(reference: &EdgeRef, coverage: &mut FixtureCoverage) {
    coverage.edge_refs.insert(match reference {
        EdgeRef::All => "All",
        EdgeRef::Ids(_) => "Ids",
        EdgeRef::Var(_) => "Var",
        EdgeRef::Param(_) => "Param",
    });
}

fn visit_projection(projection: &Projection, coverage: &mut FixtureCoverage) {
    match projection {
        Projection::Property(_) => {
            coverage.projections.insert("Property");
        }
        Projection::Expr(projection) => {
            coverage.projections.insert("Expr");
            visit_expr(&projection.expr, coverage);
        }
    }
}

fn visit_binding_target(target: &BindingTarget, coverage: &mut FixtureCoverage) {
    coverage.binding_targets.insert(match target {
        BindingTarget::Current => "Current",
        BindingTarget::Binding(_) => "Binding",
    });
}

fn visit_binding_value_ref(value_ref: &BindingValueRef, coverage: &mut FixtureCoverage) {
    visit_binding_target(&value_ref.target, coverage);
}

fn visit_binding_projection(projection: &BindingProjection, coverage: &mut FixtureCoverage) {
    match projection {
        BindingProjection::Property { target, .. } => {
            coverage.binding_projections.insert("Property");
            visit_binding_target(target, coverage);
        }
        BindingProjection::Coalesce { refs, .. } => {
            coverage.binding_projections.insert("Coalesce");
            for value_ref in refs {
                visit_binding_value_ref(value_ref, coverage);
            }
        }
    }
}

fn visit_stream_bound(bound: &StreamBound, coverage: &mut FixtureCoverage) {
    match bound {
        StreamBound::Literal(_) => {
            coverage.stream_bounds.insert("Literal");
        }
        StreamBound::Expr(expr) => {
            coverage.stream_bounds.insert("Expr");
            visit_expr(expr, coverage);
        }
    }
}

fn visit_index_spec(spec: &IndexSpec, coverage: &mut FixtureCoverage) {
    match spec {
        IndexSpec::NodeEquality { .. } => {
            coverage.index_specs.insert("NodeEquality");
        }
        IndexSpec::NodeRange { direction, .. } => {
            coverage.index_specs.insert("NodeRange");
            visit_range_direction(direction, coverage);
        }
        IndexSpec::EdgeEquality { .. } => {
            coverage.index_specs.insert("EdgeEquality");
        }
        IndexSpec::EdgeRange { direction, .. } => {
            coverage.index_specs.insert("EdgeRange");
            visit_range_direction(direction, coverage);
        }
        IndexSpec::NodeVector { metric, .. } => {
            coverage.index_specs.insert("NodeVector");
            visit_vector_metric(metric, coverage);
        }
        IndexSpec::NodeText { .. } => {
            coverage.index_specs.insert("NodeText");
        }
        IndexSpec::EdgeVector { metric, .. } => {
            coverage.index_specs.insert("EdgeVector");
            visit_vector_metric(metric, coverage);
        }
        IndexSpec::EdgeText { .. } => {
            coverage.index_specs.insert("EdgeText");
        }
    }
}

fn visit_range_direction(direction: &RangeIndexDirection, coverage: &mut FixtureCoverage) {
    coverage.range_directions.insert(match direction {
        RangeIndexDirection::Asc => "Asc",
        RangeIndexDirection::Desc => "Desc",
    });
}

fn visit_vector_metric(metric: &VectorDistanceMetric, coverage: &mut FixtureCoverage) {
    coverage.vector_metrics.insert(match metric {
        VectorDistanceMetric::Cosine => "Cosine",
        VectorDistanceMetric::Euclidean => "Euclidean",
        VectorDistanceMetric::Manhattan => "Manhattan",
    });
}

fn visit_order(order: &Order, coverage: &mut FixtureCoverage) {
    coverage.orders.insert(match order {
        Order::Asc => "Asc",
        Order::Desc => "Desc",
    });
}

fn visit_shortest_path_direction(
    direction: &ShortestPathDirection,
    coverage: &mut FixtureCoverage,
) {
    coverage.shortest_path_directions.insert(match direction {
        ShortestPathDirection::Out => "Out",
        ShortestPathDirection::In => "In",
        ShortestPathDirection::Both => "Both",
    });
}

fn visit_repeat_config(config: &RepeatConfig, coverage: &mut FixtureCoverage) {
    visit_ast_node(&config.traversal.root, coverage);
    if let Some(predicate) = &config.until {
        visit_predicate(predicate, coverage);
    }
    coverage.emit_behaviors.insert(match config.emit {
        EmitBehavior::None => "None",
        EmitBehavior::Before => "Before",
        EmitBehavior::After => "After",
        EmitBehavior::All => "All",
    });
    if let Some(predicate) = &config.emit_predicate {
        visit_predicate(predicate, coverage);
    }
}

fn visit_aggregate_function(function: &AggregateFunction, coverage: &mut FixtureCoverage) {
    coverage.aggregate_functions.insert(match function {
        AggregateFunction::Count => "Count",
        AggregateFunction::Sum => "Sum",
        AggregateFunction::Min => "Min",
        AggregateFunction::Max => "Max",
        AggregateFunction::Mean => "Mean",
    });
}

fn visit_query_param_type(param_type: &QueryParamType, coverage: &mut FixtureCoverage) {
    match param_type {
        QueryParamType::Bool => {
            coverage.query_param_types.insert("Bool");
        }
        QueryParamType::I64 => {
            coverage.query_param_types.insert("I64");
        }
        QueryParamType::F64 => {
            coverage.query_param_types.insert("F64");
        }
        QueryParamType::F32 => {
            coverage.query_param_types.insert("F32");
        }
        QueryParamType::String => {
            coverage.query_param_types.insert("String");
        }
        QueryParamType::DateTime => {
            coverage.query_param_types.insert("DateTime");
        }
        QueryParamType::Bytes => {
            coverage.query_param_types.insert("Bytes");
        }
        QueryParamType::Value => {
            coverage.query_param_types.insert("Value");
        }
        QueryParamType::Object => {
            coverage.query_param_types.insert("Object");
        }
        QueryParamType::Array(inner) => {
            coverage.query_param_types.insert("Array");
            visit_query_param_type(inner, coverage);
        }
    }
}

fn reset_dir(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)
}

fn runtime(name: impl Into<String>, request: QueryRequest) -> Fixture {
    Fixture {
        bucket: "runtime",
        name: name.into(),
        request,
    }
}

fn json_only(name: impl Into<String>, request: QueryRequest) -> Fixture {
    Fixture {
        bucket: "json-only",
        name: name.into(),
        request,
    }
}

fn read_request(batch: ReadBatch) -> QueryRequest {
    QueryRequest::read(batch)
}

fn write_request(batch: WriteBatch) -> QueryRequest {
    QueryRequest::write(batch)
}

fn object(entries: Vec<(&str, QueryValue)>) -> QueryValue {
    QueryValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn array(values: Vec<QueryValue>) -> QueryValue {
    QueryValue::Array(values)
}

fn string(value: &str) -> QueryValue {
    QueryValue::String(value.to_string())
}

fn i64_value(value: i64) -> QueryValue {
    QueryValue::I64(value)
}

fn f64_value(value: f64) -> QueryValue {
    QueryValue::F64(value)
}

fn with_params(
    mut request: QueryRequest,
    values: Vec<(&str, QueryValue)>,
    types: Vec<(&str, QueryParamType)>,
) -> QueryRequest {
    assert_eq!(values.len(), types.len(), "fixture schema/value mismatch");
    for ((value_name, value), (type_name, ty)) in values.into_iter().zip(types) {
        assert_eq!(value_name, type_name, "fixture parameter name mismatch");
        request
            .try_insert_typed_parameter(value_name, ty, value)
            .expect("fixture parameter should be valid");
    }
    request
}

fn user_props(user: UserProps) -> Vec<(&'static str, PropertyInput)> {
    vec![
        ("externalId", PropertyInput::from(user.external_id)),
        ("name", PropertyInput::from(user.name)),
        ("age", PropertyInput::from(user.age)),
        ("score", PropertyInput::from(user.score)),
        ("status", PropertyInput::from(user.status)),
        ("tenantId", PropertyInput::from("tenant-a")),
        ("city", PropertyInput::from(user.city)),
        ("bio", PropertyInput::from(user.bio)),
        (
            "createdAt",
            PropertyInput::from(DateTime::from_millis(1_776_000_000_000)),
        ),
        (
            "embedding",
            PropertyInput::from(PropertyValue::from(user.embedding)),
        ),
    ]
}

fn nested_metadata_property(external_id: &str, score: i64) -> PropertyValue {
    PropertyValue::object(vec![
        ("externalID", PropertyValue::from(external_id)),
        ("score", PropertyValue::from(score)),
        (
            "tags",
            PropertyValue::array(vec![
                PropertyValue::from("alpha"),
                PropertyValue::from(7i64),
            ]),
        ),
    ])
}

fn nested_metadata_param(external_id: &str, score: i64) -> QueryValue {
    object(vec![
        ("externalID", string(external_id)),
        ("score", i64_value(score)),
        ("tags", array(vec![string("alpha"), i64_value(7)])),
    ])
}

fn runtime_fixtures() -> Vec<Fixture> {
    vec![
        runtime(
            "001-write-seed-core",
            write_request(
                write_batch()
                    .var_as(
                        "alice",
                        g().add_n(
                            "ParityUser",
                            user_props(UserProps {
                                external_id: "user-alice",
                                name: "Alice",
                                age: 31,
                                score: 90.5,
                                status: "active",
                                city: "London",
                                bio: "Alice writes graph database tests",
                                embedding: vec![1.0, 0.0, 0.0],
                            }),
                        ),
                    )
                    .var_as(
                        "bob",
                        g().add_n(
                            "ParityUser",
                            user_props(UserProps {
                                external_id: "user-bob",
                                name: "Bob",
                                age: 27,
                                score: 72.25,
                                status: "active",
                                city: "Paris",
                                bio: "Bob likes traversal testing",
                                embedding: vec![0.9, 0.1, 0.0],
                            }),
                        ),
                    )
                    .var_as(
                        "carol",
                        g().add_n(
                            "ParityUser",
                            user_props(UserProps {
                                external_id: "user-carol",
                                name: "Carol",
                                age: 42,
                                score: 64.0,
                                status: "inactive",
                                city: "Berlin",
                                bio: "Carol archives old records",
                                embedding: vec![0.0, 1.0, 0.0],
                            }),
                        ),
                    )
                    .var_as(
                        "alice_follows_bob",
                        g().n(NodeRef::var("alice")).add_e(
                            "FOLLOWS",
                            NodeRef::var("bob"),
                            vec![
                                ("weight", PropertyInput::from(1.0f64)),
                                ("since", PropertyInput::from("2024-01-01")),
                                ("note", PropertyInput::from("Alice follows Bob")),
                                (
                                    "embedding",
                                    PropertyInput::from(PropertyValue::from(vec![1.0f32, 0.0])),
                                ),
                            ],
                        ),
                    )
                    .var_as(
                        "bob_follows_carol",
                        g().n(NodeRef::var("bob")).add_e(
                            "FOLLOWS",
                            NodeRef::var("carol"),
                            vec![
                                ("weight", PropertyInput::from(0.5f64)),
                                ("since", PropertyInput::from("2024-02-01")),
                                ("note", PropertyInput::from("Bob follows Carol")),
                                (
                                    "embedding",
                                    PropertyInput::from(PropertyValue::from(vec![0.0f32, 1.0])),
                                ),
                            ],
                        ),
                    )
                    .returning([
                        "alice",
                        "bob",
                        "carol",
                        "alice_follows_bob",
                        "bob_follows_carol",
                    ]),
            ),
        ),
        runtime(
            "002-read-count-all-users",
            read_request(
                read_batch()
                    .var_as("user_count", g().n_with_label("ParityUser").count())
                    .returning(["user_count"]),
            ),
        ),
        runtime(
            "003-read-source-predicate-and-count",
            read_request(
                read_batch()
                    .var_as(
                        "active_adults",
                        g().n_with_label_where(
                            "ParityUser",
                            SourcePredicate::and(vec![
                                SourcePredicate::eq("status", "active"),
                                SourcePredicate::gte("age", 30i64),
                            ]),
                        )
                        .count(),
                    )
                    .returning(["active_adults"]),
            ),
        ),
        runtime(
            "004-read-value-map-projection",
            read_request(
                read_batch()
                    .var_as(
                        "alice",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-alice"))
                            .project(vec![
                                Projection::property("externalId", "id"),
                                Projection::property("name", "name"),
                                Projection::expr(
                                    "score_plus_one",
                                    Expr::prop("score").add(Expr::val(1.0f64)),
                                ),
                                Projection::expr(
                                    "status_label",
                                    Expr::case(
                                        vec![(
                                            Predicate::eq("status", "active"),
                                            Expr::val("enabled"),
                                        )],
                                        Some(Expr::val("disabled")),
                                    ),
                                ),
                            ]),
                    )
                    .returning(["alice"]),
            ),
        ),
        runtime(
            "005-read-order-range-values",
            read_request(
                read_batch()
                    .var_as(
                        "ordered",
                        g().n_with_label("ParityUser")
                            .order_by_multiple(vec![("status", Order::Asc), ("age", Order::Desc)])
                            .range(0usize, 2usize)
                            .value_map(Some(vec!["externalId", "age", "status"])),
                    )
                    .returning(["ordered"]),
            ),
        ),
        runtime(
            "006-read-edge-count",
            read_request(
                read_batch()
                    .var_as(
                        "edge_count",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-alice"))
                            .out_e(Some("FOLLOWS"))
                            .count(),
                    )
                    .returning(["edge_count"]),
            ),
        ),
        runtime(
            "007-read-edge-properties",
            read_request(
                read_batch()
                    .var_as(
                        "edges",
                        g().e_with_label("FOLLOWS")
                            .edge_has("weight", PropertyInput::from(1.0f64))
                            .edge_properties(),
                    )
                    .returning(["edges"]),
            ),
        ),
        runtime(
            "008-read-edge-endpoints",
            read_request(
                read_batch()
                    .var_as(
                        "from_nodes",
                        g().e_with_label("FOLLOWS")
                            .edge_has_label("FOLLOWS")
                            .in_n()
                            .value_map(Some(vec!["externalId", "name"])),
                    )
                    .var_as(
                        "to_nodes",
                        g().e_with_label("FOLLOWS")
                            .out_n()
                            .value_map(Some(vec!["externalId", "name"])),
                    )
                    .returning(["from_nodes", "to_nodes"]),
            ),
        ),
        runtime(
            "009-read-conditional-var-not-empty",
            read_request(
                read_batch()
                    .var_as(
                        "alice",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-alice")),
                    )
                    .var_as_if(
                        "friends",
                        BatchCondition::VarNotEmpty("alice".to_string()),
                        g().n(NodeRef::var("alice"))
                            .out(Some("FOLLOWS"))
                            .value_map(Some(vec!["externalId", "name"])),
                    )
                    .returning(["alice", "friends"]),
            ),
        ),
        runtime(
            "010-read-conditional-var-empty",
            read_request(
                read_batch()
                    .var_as(
                        "missing",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "missing-user")),
                    )
                    .var_as_if(
                        "fallback",
                        BatchCondition::VarEmpty("missing".to_string()),
                        g().n_with_label("ParityUser")
                            .limit(1usize)
                            .value_map(Some(vec!["externalId"])),
                    )
                    .returning(["missing", "fallback"]),
            ),
        ),
        runtime(
            "011-read-conditional-var-min-size-prev",
            read_request(
                read_batch()
                    .var_as("users", g().n_with_label("ParityUser").limit(3usize))
                    .var_as_if(
                        "min_two",
                        BatchCondition::VarMinSize("users".to_string(), 2),
                        g().n(NodeRef::var("users")).count(),
                    )
                    .var_as_if(
                        "prev_ok",
                        BatchCondition::PrevNotEmpty,
                        g().n(NodeRef::var("users")).exists(),
                    )
                    .returning(["min_two", "prev_ok"]),
            ),
        ),
        runtime(
            "012-read-foreach-param",
            with_params(
                read_request(
                    read_batch()
                        .for_each_param(
                            "lookups",
                            read_batch().var_as(
                                "matched",
                                g().n_with_label("ParityUser")
                                    .where_(Predicate::eq_param("externalId", "externalId"))
                                    .value_map(Some(vec!["externalId", "name"])),
                            ),
                        )
                        .returning(["matched"]),
                ),
                vec![(
                    "lookups",
                    array(vec![
                        object(vec![("externalId", string("user-alice"))]),
                        object(vec![("externalId", string("user-carol"))]),
                    ]),
                )],
                vec![(
                    "lookups",
                    QueryParamType::Array(Box::new(QueryParamType::Object)),
                )],
            ),
        ),
        runtime(
            "013-write-foreach-param-create",
            with_params(
                write_request(
                    write_batch()
                        .for_each_param(
                            "rows",
                            write_batch().var_as(
                                "created",
                                g().add_n(
                                    "ParityEvent",
                                    vec![
                                        ("eventId", PropertyInput::param("eventId")),
                                        ("kind", PropertyInput::param("kind")),
                                        ("score", PropertyInput::param("score")),
                                    ],
                                ),
                            ),
                        )
                        .returning(["created"]),
                ),
                vec![(
                    "rows",
                    array(vec![
                        object(vec![
                            ("eventId", string("event-1")),
                            ("kind", string("click")),
                            ("score", i64_value(10)),
                        ]),
                        object(vec![
                            ("eventId", string("event-2")),
                            ("kind", string("view")),
                            ("score", i64_value(5)),
                        ]),
                    ]),
                )],
                vec![(
                    "rows",
                    QueryParamType::Array(Box::new(QueryParamType::Object)),
                )],
            ),
        ),
        runtime(
            "014-read-after-foreach-param",
            read_request(
                read_batch()
                    .var_as("event_count", g().n_with_label("ParityEvent").count())
                    .returning(["event_count"]),
            ),
        ),
        runtime(
            "015-write-set-remove-properties",
            write_request(
                write_batch()
                    .var_as(
                        "updated",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-bob"))
                            .set_property("status", PropertyInput::from("inactive"))
                            .set_property(
                                "updatedAt",
                                PropertyInput::from(DateTime::from_millis(1_777_000_000_000)),
                            )
                            .remove_property("city")
                            .count(),
                    )
                    .returning(["updated"]),
            ),
        ),
        runtime(
            "016-read-updated-properties",
            read_request(
                read_batch()
                    .var_as(
                        "bob",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-bob"))
                            .value_map(Some(vec!["externalId", "status", "updatedAt", "city"])),
                    )
                    .returning(["bob"]),
            ),
        ),
        runtime(
            "017-read-repeat-union",
            read_request(
                read_batch()
                    .var_as(
                        "walked",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-alice"))
                            .repeat(
                                RepeatConfig::new(sub().out(Some("FOLLOWS")))
                                    .times(2)
                                    .emit_all()
                                    .max_depth(4),
                            )
                            .union(vec![sub().out(Some("FOLLOWS")), sub().in_(Some("FOLLOWS"))])
                            .dedup()
                            .value_map(Some(vec!["externalId", "name"])),
                    )
                    .returning(["walked"]),
            ),
        ),
        runtime(
            "018-read-choose-coalesce-optional",
            read_request(
                read_batch()
                    .var_as(
                        "branched",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "user-alice"))
                            .choose(
                                Predicate::eq("status", "active"),
                                sub().out(Some("FOLLOWS")),
                                Some(sub().in_(Some("FOLLOWS"))),
                            )
                            .coalesce(vec![sub().out(Some("FOLLOWS")), sub().in_(Some("FOLLOWS"))])
                            .optional(sub().out(Some("FOLLOWS")))
                            .dedup()
                            .value_map(Some(vec!["externalId", "name"])),
                    )
                    .returning(["branched"]),
            ),
        ),
        runtime(
            "019-read-aggregations",
            read_request(
                read_batch()
                    .var_as(
                        "by_status",
                        g().n_with_label("ParityUser").group_count("status"),
                    )
                    .var_as(
                        "mean_score",
                        g().n_with_label("ParityUser")
                            .aggregate_by(AggregateFunction::Mean, "score"),
                    )
                    .var_as(
                        "max_age",
                        g().n_with_label("ParityUser")
                            .aggregate_by(AggregateFunction::Max, "age"),
                    )
                    .returning(["by_status", "mean_score", "max_age"]),
            ),
        ),
        runtime(
            "020-write-index-create",
            write_request(
                write_batch()
                    .var_as(
                        "node_eq",
                        g().create_index_if_not_exists(IndexSpec::node_equality(
                            "ParityUser",
                            "externalId",
                        )),
                    )
                    .var_as(
                        "node_range",
                        g().create_index_if_not_exists(IndexSpec::node_range("ParityUser", "age")),
                    )
                    .var_as(
                        "edge_eq",
                        g().create_index_if_not_exists(IndexSpec::edge_equality(
                            "FOLLOWS", "since",
                        )),
                    )
                    .var_as(
                        "edge_range",
                        g().create_index_if_not_exists(IndexSpec::edge_range("FOLLOWS", "weight")),
                    )
                    .returning(["node_eq", "node_range", "edge_eq", "edge_range"]),
            ),
        ),
        runtime(
            "021-read-parameter-types",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "matches",
                            g().n_with_label("ParityUser")
                                .where_(Predicate::is_in_param("status", "statuses"))
                                .where_(Predicate::gte_param("createdAt", "created_after"))
                                .limit(Expr::param("limit"))
                                .value_map(Some(vec!["externalId", "status"])),
                        )
                        .returning(["matches"]),
                ),
                vec![
                    (
                        "statuses",
                        array(vec![string("active"), string("inactive")]),
                    ),
                    ("created_after", string("2026-01-01T00:00:00.000Z")),
                    ("limit", i64_value(5)),
                ],
                vec![
                    (
                        "statuses",
                        QueryParamType::Array(Box::new(QueryParamType::String)),
                    ),
                    ("created_after", QueryParamType::DateTime),
                    ("limit", QueryParamType::I64),
                ],
            ),
        ),
        runtime(
            "022-write-property-value-variants",
            write_request(
                write_batch()
                    .var_as(
                        "variant_node",
                        g().add_n(
                            "ParityVariant",
                            vec![
                                ("nullValue", PropertyInput::from(PropertyValue::Null)),
                                ("boolValue", PropertyInput::from(true)),
                                (
                                    "i64Value",
                                    PropertyInput::from(9_223_372_036_854_775_000i64),
                                ),
                                (
                                    "dateTimeValue",
                                    PropertyInput::from(DateTime::from_millis(-1)),
                                ),
                                ("f64Value", PropertyInput::from(3.25f64)),
                                ("f32Value", PropertyInput::from(1.5f32)),
                                ("stringValue", PropertyInput::from("variant")),
                                (
                                    "bytesValue",
                                    PropertyInput::from(PropertyValue::from(vec![1u8, 2u8, 3u8])),
                                ),
                                (
                                    "i64Array",
                                    PropertyInput::from(PropertyValue::from(vec![
                                        1i64, 2i64, 3i64,
                                    ])),
                                ),
                                (
                                    "f64Array",
                                    PropertyInput::from(PropertyValue::from(vec![1.0f64, 2.0f64])),
                                ),
                                (
                                    "f32Array",
                                    PropertyInput::from(PropertyValue::from(vec![1.0f32, 2.0f32])),
                                ),
                                (
                                    "stringArray",
                                    PropertyInput::from(PropertyValue::from(vec![
                                        "a".to_string(),
                                        "b".to_string(),
                                    ])),
                                ),
                            ],
                        ),
                    )
                    .returning(["variant_node"]),
            ),
        ),
        runtime(
            "023-read-property-value-variants",
            read_request(
                read_batch()
                    .var_as(
                        "variant",
                        g().n_with_label("ParityVariant")
                            .value_map(None::<Vec<&str>>),
                    )
                    .returning(["variant"]),
            ),
        ),
        runtime(
            "024-write-text-vector-indexes",
            write_request(
                write_batch()
                    .var_as(
                        "node_text",
                        g().create_text_index_nodes("ParityUser", "bio", None::<&str>),
                    )
                    .var_as(
                        "node_vector",
                        g().create_vector_index_nodes(
                            "ParityUser",
                            "embedding",
                            NonZeroUsize::new(3).expect("node vector dimension is non-zero"),
                            VectorDistanceMetric::Cosine,
                            None::<&str>,
                        ),
                    )
                    .var_as(
                        "edge_text",
                        g().create_text_index_edges("FOLLOWS", "note", None::<&str>),
                    )
                    .var_as(
                        "edge_vector",
                        g().create_vector_index_edges(
                            "FOLLOWS",
                            "embedding",
                            NonZeroUsize::new(2).expect("edge vector dimension is non-zero"),
                            VectorDistanceMetric::Cosine,
                            None::<&str>,
                        ),
                    )
                    .returning(["node_text", "node_vector", "edge_text", "edge_vector"]),
            ),
        ),
        runtime(
            "025-read-text-search-nodes",
            read_request(
                read_batch()
                    .var_as(
                        "text_hits",
                        g().text_search_nodes("ParityUser", "bio", "graph", 5, None)
                            .value_map(Some(vec!["externalId", "bio", "$distance"])),
                    )
                    .returning(["text_hits"]),
            ),
        ),
        runtime(
            "026-read-vector-search-nodes",
            read_request(
                read_batch()
                    .var_as(
                        "vector_hits",
                        g().vector_search_nodes(
                            "ParityUser",
                            "embedding",
                            vec![1.0, 0.0, 0.0],
                            3,
                            None,
                        )
                        .project(vec![
                            Projection::property("externalId", "externalId"),
                            Projection::property("$distance", "distance"),
                        ]),
                    )
                    .returning(["vector_hits"]),
            ),
        ),
        runtime(
            "027-read-text-search-edges",
            read_request(
                read_batch()
                    .var_as(
                        "edge_text_hits",
                        g().text_search_edges("FOLLOWS", "note", "follows", 5, None)
                            .edge_properties(),
                    )
                    .returning(["edge_text_hits"]),
            ),
        ),
        runtime(
            "028-read-vector-search-edges",
            read_request(
                read_batch()
                    .var_as(
                        "edge_vector_hits",
                        g().vector_search_edges("FOLLOWS", "embedding", vec![1.0, 0.0], 5, None)
                            .edge_properties(),
                    )
                    .returning(["edge_vector_hits"]),
            ),
        ),
        runtime(
            "029-write-drop-temp-node",
            write_request(
                write_batch()
                    .var_as(
                        "temp",
                        g().add_n("ParityTemp", vec![("name", PropertyInput::from("temp"))]),
                    )
                    .var_as("dropped", g().n(NodeRef::var("temp")).drop().count())
                    .returning(["dropped"]),
            ),
        ),
        runtime(
            "030-read-final-counts",
            read_request(
                read_batch()
                    .var_as("users", g().n_with_label("ParityUser").count())
                    .var_as("events", g().n_with_label("ParityEvent").count())
                    .var_as("variants", g().n_with_label("ParityVariant").count())
                    .returning(["users", "events", "variants"]),
            ),
        ),
        runtime(
            "031-read-source-predicate-eq-param",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "user",
                            g().n_where(SourcePredicate::and(vec![
                                SourcePredicate::eq("$label", "ParityUser"),
                                SourcePredicate::eq("name", Expr::param("name")),
                            ]))
                            .value_map(Some(vec!["externalId", "name"])),
                        )
                        .returning(["user"]),
                ),
                vec![("name", string("Alice"))],
                vec![("name", QueryParamType::String)],
            ),
        ),
        runtime(
            "032-read-source-predicate-between-param",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "adults",
                            g().n_where(SourcePredicate::and(vec![
                                SourcePredicate::eq("$label", "ParityUser"),
                                SourcePredicate::between("age", Expr::param("min_age"), 65i64),
                            ]))
                            .value_map(Some(vec!["externalId", "age"])),
                        )
                        .returning(["adults"]),
                ),
                vec![("min_age", i64_value(30))],
                vec![("min_age", QueryParamType::I64)],
            ),
        ),
        runtime(
            "900-write-active-text-items",
            write_request(
                write_batch()
                    .var_as(
                        "source",
                        g().add_n(
                            "ParityUser",
                            vec![
                                ("externalId", PropertyInput::from("active-text-source")),
                                ("bio", PropertyInput::from("activeinsertnode")),
                            ],
                        ),
                    )
                    .var_as(
                        "target",
                        g().add_n(
                            "ParityUser",
                            vec![("externalId", PropertyInput::from("active-text-target"))],
                        ),
                    )
                    .var_as(
                        "edge",
                        g().n(NodeRef::var("source")).add_e(
                            "FOLLOWS",
                            NodeRef::var("target"),
                            vec![("note", PropertyInput::from("activeinsertedge"))],
                        ),
                    )
                    .returning(["source", "target", "edge"]),
            ),
        ),
        runtime(
            "901-read-active-text-items",
            read_request(
                read_batch()
                    .var_as(
                        "nodes",
                        g().text_search_nodes("ParityUser", "bio", "activeinsertnode", 5, None)
                            .count(),
                    )
                    .var_as(
                        "edges",
                        g().text_search_edges("FOLLOWS", "note", "activeinsertedge", 5, None)
                            .count(),
                    )
                    .returning(["nodes", "edges"]),
            ),
        ),
        runtime(
            "902-write-remove-indexed-properties",
            write_request(
                write_batch()
                    .var_as(
                        "nodes",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "active-text-source"))
                            .remove_property("bio")
                            .count(),
                    )
                    .var_as(
                        "edges",
                        g().e_with_label("FOLLOWS")
                            .where_(Predicate::eq("note", "activeinsertedge"))
                            .remove_property("note")
                            .count(),
                    )
                    .returning(["nodes", "edges"]),
            ),
        ),
        runtime(
            "903-read-removed-indexed-properties",
            read_request(
                read_batch()
                    .var_as(
                        "nodes",
                        g().text_search_nodes("ParityUser", "bio", "activeinsertnode", 5, None)
                            .count(),
                    )
                    .var_as(
                        "edges",
                        g().text_search_edges("FOLLOWS", "note", "activeinsertedge", 5, None)
                            .count(),
                    )
                    .returning(["nodes", "edges"]),
            ),
        ),
        runtime(
            "904-write-text-drop-candidates",
            write_request(
                write_batch()
                    .var_as(
                        "source",
                        g().add_n(
                            "ParityUser",
                            vec![
                                ("externalId", PropertyInput::from("drop-text-source")),
                                ("bio", PropertyInput::from("dropitemnode")),
                            ],
                        ),
                    )
                    .var_as(
                        "target",
                        g().add_n(
                            "ParityUser",
                            vec![("externalId", PropertyInput::from("drop-text-target"))],
                        ),
                    )
                    .var_as(
                        "edge",
                        g().n(NodeRef::var("source")).add_e(
                            "FOLLOWS",
                            NodeRef::var("target"),
                            vec![("note", PropertyInput::from("dropitemedge"))],
                        ),
                    )
                    .var_as(
                        "source_values",
                        g().n(NodeRef::var("source"))
                            .values(vec!["externalId", "bio"]),
                    )
                    .var_as(
                        "target_values",
                        g().n(NodeRef::var("target")).values(vec!["externalId"]),
                    )
                    .var_as(
                        "edge_values",
                        g().e(EdgeRef::var("edge")).values(vec!["note"]),
                    )
                    .returning(["source_values", "target_values", "edge_values"]),
            ),
        ),
        runtime(
            "905-read-text-drop-candidates",
            read_request(
                read_batch()
                    .var_as(
                        "nodes",
                        g().text_search_nodes("ParityUser", "bio", "dropitemnode", 5, None)
                            .count(),
                    )
                    .var_as(
                        "edges",
                        g().text_search_edges("FOLLOWS", "note", "dropitemedge", 5, None)
                            .count(),
                    )
                    .returning(["nodes", "edges"]),
            ),
        ),
        runtime(
            "906-write-drop-indexed-items",
            write_request(
                write_batch()
                    .var_as(
                        "edge_matches",
                        g().e_with_label("FOLLOWS")
                            .where_(Predicate::eq("note", "dropitemedge")),
                    )
                    .var_as(
                        "edges",
                        g().drop_edge_by_id(EdgeRef::var("edge_matches")).count(),
                    )
                    .var_as(
                        "source",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "drop-text-source"))
                            .drop()
                            .count(),
                    )
                    .var_as(
                        "target",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "drop-text-target"))
                            .drop()
                            .count(),
                    )
                    .var_as(
                        "active_source",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "active-text-source"))
                            .drop()
                            .count(),
                    )
                    .var_as(
                        "active_target",
                        g().n_with_label("ParityUser")
                            .where_(Predicate::eq("externalId", "active-text-target"))
                            .drop()
                            .count(),
                    )
                    .returning([
                        "edges",
                        "source",
                        "target",
                        "active_source",
                        "active_target",
                    ]),
            ),
        ),
        runtime(
            "907-read-dropped-indexed-items",
            read_request(
                read_batch()
                    .var_as(
                        "nodes",
                        g().text_search_nodes("ParityUser", "bio", "dropitemnode", 5, None)
                            .count(),
                    )
                    .var_as(
                        "edges",
                        g().text_search_edges("FOLLOWS", "note", "dropitemedge", 5, None)
                            .count(),
                    )
                    .returning(["nodes", "edges"]),
            ),
        ),
        runtime(
            "908-write-drop-text-indexes",
            write_request(
                write_batch()
                    .var_as(
                        "node_text",
                        g().drop_index(IndexSpec::node_text("ParityUser", "bio", None::<&str>)),
                    )
                    .var_as(
                        "edge_text",
                        g().drop_index(IndexSpec::edge_text("FOLLOWS", "note", None::<&str>)),
                    )
                    .returning(["node_text", "edge_text"]),
            ),
        ),
    ]
}

fn node_permutation_fixtures() -> Vec<Fixture> {
    let sources = ["label", "where", "all"];
    let filters = ["none", "has", "logic", "expr"];
    let bounds = ["none", "limit", "skip", "range"];
    let terminals = ["count", "exists", "value_map", "project"];

    let mut fixtures = Vec::new();
    let mut index = 100;
    for source in sources {
        for filter in filters {
            for bound in bounds {
                for terminal in terminals {
                    let name =
                        format!("{index:03}-combo-node-{source}-{filter}-{bound}-{terminal}");
                    index += 1;
                    fixtures.push(runtime(
                        name,
                        read_request(node_combo_batch(source, filter, bound, terminal)),
                    ));
                }
            }
        }
    }
    fixtures
}

fn node_combo_batch(source: &str, filter: &str, bound: &str, terminal: &str) -> ReadBatch {
    let traversal = apply_node_bound(apply_node_filter(node_source(source), filter), bound)
        .order_by("externalId", Order::Asc);
    let traversal = match terminal {
        "count" => traversal.count(),
        "exists" => traversal.exists(),
        "value_map" => traversal.value_map(Some(vec!["externalId", "name", "age", "status"])),
        "project" => traversal.project(vec![
            Projection::property("externalId", "externalId"),
            Projection::property("status", "status"),
            Projection::expr("age_plus_two", Expr::prop("age").add(Expr::val(2i64))),
        ]),
        other => panic!("unknown terminal {other}"),
    };
    read_batch()
        .var_as("result", traversal)
        .returning(["result"])
}

fn node_source(source: &str) -> Traversal<OnNodes, ReadOnly> {
    match source {
        "label" => g().n_with_label("ParityUser"),
        "where" => g().n_where(SourcePredicate::eq("$label", "ParityUser")),
        "all" => g().n(NodeRef::all()).has_label("ParityUser"),
        other => panic!("unknown source {other}"),
    }
}

fn apply_node_filter(
    traversal: Traversal<OnNodes, ReadOnly>,
    filter: &str,
) -> Traversal<OnNodes, ReadOnly> {
    match filter {
        "none" => traversal,
        "has" => traversal.has("status", "active"),
        "logic" => traversal.where_(Predicate::and(vec![
            Predicate::has_key("externalId"),
            Predicate::or(vec![
                Predicate::starts_with("name", "A"),
                Predicate::ends_with("name", "b"),
            ]),
            Predicate::not(Predicate::is_null("age")),
        ])),
        "expr" => traversal.where_(Predicate::compare(
            Expr::prop("score").add(Expr::val(1.0f64)),
            CompareOp::Gt,
            Expr::val(65.0f64),
        )),
        other => panic!("unknown filter {other}"),
    }
}

fn apply_node_bound(
    traversal: Traversal<OnNodes, ReadOnly>,
    bound: &str,
) -> Traversal<OnNodes, ReadOnly> {
    match bound {
        "none" => traversal,
        "limit" => traversal.limit(2usize),
        "skip" => traversal.skip(1usize),
        "range" => traversal.range(0usize, 2usize),
        other => panic!("unknown bound {other}"),
    }
}

fn json_only_fixtures() -> Vec<Fixture> {
    vec![
        json_only(
            "900-exhaustive-raw-read-steps",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "raw_nodes",
                            g().n(NodeRef::Param("node_ids".to_string()))
                                .has("name", "Alice")
                                .where_(Predicate::contains_param("bio", "needle"))
                                .limit(Expr::param("limit"))
                                .skip(Expr::param("skip"))
                                .range(
                                    StreamBound::literal(0),
                                    StreamBound::expr(Expr::param("end")),
                                )
                                .as_("a")
                                .store("stored")
                                .select("stored")
                                .dedup()
                                .within("stored")
                                .without("missing")
                                .fold()
                                .unfold()
                                .path()
                                .simple_path()
                                .with_sack(PropertyValue::from(0i64))
                                .sack_set("score")
                                .sack_add("score")
                                .sack_get()
                                .project(vec![
                                    Projection::property("externalId", "externalId"),
                                    Projection::expr("neg_age", Expr::prop("age").neg()),
                                ]),
                        )
                        .var_as(
                            "raw_edges",
                            g().e(EdgeRef::Param("edge_ids".to_string()))
                                .where_(SourcePredicate::or(vec![
                                    SourcePredicate::has_key("since"),
                                    SourcePredicate::starts_with("note", "Alice"),
                                ]))
                                .edge_has("weight", PropertyInput::from(1.0f64))
                                .edge_has_label("FOLLOWS")
                                .order_by("weight", Order::Desc)
                                .edge_properties(),
                        )
                        .var_as(
                            "index_operation",
                            g().get_index_operation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"),
                        )
                        .returning(["raw_nodes", "raw_edges", "index_operation"]),
                ),
                vec![
                    ("node_ids", array(vec![i64_value(1), i64_value(2)])),
                    ("edge_ids", array(vec![i64_value(1)])),
                    ("needle", string("graph")),
                    ("limit", i64_value(10)),
                    ("skip", i64_value(0)),
                    ("end", i64_value(10)),
                ],
                vec![
                    (
                        "node_ids",
                        QueryParamType::Array(Box::new(QueryParamType::I64)),
                    ),
                    (
                        "edge_ids",
                        QueryParamType::Array(Box::new(QueryParamType::I64)),
                    ),
                    ("needle", QueryParamType::String),
                    ("limit", QueryParamType::I64),
                    ("skip", QueryParamType::I64),
                    ("end", QueryParamType::I64),
                ],
            ),
        ),
        json_only(
            "901-exhaustive-raw-write-steps",
            write_request(
                write_batch()
                    .var_as(
                        "raw_unique_index",
                        g().create_index_if_not_exists(IndexSpec::node_unique_equality(
                            "ParityUser",
                            "externalId",
                        )),
                    )
                    .var_as(
                        "raw_drop_range_index",
                        g().drop_index(IndexSpec::node_range("ParityUser", "age")),
                    )
                    .var_as(
                        "raw_node_vector_index",
                        g().create_vector_index_nodes(
                            "ParityUser",
                            "embedding",
                            NonZeroUsize::new(3).expect("node vector dimension is non-zero"),
                            VectorDistanceMetric::Cosine,
                            Some("tenantId"),
                        ),
                    )
                    .var_as(
                        "raw_edge_vector_index",
                        g().create_vector_index_edges(
                            "FOLLOWS",
                            "embedding",
                            NonZeroUsize::new(2).expect("edge vector dimension is non-zero"),
                            VectorDistanceMetric::Cosine,
                            Some("tenantId"),
                        ),
                    )
                    .var_as(
                        "raw_node_text_index",
                        g().create_text_index_nodes("ParityUser", "bio", Some("tenantId")),
                    )
                    .var_as(
                        "raw_edge_text_index",
                        g().create_text_index_edges("FOLLOWS", "note", Some("tenantId")),
                    )
                    .var_as(
                        "raw_mutations",
                        g().add_n("RawNode", vec![("name", PropertyInput::from("raw"))])
                            .add_e(
                                "RAW_EDGE",
                                NodeRef::Var("raw_mutations".to_string()),
                                vec![("weight", PropertyInput::from(1i64))],
                            )
                            .set_property("name", PropertyInput::Expr(Expr::param("name")))
                            .remove_property("old")
                            .drop_edge(NodeRef::Ids(vec![999_999]))
                            .drop_edge_labeled(NodeRef::Ids(vec![999_999]), "RAW_EDGE")
                            .drop_edge_by_id(EdgeRef::Ids(vec![999_999]))
                            .drop(),
                    )
                    .var_as(
                        "retry_index_operation",
                        g().retry_index_operation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"),
                    )
                    .var_as(
                        "abort_index_operation",
                        g().abort_index_operation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"),
                    )
                    .returning([
                        "raw_unique_index",
                        "raw_drop_range_index",
                        "raw_node_vector_index",
                        "raw_edge_vector_index",
                        "raw_node_text_index",
                        "raw_edge_text_index",
                        "raw_mutations",
                        "retry_index_operation",
                        "abort_index_operation",
                    ]),
            ),
        ),
        json_only(
            "902-query-value-and-param-type-shapes",
            with_params(
                read_request(
                    read_batch()
                        .var_as("empty", g().n_with_label("Missing").count())
                        .returning(["empty"]),
                ),
                vec![
                    ("null", QueryValue::Null),
                    ("bool", QueryValue::Bool(true)),
                    ("i64", QueryValue::I64(i64::MAX)),
                    ("f64", QueryValue::F64(1.25)),
                    ("f32", QueryValue::F32(1.5)),
                    ("string", string("value")),
                    ("array", array(vec![i64_value(1), string("two")])),
                    ("object", object(vec![("nested", QueryValue::Bool(true))])),
                ],
                vec![
                    ("null", QueryParamType::Value),
                    ("bool", QueryParamType::Bool),
                    ("i64", QueryParamType::I64),
                    ("f64", QueryParamType::F64),
                    ("f32", QueryParamType::F32),
                    ("string", QueryParamType::String),
                    (
                        "array",
                        QueryParamType::Array(Box::new(QueryParamType::Value)),
                    ),
                    ("object", QueryParamType::Object),
                ],
            ),
        ),
        json_only(
            "903-empty-source-vector-text-runtime-inputs",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "vector_nodes",
                            g().vector_search_nodes_with(
                                "ParityUser",
                                "embedding",
                                PropertyInput::param("query_vector"),
                                Expr::param("limit"),
                                Some(PropertyInput::param("tenant")),
                            ),
                        )
                        .var_as(
                            "text_nodes",
                            g().text_search_nodes_with(
                                "ParityUser",
                                "bio",
                                PropertyInput::param("query_text"),
                                Expr::param("limit"),
                                Some(PropertyInput::param("tenant")),
                            ),
                        )
                        .returning(["vector_nodes", "text_nodes"]),
                ),
                vec![
                    (
                        "query_vector",
                        array(vec![f64_value(1.0), f64_value(0.0), f64_value(0.0)]),
                    ),
                    ("query_text", string("graph")),
                    ("limit", i64_value(5)),
                    ("tenant", string("tenant-a")),
                ],
                vec![
                    (
                        "query_vector",
                        QueryParamType::Array(Box::new(QueryParamType::F64)),
                    ),
                    ("query_text", QueryParamType::String),
                    ("limit", QueryParamType::I64),
                    ("tenant", QueryParamType::String),
                ],
            ),
        ),
        json_only(
            "904-empty-query-and-node-edge-ref-shapes",
            read_request(
                read_batch()
                    .var_as("all_nodes", g().n(NodeRef::All).count())
                    .var_as("node_ids", g().n(NodeRef::ids([1, 2])).id())
                    .var_as(
                        "node_var",
                        g().n(NodeRef::Var("all_nodes".to_string())).label(),
                    )
                    .var_as("edge_ids", g().e(EdgeRef::ids([1, 2])).id())
                    .var_as(
                        "edge_var",
                        g().e(EdgeRef::Var("edge_ids".to_string())).label(),
                    )
                    .returning(["all_nodes", "node_ids", "node_var", "edge_ids", "edge_var"]),
            ),
        ),
        json_only(
            "905-empty-traversal-source-mutators",
            write_request(
                write_batch()
                    .var_as(
                        "inject",
                        Traversal::<Empty, ReadOnly>::new()
                            .inject("some_var")
                            .count(),
                    )
                    .var_as(
                        "drop_edge_by_id",
                        g().drop_edge_by_id(EdgeRef::id(123_456)).count(),
                    )
                    .returning(["inject", "drop_edge_by_id"]),
            ),
        ),
        json_only(
            "906-nested-query-property-write-shapes",
            with_params(
                write_request(
                    write_batch()
                        .var_as(
                            "created",
                            g().add_n(
                                "ParityNested",
                                vec![
                                    ("name", PropertyInput::from("nested")),
                                    (
                                        "metadata",
                                        PropertyInput::from(nested_metadata_property(
                                            "some_id", 20,
                                        )),
                                    ),
                                ],
                            ),
                        )
                        .var_as(
                            "updated",
                            g().n(NodeRef::var("created"))
                                .set_property("metadata", PropertyInput::param("metadata"))
                                .value_map(Some(vec!["metadata.externalID"])),
                        )
                        .var_as(
                            "target",
                            g().add_n(
                                "ParityNestedTarget",
                                vec![("name", PropertyInput::from("target"))],
                            ),
                        )
                        .var_as(
                            "edge",
                            g().n(NodeRef::var("created"))
                                .add_e(
                                    "NESTED_LINK",
                                    NodeRef::var("target"),
                                    vec![(
                                        "metadata",
                                        PropertyInput::from(nested_metadata_property("edge_id", 5)),
                                    )],
                                )
                                .count(),
                        )
                        .returning(["created", "updated", "edge"]),
                ),
                vec![("metadata", nested_metadata_param("param_id", 22))],
                vec![("metadata", QueryParamType::Object)],
            ),
        ),
        json_only(
            "907-nested-query-property-read-shapes",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "nested_users",
                            g().n_where(SourcePredicate::and(vec![
                                SourcePredicate::eq("$label", "ParityNested"),
                                SourcePredicate::eq(
                                    "metadata.externalID",
                                    Expr::param("external_id"),
                                ),
                            ]))
                            .where_(Predicate::compare(
                                Expr::prop("metadata.score"),
                                CompareOp::Gt,
                                Expr::val(10i64),
                            ))
                            .order_by_multiple(vec![
                                ("metadata.score", Order::Desc),
                                ("name", Order::Asc),
                            ])
                            .project(vec![
                                Projection::property("metadata.externalID", "external_id"),
                                Projection::expr("score_copy", Expr::prop("metadata.score")),
                            ]),
                        )
                        .var_as(
                            "nested_values",
                            g().n_with_label("ParityNested")
                                .values(vec!["metadata.externalID"]),
                        )
                        .var_as(
                            "nested_map",
                            g().n_with_label("ParityNested")
                                .value_map(Some(vec!["metadata.externalID", "metadata.score"])),
                        )
                        .var_as(
                            "nested_edges",
                            g().e_where(SourcePredicate::and(vec![
                                SourcePredicate::eq("$label", "NESTED_LINK"),
                                SourcePredicate::eq("metadata.externalID", "edge_id"),
                            ]))
                            .edge_has("metadata.externalID", PropertyInput::from("edge_id"))
                            .edge_properties(),
                        )
                        .returning([
                            "nested_users",
                            "nested_values",
                            "nested_map",
                            "nested_edges",
                        ]),
                ),
                vec![("external_id", string("param_id"))],
                vec![("external_id", QueryParamType::String)],
            ),
        ),
        json_only(
            "908-edge-endpoint-projection",
            read_request(
                read_batch()
                    .var_as(
                        "endpoints",
                        g().e_with_label("FOLLOWS").project(vec![
                            Projection::from_endpoint("externalId", "from_id"),
                            Projection::to_endpoint("externalId", "to_id"),
                            Projection::property("$id", "edge_id"),
                        ]),
                    )
                    .returning(["endpoints"]),
            ),
        ),
        json_only(
            "909-row-binding-basic-projection",
            read_request(
                read_batch()
                    .var_as(
                        "bindings",
                        g().n_with_label("ParityService")
                            .bind("service")
                            .project_bindings(vec![
                                BindingProjection::binding("service", "$id", "service_id"),
                                BindingProjection::current("metadata.name", "current_name"),
                                BindingProjection::binding(
                                    "missing_binding",
                                    "externalId",
                                    "missing_external_id",
                                ),
                            ]),
                    )
                    .returning(["bindings"]),
            ),
        ),
        json_only(
            "910-row-binding-branch-distinct-projection",
            read_request(
                read_batch()
                    .var_as(
                        "workloads",
                        g().n_with_label("ParityService")
                            .bind("service")
                            .out(Some("ROUTES_TO"))
                            .bind("pod")
                            .optional(sub().in_(Some("CREATES")).bind("deployment"))
                            .union(vec![
                                sub().in_(Some("MANAGES")).bind("owner"),
                                sub().out(Some("ROUTES_TO")).bind("workload"),
                            ])
                            .project_distinct_bindings(vec![
                                BindingProjection::binding("service", "$id", "service_id"),
                                BindingProjection::coalesce(
                                    vec![
                                        BindingValueRef::binding("deployment", "$id"),
                                        BindingValueRef::binding("owner", "$id"),
                                        BindingValueRef::binding("workload", "$id"),
                                    ],
                                    "workload_id",
                                ),
                            ]),
                    )
                    .returning(["workloads"]),
            ),
        ),
        json_only(
            "911-range-index-direction",
            write_request(
                write_batch()
                    .var_as(
                        "node_desc",
                        g().create_index_if_not_exists(IndexSpec::node_range_desc(
                            "ParityUser",
                            "age",
                        )),
                    )
                    .var_as(
                        "edge_desc",
                        g().create_index_if_not_exists(IndexSpec::edge_range_desc(
                            "FOLLOWS", "weight",
                        )),
                    )
                    .var_as(
                        "node_asc",
                        g().create_index_if_not_exists(IndexSpec::node_range(
                            "ParityUser",
                            "score",
                        )),
                    )
                    .returning(["node_desc", "edge_desc", "node_asc"]),
            ),
        ),
        json_only(
            "912-shortest-path-terminal",
            with_params(
                read_request(
                    read_batch()
                        .var_as(
                            "path",
                            g().shortest_path_with(
                                NodeRef::id(1),
                                NodeRef::param("target"),
                                Some("FOLLOWS"),
                                ShortestPathDirection::Both,
                                5,
                            ),
                        )
                        .returning(["path"]),
                ),
                vec![("target", i64_value(3))],
                vec![("target", QueryParamType::I64)],
            ),
        ),
        remaining_read_contract_fixture(),
        remaining_write_contract_fixture(),
    ]
}

fn remaining_read_contract_fixture() -> Fixture {
    let comparisons = Predicate::and(vec![
        Predicate::neq("neq", 1i64),
        Predicate::gt("gt", 1i64),
        Predicate::gte("gte", 1i64),
        Predicate::lt("lt", 1i64),
        Predicate::lte("lte", 1i64),
        Predicate::between("between", 1i64, 3i64),
        Predicate::ends_with("suffix", "end"),
        Predicate::is_in("status", vec!["active".to_string(), "inactive".to_string()]),
        Predicate::is_null("missing"),
        Predicate::is_not_null("present"),
        Predicate::not(Predicate::eq("disabled", true)),
        Predicate::compare(Expr::id(), CompareOp::Eq, Expr::val(1i64)),
        Predicate::compare(Expr::id(), CompareOp::Neq, Expr::val(1i64)),
        Predicate::compare(Expr::id(), CompareOp::Gt, Expr::val(1i64)),
        Predicate::compare(Expr::id(), CompareOp::Gte, Expr::val(1i64)),
        Predicate::compare(Expr::id(), CompareOp::Lt, Expr::val(1i64)),
        Predicate::compare(Expr::id(), CompareOp::Lte, Expr::val(1i64)),
    ]);
    let mut request = read_request(
        read_batch()
            .var_as(
                "expressions_and_predicates",
                g().n(NodeRef::all()).where_(comparisons).project(vec![
                    Projection::expr("id", Expr::id()),
                    Projection::expr("timestamp", Expr::timestamp()),
                    Projection::expr("datetime", Expr::datetime()),
                    Projection::expr("null", Expr::val(PropertyValue::Null)),
                    Projection::expr(
                        "date_value",
                        Expr::val(PropertyValue::datetime_millis(1_777_000_000_000)),
                    ),
                    Projection::expr("f32", Expr::val(PropertyValue::F32(1.25))),
                    Projection::expr("bytes", Expr::val(PropertyValue::Bytes(vec![1, 2, 3]))),
                    Projection::expr(
                        "i64_array",
                        Expr::val(PropertyValue::I64Array(vec![1, 2, 3])),
                    ),
                    Projection::expr(
                        "f64_array",
                        Expr::val(PropertyValue::F64Array(vec![1.25, 2.5])),
                    ),
                    Projection::expr("add", Expr::val(4i64).add_expr(Expr::val(1i64))),
                    Projection::expr("sub", Expr::val(4i64).sub_expr(Expr::val(1i64))),
                    Projection::expr("mul", Expr::val(4i64).mul_expr(Expr::val(2i64))),
                    Projection::expr("div", Expr::val(4i64).div_expr(Expr::val(2i64))),
                    Projection::expr("mod", Expr::val(5i64).modulo(Expr::val(2i64))),
                    Projection::expr(
                        "case",
                        Expr::case(
                            vec![(Predicate::eq("status", "active"), Expr::val("enabled"))],
                            Some(Expr::val("disabled")),
                        ),
                    ),
                ]),
            )
            .var_as("both", g().n(NodeRef::id(1)).both(None::<&str>).count())
            .var_as(
                "in_e",
                g().n(NodeRef::id(1)).in_e(None::<&str>).edge_properties(),
            )
            .var_as(
                "out_e",
                g().n(NodeRef::id(1)).out_e(None::<&str>).edge_properties(),
            )
            .var_as(
                "both_e",
                g().n(NodeRef::id(1)).both_e(None::<&str>).edge_properties(),
            )
            .var_as(
                "in_n",
                g().e(EdgeRef::all()).in_n().value_map(None::<Vec<&str>>),
            )
            .var_as(
                "out_n",
                g().e(EdgeRef::all()).out_n().value_map(None::<Vec<&str>>),
            )
            .var_as(
                "other_n",
                g().e(EdgeRef::all()).other_n().value_map(None::<Vec<&str>>),
            )
            .var_as(
                "direct_has_key",
                g().n(NodeRef::all()).has_key("externalId").count(),
            )
            .var_as(
                "has_label",
                g().n(NodeRef::all()).has_label("ParityUser").count(),
            )
            .var_as("exists", g().n(NodeRef::all()).exists())
            .var_as(
                "choose",
                g().n(NodeRef::all())
                    .choose(
                        Predicate::is_not_null("status"),
                        sub().out(None::<&str>),
                        Some(sub().in_(None::<&str>)),
                    )
                    .count(),
            )
            .var_as(
                "coalesce",
                g().n(NodeRef::all())
                    .coalesce(vec![sub().out(None::<&str>), sub().in_(None::<&str>)])
                    .count(),
            )
            .var_as("group", g().n(NodeRef::all()).group("status"))
            .var_as("group_count", g().n(NodeRef::all()).group_count("status"))
            .var_as(
                "aggregate_count",
                g().n(NodeRef::all())
                    .aggregate_by(AggregateFunction::Count, "age"),
            )
            .var_as(
                "aggregate_sum",
                g().n(NodeRef::all())
                    .aggregate_by(AggregateFunction::Sum, "age"),
            )
            .var_as(
                "aggregate_min",
                g().n(NodeRef::all())
                    .aggregate_by(AggregateFunction::Min, "age"),
            )
            .var_as(
                "aggregate_max",
                g().n(NodeRef::all())
                    .aggregate_by(AggregateFunction::Max, "age"),
            )
            .var_as(
                "aggregate_mean",
                g().n(NodeRef::all())
                    .aggregate_by(AggregateFunction::Mean, "age"),
            )
            .var_as(
                "repeat_none",
                g().n(NodeRef::id(1))
                    .repeat(RepeatConfig::new(sub().out(None::<&str>)))
                    .count(),
            )
            .var_as(
                "repeat_before",
                g().n(NodeRef::id(1))
                    .repeat(RepeatConfig::new(sub().out(None::<&str>)).emit_before())
                    .count(),
            )
            .var_as(
                "repeat_after",
                g().n(NodeRef::id(1))
                    .repeat(RepeatConfig::new(sub().out(None::<&str>)).emit_after())
                    .count(),
            )
            .var_as(
                "repeat_all",
                g().n(NodeRef::id(1))
                    .repeat(RepeatConfig::new(sub().out(None::<&str>)).emit_all())
                    .count(),
            )
            .var_as(
                "shortest_out",
                g().shortest_path_with(
                    NodeRef::id(1),
                    NodeRef::id(2),
                    None::<&str>,
                    ShortestPathDirection::Out,
                    5,
                ),
            )
            .var_as(
                "shortest_in",
                g().shortest_path_with(
                    NodeRef::id(1),
                    NodeRef::id(2),
                    None::<&str>,
                    ShortestPathDirection::In,
                    5,
                ),
            )
            .var_as(
                "vector_edges",
                g().vector_search_edges("FOLLOWS", "embedding", vec![1.0f32, 0.0], 5usize, None)
                    .edge_properties(),
            )
            .var_as(
                "vector_nodes_within",
                g().n_with_label("ParityUser").vector_search(
                    "ParityUser",
                    "embedding",
                    vec![1.0f32, 0.0, 0.0],
                    5,
                    None,
                ),
            )
            .var_as(
                "vector_edges_within",
                g().e(EdgeRef::all()).has_label("FOLLOWS").vector_search(
                    "FOLLOWS",
                    "embedding",
                    vec![1.0f32, 0.0],
                    5,
                    None,
                ),
            )
            .var_as(
                "text_edges",
                g().text_search_edges("FOLLOWS", "note", "graph", 5usize, None)
                    .edge_properties(),
            )
            .var_as(
                "text_nodes_within",
                g().n_with_label("ParityUser")
                    .text_search("ParityUser", "bio", "graph", 5, None),
            )
            .var_as(
                "text_edges_within",
                g().e(EdgeRef::all())
                    .has_label("FOLLOWS")
                    .text_search("FOLLOWS", "note", "graph", 5, None),
            )
            .var_as_if(
                "previous",
                BatchCondition::PrevNotEmpty,
                g().n(NodeRef::all()).count(),
            )
            .var_as_if(
                "not_empty",
                BatchCondition::VarNotEmpty("expressions_and_predicates".to_string()),
                g().n(NodeRef::all()).count(),
            )
            .var_as_if(
                "empty",
                BatchCondition::VarEmpty("missing".to_string()),
                g().n(NodeRef::all()).count(),
            )
            .var_as_if(
                "min_size",
                BatchCondition::VarMinSize("expressions_and_predicates".to_string(), 1),
                g().n(NodeRef::all()).count(),
            )
            .for_each_param(
                "rows",
                read_batch().var_as("foreach", g().n(NodeRef::all()).count()),
            )
            .returning([
                "expressions_and_predicates",
                "both",
                "in_e",
                "out_e",
                "both_e",
                "in_n",
                "out_n",
                "other_n",
                "direct_has_key",
                "has_label",
                "exists",
                "choose",
                "coalesce",
                "group",
                "group_count",
                "aggregate_count",
                "aggregate_sum",
                "aggregate_min",
                "aggregate_max",
                "aggregate_mean",
                "repeat_none",
                "repeat_before",
                "repeat_after",
                "repeat_all",
                "shortest_out",
                "shortest_in",
                "vector_edges",
                "vector_nodes_within",
                "vector_edges_within",
                "text_edges",
                "text_nodes_within",
                "text_edges_within",
                "previous",
                "not_empty",
                "empty",
                "min_size",
                "foreach",
            ]),
    );
    request
        .try_insert_typed_parameter(
            "date_time",
            QueryParamType::DateTime,
            string("2026-01-01T00:00:00.000Z"),
        )
        .expect("fixture datetime should be valid");
    json_only("913-remaining-read-contract", request)
}

fn remaining_write_contract_fixture() -> Fixture {
    json_only(
        "914-remaining-write-contract",
        write_request(
            write_batch()
                .var_as(
                    "edge_equality",
                    g().create_index_if_not_exists(IndexSpec::edge_equality("FOLLOWS", "since")),
                )
                .var_as(
                    "node_euclidean",
                    g().create_index_if_not_exists(IndexSpec::node_vector(
                        "ParityUser",
                        "euclidean_embedding",
                        NonZeroUsize::new(4).expect("node vector dimension is non-zero"),
                        VectorDistanceMetric::Euclidean,
                        None::<&str>,
                    )),
                )
                .var_as(
                    "edge_manhattan",
                    g().create_index_if_not_exists(IndexSpec::edge_vector(
                        "FOLLOWS",
                        "manhattan_embedding",
                        NonZeroUsize::new(4).expect("edge vector dimension is non-zero"),
                        VectorDistanceMetric::Manhattan,
                        None::<&str>,
                    )),
                )
                .returning(["edge_equality", "node_euclidean", "edge_manhattan"]),
        ),
    )
}
