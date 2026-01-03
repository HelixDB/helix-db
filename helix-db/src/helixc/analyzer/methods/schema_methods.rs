use std::{borrow::Cow, collections::HashMap};

use crate::helixc::{
    analyzer::{Ctx, error_codes::ErrorCode, errors::push_schema_err},
    parser::{
        errors::ParserError,
        location::Loc,
        types::{Field, FieldPrefix, FieldType, Source},
    },
};

type FieldLookup<'a> = HashMap<&'a str, HashMap<&'a str, Cow<'a, Field>>>;

pub(crate) struct SchemaVersionMap<'a>(
    HashMap<usize, (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>)>,
);

impl<'a> SchemaVersionMap<'a> {
    pub fn get_latest(&self) -> (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>) {
        self.0
            .get(self.0.keys().max().unwrap_or(&1))
            .unwrap_or(&(HashMap::new(), HashMap::new(), HashMap::new()))
            .clone()
    }

    pub fn inner(&self) -> &HashMap<usize, (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>)> {
        &self.0
    }
}

pub(crate) fn build_field_lookups<'a>(src: &'a Source) -> SchemaVersionMap<'a> {
    SchemaVersionMap(
        src.get_schemas_in_order()
            .iter()
            .map(|schema| {
                let node_fields = schema
                    .node_schemas
                    .iter()
                    .map(|n| {
                        let mut props = n
                            .fields
                            .iter()
                            .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                            .collect::<HashMap<&str, Cow<'a, Field>>>();
                        props.insert(
                            "id",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "id".to_string(),
                                field_type: FieldType::Uuid,
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "label",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "label".to_string(),
                                field_type: FieldType::String,
                                loc: Loc::empty(),
                            }),
                        );
                        (n.name.1.as_str(), props)
                    })
                    .collect();

                let edge_fields = schema
                    .edge_schemas
                    .iter()
                    .map(|e| {
                        let mut props: HashMap<_, _> = e
                            .properties
                            .as_ref()
                            .map(|v| {
                                v.iter()
                                    .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                                    .collect()
                            })
                            .unwrap_or_default();
                        props.insert(
                            "id",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "id".to_string(),
                                field_type: FieldType::Uuid,
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "label",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "label".to_string(),
                                field_type: FieldType::String,
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "from_node",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "from_node".to_string(),
                                field_type: FieldType::Uuid,
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "to_node",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "to_node".to_string(),
                                field_type: FieldType::Uuid,
                                loc: Loc::empty(),
                            }),
                        );
                        (e.name.1.as_str(), props)
                    })
                    .collect();

                let vector_fields = schema
                    .vector_schemas
                    .iter()
                    .map(|v| {
                        let mut props = v
                            .fields
                            .iter()
                            .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                            .collect::<HashMap<&str, Cow<'a, Field>>>();
                        props.insert(
                            "id",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "id".to_string(),
                                field_type: FieldType::Uuid,
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "label",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "label".to_string(),
                                field_type: FieldType::String,
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "data",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "data".to_string(),
                                field_type: FieldType::Array(Box::new(FieldType::F64)),
                                loc: Loc::empty(),
                            }),
                        );
                        props.insert(
                            "score",
                            Cow::Owned(Field {
                                prefix: FieldPrefix::Empty,
                                defaults: None,
                                name: "score".to_string(),
                                field_type: FieldType::F64,
                                loc: Loc::empty(),
                            }),
                        );
                        (v.name.as_str(), props)
                    })
                    .collect();

                (schema.version.1, (node_fields, edge_fields, vector_fields))
            })
            .collect(),
    )
}

fn check_duplicate_schema_definitions(ctx: &mut Ctx) -> Result<(), ParserError> {
    use std::collections::HashMap;

    // Track seen names for each schema type
    let mut seen_nodes: HashMap<String, (crate::helixc::parser::location::Loc, String)> =
        HashMap::new();
    let mut seen_edges: HashMap<String, (crate::helixc::parser::location::Loc, String)> =
        HashMap::new();
    let mut seen_vectors: HashMap<String, (crate::helixc::parser::location::Loc, String)> =
        HashMap::new();

    let schema = ctx.src.get_latest_schema()?;

    // Check duplicate nodes and reserved names
    for node in &schema.node_schemas {
        // Check for reserved type names
        if RESERVED_TYPE_NAMES.contains(&node.name.1.as_str()) {
            push_schema_err(
                ctx,
                node.name.0.clone(),
                ErrorCode::E110,
                format!(
                    "`{}` is a reserved type name and cannot be used as a node name",
                    node.name.1
                ),
                Some("rename the node to something else".to_string()),
            );
        }
        if let Some((_first_loc, _)) = seen_nodes.get(&node.name.1) {
            push_schema_err(
                ctx,
                node.name.0.clone(),
                ErrorCode::E107,
                format!("duplicate node definition `{}`", node.name.1),
                Some("rename the node or remove the duplicate definition".to_string()),
            );
        } else {
            seen_nodes.insert(
                node.name.1.clone(),
                (node.name.0.clone(), node.name.1.clone()),
            );
        }
    }

    // Check duplicate edges and reserved names
    for edge in &schema.edge_schemas {
        // Check for reserved type names
        if RESERVED_TYPE_NAMES.contains(&edge.name.1.as_str()) {
            push_schema_err(
                ctx,
                edge.name.0.clone(),
                ErrorCode::E110,
                format!(
                    "`{}` is a reserved type name and cannot be used as an edge name",
                    edge.name.1
                ),
                Some("rename the edge to something else".to_string()),
            );
        }
        if let Some((_first_loc, _)) = seen_edges.get(&edge.name.1) {
            push_schema_err(
                ctx,
                edge.name.0.clone(),
                ErrorCode::E107,
                format!("duplicate edge definition `{}`", edge.name.1),
                Some("rename the edge or remove the duplicate definition".to_string()),
            );
        } else {
            seen_edges.insert(
                edge.name.1.clone(),
                (edge.name.0.clone(), edge.name.1.clone()),
            );
        }
    }

    // Check duplicate vectors and reserved names
    for vector in &schema.vector_schemas {
        // Check for reserved type names
        if RESERVED_TYPE_NAMES.contains(&vector.name.as_str()) {
            push_schema_err(
                ctx,
                vector.loc.clone(),
                ErrorCode::E110,
                format!(
                    "`{}` is a reserved type name and cannot be used as a vector name",
                    vector.name
                ),
                Some("rename the vector to something else".to_string()),
            );
        }
        if let Some((_first_loc, _)) = seen_vectors.get(&vector.name) {
            push_schema_err(
                ctx,
                vector.loc.clone(),
                ErrorCode::E107,
                format!("duplicate vector definition `{}`", vector.name),
                Some("rename the vector or remove the duplicate definition".to_string()),
            );
        } else {
            seen_vectors.insert(
                vector.name.clone(),
                (vector.loc.clone(), vector.name.clone()),
            );
        }
    }
    Ok(())
}

pub(crate) fn check_schema(ctx: &mut Ctx) -> Result<(), ParserError> {
    // Check for duplicate schema definitions
    check_duplicate_schema_definitions(ctx)?;

    for edge in &ctx.src.get_latest_schema()?.edge_schemas {
        if !ctx.node_set.contains(edge.from.1.as_str())
            && !ctx.vector_set.contains(edge.from.1.as_str())
        {
            push_schema_err(
                ctx,
                edge.from.0.clone(),
                ErrorCode::E106,
                format!(
                    "use of undeclared node or vector type `{}` in schema",
                    edge.from.1
                ),
                Some(format!(
                    "declare `{}` in the schema before using it in an edge",
                    edge.from.1
                )),
            );
        }
        if !ctx.node_set.contains(edge.to.1.as_str())
            && !ctx.vector_set.contains(edge.to.1.as_str())
        {
            push_schema_err(
                ctx,
                edge.to.0.clone(),
                ErrorCode::E106,
                format!(
                    "use of undeclared node or vector type `{}` in schema",
                    edge.to.1
                ),
                Some(format!(
                    "declare `{}` in the schema before using it in an edge",
                    edge.to.1
                )),
            );
        }
        if let Some(v) = edge.properties.as_ref() {
            // Check for duplicate field names (case-insensitive)
            let mut seen_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for f in v {
                let lower_name = f.name.to_lowercase();
                if !seen_fields.insert(lower_name) {
                    push_schema_err(
                        ctx,
                        f.loc.clone(),
                        ErrorCode::E109,
                        format!("duplicate field `{}` in edge `{}`", f.name, edge.name.1),
                        Some("rename the field or remove the duplicate".to_string()),
                    );
                }
                if NODE_RESERVED_FIELD_NAMES.contains(&f.name.to_lowercase().as_str()) {
                    push_schema_err(
                        ctx,
                        f.loc.clone(),
                        ErrorCode::E204,
                        format!("field `{}` is a reserved field name", f.name),
                        Some("rename the field".to_string()),
                    );
                }
                if !is_valid_schema_field_type(&f.field_type) {
                    push_schema_err(
                        ctx,
                        f.loc.clone(),
                        ErrorCode::E209,
                        format!("invalid type in schema field: `{}`", f.name),
                        Some("use built-in types only (String, U32, etc.)".to_string()),
                    );
                }
            }
        }
        ctx.output.edges.push(edge.clone().into());
    }
    for node in &ctx.src.get_latest_schema()?.node_schemas {
        // Check for duplicate field names (case-insensitive)
        let mut seen_fields: std::collections::HashSet<String> = std::collections::HashSet::new();
        for f in &node.fields {
            let lower_name = f.name.to_lowercase();
            if !seen_fields.insert(lower_name) {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E109,
                    format!("duplicate field `{}` in node `{}`", f.name, node.name.1),
                    Some("rename the field or remove the duplicate".to_string()),
                );
            }
            if EDGE_RESERVED_FIELD_NAMES.contains(&f.name.to_lowercase().as_str()) {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E204,
                    format!("field `{}` is a reserved field name", f.name),
                    Some("rename the field".to_string()),
                );
            }
            if !is_valid_schema_field_type(&f.field_type) {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E209,
                    format!("invalid type in schema field: `{}`", f.name),
                    Some("use built-in types only (String, U32, etc.)".to_string()),
                );
            }
        }
        ctx.output.nodes.push(node.clone().into());
    }
    for vector in &ctx.src.get_latest_schema()?.vector_schemas {
        // Check for duplicate field names (case-insensitive)
        let mut seen_fields: std::collections::HashSet<String> = std::collections::HashSet::new();
        for f in &vector.fields {
            let lower_name = f.name.to_lowercase();
            if !seen_fields.insert(lower_name) {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E109,
                    format!("duplicate field `{}` in vector `{}`", f.name, vector.name),
                    Some("rename the field or remove the duplicate".to_string()),
                );
            }
            if VEC_RESERVED_FIELD_NAMES.contains(&f.name.to_lowercase().as_str()) {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E204,
                    format!("field `{}` is a reserved field name", f.name),
                    Some("rename the field".to_string()),
                );
            }
            if !is_valid_schema_field_type(&f.field_type) {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E209,
                    format!("invalid type in schema field: `{}`", f.name),
                    Some("use built-in types only (String, U32, etc.)".to_string()),
                );
            }
        }
        ctx.output.vectors.push(vector.clone().into());
    }
    Ok(())
}

fn is_valid_schema_field_type(ft: &FieldType) -> bool {
    match ft {
        FieldType::Identifier(_) => false,
        FieldType::Object(_) => false,
        FieldType::Array(inner) => is_valid_schema_field_type(inner),
        _ => true,
    }
}

const NODE_RESERVED_FIELD_NAMES: &[&str] = &["id", "label", "type", "version"];
const EDGE_RESERVED_FIELD_NAMES: &[&str] =
    &["id", "label", "to_node", "from_node", "type", "version"];
const VEC_RESERVED_FIELD_NAMES: &[&str] = &["id", "label", "data", "score", "type", "version"];

/// Reserved type names that cannot be used as schema item names (node, edge, vector).
/// These names conflict with built-in helix-db types and imports.
const RESERVED_TYPE_NAMES: &[&str] = &[
    // Core graph types
    "Node",
    "Edge",
    "HVector",
    // Protocol and error types
    "Value",
    "GraphError",
    "VectorError",
    // Other commonly used types
    "Response",
    "HandlerInput",
    "Aggregate",
    "AggregateItem",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Duplicate Schema Definition Tests
    // ============================================================================

    #[test]
    fn test_duplicate_node_definition() {
        let source = r#"
            N::Person { name: String }
            N::Person { age: U32 }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E107));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("duplicate node definition"))
        );
    }

    #[test]
    fn test_duplicate_edge_definition() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }
            E::Knows { From: Person, To: Person, Properties: { since: String } }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E107));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("duplicate edge definition"))
        );
    }

    #[test]
    fn test_duplicate_vector_definition() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }
            V::Document { title: String, embedding: [F32] }

            QUERY test() =>
                d <- V<Document>
                RETURN d
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E107));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("duplicate vector definition"))
        );
    }

    // ============================================================================
    // Undeclared Type Reference Tests
    // ============================================================================

    #[test]
    fn test_edge_references_undeclared_from_node() {
        let source = r#"
            N::Person { name: String }
            E::Works { From: Company, To: Person }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E106));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("undeclared node or vector type")
                    && d.message.contains("Company"))
        );
    }

    #[test]
    fn test_edge_references_undeclared_to_node() {
        let source = r#"
            N::Person { name: String }
            E::Likes { From: Person, To: Product }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E106));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("undeclared node or vector type")
                    && d.message.contains("Product"))
        );
    }

    #[test]
    fn test_edge_with_valid_node_references() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E106));
    }

    #[test]
    fn test_edge_references_vector_type() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }
            N::Person { name: String }
            E::References { From: Person, To: Document }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should not error - vectors can be referenced in edges
        assert!(
            !diagnostics
                .iter()
                .any(|d| d.error_code == ErrorCode::E106 && d.message.contains("Document"))
        );
    }

    // ============================================================================
    // Reserved Field Name Tests
    // ============================================================================

    #[test]
    fn test_reserved_field_name_id_in_node() {
        let source = r#"
            N::Person { id: String, name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E204));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("reserved field name") && d.message.contains("id"))
        );
    }

    #[test]
    fn test_reserved_field_name_label_in_edge() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person, Properties: { label: String } }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E204));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("reserved field name") && d.message.contains("label"))
        );
    }

    #[test]
    fn test_reserved_field_name_to_node() {
        let source = r#"
            N::Person { to_node: ID }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E204));
    }

    #[test]
    fn test_reserved_field_name_data_in_vector() {
        let source = r#"
            V::Document { data: String, embedding: [F32] }

            QUERY test() =>
                d <- V<Document>
                RETURN d
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E204));
    }

    #[test]
    fn test_reserved_field_name_case_insensitive() {
        let source = r#"
            N::Person { ID: String }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Reserved field names are checked case-insensitively
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E204));
    }

    // ============================================================================
    // Invalid Schema Field Type Tests
    // ============================================================================

    #[test]
    fn test_valid_primitive_field_types() {
        let source = r#"
            N::Person {
                name: String,
                age: U32,
                score: F64,
                active: Boolean,
                user_id: ID,
                created_at: Date
            }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should not have any E209 errors for these valid types
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E209));
    }

    #[test]
    fn test_valid_array_field_types() {
        let source = r#"
            N::Person {
                tags: [String],
                scores: [F64],
                ids: [ID]
            }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E209));
    }

    // ============================================================================
    // Field Lookup Tests
    // ============================================================================

    #[test]
    fn test_field_lookup_includes_implicit_fields() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let ctx = Ctx::new(&parsed).unwrap();

        // Node should have implicit 'id' and 'label' fields
        let person_fields = ctx.node_fields.get("Person").unwrap();
        assert!(person_fields.contains_key("id"));
        assert!(person_fields.contains_key("label"));
        assert!(person_fields.contains_key("name"));
    }

    #[test]
    fn test_edge_field_lookup_includes_implicit_fields() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person, Properties: { since: Date } }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let ctx = Ctx::new(&parsed).unwrap();

        // Edge should have implicit fields
        let knows_fields = ctx.edge_fields.get("Knows").unwrap();
        assert!(knows_fields.contains_key("id"));
        assert!(knows_fields.contains_key("label"));
        assert!(knows_fields.contains_key("from_node"));
        assert!(knows_fields.contains_key("to_node"));
        assert!(knows_fields.contains_key("since"));
    }

    #[test]
    fn test_vector_field_lookup_includes_implicit_fields() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY test() =>
                d <- V<Document>
                RETURN d
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let ctx = Ctx::new(&parsed).unwrap();

        // Vector should have implicit fields
        let doc_fields = ctx.vector_fields.get("Document").unwrap();
        assert!(doc_fields.contains_key("id"));
        assert!(doc_fields.contains_key("label"));
        assert!(doc_fields.contains_key("data"));
        assert!(doc_fields.contains_key("score"));
        assert!(doc_fields.contains_key("content"));
    }

    // ============================================================================
    // Complex Schema Tests
    // ============================================================================

    #[test]
    fn test_multiple_edges_between_same_types() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }
            E::Follows { From: Person, To: Person }
            E::Blocks { From: Person, To: Person }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should not error - multiple edges between same types is valid
        assert!(
            !diagnostics
                .iter()
                .any(|d| d.error_code == ErrorCode::E106 || d.error_code == ErrorCode::E107)
        );
    }

    #[test]
    fn test_schema_with_no_nodes_only_vectors() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY test() =>
                d <- V<Document>
                RETURN d
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        // Schema with only vectors should be valid
    }

    // ============================================================================
    // Duplicate Field Name Tests
    // ============================================================================

    #[test]
    fn test_duplicate_field_name_in_node() {
        let source = r#"
            N::Person { name: String, age: U32, name: U32 }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E109));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("duplicate field") && d.message.contains("name"))
        );
    }

    #[test]
    fn test_duplicate_field_name_in_edge() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person, Properties: { since: Date, since: String } }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E109));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("duplicate field") && d.message.contains("since"))
        );
    }

    #[test]
    fn test_duplicate_field_name_in_vector() {
        let source = r#"
            V::Document { content: String, embedding: [F32], content: U32 }

            QUERY test() =>
                d <- V<Document>
                RETURN d
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E109));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("duplicate field") && d.message.contains("content"))
        );
    }

    #[test]
    fn test_duplicate_field_name_case_insensitive() {
        let source = r#"
            N::Person { name: String, Name: U32 }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should detect "Name" as duplicate of "name" (case-insensitive)
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E109));
    }

    #[test]
    fn test_no_duplicate_field_names_valid_schema() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }
            E::Knows { From: Person, To: Person, Properties: { since: Date, strength: F64 } }
            V::Document { content: String, embedding: [F32] }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should not have any E109 errors for valid schema
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E109));
    }

    #[test]
    fn test_multiple_duplicate_fields_in_node() {
        let source = r#"
            N::Person { name: String, age: U32, name: U32, age: F64 }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should detect both duplicates
        let dup_errors: Vec<_> = diagnostics
            .iter()
            .filter(|d| d.error_code == ErrorCode::E109)
            .collect();
        assert_eq!(dup_errors.len(), 2);
    }

    // ============================================================================
    // Reserved Type Name Tests
    // ============================================================================

    #[test]
    fn test_reserved_type_name_node() {
        let source = r#"
            N::Node { name: String }

            QUERY test() =>
                n <- N<Node>
                RETURN n
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E110));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("reserved type name") && d.message.contains("Node"))
        );
    }

    #[test]
    fn test_reserved_type_name_edge() {
        let source = r#"
            N::Person { name: String }
            E::Edge { From: Person, To: Person }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E110));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("reserved type name") && d.message.contains("Edge"))
        );
    }

    #[test]
    fn test_reserved_type_name_hvector() {
        let source = r#"
            V::HVector { content: String, embedding: [F32] }

            QUERY test() =>
                v <- V<HVector>
                RETURN v
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E110));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("reserved type name") && d.message.contains("HVector"))
        );
    }

    #[test]
    fn test_reserved_type_name_value() {
        let source = r#"
            N::Value { data: String }

            QUERY test() =>
                v <- N<Value>
                RETURN v
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E110));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("reserved type name") && d.message.contains("Value"))
        );
    }

    #[test]
    fn test_reserved_type_name_graph_error() {
        let source = r#"
            N::GraphError { message: String }

            QUERY test() =>
                e <- N<GraphError>
                RETURN e
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E110));
    }

    #[test]
    fn test_non_reserved_type_names_valid() {
        let source = r#"
            N::Person { name: String }
            N::Company { title: String }
            E::WorksAt { From: Person, To: Company }
            V::Document { content: String, embedding: [F32] }

            QUERY test() =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should not have any E110 errors for valid names
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E110));
    }
}
