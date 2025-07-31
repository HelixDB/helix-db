//! Semantic analyzer for Helix‑QL.
use crate::helixc::{
    analyzer::{
        diagnostic::Diagnostic,
        methods::{
            query_validation::validate_query,
            schema_methods::{build_field_lookups, check_schema},
        },
        types::Type,
    },
    generator::generator_types::Source as GeneratedSource,
    parser::helix_parser::{EdgeSchema, ExpressionType, Field, Query, Source},
};
use serde::Serialize;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

pub fn analyze(src: &Source) -> (Vec<Diagnostic>, GeneratedSource) {
    let mut ctx = Ctx::new(src);
    ctx.check_schema();
    ctx.check_queries();
    (ctx.diagnostics, ctx.output)
}

/// Internal working context shared by all passes.
pub(crate) struct Ctx<'a> {
    pub(super) src: &'a Source,

    /// Quick look‑ups
    pub(super) node_set: HashSet<&'a str>,
    pub(super) vector_set: HashSet<&'a str>,
    pub(super) edge_map: HashMap<&'a str, &'a EdgeSchema>,
    pub(super) node_fields: HashMap<&'a str, HashMap<&'a str, Cow<'a, Field>>>,
    pub(super) edge_fields: HashMap<&'a str, HashMap<&'a str, Cow<'a, Field>>>,
    pub(super) vector_fields: HashMap<&'a str, HashMap<&'a str, Cow<'a, Field>>>,
    pub(super) diagnostics: Vec<Diagnostic>,
    pub(super) output: GeneratedSource,
}

pub static INTROSPECTION_DATA: OnceLock<IntrospectionData> = OnceLock::new();
pub static SECONDARY_INDICES: OnceLock<Vec<String>> = OnceLock::new();

impl<'a> Ctx<'a> {
    pub(super) fn new(src: &'a Source) -> Self {
        // Build field look‑ups once
        let (node_fields, edge_fields, vector_fields) = build_field_lookups(src);

        let output = GeneratedSource {
            src: src.source.clone(),
            ..Default::default()
        };

        let out = Self {
            node_set: src.node_schemas.iter().map(|n| n.name.1.as_str()).collect(),
            vector_set: src.vector_schemas.iter().map(|v| v.name.as_str()).collect(),
            edge_map: src
                .edge_schemas
                .iter()
                .map(|e| (e.name.1.as_str(), e))
                .collect(),
            node_fields,
            edge_fields,
            vector_fields,
            src,
            diagnostics: Vec::new(),
            output,
        };

        INTROSPECTION_DATA
            .set(IntrospectionData::from_schema(&out))
            .ok();

        SECONDARY_INDICES
            .set(
                src.node_schemas
                    .iter()
                    .flat_map(|schema| {
                        schema
                            .fields
                            .iter()
                            .filter(|f| f.is_indexed())
                            .map(|f| f.name.clone())
                    })
                    .collect(),
            )
            .ok();
        out
    }

    #[allow(unused)]
    pub(super) fn get_item_fields(&self, item_type: &Type) -> Option<&HashMap<&str, Cow<'_, Field>>> {
        match item_type {
            Type::Node(Some(node_type)) | Type::Nodes(Some(node_type)) => {
                self.node_fields.get(node_type.as_str())
            }
            Type::Edge(Some(edge_type)) | Type::Edges(Some(edge_type)) => {
                self.edge_fields.get(edge_type.as_str())
            }
            Type::Vector(Some(vector_type)) | Type::Vectors(Some(vector_type)) => {
                self.vector_fields.get(vector_type.as_str())
            }
            _ => None,
        }
    }

    // ---------- Pass #1: schema --------------------------
    /// Validate that every edge references declared node types.
    pub(super) fn check_schema(&mut self) {
        check_schema(self);
    }

    // ---------- Pass #2: queries -------------------------
    pub(super) fn check_queries(&mut self) {
        for q in &self.src.queries {
            validate_query(self, q);
        }
    }
}

#[derive(Serialize)]
pub struct IntrospectionData {
    schema: SchemaData,
    queries: Vec<QueryData>,
}

impl IntrospectionData {
    fn from_schema(ctx: &Ctx) -> Self {
        let queries = ctx.src.queries.iter().map(QueryData::from_query).collect();
        Self {
            schema: SchemaData::from_ctx(ctx),
            queries,
        }
    }
}

#[derive(Serialize)]
pub struct SchemaData {
    nodes: Vec<NodeData>,
    vectors: Vec<NodeData>,
    edges: Vec<EdgeData>,
}

impl SchemaData {
    fn from_ctx(ctx: &Ctx) -> Self {
        let nodes = ctx.node_fields.iter().map(NodeData::from_entry).collect();
        let vectors = ctx.vector_fields.iter().map(NodeData::from_entry).collect();
        let edges = ctx.edge_map.iter().map(EdgeData::from_entry).collect();

        SchemaData {
            nodes,
            vectors,
            edges,
        }
    }
}

#[derive(Serialize)]
pub struct NodeData {
    name: String,
    properties: HashMap<String, String>,
}

impl NodeData {
    fn from_entry(val: (&&str, &HashMap<&str, Cow<Field>>)) -> Self {
        let properties = val
            .1
            .iter()
            .map(|(n, f)| (n.to_string(), f.field_type.to_string()))
            .collect();
        NodeData {
            name: val.0.to_string(),
            properties,
        }
    }
}

#[derive(Serialize)]
pub struct EdgeData {
    name: String,
    from: String,
    to: String,
    properties: HashMap<String, String>,
}

impl EdgeData {
    fn from_entry((name, es): (&&str, &&EdgeSchema)) -> Self {
        let properties = es
            .properties
            .iter()
            .flatten()
            .map(|f| (f.name.to_string(), f.field_type.to_string()))
            .collect();

        EdgeData {
            name: name.to_string(),
            from: es.from.1.clone(),
            to: es.to.1.clone(),
            properties,
        }
    }
}

#[derive(Serialize)]
pub struct QueryData {
    name: String,
    parameters: HashMap<String, String>,
    returns: Vec<String>,
}

impl QueryData {
    fn from_query(query: &Query) -> Self {
        let parameters = query
            .parameters
            .iter()
            .map(|p| (p.name.1.clone(), p.param_type.1.to_string()))
            .collect();

        let returns = query
            .return_values
            .iter()
            .flat_map(|e| {
                if let ExpressionType::Identifier(ident) = &e.expr {
                    Some(ident.clone())
                } else {
                    None
                }
            })
            .collect();

        QueryData {
            name: query.name.to_string(),
            parameters,
            returns,
        }
    }
}
