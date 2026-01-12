// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! This is the static analyzer for HelixQL.
//! It type checks the queries for grammatical and semantic correctness.
//! The analyzer methods are broken up into separate files within /methods, grouped by general functionality.
//! File names should be self-explanatory as to what is included in the file.

use crate::{
    helix_engine::types::SecondaryIndex,
    helixc::{
        analyzer::{
            diagnostic::Diagnostic,
            methods::{
                migration_validation::validate_migration,
                query_validation::validate_query,
                schema_methods::{SchemaVersionMap, build_field_lookups, check_schema},
            },
            types::Type,
        },
        generator::Source as GeneratedSource,
        parser::{
            errors::ParserError,
            types::{EdgeSchema, ExpressionType, Field, Query, ReturnType, Source},
        },
    },
};
use itertools::Itertools;
use serde::Serialize;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

pub fn analyze(src: &Source) -> Result<(Vec<Diagnostic>, GeneratedSource), ParserError> {
    let mut ctx = Ctx::new(src)?;
    ctx.check_schema()?;
    ctx.check_schema_migrations();
    ctx.check_queries();
    Ok((ctx.diagnostics, ctx.output))
}

pub mod diagnostic;
pub mod error_codes;
pub mod errors;
pub mod fix;
pub mod methods;
pub mod pretty;
pub mod types;
pub mod utils;

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
    pub(super) all_schemas: SchemaVersionMap<'a>,
    pub(super) diagnostics: Vec<Diagnostic>,
    pub(super) output: GeneratedSource,
}

impl<'a> Ctx<'a> {
    pub(super) fn new(src: &'a Source) -> Result<Self, ParserError> {
        // Build field look‑ups once
        let all_schemas = build_field_lookups(src);
        let (node_fields, edge_fields, vector_fields) = all_schemas.get_latest();

        // Build secondary indices from indexed fields
        let secondary_indices: Vec<SecondaryIndex> = src
            .get_latest_schema()?
            .node_schemas
            .iter()
            .flat_map(|schema| schema.fields.iter().filter(|f| f.is_indexed()))
            .dedup()
            .map(SecondaryIndex::from_field)
            .collect();

        // Create the context first (without output populated)
        let mut ctx = Self {
            node_set: src
                .get_latest_schema()?
                .node_schemas
                .iter()
                .map(|n| n.name.1.as_str())
                .collect(),
            vector_set: src
                .get_latest_schema()?
                .vector_schemas
                .iter()
                .map(|v| v.name.as_str())
                .collect(),
            edge_map: src
                .get_latest_schema()?
                .edge_schemas
                .iter()
                .map(|e| (e.name.1.as_str(), e))
                .collect(),
            node_fields,
            edge_fields,
            vector_fields,
            all_schemas,
            src,
            diagnostics: Vec::new(),
            output: GeneratedSource {
                src: src.source.clone(),
                ..Default::default()
            },
        };

        // Now build introspection data from the context
        let introspection_data = IntrospectionData::from_schema(&ctx);

        // Update the output with introspection data and secondary indices
        ctx.output.introspection_data = Some(introspection_data);
        ctx.output.secondary_indices = secondary_indices;

        Ok(ctx)
    }

    #[allow(unused)]
    pub(super) fn get_item_fields(
        &self,
        item_type: &Type,
    ) -> Option<&HashMap<&str, Cow<'_, Field>>> {
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
    pub(super) fn check_schema(&mut self) -> Result<(), ParserError> {
        check_schema(self)
    }

    // ---------- Pass #1.5: schema migrations --------------------------
    pub(super) fn check_schema_migrations(&mut self) {
        for m in &self.src.migrations {
            validate_migration(self, m);
        }
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
                if let ReturnType::Expression(expr) = e {
                    if let ExpressionType::Identifier(ident) = &expr.expr {
                        Some(ident.clone())
                    } else {
                        None
                    }
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
