// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! This is the parser for HelixQL.
//! The parsing methods are broken up into separate files, grouped by general functionality.
//! File names should be self-explanatory as to what is included in the file.

use crate::helixc::parser::errors::ParserError;
use crate::helixc::parser::types::{Content, HxFile, Schema, Source};
use location::HasLoc;
use pest::Parser as PestParser;
use pest_derive::Parser;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    io::Write,
};

pub mod creation_step_parse_methods;
pub mod errors;
pub mod expression_parse_methods;
pub mod graph_step_parse_methods;
pub mod location;
pub mod object_parse_methods;
pub mod query_parse_methods;
pub mod return_value_parse_methods;
pub mod schema_parse_methods;
pub mod traversal_parse_methods;
pub mod types;
pub mod utils;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct HelixParser {
    pub(super) source: Source,
}

impl HelixParser {
    pub fn parse_source(input: &Content) -> Result<Source, ParserError> {
        let mut source = Source {
            source: String::new(),
            schema: HashMap::new(),
            migrations: Vec::new(),
            queries: Vec::new(),
        };

        input.files.iter().try_for_each(|file| {
            source.source.push_str(&file.content);
            source.source.push('\n');
            let pair = match HelixParser::parse(Rule::source, &file.content) {
                Ok(mut pairs) => pairs
                    .next()
                    .ok_or_else(|| ParserError::from("Empty input"))?,
                Err(e) => {
                    return Err(ParserError::from(e));
                }
            };
            let mut parser = HelixParser {
                source: Source::default(),
            };

            let pairs = pair.into_inner();
            let mut remaining_queries = HashSet::new();
            let mut remaining_migrations = HashSet::new();
            for pair in pairs {
                match pair.as_rule() {
                    Rule::schema_def => {
                        let mut schema_pairs = pair.into_inner();

                        let schema_version = match schema_pairs.peek() {
                            Some(pair) => {
                                if pair.as_rule() == Rule::schema_version {
                                    let version_pair = schema_pairs.next().ok_or_else(|| {
                                        ParserError::from("Expected schema version")
                                    })?;
                                    let version_str = version_pair
                                        .into_inner()
                                        .next()
                                        .ok_or_else(|| {
                                            ParserError::from("Schema version missing value")
                                        })?
                                        .as_str();
                                    version_str.parse::<usize>().map_err(|e| {
                                        ParserError::from(format!(
                                            "Invalid schema version number '{version_str}': {e}"
                                        ))
                                    })?
                                } else {
                                    1
                                }
                            }
                            None => 1,
                        };

                        for pair in schema_pairs {
                            match pair.as_rule() {
                                Rule::node_def => {
                                    let node_schema =
                                        parser.parse_node_def(pair.clone(), file.name.clone())?;
                                    parser
                                        .source
                                        .schema
                                        .entry(schema_version)
                                        .and_modify(|schema| {
                                            schema.node_schemas.push(node_schema.clone())
                                        })
                                        .or_insert(Schema {
                                            loc: pair.loc(),
                                            version: (pair.loc(), schema_version),
                                            node_schemas: vec![node_schema],
                                            edge_schemas: vec![],
                                            vector_schemas: vec![],
                                        });
                                }
                                Rule::edge_def => {
                                    let edge_schema =
                                        parser.parse_edge_def(pair.clone(), file.name.clone())?;
                                    parser
                                        .source
                                        .schema
                                        .entry(schema_version)
                                        .and_modify(|schema| {
                                            schema.edge_schemas.push(edge_schema.clone())
                                        })
                                        .or_insert(Schema {
                                            loc: pair.loc(),
                                            version: (pair.loc(), schema_version),
                                            node_schemas: vec![],
                                            edge_schemas: vec![edge_schema],
                                            vector_schemas: vec![],
                                        });
                                }
                                Rule::vector_def => {
                                    let vector_schema =
                                        parser.parse_vector_def(pair.clone(), file.name.clone())?;
                                    parser
                                        .source
                                        .schema
                                        .entry(schema_version)
                                        .and_modify(|schema| {
                                            schema.vector_schemas.push(vector_schema.clone())
                                        })
                                        .or_insert(Schema {
                                            loc: pair.loc(),
                                            version: (pair.loc(), schema_version),
                                            node_schemas: vec![],
                                            edge_schemas: vec![],
                                            vector_schemas: vec![vector_schema],
                                        });
                                }
                                _ => return Err(ParserError::from("Unexpected rule encountered")),
                            }
                        }
                    }
                    Rule::migration_def => {
                        remaining_migrations.insert(pair);
                    }
                    Rule::query_def => {
                        remaining_queries.insert(pair);
                    }
                    Rule::EOI => (),
                    _ => return Err(ParserError::from("Unexpected rule encountered")),
                }
            }

            for pair in remaining_migrations {
                let migration = parser.parse_migration_def(pair, file.name.clone())?;
                parser.source.migrations.push(migration);
            }

            for pair in remaining_queries {
                parser
                    .source
                    .queries
                    .push(parser.parse_query_def(pair, file.name.clone())?);
            }

            // parse all schemas first then parse queries using self
            source.schema.extend(parser.source.schema);
            source.queries.extend(parser.source.queries);
            source.migrations.extend(parser.source.migrations);
            Ok(())
        })?;

        Ok(source)
    }
}

pub fn write_to_temp_file(content: Vec<&str>) -> Content {
    let mut files = Vec::new();
    for c in content {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(c.as_bytes()).unwrap();
        let path = file.path().to_string_lossy().into_owned();
        files.push(HxFile {
            name: path,
            content: c.to_string(),
        });
    }
    Content {
        content: String::new(),
        files,
        source: Source::default(),
    }
}
