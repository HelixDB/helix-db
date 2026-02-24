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
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    io::Write,
};

pub mod creation_step_parse_methods;
pub mod errors;
pub mod expression_parse_methods;
pub mod graph_step_parse_methods;
pub mod location;
pub mod migration_parse_methods;
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
            let mut remaining_queries = Vec::new();
            let mut remaining_migrations = Vec::new();
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
                                    let loc = pair.loc();
                                    let node_schema =
                                        parser.parse_node_def(pair, file.name.as_str())?;
                                    let schema =
                                        parser.source.schema.entry(schema_version).or_insert_with(
                                            || Schema {
                                                loc: loc.clone(),
                                                version: (loc, schema_version),
                                                node_schemas: vec![],
                                                edge_schemas: vec![],
                                                vector_schemas: vec![],
                                            },
                                        );
                                    schema.node_schemas.push(node_schema);
                                }
                                Rule::edge_def => {
                                    let loc = pair.loc();
                                    let edge_schema =
                                        parser.parse_edge_def(pair, file.name.as_str())?;
                                    let schema =
                                        parser.source.schema.entry(schema_version).or_insert_with(
                                            || Schema {
                                                loc: loc.clone(),
                                                version: (loc, schema_version),
                                                node_schemas: vec![],
                                                edge_schemas: vec![],
                                                vector_schemas: vec![],
                                            },
                                        );
                                    schema.edge_schemas.push(edge_schema);
                                }
                                Rule::vector_def => {
                                    let loc = pair.loc();
                                    let vector_schema =
                                        parser.parse_vector_def(pair, file.name.as_str())?;
                                    let schema =
                                        parser.source.schema.entry(schema_version).or_insert_with(
                                            || Schema {
                                                loc: loc.clone(),
                                                version: (loc, schema_version),
                                                node_schemas: vec![],
                                                edge_schemas: vec![],
                                                vector_schemas: vec![],
                                            },
                                        );
                                    schema.vector_schemas.push(vector_schema);
                                }
                                _ => return Err(ParserError::from("Unexpected rule encountered")),
                            }
                        }
                    }
                    Rule::migration_def => {
                        remaining_migrations.push(pair);
                    }
                    Rule::query_def => {
                        remaining_queries.push(pair);
                    }
                    Rule::EOI => (),
                    _ => return Err(ParserError::from("Unexpected rule encountered")),
                }
            }

            for pair in remaining_migrations {
                let migration = parser.parse_migration_def(pair, file.name.as_str())?;
                parser.source.migrations.push(migration);
            }

            for pair in remaining_queries {
                parser
                    .source
                    .queries
                    .push(parser.parse_query_def(pair, file.name.as_str())?);
            }

            // Merge schemas by version - combine node/edge/vector schemas instead of replacing
            for (version, mut new_schema) in parser.source.schema {
                match source.schema.entry(version) {
                    Entry::Occupied(mut existing) => {
                        let existing = existing.get_mut();
                        existing.node_schemas.append(&mut new_schema.node_schemas);
                        existing.edge_schemas.append(&mut new_schema.edge_schemas);
                        existing
                            .vector_schemas
                            .append(&mut new_schema.vector_schemas);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(new_schema);
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use super::{HelixParser, write_to_temp_file};
    use crate::helixc::parser::types::MigrationItem;

    #[test]
    fn preserves_query_order_across_files() {
        let file_one = r#"
            N::User { name: String }

            QUERY first() =>
                u <- N<User>
                RETURN u
        "#;

        let file_two = r#"
            QUERY second() =>
                u <- N<User>
                RETURN u
        "#;

        let content = write_to_temp_file(vec![file_one, file_two]);
        let parsed = HelixParser::parse_source(&content).expect("query parse should succeed");

        let query_names = parsed
            .queries
            .iter()
            .map(|query| query.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(query_names, vec!["first", "second"]);
    }

    #[test]
    fn preserves_migration_order_across_files() {
        let file_one = r#"
            schema::1 {
                N::User { name: String }
            }

            schema::2 {
                N::User { name: String }
            }

            MIGRATION schema::1 => schema::2 {
                N::User => _:: { name: name }
            }
        "#;

        let file_two = r#"
            schema::3 {
                N::User { name: String }
            }

            MIGRATION schema::2 => schema::3 {
                N::User => _:: { name: name }
            }
        "#;

        let content = write_to_temp_file(vec![file_one, file_two]);
        let parsed = HelixParser::parse_source(&content).expect("migration parse should succeed");

        let versions = parsed
            .migrations
            .iter()
            .map(|migration| (migration.from_version.1, migration.to_version.1))
            .collect::<Vec<_>>();

        assert_eq!(versions, vec![(1, 2), (2, 3)]);
    }

    #[test]
    fn benchmark_fixtures_parse_cleanly() {
        let fixtures = vec![
            include_str!("../fixtures/benchmarks/schema_heavy.hql"),
            include_str!("../fixtures/benchmarks/traversal_heavy.hql"),
            include_str!("../fixtures/benchmarks/projection_heavy.hql"),
            include_str!("../fixtures/benchmarks/vector_search_heavy.hql"),
            include_str!("../fixtures/benchmarks/migration_heavy.hql"),
        ];

        let content = write_to_temp_file(fixtures);
        let parsed = HelixParser::parse_source(&content);
        assert!(parsed.is_ok(), "fixture parse failed: {parsed:?}");
    }

    #[test]
    fn migration_mapping_edge_cases_parse_cleanly() {
        let source = r#"
            schema::1 {
                N::User { name: String, age: I32 }
                E::Follows { From: User, To: User, Properties: { weight: F64 } }
            }

            schema::2 {
                N::User { full_name: String, age: I64 }
                E::Follows { From: User, To: User, Properties: { weight: F64 } }
            }

            MIGRATION schema::1 => schema::2 {
                N::User => _:: { full_name: name, age: age AS I64 }
                E::Follows => _:: { Properties: { weight: weight } }
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).expect("migration parse should succeed");
        let migration = &parsed.migrations[0];

        assert_eq!(migration.body.len(), 2);

        assert!(matches!(
            migration.body[0].from_item.1,
            MigrationItem::Node(ref item) if item == "User"
        ));
        assert!(matches!(
            migration.body[0].to_item.1,
            MigrationItem::Node(ref item) if item == "User"
        ));
        assert!(
            migration.body[0]
                .remappings
                .iter()
                .any(|mapping| mapping.property_name.1 == "age" && mapping.cast.is_some())
        );

        assert!(matches!(
            migration.body[1].from_item.1,
            MigrationItem::Edge(ref item) if item == "Follows"
        ));
        assert!(matches!(
            migration.body[1].to_item.1,
            MigrationItem::Edge(ref item) if item == "Follows"
        ));
        assert_eq!(migration.body[1].remappings.len(), 1);
    }
}
