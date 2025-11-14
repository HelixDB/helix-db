use std::collections::HashMap;

use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{
        DefaultValue, EdgeSchema, Field, FieldPrefix, FieldType, Migration, MigrationItem,
        MigrationItemMapping, MigrationPropertyMapping, NodeSchema, Source, ValueCast,
        VectorSchema,
    },
    utils::{PairTools, PairsTools},
};
use pest::iterators::{Pair, Pairs};

impl HelixParser {
    pub(super) fn parse_node_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<NodeSchema, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let name = pairs.try_next()?.as_str().to_string();
        let fields = self.parse_node_body(pairs.try_next()?, filepath.clone())?;
        Ok(NodeSchema {
            name: (pair.loc(), name),
            fields,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    pub(super) fn parse_vector_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<VectorSchema, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let name = pairs.try_next()?.as_str().to_string();
        let fields = self.parse_node_body(pairs.try_next()?, filepath.clone())?;
        Ok(VectorSchema {
            name,
            fields,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    pub(super) fn parse_node_body(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<Vec<Field>, ParserError> {
        let field_defs = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .ok_or_else(|| ParserError::from("Expected field_defs in properties"))?;

        // Now parse each individual field_def
        field_defs
            .into_inner()
            .map(|p| self.parse_field_def(p, filepath.clone()))
            .collect::<Result<Vec<_>, _>>()
    }

    pub(super) fn parse_migration_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<Migration, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let from_version = pairs.try_next_inner()?.try_next()?;
        let to_version = pairs.try_next_inner()?.try_next()?;

        // migration body -> [migration-item-mapping, migration-item-mapping, ...]
        let body = pairs
            .try_next_inner()?
            .map(|p| self.parse_migration_item_mapping(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Migration {
            from_version: (
                from_version.loc(),
                from_version.as_str().parse::<usize>().map_err(|e| {
                    ParserError::from(format!(
                        "Invalid schema version number '{}': {e}",
                        from_version.as_str()
                    ))
                })?,
            ),
            to_version: (
                to_version.loc(),
                to_version.as_str().parse::<usize>().map_err(|e| {
                    ParserError::from(format!(
                        "Invalid schema version number '{}': {e}",
                        to_version.as_str()
                    ))
                })?,
            ),
            body,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    pub(super) fn parse_migration_item_mapping(
        &self,
        pair: Pair<Rule>,
    ) -> Result<MigrationItemMapping, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let from_item_type = match pairs.next() {
            Some(item_def) => match item_def.into_inner().next() {
                Some(item_decl) => match item_decl.as_rule() {
                    Rule::node_decl => (
                        item_decl.loc(),
                        MigrationItem::Node(item_decl.try_inner_next()?.as_str().to_string()),
                    ),
                    Rule::edge_decl => (
                        item_decl.loc(),
                        MigrationItem::Edge(item_decl.try_inner_next()?.as_str().to_string()),
                    ),
                    Rule::vec_decl => (
                        item_decl.loc(),
                        MigrationItem::Vector(item_decl.try_inner_next()?.as_str().to_string()),
                    ),
                    _ => {
                        return Err(ParserError::from(format!(
                            "Expected item declaration, got {:?}",
                            item_decl.as_rule()
                        )));
                    }
                },
                None => {
                    return Err(ParserError::from(format!(
                        "Expected item declaration, got {:?}",
                        pair.as_rule()
                    )));
                }
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Expected item declaration, got {:?}",
                    pair.as_rule()
                )));
            }
        };

        let to_item_type = match pairs.next() {
            Some(pair) => match pair.as_rule() {
                Rule::item_def => match pair.into_inner().next() {
                    Some(item_decl) => match item_decl.as_rule() {
                        Rule::node_decl => (
                            item_decl.loc(),
                            MigrationItem::Node(item_decl.try_inner_next()?.as_str().to_string()),
                        ),
                        Rule::edge_decl => (
                            item_decl.loc(),
                            MigrationItem::Edge(item_decl.try_inner_next()?.as_str().to_string()),
                        ),
                        Rule::vec_decl => (
                            item_decl.loc(),
                            MigrationItem::Vector(item_decl.try_inner_next()?.as_str().to_string()),
                        ),
                        _ => {
                            return Err(ParserError::from(format!(
                                "Expected item declaration, got {:?}",
                                item_decl.as_rule()
                            )));
                        }
                    },
                    None => {
                        return Err(ParserError::from(format!(
                            "Expected item, got {:?}",
                            pairs.peek()
                        )));
                    }
                },
                Rule::anon_decl => from_item_type.clone(),
                _ => {
                    return Err(ParserError::from(format!(
                        "Invalid item declaration, got {:?}",
                        pair.as_rule()
                    )));
                }
            },
            None => {
                return Err(ParserError::from(format!(
                    "Expected item_def, got {:?}",
                    pairs.peek()
                )));
            }
        };
        let remappings = match pairs.next() {
            Some(p) => match p.as_rule() {
                Rule::node_migration => p
                    .try_inner_next()?
                    .into_inner()
                    .map(|p| self.parse_field_migration(p))
                    .collect::<Result<Vec<_>, _>>()?,
                Rule::edge_migration => p
                    .try_inner_next()?
                    .into_inner()
                    .map(|p| self.parse_field_migration(p))
                    .collect::<Result<Vec<_>, _>>()?,
                _ => {
                    return Err(ParserError::from(
                        "Expected node_migration or edge_migration",
                    ));
                }
            },
            None => {
                return Err(ParserError::from(
                    "Expected node_migration or edge_migration",
                ));
            }
        };

        Ok(MigrationItemMapping {
            from_item: from_item_type,
            to_item: to_item_type,
            remappings,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_default_value(
        &self,
        pairs: &mut Pairs<Rule>,
        field_type: &FieldType,
    ) -> Result<Option<DefaultValue>, ParserError> {
        match pairs.peek() {
            Some(pair) => {
                if pair.as_rule() == Rule::default {
                    pairs.next();
                    let default_value = match pair.into_inner().next() {
                        Some(pair) => match pair.as_rule() {
                            Rule::string_literal => DefaultValue::String(pair.as_str().to_string()),
                            Rule::float => {
                                match field_type {
                                    FieldType::F32 => DefaultValue::F32(
                                        pair.as_str().parse::<f32>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid float value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::F64 => DefaultValue::F64(
                                        pair.as_str().parse::<f64>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid float value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    _ => unreachable!(), // throw error
                                }
                            }
                            Rule::integer => {
                                match field_type {
                                    FieldType::I8 => DefaultValue::I8(
                                        pair.as_str().parse::<i8>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::I16 => DefaultValue::I16(
                                        pair.as_str().parse::<i16>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::I32 => DefaultValue::I32(
                                        pair.as_str().parse::<i32>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::I64 => DefaultValue::I64(
                                        pair.as_str().parse::<i64>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::U8 => DefaultValue::U8(
                                        pair.as_str().parse::<u8>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::U16 => DefaultValue::U16(
                                        pair.as_str().parse::<u16>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::U32 => DefaultValue::U32(
                                        pair.as_str().parse::<u32>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::U64 => DefaultValue::U64(
                                        pair.as_str().parse::<u64>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    FieldType::U128 => DefaultValue::U128(
                                        pair.as_str().parse::<u128>().map_err(|e| {
                                            ParserError::from(format!(
                                                "Invalid integer value '{}': {e}",
                                                pair.as_str()
                                            ))
                                        })?,
                                    ),
                                    _ => unreachable!(), // throw error
                                }
                            }
                            Rule::now => DefaultValue::Now,
                            Rule::boolean => DefaultValue::Boolean(
                                pair.as_str().parse::<bool>().map_err(|e| {
                                    ParserError::from(format!(
                                        "Invalid boolean value '{}': {e}",
                                        pair.as_str()
                                    ))
                                })?,
                            ),
                            _ => unreachable!(), // throw error
                        },
                        None => DefaultValue::Empty,
                    };
                    Ok(Some(default_value))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub(super) fn parse_cast(&self, pair: Pair<Rule>) -> Result<Option<ValueCast>, ParserError> {
        match pair.as_rule() {
            Rule::cast => Ok(Some(ValueCast {
                loc: pair.loc(),
                cast_to: self.parse_field_type(pair.try_inner_next()?, None)?,
            })),
            _ => Ok(None),
        }
    }

    pub(super) fn parse_field_migration(
        &self,
        pair: Pair<Rule>,
    ) -> Result<MigrationPropertyMapping, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let property_name = pairs.try_next()?;
        let property_value = pairs.try_next()?;
        let cast = if let Some(cast_pair) = pairs.next() {
            self.parse_cast(cast_pair)?
        } else {
            None
        };

        Ok(MigrationPropertyMapping {
            property_name: (property_name.loc(), property_name.as_str().to_string()),
            property_value: self.parse_field_value(property_value)?,
            default: None,
            cast,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_field_type(
        &self,
        field: Pair<Rule>,
        _schema: Option<&Source>,
    ) -> Result<FieldType, ParserError> {
        match field.as_rule() {
            Rule::named_type => {
                let type_str = field.as_str();
                match type_str {
                    "String" => Ok(FieldType::String),
                    "Boolean" => Ok(FieldType::Boolean),
                    "F32" => Ok(FieldType::F32),
                    "F64" => Ok(FieldType::F64),
                    "I8" => Ok(FieldType::I8),
                    "I16" => Ok(FieldType::I16),
                    "I32" => Ok(FieldType::I32),
                    "I64" => Ok(FieldType::I64),
                    "U8" => Ok(FieldType::U8),
                    "U16" => Ok(FieldType::U16),
                    "U32" => Ok(FieldType::U32),
                    "U64" => Ok(FieldType::U64),
                    "U128" => Ok(FieldType::U128),
                    _ => unreachable!(),
                }
            }
            Rule::array => {
                Ok(FieldType::Array(Box::new(self.parse_field_type(
                    // unwraps the array type because grammar type is
                    // { array { param_type { array | object | named_type } } }
                    field.try_inner_next().try_inner_next()?,
                    _schema,
                )?)))
            }
            Rule::object => {
                let mut fields = HashMap::new();
                for field in field.try_inner_next()?.into_inner() {
                    let (field_name, field_type) = {
                        let mut field_pair = field.clone().into_inner();
                        (
                            field_pair.try_next()?.as_str().to_string(),
                            field_pair.try_next_inner().try_next()?,
                        )
                    };
                    let field_type = self.parse_field_type(field_type, Some(&self.source))?;
                    fields.insert(field_name, field_type);
                }
                Ok(FieldType::Object(fields))
            }
            Rule::identifier => Ok(FieldType::Identifier(field.as_str().to_string())),
            Rule::ID_TYPE => Ok(FieldType::Uuid),
            Rule::date_type => Ok(FieldType::Date),
            _ => {
                unreachable!()
            }
        }
    }

    pub(super) fn parse_field_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<Field, ParserError> {
        let mut pairs = pair.clone().into_inner();
        // structure is index? ~ identifier ~ ":" ~ param_type
        let prefix: FieldPrefix = match pairs.clone().try_next()?.as_rule() {
            Rule::index => {
                pairs.try_next()?;
                FieldPrefix::Index
            }
            // Rule::optional => {
            //     pairs.next().unwrap();
            //     FieldPrefix::Optional
            // }
            _ => FieldPrefix::Empty,
        };
        let name = pairs.try_next()?.as_str().to_string();

        let field_type =
            self.parse_field_type(pairs.try_next_inner().try_next()?, Some(&self.source))?;

        let defaults = self.parse_default_value(&mut pairs, &field_type)?;

        Ok(Field {
            prefix,
            defaults,
            name,
            field_type,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    pub(super) fn parse_edge_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<EdgeSchema, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let name = pairs.try_next()?.as_str().to_string();
        let body = pairs.try_next()?;
        let mut body_pairs = body.into_inner();

        let from = {
            let pair = body_pairs.try_next()?;
            (pair.loc(), pair.as_str().to_string())
        };
        let to = {
            let pair = body_pairs.try_next()?;
            (pair.loc(), pair.as_str().to_string())
        };
        let properties = match body_pairs.next() {
            Some(pair) => Some(self.parse_properties(pair, filepath.clone())?),
            None => None,
        };

        Ok(EdgeSchema {
            name: (pair.loc(), name),
            from,
            to,
            properties,
            loc: pair.loc_with_filepath(filepath),
        })
    }
    pub(super) fn parse_properties(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<Vec<Field>, ParserError> {
        pair.into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .map_or(Ok(Vec::new()), |field_defs| {
                field_defs
                    .into_inner()
                    .map(|p| self.parse_field_def(p, filepath.clone()))
                    .collect::<Result<Vec<_>, _>>()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Node Definition Tests
    // ============================================================================

    #[test]
    fn test_parse_node_definition_basic() {
        let source = r#"
            N::Person {
                name: String,
                age: U32
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.schema.len(), 1);
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas.len(), 1);
        assert_eq!(schema.node_schemas[0].name.1, "Person");
        assert_eq!(schema.node_schemas[0].fields.len(), 2);
        assert_eq!(schema.node_schemas[0].fields[0].name, "name");
        assert_eq!(schema.node_schemas[0].fields[1].name, "age");
    }

    #[test]
    fn test_parse_node_definition_with_index() {
        let source = r#"
            N::Person {
                INDEX email: String,
                name: String
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert!(matches!(
            schema.node_schemas[0].fields[0].prefix,
            FieldPrefix::Index
        ));
        assert!(matches!(
            schema.node_schemas[0].fields[1].prefix,
            FieldPrefix::Empty
        ));
    }

    #[test]
    fn test_parse_node_definition_all_types() {
        let source = r#"
            N::AllTypes {
                str_field: String,
                bool_field: Boolean,
                f32_field: F32,
                f64_field: F64,
                i8_field: I8,
                i16_field: I16,
                i32_field: I32,
                i64_field: I64,
                u8_field: U8,
                u16_field: U16,
                u32_field: U32,
                u64_field: U64,
                u128_field: U128
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas[0].fields.len(), 13);
    }

    #[test]
    fn test_parse_node_definition_with_default_values() {
        let source = r#"
            N::Person {
                name: String DEFAULT "Unknown",
                age: U32 DEFAULT 0,
                active: Boolean DEFAULT true,
                score: F64 DEFAULT 0.0
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas[0].fields.len(), 4);
        assert!(schema.node_schemas[0].fields[0].defaults.is_some());
        assert!(schema.node_schemas[0].fields[1].defaults.is_some());
        assert!(schema.node_schemas[0].fields[2].defaults.is_some());
        assert!(schema.node_schemas[0].fields[3].defaults.is_some());
    }

    #[test]
    fn test_parse_node_definition_array_type() {
        let source = r#"
            N::Person {
                tags: [String],
                scores: [I32]
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas[0].fields.len(), 2);
        assert!(matches!(
            schema.node_schemas[0].fields[0].field_type,
            FieldType::Array(_)
        ));
        assert!(matches!(
            schema.node_schemas[0].fields[1].field_type,
            FieldType::Array(_)
        ));
    }

    #[test]
    fn test_parse_node_definition_object_type() {
        let source = r#"
            N::Person {
                address: { street: String, city: String, zip: U32 }
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas[0].fields.len(), 1);
        assert!(matches!(
            schema.node_schemas[0].fields[0].field_type,
            FieldType::Object(_)
        ));
    }

    #[test]
    fn test_parse_node_definition_empty_body() {
        let source = r#"
            N::EmptyNode {}
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas[0].fields.len(), 0);
    }

    #[test]
    fn test_parse_node_definition_invalid_syntax_missing_colon() {
        let source = r#"
            N::Person {
                name String
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_node_definition_invalid_syntax_missing_brace() {
        let source = r#"
            N::Person {
                name: String
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_node_definition_invalid_type() {
        let source = r#"
            N::Person {
                name: InvalidType
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        // Note: This may succeed parsing but fail during analysis
        // The parser allows custom types that get validated later
        assert!(result.is_ok());
    }

    // ============================================================================
    // Edge Definition Tests
    // ============================================================================

    #[test]
    fn test_parse_edge_definition_basic() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }

            E::WorksAt {
                From: Person,
                To: Company
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.edge_schemas.len(), 1);
        assert_eq!(schema.edge_schemas[0].name.1, "WorksAt");
        assert_eq!(schema.edge_schemas[0].from.1, "Person");
        assert_eq!(schema.edge_schemas[0].to.1, "Company");
    }

    #[test]
    fn test_parse_edge_definition_with_properties() {
        let source = r#"
            N::Person { name: String }

            E::Knows {
                From: Person,
                To: Person,
                Properties: {
                    since: String,
                    strength: F64
                }
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.edge_schemas.len(), 1);
        assert!(schema.edge_schemas[0].properties.is_some());
        let props = schema.edge_schemas[0].properties.as_ref().unwrap();
        assert_eq!(props.len(), 2);
    }

    #[test]
    fn test_parse_edge_definition_self_referential() {
        let source = r#"
            N::Person { name: String }

            E::Knows {
                From: Person,
                To: Person
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.edge_schemas[0].from.1, "Person");
        assert_eq!(schema.edge_schemas[0].to.1, "Person");
    }

    #[test]
    fn test_parse_edge_definition_invalid_missing_from_to() {
        let source = r#"
            N::Person { name: String }

            E::Knows {
                Person, Person
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());
    }

    // ============================================================================
    // Vector Definition Tests
    // ============================================================================

    #[test]
    fn test_parse_vector_definition() {
        let source = r#"
            V::Document {
                content: String,
                embedding: [F32]
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.vector_schemas.len(), 1);
        assert_eq!(schema.vector_schemas[0].name, "Document");
        assert_eq!(schema.vector_schemas[0].fields.len(), 2);
    }

    // ============================================================================
    // Multiple Schemas Test
    // ============================================================================

    #[test]
    fn test_parse_multiple_nodes() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            N::Location { city: String }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.node_schemas.len(), 3);
    }

    #[test]
    fn test_parse_multiple_edges() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }

            E::WorksAt { From: Person, To: Company }
            E::Manages { From: Person, To: Person }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert_eq!(schema.edge_schemas.len(), 2);
    }

    // ============================================================================
    // Schema Versioning Tests
    // ============================================================================

    #[test]
    fn test_parse_schema_with_version() {
        let source = r#"
            schema::2 {
                N::Person { name: String }
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(parsed.schema.contains_key(&2));
        assert_eq!(parsed.schema.get(&2).unwrap().node_schemas.len(), 1);
    }

    #[test]
    fn test_parse_schema_default_version() {
        let source = r#"
            N::Person { name: String }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(parsed.schema.contains_key(&1));
    }

    // ============================================================================
    // Edge Cases and Whitespace Tests
    // ============================================================================

    #[test]
    fn test_parse_with_extra_whitespace() {
        let source = r#"
            N::Person    {
                name   :   String   ,
                age    :   U32
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_with_trailing_comma() {
        let source = r#"
            N::Person {
                name: String,
                age: U32,
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_compact_format() {
        let source = r#"N::Person{name:String,age:U32}"#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Complex Nested Structure Tests
    // ============================================================================

    #[test]
    fn test_parse_nested_arrays() {
        let source = r#"
            N::Data {
                matrix: [[I32]]
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert!(matches!(
            schema.node_schemas[0].fields[0].field_type,
            FieldType::Array(_)
        ));
    }

    #[test]
    fn test_parse_nested_objects() {
        let source = r#"
            N::Person {
                address: {
                    home: { street: String, city: String },
                    work: { street: String, city: String }
                }
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_array_of_objects() {
        let source = r#"
            N::Company {
                employees: [{ name: String, role: String }]
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Default Value Edge Cases
    // ============================================================================

    #[test]
    fn test_parse_default_now() {
        let source = r#"
            N::Event {
                created_at: String DEFAULT NOW
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        let schema = parsed.schema.get(&1).unwrap();
        assert!(matches!(
            schema.node_schemas[0].fields[0].defaults,
            Some(DefaultValue::Now)
        ));
    }

    #[test]
    fn test_parse_default_various_numeric_types() {
        let source = r#"
            N::Config {
                i8_val: I8 DEFAULT 127,
                i16_val: I16 DEFAULT 32767,
                i32_val: I32 DEFAULT 2147483647,
                i64_val: I64 DEFAULT 9223372036854775807,
                u8_val: U8 DEFAULT 255,
                u16_val: U16 DEFAULT 65535,
                u32_val: U32 DEFAULT 4294967295,
                u64_val: U64 DEFAULT 18446744073709551615,
                f32_val: F32 DEFAULT 3.14,
                f64_val: F64 DEFAULT 2.718281828
            }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Error Message Quality Tests
    // ============================================================================

    #[test]
    fn test_parse_error_message_contains_context() {
        let source = r#"
            N::Person { invalid }
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_string = err.to_string();
        // Error should provide helpful context
        assert!(!err_string.is_empty());
    }

    #[test]
    fn test_parse_empty_input() {
        let source = "";

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.schema.len(), 0);
    }
}
