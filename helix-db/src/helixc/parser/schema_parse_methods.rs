use std::collections::HashMap;

use crate::helixc::parser::{
    location::HasLoc, types::{
        DefaultValue, EdgeSchema, Field, FieldPrefix, FieldType, Migration, MigrationItem, MigrationItemMapping, MigrationPropertyMapping, NodeSchema, Precision, Source, ValueCast, VectorSchema
    }, utils::{PairTools, PairsTools}, HelixParser, ParserError, Rule
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
        let precision = match pairs.try_peek_matches(Rule::precision) {
            Some(pair) => match pair.try_inner_next()?.as_rule() {
                Rule::f64 => Precision::F64,
                Rule::f32 => Precision::F32,
                Rule::f16 => Precision::F16,
                _ => unreachable!(),
            },
            None => Precision::default(),
        };

        let fields = self.parse_node_body(pairs.try_next()?, filepath.clone())?;
        Ok(VectorSchema {
            name,
            fields,
            precision,
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
                    "F16" => Ok(FieldType::F16),
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
