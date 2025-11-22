use crate::{
    helixc::parser::{
        HelixParser, ParserError, Rule,
        location::HasLoc,
        types::{FieldAddition, FieldValue, FieldValueType, ValueType},
        utils::{PairTools, PairsTools},
    },
    protocol::value::Value,
};
use std::collections::HashMap;

use pest::iterators::Pair;

impl HelixParser {
    pub(super) fn parse_property_assignments(
        &self,
        pair: Pair<Rule>,
    ) -> Result<HashMap<String, ValueType>, ParserError> {
        pair.into_inner()
            .map(|p| {
                let mut pairs = p.into_inner();
                let prop_key = pairs.try_next()?.as_str().to_string();

                let value_pair = pairs.try_next().try_inner_next()?;

                let prop_val = match value_pair.as_rule() {
                    Rule::string_literal => Ok(ValueType::new(
                        Value::from(value_pair.as_str().to_string()),
                        value_pair.loc(),
                    )),
                    Rule::integer => value_pair
                        .as_str()
                        .parse()
                        .map(|i| ValueType::new(Value::I32(i), value_pair.loc()))
                        .map_err(|_| ParserError::from("Invalid integer value")),
                    Rule::float => value_pair
                        .as_str()
                        .parse()
                        .map(|f| ValueType::new(Value::F32(f), value_pair.loc()))
                        .map_err(|_| ParserError::from("Invalid float value")),
                    Rule::boolean => Ok(ValueType::new(
                        Value::Boolean(value_pair.as_str() == "true"),
                        value_pair.loc(),
                    )),
                    Rule::identifier => Ok(ValueType::Identifier {
                        value: value_pair.as_str().to_string(),
                        loc: value_pair.loc(),
                    }),
                    _ => Err(ParserError::from("Invalid property value type")),
                }?;

                Ok((prop_key, prop_val))
            })
            .collect()
    }

    pub(super) fn parse_object_fields(
        &self,
        pair: Pair<Rule>,
    ) -> Result<Vec<FieldAddition>, ParserError> {
        pair.into_inner()
            .map(|p| self.parse_new_field_pair(p))
            .collect()
    }

    pub(super) fn parse_field_value(
        &self,
        value_pair: Pair<Rule>,
    ) -> Result<FieldValue, ParserError> {
        Ok(match value_pair.as_rule() {
            Rule::evaluates_to_anything => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Expression(self.parse_expression(value_pair)?),
            },
            Rule::anonymous_traversal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Traversal(Box::new(self.parse_traversal(value_pair)?)),
            },
            Rule::id_traversal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Traversal(Box::new(self.parse_traversal(value_pair)?)),
            },
            Rule::object_step => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_object_fields(value_pair)?),
            },
            Rule::string_literal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::String(
                    self.parse_string_literal(value_pair)?,
                )),
            },
            Rule::integer => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::I32(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid integer literal"))?,
                )),
            },
            Rule::float => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::F32(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid float literal"))?,
                )),
            },
            Rule::boolean => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::Boolean(value_pair.as_str() == "true")),
            },
            Rule::none => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Empty,
            },
            Rule::mapping_field => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_object_fields(value_pair)?),
            },
            Rule::identifier => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Identifier(value_pair.as_str().to_string()),
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field pair type: {:?} \n {:?}",
                    value_pair.as_rule(),
                    value_pair
                )));
            }
        })
    }

    pub(super) fn parse_new_field_pair(
        &self,
        pair: Pair<Rule>,
    ) -> Result<FieldAddition, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let key = pairs.try_next()?.as_str().to_string();
        let value_pair = pairs.try_next()?;
        let value = self.parse_field_value(value_pair)?;

        Ok(FieldAddition {
            loc: pair.loc(),
            key,
            value,
        })
    }

    pub(super) fn parse_new_field_value(
        &self,
        pair: Pair<Rule>,
    ) -> Result<FieldValue, ParserError> {
        let value_pair = pair.try_inner_next()?;
        let value: FieldValue = match value_pair.as_rule() {
            Rule::evaluates_to_anything => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Expression(self.parse_expression(value_pair)?),
            },
            Rule::anonymous_traversal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Traversal(Box::new(self.parse_traversal(value_pair)?)),
            },
            Rule::id_traversal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Traversal(Box::new(self.parse_traversal(value_pair)?)),
            },
            Rule::identifier => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Identifier(value_pair.as_str().to_string()),
            },
            Rule::object_step => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_object_fields(value_pair)?),
            },
            Rule::string_literal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::String(
                    self.parse_string_literal(value_pair)?,
                )),
            },
            Rule::integer => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::I32(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid integer literal"))?,
                )),
            },
            Rule::float => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::F32(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid float literal"))?,
                )),
            },
            Rule::boolean => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::Boolean(value_pair.as_str() == "true")),
            },
            Rule::none => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Empty,
            },
            Rule::mapping_field => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_object_fields(value_pair)?),
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field value type: {:?} \n {:?}",
                    value_pair.as_rule(),
                    value_pair,
                )));
            }
        };

        Ok(value)
    }
}
