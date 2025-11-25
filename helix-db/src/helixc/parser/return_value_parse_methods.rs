use std::collections::HashMap;

use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{Expression, ExpressionType, ReturnType},
};
use pest::iterators::Pair;

impl HelixParser {
    pub(super) fn parse_return_statement(
        &self,
        pair: Pair<Rule>,
    ) -> Result<Vec<ReturnType>, ParserError> {
        let inner = pair.into_inner();
        let mut return_types = Vec::new();
        for pair in inner {
            match pair.as_rule() {
                Rule::array_creation => {
                    return_types.push(ReturnType::Array(self.parse_array_creation(pair)?));
                }
                Rule::object_creation => {
                    return_types.push(ReturnType::Object(self.parse_object_creation(pair)?));
                }
                Rule::evaluates_to_anything => {
                    return_types.push(ReturnType::Expression(self.parse_expression(pair)?));
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in return statement: {:?}",
                        pair.as_rule()
                    )));
                }
            }
        }
        Ok(return_types)
    }

    pub(super) fn parse_array_creation(
        &self,
        pair: Pair<Rule>,
    ) -> Result<Vec<ReturnType>, ParserError> {
        let pairs = pair.into_inner();
        let mut objects = Vec::new();
        for p in pairs {
            match p.as_rule() {
                Rule::identifier => {
                    objects.push(ReturnType::Expression(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Identifier(p.as_str().to_string()),
                    }));
                }
                _ => {
                    objects.push(ReturnType::Object(self.parse_object_creation(p)?));
                }
            }
        }
        Ok(objects)
    }

    pub(super) fn parse_object_creation(
        &self,
        pair: Pair<Rule>,
    ) -> Result<HashMap<String, ReturnType>, ParserError> {
        pair.into_inner()
            .map(|p| {
                let mut object_inner = p.into_inner();
                let key = object_inner
                    .next()
                    .ok_or_else(|| ParserError::from("Missing object inner"))?;
                let value = object_inner
                    .next()
                    .ok_or_else(|| ParserError::from("Missing object inner"))?;
                let value = self.parse_object_inner(value)?;
                Ok((key.as_str().to_string(), value))
            })
            .collect::<Result<HashMap<String, ReturnType>, _>>()
    }

    pub(super) fn parse_object_inner(
        &self,
        object_field: Pair<Rule>,
    ) -> Result<ReturnType, ParserError> {
        let object_field_inner = object_field
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Missing object inner"))?;

        match object_field_inner.as_rule() {
            Rule::evaluates_to_anything => Ok(ReturnType::Expression(
                self.parse_expression(object_field_inner)?,
            )),
            Rule::object_creation => Ok(ReturnType::Object(
                self.parse_object_creation(object_field_inner)?,
            )),
            Rule::array_creation => Ok(ReturnType::Array(
                self.parse_array_creation(object_field_inner)?,
            )),
            _ => Err(ParserError::from(format!(
                "Unexpected rule in parse_object_inner: {:?}",
                object_field_inner.as_rule()
            ))),
        }
    }
}
