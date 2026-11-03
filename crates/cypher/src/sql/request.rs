//! JSON SQL requests, as sent to `POST /v2/sql`.
//!
//! A SQL request compiles to the same [`CompiledRequest`] as a Cypher request,
//! so transports route, execute and report it exactly like Cypher. Its text is
//! the translated Cypher, which telemetry records with literals redacted.
use super::Mappings;
use crate::request::{decode_parameter, CompiledRequest};
use helix_ast::query;
use helix_planner::relational as r;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One SQL statement, its parameters and optional name mappings.
///
/// `parameters` is either an object of named values, or a list whose first
/// value binds the first `?` (`$1`), the second the second (`$2`), and so on.
/// Values use the lossless encoding of [`crate::request::Request`].
///
/// ```
/// use helix_ast::query::{QueryRequestType, QueryValue};
/// let request: helix_cypher::sql::request::Request = serde_json::from_str(
///     r#"{"query":"SELECT p.name FROM people p WHERE p.age > ?","parameters":[30],
///         "mappings":{"tables":{"people":"Person"}}}"#,
/// )?;
/// assert_eq!(request.parameters["1"], QueryValue::I64(30));
/// let compiled = request.compile().unwrap();
/// assert_eq!(compiled.text(), "MATCH (p:Person) WHERE p.age > $1 RETURN p.name");
/// assert_eq!(compiled.request_type(), QueryRequestType::Read);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub query: String,
    #[serde(default, deserialize_with = "deserialize_parameters")]
    pub parameters: BTreeMap<String, query::QueryValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
    #[serde(default)]
    pub mappings: Mappings,
}

impl Request {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            parameters: BTreeMap::new(),
            query_name: None,
            mappings: Mappings::default(),
        }
    }

    /// Translate, resolve and validate the statement without storage access.
    pub fn compile(self) -> r::Result<CompiledRequest> {
        let statement = super::translate(&self.query, &self.mappings)?;
        let query = crate::resolve(&statement)?;
        Ok(CompiledRequest::new(
            query,
            self.parameters,
            statement.to_string(),
            self.query_name,
        ))
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Parameters {
    Named(BTreeMap<String, serde_json::Value>),
    Positional(Vec<serde_json::Value>),
}

fn deserialize_parameters<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, query::QueryValue>, D::Error> {
    let named: Vec<(String, serde_json::Value)> = match Parameters::deserialize(deserializer)? {
        Parameters::Named(values) => values.into_iter().collect(),
        Parameters::Positional(values) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| ((index + 1).to_string(), value))
            .collect(),
    };
    named
        .into_iter()
        .map(|(name, value)| {
            decode_parameter(value)
                .map(|value| (name, value))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}
