//! Lossless JSON Cypher requests, shared by every transport and embedder.
//!
//! Decoding and compilation never read storage, so a gateway can learn a
//! statement's read/write effect before it routes the request to a database.
use helix_ast::query;
use helix_planner::relational as r;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One Cypher statement and its parameters, as sent to `POST /v2/cypher`.
///
/// Parameters are plain JSON values or lossless envelopes:
/// `{"$type":"integer","value":"<i64>"}`, `{"$type":"float","value":"NaN"}`
/// (also `"Infinity"` and `"-Infinity"`) and `{"$type":"map","value":{...}}`,
/// which escapes a map whose own keys include `$type`.
///
/// ```
/// use helix_ast::query::QueryValue;
/// let request: helix_cypher::request::Request = serde_json::from_str(
///     r#"{"query":"RETURN $id AS id","parameters":{"id":{"$type":"integer","value":"9223372036854775807"}}}"#,
/// )?;
/// assert_eq!(request.parameters["id"], QueryValue::I64(i64::MAX));
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
}

impl Request {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            parameters: BTreeMap::new(),
            query_name: None,
        }
    }

    /// Resolve statement effects before applying transport routing options.
    /// Use [`Self::compile`] when execution follows routing, to retain this work.
    pub fn request_type(&self) -> r::Result<query::QueryRequestType> {
        Ok(match crate::compile(&self.query)?.effect() {
            r::Effect::Read => query::QueryRequestType::Read,
            r::Effect::Write => query::QueryRequestType::Write,
        })
    }

    /// Compile once before applying read/write routing policy, retaining the
    /// validated statement for execution. This performs no storage access.
    ///
    /// ```
    /// use helix_cypher::request::Request;
    /// use helix_ast::query::QueryRequestType;
    /// let read = Request::new("MATCH (n) RETURN count(*)").compile().unwrap();
    /// let write = Request::new("CREATE (:Person {name:$name})").compile().unwrap();
    /// assert_eq!(read.request_type(), QueryRequestType::Read);
    /// assert_eq!(write.request_type(), QueryRequestType::Write);
    /// ```
    pub fn compile(self) -> r::Result<CompiledRequest> {
        let query = crate::compile(&self.query)?;
        Ok(CompiledRequest {
            query,
            parameters: self.parameters,
        })
    }
}

/// A parsed, resolved and validated statement with its original parameters.
///
/// Construct this only with [`Request::compile`]. It owns no database, catalog
/// snapshot or transaction. The executor that consumes it obtains the current
/// request's scoped catalog and resource limits, and checks and admits the
/// parameters at that boundary.
#[derive(Debug)]
pub struct CompiledRequest {
    query: r::Query,
    parameters: BTreeMap<String, query::QueryValue>,
}

impl CompiledRequest {
    /// The validated statement's effect, without parsing or allocating.
    pub fn request_type(&self) -> query::QueryRequestType {
        match self.query.effect() {
            r::Effect::Read => query::QueryRequestType::Read,
            r::Effect::Write => query::QueryRequestType::Write,
        }
    }

    /// Hand the statement and its unvalidated parameters to an executor.
    pub fn into_parts(self) -> (r::Query, BTreeMap<String, query::QueryValue>) {
        (self.query, self.parameters)
    }
}

fn deserialize_parameters<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, query::QueryValue>, D::Error> {
    let values = BTreeMap::<String, serde_json::Value>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|(name, value)| {
            decode_parameter(value)
                .map(|value| (name, value))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}

fn decode_parameter(value: serde_json::Value) -> std::result::Result<query::QueryValue, String> {
    use query::QueryValue as Q;
    match value {
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(decode_parameter)
            .collect::<std::result::Result<_, _>>()
            .map(Q::Array),
        serde_json::Value::Object(mut map) => {
            let tag = map
                .get("$type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            match tag.as_deref(){
                Some("integer") if map.len()==2=>map.get("value").and_then(serde_json::Value::as_str).ok_or_else(||"integer envelope requires a decimal string".to_owned())?.parse().map(Q::I64).map_err(|_|"integer envelope exceeds signed 64-bit range".into()),
                Some("float") if map.len()==2=>match map.get("value").and_then(serde_json::Value::as_str){Some("NaN")=>Ok(Q::F64(f64::NAN)),Some("Infinity")=>Ok(Q::F64(f64::INFINITY)),Some("-Infinity")=>Ok(Q::F64(f64::NEG_INFINITY)),_=>Err("invalid float envelope".into())},
                Some("map") if map.len()==2=>{
                    let Some(serde_json::Value::Object(values))=map.remove("value") else{return Err("map envelope requires an object".into());};
                    values.into_iter().map(|(k,v)|decode_parameter(v).map(|v|(k,v))).collect::<std::result::Result<_,_>>().map(Q::Object)
                }
                Some(_)=>Err("unknown lossless parameter envelope; escape literal $type maps with a map envelope".into()),
                None=>map.into_iter().map(|(k,v)|decode_parameter(v).map(|v|(k,v))).collect::<std::result::Result<_,_>>().map(Q::Object),
            }
        }
        serde_json::Value::Number(number) if number.is_u64() && number.as_i64().is_none() => {
            Err("integer parameter exceeds signed 64-bit range".into())
        }
        value @ serde_json::Value::Null
        | value @ serde_json::Value::Bool(_)
        | value @ serde_json::Value::Number(_)
        | value @ serde_json::Value::String(_) => {
            serde_json::from_value(value).map_err(|e| e.to_string())
        }
    }
}
