use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::batch::{BatchQuery, ReadBatch, WriteBatch};
/// Declared query parameter shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryParamType {
    /// Boolean.
    Bool,
    /// 64-bit integer.
    I64,
    /// 64-bit float.
    F64,
    /// 32-bit float.
    F32,
    /// String.
    String,
    /// Datetime.
    DateTime,
    /// Bytes.
    Bytes,
    /// Any property value.
    Value,
    /// Object.
    Object,
    /// Array.
    Array(Box<QueryParamType>),
}

/// Query request type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryRequestType {
    /// Read-only query.
    Read,
    /// Write-capable query.
    Write,
}

/// JSON-compatible query parameter value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryValue {
    /// Null.
    Null,
    /// Boolean.
    Bool(bool),
    /// 64-bit signed integer.
    I64(i64),
    /// 64-bit float.
    F64(f64),
    /// 32-bit float.
    F32(f32),
    /// String.
    String(String),
    /// Array.
    Array(Vec<QueryValue>),
    /// Object.
    Object(BTreeMap<String, QueryValue>),
}

/// Query serialization errors.
#[derive(Debug)]
pub enum QueryError {
    /// JSON serialization error.
    Serialize(sonic_rs::Error),
    /// UTF-8 conversion error.
    Utf8(std::string::FromUtf8Error),
    /// Bytes cannot be represented safely in query parameters.
    UnsupportedBytesParameter(String),
    /// Datetime could not be rendered.
    InvalidDateTimeParameter {
        /// Parameter path.
        path: String,
        /// Raw millis.
        millis: i64,
    },
}

impl QueryError {
    /// Bytes parameter error.
    pub fn unsupported_bytes(path: impl Into<String>) -> Self {
        Self::UnsupportedBytesParameter(path.into())
    }

    /// Datetime parameter error.
    pub fn invalid_datetime(path: impl Into<String>, millis: i64) -> Self {
        Self::InvalidDateTimeParameter {
            path: path.into(),
            millis,
        }
    }
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize(err) => write!(f, "json serialization error: {err}"),
            Self::Utf8(err) => write!(f, "utf8 conversion error: {err}"),
            Self::UnsupportedBytesParameter(path) => write!(
                f,
                "parameter '{path}' uses bytes, which the query JSON route cannot represent"
            ),
            Self::InvalidDateTimeParameter { path, millis } => write!(
                f,
                "parameter '{path}' uses datetime millis '{millis}', which cannot be rendered as RFC3339"
            ),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<sonic_rs::Error> for QueryError {
    fn from(value: sonic_rs::Error) -> Self {
        Self::Serialize(value)
    }
}

impl From<std::string::FromUtf8Error> for QueryError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Utf8(value)
    }
}

/// Full query request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Request type.
    pub request_type: QueryRequestType,
    /// Optional query name.
    #[serde(default)]
    pub query_name: Option<String>,
    /// Query AST payload.
    pub query: BatchQuery,
    /// Runtime parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<BTreeMap<String, QueryValue>>,
    /// Optional parameter schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter_types: Option<BTreeMap<String, QueryParamType>>,
}

impl QueryRequest {
    fn new(request_type: QueryRequestType, query: BatchQuery) -> Self {
        Self {
            request_type,
            query_name: None,
            query,
            parameters: None,
            parameter_types: None,
        }
    }

    /// Create a read request.
    pub fn read(query: ReadBatch) -> Self {
        Self::new(QueryRequestType::Read, BatchQuery::Read(query))
    }

    /// Create a write request.
    pub fn write(query: WriteBatch) -> Self {
        Self::new(QueryRequestType::Write, BatchQuery::Write(query))
    }

    /// Insert a parameter value.
    pub fn insert_parameter_value(&mut self, name: impl Into<String>, value: QueryValue) {
        self.parameters
            .get_or_insert_with(BTreeMap::new)
            .insert(name.into(), value);
    }

    /// Insert a parameter type.
    pub fn insert_parameter_type(&mut self, name: impl Into<String>, ty: QueryParamType) {
        self.parameter_types
            .get_or_insert_with(BTreeMap::new)
            .insert(name.into(), ty);
    }

    /// Set query name.
    pub fn set_query_name(&mut self, name: impl Into<String>) {
        self.query_name = Some(name.into());
    }

    /// Clear query name.
    pub fn clear_query_name(&mut self) {
        self.query_name = None;
    }

    /// Add parameter value.
    pub fn with_parameter_value(mut self, name: impl Into<String>, value: QueryValue) -> Self {
        self.insert_parameter_value(name, value);
        self
    }

    /// Add parameter type.
    pub fn with_parameter_type(mut self, name: impl Into<String>, ty: QueryParamType) -> Self {
        self.insert_parameter_type(name, ty);
        self
    }

    /// Set query name.
    pub fn with_query_name(mut self, name: impl Into<String>) -> Self {
        self.set_query_name(name);
        self
    }

    /// Serialize to JSON bytes.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, QueryError> {
        Ok(sonic_rs::to_vec(self)?)
    }

    /// Serialize to JSON string.
    pub fn to_json_string(&self) -> Result<String, QueryError> {
        Ok(String::from_utf8(self.to_json_bytes()?)?)
    }
}
