use std::collections::BTreeMap;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::batch::{BatchQuery, ReadBatch, WriteBatch};
use crate::value::PropertyValue;
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

impl From<&QueryValue> for PropertyValue {
    fn from(value: &QueryValue) -> Self {
        match value {
            QueryValue::Null => Self::Null,
            QueryValue::Bool(value) => Self::Bool(*value),
            QueryValue::I64(value) => Self::I64(*value),
            QueryValue::F64(value) => Self::F64(*value),
            QueryValue::F32(value) => Self::F32(*value),
            QueryValue::String(value) => Self::String(value.clone()),
            QueryValue::Array(values) => {
                Self::Array(values.iter().map(PropertyValue::from).collect())
            }
            QueryValue::Object(values) => Self::Object(
                values
                    .iter()
                    .map(|(name, value)| (name.clone(), PropertyValue::from(value)))
                    .collect(),
            ),
        }
    }
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
    /// Parameter names must be non-empty.
    InvalidParameterName,
    /// Parameter names must be unique within one request.
    DuplicateParameterName(String),
    /// Typed and untyped parameters cannot be mixed.
    MixedParameterModes,
    /// A value does not satisfy its declared schema.
    ParameterTypeMismatch {
        /// Parameter path.
        path: String,
        /// Expected schema.
        expected: QueryParamType,
        /// Observed JSON value family.
        actual: &'static str,
    },
    /// Typed parameter names must exactly match value names.
    ParameterNameMismatch {
        /// Declared names without values.
        missing_values: Vec<String>,
        /// Value names without declarations.
        extra_values: Vec<String>,
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
            Self::InvalidParameterName => write!(f, "parameter name must not be empty"),
            Self::DuplicateParameterName(name) => {
                write!(f, "parameter name '{name}' is duplicated")
            }
            Self::MixedParameterModes => {
                write!(f, "typed and untyped query parameters cannot be mixed")
            }
            Self::ParameterTypeMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "parameter '{path}' expected {expected:?}, but received {actual}"
            ),
            Self::ParameterNameMismatch {
                missing_values,
                extra_values,
            } => write!(
                f,
                "parameter schema names do not match values (missing values: {missing_values:?}, extra values: {extra_values:?})"
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

#[derive(Debug, Clone, PartialEq)]
enum QueryParameters {
    Untyped(BTreeMap<String, QueryValue>),
    Typed {
        values: BTreeMap<String, QueryValue>,
        types: BTreeMap<String, QueryParamType>,
    },
}

impl Default for QueryParameters {
    fn default() -> Self {
        Self::Untyped(BTreeMap::new())
    }
}

/// Full query request.
///
/// The request kind is derived from the closed [`BatchQuery`] variant. The
/// serializer retains the redundant legacy `request_type` wire field, while
/// deserialization rejects disagreement between the two tags.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    /// Optional query name.
    query_name: Option<String>,
    /// Query AST payload.
    query: BatchQuery,
    parameters: QueryParameters,
}

impl QueryRequest {
    fn new(query: BatchQuery) -> Self {
        Self {
            query_name: None,
            query,
            parameters: QueryParameters::default(),
        }
    }

    /// Create a read request.
    pub fn read(query: ReadBatch) -> Self {
        Self::new(BatchQuery::Read(query))
    }

    /// Create a write request.
    pub fn write(query: WriteBatch) -> Self {
        Self::new(BatchQuery::Write(query))
    }

    /// Derived request kind.
    pub const fn request_type(&self) -> QueryRequestType {
        match self.query {
            BatchQuery::Read(_) => QueryRequestType::Read,
            BatchQuery::Write(_) => QueryRequestType::Write,
        }
    }

    /// Closed query payload.
    pub const fn query(&self) -> &BatchQuery {
        &self.query
    }

    /// Optional query name.
    pub fn query_name(&self) -> Option<&str> {
        self.query_name.as_deref()
    }

    /// Runtime parameter values.
    pub fn parameters(&self) -> Option<&BTreeMap<String, QueryValue>> {
        let values = match &self.parameters {
            QueryParameters::Untyped(values) | QueryParameters::Typed { values, .. } => values,
        };
        (!values.is_empty()).then_some(values)
    }

    /// Declared parameter schema, when the request uses typed parameters.
    pub fn parameter_types(&self) -> Option<&BTreeMap<String, QueryParamType>> {
        match &self.parameters {
            QueryParameters::Untyped(_) => None,
            QueryParameters::Typed { types, .. } => Some(types),
        }
    }

    /// Consume the validated request into its closed query and runtime values.
    pub fn into_query(self) -> (BatchQuery, BTreeMap<String, QueryValue>) {
        let values = match self.parameters {
            QueryParameters::Untyped(values) | QueryParameters::Typed { values, .. } => values,
        };
        (self.query, values)
    }

    /// Insert an explicitly untyped parameter.
    pub fn try_insert_untyped_parameter(
        &mut self,
        name: impl Into<String>,
        value: QueryValue,
    ) -> Result<(), QueryError> {
        let name = name.into();
        validate_parameter_name(&name)?;
        validate_json_value(&value, &name)?;
        match &mut self.parameters {
            QueryParameters::Untyped(values) => {
                if values.contains_key(&name) {
                    return Err(QueryError::DuplicateParameterName(name));
                }
                values.insert(name, value);
                Ok(())
            }
            QueryParameters::Typed { .. } => Err(QueryError::MixedParameterModes),
        }
    }

    /// Insert an explicitly untyped parameter.
    ///
    /// This compatibility builder cannot create an invalid request: invalid
    /// names, values, or typed/untyped mixing panic at the call site.
    pub fn insert_parameter_value(&mut self, name: impl Into<String>, value: QueryValue) {
        self.try_insert_untyped_parameter(name, value)
            .expect("untyped query parameter must be valid");
    }

    /// Atomically insert a typed parameter.
    pub fn try_insert_typed_parameter(
        &mut self,
        name: impl Into<String>,
        ty: QueryParamType,
        value: QueryValue,
    ) -> Result<(), QueryError> {
        let name = name.into();
        validate_parameter_name(&name)?;
        let value = normalize_typed_value(&ty, value, &name)?;
        if matches!(&self.parameters, QueryParameters::Untyped(values) if values.is_empty()) {
            self.parameters = QueryParameters::Typed {
                values: BTreeMap::new(),
                types: BTreeMap::new(),
            };
        }
        match &mut self.parameters {
            QueryParameters::Untyped(_) => Err(QueryError::MixedParameterModes),
            QueryParameters::Typed { values, types } => {
                if values.contains_key(&name) {
                    return Err(QueryError::DuplicateParameterName(name));
                }
                values.insert(name.clone(), value);
                types.insert(name, ty);
                Ok(())
            }
        }
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

    /// Add an atomic typed parameter.
    pub fn with_typed_parameter(
        mut self,
        name: impl Into<String>,
        ty: QueryParamType,
        value: QueryValue,
    ) -> Result<Self, QueryError> {
        self.try_insert_typed_parameter(name, ty, value)?;
        Ok(self)
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

#[derive(Serialize)]
struct QueryRequestRef<'a> {
    request_type: QueryRequestType,
    query_name: &'a Option<String>,
    query: &'a BatchQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<&'a BTreeMap<String, QueryValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameter_types: Option<&'a BTreeMap<String, QueryParamType>>,
}

impl Serialize for QueryRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        QueryRequestRef {
            request_type: self.request_type(),
            query_name: &self.query_name,
            query: &self.query,
            parameters: self.parameters(),
            parameter_types: self.parameter_types(),
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
struct RawQueryRequest {
    request_type: QueryRequestType,
    #[serde(default)]
    query_name: Option<String>,
    query: BatchQuery,
    #[serde(default)]
    parameters: Option<UniqueMap<QueryValue>>,
    #[serde(default)]
    parameter_types: Option<UniqueMap<QueryParamType>>,
}

struct UniqueMap<T>(BTreeMap<String, T>);

impl<'de, T> Deserialize<'de> for UniqueMap<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UniqueMapVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> Visitor<'de> for UniqueMapVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = UniqueMap<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object with unique parameter names")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((name, value)) = map.next_entry::<String, T>()? {
                    if values.insert(name.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(
                            QueryError::DuplicateParameterName(name),
                        ));
                    }
                }
                Ok(UniqueMap(values))
            }
        }

        deserializer.deserialize_map(UniqueMapVisitor(std::marker::PhantomData))
    }
}

impl<'de> Deserialize<'de> for QueryRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawQueryRequest::deserialize(deserializer)?;
        if !matches!(
            (&raw.request_type, &raw.query),
            (QueryRequestType::Read, BatchQuery::Read(_))
                | (QueryRequestType::Write, BatchQuery::Write(_))
        ) {
            return Err(serde::de::Error::custom(
                "request_type must match the query batch variant",
            ));
        }

        let values = raw.parameters.map_or_else(BTreeMap::new, |values| values.0);
        let parameters = match raw.parameter_types.map(|types| types.0) {
            None => {
                for (name, value) in &values {
                    validate_parameter_name(name).map_err(serde::de::Error::custom)?;
                    validate_json_value(value, name).map_err(serde::de::Error::custom)?;
                }
                QueryParameters::Untyped(values)
            }
            Some(types) => {
                let missing_values = types
                    .keys()
                    .filter(|name| !values.contains_key(*name))
                    .cloned()
                    .collect::<Vec<_>>();
                let extra_values = values
                    .keys()
                    .filter(|name| !types.contains_key(*name))
                    .cloned()
                    .collect::<Vec<_>>();
                if !missing_values.is_empty() || !extra_values.is_empty() {
                    return Err(serde::de::Error::custom(
                        QueryError::ParameterNameMismatch {
                            missing_values,
                            extra_values,
                        },
                    ));
                }
                let values = values
                    .into_iter()
                    .map(|(name, value)| {
                        validate_parameter_name(&name).map_err(serde::de::Error::custom)?;
                        let ty = types
                            .get(&name)
                            .expect("schema and value names were proven equal");
                        normalize_typed_value(ty, value, &name)
                            .map(|value| (name, value))
                            .map_err(serde::de::Error::custom)
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                QueryParameters::Typed { values, types }
            }
        };

        Ok(Self {
            query_name: raw.query_name,
            query: raw.query,
            parameters,
        })
    }
}

fn validate_parameter_name(name: &str) -> Result<(), QueryError> {
    if name.is_empty() {
        Err(QueryError::InvalidParameterName)
    } else {
        Ok(())
    }
}

fn validate_json_value(value: &QueryValue, path: &str) -> Result<(), QueryError> {
    match value {
        QueryValue::F64(value) if !value.is_finite() => Err(QueryError::ParameterTypeMismatch {
            path: path.to_owned(),
            expected: QueryParamType::Value,
            actual: "non-finite f64",
        }),
        QueryValue::F32(value) if !value.is_finite() => Err(QueryError::ParameterTypeMismatch {
            path: path.to_owned(),
            expected: QueryParamType::Value,
            actual: "non-finite f32",
        }),
        QueryValue::Array(values) => values
            .iter()
            .enumerate()
            .try_for_each(|(index, value)| validate_json_value(value, &format!("{path}[{index}]"))),
        QueryValue::Object(values) => values
            .iter()
            .try_for_each(|(name, value)| validate_json_value(value, &format!("{path}.{name}"))),
        QueryValue::Null
        | QueryValue::Bool(_)
        | QueryValue::I64(_)
        | QueryValue::F64(_)
        | QueryValue::F32(_)
        | QueryValue::String(_) => Ok(()),
    }
}

fn normalize_typed_value(
    ty: &QueryParamType,
    value: QueryValue,
    path: &str,
) -> Result<QueryValue, QueryError> {
    let actual = query_value_kind(&value);
    match (ty, value) {
        (QueryParamType::Bool, value @ QueryValue::Bool(_))
        | (QueryParamType::I64, value @ QueryValue::I64(_))
        | (QueryParamType::String, value @ QueryValue::String(_)) => Ok(value),
        (QueryParamType::F64, QueryValue::F64(value)) if value.is_finite() => {
            Ok(QueryValue::F64(value))
        }
        (QueryParamType::F64, QueryValue::F32(value)) if value.is_finite() => {
            Ok(QueryValue::F64(value.into()))
        }
        (QueryParamType::F32, QueryValue::F32(value)) if value.is_finite() => {
            Ok(QueryValue::F32(value))
        }
        (QueryParamType::F32, QueryValue::F64(value))
            if value.is_finite()
                && value >= f64::from(f32::MIN)
                && value <= f64::from(f32::MAX) =>
        {
            Ok(QueryValue::F32(value as f32))
        }
        (QueryParamType::DateTime, QueryValue::String(datetime))
            if chrono::DateTime::parse_from_rfc3339(&datetime).is_ok() =>
        {
            Ok(QueryValue::String(datetime))
        }
        (QueryParamType::Value, value) => {
            validate_json_value(&value, path)?;
            Ok(value)
        }
        (QueryParamType::Object, value @ QueryValue::Object(_)) => {
            validate_json_value(&value, path)?;
            Ok(value)
        }
        (QueryParamType::Array(inner), QueryValue::Array(values)) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| normalize_typed_value(inner, value, &format!("{path}[{index}]")))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryValue::Array),
        (QueryParamType::Bytes, _) => Err(QueryError::unsupported_bytes(path)),
        (expected, _) => Err(QueryError::ParameterTypeMismatch {
            path: path.to_owned(),
            expected: expected.clone(),
            actual,
        }),
    }
}

fn query_value_kind(value: &QueryValue) -> &'static str {
    match value {
        QueryValue::Null => "null",
        QueryValue::Bool(_) => "bool",
        QueryValue::I64(_) => "i64",
        QueryValue::F64(_) => "f64",
        QueryValue::F32(_) => "f32",
        QueryValue::String(_) => "string",
        QueryValue::Array(_) => "array",
        QueryValue::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{read_batch, write_batch};

    fn typed(ty: QueryParamType, value: QueryValue) -> Result<QueryRequest, QueryError> {
        QueryRequest::read(read_batch()).with_typed_parameter("value", ty, value)
    }

    #[test]
    fn query_values_convert_losslessly_to_property_values() {
        let value = QueryValue::Object(BTreeMap::from([
            (
                "array".to_owned(),
                QueryValue::Array(vec![QueryValue::Null, QueryValue::Bool(true)]),
            ),
            ("f32".to_owned(), QueryValue::F32(1.25)),
            ("f64".to_owned(), QueryValue::F64(2.5)),
            ("i64".to_owned(), QueryValue::I64(3)),
            ("string".to_owned(), QueryValue::String("value".to_owned())),
        ]));

        assert_eq!(
            PropertyValue::from(&value),
            PropertyValue::object([
                (
                    "array",
                    PropertyValue::array([PropertyValue::Null, PropertyValue::Bool(true)]),
                ),
                ("f32", PropertyValue::F32(1.25)),
                ("f64", PropertyValue::F64(2.5)),
                ("i64", PropertyValue::I64(3)),
                ("string", PropertyValue::String("value".to_owned())),
            ])
        );
    }

    fn read_wire(parameters: &str, parameter_types: Option<&str>) -> String {
        let parameter_types = parameter_types
            .map(|types| format!(r#","parameter_types":{types}"#))
            .unwrap_or_default();
        format!(
            r#"{{"request_type":"read","query_name":null,"query":{{"read":{{"entries":[],"returns":[]}}}},"parameters":{parameters}{parameter_types}}}"#
        )
    }

    #[test]
    fn request_serde_accepts_matching_tags_and_rejects_both_disagreements() {
        let read = QueryRequest::read(read_batch())
            .to_json_string()
            .expect("read request should serialize");
        let write = QueryRequest::write(write_batch())
            .to_json_string()
            .expect("write request should serialize");

        let parsed_read =
            sonic_rs::from_str::<QueryRequest>(&read).expect("read/read should deserialize");
        let parsed_write =
            sonic_rs::from_str::<QueryRequest>(&write).expect("write/write should deserialize");
        assert_eq!(parsed_read.request_type(), QueryRequestType::Read);
        assert_eq!(parsed_write.request_type(), QueryRequestType::Write);

        let read_tagged_write =
            write.replacen(r#""request_type":"write""#, r#""request_type":"read""#, 1);
        let write_tagged_read =
            read.replacen(r#""request_type":"read""#, r#""request_type":"write""#, 1);
        assert!(sonic_rs::from_str::<QueryRequest>(&read_tagged_write).is_err());
        assert!(sonic_rs::from_str::<QueryRequest>(&write_tagged_read).is_err());
    }

    #[test]
    fn published_openapi_examples_are_valid_query_requests() {
        let specification =
            sonic_rs::from_str::<sonic_rs::Value>(include_str!("../../../docs/openapi.json"))
                .expect("published OpenAPI document is valid JSON");
        let examples = &specification["paths"]["/v2/query"]["post"]["requestBody"]["content"]
            ["application/json"]["examples"];

        for (name, expected_type) in [
            ("read", QueryRequestType::Read),
            ("write", QueryRequestType::Write),
        ] {
            let example = sonic_rs::to_string(&examples[name]["value"])
                .expect("OpenAPI query example is serializable");
            let request = sonic_rs::from_str::<QueryRequest>(&example)
                .unwrap_or_else(|error| panic!("OpenAPI {name} example is invalid: {error}"));
            assert_eq!(request.request_type(), expected_type);
        }
    }

    #[test]
    fn typed_parameter_schema_matrix_accepts_only_valid_shapes() {
        assert!(typed(QueryParamType::Bool, QueryValue::Bool(true)).is_ok());
        assert!(typed(QueryParamType::Bool, QueryValue::I64(1)).is_err());

        assert!(typed(QueryParamType::I64, QueryValue::I64(i64::MAX)).is_ok());
        assert!(typed(QueryParamType::I64, QueryValue::F64(1.0)).is_err());

        assert!(typed(QueryParamType::F64, QueryValue::F64(1.25)).is_ok());
        let f64_from_f32 = typed(QueryParamType::F64, QueryValue::F32(1.25)).unwrap();
        assert!(matches!(
            f64_from_f32.parameters().unwrap().get("value"),
            Some(QueryValue::F64(value)) if *value == 1.25
        ));
        assert!(typed(QueryParamType::F64, QueryValue::F64(f64::NAN)).is_err());

        let f32_from_json = typed(QueryParamType::F32, QueryValue::F64(1.25)).unwrap();
        assert!(matches!(
            f32_from_json.parameters().unwrap().get("value"),
            Some(QueryValue::F32(value)) if *value == 1.25
        ));
        assert!(typed(QueryParamType::F32, QueryValue::F64(f64::MAX)).is_err());
        assert!(typed(QueryParamType::F32, QueryValue::F32(f32::INFINITY)).is_err());

        assert!(typed(QueryParamType::String, QueryValue::String("x".to_owned())).is_ok());
        assert!(typed(QueryParamType::String, QueryValue::Null).is_err());

        assert!(typed(
            QueryParamType::DateTime,
            QueryValue::String("2026-07-28T12:34:56Z".to_owned()),
        )
        .is_ok());
        assert!(typed(
            QueryParamType::DateTime,
            QueryValue::String("28 July 2026".to_owned()),
        )
        .is_err());

        assert!(matches!(
            typed(QueryParamType::Bytes, QueryValue::String("AQID".to_owned())),
            Err(QueryError::UnsupportedBytesParameter(path)) if path == "value"
        ));

        assert!(typed(
            QueryParamType::Value,
            QueryValue::Array(vec![QueryValue::Object(BTreeMap::from([(
                "nested".to_owned(),
                QueryValue::Null,
            )]))]),
        )
        .is_ok());
        assert!(typed(
            QueryParamType::Value,
            QueryValue::Array(vec![QueryValue::F64(f64::INFINITY)]),
        )
        .is_err());

        assert!(typed(QueryParamType::Object, QueryValue::Object(BTreeMap::new())).is_ok());
        assert!(typed(QueryParamType::Object, QueryValue::Array(Vec::new())).is_err());

        assert!(typed(
            QueryParamType::Array(Box::new(QueryParamType::Bool)),
            QueryValue::Array(vec![QueryValue::Bool(true), QueryValue::Bool(false)]),
        )
        .is_ok());
        assert!(typed(
            QueryParamType::Array(Box::new(QueryParamType::Bool)),
            QueryValue::Array(vec![QueryValue::Bool(true), QueryValue::I64(0)]),
        )
        .is_err());
    }

    #[test]
    fn parameter_modes_names_and_duplicate_entries_are_closed() {
        let mut untyped = QueryRequest::read(read_batch());
        untyped
            .try_insert_untyped_parameter("value", QueryValue::Bool(true))
            .unwrap();
        assert!(matches!(
            untyped.try_insert_untyped_parameter("value", QueryValue::Bool(false)),
            Err(QueryError::DuplicateParameterName(name)) if name == "value"
        ));
        assert!(matches!(
            untyped.try_insert_typed_parameter(
                "typed",
                QueryParamType::Bool,
                QueryValue::Bool(true),
            ),
            Err(QueryError::MixedParameterModes)
        ));

        let mut typed = QueryRequest::read(read_batch());
        typed
            .try_insert_typed_parameter("value", QueryParamType::Bool, QueryValue::Bool(true))
            .unwrap();
        assert!(matches!(
            typed.try_insert_typed_parameter(
                "value",
                QueryParamType::Bool,
                QueryValue::Bool(false),
            ),
            Err(QueryError::DuplicateParameterName(name)) if name == "value"
        ));
        assert!(matches!(
            typed.try_insert_untyped_parameter("untyped", QueryValue::Bool(true)),
            Err(QueryError::MixedParameterModes)
        ));

        assert!(matches!(
            QueryRequest::read(read_batch()).with_typed_parameter(
                "",
                QueryParamType::Bool,
                QueryValue::Bool(true),
            ),
            Err(QueryError::InvalidParameterName)
        ));
    }

    #[test]
    fn raw_parameter_dto_rejects_mismatched_empty_and_duplicate_names() {
        let missing_value = read_wire(r#"{}"#, Some(r#"{"value":"bool"}"#));
        let extra_value = read_wire(r#"{"value":true}"#, Some(r#"{}"#));
        let empty_name = read_wire(r#"{"":true}"#, Some(r#"{"":"bool"}"#));
        let duplicate_value = read_wire(
            r#"{"value":true,"value":false}"#,
            Some(r#"{"value":"bool"}"#),
        );
        let duplicate_type = read_wire(
            r#"{"value":true}"#,
            Some(r#"{"value":"bool","value":"bool"}"#),
        );

        for invalid in [
            missing_value,
            extra_value,
            empty_name,
            duplicate_value,
            duplicate_type,
        ] {
            assert!(
                sonic_rs::from_str::<QueryRequest>(&invalid).is_err(),
                "invalid DTO should be rejected: {invalid}"
            );
        }
    }

    #[test]
    fn raw_f32_is_normalized_and_untyped_parameters_remain_explicit() {
        let raw = read_wire(r#"{"value":1.25}"#, Some(r#"{"value":"f32"}"#));
        let typed = sonic_rs::from_str::<QueryRequest>(&raw).expect("valid typed f32 request");
        assert!(matches!(
            typed.parameters().unwrap().get("value"),
            Some(QueryValue::F32(value)) if *value == 1.25
        ));

        let raw = read_wire(r#"{"value":{"nested":[true,1,"x"]}}"#, None);
        let untyped = sonic_rs::from_str::<QueryRequest>(&raw).expect("valid untyped JSON request");
        assert!(untyped.parameter_types().is_none());
        assert!(matches!(
            untyped.parameters().unwrap().get("value"),
            Some(QueryValue::Object(_))
        ));
    }
}
