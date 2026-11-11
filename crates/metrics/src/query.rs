//! Privacy-safe query telemetry for server and embedded Helix runtimes.

use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use uuid::{Uuid, Version};

use crate::{attribution, telemetry};

pub mod transport;

const MAX_CONTEXT_ID_BYTES: usize = 64;
const MAX_USER_AGENT_BYTES: usize = 64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("{0} cannot be empty")]
    Empty(&'static str),
    #[error(
        "{0} must be at most {MAX_CONTEXT_ID_BYTES} bytes with no control characters or surrounding whitespace"
    )]
    InvalidContextId(&'static str),
    #[error("installation_id must be a UUIDv4")]
    InvalidInstallationId,
    #[error("raw_query must be valid JSON")]
    InvalidRawQuery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        context_id(value, "cluster_id").map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        context_id(value, "tenant_id").map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// First product token of a request's `User-Agent`, such as
/// `helix-foundry/0.1.3` or `python-httpx/0.27.0`. Later tokens and comments,
/// which can describe the caller's OS or device, are dropped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserAgent(String);

impl UserAgent {
    /// Keeps the first token, cut to 64 bytes. A header with no token or
    /// with a non-ASCII or control byte in it gives `None`.
    ///
    /// ```
    /// use helix_metrics::query::UserAgent;
    ///
    /// let product = |header| UserAgent::from_header(header).map(|agent| agent.as_str().to_owned());
    /// assert_eq!(product("helix-foundry/0.1.3").as_deref(), Some("helix-foundry/0.1.3"));
    /// assert_eq!(product("Mozilla/5.0 (Macintosh; Intel Mac OS X)").as_deref(), Some("Mozilla/5.0"));
    /// assert_eq!(product("  "), None);
    /// assert_eq!(product("caf\u{e9}/1"), None);
    /// ```
    #[must_use]
    pub fn from_header(value: &str) -> Option<Self> {
        let token = value.split_ascii_whitespace().next()?;
        let token = token.get(..MAX_USER_AGENT_BYTES).unwrap_or(token);
        token
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
            .then(|| Self(token.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a transport knows about the request a query arrived in. Neither
/// value affects execution; both are only reported.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestContext {
    /// From the `x-helix-tenant-id` header.
    pub tenant_id: Option<TenantId>,
    /// From the `User-Agent` header.
    pub user_agent: Option<UserAgent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstallationId(Uuid);

impl InstallationId {
    #[must_use]
    pub fn now() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn parse(value: &str) -> Result<Self, ValidationError> {
        let value = Uuid::parse_str(value).map_err(|_| ValidationError::InvalidInstallationId)?;
        if value.get_version() != Some(Version::Random) {
            return Err(ValidationError::InvalidInstallationId);
        }
        Ok(Self(value))
    }
}

impl core::fmt::Display for InstallationId {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserId(String);

impl UserId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        non_empty(value, "user_id").map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OssIdentity {
    installation_id: InstallationId,
    user_id: Option<UserId>,
}

impl OssIdentity {
    #[must_use]
    pub const fn new(installation_id: InstallationId, user_id: Option<UserId>) -> Self {
        Self {
            installation_id,
            user_id,
        }
    }

    #[must_use]
    pub const fn installation_id(&self) -> InstallationId {
        self.installation_id
    }

    #[must_use]
    pub fn user_id(&self) -> Option<&UserId> {
        self.user_id.as_ref()
    }

    pub(crate) fn client_info(
        &self,
        version: &str,
        attribution: &attribution::Attribution,
    ) -> Result<telemetry::ClientInfo, telemetry::TelemetryError> {
        telemetry::ClientInfo::new(
            version,
            self.installation_id.to_string(),
            self.user_id.as_ref().map(|user_id| user_id.0.clone()),
            attribution,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryName(String);

impl QueryName {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        non_empty(value, "query name").map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalQuery(String);

impl CanonicalQuery {
    /// Validates a canonical query JSON string.
    ///
    /// ```
    /// use helix_metrics::query::CanonicalQuery;
    ///
    /// assert!(CanonicalQuery::new(r#"{"queries":[],"returns":[]}"#).is_ok());
    /// assert!(CanonicalQuery::new("not-json").is_err());
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = non_empty(value, "raw_query")?;
        serde_json::from_str::<Value>(&value).map_err(|_| ValidationError::InvalidRawQuery)?;
        Ok(Self(value))
    }

    /// Serializes only the supplied query AST with recursively sorted keys.
    ///
    /// ```
    /// use helix_metrics::query::CanonicalQuery;
    ///
    /// let query = serde_json::json!({"z": {"b": 2, "a": 1}, "a": []});
    /// assert_eq!(
    ///     CanonicalQuery::from_serializable(&query).unwrap().as_str(),
    ///     r#"{"a":[],"z":{"a":1,"b":2}}"#,
    /// );
    /// ```
    pub fn from_serializable<T>(value: &T) -> Result<Self, ValidationError>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).map_err(|_| ValidationError::InvalidRawQuery)?;
        let canonical = serde_json::to_string(&sort_json(value))
            .map_err(|_| ValidationError::InvalidRawQuery)?;
        Self::new(canonical)
    }

    /// Serializes a query AST after replacing every literal property value
    /// with its stable value kind.
    ///
    /// Identifiers, operations, and parameter references are preserved, while
    /// strings, vectors, bytes, and other request literals cannot enter
    /// telemetry.
    pub fn from_telemetry_serializable<T>(value: &T) -> Result<Self, ValidationError>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).map_err(|_| ValidationError::InvalidRawQuery)?;
        let canonical = serde_json::to_string(&sort_json(redact_query_literals(value)))
            .map_err(|_| ValidationError::InvalidRawQuery)?;
        Self::new(canonical)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn into_value(self) -> Value {
        serde_json::from_str(&self.0).expect("canonical query was validated at construction")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryType {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryErrorType {
    InvalidRequest,
    Planning,
    Execution,
    Conflict,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryError {
    #[serde(rename = "type")]
    pub error_type: QueryErrorType,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryWarningType {
    MissingIndex,
    DeepHops,
    UnboundedScan,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryWarning {
    #[serde(rename = "type")]
    pub warning_type: QueryWarningType,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum QueryOutcome {
    Succeeded { warnings: Vec<QueryWarning> },
    Failed { errors: Vec<QueryError> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryEvent {
    pub event_id: String,
    pub occurred_at: String,
    pub name: Option<QueryName>,
    pub raw_query: CanonicalQuery,
    pub query_type: QueryType,
    pub latency_micros: u64,
    pub context: RequestContext,
    pub outcome: QueryOutcome,
    pub planner_diagnostics: Option<Value>,
}

impl QueryEvent {
    #[must_use]
    pub fn now(
        name: Option<QueryName>,
        raw_query: CanonicalQuery,
        query_type: QueryType,
        latency_micros: u64,
        context: RequestContext,
        outcome: QueryOutcome,
        planner_diagnostics: Option<Value>,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7().to_string(),
            occurred_at: telemetry::timestamp_now(),
            name,
            raw_query,
            query_type,
            latency_micros,
            context,
            outcome,
            planner_diagnostics,
        }
    }

    pub fn into_telemetry(self) -> Result<telemetry::Event, telemetry::TelemetryError> {
        self.into_telemetry_with_cluster(None)
    }

    /// Converts to a `query.execute` event.
    ///
    /// An event over the properties limit is still sent so it is counted:
    /// planner diagnostics are dropped first, then the canonical query is
    /// replaced by `{"omitted":"too_large"}`.
    pub(crate) fn into_telemetry_with_cluster(
        self,
        cluster_id: Option<&ClusterId>,
    ) -> Result<telemetry::Event, telemetry::TelemetryError> {
        #[derive(Serialize)]
        struct Properties {
            #[serde(skip_serializing_if = "Option::is_none")]
            cluster_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            tenant_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            user_agent: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            query_name: Option<String>,
            canonical_query: Value,
            query_type: QueryType,
            latency_micros: u64,
            outcome: QueryOutcome,
            #[serde(skip_serializing_if = "Option::is_none")]
            planner_diagnostics: Option<Value>,
        }

        let mut properties = Properties {
            cluster_id: cluster_id.map(|cluster_id| cluster_id.0.clone()),
            tenant_id: self.context.tenant_id.map(|tenant_id| tenant_id.0),
            user_agent: self.context.user_agent.map(|user_agent| user_agent.0),
            query_name: self.name.map(|name| name.0),
            canonical_query: self.raw_query.into_value(),
            query_type: self.query_type,
            latency_micros: self.latency_micros,
            outcome: self.outcome,
            planner_diagnostics: self.planner_diagnostics,
        };
        let encode = |properties: &Properties| {
            telemetry::Event::with_identity(
                self.event_id.as_str(),
                "query.execute",
                self.occurred_at.as_str(),
                serde_json::to_value(properties)?,
            )
        };
        let event = encode(&properties);
        if !matches!(event, Err(telemetry::TelemetryError::PropertiesTooLarge)) {
            return event;
        }
        properties.planner_diagnostics = None;
        let event = encode(&properties);
        if !matches!(event, Err(telemetry::TelemetryError::PropertiesTooLarge)) {
            return event;
        }
        properties.canonical_query = serde_json::json!({"omitted": "too_large"});
        encode(&properties)
    }
}

pub(crate) fn context_id(
    value: impl Into<String>,
    name: &'static str,
) -> Result<String, ValidationError> {
    let value = non_empty(value, name)?;
    if value.len() > MAX_CONTEXT_ID_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(ValidationError::InvalidContextId(name));
    }
    Ok(value)
}

fn non_empty(value: impl Into<String>, name: &'static str) -> Result<String, ValidationError> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(ValidationError::Empty(name));
    }
    Ok(value)
}

fn sort_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json).collect()),
        Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_json(value)))
                    .collect(),
            )
        }
        scalar @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => scalar,
    }
}

fn redact_query_literals(value: Value) -> Value {
    match value {
        Value::Array(values) => {
            Value::Array(values.into_iter().map(redact_query_literals).collect())
        }
        Value::Object(values) => match serialized_property_value_object_kind(&values) {
            Some(kind) => serde_json::json!({"redacted_property_value": kind}),
            None => Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, redact_query_literals(value)))
                    .collect(),
            ),
        },
        scalar @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => scalar,
    }
}

fn serialized_property_value_object_kind(
    fields: &serde_json::Map<String, Value>,
) -> Option<String> {
    if fields.len() != 1 {
        return None;
    }
    let kind = fields.keys().next().expect("single property value variant");
    matches!(
        kind.as_str(),
        "bool"
            | "i64"
            | "date_time"
            | "f64"
            | "f32"
            | "string"
            | "bytes"
            | "i64_array"
            | "f64_array"
            | "f32_array"
            | "string_array"
            | "array"
            | "object"
    )
    .then(|| kind.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_event() -> telemetry::Event {
        QueryEvent {
            event_id: "018f6f1e-0000-7000-8000-000000000001".to_owned(),
            occurred_at: "2026-07-28T12:00:00Z".to_owned(),
            name: Some(QueryName::new("find_users").expect("query name")),
            raw_query: CanonicalQuery::new(r#"{"queries":[],"returns":["users"]}"#).expect("query"),
            query_type: QueryType::Read,
            latency_micros: 42,
            context: RequestContext {
                tenant_id: Some(TenantId::new("tenant-1").expect("tenant ID")),
                user_agent: UserAgent::from_header("helix-foundry/0.1.3 (linux)"),
            },
            outcome: QueryOutcome::Succeeded {
                warnings: Vec::new(),
            },
            planner_diagnostics: Some(serde_json::json!({
                "statistics": {"total_operators": 1},
                "insights": []
            })),
        }
        .into_telemetry_with_cluster(Some(&ClusterId::new("cluster-1").expect("cluster ID")))
        .expect("telemetry event")
    }

    #[test]
    fn query_event_properties_are_exact_and_private() {
        let event = query_event();
        assert_eq!(
            serde_json::to_string(&event).expect("encode event"),
            r#"{"event_id":"018f6f1e-0000-7000-8000-000000000001","name":"query.execute","occurred_at":"2026-07-28T12:00:00Z","properties":{"canonical_query":{"queries":[],"returns":["users"]},"cluster_id":"cluster-1","latency_micros":42,"outcome":{"status":"succeeded","warnings":[]},"planner_diagnostics":{"insights":[],"statistics":{"total_operators":1}},"query_name":"find_users","query_type":"read","tenant_id":"tenant-1","user_agent":"helix-foundry/0.1.3"}}"#
        );
        let encoded = serde_json::to_string(&event.properties).expect("properties");
        for sensitive in ["parameters", "embeddings", "email", "returned_rows"] {
            assert!(!encoded.contains(sensitive));
        }
    }

    #[test]
    fn telemetry_canonical_query_redacts_literals_but_preserves_structure() {
        let query = serde_json::json!({
            "steps": [
                {"eq": {
                    "left": {"property": "email"},
                    "right": {"constant": {"string": "secret@example.com"}}
                }},
                {"has": {
                    "property": "phone",
                    "value": {"string": "+44 1234"}
                }},
                {"vector_search": {
                    "query_vector": {"value": {"f32_array": [0.1, 0.2]}},
                    "tenant_value": {"expr": {"param": "tenant"}}
                }}
            ]
        });
        let canonical =
            CanonicalQuery::from_telemetry_serializable(&query).expect("telemetry-safe query");

        assert_eq!(
            canonical.as_str(),
            r#"{"steps":[{"eq":{"left":{"property":"email"},"right":{"constant":{"redacted_property_value":"string"}}}},{"has":{"property":"phone","value":{"redacted_property_value":"string"}}},{"vector_search":{"query_vector":{"value":{"redacted_property_value":"f32_array"}},"tenant_value":{"expr":{"param":"tenant"}}}}]}"#
        );
        assert!(!canonical.as_str().contains("secret@example.com"));
        assert!(!canonical.as_str().contains("+44 1234"));
        assert!(!canonical.as_str().contains("0.1"));
    }

    #[test]
    fn exact_server_query_envelope_is_stable() {
        let client = telemetry::ClientInfo {
            version: "1.2.3".to_owned(),
            os: "darwin".to_owned(),
            arch: "arm64".to_owned(),
            installation_id: "installation-1".to_owned(),
            user_id: None,
            session_id: None,
            environment: None,
            project_id: None,
            deployment_id: None,
        };
        let envelope = telemetry::Envelope::with_sent_at(
            telemetry::Source::Server,
            "2026-07-28T12:00:01Z",
            client,
            vec![query_event()],
        )
        .expect("query envelope");
        assert_eq!(
            String::from_utf8(envelope.to_vec().expect("encode envelope")).expect("UTF-8"),
            r#"{"schema_version":1,"source":"helix-server","sent_at":"2026-07-28T12:00:01Z","client":{"version":"1.2.3","os":"darwin","arch":"arm64","installation_id":"installation-1"},"events":[{"event_id":"018f6f1e-0000-7000-8000-000000000001","name":"query.execute","occurred_at":"2026-07-28T12:00:00Z","properties":{"canonical_query":{"queries":[],"returns":["users"]},"cluster_id":"cluster-1","latency_micros":42,"outcome":{"status":"succeeded","warnings":[]},"planner_diagnostics":{"insights":[],"statistics":{"total_operators":1}},"query_name":"find_users","query_type":"read","tenant_id":"tenant-1","user_agent":"helix-foundry/0.1.3"}}]}"#
        );
    }

    #[test]
    fn exact_embedded_query_envelope_omits_transport_tenant() {
        let event = QueryEvent {
            event_id: "018f6f1e-0000-7000-8000-000000000001".to_owned(),
            occurred_at: "2026-07-28T12:00:00Z".to_owned(),
            name: None,
            raw_query: CanonicalQuery::new(r#"{"queries":[],"returns":[]}"#).expect("query"),
            query_type: QueryType::Read,
            latency_micros: 7,
            context: RequestContext::default(),
            outcome: QueryOutcome::Succeeded {
                warnings: Vec::new(),
            },
            planner_diagnostics: None,
        }
        .into_telemetry_with_cluster(Some(&ClusterId::new("cluster-1").expect("cluster ID")))
        .expect("telemetry event");
        let envelope = telemetry::Envelope::with_sent_at(
            telemetry::Source::Embedded,
            "2026-07-28T12:00:01Z",
            telemetry::ClientInfo {
                version: "1.2.3".to_owned(),
                os: "darwin".to_owned(),
                arch: "arm64".to_owned(),
                installation_id: "installation-1".to_owned(),
                user_id: None,
                session_id: None,
                environment: None,
                project_id: None,
                deployment_id: None,
            },
            vec![event],
        )
        .expect("query envelope");
        assert_eq!(
            String::from_utf8(envelope.to_vec().expect("encode envelope")).expect("UTF-8"),
            r#"{"schema_version":1,"source":"helix-embedded","sent_at":"2026-07-28T12:00:01Z","client":{"version":"1.2.3","os":"darwin","arch":"arm64","installation_id":"installation-1"},"events":[{"event_id":"018f6f1e-0000-7000-8000-000000000001","name":"query.execute","occurred_at":"2026-07-28T12:00:00Z","properties":{"canonical_query":{"queries":[],"returns":[]},"cluster_id":"cluster-1","latency_micros":7,"outcome":{"status":"succeeded","warnings":[]},"query_type":"read"}}]}"#
        );
    }

    fn oversized_event(
        name: &str,
        raw_query: CanonicalQuery,
        planner_diagnostics: Option<Value>,
    ) -> QueryEvent {
        QueryEvent {
            event_id: "018f6f1e-0000-7000-8000-000000000001".to_owned(),
            occurred_at: "2026-07-28T12:00:00Z".to_owned(),
            name: Some(QueryName::new(name).expect("query name")),
            raw_query,
            query_type: QueryType::Write,
            latency_micros: 9,
            context: RequestContext::default(),
            outcome: QueryOutcome::Succeeded {
                warnings: Vec::new(),
            },
            planner_diagnostics,
        }
    }

    #[test]
    fn oversized_events_drop_diagnostics_then_the_query_but_are_still_sent() {
        let small = || CanonicalQuery::new(r#"{"queries":[]}"#).expect("query");
        let large = || {
            CanonicalQuery::new(
                serde_json::json!({"queries": "q".repeat(telemetry::MAX_PROPERTIES_BYTES)})
                    .to_string(),
            )
            .expect("query")
        };
        let diagnostics =
            serde_json::json!({"insights": "d".repeat(telemetry::MAX_PROPERTIES_BYTES)});

        let without_diagnostics = oversized_event("q", small(), Some(diagnostics.clone()))
            .into_telemetry()
            .expect("diagnostics dropped");
        assert_eq!(
            without_diagnostics.properties.get("planner_diagnostics"),
            None
        );
        assert_eq!(
            without_diagnostics.properties["canonical_query"],
            serde_json::json!({"queries": []})
        );

        let without_query = oversized_event("q", large(), Some(diagnostics))
            .into_telemetry()
            .expect("query omitted");
        assert_eq!(without_query.properties.get("planner_diagnostics"), None);
        assert_eq!(
            without_query.properties["canonical_query"],
            serde_json::json!({"omitted": "too_large"})
        );
        assert_eq!(without_query.properties["query_name"], "q");
        assert_eq!(without_query.properties["latency_micros"], 9);
        assert_eq!(
            without_query.event_id,
            "018f6f1e-0000-7000-8000-000000000001"
        );

        assert!(matches!(
            oversized_event(&"n".repeat(telemetry::MAX_PROPERTIES_BYTES), small(), None)
                .into_telemetry(),
            Err(telemetry::TelemetryError::PropertiesTooLarge)
        ));
    }

    #[test]
    fn user_agent_keeps_one_bounded_ascii_token() {
        let product = |header: &str| UserAgent::from_header(header).map(|agent| agent.0);
        assert_eq!(
            product("\tpython-httpx/0.27.0  extra").as_deref(),
            Some("python-httpx/0.27.0")
        );
        assert_eq!(
            product(&format!("{}/1", "a".repeat(100))),
            Some("a".repeat(MAX_USER_AGENT_BYTES))
        );
        for rejected in [
            "",
            "\t \n",
            "tool\u{1}/1",
            "tool\u{7f}/1",
            "\u{e9}t\u{e9}/1",
        ] {
            assert_eq!(product(rejected), None, "{rejected:?}");
        }
    }

    #[test]
    fn telemetry_context_ids_reject_ambiguous_values() {
        assert!(matches!(
            ClusterId::new(" cluster-1"),
            Err(ValidationError::InvalidContextId("cluster_id"))
        ));
        assert!(matches!(
            TenantId::new("tenant\n1"),
            Err(ValidationError::InvalidContextId("tenant_id"))
        ));
        assert!(matches!(
            TenantId::new("x".repeat(MAX_CONTEXT_ID_BYTES + 1)),
            Err(ValidationError::InvalidContextId("tenant_id"))
        ));
    }
}
