//! Cypher request boundary. Parsing, planning and execution preserve the same
//! tenant, catalog, transaction and deadline authority as native queries.
use crate::{
    encoding::v2::keys::scope::DataScope, execution_control::ExecutionControl, HelixDB,
    HelixDbError,
};
use helix_planner::{context, ir, relational as r};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod compiled;
mod explain;
pub use compiled::CompiledRequest;
pub(crate) use compiled::Input;
pub(crate) mod output;
mod parameters;
pub use explain::{explain, Explanation};
pub use output::EncodedResponse;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub query: String,
    #[serde(default, deserialize_with = "deserialize_parameters")]
    pub parameters: BTreeMap<String, helix_ast::query::QueryValue>,
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
}

impl Request {
    /// Resolve statement effects before applying transport routing options.
    /// Use [`Self::compile`] when execution follows routing, to retain this work.
    pub fn request_type(&self) -> r::Result<helix_ast::query::QueryRequestType> {
        Ok(match helix_cypher::compile(&self.query)?.effect() {
            r::Effect::Read => helix_ast::query::QueryRequestType::Read,
            r::Effect::Write => helix_ast::query::QueryRequestType::Write,
        })
    }
}

/// Bounded per-query resources. These limits never truncate a successful result.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub memory_bytes: usize,
    pub result_bytes: usize,
    pub batch_rows: usize,
    /// Maximum items in each materialized expression list and retained
    /// DISTINCT aggregate set. Map entries, function arguments, and streamed
    /// rows use separate validation and memory bounds.
    pub collection_items: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 256 * 1024 * 1024,
            result_bytes: 16 * 1024 * 1024,
            batch_rows: 512,
            collection_items: 1_000_000,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Response {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(skip)]
    pub diagnostics: helix_planner::exec::PlannerMetrics,
    #[serde(skip)]
    pub resources: ResourceUsage,
}

/// Local execution measurements are separate from public result values.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ResourceUsage {
    pub peak_memory_bytes: usize,
    pub reads: StorageReadUsage,
}

pub use crate::query_resources::StorageReadUsage;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Query(#[from] r::QueryError),
    #[error(transparent)]
    Storage(HelixDbError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

impl From<HelixDbError> for Error {
    fn from(error: HelixDbError) -> Self {
        let (detail, message) = if error.error_code()
            == helix_ast::error_code::QueryErrorCode::QueryMemoryLimitExceeded
        {
            ("MemoryLimit", "query live buffers exceed the memory budget")
        } else if matches!(
            error,
            HelixDbError::Encoding(crate::encoding::error::EncodingError::PropertyNestingLimit)
        ) {
            (
                "StoredValueNestingLimit",
                "stored property archive exceeds the decoder nesting limit",
            )
        } else {
            return Self::Storage(error);
        };
        Self::Query(r::QueryError::runtime("ResourceLimit", detail, message))
    }
}

impl HelixDB {
    /// Prepare lossless Cypher JSON before committing a modifying statement.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let database = db::HelixDB::open(db::HelixDbSource::InMemory {
    /// #     database: "cypher-json-example".into(),
    /// # }).await?;
    /// let response = database.cypher_json(db::cypher::Request::new("RETURN 1 AS value")).await?;
    /// let value: serde_json::Value = serde_json::from_slice(response.body())?;
    /// assert_eq!(value["rows"][0][0], 1);
    /// # database.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cypher_json(&self, request: Request) -> Result<EncodedResponse> {
        execute_json(
            self,
            request,
            DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            ExecutionControl::from_timeout(std::time::Duration::from_secs(30)),
            Limits::default(),
        )
        .await
    }

    /// Execute one Cypher statement. Writes commit atomically after evaluation.
    pub async fn cypher(&self, request: Request) -> Result<Response> {
        execute(
            self,
            request,
            DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            ExecutionControl::from_timeout(std::time::Duration::from_secs(30)),
            Limits::default(),
        )
        .await
    }
}

pub async fn execute(
    db: &HelixDB,
    request: Request,
    scope: DataScope,
    mode: crate::query_service::QueryMode,
    control: ExecutionControl,
    limits: Limits,
) -> Result<Response> {
    execute_with::<output::Typed>(db, Input::Source(request), scope, mode, control, limits).await
}

/// Execute and prepare a bounded JSON body before the write transaction commits.
/// The request uses the same tenant, planner and cancellation authority as
/// [`execute`]. Only the response representation differs.
pub async fn execute_json(
    db: &HelixDB,
    request: Request,
    scope: DataScope,
    mode: crate::query_service::QueryMode,
    control: ExecutionControl,
    limits: Limits,
) -> Result<EncodedResponse> {
    execute_with::<output::Json>(db, Input::Source(request), scope, mode, control, limits).await
}

pub(crate) async fn execute_with<O: output::Format>(
    db: &HelixDB,
    request: Input,
    scope: DataScope,
    mode: crate::query_service::QueryMode,
    control: ExecutionControl,
    limits: Limits,
) -> Result<O::Value> {
    control.check()?;
    let PreparedRequest {
        query,
        params,
        values,
        parameter_memory,
    } = prepare_request(request, limits)?;
    if query.effect() == r::Effect::Write
        && (db.is_reader_mode() || mode == crate::query_service::QueryMode::Warm)
    {
        return Err(r::QueryError::compile(
            "AccessModeError",
            "WriterRequired",
            "mutating Cypher requires a writer",
        )
        .into());
    }
    let prepared = control
        .run(db.planner_context_scoped_prepared(params, scope))
        .await?;
    control.check()?;
    let plan = r::plan(query, prepared.context())?;
    control.check()?;
    let (params, proof) = prepared.into_execution_inputs();
    let mut response = crate::execution::interpreter::Interpreter::new_scoped_controlled_prepared(
        db, params, scope, control, proof,
    )
    .execute_rows_with::<O>(
        &plan,
        &values,
        Limits {
            memory_bytes: limits
                .memory_bytes
                .saturating_sub(parameter_memory.retained()),
            ..limits
        },
    )
    .await?;
    let resources = O::resources(&mut response);
    resources.peak_memory_bytes = parameter_memory.construction().max(
        parameter_memory
            .retained()
            .saturating_add(resources.peak_memory_bytes),
    );
    Ok(response)
}

struct PreparedRequest {
    query: r::Query,
    params: context::ParamBindings,
    values: BTreeMap<String, r::Value>,
    parameter_memory: parameters::Footprint,
}

// Shared validation keeps planning-only requests on the execution parameter and
// structural-limit contract without opening an execution transaction.
fn prepare_request(request: Input, limits: Limits) -> Result<PreparedRequest> {
    if limits.batch_rows == 0
        || limits.memory_bytes == 0
        || limits.result_bytes == 0
        || limits.collection_items == 0
    {
        return Err(r::QueryError::compile(
            "ResourceLimit",
            "InvalidLimits",
            "resource budgets must be positive",
        )
        .into());
    }
    let CompiledRequest { query, parameters } = match request {
        Input::Source(request) => request.compile()?,
        Input::Compiled(request) => request,
    };
    for name in query.parameters() {
        if !parameters.contains_key(&name) {
            return Err(r::QueryError::compile(
                "ParameterMissing",
                "MissingParameter",
                format!("missing parameter ${name}"),
            )
            .into());
        }
    }
    let parameters::Prepared {
        bindings: params,
        values,
        footprint: parameter_memory,
    } = parameters::prepare(parameters, limits.memory_bytes)?;
    Ok(PreparedRequest {
        query,
        params,
        values,
        parameter_memory,
    })
}

fn parameter_value(value: &helix_ast::query::QueryValue) -> r::Value {
    use helix_ast::query::QueryValue as Q;
    match value {
        Q::Null => r::Value::Null,
        Q::Bool(b) => r::Value::Boolean(*b),
        Q::I64(i) => r::Value::Integer(*i),
        Q::F64(f) => r::Value::Float(*f),
        Q::F32(f) => r::Value::Float(f64::from(*f)),
        Q::String(s) => r::Value::String(s.clone()),
        Q::Array(xs) => r::Value::List(xs.iter().map(parameter_value).collect()),
        Q::Object(xs) => r::Value::Map(
            xs.iter()
                .map(|(k, v)| (k.clone(), parameter_value(v)))
                .collect(),
        ),
    }
}

fn deserialize_parameters<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, helix_ast::query::QueryValue>, D::Error> {
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

fn decode_parameter(
    value: serde_json::Value,
) -> std::result::Result<helix_ast::query::QueryValue, String> {
    use helix_ast::query::QueryValue as Q;
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

#[cfg(test)]
#[path = "cypher/tests/parameters.rs"]
mod parameter_tests;
