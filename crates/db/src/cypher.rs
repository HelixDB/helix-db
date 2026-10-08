//! Cypher request boundary. Parsing, planning and execution preserve the same
//! tenant, catalog, transaction and deadline authority as native queries.
use crate::{
    encoding::v2::keys::scope::DataScope, execution_control::ExecutionControl, HelixDB,
    HelixDbError,
};
use helix_planner::{context, ir, relational as r};
use serde::Serialize;
use std::collections::BTreeMap;

mod explain;
pub use explain::{explain, Explanation};
pub use helix_cypher::request::{CompiledRequest, Request};
pub(crate) mod output;
mod parameters;
pub use output::EncodedResponse;

/// Deadline applied when a caller does not supply its own execution control.
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A request in either form accepted by [`execute_with`].
pub(crate) enum Input {
    Source(Request),
    Compiled(CompiledRequest),
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
            ExecutionControl::from_timeout(DEFAULT_TIMEOUT),
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
            ExecutionControl::from_timeout(DEFAULT_TIMEOUT),
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
    let (query, parameters) = match request {
        Input::Source(request) => request.compile()?,
        Input::Compiled(request) => request,
    }
    .into_parts();
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

#[cfg(test)]
#[path = "cypher/tests/parameters.rs"]
mod parameter_tests;

#[cfg(test)]
#[path = "cypher/tests/compiled.rs"]
mod compiled_tests;
