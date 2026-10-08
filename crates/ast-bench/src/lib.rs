//! Request stages shared by the native request parse, memory and throughput
//! benchmarks.
//!
//! Each stage mirrors the production code it names, so a benchmark times the
//! same work a native request does between its transport and the planner:
//!
//! 1. the transport parses the body (`QueryRequest::from_json_slice`),
//! 2. the query service bounds the nesting (`QueryRequest::check_nesting`),
//! 3. splits the request and binds its parameters ([`param_bindings`]),
//! 4. copies the parameters into the planner context ([`planner_context`]),
//! 5. and plans the batch.
//!
//! ```
//! use helix_ast::{batch, query::{QueryRequest, QueryValue}, traversal};
//!
//! let request = QueryRequest::read(
//!     batch::read_batch()
//!         .var_as("users", traversal::g().n_with_label("User").count())
//!         .returning(["users"]),
//! )
//! .with_parameter_value("unused", QueryValue::I64(3));
//! let (batch, parameters) = request.into_query();
//! let params = helix_ast_bench::param_bindings(parameters);
//! assert_eq!(params.query_values.len(), 1);
//! let context = helix_ast_bench::planner_context(params);
//! assert!(helix_planner::planning::plan_with_diagnostics(&batch, &context).is_ok());
//! ```

use std::collections::BTreeMap;

use helix_ast::query::QueryValue;
use helix_planner::context::{ParamBindings, PlannerContext};
use helix_planner::ir::NonEmptyString;

/// Bind a parsed request's parameters as `db::query_service` does before
/// planning (`query_param_bindings`).
///
/// # Panics
///
/// Panics on an empty parameter name, which request parsing already rejects.
pub fn param_bindings(parameters: BTreeMap<String, QueryValue>) -> ParamBindings {
    ParamBindings {
        values: BTreeMap::new(),
        query_values: parameters
            .into_iter()
            .map(|(name, value)| {
                let name = NonEmptyString::new(name).expect("parsed requests have non-empty names");
                (name, value)
            })
            .collect(),
    }
}

/// A planner context holding `params` and no index catalog, as planning
/// sees a request against an empty database.
pub fn planner_context(params: ParamBindings) -> PlannerContext {
    PlannerContext {
        params: params.into(),
        ..PlannerContext::default()
    }
}
