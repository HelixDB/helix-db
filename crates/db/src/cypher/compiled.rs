//! Owned frontend output shared by transport routing and request execution.
use super::{r, Request};
use helix_ast::query;
use std::collections::BTreeMap;

/// A parsed, resolved and validated statement with its original parameters.
///
/// Construct this only with [`Request::compile`]. It owns no database, catalog
/// snapshot or transaction. Execution consumes it through
/// [`crate::query_service::HelixQueryService::execute_compiled_cypher_json_scoped_controlled`]
/// and obtains the current request's scoped catalog and resource limits.
/// Parameters are checked and admitted at that execution boundary.
pub struct CompiledRequest {
    pub(super) query: r::Query,
    pub(super) parameters: BTreeMap<String, query::QueryValue>,
}

pub(crate) enum Input {
    Source(Request),
    Compiled(CompiledRequest),
}

impl Request {
    /// Compile once before applying read/write routing policy, retaining the
    /// validated statement for execution. This performs no storage access.
    ///
    /// ```
    /// use db::cypher::Request;
    /// use helix_ast::query::QueryRequestType;
    /// let read = Request::new("MATCH (n) RETURN count(*)").compile().unwrap();
    /// let write = Request::new("CREATE (:Person {name:$name})").compile().unwrap();
    /// assert_eq!(read.request_type(), QueryRequestType::Read);
    /// assert_eq!(write.request_type(), QueryRequestType::Write);
    /// ```
    pub fn compile(self) -> r::Result<CompiledRequest> {
        let query = helix_cypher::compile(&self.query)?;
        Ok(CompiledRequest {
            query,
            parameters: self.parameters,
        })
    }
}

impl CompiledRequest {
    /// The validated statement's effect, without parsing or allocating.
    pub fn request_type(&self) -> query::QueryRequestType {
        match self.query.effect() {
            r::Effect::Read => query::QueryRequestType::Read,
            r::Effect::Write => query::QueryRequestType::Write,
        }
    }
}

#[cfg(test)]
#[path = "tests/compiled.rs"]
mod tests;
