//! Search access contract builders.
//!
//! This module validates catalog-backed vector/text search AST payloads into
//! residual-free access plans for native selected AST lowering. The public
//! facade keeps node/edge search builders and index metadata construction
//! stable while child modules own catalog lookup, query/limit validation, and
//! tenant-scope contracts independently.

mod access;
mod index;
mod input;
mod lookup;

pub use access::{
    edge_text_search, edge_vector_search, node_text_search, node_vector_search, SearchAccessPlan,
};
pub use index::{search_index_plan, SearchTenantInput};

use crate::{error, ir};

fn non_empty(value: &str, field: ir::NameField) -> Result<ir::NonEmptyString, error::PlannerError> {
    ir::NonEmptyString::new(value).ok_or(error::PlannerError::InvalidEmptyName { field })
}
