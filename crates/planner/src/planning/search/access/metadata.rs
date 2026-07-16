//! Tenant-aware search index metadata construction.

use helix_ast::value::PropertyInput;

use super::super::{index, input};
use crate::{catalog, error, ir};

pub(super) fn search_index_metadata(
    index_id: ir::NonEmptyString,
    scope: catalog::SearchIndexScope,
    tenant_value: Option<&PropertyInput>,
    kind: catalog::SearchIndexKind,
) -> Result<ir::SearchIndexPlan, error::PlannerError> {
    let tenant = index::tenant_input_from_scope(
        &index_id,
        scope,
        input::tenant_value_plan(tenant_value)?,
        kind,
    )?;
    Ok(index::search_index_plan(index_id, tenant))
}
