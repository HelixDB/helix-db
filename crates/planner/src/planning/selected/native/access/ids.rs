//! Native element-ID source validation.

use crate::{catalog, error, ir};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum NativeElementIds {
    NonEmpty(ir::ElementIds),
    EmptyReference,
}

pub(super) fn element_ids(
    ids: &[u64],
    element: catalog::ElementKind,
) -> Result<NativeElementIds, error::PlannerError> {
    let Some(ids) = ir::AtLeast::<_, 1>::try_from_vec(ids.to_vec()) else {
        return Ok(NativeElementIds::EmptyReference);
    };
    ir::ElementIds::new(ids)
        .map(NativeElementIds::NonEmpty)
        .map_err(|err| match err {
            ir::ElementIdsError::DuplicateId { id } => {
                error::PlannerError::DuplicateElementId { element, id }
            }
        })
}
