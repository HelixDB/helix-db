use crate::{catalog, error, ir};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum MutationElementIds {
    NonEmpty(ir::ElementIds),
    EmptyReference,
}

pub(super) fn non_empty_name(
    value: &str,
    field: ir::NameField,
) -> Result<ir::NonEmptyString, error::PlannerError> {
    ir::NonEmptyString::new(value).ok_or(error::PlannerError::InvalidEmptyName { field })
}

pub(super) fn property_name(value: &str) -> Result<ir::NonEmptyString, error::PlannerError> {
    non_empty_name(value, ir::NameField::Property)
}

pub(super) fn variable_name(value: &str) -> Result<ir::NonEmptyString, error::PlannerError> {
    non_empty_name(value, ir::NameField::Variable)
}

pub(super) fn param_name(value: &str) -> Result<ir::NonEmptyString, error::PlannerError> {
    non_empty_name(value, ir::NameField::Param)
}

pub(super) fn element_ids(
    ids: &[u64],
    element: catalog::ElementKind,
) -> Result<MutationElementIds, error::PlannerError> {
    let Some(ids) = ir::AtLeast::<_, 1>::try_from_vec(ids.to_vec()) else {
        return Ok(MutationElementIds::EmptyReference);
    };
    ir::ElementIds::new(ids)
        .map(MutationElementIds::NonEmpty)
        .map_err(|err| match err {
            ir::ElementIdsError::DuplicateId { id } => {
                error::PlannerError::DuplicateElementId { element, id }
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_empty_name_maps_empty_inputs_to_field_specific_errors() {
        assert_eq!(property_name("name").unwrap().as_ref(), "name");
        assert!(matches!(
            property_name(""),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Property
            })
        ));
        assert!(matches!(
            variable_name(""),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Variable
            })
        ));
        assert!(matches!(
            param_name(""),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Param
            })
        ));
    }

    #[test]
    fn element_ids_distinguish_empty_duplicate_and_valid_sets_by_element_kind() {
        assert!(matches!(
            element_ids(&[], catalog::ElementKind::Node).unwrap(),
            MutationElementIds::EmptyReference
        ));
        assert!(matches!(
            element_ids(&[7, 9], catalog::ElementKind::Node).unwrap(),
            MutationElementIds::NonEmpty(ids) if ids.as_ref() == [7, 9]
        ));
        assert!(matches!(
            element_ids(&[7, 7], catalog::ElementKind::Edge),
            Err(error::PlannerError::DuplicateElementId {
                element: catalog::ElementKind::Edge,
                id: 7,
            })
        ));
    }
}
