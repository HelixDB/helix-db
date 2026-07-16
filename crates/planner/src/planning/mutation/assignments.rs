use helix_ast::value::PropertyInput;

use crate::{error, ir};

use super::shared;

/// Convert AST property assignments into the mutation IR contract.
///
/// The returned contract allows an empty assignment list, rejects empty
/// property names, rejects duplicate property names, and validates each input
/// expression through [`ir::PropertyInputPlan`].
///
/// ```
/// use helix_ast::value::PropertyInput;
/// use helix_planner::planning::mutation;
///
/// let assignments = mutation::property_assignments(&[
///     ("name".to_owned(), PropertyInput::from("alice")),
/// ])
/// .unwrap();
/// assert_eq!(assignments.as_ref().len(), 1);
/// ```
pub fn property_assignments(
    properties: &[(String, PropertyInput)],
) -> Result<ir::PropertyAssignments, error::PlannerError> {
    let properties = properties
        .iter()
        .map(|(property, value)| {
            Ok((
                shared::property_name(property)?,
                ir::PropertyInputPlan::new(value.clone())?,
            ))
        })
        .collect::<Result<Vec<_>, error::PlannerError>>()?;

    ir::PropertyAssignments::try_from_vec(properties).map_err(|err| match err {
        ir::PropertyAssignmentsError::DuplicateProperty { property } => {
            error::PlannerError::DuplicatePropertyAssignment { property }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::Expr;

    #[test]
    fn property_assignments_allow_empty_and_validate_expression_payloads() {
        let empty = property_assignments(&[]).unwrap();
        assert!(empty.as_ref().is_empty());

        let assignment = property_assignments(&[(
            "name".to_owned(),
            PropertyInput::from(Expr::prop("source_name")),
        )])
        .unwrap();

        assert_eq!(assignment.as_ref().len(), 1);
    }

    #[test]
    fn property_assignments_reject_empty_and_duplicate_property_names() {
        let empty_name =
            property_assignments(&[(String::new(), PropertyInput::from("alice"))]).unwrap_err();
        assert!(matches!(
            empty_name,
            error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Property
            }
        ));

        let duplicate = property_assignments(&[
            ("name".to_owned(), PropertyInput::from("alice")),
            ("name".to_owned(), PropertyInput::from("bob")),
        ])
        .unwrap_err();
        assert!(matches!(
            duplicate,
            error::PlannerError::DuplicatePropertyAssignment { property }
                if property.as_ref() == "name"
        ));
    }
}
