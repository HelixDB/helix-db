use super::*;
use crate::{error, ir};
use helix_ast::expr::Expr;
use helix_ast::projection::{BindingProjection, BindingValueRef, Projection};

#[test]
fn projection_payloads_validate_cardinality_and_duplicates() {
    assert!(matches!(
        values_properties(&[]),
        Err(error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::Values,
            min: 1,
            actual: 0
        })
    ));
    assert!(matches!(
        values_properties(&["name".to_owned(), "name".to_owned()]),
        Err(error::PlannerError::DuplicatePropertySelection { property })
            if property.as_ref() == "name"
    ));

    assert!(matches!(
        projection_items(&[
            Projection::property("name", "display"),
            Projection::expr("display", Expr::val(1)),
        ]),
        Err(error::PlannerError::DuplicateProjectionAlias { alias })
            if alias.as_ref() == "display"
    ));
}

#[test]
fn projection_payloads_lower_valid_property_selection_and_items() {
    assert!(matches!(
        property_selection(None).unwrap(),
        ir::PropertySelection::All
    ));
    assert!(matches!(
        property_selection(Some(&[])).unwrap(),
        ir::PropertySelection::All
    ));

    let selected = property_selection(Some(&["name".to_owned()])).unwrap();
    assert!(matches!(
        selected,
        ir::PropertySelection::Selected(properties)
            if properties.as_ref()[0].as_ref() == "name"
    ));

    let projection = projection_items(&[
        Projection::property("name", "display"),
        Projection::expr("one", Expr::val(1)),
    ])
    .unwrap();
    assert!(matches!(
        projection.as_ref(),
        [
            ir::ProjectionItem::Property { source, alias },
            ir::ProjectionItem::Expr { alias: expr_alias, .. }
        ] if source.as_ref() == "name"
            && alias.as_ref() == "display"
            && expr_alias.as_ref() == "one"
    ));
}

#[test]
fn binding_projection_payloads_validate_coalesce_refs() {
    let invalid = binding_projection_items(&[BindingProjection::Coalesce {
        refs: Vec::new(),
        alias: "id".to_owned(),
    }]);
    assert!(matches!(
        invalid,
        Err(error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::Coalesce,
            min: 1,
            actual: 0
        })
    ));
}

#[test]
fn binding_projection_payloads_lower_valid_targets_and_refs() {
    let projections = binding_projection_items(&[
        BindingProjection::binding("owner", "name", "owner_name"),
        BindingProjection::coalesce(
            vec![
                BindingValueRef::binding("owner", "$id"),
                BindingValueRef::current("$id"),
            ],
            "any_id",
        ),
    ])
    .unwrap();

    assert!(matches!(
        projections.as_ref(),
        [
            ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Binding(binding),
                source,
                alias,
            },
            ir::BindingProjectionPlan::Coalesce { refs, alias: coalesce_alias }
        ] if binding.as_ref() == "owner"
            && source.as_ref() == "name"
            && alias.as_ref() == "owner_name"
            && refs.as_ref().len() == 2
            && coalesce_alias.as_ref() == "any_id"
    ));
}
