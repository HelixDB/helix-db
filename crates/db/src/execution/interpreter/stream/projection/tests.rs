mod bindings;
mod helpers;
mod rows;
mod scalar;

use std::collections::BTreeMap;

use helix_ast::expr::Expr;
use helix_ast::value::PropertyValue;

use super::super::super::test_support;
use super::*;

fn name(value: &str) -> ir::NonEmptyString {
    test_support::name(value)
}

fn object(value: ExecutionScalar) -> BTreeMap<String, DbPropertyValue> {
    let ExecutionScalar::Object(object) = value else {
        panic!("expected object projection scalar");
    };
    object
}

fn projection_items(items: Vec<ir::ProjectionItem>) -> ir::ProjectionItems {
    ir::ProjectionItems::new(
        ir::AtLeast::<_, 1>::try_from_vec(items).expect("test projection items are non-empty"),
    )
    .expect("test projection aliases are unique")
}

fn property_names(names: Vec<&str>) -> ir::PropertyNames {
    ir::PropertyNames::new(
        ir::AtLeast::<_, 1>::try_from_vec(names.into_iter().map(name).collect())
            .expect("test property list is non-empty"),
    )
    .expect("test property names are unique")
}

fn binding_projection_items(items: Vec<ir::BindingProjectionPlan>) -> ir::BindingProjectionItems {
    ir::BindingProjectionItems::new(
        ir::AtLeast::<_, 1>::try_from_vec(items)
            .expect("test binding projection list is non-empty"),
    )
    .expect("test binding projection aliases are unique")
}

fn binding_refs(items: Vec<ir::BindingValueRefPlan>) -> ir::AtLeast<ir::BindingValueRefPlan, 1> {
    ir::AtLeast::<_, 1>::try_from_vec(items).expect("test binding refs are non-empty")
}
