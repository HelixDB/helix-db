//! Search index execution metadata contracts.

use super::input;
use crate::{catalog, error, ir};

/// Search tenant scope selected for an executable search index access.
#[derive(Debug, Clone, PartialEq)]
pub enum SearchTenantInput {
    Unscoped,
    Scoped {
        property: ir::NonEmptyString,
    },
    ScopedValue {
        property: ir::NonEmptyString,
        value: ir::SearchTenantValuePlan,
    },
}

/// Build search index execution metadata from a validated tenant scope.
///
/// ```
/// use helix_planner::{ir, planning::search};
///
/// let index = search::search_index_plan(
///     ir::NonEmptyString::new("idx").unwrap(),
///     search::SearchTenantInput::Unscoped,
/// );
/// assert!(matches!(index.tenant, ir::SearchTenantPlan::Unscoped));
/// ```
pub fn search_index_plan(
    index_id: ir::NonEmptyString,
    tenant: SearchTenantInput,
) -> ir::SearchIndexPlan {
    match tenant {
        SearchTenantInput::ScopedValue { property, value } => ir::SearchIndexPlan {
            index_id,
            tenant: ir::SearchTenantPlan::ScopedValue { property, value },
        },
        SearchTenantInput::Scoped { property } => ir::SearchIndexPlan {
            index_id,
            tenant: ir::SearchTenantPlan::Scoped { property },
        },
        SearchTenantInput::Unscoped => ir::SearchIndexPlan {
            index_id,
            tenant: ir::SearchTenantPlan::Unscoped,
        },
    }
}

pub(super) fn tenant_input_from_scope(
    index_id: &ir::NonEmptyString,
    scope: catalog::SearchIndexScope,
    tenant_value: input::SearchTenantValueInput,
    kind: catalog::SearchIndexKind,
) -> Result<SearchTenantInput, error::PlannerError> {
    match (scope, tenant_value) {
        (
            catalog::SearchIndexScope::Tenant { property },
            input::SearchTenantValueInput::Value(value),
        ) => Ok(SearchTenantInput::ScopedValue {
            property,
            value: ir::SearchTenantValuePlan::new(value).map_err(|_| {
                error::PlannerError::InvalidSearchTenantValue {
                    kind,
                    expected: error::SearchTenantValueExpected::NonNullPropertyInput,
                }
            })?,
        }),
        (
            catalog::SearchIndexScope::Tenant { property },
            input::SearchTenantValueInput::NotProvided,
        ) => Ok(SearchTenantInput::Scoped { property }),
        (catalog::SearchIndexScope::Unscoped, input::SearchTenantValueInput::NotProvided) => {
            Ok(SearchTenantInput::Unscoped)
        }
        (catalog::SearchIndexScope::Unscoped, input::SearchTenantValueInput::Value(_)) => {
            Err(error::PlannerError::InvalidSearchTenant {
                kind,
                index_id: index_id.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::value::{PropertyInput, PropertyValue};

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn tenant_value(value: PropertyInput) -> input::SearchTenantValueInput {
        input::SearchTenantValueInput::Value(ir::PropertyInputPlan::new(value).unwrap())
    }

    #[test]
    fn search_index_plan_builds_only_validated_tenant_states() {
        let unscoped = search_index_plan(name("idx"), SearchTenantInput::Unscoped);
        assert!(matches!(unscoped.tenant, ir::SearchTenantPlan::Unscoped));

        let scoped = search_index_plan(
            name("idx"),
            SearchTenantInput::Scoped {
                property: name("tenant_id"),
            },
        );
        assert!(matches!(
            scoped.tenant,
            ir::SearchTenantPlan::Scoped { property } if property.as_ref() == "tenant_id"
        ));
    }

    #[test]
    fn tenant_input_from_scope_rejects_invalid_scope_value_pairs() {
        let index_id = name("idx");
        assert!(matches!(
            tenant_input_from_scope(
                &index_id,
                catalog::SearchIndexScope::Unscoped,
                input::SearchTenantValueInput::NotProvided,
                catalog::SearchIndexKind::Text,
            )
            .unwrap(),
            SearchTenantInput::Unscoped
        ));
        assert!(matches!(
            tenant_input_from_scope(
                &index_id,
                catalog::SearchIndexScope::Tenant {
                    property: name("tenant_id")
                },
                input::SearchTenantValueInput::NotProvided,
                catalog::SearchIndexKind::Text,
            )
            .unwrap(),
            SearchTenantInput::Scoped { property } if property.as_ref() == "tenant_id"
        ));
        assert!(matches!(
            tenant_input_from_scope(
                &index_id,
                catalog::SearchIndexScope::Tenant {
                    property: name("tenant_id")
                },
                tenant_value(PropertyInput::from("tenant-a")),
                catalog::SearchIndexKind::Text,
            )
            .unwrap(),
            SearchTenantInput::ScopedValue { property, .. } if property.as_ref() == "tenant_id"
        ));

        assert!(matches!(
            tenant_input_from_scope(
                &index_id,
                catalog::SearchIndexScope::Unscoped,
                tenant_value(PropertyInput::from("tenant-a")),
                catalog::SearchIndexKind::Text,
            ),
            Err(error::PlannerError::InvalidSearchTenant { .. })
        ));
        assert!(matches!(
            tenant_input_from_scope(
                &index_id,
                catalog::SearchIndexScope::Tenant {
                    property: name("tenant_id")
                },
                tenant_value(PropertyInput::from(PropertyValue::Null)),
                catalog::SearchIndexKind::Vector,
            ),
            Err(error::PlannerError::InvalidSearchTenantValue {
                kind: catalog::SearchIndexKind::Vector,
                expected: error::SearchTenantValueExpected::NonNullPropertyInput,
            })
        ));
    }
}
