//! Tenant-plan validation for generation-owned physical search.

use helix_planner::ir;

use super::super::super::stream::ast_to_db_value;
use super::*;
use crate::config::{TextIndexDefinition, VectorIndexDefinition};

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::access::search) async fn search_tenant_value(
        &self,
        tenant: &ir::SearchTenantPlan,
    ) -> Result<Option<DbPropertyValue>> {
        match tenant {
            ir::SearchTenantPlan::Unscoped | ir::SearchTenantPlan::Scoped { .. } => Ok(None),
            ir::SearchTenantPlan::ScopedValue { value, .. } => {
                Ok(Some(self.search_property_input_value(value.value()).await?))
            }
        }
    }

    async fn search_property_input_value(
        &self,
        input: &ir::PropertyInputPlan,
    ) -> Result<DbPropertyValue> {
        match input {
            ir::PropertyInputPlan::Value(value) => Ok(ast_to_db_value(value.clone())),
            ir::PropertyInputPlan::Expr(expr) => {
                self.eval_expr(&search_eval_row(), expr.expr().expr()).await
            }
        }
    }
}

/// Validates that a vector query supplies exactly the configured tenant shape.
///
/// Physical V2 identity comes from the active generation and its typed mapping;
/// this boundary deliberately returns no display-name-derived storage key.
pub(in crate::execution::interpreter::access) fn validate_vector_search_tenant(
    definition: &VectorIndexDefinition,
    tenant: &ir::SearchTenantPlan,
    tenant_value: Option<&DbPropertyValue>,
) -> Result<()> {
    validate_search_tenant_shape(
        "vector",
        definition.label(),
        definition.property(),
        definition.tenant_property(),
        tenant,
        tenant_value,
    )
}

/// Validates that a text query supplies exactly the configured tenant shape.
///
/// The selected value is normalized into a typed V2 partition only after this
/// boundary proves that the planner property matches the canonical definition.
pub(in crate::execution::interpreter::access) fn validate_text_search_tenant(
    definition: &TextIndexDefinition,
    tenant: &ir::SearchTenantPlan,
    tenant_value: Option<&DbPropertyValue>,
) -> Result<()> {
    validate_search_tenant_shape(
        "text",
        definition.label(),
        definition.property(),
        definition.tenant_property(),
        tenant,
        tenant_value,
    )
}

/// Enforces the shared closed tenant-plan matrix for physical search families.
fn validate_search_tenant_shape(
    family: &'static str,
    label: &str,
    property_name: &str,
    tenant_property: Option<&str>,
    tenant: &ir::SearchTenantPlan,
    tenant_value: Option<&DbPropertyValue>,
) -> Result<()> {
    match (tenant_property, tenant, tenant_value) {
        (None, ir::SearchTenantPlan::Unscoped, None) => Ok(()),
        (None, ir::SearchTenantPlan::Scoped { property }, None) => {
            Err(HelixDbError::Query(format!(
                "{family} search for {label}:{property_name} is not tenant-scoped by '{}'",
                property
            )))
        }
        (None, ir::SearchTenantPlan::ScopedValue { property, .. }, Some(_)) => {
            Err(HelixDbError::Query(format!(
                "{family} search for {label}:{property_name} does not support tenant value for '{}'",
                property
            )))
        }
        (Some(tenant_property), ir::SearchTenantPlan::Scoped { property }, None)
            if tenant_property == property.as_ref() =>
        {
            Err(HelixDbError::Query(format!(
                "{family} search for {label}:{property_name} requires tenant value for partition property '{tenant_property}'",
            )))
        }
        (Some(tenant_property), ir::SearchTenantPlan::ScopedValue { property, .. }, Some(_))
            if tenant_property == property.as_ref() =>
        {
            Ok(())
        }
        (Some(tenant_property), ir::SearchTenantPlan::Unscoped, None) => {
            Err(HelixDbError::Query(format!(
                "{family} search for {label}:{property_name} requires tenant value for partition property '{tenant_property}'",
            )))
        }
        (Some(tenant_property), ir::SearchTenantPlan::Scoped { property }, None)
        | (Some(tenant_property), ir::SearchTenantPlan::ScopedValue { property, .. }, Some(_)) => {
            Err(HelixDbError::Query(format!(
                "{family} search for {label}:{property_name} is scoped by '{tenant_property}' not '{property}'",
            )))
        }
        (_, ir::SearchTenantPlan::ScopedValue { .. }, None)
        | (_, ir::SearchTenantPlan::Unscoped, Some(_))
        | (_, ir::SearchTenantPlan::Scoped { .. }, Some(_)) => Err(
            HelixDbError::InvariantViolation("inconsistent search tenant evaluation".to_string()),
        ),
    }
}

pub(in crate::execution::interpreter::access::search) fn search_eval_row() -> ExecutionRow {
    ExecutionRow::empty()
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Expr;
    use helix_planner::context;

    use super::super::super::super::test_support;
    use super::*;
    use crate::{config, search};
    use helix_ast::value::PropertyValue;

    fn unscoped_definition() -> VectorIndexDefinition {
        config::VectorIndexDefinition::new_node(
            "Doc",
            "embedding",
            2,
            search::vector::VectorDistanceMetric::Cosine,
        )
        .expect("valid vector index definition")
    }

    fn scoped_definition() -> VectorIndexDefinition {
        unscoped_definition()
            .with_tenant_property("tenant_id")
            .expect("valid tenant property")
    }

    fn text_definition(tenant_property: Option<&str>) -> TextIndexDefinition {
        let definition = config::TextIndexDefinition::new_node("Doc", "body")
            .expect("valid text index definition");
        match tenant_property {
            Some(property) => definition
                .with_tenant_property(property)
                .expect("valid text tenant property"),
            None => definition,
        }
    }

    fn tenant_value_plan(value: PropertyValue) -> ir::SearchTenantValuePlan {
        ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(value))
            .expect("valid tenant value")
    }

    #[test]
    fn vector_tenant_validation_accepts_unscoped_definition_without_tenant() {
        let definition = unscoped_definition();

        validate_vector_search_tenant(&definition, &ir::SearchTenantPlan::Unscoped, None).unwrap();
    }

    #[test]
    fn vector_tenant_validation_rejects_payloads_for_unscoped_definition() {
        let definition = unscoped_definition();
        let tenant_value = tenant_value_plan(PropertyValue::from("acme"));
        let value = DbPropertyValue::String("acme".to_string());

        assert!(validate_vector_search_tenant(
            &definition,
            &ir::SearchTenantPlan::Scoped {
                property: ir::NonEmptyString::from_static("tenant_id"),
            },
            None,
        )
        .is_err());
        assert!(matches!(
            validate_vector_search_tenant(
                &definition,
                &ir::SearchTenantPlan::ScopedValue {
                    property: ir::NonEmptyString::from_static("tenant_id"),
                    value: tenant_value_plan(PropertyValue::from("acme")),
                },
                None,
            ),
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(matches!(
            validate_vector_search_tenant(
                &definition,
                &ir::SearchTenantPlan::Unscoped,
                Some(&value),
            ),
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(matches!(
            validate_vector_search_tenant(
                &definition,
                &ir::SearchTenantPlan::Scoped {
                    property: ir::NonEmptyString::from_static("tenant_id"),
                },
                Some(&value),
            ),
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(validate_vector_search_tenant(
            &definition,
            &ir::SearchTenantPlan::ScopedValue {
                property: ir::NonEmptyString::from_static("tenant_id"),
                value: tenant_value,
            },
            Some(&value),
        )
        .is_err());
    }

    #[test]
    fn vector_tenant_validation_accepts_matching_scoped_value() {
        let definition = scoped_definition();
        let tenant_value = tenant_value_plan(PropertyValue::from("acme"));
        let value = DbPropertyValue::String("acme".to_string());

        validate_vector_search_tenant(
            &definition,
            &ir::SearchTenantPlan::ScopedValue {
                property: ir::NonEmptyString::from_static("tenant_id"),
                value: tenant_value,
            },
            Some(&value),
        )
        .unwrap();
    }

    #[test]
    fn vector_tenant_validation_rejects_missing_or_wrong_scoped_tenant() {
        let definition = scoped_definition();
        let wrong_value = tenant_value_plan(PropertyValue::from("acme"));

        assert!(
            validate_vector_search_tenant(&definition, &ir::SearchTenantPlan::Unscoped, None)
                .is_err()
        );
        assert!(validate_vector_search_tenant(
            &definition,
            &ir::SearchTenantPlan::Scoped {
                property: ir::NonEmptyString::from_static("tenant_id"),
            },
            None,
        )
        .is_err());
        assert!(validate_vector_search_tenant(
            &definition,
            &ir::SearchTenantPlan::ScopedValue {
                property: ir::NonEmptyString::from_static("account_id"),
                value: wrong_value,
            },
            Some(&DbPropertyValue::String("acme".to_string())),
        )
        .is_err());
        assert!(validate_vector_search_tenant(
            &definition,
            &ir::SearchTenantPlan::Scoped {
                property: ir::NonEmptyString::from_static("account_id"),
            },
            None,
        )
        .is_err());
    }

    #[test]
    fn text_tenant_validation_accepts_only_the_canonical_shape() {
        let unscoped = text_definition(None);
        validate_text_search_tenant(&unscoped, &ir::SearchTenantPlan::Unscoped, None)
            .expect("unscoped definition accepts an unscoped search");

        let scoped = text_definition(Some("tenant_id"));
        let value = DbPropertyValue::String("acme".to_string());
        validate_text_search_tenant(
            &scoped,
            &ir::SearchTenantPlan::ScopedValue {
                property: ir::NonEmptyString::from_static("tenant_id"),
                value: tenant_value_plan(PropertyValue::from("acme")),
            },
            Some(&value),
        )
        .expect("matching text tenant property and value are accepted");

        assert!(validate_text_search_tenant(
            &scoped,
            &ir::SearchTenantPlan::ScopedValue {
                property: ir::NonEmptyString::from_static("account_id"),
                value: tenant_value_plan(PropertyValue::from("acme")),
            },
            Some(&value),
        )
        .is_err());
        assert!(validate_text_search_tenant(
            &scoped,
            &ir::SearchTenantPlan::Scoped {
                property: ir::NonEmptyString::from_static("tenant_id"),
            },
            None,
        )
        .is_err());
    }

    #[tokio::test]
    async fn search_tenant_value_evaluates_runtime_expression() {
        let db = test_support::open_db("search-tenant-expression").await;
        let tenant = test_support::name("tenant");
        let context = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(tenant.clone(), PropertyValue::from("acme")),
        );
        let plan = ir::SearchTenantPlan::ScopedValue {
            property: ir::NonEmptyString::from_static("tenant_id"),
            value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Expr(
                ir::PropertyInputExprPlan::new(Expr::param(tenant.as_ref()))
                    .expect("valid tenant expression"),
            ))
            .expect("valid tenant value"),
        };

        assert_eq!(
            context
                .search_tenant_value(&plan)
                .await
                .expect("tenant expression evaluates"),
            Some(DbPropertyValue::String("acme".to_string()))
        );
    }
}
