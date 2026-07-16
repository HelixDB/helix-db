use helix_ast::index;

use crate::{catalog, error, ir};

pub(super) fn scoped_property_key(
    label: &str,
    property: &str,
) -> Result<catalog::ScopedPropertyKey, error::PlannerError> {
    Ok(catalog::ScopedPropertyKey::new(
        non_empty(label, ir::NameField::Label)?,
        non_empty(property, ir::NameField::Property)?,
    ))
}

pub(super) fn scoped_property_direction_key(
    label: &str,
    property: &str,
    direction: index::RangeIndexDirection,
) -> Result<catalog::ScopedPropertyDirectionKey, error::PlannerError> {
    Ok(catalog::ScopedPropertyDirectionKey::new(
        non_empty(label, ir::NameField::Label)?,
        non_empty(property, ir::NameField::Property)?,
        direction,
    ))
}

pub(super) fn search_scope(
    tenant_property: &Option<String>,
) -> Result<catalog::SearchIndexScope, error::PlannerError> {
    catalog::SearchIndexScope::try_new(tenant_property.clone()).ok_or(
        error::PlannerError::InvalidEmptyName {
            field: ir::NameField::TenantProperty,
        },
    )
}

pub(super) fn vector_index_metric(metric: index::VectorDistanceMetric) -> ir::VectorIndexMetric {
    match metric {
        index::VectorDistanceMetric::Cosine => ir::VectorIndexMetric::Cosine,
        index::VectorDistanceMetric::Euclidean => ir::VectorIndexMetric::Euclidean,
        index::VectorDistanceMetric::Manhattan => ir::VectorIndexMetric::Manhattan,
    }
}

fn non_empty(value: &str, field: ir::NameField) -> Result<ir::NonEmptyString, error::PlannerError> {
    ir::NonEmptyString::new(value).ok_or(error::PlannerError::InvalidEmptyName { field })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_property_helpers_validate_label_before_property() {
        assert!(matches!(
            scoped_property_key("", ""),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Label
            })
        ));
        assert!(matches!(
            scoped_property_direction_key("User", "", index::RangeIndexDirection::Asc),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Property
            })
        ));

        let key =
            scoped_property_direction_key("User", "age", index::RangeIndexDirection::Desc).unwrap();
        assert_eq!(key.label.as_ref(), "User");
        assert_eq!(key.property.as_ref(), "age");
        assert_eq!(key.direction, index::RangeIndexDirection::Desc);
    }

    #[test]
    fn search_scope_validates_optional_tenant_property() {
        assert_eq!(
            search_scope(&None).unwrap(),
            catalog::SearchIndexScope::Unscoped
        );
        assert_eq!(
            search_scope(&Some("tenant_id".to_string())).unwrap(),
            catalog::SearchIndexScope::Tenant {
                property: ir::NonEmptyString::new("tenant_id").unwrap()
            }
        );
        assert!(matches!(
            search_scope(&Some(String::new())),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::TenantProperty
            })
        ));
    }

    #[test]
    fn vector_metric_mapping_is_explicit_for_every_ast_metric() {
        assert_eq!(
            vector_index_metric(index::VectorDistanceMetric::Cosine),
            ir::VectorIndexMetric::Cosine
        );
        assert_eq!(
            vector_index_metric(index::VectorDistanceMetric::Euclidean),
            ir::VectorIndexMetric::Euclidean
        );
        assert_eq!(
            vector_index_metric(index::VectorDistanceMetric::Manhattan),
            ir::VectorIndexMetric::Manhattan
        );
    }
}
