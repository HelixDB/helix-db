use helix_ast::expr::StreamBound;
use helix_ast::value::{PropertyInput, PropertyValue};

use super::*;
use crate::{catalog, error, ir};

fn search_key(
    element: catalog::ElementKind,
    label: &str,
    property: &str,
) -> catalog::SearchIndexKey {
    catalog::SearchIndexKey::try_new(element, label, property).unwrap()
}

fn indexes() -> catalog::IndexCatalogSnapshot {
    catalog::IndexCatalogSnapshot::default()
        .with_vector(
            search_key(catalog::ElementKind::Node, "Doc", "embedding"),
            catalog::SearchIndexScope::Unscoped,
        )
        .with_text(
            search_key(catalog::ElementKind::Node, "Doc", "body"),
            catalog::SearchIndexScope::Unscoped,
        )
        .with_vector(
            search_key(catalog::ElementKind::Edge, "MENTIONS", "embedding"),
            catalog::SearchIndexScope::Unscoped,
        )
        .with_text(
            search_key(catalog::ElementKind::Edge, "MENTIONS", "body"),
            catalog::SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
}

fn vector_input() -> PropertyInput {
    PropertyInput::from(PropertyValue::F32Array(vec![0.1, 0.2]))
}

#[test]
fn search_access_builders_validate_all_element_and_search_families() {
    let node_vector = node_vector_search(
        &indexes(),
        "Doc",
        "embedding",
        None,
        &vector_input(),
        &StreamBound::Literal(10),
    )
    .unwrap();
    assert!(matches!(
        node_vector.plan,
        ir::NodeAccessPlan::VectorSearch { .. }
    ));
    assert_eq!(node_vector.index_id.as_ref(), "vector:node:Doc:embedding");

    let node_text = node_text_search(
        &indexes(),
        "Doc",
        "body",
        None,
        &PropertyInput::from("needle"),
        &StreamBound::Literal(4),
    )
    .unwrap();
    assert!(matches!(
        node_text.plan,
        ir::NodeAccessPlan::TextSearch { .. }
    ));
    assert_eq!(node_text.index_id.as_ref(), "text:node:Doc:body");

    let edge_vector = edge_vector_search(
        &indexes(),
        "MENTIONS",
        "embedding",
        None,
        &vector_input(),
        &StreamBound::Literal(2),
    )
    .unwrap();
    assert!(matches!(
        edge_vector.plan,
        ir::EdgeAccessPlan::VectorSearch { .. }
    ));
    assert_eq!(
        edge_vector.index_id.as_ref(),
        "vector:edge:MENTIONS:embedding"
    );

    let edge_text = edge_text_search(
        &indexes(),
        "MENTIONS",
        "body",
        Some(&PropertyInput::from("tenant-a")),
        &PropertyInput::from("needle"),
        &StreamBound::Literal(3),
    )
    .unwrap();
    match edge_text.plan {
        ir::EdgeAccessPlan::TextSearch { index, .. } => {
            assert_eq!(edge_text.index_id.as_ref(), "text:edge:MENTIONS:body");
            assert!(matches!(
                index.tenant,
                ir::SearchTenantPlan::ScopedValue { .. }
            ));
        }
        other => panic!("expected edge text search, got {other:?}"),
    }
}

#[test]
fn search_access_builders_report_catalog_and_name_errors() {
    let missing = node_text_search(
        &catalog::IndexCatalogSnapshot::default(),
        "Doc",
        "body",
        None,
        &PropertyInput::from("needle"),
        &StreamBound::Literal(3),
    )
    .unwrap_err();
    assert!(matches!(
        missing,
        error::PlannerError::MissingSearchIndex {
            element: catalog::ElementKind::Node,
            kind: catalog::SearchIndexKind::Text,
            ..
        }
    ));

    let invalid_name = edge_vector_search(
        &indexes(),
        "",
        "embedding",
        None,
        &vector_input(),
        &StreamBound::Literal(3),
    )
    .unwrap_err();
    assert!(matches!(
        invalid_name,
        error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        }
    ));
}
