use super::ids::{element_ids, NativeElementIds};
use super::*;
use crate::{catalog, error, ir, logical};
use helix_ast::graph::{EdgeRef, NodeRef};

#[test]
fn node_refs_lower_to_residual_free_access_paths() {
    let point = NativeAccessPath::nodes(&NodeRef::Ids(vec![7, 9]))
        .unwrap()
        .into_logical();
    assert!(matches!(
        point,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [7, 9])
    ));

    let empty = NativeAccessPath::nodes(&NodeRef::Ids(vec![]))
        .unwrap()
        .into_logical();
    assert!(matches!(
        empty,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));

    let dynamic = NativeAccessPath::nodes(&NodeRef::Param("node_ids".to_owned()))
        .unwrap()
        .into_logical();
    assert!(matches!(
        dynamic,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::FromParam { param } if param.as_ref() == "node_ids")
    ));
}

#[test]
fn edge_refs_lower_to_residual_free_access_paths() {
    let all = NativeAccessPath::edges(&EdgeRef::All)
        .unwrap()
        .into_logical();
    assert!(matches!(
        all,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::AllScan)
    ));

    let dynamic = NativeAccessPath::edges(&EdgeRef::Var("edges".to_owned()))
        .unwrap()
        .into_logical();
    assert!(matches!(
        dynamic,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::FromVar { variable } if variable.as_ref() == "edges")
    ));
}

#[test]
fn access_sources_validate_ids_and_names() {
    assert!(matches!(
        element_ids(&[], catalog::ElementKind::Node).unwrap(),
        NativeElementIds::EmptyReference
    ));
    assert!(matches!(
        element_ids(&[4], catalog::ElementKind::Node).unwrap(),
        NativeElementIds::NonEmpty(ids) if ids.as_ref() == [4]
    ));

    let duplicate = NativeAccessPath::nodes(&NodeRef::Ids(vec![4, 4]));
    assert!(matches!(
        duplicate,
        Err(error::PlannerError::DuplicateElementId {
            element: catalog::ElementKind::Node,
            id: 4
        })
    ));

    let empty_variable = NativeAccessPath::edges(&EdgeRef::Var(String::new()));
    assert!(matches!(
        empty_variable,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Variable
        })
    ));
}
