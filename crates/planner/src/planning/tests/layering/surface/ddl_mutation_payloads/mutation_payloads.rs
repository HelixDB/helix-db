use super::super::*;

#[test]
fn mutation_nodes_edges_and_properties_preserve_inputs() {
    let add_without_input = mutation_of(AstNode::AddN {
        input: None,
        label: "User".to_string(),
        properties: vec![("name".to_string(), PropertyInput::from("alice"))],
    });
    assert!(matches!(
        add_without_input,
        ExecMutationPlan::AddNodeSource {
            label,
            properties,
        } if label == "User" && properties.as_ref().len() == 1
    ));

    let add_with_input = mutation_of(AstNode::AddN {
        input: Some(boxed(nodes_root())),
        label: "Audit".to_string(),
        properties: vec![("source".to_string(), PropertyInput::param("source"))],
    });
    assert!(matches!(
        add_with_input,
        ExecMutationPlan::AddNodeFromInput {
            label,
            ..
        } if label == "Audit"
    ));

    let add_edge = mutation_of(AstNode::AddE {
        input: boxed(nodes_root()),
        label: "FOLLOWS".to_string(),
        to: NodeRef::param("targets"),
        properties: vec![("since".to_string(), PropertyInput::from(2024))],
    });
    assert!(matches!(
        add_edge,
        ExecMutationPlan::AddEdge {
            label,
            to: NodeTargetPlan::FromParam { param },
            properties,
        } if label == "FOLLOWS" && param == "targets" && properties.as_ref().len() == 1
    ));

    assert!(matches!(
        mutation_of(AstNode::AddE {
            input: boxed(nodes_root()),
            label: "MENTIONS".to_string(),
            to: NodeRef::all(),
            properties: Vec::new(),
        }),
        ExecMutationPlan::AddEdge {
            to: NodeTargetPlan::All,
            ..
        }
    ));

    assert!(matches!(
        mutation_of(AstNode::SetProperty {
            input: boxed(nodes_root()),
            name: "active".to_string(),
            value: PropertyInput::from(true),
        }),
        ExecMutationPlan::SetProperty { name, value }
            if name.as_ref() == "active"
                && value == PropertyInputPlan::new(PropertyInput::from(true)).unwrap()
    ));

    assert!(matches!(
        mutation_of(AstNode::RemoveProperty {
            input: boxed(nodes_root()),
            name: "stale".to_string(),
        }),
        ExecMutationPlan::RemoveProperty { name } if name.as_ref() == "stale"
    ));
}

#[test]
fn mutation_drop_variants_preserve_targets() {
    assert!(matches!(
        mutation_of(AstNode::Drop {
            input: boxed(nodes_root())
        }),
        ExecMutationPlan::Drop
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdge {
            input: boxed(nodes_root()),
            to: NodeRef::ids([2u64, 3]),
        }),
        ExecMutationPlan::DropEdge {
            to: NodeTargetPlan::PointIds { ids },
        } if ids.as_ref() == [2, 3]
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdge {
            input: boxed(nodes_root()),
            to: NodeRef::ids(Vec::<u64>::new()),
        }),
        ExecMutationPlan::DropEdge {
            to: NodeTargetPlan::Empty,
        }
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdgeLabeled {
            input: boxed(nodes_root()),
            to: NodeRef::param("targets"),
            label: "LIKES".to_string(),
        }),
        ExecMutationPlan::DropEdgeLabeled {
            to: NodeTargetPlan::FromParam { param },
            label,
        } if param == "targets" && label == "LIKES"
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdgeLabeled {
            input: boxed(nodes_root()),
            to: NodeRef::var("target_nodes"),
            label: "MENTIONS".to_string(),
        }),
        ExecMutationPlan::DropEdgeLabeled {
            to: NodeTargetPlan::FromVar { variable },
            label,
        } if variable == "target_nodes" && label == "MENTIONS"
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdgeById {
            input: None,
            edges: EdgeRef::ids([8u64, 9]),
        }),
        ExecMutationPlan::DropEdgeByIdSource {
            edges: EdgeTargetPlan::PointIds { ids },
        } if ids.as_ref() == [8, 9]
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdgeById {
            input: None,
            edges: EdgeRef::ids(Vec::<u64>::new()),
        }),
        ExecMutationPlan::DropEdgeByIdSource {
            edges: EdgeTargetPlan::Empty,
        }
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdgeById {
            input: Some(boxed(nodes_root())),
            edges: EdgeRef::var("edge_ids"),
        }),
        ExecMutationPlan::DropEdgeByIdFromInput {
            edges: EdgeTargetPlan::FromVar { variable },
        } if variable == "edge_ids"
    ));

    assert!(matches!(
        mutation_of(AstNode::DropEdgeById {
            input: Some(boxed(nodes_root())),
            edges: EdgeRef::param("edge_ids"),
        }),
        ExecMutationPlan::DropEdgeByIdFromInput {
            edges: EdgeTargetPlan::FromParam { param },
        } if param == "edge_ids"
    ));
}
