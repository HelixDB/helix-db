use super::*;
use crate::{context, error, ir, logical};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::value::{PropertyInput, PropertyValue};

fn nodes() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

fn mutation_plan(root: AstNode) -> ir::MutationPlan<logical::LogicalExpr> {
    let root = input_mutation_from_ast(
        &context::PlannerContext::default(),
        &root,
        NativeAstScope::QueryRoot,
    )
    .unwrap();
    let InputMutationRoot::Mutation(mutation) = root else {
        panic!("input mutation is selectable");
    };
    mutation.plan().clone()
}

#[test]
fn scoped_roots_keep_input_mutations_inside_cascades() {
    let plan = mutation_plan(AstNode::SetProperty {
        input: nodes(),
        name: "active".to_owned(),
        value: PropertyInput::from(PropertyValue::Bool(true)),
    });

    assert!(matches!(
        &plan,
        ir::MutationPlan::SetProperty { input, name, .. }
            if name.as_ref() == "active"
                && matches!(input.as_ref(), logical::LogicalExpr::AccessPath(_))
    ));
}

#[test]
fn scoped_roots_cover_input_mutation_variants() {
    assert!(matches!(
        mutation_plan(AstNode::AddN {
            input: Some(nodes()),
            label: "Audit".to_owned(),
            properties: vec![("kind".to_owned(), PropertyInput::from("login"))],
        }),
        ir::MutationPlan::AddNode {
            input: ir::MutationInput::FromInput { .. },
            label,
            properties,
        } if label.as_ref() == "Audit" && properties.as_ref().len() == 1
    ));

    assert!(matches!(
        mutation_plan(AstNode::AddE {
            input: nodes(),
            label: "FOLLOWS".to_owned(),
            to: NodeRef::param("targets"),
            properties: vec![("since".to_owned(), PropertyInput::from(2024))],
        }),
        ir::MutationPlan::AddEdge {
            input,
            label,
            to: ir::NodeTargetPlan::FromParam { param },
            properties,
        } if matches!(input.as_ref(), logical::LogicalExpr::AccessPath(_))
            && label.as_ref() == "FOLLOWS"
            && param.as_ref() == "targets"
            && properties.as_ref().len() == 1
    ));

    assert!(matches!(
        mutation_plan(AstNode::RemoveProperty {
            input: nodes(),
            name: "stale".to_owned(),
        }),
        ir::MutationPlan::RemoveProperty { name, .. } if name.as_ref() == "stale"
    ));

    assert!(matches!(
        mutation_plan(AstNode::Drop { input: nodes() }),
        ir::MutationPlan::Drop { input }
            if matches!(input.as_ref(), logical::LogicalExpr::AccessPath(_))
    ));

    assert!(matches!(
        mutation_plan(AstNode::DropEdge {
            input: nodes(),
            to: NodeRef::ids([2, 3]),
        }),
        ir::MutationPlan::DropEdge {
            to: ir::NodeTargetPlan::PointIds { ids },
            ..
        } if ids.as_ref() == [2, 3]
    ));

    assert!(matches!(
        mutation_plan(AstNode::DropEdgeLabeled {
            input: nodes(),
            to: NodeRef::var("targets"),
            label: "LIKES".to_owned(),
        }),
        ir::MutationPlan::DropEdgeLabeled {
            to: ir::NodeTargetPlan::FromVar { variable },
            label,
            ..
        } if variable.as_ref() == "targets" && label.as_ref() == "LIKES"
    ));

    assert!(matches!(
        mutation_plan(AstNode::DropEdgeById {
            input: Some(nodes()),
            edges: EdgeRef::var("edge_ids"),
        }),
        ir::MutationPlan::DropEdgeById {
            input: ir::MutationInput::FromInput { .. },
            edges: ir::EdgeTargetPlan::FromVar { variable },
        } if variable.as_ref() == "edge_ids"
    ));
}

#[test]
fn scoped_input_mutations_reject_source_only_and_invalid_payloads() {
    assert!(matches!(
        input_mutation_from_ast(
            &context::PlannerContext::default(),
            &AstNode::AddN {
                input: None,
                label: "Source".to_owned(),
                properties: Vec::new(),
            },
            NativeAstScope::QueryRoot,
        )
        .unwrap(),
        InputMutationRoot::SourceOnly
    ));
    assert!(matches!(
        input_mutation_from_ast(
            &context::PlannerContext::default(),
            &AstNode::DropEdgeById {
                input: None,
                edges: EdgeRef::ids([1]),
            },
            NativeAstScope::QueryRoot,
        )
        .unwrap(),
        InputMutationRoot::SourceOnly
    ));
    assert!(matches!(
        input_mutation_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Context,
            NativeAstScope::QueryRoot,
        )
        .unwrap(),
        InputMutationRoot::NotMutation
    ));

    assert!(matches!(
        input_mutation_from_ast(
            &context::PlannerContext::default(),
            &AstNode::AddE {
                input: nodes(),
                label: String::new(),
                to: NodeRef::all(),
                properties: Vec::new(),
            },
            NativeAstScope::QueryRoot,
        ),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        })
    ));
    assert!(matches!(
        input_mutation_from_ast(
            &context::PlannerContext::default(),
            &AstNode::RemoveProperty {
                input: nodes(),
                name: String::new(),
            },
            NativeAstScope::QueryRoot,
        ),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));
}
