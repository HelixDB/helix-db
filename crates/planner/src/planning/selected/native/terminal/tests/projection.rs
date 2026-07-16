use super::support;
use crate::{ir, logical};
use helix_ast::projection::{BindingProjection, Projection};
use helix_ast::traversal::AstNode;

#[test]
fn native_terminals_lower_projection_contracts() {
    [
        AstNode::Count {
            input: support::node_source(),
        },
        AstNode::Exists {
            input: support::node_source(),
        },
        AstNode::Id {
            input: support::node_source(),
        },
        AstNode::Label {
            input: support::node_source(),
        },
        AstNode::EdgeProperties {
            input: support::node_source(),
        },
    ]
    .into_iter()
    .for_each(|root| {
        let expr = support::lower(&root)
            .unwrap()
            .expect_native("terminal should lower");
        assert!(matches!(expr, logical::LogicalExpr::StreamProject(_)));
    });
}

#[test]
fn native_terminals_preserve_projection_payloads() {
    let values = support::lower(&AstNode::Values {
        input: support::node_source(),
        properties: vec!["name".to_owned()],
    })
    .unwrap()
    .expect_native("values is native");
    assert!(matches!(
        values,
        logical::LogicalExpr::StreamProject(project)
            if matches!(project.projection(), ir::ProjectionPlan::Values(properties)
                if properties.as_ref()[0].as_ref() == "name")
    ));

    let bindings = support::lower(&AstNode::ProjectBindings {
        input: support::node_source(),
        projections: vec![BindingProjection::current("$id", "id")],
        distinct: true,
    })
    .unwrap()
    .expect_native("binding projection is native");
    assert!(matches!(
        bindings,
        logical::LogicalExpr::StreamProject(project)
            if matches!(
                project.projection(),
                ir::ProjectionPlan::ProjectBindings {
                    dedup: ir::ProjectionDedupMode::Distinct,
                    ..
                }
            )
    ));

    let project = support::lower(&AstNode::Project {
        input: support::node_source(),
        projections: vec![Projection::property("name", "display")],
    })
    .unwrap()
    .expect_native("projection is native");
    assert!(matches!(
        project,
        logical::LogicalExpr::StreamProject(project)
            if matches!(project.projection(), ir::ProjectionPlan::Project(_))
    ));
}
