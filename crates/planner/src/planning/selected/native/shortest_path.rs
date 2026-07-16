//! Native shortest-path root lowering.

use std::num::NonZeroUsize;

use helix_ast::traversal::AstNode;

use super::names;
use crate::{error, ir, logical};

/// Native shortest-path root recognition result.
pub(super) enum NativeShortestPathRoot {
    /// The AST root is a validated shortest-path query.
    Root(logical::RootShortestPath),
    /// The AST root is not a shortest-path query.
    NotShortestPath,
}

pub(super) fn native_shortest_path_from_ast(
    root: &AstNode,
) -> Result<NativeShortestPathRoot, error::PlannerError> {
    let AstNode::ShortestPath {
        source,
        target,
        label,
        direction,
        max_depth,
    } = root
    else {
        return Ok(NativeShortestPathRoot::NotShortestPath);
    };

    let Some(max_depth) = NonZeroUsize::new(*max_depth) else {
        return Err(error::PlannerError::InvalidShortestPathCount {
            field: error::ShortestPathCountField::MaxDepth,
            actual: *max_depth,
        });
    };

    Ok(NativeShortestPathRoot::Root(
        logical::RootShortestPath::new(ir::ShortestPathPlan {
            source: source.clone(),
            target: target.clone(),
            label: label
                .as_ref()
                .map(|label| names::non_empty(label.clone(), ir::NameField::Label))
                .transpose()?,
            direction: *direction,
            max_depth,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::graph::NodeRef;
    use helix_ast::traversal::ShortestPathDirection;

    #[test]
    fn shortest_path_roots_lower_validated_payloads() {
        let root = native_shortest_path_from_ast(&AstNode::ShortestPath {
            source: NodeRef::id(1),
            target: NodeRef::param("target"),
            label: Some("KNOWS".to_string()),
            direction: ShortestPathDirection::Both,
            max_depth: 3,
        })
        .unwrap();

        let NativeShortestPathRoot::Root(root) = root else {
            panic!("shortest path is native");
        };
        assert_eq!(root.plan().source, NodeRef::id(1));
        assert_eq!(root.plan().target, NodeRef::param("target"));
        assert_eq!(root.plan().label.as_ref().unwrap().as_ref(), "KNOWS");
        assert_eq!(root.plan().direction, ShortestPathDirection::Both);
        assert_eq!(root.plan().max_depth.get(), 3);
    }

    #[test]
    fn shortest_path_roots_validate_label_and_depth() {
        assert!(matches!(
            native_shortest_path_from_ast(&AstNode::ShortestPath {
                source: NodeRef::id(1),
                target: NodeRef::id(2),
                label: Some(String::new()),
                direction: ShortestPathDirection::Out,
                max_depth: 3,
            }),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Label
            })
        ));

        assert!(matches!(
            native_shortest_path_from_ast(&AstNode::ShortestPath {
                source: NodeRef::id(1),
                target: NodeRef::id(2),
                label: None,
                direction: ShortestPathDirection::Out,
                max_depth: 0,
            }),
            Err(error::PlannerError::InvalidShortestPathCount {
                field: error::ShortestPathCountField::MaxDepth,
                actual: 0,
            })
        ));
        assert!(matches!(
            native_shortest_path_from_ast(&AstNode::Context).unwrap(),
            NativeShortestPathRoot::NotShortestPath
        ));
    }
}
