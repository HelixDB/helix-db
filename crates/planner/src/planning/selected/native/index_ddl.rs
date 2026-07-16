//! Native index-DDL root lowering.
//!
//! Index DDL is a root-only barrier contract. This module converts AST DDL
//! roots directly into `RootIndexDdl` so production selected planning can choose
//! the Cascades barrier implementation without first building a compatibility
//! physical tree.

use helix_ast::traversal::AstNode;

use crate::planning::index_ddl;
use crate::{error, ir, logical};

/// Native index-DDL root recognition result.
pub(super) enum NativeIndexDdlRoot {
    /// The AST root is a validated index-DDL barrier.
    Root(logical::RootIndexDdl),
    /// The AST root is not an index-DDL barrier.
    NotIndexDdl,
}

pub(super) fn native_index_ddl_from_ast(
    root: &AstNode,
) -> Result<NativeIndexDdlRoot, error::PlannerError> {
    match root {
        AstNode::CreateIndex {
            spec,
            if_not_exists,
        } => Ok(NativeIndexDdlRoot::Root(logical::RootIndexDdl::new(
            ir::IndexDdlPlan::Create {
                spec: index_ddl::index_ddl_create_spec(spec)?,
                mode: if *if_not_exists {
                    ir::IndexCreateMode::IfNotExists
                } else {
                    ir::IndexCreateMode::ErrorIfExists
                },
            },
        ))),
        AstNode::DropIndex { spec } => Ok(NativeIndexDdlRoot::Root(logical::RootIndexDdl::new(
            ir::IndexDdlPlan::Drop {
                spec: index_ddl::index_ddl_drop_spec(spec)?,
            },
        ))),
        AstNode::GetIndexOperation { operation_id } => Ok(NativeIndexDdlRoot::Root(
            logical::RootIndexDdl::new(ir::IndexDdlPlan::GetOperation {
                operation_id: ir::IndexOperationId::try_new(operation_id.clone())?,
            }),
        )),
        AstNode::RetryIndexOperation { operation_id } => Ok(NativeIndexDdlRoot::Root(
            logical::RootIndexDdl::new(ir::IndexDdlPlan::RetryOperation {
                operation_id: ir::IndexOperationId::try_new(operation_id.clone())?,
            }),
        )),
        AstNode::AbortIndexOperation { operation_id } => Ok(NativeIndexDdlRoot::Root(
            logical::RootIndexDdl::new(ir::IndexDdlPlan::AbortOperation {
                operation_id: ir::IndexOperationId::try_new(operation_id.clone())?,
            }),
        )),
        _ => Ok(NativeIndexDdlRoot::NotIndexDdl),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::index::IndexSpec;

    #[test]
    fn index_ddl_roots_lower_create_and_drop_payloads() {
        let create = native_index_ddl_from_ast(&AstNode::CreateIndex {
            spec: IndexSpec::node_unique_equality("Person", "email"),
            if_not_exists: true,
        })
        .unwrap();
        let NativeIndexDdlRoot::Root(create) = create else {
            panic!("create index is native");
        };
        assert!(matches!(
            create.plan(),
            ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeEquality { key, uniqueness },
                mode: ir::IndexCreateMode::IfNotExists,
            } if key.label.as_ref() == "Person"
                && key.property.as_ref() == "email"
                && matches!(uniqueness, crate::catalog::IndexUniqueness::Unique)
        ));

        let drop = native_index_ddl_from_ast(&AstNode::DropIndex {
            spec: IndexSpec::edge_range("Knows", "since"),
        })
        .unwrap();
        let NativeIndexDdlRoot::Root(drop) = drop else {
            panic!("drop index is native");
        };
        assert!(matches!(
            drop.plan(),
            ir::IndexDdlPlan::Drop {
                spec: ir::IndexDdlDropSpec::EdgeRange { key },
            } if key.label.as_ref() == "Knows" && key.property.as_ref() == "since"
        ));
    }

    #[test]
    fn index_ddl_roots_validate_names() {
        let create = native_index_ddl_from_ast(&AstNode::CreateIndex {
            spec: IndexSpec::node_equality("", "email"),
            if_not_exists: false,
        });
        assert!(matches!(
            create,
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Label
            })
        ));
        assert!(matches!(
            native_index_ddl_from_ast(&AstNode::Context).unwrap(),
            NativeIndexDdlRoot::NotIndexDdl
        ));
    }

    #[test]
    fn lifecycle_controls_lower_exact_ids_and_reject_noncanonical_input() {
        const OPERATION_ID: &str = "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001";

        for (root, expected) in [
            (
                AstNode::GetIndexOperation {
                    operation_id: OPERATION_ID.to_string(),
                },
                "get",
            ),
            (
                AstNode::RetryIndexOperation {
                    operation_id: OPERATION_ID.to_string(),
                },
                "retry",
            ),
            (
                AstNode::AbortIndexOperation {
                    operation_id: OPERATION_ID.to_string(),
                },
                "abort",
            ),
        ] {
            let NativeIndexDdlRoot::Root(lowered) =
                native_index_ddl_from_ast(&root).expect("canonical operation ID lowers")
            else {
                panic!("lifecycle control must be an index DDL root");
            };
            match (expected, lowered.plan()) {
                ("get", ir::IndexDdlPlan::GetOperation { operation_id })
                | ("retry", ir::IndexDdlPlan::RetryOperation { operation_id })
                | ("abort", ir::IndexDdlPlan::AbortOperation { operation_id }) => {
                    assert_eq!(operation_id.as_str(), OPERATION_ID);
                }
                _ => panic!("lifecycle control lowered to the wrong plan"),
            }
        }

        assert!(matches!(
            native_index_ddl_from_ast(&AstNode::GetIndexOperation {
                operation_id: OPERATION_ID.to_uppercase(),
            }),
            Err(error::PlannerError::InvalidIndexOperationId(_))
        ));
    }
}
