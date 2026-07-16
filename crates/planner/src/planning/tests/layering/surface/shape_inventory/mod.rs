//! Production executable-surface inventory tests.
//!
//! The cases live in read/write modules so adding an AST family updates a
//! focused contract list instead of growing one broad test body.

mod read;
mod support;
mod write;

use super::*;
use support::{read_root, write_root};

#[test]
fn executable_surface_accepts_representatives_for_every_supported_read_ast_shape() {
    for case in read::surface_cases() {
        let plan = read_root(case.root, &case.context)
            .unwrap_or_else(|err| panic!("read surface case {} failed: {err:?}", case.name));

        assert_eq!(plan.kind(), PlanKind::Read, "{}", case.name);
        assert!(!plan.steps().is_empty(), "{}", case.name);
        assert!(plan.metrics().memo_groups >= 1, "{}", case.name);
    }
}

#[test]
fn executable_surface_accepts_representatives_for_every_supported_write_ast_shape() {
    for case in write::surface_cases() {
        let plan = write_root(case.root, &case.context)
            .unwrap_or_else(|err| panic!("write surface case {} failed: {err:?}", case.name));

        assert_eq!(plan.kind(), PlanKind::Write, "{}", case.name);
        assert!(!plan.steps().is_empty(), "{}", case.name);
        assert!(plan.metrics().memo_groups >= 1, "{}", case.name);
    }
}

#[test]
fn executable_surface_keeps_query_root_context_unrepresentable() {
    let err = read_root(AstNode::Context, &PlannerContext::default()).unwrap_err();

    assert!(matches!(err, PlannerError::UnboundContext));
}
