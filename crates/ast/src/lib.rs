//! Shared Helix query AST.
//!
//! This crate owns the public JSON contract used by SDKs and the planner. The
//! wire format is an operation tree: each traversal builder call creates one
//! node, and chaining wraps the previous root as that node's `input`.
//!
//! Modules define the stable contract boundaries:
//!
//! - [`value`] contains JSON/property values and mutation inputs.
//! - [`expr`] contains predicates, expressions, and stream bounds.
//! - [`traversal`] contains the operation-tree AST and traversal builders.
//! - [`batch`] and [`query`] contain batch and request wire formats.
//!
//! ```
//! use helix_ast::prelude::*;
//!
//! let query = read_batch()
//!     .var_as(
//!         "users",
//!         g()
//!             .n_with_label("User")
//!             .where_(Predicate::eq("username", "alice"))
//!             .limit(1),
//!     )
//!     .returning(["users"]);
//! let json = sonic_rs::to_string(&query).unwrap();
//!
//! assert!(json.contains(r#""root":{"limit""#));
//! assert!(json.contains(r#""input":{"where""#));
//! assert!(json.contains(r#""eq":{"left":{"property":"username"}"#));
//! ```

#![deny(unsafe_code)]

pub mod batch;
pub mod expr;
pub mod graph;
pub mod index;
pub mod prelude;
pub mod projection;
pub mod query;
pub mod traversal;
pub mod value;

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    fn query_entry(entry: &BatchEntry) -> &NamedQuery {
        match entry {
            BatchEntry::Query(query) => query.as_ref(),
            BatchEntry::ForEach { .. } => panic!("expected query entry"),
        }
    }

    #[test]
    fn traversal_builds_nested_ast_json() {
        let batch = read_batch()
            .var_as(
                "users",
                g().n_with_label("User")
                    .where_(Predicate::eq("username", "alice"))
                    .limit(1usize)
                    .value_map(Some(vec!["$id", "username"])),
            )
            .returning(["users"]);

        let query = query_entry(&batch.entries[0]);
        assert!(matches!(query.root, AstNode::ValueMap { .. }));

        let json = sonic_rs::to_string(&QueryRequest::read(batch)).unwrap();
        assert!(json.contains(r#""root":{"value_map":{"input":{"limit""#));
        assert!(json.contains(r#""eq":{"left":{"property":"username"}"#));
        assert!(!json.contains("steps"));
    }

    #[test]
    fn sub_traversal_starts_from_context() {
        let traversal = g()
            .n(1u64)
            .union(vec![sub().out(Some("FOLLOWS")).limit(10usize)]);
        let AstNode::Union { traversals, .. } = traversal.into_ast() else {
            panic!("expected union");
        };
        let AstNode::Limit { input, .. } = &*traversals[0].root else {
            panic!("expected limit");
        };
        assert!(matches!(input.as_ref(), AstNode::Out { .. }));
    }

    #[test]
    fn shortest_path_builds_terminal_ast_json() {
        let batch = read_batch()
            .var_as(
                "path",
                g().shortest_path_with(
                    NodeRef::id(1),
                    NodeRef::param("target"),
                    Some("FOLLOWS"),
                    ShortestPathDirection::Both,
                    5,
                ),
            )
            .returning(["path"]);

        let query = query_entry(&batch.entries[0]);
        assert!(matches!(query.root, AstNode::ShortestPath { .. }));

        let json = sonic_rs::to_string(&QueryRequest::read(batch)).unwrap();
        assert!(json.contains(r#""shortest_path""#));
        assert!(json.contains(r#""direction":"both""#));
        assert!(json.contains(r#""max_depth":5"#));
        assert!(json.contains(r#""target":{"param":"target"}"#));
    }

    #[test]
    fn row_binding_builder_invariants() {
        assert!(std::panic::catch_unwind(|| {
            let _ = g().n(1u64).bind("");
        })
        .is_err());

        assert!(std::panic::catch_unwind(|| {
            let _ = BindingProjection::binding("service", "$id", "");
        })
        .is_err());

        assert!(std::panic::catch_unwind(|| {
            let _ = BindingProjection::coalesce(Vec::new(), "workload_id");
        })
        .is_err());

        assert!(std::panic::catch_unwind(|| {
            let _ = g().n(1u64).project_bindings(Vec::new());
        })
        .is_err());
    }
}
