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
pub mod error_code;
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

        let query = query_entry(&batch.entries()[0]);
        assert!(matches!(query.root, AstNode::ValueMap { .. }));

        let json = sonic_rs::to_string(&QueryRequest::read(batch)).unwrap();
        assert!(json.contains(r#""root":{"value_map":{"input":{"limit""#));
        assert!(json.contains(r#""eq":{"left":{"property":"username"}"#));
        assert!(!json.contains("steps"));
    }

    fn node_search_distance(traversal: Traversal<OnNodes>) -> u8 {
        match traversal.into_ast() {
            AstNode::TextSearchNodes { fuzzy_distance, .. } => fuzzy_distance,
            other => panic!("expected a node text search, got {other:?}"),
        }
    }

    #[test]
    fn fuzzy_widens_the_search_it_follows() {
        assert_eq!(
            node_search_distance(g().text_search_nodes("Doc", "body", "helox", 10usize, None)),
            0,
            "a search that never asks for latitude does not get any"
        );
        assert_eq!(
            node_search_distance(
                g().text_search_nodes("Doc", "body", "helox", 10usize, None)
                    .fuzzy(1)
            ),
            1
        );
    }

    #[test]
    fn fuzzy_is_capped_at_what_the_index_can_be_walked_with() {
        assert_eq!(
            node_search_distance(
                g().text_search_nodes("Doc", "body", "helox", 10usize, None)
                    .fuzzy(9)
            ),
            MAX_FUZZY_DISTANCE
        );
    }

    #[test]
    fn an_exact_search_serialises_the_way_it_always_did() {
        let exact = read_batch()
            .var_as(
                "hits",
                g().text_search_nodes("Doc", "body", "helox", 10usize, None),
            )
            .returning(["hits"]);
        let json = sonic_rs::to_string(&QueryRequest::read(exact)).unwrap();
        assert!(
            !json.contains("fuzzy_distance"),
            "an older reader has to see exactly the document it saw before: {json}"
        );

        let widened = read_batch()
            .var_as(
                "hits",
                g().text_search_nodes("Doc", "body", "helox", 10usize, None)
                    .fuzzy(1),
            )
            .returning(["hits"]);
        let json = sonic_rs::to_string(&QueryRequest::read(widened)).unwrap();
        assert!(json.contains(r#""fuzzy_distance":1"#), "{json}");
    }

    #[test]
    #[should_panic(expected = "fuzzy can only follow a text search step")]
    fn fuzzy_refuses_a_step_that_has_no_edit_distance() {
        let _ = g().n_with_label("User").fuzzy(1);
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

        let query = query_entry(&batch.entries()[0]);
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
