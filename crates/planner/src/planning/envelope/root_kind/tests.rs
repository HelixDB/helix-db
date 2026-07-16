//! Root-kind classification tests for planner request envelopes.

use super::*;
use helix_ast::expr::{Predicate, StreamBound};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index::IndexSpec;
use helix_ast::projection::{BindingProjection, Projection};
use helix_ast::traversal::{AggregateFunction, Order, RepeatConfig, SubTraversal};
use helix_ast::value::PropertyInput;

fn input() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

fn predicate() -> Predicate {
    Predicate::eq("name", "alice")
}

#[test]
fn ast_root_kind_names_are_stable_for_all_ast_variants() {
    let cases = vec![
        (AstNode::Context, "context"),
        (
            AstNode::Nodes {
                reference: NodeRef::All,
            },
            "nodes",
        ),
        (
            AstNode::NodesWhere {
                predicate: predicate(),
            },
            "nodes_where",
        ),
        (
            AstNode::Edges {
                reference: EdgeRef::All,
            },
            "edges",
        ),
        (
            AstNode::EdgesWhere {
                predicate: predicate(),
            },
            "edges_where",
        ),
        (
            AstNode::VectorSearchNodes {
                label: "User".to_owned(),
                property: "embedding".to_owned(),
                tenant_value: None,
                query_vector: PropertyInput::param("vector"),
                k: StreamBound::literal(10),
            },
            "vector_search_nodes",
        ),
        (
            AstNode::TextSearchNodes {
                label: "User".to_owned(),
                property: "body".to_owned(),
                tenant_value: None,
                query_text: PropertyInput::param("query"),
                k: StreamBound::literal(10),
            },
            "text_search_nodes",
        ),
        (
            AstNode::VectorSearchEdges {
                label: "LIKES".to_owned(),
                property: "embedding".to_owned(),
                tenant_value: None,
                query_vector: PropertyInput::param("vector"),
                k: StreamBound::literal(10),
            },
            "vector_search_edges",
        ),
        (
            AstNode::TextSearchEdges {
                label: "LIKES".to_owned(),
                property: "body".to_owned(),
                tenant_value: None,
                query_text: PropertyInput::param("query"),
                k: StreamBound::literal(10),
            },
            "text_search_edges",
        ),
        (
            AstNode::Out {
                input: input(),
                label: Some("FOLLOWS".to_owned()),
            },
            "out",
        ),
        (
            AstNode::In {
                input: input(),
                label: Some("FOLLOWS".to_owned()),
            },
            "in",
        ),
        (
            AstNode::Both {
                input: input(),
                label: None,
            },
            "both",
        ),
        (
            AstNode::OutE {
                input: input(),
                label: Some("LIKES".to_owned()),
            },
            "out_e",
        ),
        (
            AstNode::InE {
                input: input(),
                label: Some("LIKES".to_owned()),
            },
            "in_e",
        ),
        (
            AstNode::BothE {
                input: input(),
                label: None,
            },
            "both_e",
        ),
        (
            AstNode::OutN {
                input: Box::new(AstNode::Edges {
                    reference: EdgeRef::All,
                }),
            },
            "out_n",
        ),
        (
            AstNode::InN {
                input: Box::new(AstNode::Edges {
                    reference: EdgeRef::All,
                }),
            },
            "in_n",
        ),
        (
            AstNode::OtherN {
                input: Box::new(AstNode::Edges {
                    reference: EdgeRef::All,
                }),
            },
            "other_n",
        ),
        (
            AstNode::Has {
                input: input(),
                property: "name".to_owned(),
                value: "alice".into(),
            },
            "has",
        ),
        (
            AstNode::HasLabel {
                input: input(),
                label: "User".to_owned(),
            },
            "has_label",
        ),
        (
            AstNode::HasKey {
                input: input(),
                property: "email".to_owned(),
            },
            "has_key",
        ),
        (
            AstNode::Where {
                input: input(),
                predicate: predicate(),
            },
            "where",
        ),
        (AstNode::Dedup { input: input() }, "dedup"),
        (
            AstNode::Within {
                input: input(),
                variable: "users".to_owned(),
            },
            "within",
        ),
        (
            AstNode::Without {
                input: input(),
                variable: "users".to_owned(),
            },
            "without",
        ),
        (
            AstNode::EdgeHas {
                input: input(),
                property: "weight".to_owned(),
                value: 1.into(),
            },
            "edge_has",
        ),
        (
            AstNode::EdgeHasLabel {
                input: input(),
                label: "LIKES".to_owned(),
            },
            "edge_has_label",
        ),
        (
            AstNode::Limit {
                input: input(),
                count: StreamBound::literal(5),
            },
            "limit",
        ),
        (
            AstNode::Skip {
                input: input(),
                count: StreamBound::literal(5),
            },
            "skip",
        ),
        (
            AstNode::Range {
                input: input(),
                start: StreamBound::literal(1),
                end: StreamBound::literal(5),
            },
            "range",
        ),
        (
            AstNode::As {
                input: input(),
                name: "u".to_owned(),
            },
            "as",
        ),
        (
            AstNode::Store {
                input: input(),
                name: "u".to_owned(),
            },
            "store",
        ),
        (
            AstNode::Select {
                input: input(),
                name: "u".to_owned(),
            },
            "select",
        ),
        (
            AstNode::Bind {
                input: input(),
                name: "u".to_owned(),
            },
            "bind",
        ),
        (
            AstNode::Inject {
                input: None,
                variable: "users".to_owned(),
            },
            "inject_source",
        ),
        (
            AstNode::Inject {
                input: Some(input()),
                variable: "users".to_owned(),
            },
            "inject",
        ),
        (AstNode::Count { input: input() }, "count"),
        (AstNode::Exists { input: input() }, "exists"),
        (AstNode::Id { input: input() }, "id"),
        (AstNode::Label { input: input() }, "label"),
        (
            AstNode::Values {
                input: input(),
                properties: vec!["name".to_owned()],
            },
            "values",
        ),
        (
            AstNode::ValueMap {
                input: input(),
                properties: Some(vec!["name".to_owned()]),
            },
            "value_map",
        ),
        (
            AstNode::Project {
                input: input(),
                projections: vec![Projection::property("name", "name")],
            },
            "project",
        ),
        (
            AstNode::ProjectBindings {
                input: input(),
                projections: vec![BindingProjection::current("$id", "id")],
                distinct: false,
            },
            "project_bindings",
        ),
        (
            AstNode::EdgeProperties { input: input() },
            "edge_properties",
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_equality("User", "email"),
                if_not_exists: false,
            },
            "create_index",
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_equality("User", "email"),
            },
            "drop_index",
        ),
        (
            AstNode::GetIndexOperation {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_owned(),
            },
            "get_index_operation",
        ),
        (
            AstNode::RetryIndexOperation {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_owned(),
            },
            "retry_index_operation",
        ),
        (
            AstNode::AbortIndexOperation {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_owned(),
            },
            "abort_index_operation",
        ),
        (
            AstNode::AddN {
                input: None,
                label: "User".to_owned(),
                properties: Vec::new(),
            },
            "add_node_source",
        ),
        (
            AstNode::AddN {
                input: Some(input()),
                label: "User".to_owned(),
                properties: Vec::new(),
            },
            "add_node",
        ),
        (
            AstNode::AddE {
                input: input(),
                label: "LIKES".to_owned(),
                to: NodeRef::id(1),
                properties: Vec::new(),
            },
            "add_edge",
        ),
        (
            AstNode::SetProperty {
                input: input(),
                name: "active".to_owned(),
                value: true.into(),
            },
            "set_property",
        ),
        (
            AstNode::RemoveProperty {
                input: input(),
                name: "active".to_owned(),
            },
            "remove_property",
        ),
        (AstNode::Drop { input: input() }, "drop"),
        (
            AstNode::DropEdge {
                input: input(),
                to: NodeRef::id(1),
            },
            "drop_edge",
        ),
        (
            AstNode::DropEdgeLabeled {
                input: input(),
                to: NodeRef::id(1),
                label: "LIKES".to_owned(),
            },
            "drop_edge_labeled",
        ),
        (
            AstNode::DropEdgeById {
                input: None,
                edges: EdgeRef::id(1),
            },
            "drop_edge_by_id_source",
        ),
        (
            AstNode::DropEdgeById {
                input: Some(input()),
                edges: EdgeRef::id(1),
            },
            "drop_edge_by_id",
        ),
        (
            AstNode::OrderBy {
                input: input(),
                property: "name".to_owned(),
                order: Order::Asc,
            },
            "order_by",
        ),
        (
            AstNode::OrderByMultiple {
                input: input(),
                orderings: vec![("name".to_owned(), Order::Asc)],
            },
            "order_by_multiple",
        ),
        (
            AstNode::Repeat {
                input: input(),
                config: RepeatConfig::new(SubTraversal::new()),
            },
            "repeat",
        ),
        (
            AstNode::Union {
                input: input(),
                traversals: vec![SubTraversal::new()],
            },
            "union",
        ),
        (
            AstNode::Choose {
                input: input(),
                condition: predicate(),
                then_traversal: SubTraversal::new(),
                else_traversal: Some(SubTraversal::new()),
            },
            "choose",
        ),
        (
            AstNode::Coalesce {
                input: input(),
                traversals: vec![SubTraversal::new()],
            },
            "coalesce",
        ),
        (
            AstNode::Optional {
                input: input(),
                traversal: SubTraversal::new(),
            },
            "optional",
        ),
        (
            AstNode::Group {
                input: input(),
                property: "name".to_owned(),
            },
            "group",
        ),
        (
            AstNode::GroupCount {
                input: input(),
                property: "name".to_owned(),
            },
            "group_count",
        ),
        (
            AstNode::AggregateBy {
                input: input(),
                function: AggregateFunction::Count,
                property: "age".to_owned(),
            },
            "aggregate_by",
        ),
        (AstNode::Fold { input: input() }, "fold"),
        (AstNode::Unfold { input: input() }, "unfold"),
        (AstNode::Path { input: input() }, "path"),
        (AstNode::SimplePath { input: input() }, "simple_path"),
        (
            AstNode::WithSack {
                input: input(),
                initial: 0.into(),
            },
            "with_sack",
        ),
        (
            AstNode::SackSet {
                input: input(),
                property: "weight".to_owned(),
            },
            "sack_set",
        ),
        (
            AstNode::SackAdd {
                input: input(),
                property: "weight".to_owned(),
            },
            "sack_add",
        ),
        (AstNode::SackGet { input: input() }, "sack_get"),
    ];

    cases
        .into_iter()
        .for_each(|(root, expected)| assert_eq!(ast_root_kind(&root), expected));
}
