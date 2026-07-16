use super::super::*;
use super::support;
use super::support::SurfaceCase;

pub(super) fn surface_cases() -> Vec<SurfaceCase> {
    let default_context = PlannerContext::default();
    let search_context = support::search_context();
    let tenant_search_context = support::tenant_search_context();

    vec![
        SurfaceCase {
            name: "nodes",
            root: nodes_root(),
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "nodes_where",
            root: AstNode::NodesWhere {
                predicate: Predicate::eq("active", true),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "edges",
            root: edges_root(),
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "edges_where",
            root: AstNode::EdgesWhere {
                predicate: Predicate::eq("weight", 1),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "vector_search_nodes",
            root: support::node_vector_search(),
            context: search_context.clone(),
        },
        SurfaceCase {
            name: "text_search_nodes",
            root: support::node_text_search(),
            context: search_context.clone(),
        },
        SurfaceCase {
            name: "vector_search_edges",
            root: support::edge_vector_search(),
            context: search_context.clone(),
        },
        SurfaceCase {
            name: "text_search_edges",
            root: support::edge_text_search(),
            context: search_context.clone(),
        },
        SurfaceCase {
            name: "tenant_vector_search_nodes",
            root: support::tenant_node_vector_search(),
            context: tenant_search_context.clone(),
        },
        SurfaceCase {
            name: "tenant_text_search_nodes",
            root: support::tenant_node_text_search(),
            context: tenant_search_context.clone(),
        },
        SurfaceCase {
            name: "tenant_vector_search_edges",
            root: support::tenant_edge_vector_search(),
            context: tenant_search_context.clone(),
        },
        SurfaceCase {
            name: "tenant_text_search_edges",
            root: support::tenant_edge_text_search(),
            context: tenant_search_context,
        },
        SurfaceCase {
            name: "out",
            root: AstNode::Out {
                input: boxed(nodes_root()),
                label: Some("FOLLOWS".to_owned()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "in",
            root: AstNode::In {
                input: boxed(nodes_root()),
                label: Some("MENTIONS".to_owned()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "both",
            root: AstNode::Both {
                input: boxed(nodes_root()),
                label: None,
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "out_e",
            root: AstNode::OutE {
                input: boxed(nodes_root()),
                label: Some("LIKES".to_owned()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "in_e",
            root: AstNode::InE {
                input: boxed(nodes_root()),
                label: Some("MENTIONS".to_owned()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "both_e",
            root: AstNode::BothE {
                input: boxed(nodes_root()),
                label: None,
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "out_n",
            root: AstNode::OutN {
                input: boxed(edges_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "in_n",
            root: AstNode::InN {
                input: boxed(edges_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "other_n",
            root: AstNode::OtherN {
                input: boxed(edges_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "has",
            root: AstNode::Has {
                input: boxed(nodes_root()),
                property: "active".to_owned(),
                value: PropertyValue::from(true),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "has_label",
            root: AstNode::HasLabel {
                input: boxed(nodes_root()),
                label: "User".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "has_key",
            root: AstNode::HasKey {
                input: boxed(nodes_root()),
                property: "email".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "where",
            root: AstNode::Where {
                input: boxed(nodes_root()),
                predicate: Predicate::is_not_null("email"),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "dedup",
            root: AstNode::Dedup {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "within",
            root: AstNode::Within {
                input: boxed(nodes_root()),
                variable: "allowed".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "without",
            root: AstNode::Without {
                input: boxed(nodes_root()),
                variable: "blocked".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "edge_has",
            root: AstNode::EdgeHas {
                input: boxed(edges_root()),
                property: "weight".to_owned(),
                value: PropertyInput::from(1),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "edge_has_label",
            root: AstNode::EdgeHasLabel {
                input: boxed(edges_root()),
                label: "MENTIONS".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "limit",
            root: AstNode::Limit {
                input: boxed(nodes_root()),
                count: StreamBound::Literal(2),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "skip",
            root: AstNode::Skip {
                input: boxed(nodes_root()),
                count: StreamBound::Literal(1),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "range",
            root: AstNode::Range {
                input: boxed(nodes_root()),
                start: StreamBound::Literal(1),
                end: StreamBound::Literal(3),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "limit_dynamic",
            root: AstNode::Limit {
                input: boxed(nodes_root()),
                count: StreamBound::expr(Expr::param("limit")),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "skip_dynamic",
            root: AstNode::Skip {
                input: boxed(nodes_root()),
                count: StreamBound::expr(Expr::param("offset")),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "range_dynamic",
            root: AstNode::Range {
                input: boxed(nodes_root()),
                start: StreamBound::expr(Expr::param("start")),
                end: StreamBound::expr(Expr::param("end")),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "as",
            root: AstNode::As {
                input: boxed(nodes_root()),
                name: "seen".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "store",
            root: AstNode::Store {
                input: boxed(nodes_root()),
                name: "seen".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "select",
            root: AstNode::Select {
                input: boxed(nodes_root()),
                name: "seen".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "bind",
            root: AstNode::Bind {
                input: boxed(nodes_root()),
                name: "row".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "inject_source",
            root: AstNode::Inject {
                input: None,
                variable: "seed".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "inject_stream",
            root: AstNode::Inject {
                input: Some(boxed(nodes_root())),
                variable: "seed".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "count",
            root: AstNode::Count {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "exists",
            root: AstNode::Exists {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "id",
            root: AstNode::Id {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "label",
            root: AstNode::Label {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "values",
            root: AstNode::Values {
                input: boxed(nodes_root()),
                properties: vec!["name".to_owned()],
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "value_map",
            root: AstNode::ValueMap {
                input: boxed(nodes_root()),
                properties: Some(vec!["name".to_owned()]),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "project",
            root: support::project_root(),
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "project_bindings",
            root: support::project_bindings_root(),
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "edge_properties",
            root: AstNode::EdgeProperties {
                input: boxed(edges_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "order_by",
            root: AstNode::OrderBy {
                input: boxed(nodes_root()),
                property: "age".to_owned(),
                order: Order::Asc,
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "order_by_multiple",
            root: AstNode::OrderByMultiple {
                input: boxed(nodes_root()),
                orderings: vec![
                    ("age".to_owned(), Order::Desc),
                    ("name".to_owned(), Order::Asc),
                ],
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "repeat",
            root: AstNode::Repeat {
                input: boxed(nodes_root()),
                config: RepeatConfig::new(sub().out(Some("FOLLOWS"))).times(2),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "union",
            root: AstNode::Union {
                input: boxed(nodes_root()),
                traversals: vec![sub().out(Some("FOLLOWS")), sub().in_(Some("MENTIONS"))],
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "choose",
            root: AstNode::Choose {
                input: boxed(nodes_root()),
                condition: Predicate::eq("active", true),
                then_traversal: sub().out(Some("FOLLOWS")),
                else_traversal: Some(sub().in_(Some("MENTIONS"))),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "coalesce",
            root: AstNode::Coalesce {
                input: boxed(nodes_root()),
                traversals: vec![sub().out(Some("FOLLOWS")), sub().in_(Some("MENTIONS"))],
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "optional",
            root: AstNode::Optional {
                input: boxed(nodes_root()),
                traversal: sub().both(Some("RELATED")),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "group",
            root: AstNode::Group {
                input: boxed(nodes_root()),
                property: "tenant".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "group_count",
            root: AstNode::GroupCount {
                input: boxed(nodes_root()),
                property: "status".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "aggregate_by",
            root: AstNode::AggregateBy {
                input: boxed(nodes_root()),
                function: AggregateFunction::Mean,
                property: "score".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "fold",
            root: AstNode::Fold {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "unfold",
            root: AstNode::Unfold {
                input: boxed(AstNode::Fold {
                    input: boxed(nodes_root()),
                }),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "path",
            root: AstNode::Path {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "simple_path",
            root: AstNode::SimplePath {
                input: boxed(nodes_root()),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "with_sack",
            root: AstNode::WithSack {
                input: boxed(nodes_root()),
                initial: PropertyValue::from(1),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "sack_set",
            root: AstNode::SackSet {
                input: boxed(nodes_root()),
                property: "score".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "sack_add",
            root: AstNode::SackAdd {
                input: boxed(nodes_root()),
                property: "weight".to_owned(),
            },
            context: default_context.clone(),
        },
        SurfaceCase {
            name: "sack_get",
            root: AstNode::SackGet {
                input: boxed(nodes_root()),
            },
            context: default_context,
        },
    ]
}
