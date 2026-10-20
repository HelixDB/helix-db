//! One request per variant of every request enum, so a parser comparison
//! exercises every visitor the AST has.
//!
//! The `*_variant` functions match exhaustively without a wildcard: adding a
//! variant fails to compile until it is named there, and the tests in this
//! module then fail until a sample uses it.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::batch::{BatchCondition, BatchEntry, NamedQuery, WriteBatch};
use crate::expr::{CompareOp, Expr, Predicate, StreamBound, WhenThen};
use crate::graph::{EdgeRef, NodeRef};
use crate::index::{IndexSpec, RangeIndexDirection, VectorDistanceMetric};
use crate::projection::{
    BindingProjection, BindingTarget, BindingValueRef, ExprProjection, Projection,
    PropertyProjection,
};
use crate::query::{QueryParamType, QueryRequest, QueryValue};
use crate::traversal::{
    AggregateFunction, AstNode, EmitBehavior, Order, RepeatConfig, ShortestPathDirection,
    SubTraversal,
};
use crate::value::{PropertyInput, PropertyValue};

use super::Shape;

fn nodes() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

fn edges() -> Box<AstNode> {
    Box::new(AstNode::Edges {
        reference: EdgeRef::Ids(vec![1, u64::MAX]),
    })
}

fn branch() -> SubTraversal {
    SubTraversal {
        root: Box::new(AstNode::Out {
            input: Box::new(AstNode::Context),
            label: None,
        }),
    }
}

fn input() -> PropertyInput {
    PropertyInput::Value(PropertyValue::String(String::from("value")))
}

fn predicate() -> Predicate {
    Predicate::eq("p", 1_i64)
}

/// Every [`AstNode`] variant once, with optional fields both set and unset
/// across the samples.
pub fn ast_nodes() -> Vec<AstNode> {
    let search = |within: bool| {
        (
            within.then(nodes),
            String::from("Doc"),
            String::from("embedding"),
            Some(PropertyInput::Expr(Expr::Param(String::from("tenant")))),
            StreamBound::Expr(Expr::Param(String::from("k"))),
        )
    };
    let (_, label, property, tenant_value, k) = search(false);
    vec![
        AstNode::Context,
        AstNode::Nodes {
            reference: NodeRef::Var(String::from("v")),
        },
        AstNode::NodesWhere {
            predicate: predicate(),
        },
        AstNode::Edges {
            reference: EdgeRef::Param(String::from("ids")),
        },
        AstNode::EdgesWhere {
            predicate: predicate(),
        },
        AstNode::VectorSearchNodes {
            label: label.clone(),
            property: property.clone(),
            tenant_value: tenant_value.clone(),
            query_vector: PropertyInput::Value(PropertyValue::F32Array(vec![0.5, -1.25])),
            k: k.clone(),
        },
        AstNode::TextSearchNodes {
            label: label.clone(),
            property: property.clone(),
            tenant_value: None,
            query_text: PropertyInput::Expr(Expr::Param(String::from("q"))),
            k: StreamBound::Literal(7),
        },
        AstNode::VectorSearchEdges {
            label: label.clone(),
            property: property.clone(),
            tenant_value: None,
            query_vector: PropertyInput::Expr(Expr::Param(String::from("vector"))),
            k: k.clone(),
        },
        AstNode::TextSearchEdges {
            label: label.clone(),
            property: property.clone(),
            tenant_value: tenant_value.clone(),
            query_text: input(),
            k: StreamBound::Literal(1),
        },
        AstNode::TextSearchNodesWithin {
            input: nodes(),
            label: label.clone(),
            property: property.clone(),
            tenant_value: None,
            query_text: input(),
            k: k.clone(),
        },
        AstNode::TextSearchEdgesWithin {
            input: edges(),
            label: label.clone(),
            property: property.clone(),
            tenant_value: tenant_value.clone(),
            query_text: input(),
            k: k.clone(),
        },
        AstNode::VectorSearchNodesWithin {
            input: nodes(),
            label: label.clone(),
            property: property.clone(),
            tenant_value: tenant_value.clone(),
            query_vector: PropertyInput::Value(PropertyValue::F64Array(vec![0.25])),
            k: k.clone(),
        },
        AstNode::VectorSearchEdgesWithin {
            input: edges(),
            label,
            property,
            tenant_value,
            query_vector: input(),
            k,
        },
        AstNode::Out {
            input: nodes(),
            label: Some(String::from("FOLLOWS")),
        },
        AstNode::In {
            input: nodes(),
            label: None,
        },
        AstNode::Both {
            input: nodes(),
            label: Some(String::from("KNOWS")),
        },
        AstNode::OutE {
            input: nodes(),
            label: None,
        },
        AstNode::InE {
            input: nodes(),
            label: Some(String::from("LIKES")),
        },
        AstNode::BothE {
            input: nodes(),
            label: None,
        },
        AstNode::OutN { input: edges() },
        AstNode::InN { input: edges() },
        AstNode::OtherN { input: edges() },
        AstNode::Has {
            input: nodes(),
            property: String::from("age"),
            value: PropertyValue::I64(-3),
        },
        AstNode::HasLabel {
            input: nodes(),
            label: String::from("User"),
        },
        AstNode::HasKey {
            input: nodes(),
            property: String::from("email"),
        },
        AstNode::Where {
            input: nodes(),
            predicate: predicate(),
        },
        AstNode::Dedup { input: nodes() },
        AstNode::Within {
            input: nodes(),
            variable: String::from("seen"),
        },
        AstNode::Without {
            input: nodes(),
            variable: String::from("seen"),
        },
        AstNode::EdgeHas {
            input: edges(),
            property: String::from("weight"),
            value: PropertyInput::Expr(Expr::Param(String::from("weight"))),
        },
        AstNode::EdgeHasLabel {
            input: edges(),
            label: String::from("FOLLOWS"),
        },
        AstNode::Limit {
            input: nodes(),
            count: StreamBound::Literal(usize::MAX),
        },
        AstNode::Skip {
            input: nodes(),
            count: StreamBound::Expr(Expr::Param(String::from("skip"))),
        },
        AstNode::Range {
            input: nodes(),
            start: StreamBound::Literal(0),
            end: StreamBound::Expr(Expr::Add {
                left: Box::new(Expr::Param(String::from("start"))),
                right: Box::new(Expr::Constant(PropertyValue::I64(10))),
            }),
        },
        AstNode::As {
            input: nodes(),
            name: String::from("a"),
        },
        AstNode::Store {
            input: nodes(),
            name: String::from("s"),
        },
        AstNode::Select {
            input: nodes(),
            name: String::from("s"),
        },
        AstNode::Bind {
            input: nodes(),
            name: String::from("b"),
        },
        AstNode::Inject {
            input: None,
            variable: String::from("v"),
        },
        AstNode::Count { input: nodes() },
        AstNode::Exists { input: nodes() },
        AstNode::Id { input: nodes() },
        AstNode::Label { input: nodes() },
        AstNode::Values {
            input: nodes(),
            properties: vec![String::from("a"), String::from("b")],
        },
        AstNode::ValueMap {
            input: nodes(),
            properties: None,
        },
        AstNode::Project {
            input: nodes(),
            projections: vec![
                Projection::Property(PropertyProjection::renamed("$id", "id")),
                Projection::Expr(ExprProjection::new("now", Expr::DateTimeNow)),
            ],
        },
        AstNode::ProjectBindings {
            input: nodes(),
            projections: vec![
                BindingProjection::Property {
                    target: BindingTarget::Current,
                    source: String::from("name"),
                    alias: String::from("name"),
                },
                BindingProjection::Coalesce {
                    refs: vec![BindingValueRef {
                        target: BindingTarget::Binding(String::from("owner")),
                        source: String::from("$id"),
                    }],
                    alias: String::from("owner"),
                },
            ],
            distinct: true,
        },
        AstNode::EdgeProperties { input: edges() },
        AstNode::CreateIndex {
            spec: IndexSpec::NodeEquality {
                label: String::from("User"),
                property: String::from("email"),
                unique: true,
            },
            if_not_exists: false,
        },
        AstNode::DropIndex {
            spec: IndexSpec::EdgeRange {
                label: String::from("RATED"),
                property: String::from("at"),
                direction: RangeIndexDirection::Desc,
            },
        },
        AstNode::GetIndexOperation {
            operation_id: String::from("00000000-0000-0000-0000-000000000001"),
        },
        AstNode::RetryIndexOperation {
            operation_id: String::from("00000000-0000-0000-0000-000000000002"),
        },
        AstNode::AbortIndexOperation {
            operation_id: String::from("00000000-0000-0000-0000-000000000003"),
        },
        AstNode::AddN {
            input: None,
            label: String::from("User"),
            properties: vec![
                (String::from("name"), input()),
                (
                    String::from("created"),
                    PropertyInput::Expr(Expr::Timestamp),
                ),
            ],
        },
        AstNode::AddE {
            input: nodes(),
            label: String::from("KNOWS"),
            to: NodeRef::Ids(vec![2]),
            properties: Vec::new(),
        },
        AstNode::SetProperty {
            input: nodes(),
            name: String::from("score"),
            value: PropertyInput::Expr(Expr::Neg {
                expr: Box::new(Expr::Property(String::from("score"))),
            }),
        },
        AstNode::RemoveProperty {
            input: nodes(),
            name: String::from("score"),
        },
        AstNode::Drop { input: edges() },
        AstNode::DropEdge {
            input: nodes(),
            to: NodeRef::Param(String::from("to")),
        },
        AstNode::DropEdgeLabeled {
            input: nodes(),
            to: NodeRef::All,
            label: String::from("KNOWS"),
        },
        AstNode::DropEdgeById {
            input: Some(nodes()),
            edges: EdgeRef::All,
        },
        AstNode::OrderBy {
            input: nodes(),
            property: String::from("age"),
            order: Order::Desc,
        },
        AstNode::OrderByMultiple {
            input: nodes(),
            orderings: vec![
                (String::from("a"), Order::Asc),
                (String::from("b"), Order::Desc),
            ],
        },
        AstNode::Repeat {
            input: nodes(),
            config: RepeatConfig {
                traversal: branch(),
                times: Some(2),
                until: Some(predicate()),
                emit: EmitBehavior::All,
                emit_predicate: None,
                max_depth: 5,
            },
        },
        AstNode::Union {
            input: nodes(),
            traversals: vec![branch(), SubTraversal::new()],
        },
        AstNode::Choose {
            input: nodes(),
            condition: predicate(),
            then_traversal: branch(),
            else_traversal: Some(branch()),
        },
        AstNode::Coalesce {
            input: nodes(),
            traversals: vec![branch()],
        },
        AstNode::Optional {
            input: nodes(),
            traversal: branch(),
        },
        AstNode::Group {
            input: nodes(),
            property: String::from("team"),
        },
        AstNode::GroupCount {
            input: nodes(),
            property: String::from("team"),
        },
        AstNode::AggregateBy {
            input: nodes(),
            function: AggregateFunction::Mean,
            property: String::from("age"),
        },
        AstNode::Fold { input: nodes() },
        AstNode::Unfold { input: nodes() },
        AstNode::Path { input: nodes() },
        AstNode::SimplePath { input: nodes() },
        AstNode::WithSack {
            input: nodes(),
            initial: PropertyValue::F64(1.5),
        },
        AstNode::SackSet {
            input: nodes(),
            property: String::from("w"),
        },
        AstNode::SackAdd {
            input: nodes(),
            property: String::from("w"),
        },
        AstNode::SackGet { input: nodes() },
        AstNode::ShortestPath {
            source: NodeRef::Ids(vec![1]),
            target: NodeRef::Param(String::from("target")),
            label: Some(String::from("ROAD")),
            direction: ShortestPathDirection::Both,
            max_depth: 4,
        },
    ]
}

/// Every [`Predicate`] variant once, and every [`CompareOp`].
pub fn predicates() -> Vec<Predicate> {
    let expr = || Expr::Property(String::from("p"));
    let value = || Expr::Constant(PropertyValue::String(String::from("v")));
    [
        CompareOp::Eq,
        CompareOp::Neq,
        CompareOp::Gt,
        CompareOp::Gte,
        CompareOp::Lt,
        CompareOp::Lte,
    ]
    .into_iter()
    .map(|op| Predicate::Compare {
        left: expr(),
        op,
        right: value(),
    })
    .chain([
        Predicate::Eq {
            left: expr(),
            right: value(),
        },
        Predicate::Neq {
            left: expr(),
            right: value(),
        },
        Predicate::Gt {
            left: expr(),
            right: value(),
        },
        Predicate::Gte {
            left: expr(),
            right: value(),
        },
        Predicate::Lt {
            left: expr(),
            right: value(),
        },
        Predicate::Lte {
            left: expr(),
            right: value(),
        },
        Predicate::Between {
            value: expr(),
            min: Expr::Constant(PropertyValue::I64(1)),
            max: Expr::Param(String::from("max")),
        },
        Predicate::HasKey {
            property: String::from("p"),
        },
        Predicate::IsNull {
            property: String::from("p"),
        },
        Predicate::IsNotNull {
            property: String::from("p"),
        },
        Predicate::StartsWith {
            value: expr(),
            prefix: value(),
        },
        Predicate::EndsWith {
            value: expr(),
            suffix: value(),
        },
        Predicate::Contains {
            value: expr(),
            substring: value(),
        },
        Predicate::IsIn {
            value: expr(),
            values: Expr::Constant(PropertyValue::StringArray(vec![
                String::from("a"),
                String::from("b"),
            ])),
        },
        Predicate::And {
            predicates: vec![predicate(), Predicate::is_null("q")],
        },
        Predicate::Or {
            predicates: Vec::new(),
        },
        Predicate::Not {
            predicate: Box::new(Predicate::Not {
                predicate: Box::new(predicate()),
            }),
        },
    ])
    .collect()
}

/// Every [`Expr`] variant once.
pub fn exprs() -> Vec<Expr> {
    let leaf = || Box::new(Expr::Property(String::from("x")));
    vec![
        Expr::Property(String::from("x")),
        Expr::Id,
        Expr::Timestamp,
        Expr::DateTimeNow,
        Expr::Constant(PropertyValue::Null),
        Expr::Param(String::from("p")),
        Expr::Add {
            left: leaf(),
            right: leaf(),
        },
        Expr::Sub {
            left: leaf(),
            right: leaf(),
        },
        Expr::Mul {
            left: leaf(),
            right: leaf(),
        },
        Expr::Div {
            left: leaf(),
            right: leaf(),
        },
        Expr::Mod {
            left: leaf(),
            right: leaf(),
        },
        Expr::Neg { expr: leaf() },
        Expr::Case {
            when_then: vec![WhenThen {
                when: predicate(),
                then: Expr::Constant(PropertyValue::Bool(true)),
            }],
            else_expr: Some(leaf()),
        },
        Expr::Case {
            when_then: Vec::new(),
            else_expr: None,
        },
    ]
}

/// Every [`PropertyValue`] variant once, with strings that need escaping
/// and numbers at their edges.
pub fn property_values() -> Vec<PropertyValue> {
    vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::I64(i64::MIN),
        PropertyValue::DateTime(1_788_000_000_000),
        PropertyValue::F64(-0.1),
        PropertyValue::F32(f32::MAX),
        PropertyValue::String(String::from("tab\t\"quote\" \u{1f980} \\ end")),
        PropertyValue::Bytes(vec![0, 127, 255]),
        PropertyValue::I64Array(vec![i64::MAX, 0]),
        PropertyValue::F64Array(vec![1e-300, 2.5]),
        PropertyValue::F32Array(Vec::new()),
        PropertyValue::StringArray(vec![String::from(""), String::from("é")]),
        PropertyValue::Array(vec![
            PropertyValue::Null,
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
        ]),
        PropertyValue::Object(BTreeMap::from([
            (String::from("b"), PropertyValue::Object(BTreeMap::new())),
            (String::from("a"), PropertyValue::String(String::from("x"))),
        ])),
    ]
}

/// Every [`IndexSpec`] variant once, and every direction and metric.
pub fn index_specs() -> Vec<IndexSpec> {
    let dimension = NonZeroUsize::new(3).expect("non-zero");
    vec![
        IndexSpec::NodeEquality {
            label: String::from("U"),
            property: String::from("e"),
            unique: false,
        },
        IndexSpec::NodeRange {
            label: String::from("U"),
            property: String::from("a"),
            direction: RangeIndexDirection::Asc,
        },
        IndexSpec::EdgeEquality {
            label: String::from("R"),
            property: String::from("k"),
        },
        IndexSpec::EdgeRange {
            label: String::from("R"),
            property: String::from("t"),
            direction: RangeIndexDirection::Desc,
        },
        IndexSpec::NodeVector {
            label: String::from("D"),
            property: String::from("v"),
            dimension,
            metric: VectorDistanceMetric::Cosine,
            tenant_property: Some(String::from("tenant")),
        },
        IndexSpec::NodeText {
            label: String::from("D"),
            property: String::from("body"),
            tenant_property: None,
        },
        IndexSpec::EdgeVector {
            label: String::from("R"),
            property: String::from("v"),
            dimension,
            metric: VectorDistanceMetric::Euclidean,
            tenant_property: None,
        },
        IndexSpec::EdgeText {
            label: String::from("R"),
            property: String::from("note"),
            tenant_property: Some(String::from("tenant")),
        },
        IndexSpec::EdgeVector {
            label: String::from("R"),
            property: String::from("w"),
            dimension,
            metric: VectorDistanceMetric::Manhattan,
            tenant_property: None,
        },
    ]
}

fn ast_variant(node: &AstNode) -> &'static str {
    match node {
        AstNode::Context => "context",
        AstNode::Nodes { .. } => "nodes",
        AstNode::NodesWhere { .. } => "nodes_where",
        AstNode::Edges { .. } => "edges",
        AstNode::EdgesWhere { .. } => "edges_where",
        AstNode::VectorSearchNodes { .. } => "vector_search_nodes",
        AstNode::TextSearchNodes { .. } => "text_search_nodes",
        AstNode::VectorSearchEdges { .. } => "vector_search_edges",
        AstNode::TextSearchEdges { .. } => "text_search_edges",
        AstNode::TextSearchNodesWithin { .. } => "text_search_nodes_within",
        AstNode::TextSearchEdgesWithin { .. } => "text_search_edges_within",
        AstNode::VectorSearchNodesWithin { .. } => "vector_search_nodes_within",
        AstNode::VectorSearchEdgesWithin { .. } => "vector_search_edges_within",
        AstNode::Out { .. } => "out",
        AstNode::In { .. } => "in",
        AstNode::Both { .. } => "both",
        AstNode::OutE { .. } => "out_e",
        AstNode::InE { .. } => "in_e",
        AstNode::BothE { .. } => "both_e",
        AstNode::OutN { .. } => "out_n",
        AstNode::InN { .. } => "in_n",
        AstNode::OtherN { .. } => "other_n",
        AstNode::Has { .. } => "has",
        AstNode::HasLabel { .. } => "has_label",
        AstNode::HasKey { .. } => "has_key",
        AstNode::Where { .. } => "where",
        AstNode::Dedup { .. } => "dedup",
        AstNode::Within { .. } => "within",
        AstNode::Without { .. } => "without",
        AstNode::EdgeHas { .. } => "edge_has",
        AstNode::EdgeHasLabel { .. } => "edge_has_label",
        AstNode::Limit { .. } => "limit",
        AstNode::Skip { .. } => "skip",
        AstNode::Range { .. } => "range",
        AstNode::As { .. } => "as",
        AstNode::Store { .. } => "store",
        AstNode::Select { .. } => "select",
        AstNode::Bind { .. } => "bind",
        AstNode::Inject { .. } => "inject",
        AstNode::Count { .. } => "count",
        AstNode::Exists { .. } => "exists",
        AstNode::Id { .. } => "id",
        AstNode::Label { .. } => "label",
        AstNode::Values { .. } => "values",
        AstNode::ValueMap { .. } => "value_map",
        AstNode::Project { .. } => "project",
        AstNode::ProjectBindings { .. } => "project_bindings",
        AstNode::EdgeProperties { .. } => "edge_properties",
        AstNode::CreateIndex { .. } => "create_index",
        AstNode::DropIndex { .. } => "drop_index",
        AstNode::GetIndexOperation { .. } => "get_index_operation",
        AstNode::RetryIndexOperation { .. } => "retry_index_operation",
        AstNode::AbortIndexOperation { .. } => "abort_index_operation",
        AstNode::AddN { .. } => "add_n",
        AstNode::AddE { .. } => "add_e",
        AstNode::SetProperty { .. } => "set_property",
        AstNode::RemoveProperty { .. } => "remove_property",
        AstNode::Drop { .. } => "drop",
        AstNode::DropEdge { .. } => "drop_edge",
        AstNode::DropEdgeLabeled { .. } => "drop_edge_labeled",
        AstNode::DropEdgeById { .. } => "drop_edge_by_id",
        AstNode::OrderBy { .. } => "order_by",
        AstNode::OrderByMultiple { .. } => "order_by_multiple",
        AstNode::Repeat { .. } => "repeat",
        AstNode::Union { .. } => "union",
        AstNode::Choose { .. } => "choose",
        AstNode::Coalesce { .. } => "coalesce",
        AstNode::Optional { .. } => "optional",
        AstNode::Group { .. } => "group",
        AstNode::GroupCount { .. } => "group_count",
        AstNode::AggregateBy { .. } => "aggregate_by",
        AstNode::Fold { .. } => "fold",
        AstNode::Unfold { .. } => "unfold",
        AstNode::Path { .. } => "path",
        AstNode::SimplePath { .. } => "simple_path",
        AstNode::WithSack { .. } => "with_sack",
        AstNode::SackSet { .. } => "sack_set",
        AstNode::SackAdd { .. } => "sack_add",
        AstNode::SackGet { .. } => "sack_get",
        AstNode::ShortestPath { .. } => "shortest_path",
    }
}

fn predicate_variant(predicate: &Predicate) -> &'static str {
    match predicate {
        Predicate::Eq { .. } => "eq",
        Predicate::Neq { .. } => "neq",
        Predicate::Gt { .. } => "gt",
        Predicate::Gte { .. } => "gte",
        Predicate::Lt { .. } => "lt",
        Predicate::Lte { .. } => "lte",
        Predicate::Between { .. } => "between",
        Predicate::HasKey { .. } => "has_key",
        Predicate::IsNull { .. } => "is_null",
        Predicate::IsNotNull { .. } => "is_not_null",
        Predicate::StartsWith { .. } => "starts_with",
        Predicate::EndsWith { .. } => "ends_with",
        Predicate::Contains { .. } => "contains",
        Predicate::IsIn { .. } => "is_in",
        Predicate::And { .. } => "and",
        Predicate::Or { .. } => "or",
        Predicate::Not { .. } => "not",
        Predicate::Compare { .. } => "compare",
    }
}

fn expr_variant(expr: &Expr) -> &'static str {
    match expr {
        Expr::Property(_) => "property",
        Expr::Id => "id",
        Expr::Timestamp => "timestamp",
        Expr::DateTimeNow => "date_time_now",
        Expr::Constant(_) => "constant",
        Expr::Param(_) => "param",
        Expr::Add { .. } => "add",
        Expr::Sub { .. } => "sub",
        Expr::Mul { .. } => "mul",
        Expr::Div { .. } => "div",
        Expr::Mod { .. } => "mod",
        Expr::Neg { .. } => "neg",
        Expr::Case { .. } => "case",
    }
}

fn property_value_variant(value: &PropertyValue) -> &'static str {
    match value {
        PropertyValue::Null => "null",
        PropertyValue::Bool(_) => "bool",
        PropertyValue::I64(_) => "i64",
        PropertyValue::DateTime(_) => "date_time",
        PropertyValue::F64(_) => "f64",
        PropertyValue::F32(_) => "f32",
        PropertyValue::String(_) => "string",
        PropertyValue::Bytes(_) => "bytes",
        PropertyValue::I64Array(_) => "i64_array",
        PropertyValue::F64Array(_) => "f64_array",
        PropertyValue::F32Array(_) => "f32_array",
        PropertyValue::StringArray(_) => "string_array",
        PropertyValue::Array(_) => "array",
        PropertyValue::Object(_) => "object",
    }
}

fn index_spec_variant(spec: &IndexSpec) -> &'static str {
    match spec {
        IndexSpec::NodeEquality { .. } => "node_equality",
        IndexSpec::NodeRange { .. } => "node_range",
        IndexSpec::EdgeEquality { .. } => "edge_equality",
        IndexSpec::EdgeRange { .. } => "edge_range",
        IndexSpec::NodeVector { .. } => "node_vector",
        IndexSpec::NodeText { .. } => "node_text",
        IndexSpec::EdgeVector { .. } => "edge_vector",
        IndexSpec::EdgeText { .. } => "edge_text",
    }
}

fn entry(name: Option<&str>, root: AstNode, condition: Option<BatchCondition>) -> BatchEntry {
    BatchEntry::Query(Box::new(NamedQuery {
        name: name.map(String::from),
        root,
        condition,
    }))
}

/// One write request per sample, plus requests that cover batch shapes,
/// every [`BatchCondition`], `for_each` nesting, typed and untyped
/// parameters of every [`QueryValue`] and [`QueryParamType`] kind, and read
/// batches. Every request is valid.
///
/// ```
/// use helix_ast::{query::QueryRequest, testing};
///
/// for shape in testing::every_variant() {
///     assert!(QueryRequest::from_json_slice(&shape.json).is_ok(), "{}", shape.name);
/// }
/// ```
pub fn every_variant() -> Vec<Shape> {
    let write = |name: String, entries: Vec<BatchEntry>| {
        Shape::new(
            name,
            &QueryRequest::write(WriteBatch {
                entries,
                returns: vec![String::from("r")],
            }),
        )
    };
    let samples = ast_nodes()
        .into_iter()
        .map(|node| (format!("ast/{}", ast_variant(&node)), node))
        .chain(predicates().into_iter().map(|predicate| {
            (
                format!("predicate/{}", predicate_variant(&predicate)),
                AstNode::Where {
                    input: nodes(),
                    predicate,
                },
            )
        }))
        .chain(exprs().into_iter().map(|expr| {
            (
                format!("expr/{}", expr_variant(&expr)),
                AstNode::SetProperty {
                    input: nodes(),
                    name: String::from("x"),
                    value: PropertyInput::Expr(expr),
                },
            )
        }))
        .chain(property_values().into_iter().map(|value| {
            (
                format!("property_value/{}", property_value_variant(&value)),
                AstNode::Has {
                    input: nodes(),
                    property: String::from("x"),
                    value,
                },
            )
        }))
        .chain(index_specs().into_iter().map(|spec| {
            (
                format!("index_spec/{}", index_spec_variant(&spec)),
                AstNode::CreateIndex {
                    spec,
                    if_not_exists: true,
                },
            )
        }));
    let mut shapes = samples
        .enumerate()
        .map(|(index, (name, root))| {
            write(
                format!("{name}#{index}"),
                vec![entry(Some("r"), root, None)],
            )
        })
        .collect::<Vec<_>>();
    shapes.push(write(
        String::from("batch/conditions"),
        [
            BatchCondition::VarNotEmpty(String::from("a")),
            BatchCondition::VarEmpty(String::from("a")),
            BatchCondition::VarMinSize(String::from("a"), 2),
            BatchCondition::PrevNotEmpty,
        ]
        .into_iter()
        .map(|condition| entry(None, AstNode::Count { input: nodes() }, Some(condition)))
        .collect(),
    ));
    shapes.push(write(
        String::from("batch/for_each"),
        vec![BatchEntry::ForEach {
            param: String::from("rows"),
            body: vec![
                BatchEntry::ForEach {
                    param: String::from("inner"),
                    body: vec![entry(Some("r"), *nodes(), None)],
                },
                entry(None, AstNode::Context, None),
            ],
        }],
    ));
    shapes.push(Shape::new(
        "batch/read",
        &QueryRequest::read(
            crate::batch::read_batch()
                .var_as("r", crate::traversal::g().n(NodeRef::All).count())
                .returning(["r"]),
        )
        .with_query_name("named"),
    ));
    let values = [
        QueryValue::Null,
        QueryValue::Bool(true),
        QueryValue::I64(i64::MIN),
        QueryValue::F64(0.5),
        QueryValue::F32(1.5),
        QueryValue::String(String::from("s\"\\")),
        QueryValue::Array(vec![QueryValue::Array(Vec::new()), QueryValue::I64(1)]),
        QueryValue::Object(BTreeMap::from([(String::from("k"), QueryValue::Null)])),
    ];
    shapes.push(Shape::new(
        "params/untyped",
        &values.iter().enumerate().fold(
            QueryRequest::read(crate::batch::read_batch()),
            |request, (index, value)| {
                request.with_parameter_value(format!("p{index}"), value.clone())
            },
        ),
    ));
    let typed = [
        (QueryParamType::Bool, QueryValue::Bool(false)),
        (QueryParamType::I64, QueryValue::I64(7)),
        (QueryParamType::F64, QueryValue::I64(7)),
        (QueryParamType::F32, QueryValue::F64(0.25)),
        (
            QueryParamType::String,
            QueryValue::String(String::from("s")),
        ),
        (
            QueryParamType::DateTime,
            QueryValue::String(String::from("2026-10-07T10:00:00Z")),
        ),
        (QueryParamType::Value, QueryValue::Null),
        (
            QueryParamType::Object,
            QueryValue::Object(BTreeMap::from([(String::from("k"), QueryValue::I64(1))])),
        ),
        (
            QueryParamType::Array(Box::new(QueryParamType::Array(Box::new(
                QueryParamType::F32,
            )))),
            QueryValue::Array(vec![QueryValue::Array(vec![QueryValue::F64(1.0)])]),
        ),
    ];
    shapes.push(Shape::new(
        "params/typed",
        &typed.into_iter().enumerate().fold(
            QueryRequest::read(crate::batch::read_batch()),
            |request, (index, (ty, value))| {
                request
                    .with_typed_parameter(format!("p{index}"), ty, value)
                    .expect("typed samples match their schema")
            },
        ),
    ));
    shapes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn samples_cover_every_variant() {
        // Each count is the number of arms in the matching `*_variant` guard,
        // so a variant added there without a sample fails here.
        let distinct = |names: Vec<&'static str>| names.into_iter().collect::<BTreeSet<_>>().len();
        let nodes = ast_nodes();
        assert_eq!(nodes.len(), 80);
        assert_eq!(distinct(nodes.iter().map(ast_variant).collect()), 80);
        assert_eq!(
            distinct(predicates().iter().map(predicate_variant).collect()),
            18
        );
        assert_eq!(distinct(exprs().iter().map(expr_variant).collect()), 13);
        assert_eq!(
            distinct(
                property_values()
                    .iter()
                    .map(property_value_variant)
                    .collect()
            ),
            14
        );
        assert_eq!(
            distinct(index_specs().iter().map(index_spec_variant).collect()),
            8
        );
    }

    #[test]
    fn every_variant_request_is_valid_and_named_once() {
        let shapes = every_variant();
        let names = shapes
            .iter()
            .map(|shape| shape.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), shapes.len());
        for shape in &shapes {
            let request = QueryRequest::from_json_slice(&shape.json)
                .unwrap_or_else(|error| panic!("{}: {error}", shape.name));
            assert_eq!(
                request.to_json_bytes().unwrap(),
                shape.json,
                "{}",
                shape.name
            );
        }
    }
}
