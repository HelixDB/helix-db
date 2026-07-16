//! Typed traversal states and operations for the public query AST.

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::expr::{Predicate, SourcePredicate, StreamBound};
use crate::graph::{EdgeRef, NodeRef};
use crate::index::IndexSpec;
use crate::projection::{
    validate_binding_name, validate_binding_projections, BindingProjection, Projection,
};
use crate::value::{PropertyInput, PropertyValue};
/// Marker trait for traversal states.
pub trait TraversalState: private::Sealed {}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Empty {}
    impl Sealed for super::OnNodes {}
    impl Sealed for super::OnEdges {}
    impl Sealed for super::Terminal {}
    impl Sealed for super::ReadOnly {}
    impl Sealed for super::WriteEnabled {}
}

/// Initial state with no root node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Empty;

/// Traversal currently yields nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnNodes;

/// Traversal currently yields edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnEdges;

/// Traversal is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Terminal;

impl TraversalState for Empty {}
impl TraversalState for OnNodes {}
impl TraversalState for OnEdges {}
impl TraversalState for Terminal {}

/// Marker trait for mutation capability.
pub trait MutationMode: private::Sealed {}

/// Read-only traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOnly;

/// Traversal containing write operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteEnabled;

impl MutationMode for ReadOnly {}
impl MutationMode for WriteEnabled {}
/// Sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    /// Ascending.
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// Direction used by shortest-path traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShortestPathDirection {
    /// Follow outgoing edges from the source.
    #[default]
    Out,
    /// Follow incoming edges from the source.
    In,
    /// Follow both incoming and outgoing edges.
    Both,
}

/// Repeat emit behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmitBehavior {
    /// Do not emit intermediate results.
    #[default]
    None,
    /// Emit before each iteration.
    Before,
    /// Emit after each iteration.
    After,
    /// Emit before and after.
    All,
}

/// Aggregate function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunction {
    /// Count.
    Count,
    /// Sum.
    Sum,
    /// Min.
    Min,
    /// Max.
    Max,
    /// Mean.
    Mean,
}
/// Query AST node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AstNode {
    /// Implicit branch input for sub-traversals.
    Context,
    /// Start from nodes.
    Nodes { reference: NodeRef },
    /// Start from nodes matching a predicate.
    NodesWhere { predicate: SourcePredicate },
    /// Start from edges.
    Edges { reference: EdgeRef },
    /// Start from edges matching a predicate.
    EdgesWhere { predicate: SourcePredicate },
    /// Vector search on nodes.
    VectorSearchNodes {
        /// Label scope.
        label: String,
        /// Vector property.
        property: String,
        /// Optional tenant value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// Query vector input.
        query_vector: PropertyInput,
        /// Result count.
        k: StreamBound,
    },
    /// Text search on nodes.
    TextSearchNodes {
        /// Label scope.
        label: String,
        /// Text property.
        property: String,
        /// Optional tenant value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// Query text input.
        query_text: PropertyInput,
        /// Result count.
        k: StreamBound,
    },
    /// Vector search on edges.
    VectorSearchEdges {
        /// Label scope.
        label: String,
        /// Vector property.
        property: String,
        /// Optional tenant value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// Query vector input.
        query_vector: PropertyInput,
        /// Result count.
        k: StreamBound,
    },
    /// Text search on edges.
    TextSearchEdges {
        /// Label scope.
        label: String,
        /// Text property.
        property: String,
        /// Optional tenant value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// Query text input.
        query_text: PropertyInput,
        /// Result count.
        k: StreamBound,
    },
    /// Node-to-node outgoing traversal.
    Out {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    /// Node-to-node incoming traversal.
    In {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    /// Node-to-node both-direction traversal.
    Both {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    /// Node-to-edge outgoing traversal.
    OutE {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    /// Node-to-edge incoming traversal.
    InE {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    /// Node-to-edge both-direction traversal.
    BothE {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    /// Edge-to-target-node traversal.
    OutN { input: Box<AstNode> },
    /// Edge-to-source-node traversal.
    InN { input: Box<AstNode> },
    /// Edge-to-other-node traversal.
    OtherN { input: Box<AstNode> },
    /// Property equality filter.
    Has {
        /// Input stream.
        input: Box<AstNode>,
        /// Property name.
        property: String,
        /// Literal value.
        value: PropertyValue,
    },
    /// Label filter.
    HasLabel {
        /// Input stream.
        input: Box<AstNode>,
        /// Label.
        label: String,
    },
    /// Property existence filter.
    HasKey {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
    },
    /// Predicate filter.
    Where {
        /// Input stream.
        input: Box<AstNode>,
        /// Predicate.
        predicate: Predicate,
    },
    /// Deduplicate stream.
    Dedup { input: Box<AstNode> },
    /// Keep elements within a variable.
    Within {
        /// Input stream.
        input: Box<AstNode>,
        /// Variable name.
        variable: String,
    },
    /// Keep elements outside a variable.
    Without {
        /// Input stream.
        input: Box<AstNode>,
        /// Variable name.
        variable: String,
    },
    /// Edge property filter.
    EdgeHas {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
        /// Value or expression.
        value: PropertyInput,
    },
    /// Edge label filter.
    EdgeHasLabel {
        /// Input stream.
        input: Box<AstNode>,
        /// Label.
        label: String,
    },
    /// Limit.
    Limit {
        /// Input stream.
        input: Box<AstNode>,
        /// Bound.
        count: StreamBound,
    },
    /// Skip.
    Skip {
        /// Input stream.
        input: Box<AstNode>,
        /// Bound.
        count: StreamBound,
    },
    /// Range.
    Range {
        /// Input stream.
        input: Box<AstNode>,
        /// Start bound.
        start: StreamBound,
        /// End bound.
        end: StreamBound,
    },
    /// Store current stream.
    As { input: Box<AstNode>, name: String },
    /// Store current stream.
    Store { input: Box<AstNode>, name: String },
    /// Select named stream.
    Select { input: Box<AstNode>, name: String },
    /// Capture current element as row binding.
    Bind { input: Box<AstNode>, name: String },
    /// Inject variable stream.
    Inject {
        /// Optional input stream. `None` means source inject.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Box<AstNode>>,
        /// Variable name.
        variable: String,
    },
    /// Count terminal.
    Count { input: Box<AstNode> },
    /// Exists terminal.
    Exists { input: Box<AstNode> },
    /// ID terminal.
    Id { input: Box<AstNode> },
    /// Label terminal.
    Label { input: Box<AstNode> },
    /// Values terminal.
    Values {
        /// Input stream.
        input: Box<AstNode>,
        /// Properties.
        properties: Vec<String>,
    },
    /// Value-map terminal.
    ValueMap {
        /// Input stream.
        input: Box<AstNode>,
        /// Optional property filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        properties: Option<Vec<String>>,
    },
    /// Project terminal.
    Project {
        /// Input stream.
        input: Box<AstNode>,
        /// Projection list.
        projections: Vec<Projection>,
    },
    /// Row-binding projection terminal.
    ProjectBindings {
        /// Input stream.
        input: Box<AstNode>,
        /// Projection list.
        projections: Vec<BindingProjection>,
        /// Deduplicate projected rows.
        distinct: bool,
    },
    /// Edge properties terminal.
    EdgeProperties { input: Box<AstNode> },
    /// Create index.
    CreateIndex {
        /// Index specification.
        spec: IndexSpec,
        /// Ignore existing matching index.
        if_not_exists: bool,
    },
    /// Drop index.
    DropIndex {
        /// Index specification.
        spec: IndexSpec,
    },
    /// Read one retained index operation in the request scope.
    GetIndexOperation {
        /// Canonical lowercase operation UUID.
        operation_id: String,
    },
    /// Ensure one retained operation is runnable in the request scope.
    RetryIndexOperation {
        /// Canonical lowercase operation UUID.
        operation_id: String,
    },
    /// Convert one constructing BUILD into abort cleanup.
    AbortIndexOperation {
        /// Canonical lowercase operation UUID.
        operation_id: String,
    },
    /// Add node.
    AddN {
        /// Optional prior input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Box<AstNode>>,
        /// Node label.
        label: String,
        /// Properties.
        properties: Vec<(String, PropertyInput)>,
    },
    /// Add edge.
    AddE {
        /// Input node stream.
        input: Box<AstNode>,
        /// Edge label.
        label: String,
        /// Target nodes.
        to: NodeRef,
        /// Properties.
        properties: Vec<(String, PropertyInput)>,
    },
    /// Set property.
    SetProperty {
        /// Input stream.
        input: Box<AstNode>,
        /// Property name.
        name: String,
        /// Value.
        value: PropertyInput,
    },
    /// Remove property.
    RemoveProperty {
        /// Input stream.
        input: Box<AstNode>,
        /// Property name.
        name: String,
    },
    /// Drop nodes.
    Drop { input: Box<AstNode> },
    /// Drop edges between current nodes and targets.
    DropEdge {
        /// Input node stream.
        input: Box<AstNode>,
        /// Target nodes.
        to: NodeRef,
    },
    /// Drop labeled edges.
    DropEdgeLabeled {
        /// Input node stream.
        input: Box<AstNode>,
        /// Target nodes.
        to: NodeRef,
        /// Edge label.
        label: String,
    },
    /// Drop edges by ID.
    DropEdgeById {
        /// Optional input stream.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Box<AstNode>>,
        /// Edge references.
        edges: EdgeRef,
    },
    /// Order by one property.
    OrderBy {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
        /// Order.
        order: Order,
    },
    /// Order by multiple properties.
    OrderByMultiple {
        /// Input stream.
        input: Box<AstNode>,
        /// Ordered keys.
        orderings: Vec<(String, Order)>,
    },
    /// Repeat traversal.
    Repeat {
        /// Input stream.
        input: Box<AstNode>,
        /// Repeat configuration.
        config: RepeatConfig,
    },
    /// Union branch traversals.
    Union {
        /// Input stream.
        input: Box<AstNode>,
        /// Branch traversals.
        traversals: Vec<SubTraversal>,
    },
    /// Conditional branch.
    Choose {
        /// Input stream.
        input: Box<AstNode>,
        /// Condition.
        condition: Predicate,
        /// Then branch.
        then_traversal: SubTraversal,
        /// Else branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        else_traversal: Option<SubTraversal>,
    },
    /// Coalesce branches.
    Coalesce {
        /// Input stream.
        input: Box<AstNode>,
        /// Branch traversals.
        traversals: Vec<SubTraversal>,
    },
    /// Optional branch.
    Optional {
        /// Input stream.
        input: Box<AstNode>,
        /// Branch traversal.
        traversal: SubTraversal,
    },
    /// Group terminal.
    Group {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
    },
    /// Group-count terminal.
    GroupCount {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
    },
    /// Aggregate terminal.
    AggregateBy {
        /// Input stream.
        input: Box<AstNode>,
        /// Function.
        function: AggregateFunction,
        /// Property.
        property: String,
    },
    /// Fold barrier.
    Fold { input: Box<AstNode> },
    /// Unfold barrier.
    Unfold { input: Box<AstNode> },
    /// Path operation.
    Path { input: Box<AstNode> },
    /// Simple-path operation.
    SimplePath { input: Box<AstNode> },
    /// Sack initialization.
    WithSack {
        /// Input stream.
        input: Box<AstNode>,
        /// Initial value.
        initial: PropertyValue,
    },
    /// Sack set.
    SackSet {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
    },
    /// Sack add.
    SackAdd {
        /// Input stream.
        input: Box<AstNode>,
        /// Property.
        property: String,
    },
    /// Sack get.
    SackGet { input: Box<AstNode> },
    /// Unweighted shortest path between two nodes.
    ShortestPath {
        /// Source node reference. Must resolve to exactly one node at runtime.
        source: NodeRef,
        /// Target node reference. Must resolve to exactly one node at runtime.
        target: NodeRef,
        /// Optional edge label.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        /// Traversal direction.
        direction: ShortestPathDirection,
        /// Maximum traversal depth.
        max_depth: usize,
    },
}

impl AstNode {
    /// Returns true when this node is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Count { .. }
                | Self::Exists { .. }
                | Self::Id { .. }
                | Self::Label { .. }
                | Self::Values { .. }
                | Self::ValueMap { .. }
                | Self::Project { .. }
                | Self::ProjectBindings { .. }
                | Self::EdgeProperties { .. }
                | Self::CreateIndex { .. }
                | Self::DropIndex { .. }
                | Self::GetIndexOperation { .. }
                | Self::RetryIndexOperation { .. }
                | Self::AbortIndexOperation { .. }
                | Self::Group { .. }
                | Self::GroupCount { .. }
                | Self::AggregateBy { .. }
                | Self::ShortestPath { .. }
        )
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Out(Option<String>),
    In(Option<String>),
    Both(Option<String>),
    OutE(Option<String>),
    InE(Option<String>),
    BothE(Option<String>),
    OutN,
    InN,
    OtherN,
    Has(String, PropertyValue),
    HasLabel(String),
    HasKey(String),
    Where(Predicate),
    Dedup,
    Within(String),
    Without(String),
    EdgeHas(String, PropertyInput),
    EdgeHasLabel(String),
    Limit(StreamBound),
    Skip(StreamBound),
    Range(StreamBound, StreamBound),
    As(String),
    Store(String),
    Select(String),
    Bind(String),
    Inject(String),
    Count,
    Exists,
    Id,
    Label,
    Values(Vec<String>),
    ValueMap(Option<Vec<String>>),
    Project(Vec<Projection>),
    ProjectBindings {
        projections: Vec<BindingProjection>,
        distinct: bool,
    },
    EdgeProperties,
    AddN {
        label: String,
        properties: Vec<(String, PropertyInput)>,
    },
    AddE {
        label: String,
        to: NodeRef,
        properties: Vec<(String, PropertyInput)>,
    },
    SetProperty(String, PropertyInput),
    RemoveProperty(String),
    Drop,
    DropEdge(NodeRef),
    DropEdgeLabeled {
        to: NodeRef,
        label: String,
    },
    DropEdgeById(EdgeRef),
    OrderBy(String, Order),
    OrderByMultiple(Vec<(String, Order)>),
    Repeat(RepeatConfig),
    Union(Vec<SubTraversal>),
    Choose {
        condition: Predicate,
        then_traversal: SubTraversal,
        else_traversal: Option<SubTraversal>,
    },
    Coalesce(Vec<SubTraversal>),
    Optional(SubTraversal),
    Group(String),
    GroupCount(String),
    AggregateBy(AggregateFunction, String),
    Fold,
    Unfold,
    Path,
    SimplePath,
    WithSack(PropertyValue),
    SackSet(String),
    SackAdd(String),
    SackGet,
}

impl Operation {
    fn apply(self, input: AstNode) -> AstNode {
        let input = Box::new(input);
        match self {
            Self::Out(label) => AstNode::Out { input, label },
            Self::In(label) => AstNode::In { input, label },
            Self::Both(label) => AstNode::Both { input, label },
            Self::OutE(label) => AstNode::OutE { input, label },
            Self::InE(label) => AstNode::InE { input, label },
            Self::BothE(label) => AstNode::BothE { input, label },
            Self::OutN => AstNode::OutN { input },
            Self::InN => AstNode::InN { input },
            Self::OtherN => AstNode::OtherN { input },
            Self::Has(property, value) => AstNode::Has {
                input,
                property,
                value,
            },
            Self::HasLabel(label) => AstNode::HasLabel { input, label },
            Self::HasKey(property) => AstNode::HasKey { input, property },
            Self::Where(predicate) => AstNode::Where { input, predicate },
            Self::Dedup => AstNode::Dedup { input },
            Self::Within(variable) => AstNode::Within { input, variable },
            Self::Without(variable) => AstNode::Without { input, variable },
            Self::EdgeHas(property, value) => AstNode::EdgeHas {
                input,
                property,
                value,
            },
            Self::EdgeHasLabel(label) => AstNode::EdgeHasLabel { input, label },
            Self::Limit(count) => AstNode::Limit { input, count },
            Self::Skip(count) => AstNode::Skip { input, count },
            Self::Range(start, end) => AstNode::Range { input, start, end },
            Self::As(name) => AstNode::As { input, name },
            Self::Store(name) => AstNode::Store { input, name },
            Self::Select(name) => AstNode::Select { input, name },
            Self::Bind(name) => AstNode::Bind { input, name },
            Self::Inject(variable) => AstNode::Inject {
                input: Some(input),
                variable,
            },
            Self::Count => AstNode::Count { input },
            Self::Exists => AstNode::Exists { input },
            Self::Id => AstNode::Id { input },
            Self::Label => AstNode::Label { input },
            Self::Values(properties) => AstNode::Values { input, properties },
            Self::ValueMap(properties) => AstNode::ValueMap { input, properties },
            Self::Project(projections) => AstNode::Project { input, projections },
            Self::ProjectBindings {
                projections,
                distinct,
            } => AstNode::ProjectBindings {
                input,
                projections,
                distinct,
            },
            Self::EdgeProperties => AstNode::EdgeProperties { input },
            Self::AddN { label, properties } => AstNode::AddN {
                input: Some(input),
                label,
                properties,
            },
            Self::AddE {
                label,
                to,
                properties,
            } => AstNode::AddE {
                input,
                label,
                to,
                properties,
            },
            Self::SetProperty(name, value) => AstNode::SetProperty { input, name, value },
            Self::RemoveProperty(name) => AstNode::RemoveProperty { input, name },
            Self::Drop => AstNode::Drop { input },
            Self::DropEdge(to) => AstNode::DropEdge { input, to },
            Self::DropEdgeLabeled { to, label } => AstNode::DropEdgeLabeled { input, to, label },
            Self::DropEdgeById(edges) => AstNode::DropEdgeById {
                input: Some(input),
                edges,
            },
            Self::OrderBy(property, order) => AstNode::OrderBy {
                input,
                property,
                order,
            },
            Self::OrderByMultiple(orderings) => AstNode::OrderByMultiple { input, orderings },
            Self::Repeat(config) => AstNode::Repeat { input, config },
            Self::Union(traversals) => AstNode::Union { input, traversals },
            Self::Choose {
                condition,
                then_traversal,
                else_traversal,
            } => AstNode::Choose {
                input,
                condition,
                then_traversal,
                else_traversal,
            },
            Self::Coalesce(traversals) => AstNode::Coalesce { input, traversals },
            Self::Optional(traversal) => AstNode::Optional { input, traversal },
            Self::Group(property) => AstNode::Group { input, property },
            Self::GroupCount(property) => AstNode::GroupCount { input, property },
            Self::AggregateBy(function, property) => AstNode::AggregateBy {
                input,
                function,
                property,
            },
            Self::Fold => AstNode::Fold { input },
            Self::Unfold => AstNode::Unfold { input },
            Self::Path => AstNode::Path { input },
            Self::SimplePath => AstNode::SimplePath { input },
            Self::WithSack(initial) => AstNode::WithSack { input, initial },
            Self::SackSet(property) => AstNode::SackSet { input, property },
            Self::SackAdd(property) => AstNode::SackAdd { input, property },
            Self::SackGet => AstNode::SackGet { input },
        }
    }
}

/// Sub-traversal for branching operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubTraversal {
    /// Root node. The default root is [`AstNode::Context`].
    pub root: Box<AstNode>,
}

impl Default for SubTraversal {
    fn default() -> Self {
        Self::new()
    }
}

impl SubTraversal {
    /// Create an empty sub-traversal that starts from parent context.
    pub fn new() -> Self {
        Self {
            root: Box::new(AstNode::Context),
        }
    }

    fn push(mut self, operation: Operation) -> Self {
        self.root = Box::new(operation.apply(*self.root));
        self
    }

    /// Traverse outgoing edges.
    pub fn out(self, label: Option<impl Into<String>>) -> Self {
        self.push(Operation::Out(label.map(Into::into)))
    }

    /// Traverse incoming edges.
    pub fn in_(self, label: Option<impl Into<String>>) -> Self {
        self.push(Operation::In(label.map(Into::into)))
    }

    /// Traverse both directions.
    pub fn both(self, label: Option<impl Into<String>>) -> Self {
        self.push(Operation::Both(label.map(Into::into)))
    }

    /// Traverse to outgoing edges.
    pub fn out_e(self, label: Option<impl Into<String>>) -> Self {
        self.push(Operation::OutE(label.map(Into::into)))
    }

    /// Traverse to incoming edges.
    pub fn in_e(self, label: Option<impl Into<String>>) -> Self {
        self.push(Operation::InE(label.map(Into::into)))
    }

    /// Traverse to both-direction edges.
    pub fn both_e(self, label: Option<impl Into<String>>) -> Self {
        self.push(Operation::BothE(label.map(Into::into)))
    }

    /// Edge to target node.
    pub fn out_n(self) -> Self {
        self.push(Operation::OutN)
    }

    /// Edge to source node.
    pub fn in_n(self) -> Self {
        self.push(Operation::InN)
    }

    /// Edge to other node.
    pub fn other_n(self) -> Self {
        self.push(Operation::OtherN)
    }

    /// Property equality filter.
    pub fn has(self, property: impl Into<String>, value: impl Into<PropertyValue>) -> Self {
        self.push(Operation::Has(property.into(), value.into()))
    }

    /// Label filter.
    pub fn has_label(self, label: impl Into<String>) -> Self {
        self.push(Operation::HasLabel(label.into()))
    }

    /// Property existence filter.
    pub fn has_key(self, property: impl Into<String>) -> Self {
        self.push(Operation::HasKey(property.into()))
    }

    /// Predicate filter.
    pub fn where_(self, predicate: Predicate) -> Self {
        self.push(Operation::Where(predicate))
    }

    /// Deduplicate stream.
    pub fn dedup(self) -> Self {
        self.push(Operation::Dedup)
    }

    /// Keep within variable.
    pub fn within(self, var_name: impl Into<String>) -> Self {
        self.push(Operation::Within(var_name.into()))
    }

    /// Keep outside variable.
    pub fn without(self, var_name: impl Into<String>) -> Self {
        self.push(Operation::Without(var_name.into()))
    }

    /// Edge property filter.
    pub fn edge_has(self, property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        self.push(Operation::EdgeHas(property.into(), value.into()))
    }

    /// Edge label filter.
    pub fn edge_has_label(self, label: impl Into<String>) -> Self {
        self.push(Operation::EdgeHasLabel(label.into()))
    }

    /// Limit stream.
    pub fn limit(self, n: impl Into<StreamBound>) -> Self {
        self.push(Operation::Limit(n.into()))
    }

    /// Skip stream.
    pub fn skip(self, n: impl Into<StreamBound>) -> Self {
        self.push(Operation::Skip(n.into()))
    }

    /// Range stream.
    pub fn range(self, start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Self {
        self.push(Operation::Range(start.into(), end.into()))
    }

    /// Store stream.
    pub fn as_(self, name: impl Into<String>) -> Self {
        self.push(Operation::As(name.into()))
    }

    /// Store stream.
    pub fn store(self, name: impl Into<String>) -> Self {
        self.push(Operation::Store(name.into()))
    }

    /// Select stream.
    pub fn select(self, name: impl Into<String>) -> Self {
        self.push(Operation::Select(name.into()))
    }

    /// Capture row-local binding.
    pub fn bind(self, name: impl Into<String>) -> Self {
        self.push(Operation::Bind(validate_binding_name(name)))
    }

    /// Order by one property.
    pub fn order_by(self, property: impl Into<String>, order: Order) -> Self {
        self.push(Operation::OrderBy(property.into(), order))
    }

    /// Order by multiple properties.
    pub fn order_by_multiple(self, orderings: Vec<(impl Into<String>, Order)>) -> Self {
        self.push(Operation::OrderByMultiple(
            orderings
                .into_iter()
                .map(|(property, order)| (property.into(), order))
                .collect(),
        ))
    }

    /// Path operation.
    pub fn path(self) -> Self {
        self.push(Operation::Path)
    }

    /// Simple-path operation.
    pub fn simple_path(self) -> Self {
        self.push(Operation::SimplePath)
    }
}

/// Create a sub-traversal.
pub fn sub() -> SubTraversal {
    SubTraversal::new()
}

/// Repeat configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepeatConfig {
    /// Traversal body.
    pub traversal: SubTraversal,
    /// Optional fixed iteration count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub times: Option<usize>,
    /// Optional stop predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<Predicate>,
    /// Emit behavior.
    pub emit: EmitBehavior,
    /// Optional emit predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emit_predicate: Option<Predicate>,
    /// Maximum depth.
    pub max_depth: usize,
}

impl RepeatConfig {
    /// Create repeat config.
    pub fn new(traversal: SubTraversal) -> Self {
        Self {
            traversal,
            times: None,
            until: None,
            emit: EmitBehavior::None,
            emit_predicate: None,
            max_depth: 100,
        }
    }

    /// Set times.
    pub fn times(mut self, n: usize) -> Self {
        self.times = Some(n);
        self
    }

    /// Set until predicate.
    pub fn until(mut self, predicate: Predicate) -> Self {
        self.until = Some(predicate);
        self
    }

    /// Emit before and after.
    pub fn emit_all(mut self) -> Self {
        self.emit = EmitBehavior::All;
        self
    }

    /// Emit before.
    pub fn emit_before(mut self) -> Self {
        self.emit = EmitBehavior::Before;
        self
    }

    /// Emit after.
    pub fn emit_after(mut self) -> Self {
        self.emit = EmitBehavior::After;
        self
    }

    /// Emit matching after states.
    pub fn emit_if(mut self, predicate: Predicate) -> Self {
        self.emit = EmitBehavior::After;
        self.emit_predicate = Some(predicate);
        self
    }

    /// Set maximum depth.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }
}

/// A traversal builder with typestate.
#[derive(Debug, Clone, PartialEq)]
pub struct Traversal<S: TraversalState = OnNodes, M: MutationMode = ReadOnly> {
    root: Option<AstNode>,
    _state: PhantomData<S>,
    _mode: PhantomData<M>,
}

impl<S: TraversalState, M: MutationMode> Default for Traversal<S, M> {
    fn default() -> Self {
        Self {
            root: None,
            _state: PhantomData,
            _mode: PhantomData,
        }
    }
}

impl<S: TraversalState, M: MutationMode> Traversal<S, M> {
    /// Consume this traversal into its root AST node.
    pub fn into_ast(self) -> AstNode {
        self.root
            .expect("traversal must contain at least one AST node before execution")
    }

    /// Borrow the root AST node.
    pub fn root(&self) -> Option<&AstNode> {
        self.root.as_ref()
    }

    /// Returns true if the root node is terminal.
    pub fn has_terminal(&self) -> bool {
        self.root.as_ref().is_some_and(AstNode::is_terminal)
    }

    fn from_root<T: TraversalState>(root: AstNode) -> Traversal<T, M> {
        Traversal {
            root: Some(root),
            _state: PhantomData,
            _mode: PhantomData,
        }
    }

    fn push<T: TraversalState>(self, operation: Operation) -> Traversal<T, M> {
        let root = self
            .root
            .expect("cannot append traversal operation before a source node");
        Traversal::<T, M>::from_root(operation.apply(root))
    }

    fn push_mutation<T: TraversalState>(self, operation: Operation) -> Traversal<T, WriteEnabled> {
        let root = self
            .root
            .expect("cannot append mutation operation before a source node");
        Traversal {
            root: Some(operation.apply(root)),
            _state: PhantomData,
            _mode: PhantomData,
        }
    }
}

impl Traversal<Empty, ReadOnly> {
    /// Create an empty traversal.
    pub fn new() -> Self {
        Self::default()
    }

    fn source<T: TraversalState>(self, root: AstNode) -> Traversal<T, ReadOnly> {
        assert!(
            self.root.is_none(),
            "source operation cannot be appended to an existing traversal"
        );
        Traversal {
            root: Some(root),
            _state: PhantomData,
            _mode: PhantomData,
        }
    }

    fn mutation_source<T: TraversalState>(self, root: AstNode) -> Traversal<T, WriteEnabled> {
        assert!(
            self.root.is_none(),
            "source mutation cannot be appended to an existing traversal"
        );
        Traversal {
            root: Some(root),
            _state: PhantomData,
            _mode: PhantomData,
        }
    }

    /// Start from nodes.
    pub fn n(self, nodes: impl Into<NodeRef>) -> Traversal<OnNodes> {
        self.source(AstNode::Nodes {
            reference: nodes.into(),
        })
    }

    /// Start from nodes matching a predicate.
    pub fn n_where(self, predicate: SourcePredicate) -> Traversal<OnNodes> {
        self.source(AstNode::NodesWhere { predicate })
    }

    /// Start from nodes with a label.
    pub fn n_with_label(self, label: impl Into<String>) -> Traversal<OnNodes> {
        self.n_where(Predicate::eq("$label", label.into()))
    }

    /// Start from nodes with a label and predicate.
    pub fn n_with_label_where(
        self,
        label: impl Into<String>,
        predicate: SourcePredicate,
    ) -> Traversal<OnNodes> {
        self.n_where(Predicate::and(vec![
            Predicate::eq("$label", label.into()),
            predicate,
        ]))
    }

    /// Start from node vector search.
    pub fn vector_search_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: Vec<f32>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnNodes> {
        self.vector_search_nodes_with(
            label,
            property,
            query_vector,
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start from node vector search with runtime inputs.
    pub fn vector_search_nodes_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnNodes> {
        self.source(AstNode::VectorSearchNodes {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_vector: query_vector.into(),
            k: k.into(),
        })
    }

    /// Start from node text search.
    pub fn text_search_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<String>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnNodes> {
        self.text_search_nodes_with(
            label,
            property,
            PropertyInput::from(query_text.into()),
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start from node text search with runtime inputs.
    pub fn text_search_nodes_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnNodes> {
        self.source(AstNode::TextSearchNodes {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_text: query_text.into(),
            k: k.into(),
        })
    }

    /// Start from edges.
    pub fn e(self, edges: impl Into<EdgeRef>) -> Traversal<OnEdges> {
        self.source(AstNode::Edges {
            reference: edges.into(),
        })
    }

    /// Start from edges matching a predicate.
    pub fn e_where(self, predicate: SourcePredicate) -> Traversal<OnEdges> {
        self.source(AstNode::EdgesWhere { predicate })
    }

    /// Start from edges with a label.
    pub fn e_with_label(self, label: impl Into<String>) -> Traversal<OnEdges> {
        self.e_where(Predicate::eq("$label", label.into()))
    }

    /// Start from edges with a label and predicate.
    pub fn e_with_label_where(
        self,
        label: impl Into<String>,
        predicate: SourcePredicate,
    ) -> Traversal<OnEdges> {
        self.e_where(Predicate::and(vec![
            Predicate::eq("$label", label.into()),
            predicate,
        ]))
    }

    /// Start from edge vector search.
    pub fn vector_search_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: Vec<f32>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnEdges> {
        self.vector_search_edges_with(
            label,
            property,
            query_vector,
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start from edge vector search with runtime inputs.
    pub fn vector_search_edges_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnEdges> {
        self.source(AstNode::VectorSearchEdges {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_vector: query_vector.into(),
            k: k.into(),
        })
    }

    /// Start from edge text search.
    pub fn text_search_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<String>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnEdges> {
        self.text_search_edges_with(
            label,
            property,
            PropertyInput::from(query_text.into()),
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start from edge text search with runtime inputs.
    pub fn text_search_edges_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnEdges> {
        self.source(AstNode::TextSearchEdges {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_text: query_text.into(),
            k: k.into(),
        })
    }

    /// Find an unweighted outgoing shortest path between two nodes.
    pub fn shortest_path(
        self,
        source: impl Into<NodeRef>,
        target: impl Into<NodeRef>,
        max_depth: usize,
    ) -> Traversal<Terminal> {
        self.shortest_path_with(
            source,
            target,
            None::<String>,
            ShortestPathDirection::Out,
            max_depth,
        )
    }

    /// Find an unweighted shortest path between two nodes.
    pub fn shortest_path_with(
        self,
        source: impl Into<NodeRef>,
        target: impl Into<NodeRef>,
        label: Option<impl Into<String>>,
        direction: ShortestPathDirection,
        max_depth: usize,
    ) -> Traversal<Terminal> {
        self.source(AstNode::ShortestPath {
            source: source.into(),
            target: target.into(),
            label: label.map(Into::into),
            direction,
            max_depth,
        })
    }

    /// Create an index if it does not already exist.
    pub fn create_index_if_not_exists(self, spec: IndexSpec) -> Traversal<Terminal, WriteEnabled> {
        self.mutation_source(AstNode::CreateIndex {
            spec,
            if_not_exists: true,
        })
    }

    /// Drop an index.
    pub fn drop_index(self, spec: IndexSpec) -> Traversal<Terminal, WriteEnabled> {
        self.mutation_source(AstNode::DropIndex { spec })
    }

    /// Read one retained index operation in this request's storage scope.
    pub fn get_index_operation(self, operation_id: impl Into<String>) -> Traversal<Terminal> {
        self.source(AstNode::GetIndexOperation {
            operation_id: operation_id.into(),
        })
    }

    /// Convergently ensure one retained operation is runnable.
    pub fn retry_index_operation(
        self,
        operation_id: impl Into<String>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.mutation_source(AstNode::RetryIndexOperation {
            operation_id: operation_id.into(),
        })
    }

    /// Convert one constructing BUILD into abort cleanup.
    pub fn abort_index_operation(
        self,
        operation_id: impl Into<String>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.mutation_source(AstNode::AbortIndexOperation {
            operation_id: operation_id.into(),
        })
    }

    /// Create a node vector index.
    pub fn create_vector_index_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: std::num::NonZeroUsize,
        metric: crate::index::VectorDistanceMetric,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::node_vector(
            label,
            property,
            dimension,
            metric,
            tenant_property,
        ))
    }

    /// Create an edge vector index.
    pub fn create_vector_index_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: std::num::NonZeroUsize,
        metric: crate::index::VectorDistanceMetric,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::edge_vector(
            label,
            property,
            dimension,
            metric,
            tenant_property,
        ))
    }

    /// Create a node text index.
    pub fn create_text_index_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::node_text(label, property, tenant_property))
    }

    /// Create an edge text index.
    pub fn create_text_index_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::edge_text(label, property, tenant_property))
    }

    /// Add a node.
    pub fn add_n<K, V>(
        self,
        label: impl Into<String>,
        properties: Vec<(K, V)>,
    ) -> Traversal<OnNodes, WriteEnabled>
    where
        K: Into<String>,
        V: Into<PropertyInput>,
    {
        self.mutation_source(AstNode::AddN {
            input: None,
            label: label.into(),
            properties: collect_properties(properties),
        })
    }

    /// Source-inject a variable.
    pub fn inject(self, var_name: impl Into<String>) -> Traversal<OnNodes, ReadOnly> {
        self.source(AstNode::Inject {
            input: None,
            variable: var_name.into(),
        })
    }

    /// Drop edges by ID without a source stream.
    pub fn drop_edge_by_id(self, edges: impl Into<EdgeRef>) -> Traversal<OnNodes, WriteEnabled> {
        self.mutation_source(AstNode::DropEdgeById {
            input: None,
            edges: edges.into(),
        })
    }
}

fn collect_properties<K, V>(properties: Vec<(K, V)>) -> Vec<(String, PropertyInput)>
where
    K: Into<String>,
    V: Into<PropertyInput>,
{
    properties
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
}

impl<M: MutationMode> Traversal<OnNodes, M> {
    /// Traverse outgoing edges to nodes.
    pub fn out(self, label: Option<impl Into<String>>) -> Traversal<OnNodes, M> {
        self.push(Operation::Out(label.map(Into::into)))
    }

    /// Traverse incoming edges to nodes.
    pub fn in_(self, label: Option<impl Into<String>>) -> Traversal<OnNodes, M> {
        self.push(Operation::In(label.map(Into::into)))
    }

    /// Traverse both directions to nodes.
    pub fn both(self, label: Option<impl Into<String>>) -> Traversal<OnNodes, M> {
        self.push(Operation::Both(label.map(Into::into)))
    }

    /// Traverse to outgoing edges.
    pub fn out_e(self, label: Option<impl Into<String>>) -> Traversal<OnEdges, M> {
        self.push(Operation::OutE(label.map(Into::into)))
    }

    /// Traverse to incoming edges.
    pub fn in_e(self, label: Option<impl Into<String>>) -> Traversal<OnEdges, M> {
        self.push(Operation::InE(label.map(Into::into)))
    }

    /// Traverse to both-direction edges.
    pub fn both_e(self, label: Option<impl Into<String>>) -> Traversal<OnEdges, M> {
        self.push(Operation::BothE(label.map(Into::into)))
    }

    /// Property equality filter.
    pub fn has(self, property: impl Into<String>, value: impl Into<PropertyValue>) -> Self {
        self.push(Operation::Has(property.into(), value.into()))
    }

    /// Label filter.
    pub fn has_label(self, label: impl Into<String>) -> Self {
        self.push(Operation::HasLabel(label.into()))
    }

    /// Property existence filter.
    pub fn has_key(self, property: impl Into<String>) -> Self {
        self.push(Operation::HasKey(property.into()))
    }

    /// Predicate filter.
    pub fn where_(self, predicate: Predicate) -> Self {
        self.push(Operation::Where(predicate))
    }

    /// Deduplicate.
    pub fn dedup(self) -> Self {
        self.push(Operation::Dedup)
    }

    /// Keep within variable.
    pub fn within(self, var_name: impl Into<String>) -> Self {
        self.push(Operation::Within(var_name.into()))
    }

    /// Keep outside variable.
    pub fn without(self, var_name: impl Into<String>) -> Self {
        self.push(Operation::Without(var_name.into()))
    }

    /// Limit.
    pub fn limit(self, n: impl Into<StreamBound>) -> Self {
        self.push(Operation::Limit(n.into()))
    }

    /// Skip.
    pub fn skip(self, n: impl Into<StreamBound>) -> Self {
        self.push(Operation::Skip(n.into()))
    }

    /// Range.
    pub fn range(self, start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Self {
        self.push(Operation::Range(start.into(), end.into()))
    }

    /// Store stream.
    pub fn as_(self, name: impl Into<String>) -> Self {
        self.push(Operation::As(name.into()))
    }

    /// Store stream.
    pub fn store(self, name: impl Into<String>) -> Self {
        self.push(Operation::Store(name.into()))
    }

    /// Select stream.
    pub fn select(self, name: impl Into<String>) -> Self {
        self.push(Operation::Select(name.into()))
    }

    /// Bind current row element.
    pub fn bind(self, name: impl Into<String>) -> Self {
        self.push(Operation::Bind(validate_binding_name(name)))
    }

    /// Inject variable stream.
    pub fn inject(self, var_name: impl Into<String>) -> Self {
        self.push(Operation::Inject(var_name.into()))
    }

    /// Count terminal.
    pub fn count(self) -> Traversal<Terminal, M> {
        self.push(Operation::Count)
    }

    /// Exists terminal.
    pub fn exists(self) -> Traversal<Terminal, M> {
        self.push(Operation::Exists)
    }

    /// ID terminal.
    pub fn id(self) -> Traversal<Terminal, M> {
        self.push(Operation::Id)
    }

    /// Label terminal.
    pub fn label(self) -> Traversal<Terminal, M> {
        self.push(Operation::Label)
    }

    /// Values terminal.
    pub fn values(self, properties: Vec<impl Into<String>>) -> Traversal<Terminal, M> {
        self.push(Operation::Values(
            properties.into_iter().map(Into::into).collect(),
        ))
    }

    /// Value-map terminal.
    pub fn value_map(self, properties: Option<Vec<impl Into<String>>>) -> Traversal<Terminal, M> {
        self.push(Operation::ValueMap(
            properties.map(|items| items.into_iter().map(Into::into).collect()),
        ))
    }

    /// Project terminal.
    pub fn project<P>(self, projections: Vec<P>) -> Traversal<Terminal, M>
    where
        P: Into<Projection>,
    {
        self.push(Operation::Project(
            projections.into_iter().map(Into::into).collect(),
        ))
    }

    /// Project row bindings.
    pub fn project_bindings(self, projections: Vec<BindingProjection>) -> Traversal<Terminal, M> {
        self.push(Operation::ProjectBindings {
            projections: validate_binding_projections(projections),
            distinct: false,
        })
    }

    /// Project distinct row bindings.
    pub fn project_distinct_bindings(
        self,
        projections: Vec<BindingProjection>,
    ) -> Traversal<Terminal, M> {
        self.push(Operation::ProjectBindings {
            projections: validate_binding_projections(projections),
            distinct: true,
        })
    }

    /// Order by one property.
    pub fn order_by(self, property: impl Into<String>, order: Order) -> Self {
        self.push(Operation::OrderBy(property.into(), order))
    }

    /// Order by multiple properties.
    pub fn order_by_multiple(self, orderings: Vec<(impl Into<String>, Order)>) -> Self {
        self.push(Operation::OrderByMultiple(
            orderings
                .into_iter()
                .map(|(property, order)| (property.into(), order))
                .collect(),
        ))
    }

    /// Repeat traversal.
    pub fn repeat(self, config: RepeatConfig) -> Self {
        self.push(Operation::Repeat(config))
    }

    /// Union branches.
    pub fn union(self, traversals: Vec<SubTraversal>) -> Self {
        self.push(Operation::Union(traversals))
    }

    /// Conditional branch.
    pub fn choose(
        self,
        condition: Predicate,
        then_traversal: SubTraversal,
        else_traversal: Option<SubTraversal>,
    ) -> Self {
        self.push(Operation::Choose {
            condition,
            then_traversal,
            else_traversal,
        })
    }

    /// Coalesce branches.
    pub fn coalesce(self, traversals: Vec<SubTraversal>) -> Self {
        self.push(Operation::Coalesce(traversals))
    }

    /// Optional branch.
    pub fn optional(self, traversal: SubTraversal) -> Self {
        self.push(Operation::Optional(traversal))
    }

    /// Group terminal.
    pub fn group(self, property: impl Into<String>) -> Traversal<Terminal, M> {
        self.push(Operation::Group(property.into()))
    }

    /// Group-count terminal.
    pub fn group_count(self, property: impl Into<String>) -> Traversal<Terminal, M> {
        self.push(Operation::GroupCount(property.into()))
    }

    /// Aggregate terminal.
    pub fn aggregate_by(
        self,
        function: AggregateFunction,
        property: impl Into<String>,
    ) -> Traversal<Terminal, M> {
        self.push(Operation::AggregateBy(function, property.into()))
    }

    /// Fold barrier.
    pub fn fold(self) -> Self {
        self.push(Operation::Fold)
    }

    /// Unfold barrier.
    pub fn unfold(self) -> Self {
        self.push(Operation::Unfold)
    }

    /// Path operation.
    pub fn path(self) -> Self {
        self.push(Operation::Path)
    }

    /// Simple-path operation.
    pub fn simple_path(self) -> Self {
        self.push(Operation::SimplePath)
    }

    /// Initialize sack.
    pub fn with_sack(self, initial: PropertyValue) -> Self {
        self.push(Operation::WithSack(initial))
    }

    /// Set sack.
    pub fn sack_set(self, property: impl Into<String>) -> Self {
        self.push(Operation::SackSet(property.into()))
    }

    /// Add to sack.
    pub fn sack_add(self, property: impl Into<String>) -> Self {
        self.push(Operation::SackAdd(property.into()))
    }

    /// Get sack.
    pub fn sack_get(self) -> Self {
        self.push(Operation::SackGet)
    }

    /// Add a node.
    pub fn add_n<K, V>(
        self,
        label: impl Into<String>,
        properties: Vec<(K, V)>,
    ) -> Traversal<OnNodes, WriteEnabled>
    where
        K: Into<String>,
        V: Into<PropertyInput>,
    {
        self.push_mutation(Operation::AddN {
            label: label.into(),
            properties: collect_properties(properties),
        })
    }

    /// Add edges.
    pub fn add_e<K, V>(
        self,
        label: impl Into<String>,
        to: impl Into<NodeRef>,
        properties: Vec<(K, V)>,
    ) -> Traversal<OnNodes, WriteEnabled>
    where
        K: Into<String>,
        V: Into<PropertyInput>,
    {
        self.push_mutation(Operation::AddE {
            label: label.into(),
            to: to.into(),
            properties: collect_properties(properties),
        })
    }

    /// Set property.
    pub fn set_property(
        self,
        name: impl Into<String>,
        value: impl Into<PropertyInput>,
    ) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation(Operation::SetProperty(name.into(), value.into()))
    }

    /// Remove property.
    pub fn remove_property(self, name: impl Into<String>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation(Operation::RemoveProperty(name.into()))
    }

    /// Drop nodes.
    pub fn drop(self) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation(Operation::Drop)
    }

    /// Drop edges.
    pub fn drop_edge(self, to: impl Into<NodeRef>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation(Operation::DropEdge(to.into()))
    }

    /// Drop labeled edges.
    pub fn drop_edge_labeled(
        self,
        to: impl Into<NodeRef>,
        label: impl Into<String>,
    ) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation(Operation::DropEdgeLabeled {
            to: to.into(),
            label: label.into(),
        })
    }

    /// Drop edges by ID.
    pub fn drop_edge_by_id(self, edges: impl Into<EdgeRef>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation(Operation::DropEdgeById(edges.into()))
    }
}

impl<M: MutationMode> Traversal<OnEdges, M> {
    /// Edge to target node.
    pub fn out_n(self) -> Traversal<OnNodes, M> {
        self.push(Operation::OutN)
    }

    /// Edge to source node.
    pub fn in_n(self) -> Traversal<OnNodes, M> {
        self.push(Operation::InN)
    }

    /// Edge to other node.
    pub fn other_n(self) -> Traversal<OnNodes, M> {
        self.push(Operation::OtherN)
    }

    /// Property equality filter.
    pub fn has(self, property: impl Into<String>, value: impl Into<PropertyValue>) -> Self {
        self.push(Operation::Has(property.into(), value.into()))
    }

    /// Label filter.
    pub fn has_label(self, label: impl Into<String>) -> Self {
        self.push(Operation::HasLabel(label.into()))
    }

    /// Property existence filter.
    pub fn has_key(self, property: impl Into<String>) -> Self {
        self.push(Operation::HasKey(property.into()))
    }

    /// Predicate filter.
    pub fn where_(self, predicate: Predicate) -> Self {
        self.push(Operation::Where(predicate))
    }

    /// Edge property filter.
    pub fn edge_has(self, property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        self.push(Operation::EdgeHas(property.into(), value.into()))
    }

    /// Edge label filter.
    pub fn edge_has_label(self, label: impl Into<String>) -> Self {
        self.push(Operation::EdgeHasLabel(label.into()))
    }

    /// Set a property on current edges.
    pub fn set_property(
        self,
        name: impl Into<String>,
        value: impl Into<PropertyInput>,
    ) -> Traversal<OnEdges, WriteEnabled> {
        self.push_mutation(Operation::SetProperty(name.into(), value.into()))
    }

    /// Remove a property from current edges.
    pub fn remove_property(self, name: impl Into<String>) -> Traversal<OnEdges, WriteEnabled> {
        self.push_mutation(Operation::RemoveProperty(name.into()))
    }

    /// Deduplicate.
    pub fn dedup(self) -> Self {
        self.push(Operation::Dedup)
    }

    /// Limit.
    pub fn limit(self, n: impl Into<StreamBound>) -> Self {
        self.push(Operation::Limit(n.into()))
    }

    /// Skip.
    pub fn skip(self, n: impl Into<StreamBound>) -> Self {
        self.push(Operation::Skip(n.into()))
    }

    /// Range.
    pub fn range(self, start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Self {
        self.push(Operation::Range(start.into(), end.into()))
    }

    /// Store stream.
    pub fn as_(self, name: impl Into<String>) -> Self {
        self.push(Operation::As(name.into()))
    }

    /// Store stream.
    pub fn store(self, name: impl Into<String>) -> Self {
        self.push(Operation::Store(name.into()))
    }

    /// Bind current row element.
    pub fn bind(self, name: impl Into<String>) -> Self {
        self.push(Operation::Bind(validate_binding_name(name)))
    }

    /// Count terminal.
    pub fn count(self) -> Traversal<Terminal, M> {
        self.push(Operation::Count)
    }

    /// Exists terminal.
    pub fn exists(self) -> Traversal<Terminal, M> {
        self.push(Operation::Exists)
    }

    /// ID terminal.
    pub fn id(self) -> Traversal<Terminal, M> {
        self.push(Operation::Id)
    }

    /// Label terminal.
    pub fn label(self) -> Traversal<Terminal, M> {
        self.push(Operation::Label)
    }

    /// Values terminal.
    pub fn values(self, properties: Vec<impl Into<String>>) -> Traversal<Terminal, M> {
        self.push(Operation::Values(
            properties.into_iter().map(Into::into).collect(),
        ))
    }

    /// Value-map terminal.
    pub fn value_map(self, properties: Option<Vec<impl Into<String>>>) -> Traversal<Terminal, M> {
        self.push(Operation::ValueMap(
            properties.map(|items| items.into_iter().map(Into::into).collect()),
        ))
    }

    /// Project terminal.
    pub fn project<P>(self, projections: Vec<P>) -> Traversal<Terminal, M>
    where
        P: Into<Projection>,
    {
        self.push(Operation::Project(
            projections.into_iter().map(Into::into).collect(),
        ))
    }

    /// Project row bindings.
    pub fn project_bindings(self, projections: Vec<BindingProjection>) -> Traversal<Terminal, M> {
        self.push(Operation::ProjectBindings {
            projections: validate_binding_projections(projections),
            distinct: false,
        })
    }

    /// Project distinct row bindings.
    pub fn project_distinct_bindings(
        self,
        projections: Vec<BindingProjection>,
    ) -> Traversal<Terminal, M> {
        self.push(Operation::ProjectBindings {
            projections: validate_binding_projections(projections),
            distinct: true,
        })
    }

    /// Edge-properties terminal.
    pub fn edge_properties(self) -> Traversal<Terminal, M> {
        self.push(Operation::EdgeProperties)
    }

    /// Order by one property.
    pub fn order_by(self, property: impl Into<String>, order: Order) -> Self {
        self.push(Operation::OrderBy(property.into(), order))
    }
}

/// Create a traversal.
pub fn g() -> Traversal<Empty> {
    Traversal::new()
}
