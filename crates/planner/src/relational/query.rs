use super::{Expression, QueryError, Result, Slot};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BindingType {
    Node,
    Relationship,
    Path,
    Scalar,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Binding {
    pub name: String,
    pub kind: BindingType,
    pub nullable: bool,
    pub value_type: super::ValueType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodePattern {
    pub slot: Slot,
    pub label: Option<String>,
    pub properties: Vec<(String, Expression)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RelationshipPattern {
    pub slot: Slot,
    pub from: Slot,
    pub to: Slot,
    pub direction: Direction,
    pub types: Vec<String>,
    pub properties: Vec<(String, Expression)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PathPattern {
    pub slot: Slot,
    pub nodes: Vec<Slot>,
    pub relationships: Vec<Slot>,
}

/// A conjunction of patterns; relationship uniqueness is scoped to this match.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Pattern {
    pub nodes: Vec<NodePattern>,
    pub relationships: Vec<RelationshipPattern>,
    pub paths: Vec<PathPattern>,
}

impl Pattern {
    /// A node-only pattern that can resume one correlated binding at a time.
    /// Repeated occurrences retain all their constraints and named paths.
    pub fn single_node(&self) -> Option<Slot> {
        let slot = self.nodes.first()?.slot;
        (self.relationships.is_empty() && self.nodes.iter().all(|node| node.slot == slot))
            .then_some(slot)
    }

    pub fn slots(&self) -> BTreeSet<Slot> {
        self.nodes
            .iter()
            .map(|n| n.slot)
            .chain(self.relationships.iter().map(|r| r.slot))
            .chain(self.paths.iter().map(|p| p.slot))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Ordering {
    pub expression: Expression,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PropertyMutation {
    Set {
        entity: Slot,
        key: String,
        value: Expression,
    },
    Remove {
        entity: Slot,
        key: String,
    },
    Replace {
        entity: Slot,
        properties: Expression,
    },
    Extend {
        entity: Slot,
        properties: Expression,
    },
}

/// Logical operators retain graph, scope, and effect boundaries explicitly.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Operator {
    Match {
        pattern: Pattern,
        optional: bool,
        predicate: Option<super::SelectionProgram>,
    },
    Filter(super::SelectionProgram),
    Unwind {
        expression: Expression,
        slot: Slot,
    },
    Project {
        items: super::ProjectionProgram,
        distinct: bool,
        ordering: Vec<Ordering>,
        predicate: Option<super::SelectionProgram>,
        skip: Option<Expression>,
        limit: Option<Expression>,
    },
    Create(Pattern),
    Update(Vec<PropertyMutation>),
    Delete {
        entities: Vec<Expression>,
        detach: bool,
    },
}

impl Operator {
    /// All scalar roots owned by an operator, including inline pattern
    /// predicates and windows. Used for parameter and dependency validation.
    pub fn expressions(&self) -> Vec<&Expression> {
        match self {
            Self::Match {
                pattern, predicate, ..
            } => pattern
                .expressions()
                .into_iter()
                .chain(predicate.iter().map(|predicate| predicate.expression()))
                .collect(),
            Self::Create(pattern) => pattern.expressions(),
            Self::Filter(predicate) => vec![predicate.expression()],
            Self::Unwind { expression, .. } => vec![expression],
            Self::Project {
                items,
                ordering,
                predicate,
                skip,
                limit,
                ..
            } => items
                .iter()
                .map(|i| &i.expression)
                .chain(ordering.iter().map(|o| &o.expression))
                .chain(predicate.iter().map(|predicate| predicate.expression()))
                .chain(skip.iter())
                .chain(limit.iter())
                .collect(),
            Self::Update(updates) => updates
                .iter()
                .filter_map(|update| match update {
                    PropertyMutation::Set { value, .. } => Some(value),
                    PropertyMutation::Replace { properties, .. }
                    | PropertyMutation::Extend { properties, .. } => Some(properties),
                    PropertyMutation::Remove { .. } => None,
                })
                .collect(),
            Self::Delete { entities, .. } => entities.iter().collect(),
        }
    }
}

impl Pattern {
    fn expressions(&self) -> Vec<&Expression> {
        self.nodes
            .iter()
            .flat_map(|n| &n.properties)
            .chain(self.relationships.iter().flat_map(|r| &r.properties))
            .map(|(_, e)| e)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Effect {
    Read,
    Write,
}

/// Validated resolved query. Deserialization cannot bypass its constructor.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "QueryInput")]
pub struct Query {
    bindings: Vec<Binding>,
    operators: Vec<Operator>,
    returns: Vec<(String, Slot)>,
    effect: Effect,
    contracts: Vec<super::OperatorContract>,
}

#[derive(serde::Deserialize)]
struct QueryInput {
    bindings: Vec<Binding>,
    operators: Vec<Operator>,
    returns: Vec<(String, Slot)>,
}
impl TryFrom<QueryInput> for Query {
    type Error = QueryError;
    fn try_from(input: QueryInput) -> Result<Self> {
        // Effect and schemas are derived, never trusted from serialized input.
        Self::new(input.bindings, input.operators, input.returns)
    }
}

impl Query {
    pub fn parameters(&self) -> BTreeSet<String> {
        let mut parameters = BTreeSet::new();
        for expression in self.operators.iter().flat_map(Operator::expressions) {
            expression.visit(&mut |e| {
                let Expression::Parameter(name) = e else {
                    return;
                };
                parameters.insert(name.clone());
            });
        }
        parameters
    }
    pub fn new(
        bindings: Vec<Binding>,
        operators: Vec<Operator>,
        returns: Vec<(String, Slot)>,
    ) -> Result<Self> {
        if bindings.len() > 4096 || operators.len() > 4096 {
            return Err(QueryError::compile(
                "ResourceLimit",
                "PlanSize",
                "query exceeds the binding or operator budget",
            ));
        }
        let mut defined = BTreeSet::new();
        let mut effect = Effect::Read;
        let mut contracts = Vec::with_capacity(operators.len());
        let mut row_schema = super::RowSchema::empty();
        let check = |expression: &Expression, defined: &BTreeSet<Slot>| -> Result<()> {
            expression.validate_shape()?;
            if expression.slots().iter().any(|s| !defined.contains(s)) {
                return Err(QueryError::compile(
                    "InternalPlannerError",
                    "UnboundSlot",
                    "expression references an undefined row slot",
                ));
            }
            Ok(())
        };
        for operator in &operators {
            let input = row_schema.clone();
            let outputs = match operator {
                Operator::Match { pattern, .. } | Operator::Create(pattern) => pattern.slots(),
                Operator::Unwind { slot, .. } => BTreeSet::from([*slot]),
                Operator::Project { items, .. } => items.iter().map(|i| i.slot).collect(),
                Operator::Filter(_) | Operator::Update(_) | Operator::Delete { .. } => {
                    BTreeSet::new()
                }
            };
            if outputs.iter().any(|s| s.0 as usize >= bindings.len()) {
                return Err(QueryError::compile(
                    "InternalPlannerError",
                    "InvalidSchema",
                    "operator output is outside the binding catalog",
                ));
            }
            match operator {
                Operator::Match { pattern, .. } | Operator::Create(pattern) => {
                    let mut scope = defined.clone();
                    scope.extend(pattern.slots());
                    for node in &pattern.nodes {
                        if node.label.as_ref().is_some_and(String::is_empty) {
                            return Err(QueryError::compile(
                                "InternalPlannerError",
                                "EmptyLabel",
                                "resolved node labels must be nonempty",
                            ));
                        }
                        if bindings.get(node.slot.0 as usize).is_none_or(|b| {
                            b.kind != BindingType::Node
                                && !matches!(
                                    b.value_type,
                                    super::ValueType::Any | super::ValueType::Null
                                )
                        }) {
                            return Err(QueryError::compile(
                                "InternalPlannerError",
                                "InvalidNodeSlot",
                                "node pattern must bind a node",
                            ));
                        }
                        for (_, e) in &node.properties {
                            check(e, &scope)?;
                        }
                    }
                    for rel in &pattern.relationships {
                        if rel.types.iter().any(String::is_empty)
                            || [rel.from, rel.to]
                                .iter()
                                .any(|slot| !pattern.nodes.iter().any(|node| node.slot == *slot))
                        {
                            return Err(QueryError::compile("InternalPlannerError", "InvalidRelationshipPattern", "relationship endpoints must be node pattern slots and types must be nonempty"));
                        }
                        if bindings.get(rel.slot.0 as usize).is_none_or(|b| {
                            b.kind != BindingType::Relationship
                                && !matches!(
                                    b.value_type,
                                    super::ValueType::Any | super::ValueType::Null
                                )
                        }) || !scope.contains(&rel.from)
                            || !scope.contains(&rel.to)
                        {
                            return Err(QueryError::compile(
                                "InternalPlannerError",
                                "InvalidRelationshipSlot",
                                "relationship pattern has invalid endpoints or binding",
                            ));
                        }
                        for (_, e) in &rel.properties {
                            check(e, &scope)?;
                        }
                    }
                    for path in &pattern.paths {
                        if bindings[path.slot.0 as usize].kind != BindingType::Path
                            || pattern.nodes.iter().any(|node| node.slot == path.slot)
                            || pattern
                                .relationships
                                .iter()
                                .any(|rel| rel.slot == path.slot)
                            || path.nodes.iter().any(|s| {
                                !scope.contains(s)
                                    || !pattern.nodes.iter().any(|node| node.slot == *s)
                                    || (bindings[s.0 as usize].kind != BindingType::Node
                                        && !matches!(
                                            bindings[s.0 as usize].value_type,
                                            super::ValueType::Any | super::ValueType::Null
                                        ))
                            })
                            || path.relationships.iter().any(|s| {
                                !scope.contains(s)
                                    || (bindings[s.0 as usize].kind != BindingType::Relationship
                                        && !matches!(
                                            bindings[s.0 as usize].value_type,
                                            super::ValueType::Any | super::ValueType::Null
                                        ))
                            })
                        {
                            return Err(QueryError::compile(
                                "InternalPlannerError",
                                "InvalidPath",
                                "path members must belong to their pattern and have compatible types; the path output must be separate",
                            ));
                        }
                        if path.nodes.len() != path.relationships.len().saturating_add(1) {
                            return Err(QueryError::compile(
                                "InternalPlannerError",
                                "InvalidPath",
                                "path must alternate nodes and relationships",
                            ));
                        }
                        for (index, slot) in path.relationships.iter().enumerate() {
                            let connects = pattern.relationships.iter().any(|rel| {
                                rel.slot == *slot
                                    && ((rel.from == path.nodes[index]
                                        && rel.to == path.nodes[index + 1])
                                        || (rel.to == path.nodes[index]
                                            && rel.from == path.nodes[index + 1]))
                            });
                            if !connects {
                                return Err(QueryError::compile(
                                    "InternalPlannerError",
                                    "InvalidPath",
                                    "path relationship does not connect its adjacent nodes",
                                ));
                            }
                        }
                    }
                    if let Operator::Match {
                        predicate: Some(e), ..
                    } = operator
                    {
                        e.validate_input(&scope)?;
                    }
                    defined = scope;
                    if matches!(operator, Operator::Create(_)) {
                        effect = Effect::Write;
                    }
                }
                Operator::Filter(e) => e.validate_input(&defined)?,
                Operator::Unwind { expression, slot } => {
                    check(expression, &defined)?;
                    defined.insert(*slot);
                }
                Operator::Project {
                    items,
                    ordering,
                    predicate,
                    skip,
                    limit,
                    ..
                } => {
                    items.validate_input(&defined)?;
                    let mut order_scope = defined.clone();
                    order_scope.extend(items.iter().map(|i| i.slot));
                    for order in ordering {
                        check(&order.expression, &order_scope)?;
                    }
                    if let Some(e) = predicate {
                        e.validate_input(&order_scope)?;
                    }
                    for e in skip.iter().chain(limit.iter()) {
                        check(e, &BTreeSet::new())?;
                    }
                    defined = items.iter().map(|i| i.slot).collect();
                }
                Operator::Update(items) => {
                    effect = Effect::Write;
                    for item in items {
                        let (entity, expression) = match item {
                            PropertyMutation::Set { entity, value, .. } => (entity, Some(value)),
                            PropertyMutation::Remove { entity, .. } => (entity, None),
                            PropertyMutation::Replace { entity, properties }
                            | PropertyMutation::Extend { entity, properties } => {
                                (entity, Some(properties))
                            }
                        };
                        check(&Expression::Slot(*entity), &defined)?;
                        if let Some(e) = expression {
                            check(e, &defined)?;
                        }
                    }
                }
                Operator::Delete { entities, .. } => {
                    effect = Effect::Write;
                    for e in entities {
                        check(e, &defined)?;
                    }
                }
            }
            let contract = super::OperatorContract::derive(operator, &input, &defined, &bindings);
            row_schema = contract.output().clone();
            contracts.push(contract);
        }
        if defined.iter().any(|s| s.0 as usize >= bindings.len())
            || returns.iter().any(|(_, s)| !defined.contains(s))
        {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "InvalidSchema",
                "query output does not match the binding catalog",
            ));
        }
        Ok(Self {
            bindings,
            operators,
            returns,
            effect,
            contracts,
        })
    }
    pub fn contracts(&self) -> &[super::OperatorContract] {
        &self.contracts
    }
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }
    pub fn operators(&self) -> &[Operator] {
        &self.operators
    }
    pub fn returns(&self) -> &[(String, Slot)] {
        &self.returns
    }
    pub fn effect(&self) -> Effect {
        self.effect
    }
}
