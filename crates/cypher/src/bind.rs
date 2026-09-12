use crate::syntax::{self as s, ExprKind as E};
use helix_planner::relational::{self as r, QueryError, Result};
use std::collections::{BTreeMap, BTreeSet};

type Scope = BTreeMap<String, r::Slot>;

pub fn resolve(statement: &s::Statement) -> Result<r::Query> {
    if statement.clauses.is_empty() || statement.clauses.len() > 4096 {
        return Err(QueryError::compile(
            "SyntaxError",
            "InvalidStatement",
            "statement must contain between one and 4096 clauses",
        ));
    }
    for clause in &statement.clauses {
        let patterns = match clause {
            s::Clause::Match { patterns, .. } | s::Clause::Create(patterns) => patterns,
            s::Clause::Project { .. }
            | s::Clause::Unwind { .. }
            | s::Clause::Set(_)
            | s::Clause::Remove(_)
            | s::Clause::Delete { .. } => continue,
        };
        if patterns.is_empty()
            || patterns
                .iter()
                .any(|pattern| pattern.nodes.len() != pattern.relationships.len().saturating_add(1))
        {
            return Err(QueryError::compile(
                "SyntaxError",
                "InvalidRelationshipPattern",
                "patterns must alternate nodes and relationships",
            ));
        }
    }
    let mut binder = Binder {
        bindings: Vec::new(),
        scope: Scope::new(),
        anonymous: 0,
    };
    let mut operators = Vec::new();
    let mut returns = Vec::new();
    for (position, clause) in statement.clauses.iter().enumerate() {
        match clause {
            s::Clause::Match {
                patterns,
                optional,
                predicate,
            } => {
                let pattern = binder.pattern(patterns, *optional, false)?;
                let predicate = predicate
                    .as_ref()
                    .map(|e| {
                        binder
                            .predicate(e, &binder.scope)
                            .and_then(r::SelectionProgram::new)
                    })
                    .transpose()?;
                operators.push(r::Operator::Match {
                    pattern,
                    optional: *optional,
                    predicate,
                });
            }
            s::Clause::Create(patterns) => {
                operators.push(r::Operator::Create(binder.pattern(patterns, false, true)?))
            }
            s::Clause::Unwind { expression, name } => {
                let expression = binder.expression(expression, &binder.scope, false)?;
                if binder.scope.contains_key(name) {
                    return Err(semantic(
                        "VariableAlreadyBound",
                        format!("{name} is already bound"),
                    ));
                }
                let slot = binder.allocate(name.clone(), r::BindingType::Scalar, true)?;
                binder.scope.insert(name.clone(), slot);
                operators.push(r::Operator::Unwind { expression, slot });
            }
            s::Clause::Project {
                returning,
                items,
                distinct,
                ordering,
                skip,
                limit,
                predicate,
            } => {
                if *returning && position + 1 != statement.clauses.len() {
                    return Err(semantic(
                        "InvalidClauseComposition",
                        "RETURN must finish the statement",
                    ));
                }
                let mut projections = Vec::new();
                let mut output = Scope::new();
                let mut columns = Vec::new();
                let mut missing_alias = false;
                for item in items {
                    match item {
                        s::Item::Wildcard => {
                            if binder.scope.is_empty() {
                                return Err(semantic(
                                    "NoVariablesInScope",
                                    "wildcard projection requires an input binding",
                                ));
                            }
                            for (name, slot) in binder.scope.clone() {
                                if output.insert(name.clone(), slot).is_some() {
                                    return Err(semantic(
                                        "ColumnNameConflict",
                                        "duplicate projection name",
                                    ));
                                }
                                projections.push(r::Projection {
                                    slot,
                                    expression: r::Expression::Slot(slot),
                                });
                                columns.push((name, slot));
                            }
                        }
                        s::Item::Expression {
                            expression,
                            alias,
                            text,
                        } => {
                            let name = match (alias, &expression.kind) {
                                (Some(name), _) => name.clone(),
                                (None, E::Variable(name)) => name.clone(),
                                (None, _) if *returning => text.clone(),
                                (None, _) => {
                                    missing_alias = true;
                                    text.clone()
                                }
                            };
                            if output.contains_key(&name) {
                                return Err(semantic(
                                    "ColumnNameConflict",
                                    format!("duplicate projection name {name}"),
                                ));
                            }
                            let expression = binder.expression(expression, &binder.scope, true)?;
                            let kind = match &expression {
                                r::Expression::Slot(slot) => binder.bindings[slot.0 as usize].kind,
                                r::Expression::Literal(_)
                                | r::Expression::Parameter(_)
                                | r::Expression::Property(..)
                                | r::Expression::Index(..)
                                | r::Expression::Slice { .. }
                                | r::Expression::Unary(..)
                                | r::Expression::Binary(..)
                                | r::Expression::Function(..)
                                | r::Expression::Aggregate { .. }
                                | r::Expression::List(_)
                                | r::Expression::Map(_)
                                | r::Expression::Case { .. }
                                | r::Expression::HasLabel(..) => r::BindingType::Scalar,
                            };
                            let slot = binder.allocate(name.clone(), kind, true)?;
                            binder.bindings[slot.0 as usize].value_type =
                                expression.value_type(&binder.bindings)?;
                            output.insert(name.clone(), slot);
                            columns.push((name, slot));
                            projections.push(r::Projection { slot, expression });
                        }
                    }
                }
                let aggregated = projections.iter().any(|p| p.expression.has_aggregate());
                let mut order_scope = if *distinct || aggregated {
                    Scope::new()
                } else {
                    binder.scope.clone()
                };
                order_scope.extend(output.clone());
                let grouping = projections
                    .iter()
                    .filter(|p| !p.expression.has_aggregate())
                    .map(|p| &p.expression)
                    .collect::<Vec<_>>();
                for item in projections.iter().filter(|p| p.expression.has_aggregate()) {
                    item.expression.rewrite(&mut |expression| {
                        if matches!(expression, r::Expression::Aggregate { .. })
                            || grouping.contains(&expression)
                                && matches!(
                                    expression,
                                    r::Expression::Slot(_) | r::Expression::Property(..)
                                )
                        {
                            return Ok(Some(expression.clone()));
                        }
                        if matches!(expression, r::Expression::Slot(_)) {
                            return Err(semantic(
                                "AmbiguousAggregationExpression",
                                "aggregate expression references an ungrouped binding",
                            ));
                        }
                        Ok(None)
                    })?;
                }
                let mut full_scope = binder.scope.clone();
                full_scope.extend(output.clone());
                let ordering = ordering
                    .iter()
                    .map(|(expression, descending)| {
                        let expression = binder.expression(expression, &full_scope, true)?;
                        let has_aggregate = expression.has_aggregate();
                        let expression = expression.rewrite(&mut |e| {
                            if let Some(item) = projections.iter().find(|p| {
                                p.expression == *e
                                    && (!has_aggregate
                                        || matches!(
                                            e,
                                            r::Expression::Slot(_)
                                                | r::Expression::Property(..)
                                                | r::Expression::Aggregate { .. }
                                        ))
                            }) {
                                return Ok(Some(r::Expression::Slot(item.slot)));
                            }
                            if matches!(e, r::Expression::Aggregate { .. }) {
                                return Err(semantic(
                                    if aggregated {
                                        "UndefinedVariable"
                                    } else {
                                        "InvalidAggregation"
                                    },
                                    "ORDER BY aggregate must be projected",
                                ));
                            }
                            Ok(None)
                        })?;
                        if (*distinct || aggregated)
                            && expression
                                .slots()
                                .iter()
                                .any(|slot| !output.values().any(|out| out == slot))
                        {
                            let detail = if has_aggregate
                                && grouping.iter().any(|e| {
                                    !matches!(
                                        e,
                                        r::Expression::Slot(_) | r::Expression::Property(..)
                                    )
                                }) {
                                "AmbiguousAggregationExpression"
                            } else {
                                "UndefinedVariable"
                            };
                            return Err(semantic(
                                detail,
                                "ORDER BY references an unprojected binding",
                            ));
                        }
                        Ok(r::Ordering {
                            expression,
                            descending: *descending,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if missing_alias {
                    return Err(semantic(
                        "NoExpressionAlias",
                        "WITH expressions require an alias",
                    ));
                }
                let skip = skip.as_ref().map(|e| binder.bound(e)).transpose()?;
                let limit = limit.as_ref().map(|e| binder.bound(e)).transpose()?;
                let predicate = predicate
                    .as_ref()
                    .map(|e| {
                        binder
                            .predicate(e, &order_scope)
                            .and_then(r::SelectionProgram::new)
                    })
                    .transpose()?;
                operators.push(r::Operator::Project {
                    items: r::ProjectionProgram::new(projections)?,
                    distinct: *distinct,
                    ordering,
                    predicate,
                    skip,
                    limit,
                });
                binder.scope = output;
                if *returning {
                    returns = columns;
                }
            }
            s::Clause::Set(assignments) => {
                let mut updates = Vec::new();
                for assignment in assignments {
                    updates.push(match assignment {
                        s::Assignment::Property(target, value) => {
                            let (entity, key) = binder.property_target(target)?;
                            r::PropertyMutation::Set {
                                entity,
                                key,
                                value: binder.expression(value, &binder.scope, false)?,
                            }
                        }
                        s::Assignment::Replace(name, e) => r::PropertyMutation::Replace {
                            entity: binder.entity(name)?,
                            properties: binder.expression(e, &binder.scope, false)?,
                        },
                        s::Assignment::Extend(name, e) => r::PropertyMutation::Extend {
                            entity: binder.entity(name)?,
                            properties: binder.expression(e, &binder.scope, false)?,
                        },
                    });
                }
                operators.push(r::Operator::Update(updates));
            }
            s::Clause::Remove(expressions) => {
                let updates = expressions
                    .iter()
                    .map(|e| {
                        binder
                            .property_target(e)
                            .map(|(entity, key)| r::PropertyMutation::Remove { entity, key })
                    })
                    .collect::<Result<_>>()?;
                operators.push(r::Operator::Update(updates));
            }
            s::Clause::Delete {
                expressions,
                detach,
            } => operators.push(r::Operator::Delete {
                entities: expressions
                    .iter()
                    .map(|e| {
                        let expression = binder.expression(e, &binder.scope, false)?;
                        if matches!(expression, r::Expression::HasLabel(..)) {
                            return Err(semantic("InvalidDelete", "DELETE cannot remove a label"));
                        }
                        if !matches!(
                            expression.value_type(&binder.bindings)?,
                            r::ValueType::Any
                                | r::ValueType::Null
                                | r::ValueType::Node
                                | r::ValueType::Relationship
                                | r::ValueType::Path
                        ) {
                            return Err(semantic(
                                "InvalidArgumentType",
                                "DELETE requires graph entities or paths",
                            ));
                        }
                        Ok(expression)
                    })
                    .collect::<Result<_>>()?,
                detach: *detach,
            }),
        }
    }
    if !matches!(
        statement.clauses.last(),
        Some(
            s::Clause::Project {
                returning: true,
                ..
            } | s::Clause::Create(_)
                | s::Clause::Set(_)
                | s::Clause::Remove(_)
                | s::Clause::Delete { .. }
        )
    ) {
        return Err(semantic(
            "InvalidClauseComposition",
            "statement must end in RETURN or a mutation",
        ));
    }
    r::Query::new(binder.bindings, operators, returns)
}

struct Binder {
    bindings: Vec<r::Binding>,
    scope: Scope,
    anonymous: usize,
}

impl Binder {
    fn bound(&self, e: &s::Expr) -> Result<r::Expression> {
        let expression = self.expression(e, &self.scope, false)?;
        if !expression.slots().is_empty() {
            return Err(semantic(
                "NonConstantExpression",
                "SKIP and LIMIT cannot depend on row bindings",
            ));
        }
        let r::Expression::Literal(value) = &expression else {
            return Ok(expression);
        };
        match value {
            r::Value::Integer(i) if *i >= 0 => {}
            r::Value::Integer(_) => {
                return Err(semantic(
                    "NegativeIntegerArgument",
                    "SKIP and LIMIT require nonnegative integers",
                ))
            }
            r::Value::Null
            | r::Value::Boolean(_)
            | r::Value::Float(_)
            | r::Value::String(_)
            | r::Value::List(_)
            | r::Value::Map(_)
            | r::Value::Entity(_)
            | r::Value::Path(_) => {
                return Err(semantic(
                    "InvalidArgumentType",
                    "SKIP and LIMIT require integers",
                ))
            }
        }
        Ok(expression)
    }
    fn predicate(&self, e: &s::Expr, scope: &Scope) -> Result<r::Expression> {
        let expression = self.boolean_expression(e, scope, false)?;
        if !matches!(
            expression.value_type(&self.bindings)?,
            r::ValueType::Any | r::ValueType::Null | r::ValueType::Boolean
        ) {
            return Err(semantic(
                "InvalidArgumentType",
                "predicate requires a boolean",
            ));
        }
        Ok(expression)
    }

    fn boolean_expression(
        &self,
        e: &s::Expr,
        scope: &Scope,
        allow_aggregate: bool,
    ) -> Result<r::Expression> {
        if matches!(e.kind, E::PatternPredicate) {
            return Err(QueryError::unsupported("PatternExpression").at(e.span));
        }
        self.expression(e, scope, allow_aggregate)
    }

    fn allocate(&mut self, name: String, kind: r::BindingType, nullable: bool) -> Result<r::Slot> {
        if self.bindings.len() >= 4096 {
            return Err(QueryError::compile(
                "ResourceLimit",
                "TooManyBindings",
                "query exceeds 4096 bindings",
            ));
        }
        let slot = r::Slot(self.bindings.len() as u32);
        let value_type = match kind {
            r::BindingType::Node => r::ValueType::Node,
            r::BindingType::Relationship => r::ValueType::Relationship,
            r::BindingType::Path => r::ValueType::Path,
            r::BindingType::Scalar => r::ValueType::Any,
        };
        self.bindings.push(r::Binding {
            name,
            kind,
            nullable,
            value_type,
        });
        Ok(slot)
    }
    fn binding(
        &mut self,
        name: &Option<String>,
        kind: r::BindingType,
        nullable: bool,
    ) -> Result<r::Slot> {
        if let Some(name) = name {
            if let Some(slot) = self.scope.get(name) {
                if self.bindings[slot.0 as usize].kind != kind
                    && !matches!(
                        self.bindings[slot.0 as usize].value_type,
                        r::ValueType::Any | r::ValueType::Null
                    )
                {
                    return Err(semantic(
                        "VariableTypeConflict",
                        format!("{name} has an incompatible type"),
                    ));
                }
                return Ok(*slot);
            }
            let slot = self.allocate(name.clone(), kind, nullable)?;
            self.scope.insert(name.clone(), slot);
            return Ok(slot);
        }
        let name = format!("@{}", self.anonymous);
        self.anonymous += 1;
        self.allocate(name, kind, nullable)
    }
    fn entity(&self, name: &str) -> Result<r::Slot> {
        let Some(slot) = self.scope.get(name).copied() else {
            return Err(semantic(
                "UndefinedVariable",
                format!("{name} is not defined"),
            ));
        };
        if !matches!(
            self.bindings[slot.0 as usize].kind,
            r::BindingType::Node | r::BindingType::Relationship | r::BindingType::Scalar
        ) {
            return Err(semantic(
                "InvalidArgumentType",
                "mutation target must be a graph entity",
            ));
        }
        Ok(slot)
    }
    fn property_target(&self, e: &s::Expr) -> Result<(r::Slot, String)> {
        if matches!(e.kind, E::HasLabel(..)) {
            return Err(QueryError::unsupported("LabelMutation").at(e.span));
        }
        let E::Property(target, key) = &e.kind else {
            return Err(semantic("InvalidArgumentType", "expected entity.property"));
        };
        let E::Variable(name) = &target.kind else {
            return Err(semantic(
                "InvalidArgumentType",
                "expected an entity variable",
            ));
        };
        check_property(key)?;
        Ok((self.entity(name)?, key.clone()))
    }

    fn pattern(
        &mut self,
        patterns: &[s::Pattern],
        optional: bool,
        create: bool,
    ) -> Result<r::Pattern> {
        if create
            && patterns
                .iter()
                .flat_map(|p| &p.relationships)
                .any(|rel| rel.direction == r::Direction::Undirected)
        {
            return Err(semantic(
                "RequiresDirectedRelationship",
                "CREATE requires a directed relationship",
            ));
        }
        let previous = self.scope.values().copied().collect::<BTreeSet<_>>();
        let mut nodes = Vec::new();
        let mut relationships = Vec::new();
        let mut paths = Vec::new();
        for pattern in patterns {
            let mut path_nodes = Vec::new();
            let mut path_rels = Vec::new();
            for node in &pattern.nodes {
                if node.labels.len() > 1 {
                    return Err(QueryError::unsupported("MultipleNodeLabels"));
                }
                if node.labels.iter().any(String::is_empty) {
                    return Err(QueryError::unsupported("EmptyLabel"));
                }
                let slot = self.binding(&node.name, r::BindingType::Node, optional)?;
                if create
                    && !previous.contains(&slot)
                    && !nodes.iter().any(|(s, _)| *s == slot)
                    && node.labels.len() != 1
                {
                    return Err(QueryError::unsupported("NodeLabelRequired"));
                }
                if create
                    && (previous.contains(&slot) || nodes.iter().any(|(s, _)| *s == slot))
                    && (!node.labels.is_empty()
                        || node.has_properties
                        || pattern.relationships.is_empty())
                {
                    return Err(semantic(
                        "VariableAlreadyBound",
                        "CREATE cannot redeclare a bound node",
                    ));
                }
                path_nodes.push(slot);
                nodes.push((slot, node));
            }
            for (i, rel) in pattern.relationships.iter().enumerate() {
                if rel.types.iter().any(String::is_empty) {
                    return Err(QueryError::unsupported("EmptyRelationshipType"));
                }
                if create && rel.direction == r::Direction::Undirected {
                    return Err(semantic(
                        "RequiresDirectedRelationship",
                        "CREATE requires a directed relationship",
                    ));
                }
                if create && rel.types.len() != 1 {
                    return Err(semantic(
                        "NoSingleRelationshipType",
                        "CREATE requires one type and a directed relationship",
                    ));
                }
                let slot = self.binding(&rel.name, r::BindingType::Relationship, optional)?;
                if create
                    && (previous.contains(&slot)
                        || relationships.iter().any(|(s, _, _, _)| *s == slot))
                {
                    return Err(semantic(
                        "VariableAlreadyBound",
                        "CREATE cannot redeclare a relationship",
                    ));
                }
                if !create && relationships.iter().any(|(s, _, _, _)| *s == slot) {
                    return Err(semantic(
                        "RelationshipUniquenessViolation",
                        "a relationship variable cannot repeat within one MATCH",
                    ));
                }
                path_rels.push(slot);
                relationships.push((slot, path_nodes[i], path_nodes[i + 1], rel));
            }
            if let Some(name) = &pattern.name {
                if self.scope.contains_key(name) {
                    return Err(semantic(
                        "VariableAlreadyBound",
                        format!("{name} is already bound"),
                    ));
                }
                let slot = self.binding(&Some(name.clone()), r::BindingType::Path, optional)?;
                paths.push(r::PathPattern {
                    slot,
                    nodes: path_nodes,
                    relationships: path_rels,
                });
            }
        }
        let nodes = nodes
            .into_iter()
            .map(|(slot, node)| {
                Ok(r::NodePattern {
                    slot,
                    label: node.labels.first().cloned(),
                    properties: self.properties(&node.properties)?,
                })
            })
            .collect::<Result<_>>()?;
        let relationships = relationships
            .into_iter()
            .map(|(slot, from, to, rel)| {
                Ok(r::RelationshipPattern {
                    slot,
                    from,
                    to,
                    direction: rel.direction,
                    types: rel.types.clone(),
                    properties: self.properties(&rel.properties)?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(r::Pattern {
            nodes,
            relationships,
            paths,
        })
    }

    fn properties(&self, properties: &[(String, s::Expr)]) -> Result<Vec<(String, r::Expression)>> {
        let mut names = BTreeSet::new();
        properties
            .iter()
            .map(|(name, e)| {
                check_property(name)?;
                if !names.insert(name) {
                    return Err(semantic(
                        "MapElementAccessByNonString",
                        "duplicate property key",
                    ));
                }
                Ok((name.clone(), self.expression(e, &self.scope, false)?))
            })
            .collect()
    }

    fn expression(
        &self,
        e: &s::Expr,
        scope: &Scope,
        allow_aggregate: bool,
    ) -> Result<r::Expression> {
        let resolve = |e: &s::Expr| self.expression(e, scope, allow_aggregate);
        let expression = match &e.kind {
            E::Literal(v) => r::Expression::Literal(v.clone()),
            E::Variable(name) => r::Expression::Slot(*scope.get(name).ok_or_else(|| {
                semantic("UndefinedVariable", format!("{name} is not defined")).at(e.span)
            })?),
            E::Parameter(name) => r::Expression::Parameter(name.clone()),
            E::PatternPredicate => {
                return Err(semantic(
                    "UnexpectedSyntax",
                    "a pattern predicate cannot be used as a scalar value",
                )
                .at(e.span))
            }
            E::Property(x, name) => {
                check_property(name)?;
                r::Expression::Property(Box::new(resolve(x)?), name.clone())
            }
            E::Index(a, b) => r::Expression::Index(Box::new(resolve(a)?), Box::new(resolve(b)?)),
            E::Slice { value, start, end } => r::Expression::Slice {
                value: Box::new(resolve(value)?),
                start: start
                    .as_ref()
                    .map(|e| resolve(e).map(Box::new))
                    .transpose()?,
                end: end.as_ref().map(|e| resolve(e).map(Box::new)).transpose()?,
            },
            E::Unary(op, x) => r::Expression::Unary(
                *op,
                Box::new(if *op == r::Unary::Not {
                    self.boolean_expression(x, scope, allow_aggregate)?
                } else {
                    resolve(x)?
                }),
            ),
            E::Binary(op, a, b) => {
                let operand = |e: &s::Expr| {
                    if matches!(op, r::Binary::And | r::Binary::Or | r::Binary::Xor) {
                        self.boolean_expression(e, scope, allow_aggregate)
                    } else {
                        resolve(e)
                    }
                };
                r::Expression::Binary(*op, Box::new(operand(a)?), Box::new(operand(b)?))
            }
            E::List(xs) => r::Expression::List(xs.iter().map(resolve).collect::<Result<_>>()?),
            E::Map(xs) => r::Expression::Map(
                xs.iter()
                    .map(|(name, x)| Ok((name.clone(), resolve(x)?)))
                    .collect::<Result<_>>()?,
            ),
            E::HasLabel(x, label) => {
                let r::Expression::Slot(slot) = resolve(x)? else {
                    return Err(semantic(
                        "InvalidArgumentType",
                        "label predicate requires a node",
                    ));
                };
                if self.bindings[slot.0 as usize].kind != r::BindingType::Node {
                    return Err(semantic(
                        "InvalidArgumentType",
                        "label predicate requires a node",
                    ));
                }
                r::Expression::HasLabel(slot, label.clone())
            }
            E::Case {
                operand,
                branches,
                otherwise,
            } => {
                let operand = operand.as_ref().map(|e| resolve(e)).transpose()?;
                let branches = branches
                    .iter()
                    .map(|(a, b)| {
                        let condition = match &operand {
                            Some(x) => r::Expression::Binary(
                                r::Binary::Equal,
                                Box::new(x.clone()),
                                Box::new(resolve(a)?),
                            ),
                            None => self.boolean_expression(a, scope, allow_aggregate)?,
                        };
                        Ok((condition, resolve(b)?))
                    })
                    .collect::<Result<_>>()?;
                r::Expression::Case {
                    branches,
                    otherwise: Box::new(
                        otherwise
                            .as_ref()
                            .map(|e| resolve(e))
                            .transpose()?
                            .unwrap_or(r::Expression::Literal(r::Value::Null)),
                    ),
                }
            }
            E::Call {
                name,
                arguments,
                distinct,
                star,
            } => {
                let name = name.to_ascii_lowercase();
                let aggregate = match name.as_str() {
                    "count" => Some(r::Aggregate::Count),
                    "sum" => Some(r::Aggregate::Sum),
                    "avg" => Some(r::Aggregate::Avg),
                    "min" => Some(r::Aggregate::Min),
                    "max" => Some(r::Aggregate::Max),
                    "collect" => Some(r::Aggregate::Collect),
                    _ => None,
                };
                if let Some(function) = aggregate {
                    if !allow_aggregate {
                        return Err(semantic(
                            "InvalidAggregation",
                            "aggregate is not allowed in this context",
                        ));
                    }
                    if (*star && (function != r::Aggregate::Count || *distinct))
                        || (!*star && arguments.len() != 1)
                    {
                        return Err(semantic(
                            "InvalidNumberOfArguments",
                            "aggregate requires one argument",
                        ));
                    }
                    let argument = arguments
                        .first()
                        .map(|a| {
                            let expression = self.expression(a, scope, true)?;
                            if expression.has_aggregate() {
                                return Err(semantic(
                                    "NestedAggregation",
                                    "aggregates cannot be nested",
                                ));
                            }
                            Ok(Box::new(expression))
                        })
                        .transpose()?;
                    r::Expression::Aggregate {
                        function,
                        argument,
                        distinct: *distinct,
                    }
                } else {
                    if *distinct || *star {
                        return Err(semantic(
                            "InvalidAggregation",
                            "DISTINCT and * require an aggregate",
                        ));
                    }
                    use r::Function as F;
                    let (function, min, max) = match name.as_str() {
                        "id" => (F::Id, 1, 1),
                        "type" => (F::Type, 1, 1),
                        "labels" => (F::Labels, 1, 1),
                        "properties" => (F::Properties, 1, 1),
                        "keys" => (F::Keys, 1, 1),
                        "size" => (F::Size, 1, 1),
                        "length" => (F::Length, 1, 1),
                        "nodes" => (F::Nodes, 1, 1),
                        "relationships" => (F::Relationships, 1, 1),
                        "head" => (F::Head, 1, 1),
                        "last" => (F::Last, 1, 1),
                        "coalesce" => (F::Coalesce, 1, usize::MAX),
                        "tostring" => (F::ToString, 1, 1),
                        "tointeger" | "toint" => (F::ToInteger, 1, 1),
                        "tofloat" => (F::ToFloat, 1, 1),
                        "toboolean" => (F::ToBoolean, 1, 1),
                        "exists" => (F::Exists, 1, 1),
                        "abs" => (F::Abs, 1, 1),
                        "range" => (F::Range, 2, 3),
                        "reverse" => (F::Reverse, 1, 1),
                        "trim" => (F::Trim, 1, 1),
                        "ltrim" => (F::Ltrim, 1, 1),
                        "rtrim" => (F::Rtrim, 1, 1),
                        "tolower" => (F::ToLower, 1, 1),
                        "toupper" => (F::ToUpper, 1, 1),
                        "substring" => (F::Substring, 2, 3),
                        _ => {
                            return Err(
                                QueryError::unsupported(&format!("Function:{name}")).at(e.span)
                            )
                        }
                    };
                    if arguments.len() < min || arguments.len() > max {
                        return Err(semantic(
                            "InvalidNumberOfArguments",
                            format!("wrong argument count for {name}"),
                        ));
                    }
                    r::Expression::Function(
                        function,
                        arguments
                            .iter()
                            .map(|argument| {
                                if function == F::Exists {
                                    self.boolean_expression(argument, scope, allow_aggregate)
                                } else {
                                    resolve(argument)
                                }
                            })
                            .collect::<Result<_>>()?,
                    )
                }
            }
        };
        expression.value_type(&self.bindings)?;
        Ok(expression)
    }
}

fn semantic(detail: &str, message: impl Into<String>) -> QueryError {
    QueryError::compile("SyntaxError", detail, message)
}

fn check_property(name: &str) -> Result<()> {
    if name.starts_with('$') {
        Err(QueryError::unsupported("ReservedPropertyName"))
    } else {
        Ok(())
    }
}
