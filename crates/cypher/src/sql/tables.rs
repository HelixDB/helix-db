//! FROM clauses: tables become node and relationship patterns, and column
//! references resolve against the tables in scope.
use super::{invalid, property, unsupported, variable, Translator};
use crate::syntax::{self, Expr, ExprKind};
use helix_planner::relational::{self as r, detail, QueryError, Result, Span};
use sqlparser::ast;

/// Variables of the nodes a single relationship table connects.
pub(super) const START: &str = "_start";
pub(super) const END: &str = "_end";

/// The tables of one statement, in written order.
#[derive(Debug, Default)]
pub(super) struct Scope {
    tables: Vec<Table>,
    /// Explicit select-list aliases, visible to `HAVING` and `ORDER BY`.
    pub(super) aliases: Vec<(String, Expr)>,
}

/// A table in scope and the Cypher variable that stands for it.
#[derive(Debug)]
pub(super) struct Table {
    /// The table name as written, without schema.
    name: String,
    alias: Option<String>,
    pub(super) variable: String,
    pub(super) kind: Kind,
    role: Role,
}

#[derive(Debug)]
pub(super) enum Kind {
    Node { label: String },
    Relationship { kind: String },
}

/// Whether a column without a qualifier can resolve to this table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Written,
    /// The start or end node of a relationship table, reachable only by a
    /// qualifier: its label, `_start` or `_end`.
    Endpoint,
}

/// What a table name means after [`super::Mappings::tables`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Class {
    Node(String),
    /// A `Start_TYPE_End` relationship table.
    Relationship {
        start: String,
        kind: String,
        end: String,
    },
}

/// One table reference of a FROM clause or DML target.
#[derive(Debug)]
pub(super) struct Reference {
    pub(super) name: String,
    pub(super) alias: Option<String>,
    pub(super) variable: String,
    pub(super) class: Class,
    pub(super) span: Span,
}

/// Recognize `Start_TYPE_End`: the shortest start before an underscore-delimited
/// run of uppercase letters and underscores that leaves a nonempty end, taking
/// the longest such run, as neo4j-jdbc's default relationship pattern does.
/// Unlike that pattern, the start and end must each contain a lowercase letter
/// and the type an uppercase one, so that upper-case table names such as
/// `ORDER_LINE_ITEMS` stay node labels.
///
/// ```text
/// Person_ACTED_IN_Movie → (Person, ACTED_IN, Movie)
/// My_Table, ACTED_IN, ORDER_LINE_ITEMS → not a relationship table
/// ```
pub(super) fn relationship_table(label: &str) -> Option<(&str, &str, &str)> {
    let lowercase = |text: &str| text.chars().any(char::is_lowercase);
    let typed = |c: char| c == '_' || c.is_uppercase();
    label
        .char_indices()
        .filter(|&(index, c)| c == '_' && index > 0)
        .find_map(|(underscore, _)| {
            let start = &label[..underscore];
            let rest = &label[underscore + 1..];
            let run = rest.find(|c: char| !typed(c)).unwrap_or(rest.len());
            rest[..run]
                .char_indices()
                .rev()
                .filter(|&(index, c)| c == '_' && index > 0 && index + 1 < rest.len())
                .map(|(index, _)| (&rest[..index], &rest[index + 1..]))
                .find(|(kind, end)| kind.chars().any(char::is_uppercase) && lowercase(end))
                .filter(|_| lowercase(start))
                .map(|(kind, end)| (start, kind, end))
        })
}

impl Scope {
    fn add(&mut self, table: Table, span: Span) -> Result<()> {
        if self.tables.iter().any(|t| t.variable == table.variable) {
            return Err(invalid(
                detail::VARIABLE_ALREADY_BOUND,
                format!(
                    "the table variable `{}` is used twice; give each table its own alias",
                    table.variable
                ),
                span,
            ));
        }
        self.tables.push(table);
        Ok(())
    }

    pub(super) fn written(&self) -> impl Iterator<Item = &Table> {
        self.tables.iter().filter(|t| t.role == Role::Written)
    }

    /// The table a qualifier names: its alias or variable, then its name.
    fn qualified(&self, qualifier: &str, span: Span) -> Result<&Table> {
        let by = |matches: &dyn Fn(&Table) -> bool| -> Vec<&Table> {
            self.tables.iter().filter(|t| matches(t)).collect()
        };
        let mut found = by(&|t| {
            t.alias
                .as_deref()
                .is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
                || t.variable.eq_ignore_ascii_case(qualifier)
        });
        if found.is_empty() {
            found = by(&|t| {
                t.name.eq_ignore_ascii_case(qualifier)
                    || match &t.kind {
                        Kind::Node { label } => {
                            t.role == Role::Endpoint && label.eq_ignore_ascii_case(qualifier)
                        }
                        Kind::Relationship { kind } => kind.eq_ignore_ascii_case(qualifier),
                    }
            });
        }
        match found.as_slice() {
            [table] => Ok(table),
            [] => Err(invalid(
                detail::UNDEFINED_VARIABLE,
                format!("`{qualifier}` is not a table of this statement"),
                span,
            )),
            [..] => Err(invalid(
                detail::AMBIGUOUS_COLUMN,
                format!("`{qualifier}` names several tables; qualify columns by alias"),
                span,
            )),
        }
    }

    /// A column reference: `col`, `table.col` or `schema.table.col`.
    pub(super) fn column(&self, parts: &[ast::Ident], span: Span) -> Result<Expr> {
        let id = |table: &Table| super::call("id", vec![variable(&table.variable, span)?], span);
        match parts {
            [column] => {
                let name = column.value.as_str();
                let alias = self
                    .aliases
                    .iter()
                    .find(|(alias, _)| alias == name)
                    .map(|(_, expression)| Ok(expression.clone()));
                let entity = || {
                    self.written()
                        .find(|t| {
                            t.variable.eq_ignore_ascii_case(name)
                                || t.alias
                                    .as_deref()
                                    .is_some_and(|a| a.eq_ignore_ascii_case(name))
                        })
                        .map(|table| variable(&table.variable, span))
                };
                // `v$<table>_id` is the id of the table with that variable or label.
                let virtual_id = || {
                    let target = name.strip_prefix("v$")?.strip_suffix("_id")?;
                    Some(
                        self.tables
                            .iter()
                            .find(|t| {
                                t.variable.eq_ignore_ascii_case(target)
                                    || matches!(&t.kind, Kind::Node { label } if label.eq_ignore_ascii_case(target))
                            })
                            .ok_or_else(|| {
                                invalid(
                                    detail::UNDEFINED_VARIABLE,
                                    format!("`{name}` names no table of this statement"),
                                    span,
                                )
                            })
                            .and_then(id),
                    )
                };
                alias
                    .or_else(entity)
                    .or_else(virtual_id)
                    .unwrap_or_else(|| {
                        let table = self.single(name, span)?;
                        if name.eq_ignore_ascii_case("v$id") {
                            return id(table);
                        }
                        // On a relationship table, `<label>_<column>` reads an
                        // endpoint, as in neo4j-jdbc: `person_name` is `_start.name`.
                        let endpoint = matches!(table.kind, Kind::Relationship { .. })
                            .then(|| {
                                self.tables
                                    .iter()
                                    .filter(|t| t.role == Role::Endpoint)
                                    .find_map(|t| {
                                        let Kind::Node { label } = &t.kind else {
                                            return None;
                                        };
                                        let (prefix, column) = name.split_once('_')?;
                                        (prefix.eq_ignore_ascii_case(label) && !column.is_empty())
                                            .then_some((t, column))
                                    })
                            })
                            .flatten();
                        match endpoint {
                            Some((endpoint, column)) => {
                                property(variable(&endpoint.variable, span)?, column, span)
                            }
                            None => property(variable(&table.variable, span)?, name, span),
                        }
                    })
            }
            [.., qualifier, column] if parts.len() <= 4 => {
                let table = self.qualified(&qualifier.value, span)?;
                if column.value.eq_ignore_ascii_case("v$id") {
                    id(table)
                } else {
                    property(variable(&table.variable, span)?, &column.value, span)
                }
            }
            _ => Err(unsupported("column names with more than four parts", span)),
        }
    }

    /// The only table an unqualified column can belong to.
    pub(super) fn single(&self, column: &str, span: Span) -> Result<&Table> {
        let mut written = self.written();
        match (written.next(), written.next()) {
            (Some(table), None) => Ok(table),
            (None, _) => Err(invalid(
                detail::UNDEFINED_VARIABLE,
                format!("column `{column}` needs a table in FROM"),
                span,
            )),
            (Some(_), Some(_)) => Err(invalid(
                detail::AMBIGUOUS_COLUMN,
                format!(
                    "column `{column}` needs a table qualifier because the statement reads several tables"
                ),
                span,
            )),
        }
    }

    /// The table `p.*` or `count(p.*)` names.
    pub(super) fn table(&self, name: &ast::ObjectName, span: Span) -> Result<&Table> {
        let qualifier = name
            .0
            .last()
            .and_then(ast::ObjectNamePart::as_ident)
            .ok_or_else(|| unsupported("this table qualifier", span))?;
        self.qualified(&qualifier.value, span)
    }
}

impl Translator<'_> {
    /// One table reference: a plain table name with an optional alias.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "sqlparser's FROM items are open-ended; only plain tables translate"
    )]
    pub(super) fn reference(&self, factor: &ast::TableFactor) -> Result<Reference> {
        let span = self.at(factor);
        let ast::TableFactor::Table {
            name,
            alias,
            args: None,
            with_hints,
            version: None,
            with_ordinality: false,
            partitions,
            json_path: None,
            sample: None,
            index_hints,
        } = factor
        else {
            return Err(match factor {
                ast::TableFactor::Derived { .. } => QueryError::compile(
                    r::category::UNSUPPORTED_FEATURE,
                    detail::PROCEDURES_AND_SUBQUERIES,
                    "SQL translation does not support subqueries",
                )
                .at(span),
                _ => unsupported("this kind of FROM item", span),
            });
        };
        if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
            return Err(unsupported("table hints and partitions", span));
        }
        if alias
            .as_ref()
            .is_some_and(|a| !a.columns.is_empty() || a.at.is_some())
        {
            return Err(unsupported("column aliases on a table", span));
        }
        self.named(name, alias.as_ref().map(|a| &a.name), span)
    }

    /// Resolve a table name and optional alias, as in a FROM clause or a
    /// DML target.
    pub(super) fn named(
        &self,
        name: &ast::ObjectName,
        alias: Option<&ast::Ident>,
        span: Span,
    ) -> Result<Reference> {
        let name = name
            .0
            .last()
            .and_then(ast::ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
            .ok_or_else(|| unsupported("this table name", span))?;
        let label = self.mappings.label(&name);
        let class = match relationship_table(&label) {
            Some((start, kind, end)) => Class::Relationship {
                start: start.to_owned(),
                kind: kind.to_owned(),
                end: end.to_owned(),
            },
            None => Class::Node(label),
        };
        let alias = alias.map(|a| a.value.clone());
        Ok(Reference {
            variable: alias.as_deref().unwrap_or(&name).to_lowercase(),
            name,
            alias,
            class,
            span,
        })
    }

    /// The patterns a FROM list matches and the tables it brings into scope.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "sqlparser's join kinds are open-ended; only inner and cross joins translate"
    )]
    pub(super) fn from(
        &self,
        from: &[ast::TableWithJoins],
    ) -> Result<(Vec<syntax::Pattern>, Scope)> {
        let mut scope = Scope::default();
        let mut patterns = Vec::new();
        for item in from {
            let mut chain = vec![(self.reference(&item.relation)?, None)];
            for join in &item.joins {
                let span = self.at(&join.relation);
                let reference = self.reference(&join.relation)?;
                match &join.join_operator {
                    ast::JoinOperator::CrossJoin(ast::JoinConstraint::None) => {
                        patterns.push(self.chain(std::mem::take(&mut chain), &mut scope)?);
                        chain.push((reference, None));
                    }
                    ast::JoinOperator::Join(constraint) | ast::JoinOperator::Inner(constraint) => {
                        chain.push((reference, Some(constraint)));
                    }
                    other => return Err(unsupported(join_kind(other), span)),
                }
            }
            patterns.push(self.chain(chain, &mut scope)?);
        }
        Ok((patterns, scope))
    }

    /// One standalone table: a node, or a relationship table with its endpoints.
    pub(super) fn single(
        &self,
        reference: Reference,
        scope: &mut Scope,
    ) -> Result<syntax::Pattern> {
        let span = reference.span;
        match reference.class {
            Class::Node(label) => {
                let pattern = pattern_node(&reference.variable, &label);
                scope.add(
                    Table {
                        name: reference.name,
                        alias: reference.alias,
                        variable: reference.variable,
                        kind: Kind::Node { label },
                        role: Role::Written,
                    },
                    span,
                )?;
                Ok(syntax::Pattern {
                    name: None,
                    nodes: vec![pattern],
                    relationships: Vec::new(),
                })
            }
            Class::Relationship { start, kind, end } => {
                let pattern = syntax::Pattern {
                    name: None,
                    nodes: vec![pattern_node(START, &start), pattern_node(END, &end)],
                    relationships: vec![relationship(
                        Some(reference.variable.clone()),
                        &kind,
                        r::Direction::Outgoing,
                    )],
                };
                for (variable, label) in [(START, start), (END, end)] {
                    scope.add(
                        Table {
                            name: label.clone(),
                            alias: None,
                            variable: variable.to_owned(),
                            kind: Kind::Node { label },
                            role: Role::Endpoint,
                        },
                        span,
                    )?;
                }
                scope.add(
                    Table {
                        name: reference.name,
                        alias: reference.alias,
                        variable: reference.variable,
                        kind: Kind::Relationship { kind },
                        role: Role::Written,
                    },
                    span,
                )?;
                Ok(pattern)
            }
        }
    }

    /// A join chain read left to right; see the module documentation.
    fn chain(
        &self,
        chain: Vec<(Reference, Option<&ast::JoinConstraint>)>,
        scope: &mut Scope,
    ) -> Result<syntax::Pattern> {
        let mut tables = chain.into_iter();
        let Some((first, _)) = tables.next() else {
            return Err(unsupported("an empty FROM item", Span { start: 0, end: 0 }));
        };
        let joins: Vec<_> = tables.collect();
        if joins.is_empty() {
            return self.single(first, scope);
        }
        let mut pattern = syntax::Pattern {
            name: None,
            nodes: vec![self.chain_node(first, scope)?],
            relationships: Vec::new(),
        };
        let mut rest = joins.into_iter().peekable();
        // An odd number of joins starts with a direct join between two node
        // tables; every later pair is a relationship table and a node table.
        if rest.len() % 2 == 1
            && let Some((right, constraint)) = rest.next()
        {
            let constraint = constraint.expect("chain joins carry a constraint");
            let left = scope
                .written()
                .last()
                .map(|t| t.name.clone())
                .expect("the chain's first table is in scope");
            let join = self.join(constraint, scope, Some(&right), right.span)?;
            // As in neo4j-jdbc, a mapping is keyed by the left table and the
            // column that names the type, whichever table owns it.
            let kind = join.column.as_ref().map(|column| {
                self.mappings
                    .join_type(&left, column)
                    .map_or_else(|| column.to_uppercase(), str::to_owned)
            });
            pattern.relationships.push(syntax::Relationship {
                name: kind.as_ref().map(|kind| kind.to_lowercase()),
                types: kind.into_iter().collect(),
                direction: join.direction,
                properties: Vec::new(),
            });
            pattern.nodes.push(self.chain_node(right, scope)?);
        }
        while let Some((middle, entering)) = rest.next() {
            let (right, constraint) = rest.next().expect("joins remain in pairs");
            let span = middle.span;
            let kind = match &middle.class {
                Class::Node(label) => label.clone(),
                Class::Relationship { kind, .. } => kind.clone(),
            };
            let name = middle.variable.clone();
            scope.add(
                Table {
                    name: middle.name,
                    alias: middle.alias,
                    variable: middle.variable,
                    kind: Kind::Relationship { kind: kind.clone() },
                    role: Role::Written,
                },
                span,
            )?;
            // The join that enters the relationship table only has to be a
            // column equality; the one that leaves it sets the direction.
            self.join(
                entering.expect("chain joins carry a constraint"),
                scope,
                None,
                span,
            )?;
            let join = self.join(
                constraint.expect("chain joins carry a constraint"),
                scope,
                Some(&right),
                right.span,
            )?;
            pattern
                .relationships
                .push(relationship(Some(name), &kind, join.direction));
            pattern.nodes.push(self.chain_node(right, scope)?);
        }
        Ok(pattern)
    }

    fn chain_node(&self, reference: Reference, scope: &mut Scope) -> Result<syntax::Node> {
        let Class::Node(label) = reference.class else {
            return Err(unsupported(
                format!(
                    "the relationship table `{}` outside the middle of a join",
                    reference.name
                ),
                reference.span,
            ));
        };
        let node = pattern_node(&reference.variable, &label);
        scope.add(
            Table {
                name: reference.name,
                alias: reference.alias,
                variable: reference.variable,
                kind: Kind::Node { label },
                role: Role::Written,
            },
            reference.span,
        )?;
        Ok(node)
    }

    /// Check one join condition and read its direction. `right` is the table
    /// the join adds; it is not in scope yet.
    fn join(
        &self,
        constraint: &ast::JoinConstraint,
        scope: &Scope,
        right: Option<&Reference>,
        span: Span,
    ) -> Result<Join> {
        match constraint {
            ast::JoinConstraint::Natural => Ok(Join {
                column: None,
                direction: r::Direction::Outgoing,
            }),
            ast::JoinConstraint::Using(columns) => match columns.as_slice() {
                [column] => {
                    let column = column
                        .0
                        .last()
                        .and_then(ast::ObjectNamePart::as_ident)
                        .ok_or_else(|| unsupported("this USING column", span))?;
                    Ok(Join {
                        column: Some(column.value.clone()),
                        direction: r::Direction::Outgoing,
                    })
                }
                _ => Err(unsupported("USING with several columns", span)),
            },
            ast::JoinConstraint::On(condition) => {
                let condition = strip_nesting(condition);
                let ast::Expr::BinaryOp {
                    left,
                    op: ast::BinaryOperator::Eq,
                    right: second,
                } = condition
                else {
                    return Err(unsupported(
                        "join conditions other than one column equality; move filters to WHERE",
                        self.at(condition),
                    ));
                };
                let [Some(first), Some(second)] = [left, second].map(|side| column_parts(side))
                else {
                    return Err(unsupported(
                        "join conditions other than one column equality; move filters to WHERE",
                        self.at(condition),
                    ));
                };
                let names_right = |q: &str| {
                    right.is_some_and(|right| {
                        right.alias.as_deref().map_or_else(
                            || right.name.eq_ignore_ascii_case(q),
                            |alias| alias.eq_ignore_ascii_case(q),
                        )
                    })
                };
                // Both columns must belong to tables of the join.
                for parts in [&first, &second] {
                    if let [.., qualifier, _] = parts.as_slice()
                        && !names_right(&qualifier.value)
                    {
                        scope.qualified(&qualifier.value, self.at(condition))?;
                    }
                }
                let (qualifier, column) = match second.as_slice() {
                    [column] => (None, column),
                    [.., qualifier, column] => (Some(qualifier.value.as_str()), column),
                    [] => return Err(unsupported("an empty column name", span)),
                };
                // The relationship leaves the table that owns the second column.
                let direction = if qualifier.is_some_and(names_right) {
                    r::Direction::Incoming
                } else {
                    r::Direction::Outgoing
                };
                Ok(Join {
                    column: Some(column.value.clone()),
                    direction,
                })
            }
            ast::JoinConstraint::None => {
                Err(unsupported("a JOIN without ON, USING or NATURAL", span))
            }
        }
    }
}

/// What a join condition contributes: the column that names the
/// relationship type, and the direction.
struct Join {
    column: Option<String>,
    direction: r::Direction,
}

fn strip_nesting(mut expression: &ast::Expr) -> &ast::Expr {
    while let ast::Expr::Nested(inner) = expression {
        expression = inner;
    }
    expression
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only identifiers are columns"
)]
fn column_parts(expression: &ast::Expr) -> Option<Vec<ast::Ident>> {
    match strip_nesting(expression) {
        ast::Expr::Identifier(ident) => Some(vec![ident.clone()]),
        ast::Expr::CompoundIdentifier(parts) => Some(parts.clone()),
        _ => None,
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "sqlparser's join kinds are open-ended; only inner and cross joins translate"
)]
fn join_kind(operator: &ast::JoinOperator) -> &'static str {
    match operator {
        ast::JoinOperator::Left(_)
        | ast::JoinOperator::LeftOuter(_)
        | ast::JoinOperator::Right(_)
        | ast::JoinOperator::RightOuter(_)
        | ast::JoinOperator::FullOuter(_) => "outer joins",
        ast::JoinOperator::CrossJoin(_) => "CROSS JOIN with a condition",
        _ => "this kind of join",
    }
}

fn pattern_node(variable: &str, label: &str) -> syntax::Node {
    syntax::Node {
        has_properties: false,
        name: Some(variable.to_owned()),
        labels: vec![label.to_owned()],
        properties: Vec::new(),
    }
}

fn relationship(name: Option<String>, kind: &str, direction: r::Direction) -> syntax::Relationship {
    syntax::Relationship {
        name,
        types: vec![kind.to_owned()],
        direction,
        properties: Vec::new(),
    }
}

/// `true` when `expression` is a variable that stands for one of `scope`'s tables.
pub(super) fn is_table_variable(scope: &Scope, expression: &Expr) -> bool {
    matches!(expression.kind(), ExprKind::Variable(name) if scope.tables.iter().any(|t| &t.variable == name))
}

/// Table variables `expression` reads, for grouping validation.
pub(super) fn reads_table(scope: &Scope, expression: &Expr) -> bool {
    let mut found = false;
    super::expr::visit(expression, &mut |e| found |= is_table_variable(scope, e));
    found
}
