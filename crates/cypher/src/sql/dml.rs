//! `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE`.
//!
//! Each statement is one Cypher write and so one transaction. An `INSERT`
//! into a relationship table finds its endpoints with `MATCH`: neo4j-jdbc
//! uses `MERGE`, which HelixDB's Cypher profile does not support, so a missing
//! endpoint creates nothing instead of a new node.
use super::tables::{shorthand, Class, Endpoint, Reference, Scope};
use super::{invalid, property, unsupported, variable, Translator};
use crate::syntax::{self, Assignment, Clause, Expr, ExprKind};
use helix_planner::relational::{self as r, category, detail, QueryError, Result, Span};
use sqlparser::ast;

impl Translator<'_> {
    pub(super) fn insert(&self, insert: &ast::Insert) -> Result<syntax::Statement> {
        let span = self.at(insert);
        let ast::Insert {
            insert_token: _,
            optimizer_hints,
            or,
            ignore,
            into: _,
            table,
            table_alias,
            columns,
            overwrite,
            source,
            assignments,
            partitioned,
            after_columns,
            has_table_keyword: _,
            on,
            returning,
            output,
            replace_into,
            priority,
            insert_alias,
            settings,
            format_clause,
            multi_table_insert_type,
            multi_table_into_clauses,
            multi_table_when_clauses,
            multi_table_else_clause,
        } = insert;
        if on.is_some() || or.is_some() || *ignore || *replace_into {
            return Err(QueryError::compile(
                category::UNSUPPORTED_FEATURE,
                detail::MERGE,
                "SQL translation does not support upserts: ON CONFLICT, ON DUPLICATE KEY, \
                 INSERT OR, INSERT IGNORE and REPLACE need MERGE",
            )
            .at(span));
        }
        if !optimizer_hints.is_empty()
            || *overwrite
            || !assignments.is_empty()
            || partitioned.is_some()
            || !after_columns.is_empty()
            || output.is_some()
            || priority.is_some()
            || insert_alias.is_some()
            || settings.is_some()
            || format_clause.is_some()
            || multi_table_insert_type.is_some()
            || !multi_table_into_clauses.is_empty()
            || !multi_table_when_clauses.is_empty()
            || multi_table_else_clause.is_some()
        {
            return Err(unsupported("dialect-specific INSERT clauses", span));
        }
        let ast::TableObject::TableName(name) = table else {
            return Err(unsupported("INSERT into a table function", span));
        };
        let reference = self.named(name, table_alias.as_ref().map(|a| &a.alias), span)?;
        if columns.is_empty() {
            return Err(invalid(
                detail::INVALID_STATEMENT,
                "INSERT needs a column list: columns are property names",
                span,
            ));
        }
        let rows = self.rows(source.as_deref(), columns.len(), span)?;
        let mut scope = Scope::default();
        let class = reference.class.clone();
        let target = reference.variable.clone();
        let table = reference.name.clone();
        let pattern = self.single(reference, &mut scope)?;
        let mut clauses = match class {
            Class::Node(_) => self.node_insert(pattern, &table, &target, columns, rows)?,
            Class::Relationship { start, kind, end } => {
                let labels = Endpoints { start, kind, end };
                self.relationship_insert(&labels, &table, &target, columns, rows, span)?
            }
        };
        clauses.extend(self.returning(returning.as_deref(), &scope)?);
        Ok(syntax::Statement { clauses })
    }

    /// The literal rows of `VALUES`, each with one value per column.
    fn rows(
        &self,
        source: Option<&ast::Query>,
        width: usize,
        span: Span,
    ) -> Result<Vec<Vec<Expr>>> {
        let Some(ast::Query {
            with: None,
            body,
            order_by: None,
            limit_clause: None,
            fetch: None,
            ..
        }) = source
        else {
            return Err(unsupported("INSERT without VALUES", span));
        };
        let ast::SetExpr::Values(values) = body.as_ref() else {
            return Err(unsupported("INSERT ... SELECT", span));
        };
        if values.rows.is_empty() {
            return Err(invalid(
                detail::INVALID_STATEMENT,
                "VALUES needs a row",
                span,
            ));
        }
        let empty = Scope::default();
        values
            .rows
            .iter()
            .map(|row| {
                if row.content.len() != width {
                    return Err(invalid(
                        detail::INVALID_STATEMENT,
                        format!(
                            "a VALUES row has {} values for {width} columns",
                            row.content.len()
                        ),
                        self.at(row),
                    ));
                }
                row.content
                    .iter()
                    .map(|value| self.expr(value, &empty))
                    .collect()
            })
            .collect()
    }

    /// `CREATE (t:Label {..})`, or for several rows
    /// `UNWIND [{..}, ..] AS properties CREATE (t:Label) SET t = properties`.
    fn node_insert(
        &self,
        pattern: syntax::Pattern,
        table: &str,
        target: &str,
        columns: &[ast::ObjectName],
        rows: Vec<Vec<Expr>>,
    ) -> Result<Vec<Clause>> {
        let keys = columns
            .iter()
            .map(|column| {
                let span = self.at(column);
                match self.column_name(column, span)? {
                    (None, name) => Ok(name),
                    (Some(qualifier), name)
                        if qualifier.eq_ignore_ascii_case(table)
                            || qualifier.eq_ignore_ascii_case(target) =>
                    {
                        Ok(name)
                    }
                    (Some(qualifier), _) => Err(invalid(
                        detail::UNDEFINED_VARIABLE,
                        format!("`{qualifier}` is not the table this INSERT writes"),
                        span,
                    )),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mut seen = std::collections::HashSet::new();
        keys.iter()
            .enumerate()
            .find(|(_, key)| !seen.insert(*key))
            .map_or(Ok(()), |(index, key)| {
                Err(invalid(
                    detail::INVALID_STATEMENT,
                    format!("column `{key}` is listed twice"),
                    self.at(&columns[index]),
                ))
            })?;
        let entries = |row: Vec<Expr>| keys.iter().cloned().zip(row).collect::<Vec<_>>();
        let mut rows = rows.into_iter();
        let first = rows.next().expect("VALUES has a row");
        let rest: Vec<_> = rows.collect();
        let [node] =
            <[syntax::Node; 1]>::try_from(pattern.nodes).expect("a node table is one node");
        if rest.is_empty() {
            let properties = entries(first);
            return Ok(vec![Clause::Create(vec![syntax::Pattern {
                name: None,
                nodes: vec![syntax::Node {
                    has_properties: true,
                    properties,
                    ..node
                }],
                relationships: Vec::new(),
            }])]);
        }
        let span = first.first().map_or(Span { start: 0, end: 0 }, |e| e.span);
        let row = if target == "properties" {
            "row"
        } else {
            "properties"
        };
        let list = std::iter::once(first)
            .chain(rest)
            .map(|values| Expr::new(ExprKind::Map(entries(values)), span))
            .collect::<Result<Vec<_>>>()?;
        Ok(vec![
            Clause::Unwind {
                expression: Expr::new(ExprKind::List(list), span)?,
                name: row.to_owned(),
            },
            Clause::Create(vec![syntax::Pattern {
                name: None,
                nodes: vec![node],
                relationships: Vec::new(),
            }]),
            Clause::Set(vec![Assignment::Replace(
                target.to_owned(),
                variable(row, span)?,
            )]),
        ])
    }

    /// `MATCH (_start:A {..}), (_end:B {..}) CREATE (_start)-[r:T {..}]->(_end)`,
    /// or for several rows the same after
    /// `UNWIND [{start: {..}, rel: {..}, end: {..}, start_id: .., end_id: ..}, ..] AS properties`.
    fn relationship_insert(
        &self,
        labels: &Endpoints,
        table: &str,
        target: &str,
        columns: &[ast::ObjectName],
        rows: Vec<Vec<Expr>>,
        span: Span,
    ) -> Result<Vec<Clause>> {
        const ROW: &str = "properties";
        let roles = columns
            .iter()
            .map(|column| self.role(column, labels, table, target))
            .collect::<Result<Vec<_>>>()?;
        for (side, label) in [(Side::Start, &labels.start), (Side::End, &labels.end)] {
            if !roles.iter().any(|(role, _)| role.side() == side) {
                return Err(invalid(
                    detail::INVALID_STATEMENT,
                    format!(
                        "INSERT into a relationship table needs a column of its {} node, \
                         such as `{label}.<property>` or `v${}_id`, to find the node it connects",
                        side.group(),
                        label.to_lowercase()
                    ),
                    span,
                ));
            }
        }
        let single = rows.len() == 1;
        // A column's value: the literal of the only row, or its entry in the
        // unwound row map.
        let value = |index: usize, role: &Role, at: Span| -> Result<Expr> {
            if single {
                return Ok(rows[0][index].clone());
            }
            let row = variable(ROW, at)?;
            match role {
                Role::Property(side, key) => property(property(row, side.group(), at)?, key, at),
                Role::StartId => property(row, "start_id", at),
                Role::EndId => property(row, "end_id", at),
            }
        };
        let properties = |side: Side| {
            roles
                .iter()
                .enumerate()
                .filter_map(|(index, (role, at))| match role {
                    Role::Property(s, key) if *s == side => {
                        Some(value(index, role, *at).map(|v| (key.clone(), v)))
                    }
                    Role::Property(..) | Role::StartId | Role::EndId => None,
                })
                .collect::<Result<Vec<_>>>()
        };
        let ids = roles
            .iter()
            .enumerate()
            .filter_map(|(index, (role, at))| {
                let name = match role {
                    Role::StartId => super::tables::START,
                    Role::EndId => super::tables::END,
                    Role::Property(..) => return None,
                };
                Some(
                    variable(name, *at)
                        .and_then(|node| super::call("id", vec![node], *at))
                        .and_then(|id| {
                            super::binary(r::Binary::Equal, id, value(index, role, *at)?, *at)
                        }),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let endpoint = |side: Side, label: &str, name: &str| -> Result<syntax::Pattern> {
            let properties = properties(side)?;
            Ok(syntax::Pattern {
                name: None,
                nodes: vec![syntax::Node {
                    has_properties: !properties.is_empty(),
                    name: Some(name.to_owned()),
                    labels: vec![label.to_owned()],
                    properties,
                }],
                relationships: Vec::new(),
            })
        };
        let bound = |name: &str| syntax::Node {
            has_properties: false,
            name: Some(name.to_owned()),
            labels: Vec::new(),
            properties: Vec::new(),
        };
        let mut clauses = Vec::new();
        if !single {
            let map = |values: &[Expr]| -> Result<Expr> {
                let groups = [Side::Start, Side::Relationship, Side::End].map(|side| {
                    let entries = roles
                        .iter()
                        .zip(values)
                        .filter_map(|((role, _), value)| match role {
                            Role::Property(s, key) if *s == side => {
                                Some((key.clone(), value.clone()))
                            }
                            Role::Property(..) | Role::StartId | Role::EndId => None,
                        })
                        .collect();
                    Ok((
                        side.group().to_owned(),
                        Expr::new(ExprKind::Map(entries), span)?,
                    ))
                });
                let ids = roles
                    .iter()
                    .zip(values)
                    .filter_map(|((role, _), value)| match role {
                        Role::StartId => Some(Ok(("start_id".to_owned(), value.clone()))),
                        Role::EndId => Some(Ok(("end_id".to_owned(), value.clone()))),
                        Role::Property(..) => None,
                    });
                Expr::new(
                    ExprKind::Map(groups.into_iter().chain(ids).collect::<Result<_>>()?),
                    span,
                )
            };
            clauses.push(Clause::Unwind {
                expression: Expr::new(
                    ExprKind::List(
                        rows.iter()
                            .map(|values| map(values))
                            .collect::<Result<_>>()?,
                    ),
                    span,
                )?,
                name: ROW.to_owned(),
            });
        }
        clauses.push(Clause::Match {
            patterns: vec![
                endpoint(Side::Start, &labels.start, super::tables::START)?,
                endpoint(Side::End, &labels.end, super::tables::END)?,
            ],
            optional: false,
            predicate: match ids.len() {
                0 => None,
                _ => Some(super::expr::connective(r::Connective::And, ids, span)?),
            },
        });
        clauses.push(Clause::Create(vec![syntax::Pattern {
            name: None,
            nodes: vec![bound(super::tables::START), bound(super::tables::END)],
            relationships: vec![syntax::Relationship {
                name: Some(target.to_owned()),
                types: vec![labels.kind.clone()],
                direction: r::Direction::Outgoing,
                properties: properties(Side::Relationship)?,
            }],
        }]));
        Ok(clauses)
    }

    /// Where a relationship-table column goes: a property or the id of the
    /// start node, end node or relationship.
    fn role(
        &self,
        column: &ast::ObjectName,
        labels: &Endpoints,
        table: &str,
        target: &str,
    ) -> Result<(Role, Span)> {
        let span = self.at(column);
        let (qualifier, name) = self.column_name(column, span)?;
        let side = match qualifier.as_deref() {
            None => {
                let id = name
                    .strip_prefix("v$")
                    .and_then(|rest| rest.strip_suffix("_id"))
                    .map(|label| {
                        let start = label.eq_ignore_ascii_case(&labels.start);
                        let end = label.eq_ignore_ascii_case(&labels.end);
                        match (start, end) {
                            (true, false) => Ok(Side::Start),
                            (false, true) => Ok(Side::End),
                            (true, true) => Err(invalid(
                                detail::AMBIGUOUS_COLUMN,
                                format!("`{name}` names both endpoints; qualify `v$id` with `_start` or `_end`"),
                                span,
                            )),
                            (false, false) => Err(invalid(
                                detail::UNDEFINED_VARIABLE,
                                format!("`{name}` names neither endpoint"),
                                span,
                            )),
                        }
                    })
                    .transpose()?;
                let role = match id {
                    None => match shorthand(&name, &labels.start, &labels.end, span)? {
                        Some((Endpoint::Start, column)) => {
                            Role::Property(Side::Start, column.to_owned())
                        }
                        Some((Endpoint::End, column)) => {
                            Role::Property(Side::End, column.to_owned())
                        }
                        None => Role::Property(Side::Relationship, name),
                    },
                    Some(Side::Start) => Role::StartId,
                    Some(Side::End) => Role::EndId,
                    Some(Side::Relationship) => unreachable!("an endpoint id names a node"),
                };
                return Ok((role, span));
            }
            Some(q) if q.eq_ignore_ascii_case(super::tables::START) => Side::Start,
            Some(q) if q.eq_ignore_ascii_case(super::tables::END) => Side::End,
            Some(q)
                if q.eq_ignore_ascii_case(&labels.kind)
                    || q.eq_ignore_ascii_case(table)
                    || q.eq_ignore_ascii_case(target) =>
            {
                Side::Relationship
            }
            Some(q) => match (
                q.eq_ignore_ascii_case(&labels.start),
                q.eq_ignore_ascii_case(&labels.end),
            ) {
                (true, false) => Side::Start,
                (false, true) => Side::End,
                (true, true) => {
                    return Err(invalid(
                        detail::AMBIGUOUS_COLUMN,
                        format!("`{q}` names both endpoints; qualify with `_start` or `_end`"),
                        span,
                    ));
                }
                (false, false) => {
                    return Err(invalid(
                        detail::UNDEFINED_VARIABLE,
                        format!("`{q}` is neither endpoint nor the relationship"),
                        span,
                    ));
                }
            },
        };
        let role = match (side, name.eq_ignore_ascii_case("v$id")) {
            (Side::Start, true) => Role::StartId,
            (Side::End, true) => Role::EndId,
            (Side::Relationship, true) => {
                return Err(invalid(
                    detail::INVALID_STATEMENT,
                    "a new relationship has no id to insert",
                    span,
                ));
            }
            (_, false) => Role::Property(side, name),
        };
        Ok((role, span))
    }

    /// `[qualifier.]column` of a DML column list.
    fn column_name(
        &self,
        column: &ast::ObjectName,
        span: Span,
    ) -> Result<(Option<String>, String)> {
        let parts = column
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| unsupported("this column name", span))
            })
            .collect::<Result<Vec<_>>>()?;
        match parts.as_slice() {
            [name] => Ok((None, name.clone())),
            [.., qualifier, name] => Ok((Some(qualifier.clone()), name.clone())),
            [] => Err(unsupported("an empty column name", span)),
        }
    }

    pub(super) fn update(&self, update: &ast::Update) -> Result<syntax::Statement> {
        let span = self.at(update);
        if update.from.is_some() {
            return Err(unsupported(
                "UPDATE ... FROM; join in the UPDATE target instead",
                span,
            ));
        }
        if !update.optimizer_hints.is_empty()
            || update.output.is_some()
            || update.or.is_some()
            || !update.order_by.is_empty()
            || update.limit.is_some()
        {
            return Err(unsupported("dialect-specific UPDATE clauses", span));
        }
        let (patterns, scope) = self.from(std::slice::from_ref(&update.table))?;
        let predicate = update
            .selection
            .as_ref()
            .map(|selection| self.expr(selection, &scope))
            .transpose()?;
        let assignments = update
            .assignments
            .iter()
            .map(|assignment| {
                let ast::AssignmentTarget::ColumnName(column) = &assignment.target else {
                    return Err(unsupported(
                        "assigning a row of columns",
                        self.at(assignment),
                    ));
                };
                let at = self.at(column);
                let parts = column
                    .0
                    .iter()
                    .map(|part| {
                        part.as_ident()
                            .cloned()
                            .ok_or_else(|| unsupported("this column", at))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let target = scope.column(&parts, at)?;
                if !matches!(target.kind(), ExprKind::Property(..)) {
                    return Err(invalid(
                        detail::INVALID_STATEMENT,
                        "UPDATE can only assign properties",
                        at,
                    ));
                }
                Ok((target, self.expr(&assignment.value, &scope)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut clauses = vec![Clause::Match {
            patterns,
            optional: false,
            predicate,
        }];
        // SQL computes every new value from the row before the update, while
        // Cypher applies SET items in turn; computing the values in a WITH
        // keeps `SET a = b, b = a` a swap.
        let sequential = assignments.len() > 1
            && assignments
                .iter()
                .any(|(_, value)| super::tables::reads_table(&scope, value));
        let assignments = if sequential {
            let mut items = scope
                .variables()
                .map(|name| Ok(super::item(variable(name, span)?, None)))
                .collect::<Result<Vec<_>>>()?;
            let assignments = assignments
                .into_iter()
                .enumerate()
                .map(|(index, (target, value))| {
                    let name = format!("__set_{index}");
                    let at = value.span;
                    items.push(super::item(value, Some(name.clone())));
                    Ok(Assignment::Property(target, variable(&name, at)?))
                })
                .collect::<Result<Vec<_>>>()?;
            clauses.push(Clause::Project {
                returning: false,
                items,
                distinct: false,
                ordering: Vec::new(),
                skip: None,
                limit: None,
                predicate: None,
            });
            assignments
        } else {
            assignments
                .into_iter()
                .map(|(target, value)| Assignment::Property(target, value))
                .collect()
        };
        clauses.push(Clause::Set(assignments));
        clauses.extend(self.returning(update.returning.as_deref(), &scope)?);
        Ok(syntax::Statement { clauses })
    }

    pub(super) fn delete(&self, delete: &ast::Delete) -> Result<syntax::Statement> {
        let span = self.at(delete);
        if !delete.tables.is_empty()
            || delete.using.is_some()
            || delete.returning.is_some()
            || delete.output.is_some()
            || !delete.order_by.is_empty()
            || delete.limit.is_some()
            || !delete.optimizer_hints.is_empty()
        {
            return Err(unsupported(
                "multi-table DELETE, USING, RETURNING, ORDER BY and LIMIT in DELETE",
                span,
            ));
        }
        let (ast::FromTable::WithFromKeyword(from) | ast::FromTable::WithoutKeyword(from)) =
            &delete.from;
        let [ast::TableWithJoins { relation, joins }] = from.as_slice() else {
            return Err(unsupported("DELETE from several tables", span));
        };
        if !joins.is_empty() {
            return Err(unsupported("DELETE with joins", span));
        }
        let mut scope = Scope::default();
        let reference = self.reference(relation)?;
        let target = reference.variable.clone();
        let pattern = self.single(reference, &mut scope)?;
        let predicate = delete
            .selection
            .as_ref()
            .map(|selection| self.expr(selection, &scope))
            .transpose()?;
        Ok(syntax::Statement {
            clauses: vec![
                Clause::Match {
                    patterns: vec![pattern],
                    optional: false,
                    predicate,
                },
                Clause::Delete {
                    expressions: vec![variable(&target, span)?],
                    detach: false,
                },
            ],
        })
    }

    /// `TRUNCATE t` deletes every node with the table's label and its
    /// relationships, or every relationship of a relationship table.
    pub(super) fn truncate(&self, truncate: &ast::Truncate) -> Result<syntax::Statement> {
        let span = self.at(truncate);
        let ast::Truncate {
            table_names,
            partitions: None,
            table: _,
            if_exists: false,
            identity: None,
            cascade: None,
            on_cluster: None,
        } = truncate
        else {
            return Err(unsupported("TRUNCATE options", span));
        };
        let [ast::TruncateTableTarget {
            name,
            only: false,
            has_asterisk: false,
        }] = table_names.as_slice()
        else {
            return Err(unsupported("TRUNCATE of several tables or with ONLY", span));
        };
        let reference: Reference = self.named(name, None, span)?;
        let detach = matches!(reference.class, Class::Node(_));
        let target = reference.variable.clone();
        let pattern = self.single(reference, &mut Scope::default())?;
        Ok(syntax::Statement {
            clauses: vec![
                Clause::Match {
                    patterns: vec![pattern],
                    optional: false,
                    predicate: None,
                },
                Clause::Delete {
                    expressions: vec![variable(&target, span)?],
                    detach,
                },
            ],
        })
    }

    /// `RETURNING` as a final `RETURN`.
    fn returning(
        &self,
        returning: Option<&[ast::SelectItem]>,
        scope: &Scope,
    ) -> Result<Option<Clause>> {
        returning
            .map(|items| {
                Ok(Clause::Project {
                    returning: true,
                    items: self.items(items, scope)?,
                    distinct: false,
                    ordering: Vec::new(),
                    skip: None,
                    limit: None,
                    predicate: None,
                })
            })
            .transpose()
    }
}

/// The start label, type and end label of a relationship table.
struct Endpoints {
    start: String,
    kind: String,
    end: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Start,
    End,
    Relationship,
}

impl Side {
    fn group(self) -> &'static str {
        match self {
            Side::Start => "start",
            Side::End => "end",
            Side::Relationship => "rel",
        }
    }
}

/// Where one relationship-table column goes.
#[derive(Debug)]
enum Role {
    Property(Side, String),
    /// `v$<start>_id` or `_start.v$id`: the start node's id.
    StartId,
    EndId,
}

impl Role {
    fn side(&self) -> Side {
        match self {
            Role::Property(side, _) => *side,
            Role::StartId => Side::Start,
            Role::EndId => Side::End,
        }
    }
}
