//! Clause barriers materialize input before staging any graph changes.
use super::memory::Rows;
use super::{graph, ExecutionContext, Limits, Result};
use helix_planner::relational::{self as r, GraphValues};
use std::collections::{BTreeMap, BTreeSet};

impl ExecutionContext<'_> {
    pub(super) async fn create_rows(
        &mut self,
        mut rows: Rows,
        pattern: &r::Pattern,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        for row in &mut rows {
            for node in &pattern.nodes {
                if row[node.slot.0 as usize] != r::Value::Null {
                    continue;
                }
                let Some(label) = node.label.as_deref() else {
                    continue;
                };
                self.check_execution_deadline()?;
                let graph = self.graph_batch(std::slice::from_ref(row)).await?;
                let properties = node
                    .properties
                    .iter()
                    .map(|(k, e)| {
                        Ok((
                            k.clone(),
                            self.evaluate(row, parameters, &graph, limits).eval(e)?,
                        ))
                    })
                    .collect::<r::Result<BTreeMap<_, _>>>()?;
                let id = self
                    .row_create_node(label, graph::properties(properties)?)
                    .await?;
                row[node.slot.0 as usize] = r::Value::Entity(r::Entity::Node(id));
            }
            for relationship in &pattern.relationships {
                let r::Value::Entity(r::Entity::Node(mut from)) = row[relationship.from.0 as usize]
                else {
                    return Err(r::QueryError::runtime(
                        "TypeError",
                        "ExpectedNode",
                        "relationship endpoint must be a node",
                    )
                    .into());
                };
                let r::Value::Entity(r::Entity::Node(mut to)) = row[relationship.to.0 as usize]
                else {
                    return Err(r::QueryError::runtime(
                        "TypeError",
                        "ExpectedNode",
                        "relationship endpoint must be a node",
                    )
                    .into());
                };
                if relationship.direction == r::Direction::Incoming {
                    std::mem::swap(&mut from, &mut to);
                }
                let graph = self.graph_batch(std::slice::from_ref(row)).await?;
                let properties = relationship
                    .properties
                    .iter()
                    .map(|(k, e)| {
                        Ok((
                            k.clone(),
                            self.evaluate(row, parameters, &graph, limits).eval(e)?,
                        ))
                    })
                    .collect::<r::Result<BTreeMap<_, _>>>()?;
                let label = relationship.types.first().ok_or_else(|| {
                    r::QueryError::runtime(
                        "SyntaxError",
                        "NoRelationshipType",
                        "new relationships require one type",
                    )
                })?;
                let id = self
                    .row_create_edge(from, to, label, graph::properties(properties)?)
                    .await?;
                row[relationship.slot.0 as usize] = r::Value::Entity(r::Entity::Relationship(id));
            }
            for path in &pattern.paths {
                let nodes = path
                    .nodes
                    .iter()
                    .map(|s| match row[s.0 as usize] {
                        r::Value::Entity(r::Entity::Node(id)) => Ok(id),
                        r::Value::Null
                        | r::Value::Boolean(_)
                        | r::Value::Integer(_)
                        | r::Value::Float(_)
                        | r::Value::String(_)
                        | r::Value::List(_)
                        | r::Value::Map(_)
                        | r::Value::Entity(_)
                        | r::Value::Path(_) => Err(r::QueryError::runtime(
                            "TypeError",
                            "ExpectedNode",
                            "path must contain nodes",
                        )),
                    })
                    .collect::<r::Result<_>>()?;
                let relationships = path
                    .relationships
                    .iter()
                    .map(|s| match row[s.0 as usize] {
                        r::Value::Entity(r::Entity::Relationship(id)) => Ok(id),
                        r::Value::Null
                        | r::Value::Boolean(_)
                        | r::Value::Integer(_)
                        | r::Value::Float(_)
                        | r::Value::String(_)
                        | r::Value::List(_)
                        | r::Value::Map(_)
                        | r::Value::Entity(_)
                        | r::Value::Path(_) => Err(r::QueryError::runtime(
                            "TypeError",
                            "ExpectedRelationship",
                            "path must contain relationships",
                        )),
                    })
                    .collect::<r::Result<_>>()?;
                row[path.slot.0 as usize] = r::Value::Path(r::Path::new(nodes, relationships)?);
            }
        }
        Ok(rows)
    }

    pub(super) async fn update_rows(
        &mut self,
        rows: Rows,
        updates: &[r::PropertyMutation],
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        for row in &rows {
            for update in updates {
                self.check_execution_deadline()?;
                let graph = self.graph_batch(std::slice::from_ref(row)).await?;
                let evaluation = self.evaluate(row, parameters, &graph, limits);
                let (target, changes, replace) = match update {
                    r::PropertyMutation::Set { entity, key, value } => (
                        row[entity.0 as usize].clone(),
                        BTreeMap::from([(key.clone(), evaluation.eval(value)?)]),
                        false,
                    ),
                    r::PropertyMutation::Remove { entity, key } => (
                        row[entity.0 as usize].clone(),
                        BTreeMap::from([(key.clone(), r::Value::Null)]),
                        false,
                    ),
                    r::PropertyMutation::Replace { entity, properties }
                    | r::PropertyMutation::Extend { entity, properties } => {
                        let target = row[entity.0 as usize].clone();
                        let value = evaluation.eval(properties)?;
                        let map = match value {
                            r::Value::Map(map) => map,
                            r::Value::Entity(entity) => evaluation.properties(entity)?,
                            r::Value::Null => BTreeMap::new(),
                            r::Value::Boolean(_)
                            | r::Value::Integer(_)
                            | r::Value::Float(_)
                            | r::Value::String(_)
                            | r::Value::List(_)
                            | r::Value::Path(_) => {
                                return Err(r::QueryError::runtime(
                                    "TypeError",
                                    "ExpectedMap",
                                    "property update requires a map",
                                )
                                .into())
                            }
                        };
                        (
                            target,
                            map,
                            matches!(update, r::PropertyMutation::Replace { .. }),
                        )
                    }
                };
                let entity = match target {
                    r::Value::Null => continue,
                    r::Value::Entity(entity) => entity,
                    r::Value::Boolean(_)
                    | r::Value::Integer(_)
                    | r::Value::Float(_)
                    | r::Value::String(_)
                    | r::Value::List(_)
                    | r::Value::Map(_)
                    | r::Value::Path(_) => {
                        return Err(r::QueryError::runtime(
                            "TypeError",
                            "ExpectedEntity",
                            "SET and REMOVE require a graph entity",
                        )
                        .into())
                    }
                };
                if replace {
                    for key in graph.properties(entity)?.keys() {
                        if !changes.contains_key(key) {
                            self.row_edit_property(entity, key, None).await?;
                        }
                    }
                }
                for (key, value) in changes {
                    let value = if value == r::Value::Null {
                        None
                    } else {
                        Some(graph::to_property(value)?)
                    };
                    self.row_edit_property(entity, &key, value).await?;
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn delete_rows(
        &mut self,
        rows: Rows,
        expressions: &[r::Expression],
        detach: bool,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        let mut entities = BTreeSet::new();
        for batch in rows.chunks(limits.batch_rows) {
            let graph = self.graph_batch(batch).await?;
            // The type is immutable. Retain it for references that survive an
            // explicit or detach deletion; properties still require live rows.
            for (entity, data) in &graph.entities {
                let (r::Entity::Relationship(id), graph::EntityKind::Relationship { label, .. }) =
                    (entity, &data.kind)
                else {
                    continue;
                };
                if !self.row_relationship_types.contains_key(id) {
                    let memory = self.row_budget().reserve(label.len().saturating_add(128))?;
                    self.row_relationship_types
                        .insert(*id, (label.clone(), memory));
                }
            }
            for row in batch {
                for expression in expressions {
                    match self
                        .evaluate(row, parameters, &graph, limits)
                        .eval(expression)?
                    {
                        r::Value::Null => {}
                        r::Value::Entity(entity) => {
                            entities.insert(entity);
                        }
                        r::Value::Path(path) => {
                            entities.extend(path.nodes().iter().copied().map(r::Entity::Node));
                            entities.extend(
                                path.relationships()
                                    .iter()
                                    .copied()
                                    .map(r::Entity::Relationship),
                            );
                        }
                        r::Value::Boolean(_)
                        | r::Value::Integer(_)
                        | r::Value::Float(_)
                        | r::Value::String(_)
                        | r::Value::List(_)
                        | r::Value::Map(_) => {
                            return Err(r::QueryError::runtime(
                                "TypeError",
                                "InvalidArgumentType",
                                "DELETE requires graph entities or paths",
                            )
                            .into())
                        }
                    }
                }
            }
        }
        self.row_delete_entities(entities, detach).await?;
        Ok(rows)
    }
}
