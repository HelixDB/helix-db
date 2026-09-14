//! Clause barriers materialize input before staging any graph changes.
use super::memory::Rows;
use super::{graph, ExecutionContext, Limits, Result};
use helix_planner::relational as r;
use std::collections::BTreeMap;

impl ExecutionContext<'_> {
    /// Property expressions share one remaining budget. Keep both the evaluated
    /// map and its storage conversion admitted until the write consumes them.
    fn create_properties(
        &self,
        fields: &[(String, r::Expression)],
        mut evaluation: r::Evaluation<'_>,
    ) -> Result<(
        Vec<crate::encoding::v2::values::property::Property>,
        super::memory::Reservation,
    )> {
        // Cover sparse B-tree nodes, key copies and the destination property
        // vector before allocating any entries. Values are admitted below.
        let mut bytes = fields.iter().fold(0_usize, |bytes, (key, _)| {
            bytes
                .saturating_add(1024)
                .saturating_add(key.len().saturating_mul(2))
        });
        let mut memory = self.row_budget().reserve(bytes)?;
        let mut properties = BTreeMap::new();
        for (key, expression) in fields {
            self.check_execution_deadline()?;
            // Typed list conversion may retain source and destination buffers
            // together. Admission for both precedes their construction.
            evaluation.max_value_bytes = self.row_budget().available() / 2;
            let value = evaluation.eval(expression)?;
            bytes = bytes.saturating_add(value.allocated_bytes().saturating_mul(2));
            memory.resize(bytes)?;
            properties.insert(key.clone(), value);
        }
        Ok((graph::properties(properties)?, memory))
    }

    pub(super) async fn create_rows(
        &mut self,
        mut rows: Rows,
        pattern: &r::Pattern,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        // Every surviving input row receives these paths. Reserve their exact
        // vector capacities before staging writes or allocating any path IDs.
        let path_bytes = pattern.paths.iter().fold(0_usize, |bytes, path| {
            bytes.saturating_add(
                path.nodes
                    .len()
                    .saturating_add(path.relationships.len())
                    .saturating_mul(size_of::<u64>()),
            )
        });
        rows.admit_payload(path_bytes.saturating_mul(rows.len()))?;
        for row in &mut rows {
            for node in &pattern.nodes {
                if row[node.slot.0 as usize] != r::Value::Null {
                    continue;
                }
                let Some(label) = node.label.as_deref() else {
                    continue;
                };
                self.check_execution_deadline()?;
                let graph = self
                    .expression_graph_batch(
                        std::slice::from_ref(row),
                        node.properties.iter().map(|(_, value)| value),
                    )
                    .await?;
                let (properties, _properties_memory) = self.create_properties(
                    &node.properties,
                    self.evaluate(row, parameters, &graph, limits),
                )?;
                let id = self.row_create_node(label, properties).await?;
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
                let graph = self
                    .expression_graph_batch(
                        std::slice::from_ref(row),
                        relationship.properties.iter().map(|(_, value)| value),
                    )
                    .await?;
                let (properties, _properties_memory) = self.create_properties(
                    &relationship.properties,
                    self.evaluate(row, parameters, &graph, limits),
                )?;
                let label = relationship.types.first().ok_or_else(|| {
                    r::QueryError::runtime(
                        "SyntaxError",
                        "NoRelationshipType",
                        "new relationships require one type",
                    )
                })?;
                let id = self.row_create_edge(from, to, label, properties).await?;
                row[relationship.slot.0 as usize] = r::Value::Entity(r::Entity::Relationship(id));
            }
            for path in &pattern.paths {
                let mut nodes = Vec::with_capacity(path.nodes.len());
                for s in &path.nodes {
                    nodes.push(match row[s.0 as usize] {
                        r::Value::Entity(r::Entity::Node(id)) => id,
                        r::Value::Null
                        | r::Value::Boolean(_)
                        | r::Value::Integer(_)
                        | r::Value::Float(_)
                        | r::Value::String(_)
                        | r::Value::List(_)
                        | r::Value::Map(_)
                        | r::Value::Entity(_)
                        | r::Value::Path(_) => {
                            return Err(r::QueryError::runtime(
                                "TypeError",
                                "ExpectedNode",
                                "path must contain nodes",
                            )
                            .into())
                        }
                    });
                }
                let mut relationships = Vec::with_capacity(path.relationships.len());
                for s in &path.relationships {
                    relationships.push(match row[s.0 as usize] {
                        r::Value::Entity(r::Entity::Relationship(id)) => id,
                        r::Value::Null
                        | r::Value::Boolean(_)
                        | r::Value::Integer(_)
                        | r::Value::Float(_)
                        | r::Value::String(_)
                        | r::Value::List(_)
                        | r::Value::Map(_)
                        | r::Value::Entity(_)
                        | r::Value::Path(_) => {
                            return Err(r::QueryError::runtime(
                                "TypeError",
                                "ExpectedRelationship",
                                "path must contain relationships",
                            )
                            .into())
                        }
                    });
                }
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
                let expression = match update {
                    r::PropertyMutation::Set { value, .. } => Some(value),
                    r::PropertyMutation::Replace { properties, .. }
                    | r::PropertyMutation::Extend { properties, .. } => Some(properties),
                    r::PropertyMutation::Remove { .. } => None,
                };
                let graph = self
                    .expression_graph_batch(std::slice::from_ref(row), expression)
                    .await?;
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
                            r::Value::Entity(entity) => {
                                // A bare entity slot needs no properties during
                                // scalar evaluation. Hydrate only the selected
                                // map source, without evaluating the expression again.
                                let _source_memory = self
                                    .row_budget()
                                    .reserve(size_of::<r::Row>() + size_of::<r::Value>())?;
                                let source = [vec![r::Value::Entity(entity)]];
                                let mut demand =
                                    super::requirements::Requirements::new(self.row_budget())?;
                                demand.insert(r::Slot(0), r::PropertyRequirement::All)?;
                                let source_graph =
                                    self.graph_batch_required(&source, demand.values()).await?;
                                self.evaluate(row, parameters, &source_graph, limits)
                                    .properties(entity)?
                            }
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
                // The evaluated change map owns its values. Release expression
                // hydration before taking canonical mutation snapshots.
                drop(graph);
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
                // Expression evaluation checks construction against available
                // memory; retain that ownership across the asynchronous edits
                // and allow simultaneous typed-list conversion buffers.
                let _changes_memory = self.row_budget().reserve(changes.iter().fold(
                    0_usize,
                    |bytes, (key, value)| {
                        bytes
                            .saturating_add(1024)
                            .saturating_add(key.len().saturating_mul(2))
                            .saturating_add(value.allocated_bytes().saturating_mul(2))
                    },
                ))?;
                if matches!(
                    update,
                    r::PropertyMutation::Set { .. } | r::PropertyMutation::Remove { .. }
                ) {
                    for (key, value) in changes {
                        let value = if value == r::Value::Null {
                            None
                        } else {
                            Some(graph::to_property(value)?)
                        };
                        self.row_edit_property(entity, &key, value).await?;
                    }
                } else {
                    if changes.is_empty() && !replace {
                        continue;
                    }
                    use crate::index_lifecycle::graph_mutation::map;
                    let mut edit = map::Edit::new(if replace {
                        map::Mode::ReplaceUserProperties
                    } else {
                        map::Mode::Extend
                    });
                    for (key, value) in changes {
                        let value = if value == r::Value::Null {
                            None
                        } else {
                            Some(graph::to_property(value)?)
                        };
                        // Validate after this value's conversion, before the next
                        // item, preserving the frontend's observable error order.
                        edit.insert(key, value).map_err(|error| {
                            r::QueryError::runtime(
                                "UnsupportedFeature",
                                match error {
                                    map::NameError::Empty => "EmptyPropertyName",
                                    map::NameError::Reserved => "ReservedPropertyName",
                                },
                                error.to_string(),
                            )
                        })?;
                    }
                    self.row_edit_map(entity, edit).await?;
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
        self.check_execution_deadline()?;
        let mut demand = super::requirements::Requirements::new(self.row_budget())?;
        // Retain immutable types for surviving relationship references. User
        // properties are needed only by the deletion expressions themselves.
        for slot in rows.first().into_iter().flat_map(|row| 0..row.len()) {
            self.check_execution_deadline()?;
            demand.insert(r::Slot(slot as u32), r::PropertyRequirement::Metadata)?;
        }
        for expression in expressions {
            self.check_execution_deadline()?;
            expression.try_graph_requirements(|slot, requirement| {
                self.check_execution_deadline()?;
                demand.insert(slot, requirement)
            })?;
        }
        let mut entities = super::super::mutation::DeletionTargets::new(self.row_budget())?;
        for batch in rows.chunks(limits.batch_rows) {
            let graph = self.graph_batch_required(batch, demand.values()).await?;
            // The type is immutable. Retain it for references that survive an
            // explicit or detach deletion; properties still require live rows.
            for (entity, data) in &graph.entities {
                let (r::Entity::Relationship(id), graph::EntityKind::Relationship { label, .. }) =
                    (entity, &data.kind)
                else {
                    continue;
                };
                if !self.row_relationship_types.contains_key(id) {
                    // Entries are never removed individually. Their guards
                    // own successive node-growth deltas until the request ends.
                    let count = self.row_relationship_types.len();
                    let previous = if count == 0 {
                        0
                    } else {
                        r::allocation::btree_bytes::<u64, (String, super::memory::Reservation)>(
                            count,
                        )
                    };
                    let next = r::allocation::btree_bytes::<
                        u64,
                        (String, super::memory::Reservation),
                    >(count.saturating_add(1));
                    let memory = self
                        .row_budget()
                        .reserve(label.len().saturating_add(next.saturating_sub(previous)))?;
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
                            entities.insert(entity)?;
                        }
                        r::Value::Path(path) => {
                            for id in path.nodes() {
                                entities.insert(r::Entity::Node(*id))?;
                            }
                            for id in path.relationships() {
                                entities.insert(r::Entity::Relationship(*id))?;
                            }
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
        drop(demand);
        self.row_delete_entities(entities, detach).await?;
        Ok(rows)
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;

    #[tokio::test]
    async fn property_maps_share_admission_and_keep_conversion_ownership() {
        let db =
            crate::execution::interpreter::test_support::open_db("property-map-admission").await;
        let mut ctx = ExecutionContext::new(&db, helix_planner::context::ParamBindings::default());
        ctx.row_memory = Some(super::super::memory::Budget::new(32 * 1024));
        let graph = super::super::GraphBatch::default();
        let params = BTreeMap::from([(
            "payload".to_owned(),
            r::Value::String("x".repeat(12 * 1024)),
        )]);
        let fields = vec![("a".to_owned(), r::Expression::Parameter("payload".into()))];
        let (properties, memory) = ctx
            .create_properties(
                &fields,
                ctx.evaluate(&[], &params, &graph, Limits::default()),
            )
            .unwrap();
        assert_eq!(properties.len(), 1);
        assert!(ctx.row_budget().available() < 8 * 1024);
        drop(properties);
        // The guard follows the conversion through the asynchronous write.
        assert!(ctx.row_budget().available() < 8 * 1024);
        drop(memory);
        assert_eq!(ctx.row_budget().available(), 32 * 1024);
        let mut fields = fields;
        fields.push(("b".into(), r::Expression::Parameter("payload".into())));
        assert!(
            matches!(ctx.create_properties(&fields, ctx.evaluate(&[], &params, &graph, Limits::default())), Err(crate::cypher::Error::Query(error)) if error.detail == "MemoryLimit")
        );
        assert_eq!(ctx.row_budget().available(), 32 * 1024);
        for (key, value, detail) in [
            ("$label", r::Value::Null, "ReservedPropertyName"),
            ("", r::Value::Integer(1), "EmptyPropertyName"),
            (
                "mixed",
                r::Value::List(vec![r::Value::Integer(1), r::Value::Boolean(true)]),
                "InvalidPropertyType",
            ),
        ] {
            let fields = vec![(key.into(), r::Expression::Literal(value))];
            assert!(
                matches!(ctx.create_properties(&fields, ctx.evaluate(&[], &params, &graph, Limits::default())), Err(crate::cypher::Error::Query(error)) if error.detail == detail)
            );
            assert_eq!(ctx.row_budget().available(), 32 * 1024);
        }
        ctx.fail_deadline_after(0);
        assert!(matches!(
            ctx.create_properties(
                &fields,
                ctx.evaluate(&[], &params, &graph, Limits::default())
            ),
            Err(crate::cypher::Error::Storage(
                crate::HelixDbError::QueryDeadlineExceeded
            ))
        ));
        assert_eq!(ctx.row_budget().available(), 32 * 1024);
        drop(ctx);
        db.close().await.unwrap();
    }
}
