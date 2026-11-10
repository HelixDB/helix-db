//! Clause barriers materialize input before staging any graph changes.
//!
//! CREATE stages rows in order, because later expressions may read earlier
//! entities, then proves a batch's bound endpoints with one read. SET folds
//! each entity's edits into one change. Either failure drops the whole
//! transaction, so a later check commits nothing a check per row would not.
use super::super::mutation::EditKind;
use super::memory::Rows;
use super::{graph, ExecutionContext, Limits, Result};
use crate::index_lifecycle::graph_mutation::map;
use helix_planner::relational as r;
use helix_planner::relational::{category, detail};
use std::collections::BTreeMap;

impl ExecutionContext<'_> {
    /// Property expressions share one remaining budget. Keep both the evaluated
    /// map and its storage conversion admitted until the write consumes them.
    fn create_properties<'a>(
        &'a self,
        fields: &[(String, r::Expression)],
        mut evaluation: r::Evaluation<'a>,
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
            let allowance = self.row_budget().evaluation_memory();
            evaluation.memory = allowance.capped(allowance.available() / 2);
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
        // Endpoints this clause creates exist by construction. Bound ones are
        // checked one batch at a time, after their rows are staged.
        let row_endpoints = pattern.relationships.len().saturating_mul(2);
        let endpoint_capacity = limits
            .batch_rows
            .min(rows.len())
            .saturating_mul(row_endpoints);
        let _endpoint_memory = self
            .row_budget()
            .reserve(endpoint_capacity.saturating_mul(size_of::<u64>()))?;
        let mut bound_endpoints = Vec::with_capacity(endpoint_capacity);
        for row in &mut rows {
            if bound_endpoints.len().saturating_add(row_endpoints) > endpoint_capacity {
                self.row_require_nodes(&bound_endpoints).await?;
                bound_endpoints.clear();
            }
            bound_endpoints.extend(
                pattern
                    .relationships
                    .iter()
                    .flat_map(|relationship| [relationship.from, relationship.to])
                    .filter_map(|slot| {
                        let r::Value::Entity(r::Entity::Node(id)) = row[slot.0 as usize] else {
                            return None;
                        };
                        Some(id)
                    }),
            );
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
                        category::TYPE_ERROR,
                        detail::EXPECTED_NODE,
                        "relationship endpoint must be a node",
                    )
                    .into());
                };
                let r::Value::Entity(r::Entity::Node(mut to)) = row[relationship.to.0 as usize]
                else {
                    return Err(r::QueryError::runtime(
                        category::TYPE_ERROR,
                        detail::EXPECTED_NODE,
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
                        category::SYNTAX_ERROR,
                        detail::NO_RELATIONSHIP_TYPE,
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
                                category::TYPE_ERROR,
                                detail::EXPECTED_NODE,
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
                                category::TYPE_ERROR,
                                detail::EXPECTED_RELATIONSHIP,
                                "path must contain relationships",
                            )
                            .into())
                        }
                    });
                }
                row[path.slot.0 as usize] = r::Value::Path(r::Path::new(nodes, relationships)?);
            }
        }
        self.row_require_nodes(&bound_endpoints).await?;
        Ok(rows)
    }

    pub(super) async fn update_rows(
        &mut self,
        rows: Rows,
        updates: &[r::PropertyMutation],
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        fn expression_of(update: &r::PropertyMutation) -> Option<&r::Expression> {
            match update {
                r::PropertyMutation::Set { value, .. } => Some(value),
                r::PropertyMutation::Replace { properties, .. }
                | r::PropertyMutation::Extend { properties, .. } => Some(properties),
                r::PropertyMutation::Remove { .. } => None,
            }
        }
        let mut edits = super::super::mutation::EntityEdits::new(self.row_budget())?;
        for batch in rows.chunks(limits.batch_rows) {
            // One read hydrates the batch. An edit applied midway leaves it
            // stale, so every later update in the batch hydrates on its own.
            let mut batch_graph = Some(
                self.expression_graph_batch(batch, updates.iter().filter_map(expression_of))
                    .await?,
            );
            for row in batch {
                for update in updates {
                    self.check_execution_deadline()?;
                    let expression = expression_of(update);
                    let mut reads_pending = false;
                    expression.into_iter().try_for_each(|expression| {
                        expression.try_graph_requirements(|slot, requirement| {
                            graph::visit_entities(&row[slot.0 as usize], &mut |entity| {
                                reads_pending |= edits.changes(entity, requirement);
                                Ok(())
                            })
                        })
                    })?;
                    if reads_pending {
                        batch_graph = None;
                        self.row_apply_edits(&mut edits).await?;
                    }
                    let mut update_graph = None;
                    let graph = match &batch_graph {
                        Some(graph) => graph,
                        None => &*update_graph.insert(
                            self.expression_graph_batch(std::slice::from_ref(row), expression)
                                .await?,
                        ),
                    };
                    let evaluation = self.evaluate(row, parameters, graph, limits);
                    let (target, value) = match update {
                        r::PropertyMutation::Set { entity, key, value } => (
                            row[entity.0 as usize].clone(),
                            r::Value::Map(BTreeMap::from([(key.clone(), evaluation.eval(value)?)])),
                        ),
                        r::PropertyMutation::Remove { entity, key } => (
                            row[entity.0 as usize].clone(),
                            r::Value::Map(BTreeMap::from([(key.clone(), r::Value::Null)])),
                        ),
                        r::PropertyMutation::Replace { entity, properties }
                        | r::PropertyMutation::Extend { entity, properties } => {
                            (row[entity.0 as usize].clone(), evaluation.eval(properties)?)
                        }
                    };
                    // The evaluated change map owns its values. Release
                    // expression hydration before taking canonical snapshots.
                    drop(update_graph);
                    let changes = match value {
                        r::Value::Map(map) => map,
                        r::Value::Entity(source) => {
                            if edits.changes(source, r::PropertyRequirement::All) {
                                batch_graph = None;
                                self.row_apply_edits(&mut edits).await?;
                            }
                            // A bare entity slot needs no properties during
                            // scalar evaluation. Hydrate only the selected map
                            // source, without evaluating the expression again.
                            let _source_memory = self
                                .row_budget()
                                .reserve(size_of::<r::Row>() + size_of::<r::Value>())?;
                            let source_row = [vec![r::Value::Entity(source)]];
                            let mut demand =
                                super::requirements::Requirements::new(self.row_budget())?;
                            demand.insert(r::Slot(0), r::PropertyRequirement::All)?;
                            let source_graph = self
                                .graph_batch_required(&source_row, demand.values())
                                .await?;
                            self.evaluate(row, parameters, &source_graph, limits)
                                .properties(source)?
                        }
                        r::Value::Null => BTreeMap::new(),
                        r::Value::Boolean(_)
                        | r::Value::Integer(_)
                        | r::Value::Float(_)
                        | r::Value::String(_)
                        | r::Value::List(_)
                        | r::Value::Path(_) => {
                            return Err(r::QueryError::runtime(
                                category::TYPE_ERROR,
                                detail::EXPECTED_MAP,
                                "property update requires a map",
                            )
                            .into())
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
                                category::TYPE_ERROR,
                                detail::EXPECTED_ENTITY,
                                "SET and REMOVE require a graph entity",
                            )
                            .into())
                        }
                    };
                    // Expression evaluation checks construction against
                    // available memory; retain that ownership across the
                    // asynchronous edits and allow simultaneous typed-list
                    // conversion buffers.
                    let changes_bytes = changes.iter().fold(0_usize, |bytes, (key, value)| {
                        bytes
                            .saturating_add(1024)
                            .saturating_add(key.len().saturating_mul(2))
                            .saturating_add(value.allocated_bytes().saturating_mul(2))
                    });
                    // Pending edits hold at most what stays free, so a large
                    // statement cannot refuse memory an apply would release.
                    let free = self.row_budget().available().saturating_sub(changes_bytes);
                    if edits.pending_bytes() > 0 && edits.pending_bytes() >= free {
                        batch_graph = None;
                        self.row_apply_edits(&mut edits).await?;
                    }
                    let changes_memory = self.row_budget().reserve(changes_bytes)?;
                    let kind = match update {
                        r::PropertyMutation::Set { .. } | r::PropertyMutation::Remove { .. }
                            if changes.values().all(|value| *value == r::Value::Null) =>
                        {
                            EditKind::Removal
                        }
                        r::PropertyMutation::Set { .. }
                        | r::PropertyMutation::Remove { .. }
                        | r::PropertyMutation::Extend { .. } => EditKind::Extension,
                        r::PropertyMutation::Replace { .. } => EditKind::Replacement,
                    };
                    if changes.is_empty() && kind != EditKind::Replacement {
                        continue;
                    }
                    if edits.blocks(entity, kind, changes.keys().map(String::as_str)) {
                        batch_graph = None;
                        self.row_apply_edits(&mut edits).await?;
                    }
                    let edit = edits.edit(entity, kind, changes_memory)?;
                    for (key, value) in changes {
                        let value = if value == r::Value::Null {
                            None
                        } else {
                            Some(graph::to_property(value)?)
                        };
                        // Validate after this value's conversion, before the
                        // next item, preserving the frontend's error order.
                        edit.insert(key, value).map_err(|error| {
                            r::QueryError::runtime(
                                category::UNSUPPORTED_FEATURE,
                                match error {
                                    map::NameError::Empty => detail::EMPTY_PROPERTY_NAME,
                                    map::NameError::Reserved => detail::RESERVED_PROPERTY_NAME,
                                },
                                error.to_string(),
                            )
                        })?;
                    }
                }
            }
            drop(batch_graph);
            self.row_apply_edits(&mut edits).await?;
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
                                category::TYPE_ERROR,
                                detail::INVALID_ARGUMENT_TYPE,
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
            matches!(ctx.create_properties(&fields, ctx.evaluate(&[], &params, &graph, Limits::default())), Err(crate::cypher::Error::Query(error)) if error.detail == "memory_limit")
        );
        assert_eq!(ctx.row_budget().available(), 32 * 1024);
        for (key, value, detail) in [
            ("$label", r::Value::Null, "reserved_property_name"),
            ("", r::Value::Integer(1), "empty_property_name"),
            (
                "mixed",
                r::Value::List(vec![r::Value::Integer(1), r::Value::Boolean(true)]),
                "invalid_property_type",
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
