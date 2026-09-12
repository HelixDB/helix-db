use super::ExecutionContext;
use crate::cypher::{Error, Result};
use crate::encoding::v2::{
    keys,
    values::{
        edge_endpoints::EdgeEndpointsValue,
        property::{self, property_value::PropertyValue as P, Property},
    },
};
use helix_planner::relational as r;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct GraphBatch {
    pub entities: BTreeMap<r::Entity, EntityData>,
    deleted_relationship_types: BTreeMap<u64, String>,
    _memory: Option<super::memory::Reservation>,
}

pub(super) struct EntityData {
    pub kind: EntityKind,
    pub properties: BTreeMap<String, r::Result<r::Value>>,
}

pub(super) enum EntityKind {
    Node {
        label: Option<String>,
    },
    Relationship {
        label: String,
        endpoints: (u64, u64),
    },
}

impl r::GraphValues for GraphBatch {
    fn properties(&self, entity: r::Entity) -> r::Result<&r::GraphProperties> {
        let data = self.entities.get(&entity).ok_or_else(|| missing(entity))?;
        Ok(&data.properties)
    }
    fn label(&self, entity: r::Entity) -> r::Result<Option<&str>> {
        self.entities
            .get(&entity)
            .map(|e| match &e.kind {
                EntityKind::Node { label } => label.as_deref(),
                EntityKind::Relationship { label, .. } => Some(label.as_str()),
            })
            .or_else(|| match entity {
                r::Entity::Relationship(id) => self
                    .deleted_relationship_types
                    .get(&id)
                    .map(|label| Some(label.as_str())),
                r::Entity::Node(_) => None,
            })
            .ok_or_else(|| missing(entity))
    }
}

fn missing(entity: r::Entity) -> r::QueryError {
    r::QueryError::runtime(
        "EntityNotFound",
        "DeletedEntityAccess",
        format!("graph entity {entity:?} is unavailable"),
    )
}

impl ExecutionContext<'_> {
    pub(super) async fn graph_batch(&self, rows: &[r::Row]) -> Result<GraphBatch> {
        let demand = rows
            .first()
            .into_iter()
            .flat_map(|row| (0..row.len()).map(|i| (r::Slot(i as u32), r::PropertyDemand::All)))
            .collect();
        self.graph_batch_required(rows, &demand).await
    }

    pub(super) async fn expression_graph_batch<'a>(
        &self,
        rows: &[r::Row],
        expressions: impl IntoIterator<Item = &'a r::Expression>,
    ) -> Result<GraphBatch> {
        let mut demand = BTreeMap::new();
        for expression in expressions {
            expression.graph_requirements(&mut demand);
        }
        self.graph_batch_required(rows, &demand).await
    }

    pub(super) async fn edge_endpoints_batch(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Option<(u64, u64)>>> {
        let keys = ids
            .iter()
            .map(|id| {
                self.storage_key(keys::DataKeyKind::EdgeEndpoints(
                    keys::EdgeEndpointsKey::new(*id),
                ))
            })
            .collect::<Vec<_>>();
        self.multi_get_raw(&keys)
            .await?
            .into_iter()
            .map(|bytes| {
                bytes
                    .map(|bytes| {
                        EdgeEndpointsValue::decode(&bytes)
                            .map(|v| (v.source(), v.target()))
                            .map_err(crate::HelixDbError::from)
                    })
                    .transpose()
                    .map_err(Error::from)
            })
            .collect()
    }

    /// Resolve candidate relationship types within the local expansion batch.
    /// The caller admits the bounded ID/key/result vectors. Raw values carry
    /// their own guards; decoding workspace is reserved before allocation.
    pub(super) async fn relationship_types_batch(
        &self,
        ids: &[u64],
        types: &[String],
    ) -> Result<Vec<bool>> {
        if types.is_empty() {
            return Ok(vec![true; ids.len()]);
        }
        let keys = ids
            .iter()
            .map(|id| {
                self.storage_key(keys::DataKeyKind::EdgePropertyById(
                    keys::EdgePropertyByIdKey::new(*id),
                ))
            })
            .collect::<Vec<_>>();
        self.multi_get_raw(&keys)
            .await?
            .into_iter()
            .map(|bytes| {
                let Some(bytes) = bytes else {
                    return Ok(false);
                };
                let properties = crate::query_resources::properties::Decoded::new(
                    &bytes,
                    property::prepared::Selection::Names(&["$label"]),
                    Some(self.row_budget()),
                )?;
                Ok(properties.iter().any(|property| {
                    property.name == "$label"
                        && property
                            .value
                            .as_str()
                            .is_some_and(|label| types.iter().any(|wanted| wanted == label))
                }))
            })
            .collect()
    }

    pub(super) async fn graph_batch_required(
        &self,
        rows: &[r::Row],
        demand: &BTreeMap<r::Slot, r::PropertyDemand>,
    ) -> Result<GraphBatch> {
        let mut entities = BTreeMap::<r::Entity, r::PropertyDemand>::new();
        let mut entity_memory = self.row_budget().reserve(0)?;
        let mut entity_bytes = 0_usize;
        for row in rows {
            for (slot, properties) in demand {
                let mut found = BTreeSet::new();
                let mut found_memory = self.row_budget().reserve(0)?;
                collect_entities(&row[slot.0 as usize], &mut found, &mut found_memory)?;
                for entity in found {
                    let previous = entities.get(&entity);
                    let (old_keys, new_keys, name_bytes) = match (previous, properties) {
                        (Some(r::PropertyDemand::All), _) | (_, r::PropertyDemand::All) => {
                            (0, 0, 0)
                        }
                        (previous, r::PropertyDemand::Keys(keys)) => {
                            let old = match previous {
                                Some(r::PropertyDemand::Keys(keys)) => keys.len(),
                                Some(r::PropertyDemand::All) => {
                                    unreachable!("handled all-properties demand")
                                }
                                None => 0,
                            };
                            let (added, bytes) = keys
                                .iter()
                                .filter(|key| {
                                    previous.is_none_or(|existing| !existing.contains(key))
                                })
                                .fold((0_usize, 0_usize), |(count, bytes), key| {
                                    (count.saturating_add(1), bytes.saturating_add(key.len()))
                                });
                            (old, old.saturating_add(added), bytes)
                        }
                    };
                    let old_tree = if old_keys == 0 {
                        0
                    } else {
                        r::allocation::btree_bytes::<String, ()>(old_keys)
                    };
                    let new_tree = if new_keys == 0 {
                        0
                    } else {
                        r::allocation::btree_bytes::<String, ()>(new_keys)
                    };
                    entity_bytes = entity_bytes
                        .saturating_add(name_bytes)
                        .saturating_add(new_tree.saturating_sub(old_tree));
                    entity_memory.resize(
                        entity_bytes.saturating_add(r::allocation::btree_bytes::<
                            r::Entity,
                            r::PropertyDemand,
                        >(
                            entities
                                .len()
                                .saturating_add(usize::from(previous.is_none())),
                        )),
                    )?;
                    entities.entry(entity).or_default().merge(properties);
                }
            }
        }
        if entities.is_empty() {
            return Ok(GraphBatch::default());
        }
        let edge_count = entities
            .keys()
            .filter(|entity| matches!(entity, r::Entity::Relationship(_)))
            .count();
        // Key vectors, endpoint IDs and endpoint maps live alongside the demand
        // map. Admit their bounded per-entity storage before allocating them.
        let _lookup_memory =
            self.row_budget()
                .reserve(
                    entities
                        .len()
                        .saturating_mul(256)
                        .saturating_add(if edge_count == 0 {
                            0
                        } else {
                            r::allocation::btree_bytes::<u64, Option<(u64, u64)>>(edge_count)
                        }),
                )?;
        let keys = entities
            .keys()
            .map(|entity| {
                self.storage_key(match entity {
                    r::Entity::Node(id) => {
                        keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id))
                    }
                    r::Entity::Relationship(id) => {
                        keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(*id))
                    }
                })
            })
            .collect::<Vec<_>>();
        let values = self.multi_get_raw(&keys).await?;
        // Raw value reservations travel with the storage-returned Bytes owners.
        // Lookup key vectors were admitted above before construction.
        let edge_ids = entities
            .keys()
            .filter_map(|entity| match entity {
                r::Entity::Relationship(id) => Some(*id),
                r::Entity::Node(_) => None,
            })
            .collect::<Vec<_>>();
        let endpoints = edge_ids
            .iter()
            .copied()
            .zip(self.edge_endpoints_batch(&edge_ids).await?)
            .collect::<BTreeMap<_, _>>();
        let mut graph = GraphBatch::default();
        // These maps only receive entries during hydration. Reserve their
        // maximum node storage before the first insertion, including sparse roots.
        let mut graph_bytes = r::allocation::btree_bytes::<r::Entity, EntityData>(entities.len())
            .saturating_add(if edge_count == 0 {
                0
            } else {
                r::allocation::btree_bytes::<u64, String>(edge_count)
            });
        let mut graph_memory = self.row_budget().reserve(graph_bytes)?;
        for ((entity, demand), bytes) in entities.into_iter().zip(values) {
            let Some(bytes) = bytes else {
                let r::Entity::Relationship(id) = entity else {
                    continue;
                };
                let Some((label, _)) = self.row_relationship_types.get(&id) else {
                    continue;
                };
                graph_bytes = graph_bytes.saturating_add(label.len());
                graph_memory.resize(graph_bytes)?;
                graph.deleted_relationship_types.insert(id, label.clone());
                continue;
            };
            let selection = match &demand {
                r::PropertyDemand::All => property::prepared::Selection::All,
                r::PropertyDemand::Keys(names) => property::prepared::Selection::Keys {
                    names,
                    required: "$label",
                },
            };
            let properties = crate::query_resources::properties::Decoded::new(
                &bytes,
                selection,
                Some(self.row_budget()),
            )?;
            properties.with_owned(|properties| -> Result<()> {
                let mut label = None;
                let mut values = BTreeMap::new();
                let mut property_memory = self.row_budget().reserve(0)?;
                let mut payload_bytes = 0_usize;
                for property in properties {
                    if property.name == "$label" {
                        let P::String(value) = property.value else {
                            label = None;
                            continue;
                        };
                        if value.is_empty() {
                            label = None;
                            continue;
                        }
                        payload_bytes = payload_bytes.saturating_add(value.capacity());
                        property_memory.resize(payload_bytes.saturating_add(
                            if values.is_empty() {
                                0
                            } else {
                                r::allocation::btree_bytes::<String, r::Result<r::Value>>(
                                    values.len(),
                                )
                            },
                        ))?;
                        label = Some(value);
                        continue;
                    }
                    if property.name.starts_with('$')
                        || property.value == P::Null
                        || !demand.contains(&property.name)
                    {
                        continue;
                    }
                    let conversion = super::property_conversion::Conversion::new(property.value);
                    payload_bytes = payload_bytes
                        .saturating_add(property.name.capacity())
                        .saturating_add(conversion.owned_bytes());
                    property_memory.resize(payload_bytes.saturating_add(
                        r::allocation::btree_bytes::<String, r::Result<r::Value>>(
                            values.len().saturating_add(1),
                        ),
                    ))?;
                    // The dormant error is admitted like a value. No CASE branch
                    // or short-circuit expression observes it until access.
                    values.insert(property.name, conversion.finish());
                }
                let kind = match entity {
                    r::Entity::Node(_) => EntityKind::Node { label },
                    r::Entity::Relationship(id) => {
                        let Some(endpoints) = endpoints[&id] else {
                            return Ok(());
                        };
                        let label = label.ok_or_else(|| {
                            r::QueryError::runtime(
                                "UnsupportedFeature",
                                "UntypedStoredRelationship",
                                "stored relationship has no type",
                            )
                        })?;
                        EntityKind::Relationship { label, endpoints }
                    }
                };
                let data = EntityData {
                    kind,
                    properties: values,
                };
                let retained = data
                    .allocated_bytes()
                    .saturating_sub(size_of::<EntityData>());
                property_memory.resize(retained)?;
                graph_bytes = graph_bytes.saturating_add(retained);
                graph_memory.absorb(property_memory);
                graph.entities.insert(entity, data);
                Ok(())
            })?;
        }
        graph._memory = Some(graph_memory);
        Ok(graph)
    }
}

fn collect_entities(
    value: &r::Value,
    out: &mut BTreeSet<r::Entity>,
    memory: &mut super::memory::Reservation,
) -> Result<()> {
    match value {
        r::Value::Entity(entity) => {
            if !out.contains(entity) {
                memory.resize(r::allocation::btree_bytes::<r::Entity, ()>(
                    out.len().saturating_add(1),
                ))?;
                out.insert(*entity);
            }
        }
        r::Value::Path(path) => {
            for entity in path.nodes().iter().map(|id| r::Entity::Node(*id)).chain(
                path.relationships()
                    .iter()
                    .map(|id| r::Entity::Relationship(*id)),
            ) {
                collect_entities(&r::Value::Entity(entity), out, memory)?;
            }
        }
        r::Value::List(xs) => xs
            .iter()
            .try_for_each(|x| collect_entities(x, out, memory))?,
        r::Value::Map(xs) => xs
            .values()
            .try_for_each(|x| collect_entities(x, out, memory))?,
        r::Value::Null
        | r::Value::Boolean(_)
        | r::Value::Integer(_)
        | r::Value::Float(_)
        | r::Value::String(_) => {}
    }
    Ok(())
}

impl GraphBatch {
    /// Conservative output admission without constructing JSON. In particular,
    /// a compact path can expand the same large property map many times on the
    /// wire; charging the path IDs alone would permit unbounded response growth.
    pub fn wire_memory(&self, value: &r::Value) -> Result<usize> {
        let bytes = match value {
            r::Value::Null | r::Value::Boolean(_) => size_of::<serde_json::Value>(),
            r::Value::Integer(i) if i.unsigned_abs() <= 9_007_199_254_740_991 => {
                size_of::<serde_json::Value>()
            }
            r::Value::Float(f) if f.is_finite() => size_of::<serde_json::Value>(),
            r::Value::Integer(_) | r::Value::Float(_) => 2048,
            r::Value::String(s) => size_of::<serde_json::Value>().saturating_add(s.len()),
            r::Value::List(values) => {
                // Fallible collect may grow geometrically, including the minimum
                // Vec allocation for a one-element list.
                let capacity = values
                    .len()
                    .max(2)
                    .saturating_mul(2 * size_of::<serde_json::Value>());
                values.iter().try_fold(
                    size_of::<serde_json::Value>().saturating_add(capacity),
                    |bytes, value| Ok::<_, Error>(bytes.saturating_add(self.wire_memory(value)?)),
                )?
            }
            r::Value::Map(values) => {
                values.iter().try_fold(2048_usize, |bytes, (key, value)| {
                    Ok::<_, Error>(
                        bytes
                            .saturating_add(128)
                            .saturating_add(key.len())
                            .saturating_add(self.wire_memory(value)?),
                    )
                })?
            }
            r::Value::Entity(entity) => {
                let data = self.entities.get(entity).ok_or_else(|| missing(*entity))?;
                let label_bytes = match &data.kind {
                    EntityKind::Node { label } => label.as_ref().map_or(0, String::len),
                    EntityKind::Relationship { label, .. } => label.len(),
                };
                data.properties.iter().try_fold(
                    4096_usize.saturating_add(label_bytes),
                    |bytes, (key, value)| {
                        let value = value.as_ref().map_err(Clone::clone)?;
                        Ok::<_, Error>(
                            bytes
                                .saturating_add(128)
                                .saturating_add(key.len())
                                .saturating_add(self.wire_memory(value)?),
                        )
                    },
                )?
            }
            r::Value::Path(path) => path
                .nodes()
                .iter()
                .map(|id| r::Entity::Node(*id))
                .chain(
                    path.relationships()
                        .iter()
                        .map(|id| r::Entity::Relationship(*id)),
                )
                .try_fold(2048_usize, |bytes, entity| {
                    Ok::<_, Error>(
                        bytes.saturating_add(self.wire_memory(&r::Value::Entity(entity))?),
                    )
                })?,
        };
        Ok(bytes)
    }

    pub fn wire(&self, value: &r::Value) -> Result<serde_json::Value> {
        use serde_json::{json, Value as J};
        Ok(match value {
            r::Value::Null => J::Null,
            r::Value::Boolean(b) => json!(b),
            r::Value::Integer(i) => {
                if i.unsigned_abs() <= 9_007_199_254_740_991 {
                    json!(i)
                } else {
                    json!({"$type":"integer","value":i.to_string()})
                }
            }
            r::Value::Float(f) => {
                if f.is_finite() {
                    json!(f)
                } else {
                    json!({"$type":"float","value":if f.is_nan(){"NaN"}else if *f>0.0{"Infinity"}else{"-Infinity"}})
                }
            }
            r::Value::String(s) => json!(s),
            r::Value::List(xs) => J::Array(xs.iter().map(|v| self.wire(v)).collect::<Result<_>>()?),
            r::Value::Map(xs) => {
                let map = xs
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), self.wire(v)?)))
                    .collect::<Result<serde_json::Map<_, _>>>()?;
                if map.contains_key("$type") {
                    json!({"$type":"map","value":map})
                } else {
                    J::Object(map)
                }
            }
            r::Value::Entity(entity) => {
                let data = self.entities.get(entity).ok_or_else(|| missing(*entity))?;
                let properties = data
                    .properties
                    .iter()
                    .map(|(key, value)| {
                        let value = value.as_ref().map_err(Clone::clone)?;
                        Ok((key.clone(), self.wire(value)?))
                    })
                    .collect::<Result<serde_json::Map<_, _>>>()?;
                match (entity, &data.kind) {
                    (r::Entity::Node(id), EntityKind::Node { label }) => {
                        json!({"$type":"node","id":id.to_string(),"labels":label.iter().collect::<Vec<_>>(),"properties":properties})
                    }
                    (
                        r::Entity::Relationship(id),
                        EntityKind::Relationship {
                            label,
                            endpoints: (from, to),
                        },
                    ) => {
                        json!({"$type":"relationship","id":id.to_string(),"type":label,"start":from.to_string(),"end":to.to_string(),"properties":properties})
                    }
                    _ => {
                        return Err(r::QueryError::runtime(
                            "InternalPlannerError",
                            "EntityKindMismatch",
                            "hydrated entity kind does not match its identity",
                        )
                        .into())
                    }
                }
            }
            r::Value::Path(path) => {
                json!({"$type":"path","nodes":path.nodes().iter().map(|id|self.wire(&r::Value::Entity(r::Entity::Node(*id)))).collect::<Result<Vec<_>>>()?,"relationships":path.relationships().iter().map(|id|self.wire(&r::Value::Entity(r::Entity::Relationship(*id)))).collect::<Result<Vec<_>>>()?})
            }
        })
    }
}

pub(super) fn to_property(value: r::Value) -> Result<P> {
    const {
        assert!(size_of::<P>() <= size_of::<r::Value>());
    }
    Ok(match value {
        r::Value::Null => P::Null,
        r::Value::Boolean(b) => P::Bool(b),
        r::Value::Integer(i) => P::I64(i),
        r::Value::Float(f) => P::F64(f),
        r::Value::String(s) => P::String(s),
        r::Value::List(xs) => {
            if xs.iter().any(|v| {
                matches!(
                    v,
                    r::Value::Null
                        | r::Value::List(_)
                        | r::Value::Map(_)
                        | r::Value::Entity(_)
                        | r::Value::Path(_)
                )
            }) {
                return Err(property_type());
            }
            if xs
                .windows(2)
                .any(|p| std::mem::discriminant(&p[0]) != std::mem::discriminant(&p[1]))
            {
                return Err(property_type());
            }
            // Use the existing typed storage forms so equality-index maintenance
            // accepts homogeneous numeric/string lists. Generic arrays remain
            // readable, and boolean lists retain their existing storage form.
            match xs.first() {
                None => P::I64Array(Vec::new()),
                Some(r::Value::Integer(_)) => P::I64Array(
                    xs.into_iter()
                        .map(|value| {
                            let r::Value::Integer(value) = value else {
                                unreachable!("validated homogeneous list")
                            };
                            value
                        })
                        .collect(),
                ),
                Some(r::Value::Float(_)) => P::F64Array(
                    xs.into_iter()
                        .map(|value| {
                            let r::Value::Float(value) = value else {
                                unreachable!("validated homogeneous list")
                            };
                            value
                        })
                        .collect(),
                ),
                Some(r::Value::String(_)) => P::StringArray(
                    xs.into_iter()
                        .map(|value| {
                            let r::Value::String(value) = value else {
                                unreachable!("validated homogeneous list")
                            };
                            value
                        })
                        .collect(),
                ),
                Some(r::Value::Boolean(_)) => {
                    // Exact capacity makes simultaneous source/destination
                    // admission independent of fallible-collect growth or
                    // compiler-specific in-place collection optimizations.
                    let mut values = Vec::with_capacity(xs.len());
                    for value in xs {
                        let r::Value::Boolean(value) = value else {
                            unreachable!("validated homogeneous list");
                        };
                        values.push(P::Bool(value));
                    }
                    P::Array(values)
                }
                Some(
                    r::Value::Null
                    | r::Value::List(_)
                    | r::Value::Map(_)
                    | r::Value::Entity(_)
                    | r::Value::Path(_),
                ) => unreachable!("validated scalar list"),
            }
        }
        r::Value::Map(_) | r::Value::Entity(_) | r::Value::Path(_) => return Err(property_type()),
    })
}
fn property_type() -> Error {
    r::QueryError::runtime(
        "TypeError",
        "InvalidPropertyType",
        "stored properties must be scalars or homogeneous scalar lists",
    )
    .into()
}

pub(super) fn properties(values: BTreeMap<String, r::Value>) -> Result<Vec<Property>> {
    values
        .into_iter()
        .filter_map(|(name, value)| {
            if name.starts_with('$') || name.is_empty() {
                return Some(Err(r::QueryError::runtime(
                    "UnsupportedFeature",
                    if name.is_empty() {
                        "EmptyPropertyName"
                    } else {
                        "ReservedPropertyName"
                    },
                    "property names must be nonempty and outside the reserved metadata namespace",
                )
                .into()));
            }
            (value != r::Value::Null)
                .then(|| to_property(value).map(|value| Property::new(name, value)))
        })
        .collect()
}

impl EntityData {
    fn allocated_bytes(&self) -> usize {
        let label = match &self.kind {
            EntityKind::Node { label } => label.as_ref().map_or(0, String::capacity),
            EntityKind::Relationship { label, .. } => label.capacity(),
        };
        self.properties.iter().fold(
            label.saturating_add(size_of::<Self>()).saturating_add(
                // Property maps are built fresh with inserts only; an empty one
                // has never allocated a root.
                if self.properties.is_empty() {
                    0
                } else {
                    r::allocation::btree_bytes::<String, r::Result<r::Value>>(self.properties.len())
                },
            ),
            |bytes, (key, value)| {
                bytes
                    .saturating_add(key.capacity())
                    .saturating_add(match value {
                        Ok(value) => value
                            .allocated_bytes()
                            .saturating_sub(size_of::<r::Value>()),
                        Err(error) => error
                            .category
                            .capacity()
                            .saturating_add(error.detail.capacity())
                            .saturating_add(error.message.capacity()),
                    })
            },
        )
    }
}
