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
                    let added_keys = match properties {
                        r::PropertyDemand::All => 0,
                        r::PropertyDemand::Keys(keys) => keys
                            .iter()
                            .filter(|key| previous.is_none_or(|existing| !existing.contains(key)))
                            .fold(0_usize, |bytes, key| {
                                bytes.saturating_add(key.len()).saturating_add(128)
                            }),
                    };
                    entity_bytes = entity_bytes
                        .saturating_add(added_keys)
                        .saturating_add(if previous.is_none() { 128 } else { 0 });
                    entity_memory.resize(entity_bytes)?;
                    entities.entry(entity).or_default().merge(properties);
                }
            }
        }
        // Key vectors, endpoint IDs and endpoint maps live alongside the demand
        // map. Admit their bounded per-entity storage before allocating them.
        let _lookup_memory = self
            .row_budget()
            .reserve(entities.len().saturating_mul(256))?;
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
        let mut graph_memory = self.row_budget().reserve(0)?;
        let mut graph_bytes = 0_usize;
        for ((entity, demand), bytes) in entities.into_iter().zip(values) {
            let Some(bytes) = bytes else {
                let r::Entity::Relationship(id) = entity else {
                    continue;
                };
                let Some((label, _)) = self.row_relationship_types.get(&id) else {
                    continue;
                };
                graph_bytes = graph_bytes.saturating_add(label.len()).saturating_add(128);
                graph_memory.resize(graph_bytes)?;
                graph.deleted_relationship_types.insert(id, label.clone());
                continue;
            };
            // Charge a conservative decode workspace before turning compact
            // values into owned strings, maps and per-element enum values.
            let _decode_memory = self.row_budget().reserve(bytes.len().saturating_mul(32))?;
            let properties =
                property::decode_properties(&bytes).map_err(crate::HelixDbError::from)?;
            let mut label = None;
            let mut values = BTreeMap::new();
            for property in properties {
                if property.name == "$label" {
                    label = property
                        .value
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned);
                    continue;
                }
                if property.name.starts_with('$')
                    || property.value == P::Null
                    || !demand.contains(&property.name)
                {
                    continue;
                }
                let value = match from_property(property.value) {
                    Ok(value) => Ok(value),
                    Err(Error::Query(error)) if error.category == "UnsupportedFeature" => {
                        Err(error)
                    }
                    Err(error) => return Err(error),
                };
                // Unsupported stored values remain dormant until accessed. A
                // CASE branch or short-circuit boolean must not fail on a value
                // that its expression never evaluates.
                values.insert(property.name, value);
            }
            let kind = match entity {
                r::Entity::Node(_) => EntityKind::Node { label },
                r::Entity::Relationship(id) => {
                    let Some(endpoints) = endpoints[&id] else {
                        continue;
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
            graph_bytes = graph_bytes.saturating_add(data.allocated_bytes());
            graph_memory.resize(graph_bytes)?;
            graph.entities.insert(entity, data);
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
                memory.resize(out.len().saturating_add(1).saturating_mul(128))?;
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

pub(super) fn from_property(value: P) -> Result<r::Value> {
    Ok(match value {
        P::Null => r::Value::Null,
        P::Bool(b) => r::Value::Boolean(b),
        P::I64(i) => r::Value::Integer(i),
        P::F64(f) | P::F32(f) => r::Value::Float(f),
        P::String(s) => r::Value::String(s),
        P::Array(xs) => r::Value::List(xs.into_iter().map(from_property).collect::<Result<_>>()?),
        P::Object(xs) => r::Value::Map(
            xs.into_iter()
                .map(|(k, v)| Ok((k, from_property(v)?)))
                .collect::<Result<_>>()?,
        ),
        P::I64Array(xs) => r::Value::List(xs.into_iter().map(r::Value::Integer).collect()),
        P::F64Array(xs) => r::Value::List(xs.into_iter().map(r::Value::Float).collect()),
        P::F32Array(xs) => r::Value::List(
            xs.into_iter()
                .map(|f| r::Value::Float(f64::from(f)))
                .collect(),
        ),
        P::StringArray(xs) => r::Value::List(xs.into_iter().map(r::Value::String).collect()),
        P::DateTime(_) | P::Bytes(_) => {
            return Err(r::QueryError::runtime(
                "UnsupportedFeature",
                "StoredValueType",
                "temporal and binary stored values are outside the MVP profile",
            )
            .into())
        }
    })
}

pub(super) fn to_property(value: r::Value) -> Result<P> {
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
                    P::Array(xs.into_iter().map(to_property).collect::<Result<_>>()?)
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
            label.saturating_add(size_of::<Self>()).saturating_add(64),
            |bytes, (key, value)| {
                bytes
                    .saturating_add(key.capacity())
                    .saturating_add(match value {
                        Ok(value) => value.allocated_bytes(),
                        Err(error) => error
                            .category
                            .capacity()
                            .saturating_add(error.detail.capacity())
                            .saturating_add(error.message.capacity())
                            .saturating_add(size_of::<r::QueryError>()),
                    })
                    .saturating_add(64)
            },
        )
    }
}
