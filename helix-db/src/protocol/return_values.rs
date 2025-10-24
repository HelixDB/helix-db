use super::{
    remapping::{Remapping, ResponseRemapping},
    value::Value,
};
use crate::{
    debug_println,
    helix_engine::{
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::aggregate::Aggregate,
};
use crate::{
    helix_engine::vector_core::vector::HVector,
    utils::{
        count::Count,
        filterable::{Filterable, FilterableType},
        group_by::GroupBy,
        id::ID,
        items::{Edge, Node},
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::{cell::RefMut, collections::HashMap};

/// A return value enum that represents different possible outputs from graph operations.
/// Can contain traversal results, counts, boolean flags, or empty values.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub enum ReturnValue {
    Array(Vec<ReturnValue>),
    Object(HashMap<String, ReturnValue>),
    Value(Value),
    Empty,
}

impl ReturnValue {
    #[inline]
    #[allow(unused_attributes)]
    #[ignore = "No use for this function yet, however, I believe it may be useful in the future so I'm keeping it here"]
    pub fn from_properties(properties: HashMap<String, Value>) -> Self {
        ReturnValue::Object(
            properties
                .into_iter()
                .map(|(k, v)| (k, ReturnValue::Value(v)))
                .collect(),
        )
    }

    #[inline(always)]
    fn process_nodes_with_mixin(
        item: Node,
        mixin: &mut HashMap<u128, ResponseRemapping>,
    ) -> ReturnValue {
        if let Some(m) = mixin.remove(item.id()) {
            if m.should_spread {
                ReturnValue::from(item).mixin_remapping(m.remappings)
            } else {
                ReturnValue::from_traversal_value(item, &m).mixin_remapping(m.remappings)
            }
        } else {
            ReturnValue::from(item)
        }
    }

    #[inline(always)]
    fn process_edges_with_mixin(
        item: Edge,
        mixin: &mut HashMap<u128, ResponseRemapping>,
    ) -> ReturnValue {
        if let Some(m) = mixin.remove(item.id()) {
            if m.should_spread {
                ReturnValue::from(item).mixin_remapping(m.remappings)
            } else {
                ReturnValue::from_traversal_value(item, &m).mixin_remapping(m.remappings)
            }
        } else {
            ReturnValue::from(item)
        }
    }

    #[inline(always)]
    fn process_vectors_with_mixin(
        item: HVector,
        mixin: &mut HashMap<u128, ResponseRemapping>,
    ) -> ReturnValue {
        if let Some(m) = mixin.remove(item.id()) {
            if m.should_spread {
                ReturnValue::from(item).mixin_remapping(m.remappings)
            } else {
                ReturnValue::from_traversal_value(item, &m).mixin_remapping(m.remappings)
            }
        } else {
            ReturnValue::from(item)
        }
    }

    #[inline]
    pub fn from_traversal_value_array_with_mixin(
        traversal_value: Vec<TraversalValue>,
        mut mixin: RefMut<HashMap<u128, ResponseRemapping>>,
    ) -> Self {
        ReturnValue::Array(
            traversal_value
                .into_iter()
                .map(|val| match val {
                    TraversalValue::Node(node) => {
                        ReturnValue::process_nodes_with_mixin(node, &mut mixin)
                    }
                    TraversalValue::Edge(edge) => {
                        ReturnValue::process_edges_with_mixin(edge, &mut mixin)
                    }
                    TraversalValue::Vector(vector) => {
                        ReturnValue::process_vectors_with_mixin(vector, &mut mixin)
                    }
                    TraversalValue::Count(count) => ReturnValue::from(count),
                    TraversalValue::Empty => ReturnValue::Empty,
                    TraversalValue::Value(value) => ReturnValue::from(value),
                    TraversalValue::Path((nodes, edges)) => {
                        let mut properties = HashMap::with_capacity(2);
                        properties.insert(
                            "nodes".to_string(),
                            ReturnValue::Array(nodes.into_iter().map(ReturnValue::from).collect()),
                        );
                        properties.insert(
                            "edges".to_string(),
                            ReturnValue::Array(edges.into_iter().map(ReturnValue::from).collect()),
                        );
                        ReturnValue::Object(properties)
                    }
                })
                .collect(),
        )
    }

    #[inline]
    pub fn from_traversal_value_iter_with_mixin<'a>(
        traversal_iter: RoTraversalIterator<
            'a,
            impl Iterator<Item = Result<TraversalValue, GraphError>>,
        >,
        mut mixin: RefMut<HashMap<u128, ResponseRemapping>>,
    ) -> Self {
        ReturnValue::Array(
            traversal_iter
                .filter_map(|val| val.ok())
                .map(|val| match val {
                    TraversalValue::Node(node) => {
                        ReturnValue::process_nodes_with_mixin(node, &mut mixin)
                    }
                    TraversalValue::Edge(edge) => {
                        ReturnValue::process_edges_with_mixin(edge, &mut mixin)
                    }
                    TraversalValue::Vector(vector) => {
                        ReturnValue::process_vectors_with_mixin(vector, &mut mixin)
                    }
                    TraversalValue::Count(count) => ReturnValue::from(count),
                    TraversalValue::Empty => ReturnValue::Empty,
                    TraversalValue::Value(value) => ReturnValue::from(value),
                    TraversalValue::Path((nodes, edges)) => {
                        let mut properties = HashMap::with_capacity(2);
                        properties.insert(
                            "nodes".to_string(),
                            ReturnValue::Array(nodes.into_iter().map(ReturnValue::from).collect()),
                        );
                        properties.insert(
                            "edges".to_string(),
                            ReturnValue::Array(edges.into_iter().map(ReturnValue::from).collect()),
                        );
                        ReturnValue::Object(properties)
                    }
                })
                .collect(),
        )
    }

    #[inline]
    pub fn from_traversal_value_with_mixin(
        traversal_value: TraversalValue,
        mut mixin: RefMut<HashMap<u128, ResponseRemapping>>,
    ) -> Self {
        match traversal_value {
            TraversalValue::Node(node) => {
                println!("node processing");
                ReturnValue::process_nodes_with_mixin(node, &mut mixin)
            }
            TraversalValue::Edge(edge) => ReturnValue::process_edges_with_mixin(edge, &mut mixin),
            TraversalValue::Vector(vector) => {
                ReturnValue::process_vectors_with_mixin(vector, &mut mixin)
            }
            TraversalValue::Count(count) => ReturnValue::from(count),
            TraversalValue::Empty => ReturnValue::Empty,
            TraversalValue::Value(value) => ReturnValue::from(value),
            TraversalValue::Path((nodes, edges)) => {
                let mut properties = HashMap::with_capacity(2);
                properties.insert(
                    "nodes".to_string(),
                    ReturnValue::Array(nodes.into_iter().map(ReturnValue::from).collect()),
                );
                properties.insert(
                    "edges".to_string(),
                    ReturnValue::Array(edges.into_iter().map(ReturnValue::from).collect()),
                );
                ReturnValue::Object(properties)
            }
        }
    }

    #[inline(always)]
    #[allow(unused_attributes)]
    #[ignore = "No use for this function yet, however, I believe it may be useful in the future so I'm keeping it here"]
    pub fn mixin(self, other: ReturnValue) -> Self {
        match (self, other) {
            (ReturnValue::Object(mut a), ReturnValue::Object(b)) => {
                a.extend(b);
                ReturnValue::Object(a)
            }
            _ => unreachable!(),
        }
    }

    /// Mixin a remapping to a return value.
    ///
    /// This function takes a hashmap of `Remappings` and mixes them into the return value
    ///
    /// - If the mapping is an exclude, then the key is removed from the return value
    /// - If the mapping is a remapping from an old value to a new value, then the key
    ///   is replaced with the new name and the value is the new value
    /// - If the mapping is a new mapping, then the key is added to the return value
    ///   and the value is the new value
    /// - Otherwise, the key is left unchanged and the value is the original value
    ///
    /// Basic usage:
    ///
    /// ```rust
    /// use helix_db::protocol::{ReturnValue, Remapping};
    /// use std::collections::HashMap;
    ///
    /// let remappings = HashMap::new();
    /// remappings.insert(
    ///     "old_key".to_string(),
    ///     Remapping::new(
    ///         Some("new_key".to_string()),
    ///         ReturnValue::from("new_value".to_string())
    ///     )
    /// );
    ///
    /// let return_value = ReturnValue::from("old_value".to_string());
    /// let return_value = return_value.mixin_remapping(remappings);
    ///
    /// assert_eq!(
    ///     return_value.get("new_key".to_string()),
    ///     Some(&ReturnValue::from("new_value".to_string()))
    /// );
    /// ```
    #[inline(always)]
    pub fn mixin_remapping(self, remappings: HashMap<String, Remapping>) -> Self {
        debug_println!("Remapping: {:#?}", self);
        let return_value = match self {
            ReturnValue::Object(mut a) => {
                remappings.into_iter().for_each(|(k, v)| {
                    debug_println!("k: {:?}, v: {:?}", k, v);
                    if v.exclude {
                        let _ = a.remove(&k);
                    } else if let Some(new_name) = v.new_name {
                        if let Some(value) = a.remove(&k) {
                            a.insert(new_name, value);
                        } else {
                            a.insert(k, v.return_value);
                        }
                    } else {
                        a.insert(k, v.return_value);
                    }
                });
                ReturnValue::Object(a)
            }
            _ => unreachable!(),
        };
        debug_println!("Return value: {:return_value?}");
        return_value
    }

    #[inline]
    pub fn from_traversal_value<T: Filterable + Clone>(
        item: T,
        remapping: &ResponseRemapping,
    ) -> Self {
        let length = match item.properties_ref() {
            Some(properties) => properties.len(),
            None => 0,
        };
        let mut properties = match item.type_name() {
            FilterableType::Node => HashMap::with_capacity(Node::NUM_PROPERTIES + length),
            FilterableType::Edge => {
                let mut properties = HashMap::with_capacity(Edge::NUM_PROPERTIES + length);
                properties.check_and_insert(
                    remapping,
                    "from_node".to_string(),
                    ReturnValue::from(item.from_node_uuid()),
                );
                properties.check_and_insert(
                    remapping,
                    "to_node".to_string(),
                    ReturnValue::from(item.to_node_uuid()),
                );
                properties
            }
            FilterableType::Vector => {
                let data = item.vector_data();
                let score = item.score();

                let mut properties = HashMap::with_capacity(2 + length);
                properties.check_and_insert(remapping, "data".to_string(), ReturnValue::from(data));
                properties.check_and_insert(
                    remapping,
                    "score".to_string(),
                    ReturnValue::from(score),
                );
                properties
            }
        };
        properties.check_and_insert(remapping, "id".to_string(), ReturnValue::from(item.uuid()));
        properties.check_and_insert(
            remapping,
            "label".to_string(),
            ReturnValue::from(item.label().to_string()),
        );
        if item.properties_ref().is_some() {
            for (k, v) in item.properties().unwrap().into_iter() {
                properties.check_and_insert(remapping, k, ReturnValue::from(v));
            }
        }

        ReturnValue::Object(properties)
    }
}

impl From<Node> for ReturnValue {
    #[inline]
    fn from(item: Node) -> Self {
        let length = match item.properties_ref() {
            Some(properties) => properties.len(),
            None => 0,
        };
        let mut properties = HashMap::with_capacity(Node::NUM_PROPERTIES + length);
        properties.insert("id".to_string(), ReturnValue::from(item.uuid()));
        properties.insert(
            "label".to_string(),
            ReturnValue::from(item.label().to_string()),
        );
        if item.properties_ref().is_some() {
            properties.extend(
                item.properties()
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| (k, ReturnValue::from(v))),
            );
        }

        ReturnValue::Object(properties)
    }
}

impl From<Edge> for ReturnValue {
    #[inline]
    fn from(item: Edge) -> Self {
        let length = match item.properties_ref() {
            Some(properties) => properties.len(),
            None => 0,
        };
        let mut properties = HashMap::with_capacity(Edge::NUM_PROPERTIES + length);
        properties.insert(
            "from_node".to_string(),
            ReturnValue::from(item.from_node_uuid()),
        );
        properties.insert(
            "to_node".to_string(),
            ReturnValue::from(item.to_node_uuid()),
        );

        properties.insert("id".to_string(), ReturnValue::from(item.uuid()));
        properties.insert(
            "label".to_string(),
            ReturnValue::from(item.label().to_string()),
        );
        if item.properties_ref().is_some() {
            properties.extend(
                item.properties()
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| (k, ReturnValue::from(v))),
            );
        }

        ReturnValue::Object(properties)
    }
}

impl From<HVector> for ReturnValue {
    #[inline]
    fn from(item: HVector) -> Self {
        let length = match item.properties_ref() {
            Some(properties) => properties.len(),
            None => 0,
        };

        let data = item.vector_data();
        let score = item.score();

        let mut properties = HashMap::with_capacity(2 + length);
        properties.insert("data".to_string(), ReturnValue::from(data));
        properties.insert("score".to_string(), ReturnValue::from(score));
        properties.insert("id".to_string(), ReturnValue::from(item.uuid()));
        properties.insert(
            "label".to_string(),
            ReturnValue::from(item.label().to_string()),
        );
        if item.properties_ref().is_some() {
            properties.extend(
                item.properties()
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| (k, ReturnValue::from(v))),
            );
        }

        ReturnValue::Object(properties)
    }
}

impl From<Value> for ReturnValue {
    fn from(value: Value) -> Self {
        ReturnValue::Value(value)
    }
}

impl From<&Value> for ReturnValue {
    fn from(value: &Value) -> Self {
        ReturnValue::Value(value.clone())
    }
}

impl From<Count> for ReturnValue {
    fn from(count: Count) -> Self {
        ReturnValue::Value(Value::I32(count.value() as i32))
    }
}

impl From<String> for ReturnValue {
    fn from(string: String) -> Self {
        ReturnValue::Value(Value::String(string))
    }
}

impl From<bool> for ReturnValue {
    fn from(boolean: bool) -> Self {
        ReturnValue::Value(Value::Boolean(boolean))
    }
}

impl From<&str> for ReturnValue {
    fn from(string: &str) -> Self {
        ReturnValue::Value(Value::String(string.to_string()))
    }
}

impl From<HashMap<String, ReturnValue>> for ReturnValue {
    fn from(object: HashMap<String, ReturnValue>) -> Self {
        ReturnValue::Object(object)
    }
}

impl From<&HashMap<String, ReturnValue>> for ReturnValue {
    fn from(object: &HashMap<String, ReturnValue>) -> Self {
        ReturnValue::Object(object.clone())
    }
}

impl From<Vec<(String, ReturnValue)>> for ReturnValue {
    fn from(object: Vec<(String, ReturnValue)>) -> Self {
        ReturnValue::Object(object.into_iter().collect())
    }
}

impl From<&Vec<(String, ReturnValue)>> for ReturnValue {
    fn from(object: &Vec<(String, ReturnValue)>) -> Self {
        ReturnValue::Object(object.clone().into_iter().collect())
    }
}

impl From<i8> for ReturnValue {
    fn from(integer: i8) -> Self {
        ReturnValue::Value(Value::I8(integer))
    }
}

impl From<i16> for ReturnValue {
    fn from(integer: i16) -> Self {
        ReturnValue::Value(Value::I16(integer))
    }
}

impl From<i64> for ReturnValue {
    fn from(integer: i64) -> Self {
        ReturnValue::Value(Value::I64(integer))
    }
}

impl From<u8> for ReturnValue {
    fn from(integer: u8) -> Self {
        ReturnValue::Value(Value::U8(integer))
    }
}

impl From<u16> for ReturnValue {
    fn from(integer: u16) -> Self {
        ReturnValue::Value(Value::U16(integer))
    }
}

impl From<u32> for ReturnValue {
    fn from(integer: u32) -> Self {
        ReturnValue::Value(Value::U32(integer))
    }
}

impl From<u64> for ReturnValue {
    fn from(integer: u64) -> Self {
        ReturnValue::Value(Value::U64(integer))
    }
}

impl From<u128> for ReturnValue {
    fn from(integer: u128) -> Self {
        ReturnValue::Value(Value::U128(integer))
    }
}

impl From<i32> for ReturnValue {
    fn from(integer: i32) -> Self {
        ReturnValue::Value(Value::I32(integer))
    }
}

impl From<f64> for ReturnValue {
    fn from(float: f64) -> Self {
        ReturnValue::Value(Value::F64(float))
    }
}

impl From<f32> for ReturnValue {
    fn from(float: f32) -> Self {
        ReturnValue::Value(Value::F32(float))
    }
}

impl From<ID> for ReturnValue {
    fn from(id: ID) -> Self {
        ReturnValue::Value(Value::Id(id))
    }
}

impl From<Vec<TraversalValue>> for ReturnValue {
    fn from(array: Vec<TraversalValue>) -> Self {
        ReturnValue::Array(array.into_iter().map(|val| val.into()).collect())
    }
}

impl From<Vec<ReturnValue>> for ReturnValue {
    fn from(array: Vec<ReturnValue>) -> Self {
        ReturnValue::Array(array)
    }
}

impl From<TraversalValue> for ReturnValue {
    fn from(val: TraversalValue) -> Self {
        match val {
            TraversalValue::Node(node) => ReturnValue::from(node),
            TraversalValue::Edge(edge) => ReturnValue::from(edge),
            TraversalValue::Vector(vector) => ReturnValue::from(vector),
            TraversalValue::Count(count) => ReturnValue::from(count),
            TraversalValue::Value(value) => ReturnValue::from(value),
            TraversalValue::Empty => ReturnValue::Empty,
            _ => unreachable!(),
        }
    }
}

impl From<&[f64]> for ReturnValue {
    fn from(data: &[f64]) -> Self {
        ReturnValue::Array(data.iter().map(|f| ReturnValue::from(*f)).collect())
    }
}

impl From<Aggregate> for ReturnValue {
    fn from(aggregate: Aggregate) -> Self {
        let obj = match aggregate {
            Aggregate::Group(data) => data
                .into_values()
                .map(|v| {
                    ReturnValue::Array(
                        v.values
                            .into_iter()
                            .map(ReturnValue::from)
                            .collect::<Vec<ReturnValue>>(),
                    )
                })
                .collect(),
            Aggregate::Count(data) => data
                .into_values()
                .map(|v| {
                    ReturnValue::Object(HashMap::from([
                        ("count".to_string(), ReturnValue::from(v.count)),
                        (
                            "data".to_string(),
                            ReturnValue::Array(
                                v.values.into_iter().map(ReturnValue::from).collect(),
                            ),
                        ),
                    ]))
                })
                .collect(),
        };
        ReturnValue::Array(obj)
    }
}

impl From<GroupBy> for ReturnValue {
    fn from(group_by: GroupBy) -> Self {
        let obj = match group_by {
            GroupBy::Group(data) => data
                .into_values()
                .map(|v| {
                    ReturnValue::Object(
                        v.values
                            .into_iter()
                            .map(|(k, v)| (k, ReturnValue::from(v)))
                            .collect(),
                    )
                })
                .collect(),
            GroupBy::Count(data) => data
                .into_values()
                .map(|v| {
                    let mut values = v
                        .values
                        .into_iter()
                        .map(|(k, v)| (k, ReturnValue::from(v)))
                        .collect::<HashMap<String, ReturnValue>>();
                    values.insert("count".to_string(), ReturnValue::from(v.count));
                    ReturnValue::Object(values)
                })
                .collect(),
        };
        ReturnValue::Array(obj)
    }
}

impl Serialize for ReturnValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            ReturnValue::Value(value) => value.serialize(serializer),
            ReturnValue::Object(object) => object.serialize(serializer),
            ReturnValue::Array(array) => array.serialize(serializer),
            ReturnValue::Empty => serializer.serialize_none(),
        }
    }
}

impl Default for ReturnValue {
    fn default() -> Self {
        ReturnValue::Object(HashMap::new())
    }
}

trait IfPresentThereInsertHere {
    fn check_and_insert(&mut self, there: &ResponseRemapping, key: String, value: ReturnValue);
}

impl IfPresentThereInsertHere for HashMap<String, ReturnValue> {
    fn check_and_insert(&mut self, there: &ResponseRemapping, key: String, value: ReturnValue) {
        // value in mixin
        // if there.should_spread {
        //     self.insert(key, value);
        // } else

        if let Some(existing_value) = there.remappings.get(&key)
            && !existing_value.exclude
        {
            self.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_value() {
        let node = Node {
            id: 1,
            label: "test".to_string(),
            version: 1,
            properties: Some(HashMap::from([(
                "test".to_string(),
                Value::String("test".to_string()),
            )])),
        };

        let remapping = ResponseRemapping {
            should_spread: false,
            remappings: HashMap::from([(
                "test".to_string(),
                Remapping::new(false, None, Some(ReturnValue::from("hello".to_string()))),
            )]),
        };
        let return_value = ReturnValue::from_traversal_value(node, &remapping)
            .mixin_remapping(remapping.remappings);
        assert_eq!(
            return_value,
            ReturnValue::Object(HashMap::from([(
                "test".to_string(),
                ReturnValue::from("hello".to_string())
            )]))
        );
    }
}
