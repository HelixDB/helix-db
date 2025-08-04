use std::collections::HashMap;

use crate::{
    protocol::value::Value,
    utils::items::{Edge, Node},
};

#[derive(Default, Clone)]
pub struct VersionInfo(pub HashMap<String, ItemInfo>);

impl VersionInfo {
    pub fn upgrade_to_node_latest(&self, node: Node) -> Node {
        let item_info = self
            .0
            .get(&node.label)
            .expect("All nodes should have version info");

        item_info.upgrade_node_to_latest(node)
    }

    pub fn upgrade_to_edge_latest(&self, node: Edge) -> Edge {
        let item_info = self
            .0
            .get(&node.label)
            .expect("All edges should have version info");

        item_info.upgrade_edge_to_latest(node)
    }

    pub fn get_latest(&self, label: &str) -> u8 {
        self.0
            .get(label)
            .expect("All labels should have version info")
            .latest
    }
}

type Props = HashMap<String, Value>;

#[derive(Clone)]
pub struct ItemInfo {
    /// The latest version of this item
    /// All writes should be done with this version
    latest: u8,
    /// Stores transition from version x and index x-1
    transition_fns: Vec<fn(Props) -> Props>,
}

impl ItemInfo {
    fn upgrade_node_to_latest(&self, mut node: Node) -> Node {
        if node.version < self.latest
            && let Some(mut node_props) = node.properties.take()
        {
            for trans_fn in self.transition_fns.iter().skip(node.version as usize - 1) {
                node_props = trans_fn(node_props);
            }

            node.properties = Some(node_props);
        }

        node
    }

    fn upgrade_edge_to_latest(&self, mut edge: Edge) -> Edge {
        if edge.version < self.latest
            && let Some(mut edge_props) = edge.properties.take()
        {
            for trans_fn in self.transition_fns.iter().skip(edge.version as usize - 1) {
                edge_props = trans_fn(edge_props);
            }

            edge.properties = Some(edge_props);
        }

        edge
    }
}

pub type TransitionFn = fn(Props) -> Props;

#[derive(Clone, Debug)]
pub struct Transition {
    pub item_label: &'static str,
    pub from_version: u8,
    pub to_version: u8,
    pub func: TransitionFn,
}

impl Transition {
    pub const fn new(
        item_label: &'static str,
        from_version: u8,
        to_version: u8,
        func: TransitionFn,
    ) -> Self {
        Self {
            item_label,
            from_version,
            to_version,
            func,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransitionSubmission(pub Transition);

inventory::collect!(TransitionSubmission);

#[macro_export]
macro_rules! field_addition_from_old_field {
    ($old_props:expr, $new_props:expr, $new_name:expr, $old_name:expr) => {{
        let value = $old_props.remove($old_name).unwrap();
        $new_props.insert($new_name.to_string(), value);
    }};
}

#[macro_export]
macro_rules! field_type_cast {
    ($old_props:expr, $new_props:expr, $field_to_cast:expr, $new_field_type:ident) => {{
        let value = cast(
            $old_props.remove($field_to_cast).unwrap(),
            CastType::$new_field_type,
        );
        $new_props.insert($field_to_cast.to_string(), value);
    }};
}

#[macro_export]
macro_rules! field_addition_from_value {
    ($new_props:expr, $new_field_name:expr, $value:expr) => {{
        $new_props.insert($new_field_name.to_string(), Value::from($value));
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_renaming() {
        let mut props = HashMap::from([(
            "some_name".to_string(),
            Value::String("some_value".to_string()),
        )]);

        let mut new_props = HashMap::new();
        field_addition_from_old_field!(&mut props, &mut new_props, "some_name", "new_name");

        assert_eq!(
            new_props,
            HashMap::from([(
                "new_name".to_string(),
                Value::String("some_value".to_string())
            )])
        );
    }

    #[test]
    fn test_field_type_cast() {
        use crate::protocol::value::casting::{CastType, cast};

        let mut props =
            HashMap::from([("some_name".to_string(), Value::String("123".to_string()))]);
        let mut new_props = HashMap::new();
        field_type_cast!(&mut props, &mut new_props, "some_name", U32);

        assert_eq!(
            props,
            HashMap::from([("some_name".to_string(), Value::U32(123))])
        );
    }

    #[test]
    fn test_field_addition_from_value() {
        let mut new_props = HashMap::new();

        field_addition_from_value!(&mut new_props, "new_name", 123);

        assert_eq!(
            new_props,
            HashMap::from([("new_name".to_string(), Value::U32(123))])
        );
    }
}
