use crate::utils::{
    items::{Edge, Node},
    properties::ImmutablePropertiesMap,
};
use std::collections::HashMap;

#[derive(Default, Clone)]
pub struct VersionInfo(pub HashMap<&'static str, ItemInfo>);

impl<'arena> VersionInfo {
    pub fn upgrade_to_node_latest(&self, node: Node<'arena>) -> Node<'arena> {
        match self.0.get(&node.label) {
            Some(item_info) => item_info.upgrade_node_to_latest(node),
            None => node,
        }
    }

    pub fn upgrade_to_edge_latest(&self, node: Edge<'arena>) -> Edge<'arena> {
        match self.0.get(&node.label) {
            Some(item_info) => item_info.upgrade_edge_to_latest(node),
            None => node,
        }
    }

    pub fn get_latest(&self, label: &str) -> u8 {
        self.0
            .get(label)
            .map(|item_info| item_info.latest)
            .unwrap_or(1)
    }
}

type Props<'arena> = ImmutablePropertiesMap<'arena>;
#[derive(Clone, Debug)]
pub struct TransitionFn {
    pub from_version: u8,
    pub to_version: u8,
    pub func: fn(Props) -> Props,
}

#[derive(Clone, Debug)]
pub struct ItemInfo {
    /// The latest version of this item
    /// All writes should be done with this version
    pub latest: u8,
    /// Stores transition from version x and index x-1
    pub transition_fns: Vec<TransitionFn>,
}

impl<'arena> ItemInfo {
    fn upgrade_node_to_latest(&self, mut node: Node<'arena>) -> Node<'arena> {
        if node.version < self.latest
            && let Some(mut node_props) = node.properties.take()
        {
            for TransitionFn { func, .. } in
                self.transition_fns.iter().skip(node.version as usize - 1)
            {
                node_props = func(node_props);
            }

            node.properties = Some(node_props);
        }

        node
    }

    fn upgrade_edge_to_latest(&self, mut edge: Edge<'arena>) -> Edge<'arena> {
        if edge.version < self.latest
            && let Some(mut edge_props) = edge.properties.take()
        {
            for TransitionFn { func, .. } in
                self.transition_fns.iter().skip(edge.version as usize - 1)
            {
                edge_props = func(edge_props);
            }

            edge.properties = Some(edge_props);
        }

        edge
    }
}

impl Default for ItemInfo {
    fn default() -> Self {
        Self {
            latest: 1,
            transition_fns: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Transition {
    pub item_label: &'static str,
    pub from_version: u8,
    pub to_version: u8,
    pub func: fn(Props) -> Props,
}

impl Transition {
    pub const fn new(
        item_label: &'static str,
        from_version: u8,
        to_version: u8,
        func: fn(Props) -> Props,
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
    ($new_props:expr, $new_field_name:expr, $new_field_type:ident, $value:expr) => {{
        $new_props.insert($new_field_name.to_string(), Value::$new_field_type($value));
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::value::Value;

    #[test]
    fn test_field_renaming() {
        let mut props = HashMap::from([(
            "some_name".to_string(),
            Value::String("some_value".to_string()),
        )]);

        let mut new_props = HashMap::new();
        field_addition_from_old_field!(&mut props, &mut new_props, "new_name", "some_name");

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
            new_props,
            HashMap::from([("some_name".to_string(), Value::U32(123))])
        );
    }

    #[test]
    fn test_field_addition_from_value() {
        let mut new_props = HashMap::new();

        field_addition_from_value!(&mut new_props, "new_name", U32, 123);

        assert_eq!(
            new_props,
            HashMap::from([("new_name".to_string(), Value::U32(123))])
        );
    }
}
