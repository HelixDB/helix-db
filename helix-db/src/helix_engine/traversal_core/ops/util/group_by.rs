use crate::{
    helix_engine::{
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::group_by::{GroupBy, GroupByItem},
};
use std::collections::HashMap;

pub trait GroupByAdapter: Iterator {
    fn group_by(self, properties: &[String], should_count: bool) -> Result<GroupBy, GraphError>;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    GroupByAdapter for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    // TODO: optimize this
    fn group_by(self, properties: &[String], should_count: bool) -> Result<GroupBy, GraphError> {
        let mut groups: HashMap<String, GroupByItem> = HashMap::new();

        let properties_len = properties.len();

        for item in self.inner {
            let item = item?;

            // TODO HANDLE COUNT
            let mut kvs = Vec::with_capacity(properties_len);
            let mut key_parts = Vec::with_capacity(properties_len);

            for property in properties {
                match item.get_property(property) {
                    Some(val) => {
                        key_parts.push(val.inner_stringify());
                        kvs.push((property.to_string(), val.clone()));
                    }
                    None => {
                        key_parts.push("null".to_string());
                    }
                }
            }
            let key = key_parts.join("_");

            let group = groups.entry(key).or_default();
            group.values.extend(kvs);
            group.count += 1;
        }

        if should_count {
            Ok(GroupBy::Count(groups))
        } else {
            Ok(GroupBy::Group(groups))
        }
    }
}
