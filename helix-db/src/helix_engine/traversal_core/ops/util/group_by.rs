use std::collections::HashMap;

use crate::{
    helix_engine::{
        traversal_core::{
            traversal_iter::RoTraversalIterator,
            traversal_value::{Traversable, TraversalValue},
        },
        types::GraphError,
    },
    utils::group_by::{GroupBy, GroupByItem},
};

pub trait GroupByAdapter<'a>: Iterator {
    fn group_by(self, properties: &[String], should_count: bool) -> Result<GroupBy, GraphError>;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> GroupByAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn group_by(self, properties: &[String], should_count: bool) -> Result<GroupBy, GraphError> {
        let mut groups: HashMap<String, GroupByItem> = HashMap::new();

        // Pre-calculate capacity outside the loop since properties length is constant
        let properties_len = properties.len();

        for item in self.inner {
            let item = item?;

            // TODO HANDLE COUNT
            // Pre-allocate with exact capacity - size is known from properties.len()
            let mut kvs = Vec::with_capacity(properties_len);
            let mut key_parts = Vec::with_capacity(properties_len);

            for property in properties {
                match item.check_property(property) {
                    Ok(val) => {
                        key_parts.push(val.inner_stringify());
                        kvs.push((property.to_string(), val.into_owned()));
                    }
                    Err(_) => {
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
