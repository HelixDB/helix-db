use std::collections::HashMap;

use crate::{
    helix_engine::{
        traversal_core::{
            traversal_iter::RoTraversalIterator,
            traversal_value::{Traversable, TraversalValue},
        },
        types::GraphError,
    },
    utils::aggregate::{Aggregate, AggregateItem},
};

pub trait AggregateAdapter<'a>: Iterator {
    fn aggregate_by(self, properties: &[String], should_count: bool) -> Result<Aggregate, GraphError>;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> AggregateAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn aggregate_by(self, properties: &[String], should_count: bool) -> Result<Aggregate, GraphError> {
        let mut groups: HashMap<String, AggregateItem> = HashMap::new();

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
            group.values.insert(item);
            group.count += 1;
        }

        if should_count {
            Ok(Aggregate::Count(groups))
        } else {
            Ok(Aggregate::Group(groups))
        }
    }
}
