use crate::{
    helix_engine::{
        traversal_core::{
            traversal_iter::{RoTraversalIterator, RwTraversalIterator},
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    protocol::value::Value,
    utils::count::Count,
};

pub trait CountAdapter: Iterator {
    fn count_to_traversal_value(self) -> TraversalValue;
    fn count_to_val(self) -> Value;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> CountAdapter
    for RoTraversalIterator<'a, I>
{
    fn count_to_traversal_value(self) -> TraversalValue {
        TraversalValue::Count(Count::from(self.inner.count()))
    }

    fn count_to_val(self) -> Value {
        Value::from(self.inner.count())
    }
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalValue, GraphError>>> CountAdapter
    for RwTraversalIterator<'a, 'b, I>
{
    fn count_to_traversal_value(self) -> TraversalValue {
        TraversalValue::Count(Count::from(self.inner.count()))
    }

    fn count_to_val(self) -> Value {
        Value::from(self.inner.count())
    }
}
