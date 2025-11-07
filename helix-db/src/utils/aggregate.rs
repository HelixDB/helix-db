use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::helix_engine::traversal_core::traversal_value::TraversalValue;

#[derive(Clone, Default, Serialize)]
pub struct AggregateItem<'arena> {
    pub values: HashSet<TraversalValue<'arena>>,
    pub count: i32,
}

#[derive(Clone, Serialize)]
pub enum Aggregate<'arena> {
    Group(HashMap<String, AggregateItem<'arena>>),
    Count(HashMap<String, AggregateItem<'arena>>),
}

impl<'arena> Aggregate<'arena> {
    pub fn into_count(self) -> Self {
        match self {
            Self::Group(g) => Self::Count(g),
            _ => self,
        }
    }
}
