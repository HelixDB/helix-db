use std::collections::HashMap;

use serde::Serialize;

use crate::protocol::value::Value;

#[derive(Clone, Default, Serialize)]
pub struct GroupByItem {
    pub values: HashMap<String, Value>,
    pub count: i32,
}

#[derive(Clone, Serialize)]
pub enum GroupBy {
    Group(HashMap<String, GroupByItem>),
    Count(HashMap<String, GroupByItem>),
}

impl GroupBy {
    pub fn into_count(self) -> Self {
        match self {
            Self::Group(g) => Self::Count(g),
            _ => self,
        }
    }
}
