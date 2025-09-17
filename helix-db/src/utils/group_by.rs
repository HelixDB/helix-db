use std::collections::HashMap;

use crate::protocol::value::Value;

#[derive(Default)]
pub struct GroupByItem {
    pub values: HashMap<String, Value>,
    pub count: i32,
}

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
