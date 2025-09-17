use sonic_rs::{Deserialize, Serialize};

pub enum ShouldCount {
    Yes,
    No,
}

#[derive(PartialEq, Deserialize, Clone)]
pub struct Count {
    value: usize,
}

impl Count {
    pub fn new(value: usize) -> Count {
        Count { value }
    }

    pub fn gt(&self, cmp: usize) -> bool {
        self.value > cmp
    }

    pub fn gte(&self, cmp: usize) -> bool {
        self.value >= cmp
    }

    pub fn lt(&self, cmp: usize) -> bool {
        self.value < cmp
    }

    pub fn lte(&self, cmp: usize) -> bool {
        self.value <= cmp
    }

    pub fn neq(&self, cmp: usize) -> bool {
        self.value != cmp
    }

    pub fn value(&self) -> usize {
        self.value
    }
}

impl std::fmt::Debug for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl std::cmp::PartialEq<usize> for Count {
    fn eq(&self, other: &usize) -> bool {
        &self.value == other
    }
}

impl std::cmp::PartialOrd<usize> for Count {
    fn partial_cmp(&self, other: &usize) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(other)
    }
}

impl PartialOrd for Count {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Count {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl Eq for Count {}

impl From<Count> for usize {
    fn from(value: Count) -> Self {
        value.value
    }
}

impl From<usize> for Count {
    fn from(value: usize) -> Self {
        Count::new(value)
    }
}

impl Serialize for Count {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        self.value.serialize(serializer)
    }
}
