use serde::{Deserialize, Serialize};

/// Node or edge element kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElementKind {
    /// Node element.
    Node,
    /// Edge element.
    Edge,
}

impl std::fmt::Display for ElementKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node => f.write_str("node"),
            Self::Edge => f.write_str("edge"),
        }
    }
}

/// Search index/query kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchIndexKind {
    /// Vector search.
    Vector,
    /// Text search.
    Text,
}

impl std::fmt::Display for SearchIndexKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Vector => f.write_str("vector"),
            Self::Text => f.write_str("text"),
        }
    }
}
