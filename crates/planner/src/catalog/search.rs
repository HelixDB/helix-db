use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ir::NonEmptyString;

use super::element::ElementKind;

/// Search index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchIndexKey {
    /// Node or edge.
    pub element: ElementKind,
    /// Label scope.
    pub label: NonEmptyString,
    /// Property.
    pub property: NonEmptyString,
}

impl SearchIndexKey {
    /// Build a search key from validated components.
    pub fn new(element: ElementKind, label: NonEmptyString, property: NonEmptyString) -> Self {
        Self {
            element,
            label,
            property,
        }
    }

    /// Try to build a search key from raw strings.
    pub fn try_new(
        element: ElementKind,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Option<Self> {
        Some(Self::new(
            element,
            NonEmptyString::new(label)?,
            NonEmptyString::new(property)?,
        ))
    }
}

impl std::fmt::Display for SearchIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.element, self.label, self.property)
    }
}

/// Node search index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeSearchIndexKey {
    /// Label scope.
    pub label: NonEmptyString,
    /// Property.
    pub property: NonEmptyString,
}

impl NodeSearchIndexKey {
    /// Build a node search key from validated components.
    pub fn new(label: NonEmptyString, property: NonEmptyString) -> Self {
        Self { label, property }
    }

    /// Try to build a node search key from raw strings.
    pub fn try_new(label: impl Into<String>, property: impl Into<String>) -> Option<Self> {
        Some(Self::new(
            NonEmptyString::new(label)?,
            NonEmptyString::new(property)?,
        ))
    }
}

impl From<NodeSearchIndexKey> for SearchIndexKey {
    fn from(key: NodeSearchIndexKey) -> Self {
        Self {
            element: ElementKind::Node,
            label: key.label,
            property: key.property,
        }
    }
}

impl std::fmt::Display for NodeSearchIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node:{}:{}", self.label, self.property)
    }
}

impl Serialize for NodeSearchIndexKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SearchIndexKey::from(self.clone()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeSearchIndexKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = SearchIndexKey::deserialize(deserializer)?;
        match key.element {
            ElementKind::Node => Ok(Self {
                label: key.label,
                property: key.property,
            }),
            ElementKind::Edge => Err(D::Error::custom("expected node search index key")),
        }
    }
}

/// Edge search index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeSearchIndexKey {
    /// Label scope.
    pub label: NonEmptyString,
    /// Property.
    pub property: NonEmptyString,
}

impl EdgeSearchIndexKey {
    /// Build an edge search key from validated components.
    pub fn new(label: NonEmptyString, property: NonEmptyString) -> Self {
        Self { label, property }
    }

    /// Try to build an edge search key from raw strings.
    pub fn try_new(label: impl Into<String>, property: impl Into<String>) -> Option<Self> {
        Some(Self::new(
            NonEmptyString::new(label)?,
            NonEmptyString::new(property)?,
        ))
    }
}

impl From<EdgeSearchIndexKey> for SearchIndexKey {
    fn from(key: EdgeSearchIndexKey) -> Self {
        Self {
            element: ElementKind::Edge,
            label: key.label,
            property: key.property,
        }
    }
}

impl std::fmt::Display for EdgeSearchIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "edge:{}:{}", self.label, self.property)
    }
}

impl Serialize for EdgeSearchIndexKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SearchIndexKey::from(self.clone()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EdgeSearchIndexKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = SearchIndexKey::deserialize(deserializer)?;
        match key.element {
            ElementKind::Node => Err(D::Error::custom("expected edge search index key")),
            ElementKind::Edge => Ok(Self {
                label: key.label,
                property: key.property,
            }),
        }
    }
}

/// Tenant scoping configured for a search index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchIndexScope {
    /// Search index has no tenant property.
    Unscoped,
    /// Search index is scoped by a tenant property.
    Tenant {
        /// Tenant property name.
        property: NonEmptyString,
    },
}

impl SearchIndexScope {
    /// Build a search scope from an optional validated tenant property.
    pub fn new(tenant_property: Option<NonEmptyString>) -> Self {
        match tenant_property {
            Some(property) => Self::Tenant { property },
            None => Self::Unscoped,
        }
    }

    /// Try to build a search scope from an optional tenant property.
    pub fn try_new(tenant_property: Option<impl Into<String>>) -> Option<Self> {
        match tenant_property {
            Some(property) => Some(Self::Tenant {
                property: NonEmptyString::new(property)?,
            }),
            None => Some(Self::Unscoped),
        }
    }
}
