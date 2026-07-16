use serde::{Deserialize, Serialize};

use crate::ir::NonEmptyString;

use super::search::SearchIndexScope;

/// Secondary index uniqueness mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexUniqueness {
    /// Multiple elements may share the indexed value.
    NonUnique,
    /// At most one element may have each indexed value.
    Unique,
}

/// Node equality index metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeEqualityIndexMeta {
    /// Physical/logical index identifier.
    pub index_id: NonEmptyString,
    /// Index uniqueness.
    pub uniqueness: IndexUniqueness,
}

impl NodeEqualityIndexMeta {
    /// Build non-unique index metadata from a validated identifier.
    pub fn new(index_id: NonEmptyString) -> Self {
        Self {
            index_id,
            uniqueness: IndexUniqueness::NonUnique,
        }
    }

    /// Try to build non-unique index metadata from a raw identifier.
    pub fn try_new(index_id: impl Into<String>) -> Option<Self> {
        Some(Self {
            index_id: NonEmptyString::new(index_id)?,
            uniqueness: IndexUniqueness::NonUnique,
        })
    }

    /// Set index uniqueness.
    pub fn with_uniqueness(mut self, uniqueness: IndexUniqueness) -> Self {
        self.uniqueness = uniqueness;
        self
    }
}

/// Edge equality index metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeEqualityIndexMeta {
    /// Physical/logical index identifier.
    pub index_id: NonEmptyString,
}

impl EdgeEqualityIndexMeta {
    /// Build index metadata from a validated identifier.
    pub fn new(index_id: NonEmptyString) -> Self {
        Self { index_id }
    }

    /// Try to build index metadata from a raw identifier.
    pub fn try_new(index_id: impl Into<String>) -> Option<Self> {
        Some(Self {
            index_id: NonEmptyString::new(index_id)?,
        })
    }
}

/// Node range index metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRangeIndexMeta {
    /// Physical/logical index identifier.
    pub index_id: NonEmptyString,
}

impl NodeRangeIndexMeta {
    /// Build index metadata from a validated identifier.
    pub fn new(index_id: NonEmptyString) -> Self {
        Self { index_id }
    }

    /// Try to build index metadata from a raw identifier.
    pub fn try_new(index_id: impl Into<String>) -> Option<Self> {
        Some(Self {
            index_id: NonEmptyString::new(index_id)?,
        })
    }
}

/// Edge range index metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeRangeIndexMeta {
    /// Physical/logical index identifier.
    pub index_id: NonEmptyString,
}

impl EdgeRangeIndexMeta {
    /// Build index metadata from a validated identifier.
    pub fn new(index_id: NonEmptyString) -> Self {
        Self { index_id }
    }

    /// Try to build index metadata from a raw identifier.
    pub fn try_new(index_id: impl Into<String>) -> Option<Self> {
        Some(Self {
            index_id: NonEmptyString::new(index_id)?,
        })
    }
}

/// Vector index metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorIndexMeta {
    /// Physical/logical index identifier.
    pub index_id: NonEmptyString,
    /// Tenant scoping configuration.
    pub scope: SearchIndexScope,
}

impl VectorIndexMeta {
    /// Build vector index metadata from validated parts.
    pub fn new(index_id: NonEmptyString, scope: SearchIndexScope) -> Self {
        Self { index_id, scope }
    }

    /// Try to build vector index metadata from raw identifiers.
    pub fn try_new(
        index_id: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Option<Self> {
        Some(Self {
            index_id: NonEmptyString::new(index_id)?,
            scope: SearchIndexScope::try_new(tenant_property)?,
        })
    }
}

/// Text index metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextIndexMeta {
    /// Physical/logical index identifier.
    pub index_id: NonEmptyString,
    /// Tenant scoping configuration.
    pub scope: SearchIndexScope,
}

impl TextIndexMeta {
    /// Build text index metadata from validated parts.
    pub fn new(index_id: NonEmptyString, scope: SearchIndexScope) -> Self {
        Self { index_id, scope }
    }

    /// Try to build text index metadata from raw identifiers.
    pub fn try_new(
        index_id: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Option<Self> {
        Some(Self {
            index_id: NonEmptyString::new(index_id)?,
            scope: SearchIndexScope::try_new(tenant_property)?,
        })
    }
}
