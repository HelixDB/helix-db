//! Validated planner IR for index DDL enqueue, status, and control operations.

use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

use crate::catalog;

/// Validation failure for a public index-operation UUID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid index operation ID `{value}`: expected a non-nil canonical lowercase UUID")]
pub struct IndexOperationIdError {
    value: String,
}

/// Canonical lowercase non-nil UUID used by lifecycle status/control plans.
///
/// ```
/// use helix_planner::ir::IndexOperationId;
///
/// let id = IndexOperationId::try_new("07070707-0707-0707-0707-070707070707").unwrap();
/// assert_eq!(id.as_str(), "07070707-0707-0707-0707-070707070707");
/// assert!(IndexOperationId::try_new("00000000-0000-0000-0000-000000000000").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IndexOperationId(String);

impl IndexOperationId {
    /// Validates the frozen UUID wire shape without normalizing caller input.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IndexOperationIdError> {
        let value = value.into();
        let valid_shape = value.len() == 36
            && value
                .bytes()
                .enumerate()
                .all(|(offset, byte)| match offset {
                    8 | 13 | 18 | 23 => byte == b'-',
                    _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
                });
        let non_nil = value.bytes().any(|byte| !matches!(byte, b'0' | b'-'));
        if !valid_shape || !non_nil {
            return Err(IndexOperationIdError { value });
        }
        Ok(Self(value))
    }

    /// Borrows the canonical UUID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for IndexOperationId {
    type Error = IndexOperationIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<IndexOperationId> for String {
    fn from(value: IndexOperationId) -> Self {
        value.0
    }
}

/// Positive vector dimension for vector-index DDL.
///
/// ```
/// use helix_planner::ir::VectorIndexDimension;
///
/// assert_eq!(VectorIndexDimension::new(3).unwrap().get(), 3);
/// assert!(VectorIndexDimension::new(0).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VectorIndexDimension(NonZeroUsize);

impl VectorIndexDimension {
    /// Build a positive vector dimension.
    pub fn new(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self)
    }

    /// Build from an already-positive dimension.
    pub const fn from_non_zero(value: NonZeroUsize) -> Self {
        Self(value)
    }

    /// Return the raw dimension value.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Vector distance metric for vector-index DDL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorIndexMetric {
    /// Cosine similarity.
    Cosine,
    /// Euclidean/L2 distance.
    Euclidean,
    /// Manhattan/L1 distance.
    Manhattan,
}

/// Index create duplicate handling mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexCreateMode {
    /// Error if the index already exists.
    ErrorIfExists,
    /// Do nothing if the index already exists.
    IfNotExists,
}

/// Index DDL plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexDdlPlan {
    /// Create index.
    Create {
        /// Index spec.
        spec: IndexDdlCreateSpec,
        /// Duplicate handling.
        mode: IndexCreateMode,
    },
    /// Drop index.
    Drop {
        /// Index spec.
        spec: IndexDdlDropSpec,
    },
    /// Read a retained operation in the execution scope.
    GetOperation {
        /// Validated operation ID.
        operation_id: IndexOperationId,
    },
    /// Ensure a retained operation is runnable in the execution scope.
    RetryOperation {
        /// Validated operation ID.
        operation_id: IndexOperationId,
    },
    /// Convert a constructing BUILD into abort cleanup.
    AbortOperation {
        /// Validated operation ID.
        operation_id: IndexOperationId,
    },
}

/// Index creation spec with validated labels, properties, and create-time
/// attributes.
///
/// ```
/// use helix_planner::catalog::{IndexUniqueness, ScopedPropertyKey};
/// use helix_planner::ir::IndexDdlCreateSpec;
///
/// let spec = IndexDdlCreateSpec::NodeEquality {
///     key: ScopedPropertyKey::try_new("User", "email").unwrap(),
///     uniqueness: IndexUniqueness::Unique,
/// };
///
/// assert!(matches!(spec, IndexDdlCreateSpec::NodeEquality { .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum IndexDdlCreateSpec {
    /// Node equality index.
    NodeEquality {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
        /// Unique index.
        uniqueness: catalog::IndexUniqueness,
    },
    /// Node range index.
    NodeRange {
        /// Scoped indexed property and direction.
        key: catalog::ScopedPropertyDirectionKey,
    },
    /// Edge equality index.
    EdgeEquality {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
    },
    /// Edge range index.
    EdgeRange {
        /// Scoped indexed property and direction.
        key: catalog::ScopedPropertyDirectionKey,
    },
    /// Node vector index.
    NodeVector {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
        /// Vector dimension.
        dimension: VectorIndexDimension,
        /// Distance metric.
        metric: VectorIndexMetric,
        /// Tenant scoping configuration.
        scope: catalog::SearchIndexScope,
    },
    /// Node text index.
    NodeText {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
        /// Tenant scoping configuration.
        scope: catalog::SearchIndexScope,
    },
    /// Edge vector index.
    EdgeVector {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
        /// Vector dimension.
        dimension: VectorIndexDimension,
        /// Distance metric.
        metric: VectorIndexMetric,
        /// Tenant scoping configuration.
        scope: catalog::SearchIndexScope,
    },
    /// Edge text index.
    EdgeText {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
        /// Tenant scoping configuration.
        scope: catalog::SearchIndexScope,
    },
}

/// Index drop spec with the fields needed to identify the index variant.
///
/// ```
/// use helix_planner::catalog::{IndexUniqueness, ScopedPropertyKey};
/// use helix_planner::ir::IndexDdlDropSpec;
///
/// let spec = IndexDdlDropSpec::NodeEquality {
///     key: ScopedPropertyKey::try_new("User", "email").unwrap(),
///     uniqueness: IndexUniqueness::Unique,
/// };
///
/// assert!(matches!(spec, IndexDdlDropSpec::NodeEquality { .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum IndexDdlDropSpec {
    /// Node equality index.
    NodeEquality {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
        /// Required uniqueness variant.
        uniqueness: catalog::IndexUniqueness,
    },
    /// Node range index.
    NodeRange {
        /// Scoped indexed property and direction.
        key: catalog::ScopedPropertyDirectionKey,
    },
    /// Edge equality index.
    EdgeEquality {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
    },
    /// Edge range index.
    EdgeRange {
        /// Scoped indexed property and direction.
        key: catalog::ScopedPropertyDirectionKey,
    },
    /// Node vector index.
    NodeVector {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
    },
    /// Node text index.
    NodeText {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
    },
    /// Edge vector index.
    EdgeVector {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
    },
    /// Edge text index.
    EdgeText {
        /// Scoped indexed property.
        key: catalog::ScopedPropertyKey,
    },
}
