use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

/// Physical direction for range indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RangeIndexDirection {
    /// Ascending.
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// Vector distance metric for vector index creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorDistanceMetric {
    /// Cosine similarity.
    Cosine,
    /// Euclidean/L2 distance.
    Euclidean,
    /// Manhattan/L1 distance.
    Manhattan,
}

/// Dynamic index declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexSpec {
    /// Node equality index.
    NodeEquality {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Unique index.
        #[serde(default)]
        unique: bool,
    },
    /// Node range index.
    NodeRange {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Direction.
        #[serde(default)]
        direction: RangeIndexDirection,
    },
    /// Edge equality index.
    EdgeEquality {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
    },
    /// Edge range index.
    EdgeRange {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Direction.
        #[serde(default)]
        direction: RangeIndexDirection,
    },
    /// Node vector index.
    NodeVector {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Vector dimension.
        dimension: NonZeroUsize,
        /// Distance metric.
        metric: VectorDistanceMetric,
        /// Optional tenant property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
    /// Node text index.
    NodeText {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Optional tenant property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
    /// Edge vector index.
    EdgeVector {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Vector dimension.
        dimension: NonZeroUsize,
        /// Distance metric.
        metric: VectorDistanceMetric,
        /// Optional tenant property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
    /// Edge text index.
    EdgeText {
        /// Label scope.
        label: String,
        /// Indexed property.
        property: String,
        /// Optional tenant property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
}

impl IndexSpec {
    /// Node equality index.
    pub fn node_equality(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::NodeEquality {
            label: label.into(),
            property: property.into(),
            unique: false,
        }
    }

    /// Unique node equality index.
    pub fn node_unique_equality(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::NodeEquality {
            label: label.into(),
            property: property.into(),
            unique: true,
        }
    }

    /// Node range index.
    pub fn node_range(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::node_range_with_direction(label, property, RangeIndexDirection::Asc)
    }

    /// Descending node range index.
    pub fn node_range_desc(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::node_range_with_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Node range index with direction.
    pub fn node_range_with_direction(
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> Self {
        Self::NodeRange {
            label: label.into(),
            property: property.into(),
            direction,
        }
    }

    /// Edge equality index.
    pub fn edge_equality(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::EdgeEquality {
            label: label.into(),
            property: property.into(),
        }
    }

    /// Edge range index.
    pub fn edge_range(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::edge_range_with_direction(label, property, RangeIndexDirection::Asc)
    }

    /// Descending edge range index.
    pub fn edge_range_desc(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::edge_range_with_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Edge range index with direction.
    pub fn edge_range_with_direction(
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> Self {
        Self::EdgeRange {
            label: label.into(),
            property: property.into(),
            direction,
        }
    }

    /// Node vector index.
    pub fn node_vector(
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: NonZeroUsize,
        metric: VectorDistanceMetric,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::NodeVector {
            label: label.into(),
            property: property.into(),
            dimension,
            metric,
            tenant_property: tenant_property.map(Into::into),
        }
    }

    /// Node text index.
    pub fn node_text(
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::NodeText {
            label: label.into(),
            property: property.into(),
            tenant_property: tenant_property.map(Into::into),
        }
    }

    /// Edge vector index.
    pub fn edge_vector(
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: NonZeroUsize,
        metric: VectorDistanceMetric,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::EdgeVector {
            label: label.into(),
            property: property.into(),
            dimension,
            metric,
            tenant_property: tenant_property.map(Into::into),
        }
    }

    /// Edge text index.
    pub fn edge_text(
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::EdgeText {
            label: label.into(),
            property: property.into(),
            tenant_property: tenant_property.map(Into::into),
        }
    }
}
