//! Validated configured-index definitions and runtime catalog projection.

#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

use helix_ast::index::RangeIndexDirection as PlannerRangeIndexDirection;
use helix_planner::{catalog, ir};
use serde::{Deserialize, Serialize};

use super::utils::{ConfigError, ConfigResult};
use crate::index_v2::ValidatedDynamicIndexDefinition;
use crate::search::vector::{
    CollisionThreshold, Connections, ConstructionBeamWidth, FailureProbability, Layer0Connections,
    LayerMultiplier, UnitInterval, VectorDistanceMetric, DEFAULT_SIMHASH_COLLISION_THRESHOLD,
    SIMHASH_BITS,
};

const SECONDARY_INDEX_SCOPE_SEPARATOR: char = '\x1f';

fn parse_index_component(kind: &'static str, value: impl Into<String>) -> ConfigResult<String> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(ConfigError::new(format!("{kind} is required")));
    }
    if value.contains(SECONDARY_INDEX_SCOPE_SEPARATOR) {
        return Err(ConfigError::new(format!(
            "{kind} cannot contain the internal secondary-index scope separator"
        )));
    }
    Ok(value)
}

fn parse_index_tenant_property(value: impl Into<String>) -> ConfigResult<String> {
    parse_index_component("index tenant property", value)
}

fn planner_range_direction(direction: RangeIndexDirection) -> PlannerRangeIndexDirection {
    match direction {
        RangeIndexDirection::Asc => PlannerRangeIndexDirection::Asc,
        RangeIndexDirection::Desc => PlannerRangeIndexDirection::Desc,
    }
}

fn scoped_property_key(
    label: impl Into<String>,
    property: impl Into<String>,
) -> ConfigResult<catalog::ScopedPropertyKey> {
    let label = parse_index_component("secondary index label", label)?;
    let property = parse_index_component("secondary index property", property)?;
    catalog::ScopedPropertyKey::try_new(label, property)
        .ok_or_else(|| ConfigError::new("secondary index label and property are required"))
}

fn scoped_property_direction_key(
    label: impl Into<String>,
    property: impl Into<String>,
    direction: RangeIndexDirection,
) -> ConfigResult<catalog::ScopedPropertyDirectionKey> {
    let label = parse_index_component("secondary index label", label)?;
    let property = parse_index_component("secondary index property", property)?;
    catalog::ScopedPropertyDirectionKey::try_new(
        label,
        property,
        planner_range_direction(direction),
    )
    .ok_or_else(|| ConfigError::new("secondary index label and property are required"))
}

fn scoped_property_key_from_storage(property: &str) -> Option<catalog::ScopedPropertyKey> {
    let (label, property) = split_scoped_secondary_index_property(property)?;
    catalog::ScopedPropertyKey::try_new(label, property)
}

fn scoped_property_direction_key_from_storage(
    property: &str,
    direction: RangeIndexDirection,
) -> Option<catalog::ScopedPropertyDirectionKey> {
    let (label, property) = split_scoped_secondary_index_property(property)?;
    catalog::ScopedPropertyDirectionKey::try_new(
        label,
        property,
        planner_range_direction(direction),
    )
}

#[inline]
fn default_ml_for_m(m: usize) -> f32 {
    let effective_m = m.max(2) as f32;
    1.0 / effective_m.ln()
}

/// Vector index element type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorElementType {
    /// Vector index on nodes
    Node,
    /// Vector index on edges
    Edge,
}

/// Text index element type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TextElementType {
    /// Text index on nodes.
    Node,
    /// Text index on edges.
    Edge,
}

/// Supported Tantivy analyzer presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TextAnalyzerKind {
    /// Simple tokenization with lowercase normalization.
    Standard,
    /// Simple tokenization with lowercase normalization and English stemming.
    StandardStemEn,
    /// Whitespace tokenization with lowercase normalization.
    WhitespaceLowercase,
}

impl TextAnalyzerKind {
    /// Stable analyzer name used in persisted metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::StandardStemEn => "standard_stem_en",
            Self::WhitespaceLowercase => "whitespace_lowercase",
        }
    }
}

/// Text index definition for automatic creation on open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextIndexDefinition {
    element_type: TextElementType,
    label: String,
    property: String,
    tenant_property: Option<String>,
    analyzer: TextAnalyzerKind,
    positions_enabled: bool,
}

impl TextIndexDefinition {
    /// Element type (node or edge).
    pub const fn element_type(&self) -> TextElementType {
        self.element_type
    }

    /// Label to scope the index.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Property name containing indexed text.
    pub fn property(&self) -> &str {
        &self.property
    }

    /// Optional multitenant partition property.
    pub fn tenant_property(&self) -> Option<&str> {
        self.tenant_property.as_deref()
    }

    /// Analyzer preset.
    pub const fn analyzer(&self) -> TextAnalyzerKind {
        self.analyzer
    }

    /// Whether to record positions.
    pub const fn positions_enabled(&self) -> bool {
        self.positions_enabled
    }

    /// Return the lookup key for this definition.
    pub fn key(&self) -> (TextElementType, String, String) {
        (self.element_type, self.label.clone(), self.property.clone())
    }

    /// Create a new node text index definition.
    pub fn new_node(label: impl Into<String>, property: impl Into<String>) -> ConfigResult<Self> {
        Self::new(TextElementType::Node, label, property)
    }

    /// Create a new edge text index definition.
    pub fn new_edge(label: impl Into<String>, property: impl Into<String>) -> ConfigResult<Self> {
        Self::new(TextElementType::Edge, label, property)
    }

    fn new(
        element_type: TextElementType,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        Ok(Self {
            element_type,
            label: parse_index_component("text index label", label)?,
            property: parse_index_component("text index property", property)?,
            tenant_property: None,
            analyzer: TextAnalyzerKind::Standard,
            positions_enabled: false,
        })
    }

    /// Set the multitenant partition property.
    pub fn with_tenant_property(
        mut self,
        tenant_property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.tenant_property = Some(parse_index_tenant_property(tenant_property)?);
        Ok(self)
    }

    /// Replace the multitenant partition property.
    pub fn with_tenant_property_option(
        mut self,
        tenant_property: Option<impl Into<String>>,
    ) -> ConfigResult<Self> {
        self.tenant_property = tenant_property
            .map(parse_index_tenant_property)
            .transpose()?;
        Ok(self)
    }

    /// Set the analyzer preset.
    pub fn with_analyzer(mut self, analyzer: TextAnalyzerKind) -> Self {
        self.analyzer = analyzer;
        self
    }

    /// Enable or disable term positions.
    pub fn with_positions_enabled(mut self, enabled: bool) -> Self {
        self.positions_enabled = enabled;
        self
    }
}

/// Vector index definition for automatic creation on open
#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndexDefinition {
    element_type: VectorElementType,
    label: String,
    property: String,
    tenant_property: Option<String>,
    dimension: NonZeroUsize,
    /// Distance metric (must be explicit)
    metric: VectorDistanceMetric,
    /// HNSW parameter: max connections per node per layer
    m: Connections,
    /// HNSW parameter: max connections for layer 0
    m0: Layer0Connections,
    /// HNSW parameter: candidate list size during construction
    ef_construction: ConstructionBeamWidth,
    /// HNSW level multiplier controlling layer selection
    ///
    /// Default: `1.0 / ln(m)` (for default `m=16`, approx `0.36`).
    ml: LayerMultiplier,
    /// SimHash collision threshold for filtering
    simhash_threshold: CollisionThreshold,
    /// Sampling ratio for layer 0 neighbors
    sampling_ratio: UnitInterval,

    /// Enable adaptive section-3.3 traversal behavior (default: true)
    adaptive_enabled: bool,

    /// Failure probability for adaptive Hoeffding-style thresholding.
    ///
    /// Lower values are more conservative (looser filtering, higher recall,
    /// potentially more I/O). Higher values are stricter (tighter filtering,
    /// lower I/O potential).
    /// Default: 0.1. Invalid values are rejected.
    adaptive_failure_prob: FailureProbability,
}

impl VectorIndexDefinition {
    /// Element type (node or edge)
    pub const fn element_type(&self) -> VectorElementType {
        self.element_type
    }

    /// Label to scope the index
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Property name containing the vectors
    pub fn property(&self) -> &str {
        &self.property
    }

    /// Optional multitenant partition property.
    pub fn tenant_property(&self) -> Option<&str> {
        self.tenant_property.as_deref()
    }

    /// Vector dimension
    pub const fn dimension(&self) -> usize {
        self.dimension.get()
    }

    /// Distance metric.
    pub const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// HNSW parameter: max connections per node per layer.
    pub const fn m(&self) -> usize {
        self.m.get()
    }

    /// HNSW parameter: max connections for layer 0.
    pub const fn m0(&self) -> usize {
        self.m0.get()
    }

    /// HNSW parameter: candidate list size during construction.
    pub const fn ef_construction(&self) -> usize {
        self.ef_construction.get()
    }

    /// HNSW level multiplier controlling layer selection.
    pub const fn ml(&self) -> f32 {
        self.ml.get()
    }

    /// SimHash collision threshold for filtering.
    pub const fn simhash_threshold(&self) -> usize {
        self.simhash_threshold.get()
    }

    /// Sampling ratio for layer 0 neighbors.
    pub const fn sampling_ratio(&self) -> f32 {
        self.sampling_ratio.get()
    }

    /// Whether adaptive layer-0 traversal is enabled.
    pub const fn adaptive_enabled(&self) -> bool {
        self.adaptive_enabled
    }

    /// Failure probability for adaptive thresholding.
    pub const fn adaptive_failure_prob(&self) -> f32 {
        self.adaptive_failure_prob.get()
    }

    /// Return the lookup key for this definition
    pub fn key(&self) -> (VectorElementType, String, String) {
        (self.element_type, self.label.clone(), self.property.clone())
    }

    /// Create a new node vector index definition
    pub fn new_node(
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: VectorDistanceMetric,
    ) -> ConfigResult<Self> {
        Self::new(VectorElementType::Node, label, property, dimension, metric)
    }

    /// Create a new edge vector index definition
    pub fn new_edge(
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: VectorDistanceMetric,
    ) -> ConfigResult<Self> {
        Self::new(VectorElementType::Edge, label, property, dimension, metric)
    }

    fn new(
        element_type: VectorElementType,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: VectorDistanceMetric,
    ) -> ConfigResult<Self> {
        let m = Connections::try_new(16).expect("default vector connections are nonzero");
        Ok(Self {
            element_type,
            label: parse_index_component("vector index label", label)?,
            property: parse_index_component("vector index property", property)?,
            tenant_property: None,
            dimension: NonZeroUsize::new(dimension)
                .ok_or_else(|| ConfigError::new("vector index dimension must be positive"))?,
            metric,
            m,
            m0: m
                .checked_double()
                .map_err(|error| ConfigError::new(error.to_string()))?,
            ef_construction: ConstructionBeamWidth::try_new(200, m)
                .map_err(|error| ConfigError::new(error.to_string()))?,
            ml: LayerMultiplier::try_new(default_ml_for_m(m.get()))
                .map_err(|error| ConfigError::new(error.to_string()))?,
            simhash_threshold: CollisionThreshold::try_new(
                DEFAULT_SIMHASH_COLLISION_THRESHOLD,
                NonZeroUsize::new(SIMHASH_BITS).expect("SimHash width is nonzero"),
            )
            .map_err(|error| ConfigError::new(error.to_string()))?,
            sampling_ratio: UnitInterval::try_new(0.8)
                .map_err(|error| ConfigError::new(error.to_string()))?,
            adaptive_enabled: true,
            adaptive_failure_prob: FailureProbability::try_new(0.1)
                .map_err(|error| ConfigError::new(error.to_string()))?,
        })
    }

    /// Set HNSW M and reject values that contradict dependent tuning.
    pub fn with_m(mut self, m: usize) -> ConfigResult<Self> {
        let m = Connections::try_new(m).map_err(|error| ConfigError::new(error.to_string()))?;
        let m0 = Layer0Connections::try_new(self.m0.get(), m)
            .map_err(|error| ConfigError::new(error.to_string()))?;
        let ef_construction = ConstructionBeamWidth::try_new(self.ef_construction.get(), m)
            .map_err(|error| ConfigError::new(error.to_string()))?;
        self.m = m;
        self.m0 = m0;
        self.ef_construction = ef_construction;
        self.ml = LayerMultiplier::try_new(default_ml_for_m(m.get()))
            .map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }

    /// Set optional multitenant partition property.
    pub fn with_tenant_property(
        mut self,
        tenant_property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.tenant_property = Some(parse_index_tenant_property(tenant_property)?);
        Ok(self)
    }

    /// Replace the multitenant partition property.
    pub fn with_tenant_property_option(
        mut self,
        tenant_property: Option<impl Into<String>>,
    ) -> ConfigResult<Self> {
        self.tenant_property = tenant_property
            .map(parse_index_tenant_property)
            .transpose()?;
        Ok(self)
    }

    /// Set HNSW level multiplier `ml`.
    ///
    /// Values must be finite and greater than zero.
    pub fn with_ml(mut self, ml: f32) -> ConfigResult<Self> {
        self.ml =
            LayerMultiplier::try_new(ml).map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }

    /// Set HNSW M0 parameter (layer 0 max connections)
    pub fn with_m0(mut self, m0: usize) -> ConfigResult<Self> {
        self.m0 = Layer0Connections::try_new(m0, self.m)
            .map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }

    /// Set HNSW ef_construction parameter
    pub fn with_ef_construction(mut self, ef_construction: usize) -> ConfigResult<Self> {
        self.ef_construction = ConstructionBeamWidth::try_new(ef_construction, self.m)
            .map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }

    /// Set SimHash threshold (number of matching bits required)
    pub fn with_simhash_threshold(mut self, threshold: usize) -> ConfigResult<Self> {
        self.simhash_threshold = CollisionThreshold::try_new(
            threshold,
            NonZeroUsize::new(SIMHASH_BITS).expect("SimHash width is nonzero"),
        )
        .map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }

    /// Set sampling ratio for layer 0 neighbors
    pub fn with_sampling_ratio(mut self, ratio: f32) -> ConfigResult<Self> {
        self.sampling_ratio =
            UnitInterval::try_new(ratio).map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }

    /// Enable/disable adaptive layer-0 traversal behavior.
    ///
    /// When disabled, traversal uses static sampling ratio and SimHash
    /// threshold values without section-3.3 adaptation.
    pub fn with_adaptive_enabled(mut self, enabled: bool) -> Self {
        self.adaptive_enabled = enabled;
        self
    }

    /// Set failure probability for adaptive Hoeffding-style thresholding.
    ///
    /// Values must be finite and strictly between zero and one.
    pub fn with_adaptive_failure_prob(mut self, failure_prob: f32) -> ConfigResult<Self> {
        self.adaptive_failure_prob = FailureProbability::try_new(failure_prob)
            .map_err(|error| ConfigError::new(error.to_string()))?;
        Ok(self)
    }
}

/// Secondary index element type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexElementType {
    /// Index over node properties.
    Node,
    /// Index over edge properties.
    Edge,
}

/// Secondary index kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexKind {
    /// Equality index.
    Equality,
    /// Range index.
    Range,
}

/// Physical direction for range-index storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum RangeIndexDirection {
    /// Store range keys in ascending value order.
    #[default]
    Asc,
    /// Store range keys in descending value order.
    Desc,
}

/// Dynamic secondary index definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecondaryIndexDefinition {
    /// Node equality index, optionally unique.
    NodeEquality {
        /// Node label whose property values are indexed.
        label: String,
        /// Property name indexed for equality.
        property: String,
        /// Whether one value may belong to at most one node.
        unique: bool,
    },
    /// Node range index.
    NodeRange {
        /// Node label whose property values are indexed.
        label: String,
        /// Property name indexed for ordered range access.
        property: String,
        /// Physical ordering used by range rows.
        direction: RangeIndexDirection,
    },
    /// Edge equality index.
    EdgeEquality {
        /// Edge label whose property values are indexed.
        label: String,
        /// Property name indexed for equality.
        property: String,
    },
    /// Edge range index.
    EdgeRange {
        /// Edge label whose property values are indexed.
        label: String,
        /// Property name indexed for ordered range access.
        property: String,
        /// Physical ordering used by range rows.
        direction: RangeIndexDirection,
    },
}

impl SecondaryIndexDefinition {
    /// Create a node equality index definition.
    pub fn node_equality(
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        Ok(Self::NodeEquality {
            label: parse_index_component("secondary index label", label)?,
            property: parse_index_component("secondary index property", property)?,
            unique: false,
        })
    }

    /// Create a unique node equality index definition.
    pub fn node_unique_equality(
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        Ok(Self::NodeEquality {
            label: parse_index_component("secondary index label", label)?,
            property: parse_index_component("secondary index property", property)?,
            unique: true,
        })
    }

    /// Create a node range index definition.
    pub fn node_range(label: impl Into<String>, property: impl Into<String>) -> ConfigResult<Self> {
        Self::node_range_with_direction(label, property, RangeIndexDirection::Asc)
    }

    /// Create a descending node range index definition.
    pub fn node_range_desc(
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        Self::node_range_with_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Create a node range index definition with explicit direction.
    pub fn node_range_with_direction(
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> ConfigResult<Self> {
        Ok(Self::NodeRange {
            label: parse_index_component("secondary index label", label)?,
            property: parse_index_component("secondary index property", property)?,
            direction,
        })
    }

    /// Create an edge equality index definition.
    pub fn edge_equality(
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        Ok(Self::EdgeEquality {
            label: parse_index_component("secondary index label", label)?,
            property: parse_index_component("secondary index property", property)?,
        })
    }

    /// Create an edge range index definition.
    pub fn edge_range(label: impl Into<String>, property: impl Into<String>) -> ConfigResult<Self> {
        Self::edge_range_with_direction(label, property, RangeIndexDirection::Asc)
    }

    /// Create a descending edge range index definition.
    pub fn edge_range_desc(
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        Self::edge_range_with_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Create an edge range index definition with explicit direction.
    pub fn edge_range_with_direction(
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> ConfigResult<Self> {
        Ok(Self::EdgeRange {
            label: parse_index_component("secondary index label", label)?,
            property: parse_index_component("secondary index property", property)?,
            direction,
        })
    }

    /// Whether the index applies to nodes or edges.
    pub const fn element_type(&self) -> SecondaryIndexElementType {
        match self {
            Self::NodeEquality { .. } | Self::NodeRange { .. } => SecondaryIndexElementType::Node,
            Self::EdgeEquality { .. } | Self::EdgeRange { .. } => SecondaryIndexElementType::Edge,
        }
    }

    /// Whether the index is equality or range.
    pub const fn kind(&self) -> SecondaryIndexKind {
        match self {
            Self::NodeEquality { .. } | Self::EdgeEquality { .. } => SecondaryIndexKind::Equality,
            Self::NodeRange { .. } | Self::EdgeRange { .. } => SecondaryIndexKind::Range,
        }
    }

    /// Label scope for the indexed element.
    pub fn label(&self) -> &str {
        match self {
            Self::NodeEquality { label, .. }
            | Self::NodeRange { label, .. }
            | Self::EdgeEquality { label, .. }
            | Self::EdgeRange { label, .. } => label,
        }
    }

    /// Indexed property name.
    pub fn property(&self) -> &str {
        match self {
            Self::NodeEquality { property, .. }
            | Self::NodeRange { property, .. }
            | Self::EdgeEquality { property, .. }
            | Self::EdgeRange { property, .. } => property,
        }
    }

    /// Whether this equality index enforces uniqueness for supported non-null values.
    pub const fn unique(&self) -> bool {
        match self {
            Self::NodeEquality { unique, .. } => *unique,
            Self::NodeRange { .. } | Self::EdgeEquality { .. } | Self::EdgeRange { .. } => false,
        }
    }

    /// Physical range-index ordering. Equality indexes always use `Asc`.
    pub const fn direction(&self) -> RangeIndexDirection {
        match self {
            Self::NodeRange { direction, .. } | Self::EdgeRange { direction, .. } => *direction,
            Self::NodeEquality { .. } | Self::EdgeEquality { .. } => RangeIndexDirection::Asc,
        }
    }

    /// Return the internal scoped property key used by secondary-index storage.
    pub fn scoped_property(&self) -> String {
        scoped_secondary_index_property(self.label(), self.property())
    }

    /// Return a human-readable name for the scoped property.
    pub fn display_scope(&self) -> String {
        format!("{}.{}", self.label(), self.property())
    }

    /// Whether this definition is a node equality index.
    pub fn is_node_equality(&self) -> bool {
        matches!(self, Self::NodeEquality { .. })
    }

    /// Whether this definition is a unique node equality index.
    pub fn is_unique_node_equality(&self) -> bool {
        matches!(self, Self::NodeEquality { unique: true, .. })
    }

    /// Whether this definition applies to node properties.
    pub fn is_node(&self) -> bool {
        self.element_type() == SecondaryIndexElementType::Node
    }

    /// Whether this definition applies to edge properties.
    pub fn is_edge(&self) -> bool {
        self.element_type() == SecondaryIndexElementType::Edge
    }
}

/// Build the internal property key used for label-scoped secondary indexes.
pub fn scoped_secondary_index_property(label: &str, property: &str) -> String {
    let mut scoped = String::with_capacity(label.len() + property.len() + 1);
    scoped.push_str(label);
    scoped.push(SECONDARY_INDEX_SCOPE_SEPARATOR);
    scoped.push_str(property);
    scoped
}

/// Split an internal label-scoped secondary-index key into label and property.
pub fn split_scoped_secondary_index_property(property: &str) -> Option<(&str, &str)> {
    let (label, property) = property.split_once(SECONDARY_INDEX_SCOPE_SEPARATOR)?;
    if label.is_empty() || property.is_empty() {
        return None;
    }

    Some((label, property))
}

/// Check whether an internal secondary-index key is label scoped.
pub fn is_scoped_secondary_index_property(property: &str) -> bool {
    split_scoped_secondary_index_property(property).is_some()
}

/// Effective runtime index catalog.
///
/// This is not a TOML/user-settings type. It is built from built-in index
/// defaults plus persisted dynamic index metadata during DB open. It stores the
/// full DB-facing definitions once and projects planner-facing snapshots from
/// that state.
///
/// ```
/// # use db::config::RuntimeIndexCatalog;
/// # use db::search::vector::VectorDistanceMetric;
/// # use helix_planner::catalog::ScopedPropertyKey;
/// let catalog = RuntimeIndexCatalog::new()
///     .with_equality_index("User", "email").unwrap()
///     .with_node_vector_index("Doc", "embedding", 3, VectorDistanceMetric::Cosine).unwrap();
/// let snapshot = catalog.planner_snapshot();
/// let email = ScopedPropertyKey::try_new("User", "email").unwrap();
/// assert!(snapshot.node_eq.contains_key(&email));
/// assert_eq!(snapshot.vector.len(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct RuntimeIndexCatalog {
    /// Node equality indexes keyed by `(label, property)`.
    node_equality: HashMap<catalog::ScopedPropertyKey, catalog::IndexUniqueness>,

    /// Node range indexes keyed by `(label, property, direction)`.
    node_range: HashSet<catalog::ScopedPropertyDirectionKey>,

    /// Edge equality indexes keyed by `(label, property)`.
    edge_equality: HashSet<catalog::ScopedPropertyKey>,

    /// Edge range indexes keyed by `(label, property, direction)`.
    edge_range: HashSet<catalog::ScopedPropertyDirectionKey>,

    /// Runtime vector indexes.
    vector_indexes: Vec<VectorIndexDefinition>,

    /// Runtime text indexes.
    text_indexes: Vec<TextIndexDefinition>,
}

/// Backwards-compatible name for the DB-owned runtime index catalog.
pub type IndexConfig = RuntimeIndexCatalog;

impl RuntimeIndexCatalog {
    /// Create the built-in runtime index state.
    pub fn new() -> Self {
        Self {
            node_equality: HashMap::new(),
            node_range: HashSet::new(),
            edge_equality: HashSet::new(),
            edge_range: HashSet::new(),
            vector_indexes: Vec::new(),
            text_indexes: Vec::new(),
        }
    }
}

impl Default for RuntimeIndexCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeIndexCatalog {
    fn insert_vector_definition(&mut self, definition: VectorIndexDefinition) {
        if self.vector_indexes.iter().any(|existing| {
            existing.element_type() == definition.element_type()
                && existing.label() == definition.label()
                && existing.property() == definition.property()
        }) {
            return;
        }
        self.vector_indexes.push(definition);
    }

    fn insert_text_definition(&mut self, definition: TextIndexDefinition) {
        if self.text_indexes.iter().any(|existing| {
            existing.element_type() == definition.element_type()
                && existing.label() == definition.label()
                && existing.property() == definition.property()
        }) {
            return;
        }
        self.text_indexes.push(definition);
    }

    /// Add a dynamic index loaded from metadata or created by DDL.
    pub(crate) fn insert_dynamic_index(&mut self, definition: &ValidatedDynamicIndexDefinition) {
        match definition {
            ValidatedDynamicIndexDefinition::Secondary(definition) => {
                let definition = definition.to_runtime();
                match (
                    definition.element_type(),
                    definition.kind(),
                    definition.direction(),
                ) {
                    (
                        SecondaryIndexElementType::Node,
                        SecondaryIndexKind::Equality,
                        RangeIndexDirection::Asc,
                    ) => {
                        let key = scoped_property_key(definition.label(), definition.property())
                            .expect("validated node equality index has a typed key");
                        let uniqueness = if definition.unique() {
                            catalog::IndexUniqueness::Unique
                        } else {
                            catalog::IndexUniqueness::NonUnique
                        };
                        self.node_equality.insert(key, uniqueness);
                    }
                    (SecondaryIndexElementType::Node, SecondaryIndexKind::Range, direction) => {
                        let key = scoped_property_direction_key(
                            definition.label(),
                            definition.property(),
                            direction,
                        )
                        .expect("validated node range index has a typed key");
                        self.node_range.insert(key);
                    }
                    (
                        SecondaryIndexElementType::Edge,
                        SecondaryIndexKind::Equality,
                        RangeIndexDirection::Asc,
                    ) => {
                        let key = scoped_property_key(definition.label(), definition.property())
                            .expect("validated edge equality index has a typed key");
                        self.edge_equality.insert(key);
                    }
                    (SecondaryIndexElementType::Edge, SecondaryIndexKind::Range, direction) => {
                        let key = scoped_property_direction_key(
                            definition.label(),
                            definition.property(),
                            direction,
                        )
                        .expect("validated edge range index has a typed key");
                        self.edge_range.insert(key);
                    }
                    (_, SecondaryIndexKind::Equality, RangeIndexDirection::Desc) => {
                        unreachable!("secondary equality indexes always use ascending direction")
                    }
                }
            }
            ValidatedDynamicIndexDefinition::Vector(definition) => {
                self.insert_vector_definition(definition.to_runtime());
            }
            ValidatedDynamicIndexDefinition::Text(definition) => {
                self.insert_text_definition(definition.to_runtime());
            }
        }
    }

    /// Build the planner-visible index snapshot from the DB runtime catalog.
    pub fn planner_snapshot(&self) -> catalog::IndexCatalogSnapshot {
        let mut snapshot = catalog::IndexCatalogSnapshot::default();
        snapshot
            .node_eq
            .extend(self.node_equality.iter().map(|(key, uniqueness)| {
                (
                    key.clone(),
                    catalog::NodeEqualityIndexMeta::new(ir::NonEmptyString::from_prefixed_display(
                        "node_eq:", key,
                    ))
                    .with_uniqueness(*uniqueness),
                )
            }));
        snapshot
            .node_range
            .extend(self.node_range.iter().map(|key| {
                (
                    key.clone(),
                    catalog::NodeRangeIndexMeta::new(ir::NonEmptyString::from_prefixed_display(
                        "node_range:",
                        key,
                    )),
                )
            }));
        snapshot
            .edge_eq
            .extend(self.edge_equality.iter().map(|key| {
                (
                    key.clone(),
                    catalog::EdgeEqualityIndexMeta::new(ir::NonEmptyString::from_prefixed_display(
                        "edge_eq:", key,
                    )),
                )
            }));
        snapshot
            .edge_range
            .extend(self.edge_range.iter().map(|key| {
                (
                    key.clone(),
                    catalog::EdgeRangeIndexMeta::new(ir::NonEmptyString::from_prefixed_display(
                        "edge_range:",
                        key,
                    )),
                )
            }));
        snapshot
            .vector
            .extend(self.vector_indexes.iter().filter_map(|definition| {
                let key = catalog::SearchIndexKey::try_new(
                    match definition.element_type() {
                        VectorElementType::Node => catalog::ElementKind::Node,
                        VectorElementType::Edge => catalog::ElementKind::Edge,
                    },
                    definition.label(),
                    definition.property(),
                )?;
                let scope = catalog::SearchIndexScope::try_new(definition.tenant_property())?;
                Some((
                    key.clone(),
                    catalog::VectorIndexMeta::new(
                        ir::NonEmptyString::from_prefixed_display("vector:", key),
                        scope,
                    ),
                ))
            }));
        snapshot
            .text
            .extend(self.text_indexes.iter().filter_map(|definition| {
                let key = catalog::SearchIndexKey::try_new(
                    match definition.element_type() {
                        TextElementType::Node => catalog::ElementKind::Node,
                        TextElementType::Edge => catalog::ElementKind::Edge,
                    },
                    definition.label(),
                    definition.property(),
                )?;
                let scope = catalog::SearchIndexScope::try_new(definition.tenant_property())?;
                Some((
                    key.clone(),
                    catalog::TextIndexMeta::new(
                        ir::NonEmptyString::from_prefixed_display("text:", key),
                        scope,
                    ),
                ))
            }));
        snapshot
    }

    /// Iterate label-scoped node equality index keys.
    pub fn node_equality_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.node_equality
            .keys()
            .map(|key| scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref()))
    }

    /// Iterate label-scoped unique node equality index keys.
    pub fn node_unique_equality_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.node_equality
            .iter()
            .filter(|(_key, uniqueness)| **uniqueness == catalog::IndexUniqueness::Unique)
            .map(|(key, _uniqueness)| {
                scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref())
            })
    }

    /// Iterate label-scoped node ascending range index keys.
    pub fn node_range_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.node_range
            .iter()
            .filter(|key| key.direction == PlannerRangeIndexDirection::Asc)
            .map(|key| scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref()))
    }

    /// Iterate label-scoped node descending range index keys.
    pub fn node_range_desc_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.node_range
            .iter()
            .filter(|key| key.direction == PlannerRangeIndexDirection::Desc)
            .map(|key| scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref()))
    }

    /// Iterate label-scoped edge equality index keys.
    pub fn edge_equality_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.edge_equality
            .iter()
            .map(|key| scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref()))
    }

    /// Iterate label-scoped edge ascending range index keys.
    pub fn edge_range_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.edge_range
            .iter()
            .filter(|key| key.direction == PlannerRangeIndexDirection::Asc)
            .map(|key| scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref()))
    }

    /// Iterate label-scoped edge descending range index keys.
    pub fn edge_range_desc_indexes(&self) -> impl Iterator<Item = String> + '_ {
        self.edge_range
            .iter()
            .filter(|key| key.direction == PlannerRangeIndexDirection::Desc)
            .map(|key| scoped_secondary_index_property(key.label.as_ref(), key.property.as_ref()))
    }

    /// Iterate runtime vector index definitions.
    pub fn vector_indexes(&self) -> impl Iterator<Item = &VectorIndexDefinition> {
        self.vector_indexes.iter()
    }

    /// Iterate runtime text index definitions.
    pub fn text_indexes(&self) -> impl Iterator<Item = &TextIndexDefinition> {
        self.text_indexes.iter()
    }

    /// Check an already-scoped node equality key.
    pub fn contains_node_equality_scoped(&self, scoped: &str) -> bool {
        let Some(key) = scoped_property_key_from_storage(scoped) else {
            return false;
        };
        self.node_equality.contains_key(&key)
    }

    /// Check an already-scoped unique node equality key.
    pub fn contains_node_unique_equality_scoped(&self, scoped: &str) -> bool {
        let Some(key) = scoped_property_key_from_storage(scoped) else {
            return false;
        };
        self.node_equality
            .get(&key)
            .is_some_and(|uniqueness| *uniqueness == catalog::IndexUniqueness::Unique)
    }

    /// Check an already-scoped node ascending range key.
    pub fn contains_node_range_scoped(&self, scoped: &str) -> bool {
        let Some(key) =
            scoped_property_direction_key_from_storage(scoped, RangeIndexDirection::Asc)
        else {
            return false;
        };
        self.node_range.contains(&key)
    }

    /// Check an already-scoped node descending range key.
    pub fn contains_node_range_desc_scoped(&self, scoped: &str) -> bool {
        let Some(key) =
            scoped_property_direction_key_from_storage(scoped, RangeIndexDirection::Desc)
        else {
            return false;
        };
        self.node_range.contains(&key)
    }

    /// Check an already-scoped edge equality key.
    pub fn contains_edge_equality_scoped(&self, scoped: &str) -> bool {
        let Some(key) = scoped_property_key_from_storage(scoped) else {
            return false;
        };
        self.edge_equality.contains(&key)
    }

    /// Check an already-scoped edge ascending range key.
    pub fn contains_edge_range_scoped(&self, scoped: &str) -> bool {
        let Some(key) =
            scoped_property_direction_key_from_storage(scoped, RangeIndexDirection::Asc)
        else {
            return false;
        };
        self.edge_range.contains(&key)
    }

    /// Check an already-scoped edge descending range key.
    pub fn contains_edge_range_desc_scoped(&self, scoped: &str) -> bool {
        let Some(key) =
            scoped_property_direction_key_from_storage(scoped, RangeIndexDirection::Desc)
        else {
            return false;
        };
        self.edge_range.contains(&key)
    }

    /// Add a label-scoped node property to the equality index
    ///
    /// Properties with equality indexes support fast lookups when the traversal
    /// has a known node label scope.
    ///
    /// # Example
    /// ```
    /// use db::config::{scoped_secondary_index_property, RuntimeIndexCatalog};
    ///
    /// let config = RuntimeIndexCatalog::new()
    ///     .with_equality_index("User", "status")?
    ///     .with_equality_index("Project", "status")?;
    /// let status = scoped_secondary_index_property("User", "status");
    /// assert!(config.contains_node_equality_scoped(&status));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_equality_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.node_equality
            .entry(scoped_property_key(label, property)?)
            .or_insert(catalog::IndexUniqueness::NonUnique);
        Ok(self)
    }

    /// Add a label-scoped node property to the range index
    ///
    /// Properties with range indexes support fast lookups when the traversal has
    /// a known node label scope.
    ///
    /// # Example
    /// ```
    /// use db::config::{scoped_secondary_index_property, RuntimeIndexCatalog};
    ///
    /// let config = RuntimeIndexCatalog::new()
    ///     .with_range_index("User", "score")?
    ///     .with_range_index("Event", "timestamp")?;
    /// let score = scoped_secondary_index_property("User", "score");
    /// assert!(config.contains_node_range_scoped(&score));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_range_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self = self.with_range_index_direction(label, property, RangeIndexDirection::Asc)?;
        Ok(self)
    }

    /// Add a label-scoped node property to the descending range index.
    pub fn with_range_desc_index(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.with_range_index_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Add a label-scoped node property to a range index with explicit direction.
    pub fn with_range_index_direction(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> ConfigResult<Self> {
        self.node_range
            .insert(scoped_property_direction_key(label, property, direction)?);
        Ok(self)
    }

    /// Check if the global node label index is enabled.
    pub fn has_node_label_index(&self) -> bool {
        true
    }

    /// Check if a label-scoped node property has an equality index.
    pub fn has_scoped_equality_index(&self, label: &str, property: &str) -> bool {
        let Some(key) = catalog::ScopedPropertyKey::try_new(label, property) else {
            return false;
        };
        self.node_equality.contains_key(&key)
    }

    /// Check if a label-scoped node property has a range index.
    pub fn has_scoped_range_index(&self, label: &str, property: &str) -> bool {
        self.has_scoped_range_index_direction(label, property, RangeIndexDirection::Asc)
    }

    /// Check if a label-scoped node property has a range index in the direction.
    pub fn has_scoped_range_index_direction(
        &self,
        label: &str,
        property: &str,
        direction: RangeIndexDirection,
    ) -> bool {
        let Some(key) = catalog::ScopedPropertyDirectionKey::try_new(
            label,
            property,
            planner_range_direction(direction),
        ) else {
            return false;
        };
        self.node_range.contains(&key)
    }

    /// Check if a label-scoped node property has any range index.
    pub fn has_any_scoped_range_index(&self, label: &str, property: &str) -> bool {
        self.has_scoped_range_index_direction(label, property, RangeIndexDirection::Asc)
            || self.has_scoped_range_index_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Add a label-scoped edge property to the equality index
    ///
    /// Properties with edge equality indexes support fast lookups when the
    /// traversal has a known edge label scope. Default still includes `$label`
    /// for label lookups.
    pub fn with_edge_equality_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.edge_equality
            .insert(scoped_property_key(label, property)?);
        Ok(self)
    }

    /// Add a label-scoped edge property to the range index
    ///
    /// Properties with edge range indexes support fast range queries when the
    /// traversal has a known edge label scope.
    pub fn with_edge_range_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self = self.with_edge_range_index_direction(label, property, RangeIndexDirection::Asc)?;
        Ok(self)
    }

    /// Add a label-scoped edge property to the descending range index.
    pub fn with_edge_range_desc_index(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.with_edge_range_index_direction(label, property, RangeIndexDirection::Desc)
    }

    /// Add a label-scoped edge property to a range index with explicit direction.
    pub fn with_edge_range_index_direction(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
        direction: RangeIndexDirection,
    ) -> ConfigResult<Self> {
        self.edge_range
            .insert(scoped_property_direction_key(label, property, direction)?);
        Ok(self)
    }

    /// Check if the global edge label index is enabled.
    pub fn has_edge_label_index(&self) -> bool {
        true
    }

    /// Check if a label-scoped edge property has an equality index.
    pub fn has_scoped_edge_equality_index(&self, label: &str, property: &str) -> bool {
        let Some(key) = catalog::ScopedPropertyKey::try_new(label, property) else {
            return false;
        };
        self.edge_equality.contains(&key)
    }

    /// Check if a label-scoped edge property has a range index.
    pub fn has_scoped_edge_range_index(&self, label: &str, property: &str) -> bool {
        self.has_scoped_edge_range_index_direction(label, property, RangeIndexDirection::Asc)
    }

    /// Check if a label-scoped edge property has a range index in the direction.
    pub fn has_scoped_edge_range_index_direction(
        &self,
        label: &str,
        property: &str,
        direction: RangeIndexDirection,
    ) -> bool {
        let Some(key) = catalog::ScopedPropertyDirectionKey::try_new(
            label,
            property,
            planner_range_direction(direction),
        ) else {
            return false;
        };
        self.edge_range.contains(&key)
    }

    /// Check if a label-scoped edge property has any range index.
    pub fn has_any_scoped_edge_range_index(&self, label: &str, property: &str) -> bool {
        self.has_scoped_edge_range_index_direction(label, property, RangeIndexDirection::Asc)
            || self.has_scoped_edge_range_index_direction(
                label,
                property,
                RangeIndexDirection::Desc,
            )
    }

    /// Add a node vector index (metric is required)
    pub fn with_node_vector_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: VectorDistanceMetric,
    ) -> ConfigResult<Self> {
        self.insert_vector_definition(VectorIndexDefinition::new_node(
            label, property, dimension, metric,
        )?);
        Ok(self)
    }

    /// Add an edge vector index (metric is required)
    pub fn with_edge_vector_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: VectorDistanceMetric,
    ) -> ConfigResult<Self> {
        self.insert_vector_definition(VectorIndexDefinition::new_edge(
            label, property, dimension, metric,
        )?);
        Ok(self)
    }

    /// Add a vector index definition with custom parameters
    pub fn with_vector_index(mut self, definition: VectorIndexDefinition) -> Self {
        self.insert_vector_definition(definition);
        self
    }

    /// Add a node text index using the default analyzer.
    pub fn with_node_text_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.insert_text_definition(TextIndexDefinition::new_node(label, property)?);
        Ok(self)
    }

    /// Add an edge text index using the default analyzer.
    pub fn with_edge_text_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> ConfigResult<Self> {
        self.insert_text_definition(TextIndexDefinition::new_edge(label, property)?);
        Ok(self)
    }

    /// Add a text index definition with custom parameters.
    pub fn with_text_index(mut self, definition: TextIndexDefinition) -> Self {
        self.insert_text_definition(definition);
        self
    }
}
