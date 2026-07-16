//! Valid-by-construction logical records for the V2 index namespace.

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use uuid::Uuid;

use crate::config::{
    RangeIndexDirection, SecondaryIndexDefinition, TextAnalyzerKind, TextElementType,
    TextIndexDefinition, VectorElementType, VectorIndexDefinition,
};
use crate::search::vector::{VectorDistanceMetric, SIMHASH_BITS};

/// Maximum UTF-8 byte length of an index label, property, or tenant property.
pub const INDEX_COMPONENT_MAX_LEN: usize = u16::MAX as usize;

/// Failure to construct or transition a canonical V2 model value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexV2ModelError {
    /// A logical component is empty.
    #[error("index {kind} must not be empty")]
    EmptyComponent {
        /// Component role used in diagnostics.
        kind: &'static str,
    },
    /// A logical component exceeds the frozen V2 bound.
    #[error("index {kind} is {actual} bytes; maximum is {maximum}")]
    OversizedComponent {
        /// Component role used in diagnostics.
        kind: &'static str,
        /// Encoded UTF-8 byte length.
        actual: usize,
        /// Frozen maximum.
        maximum: usize,
    },
    /// A numeric identity or revision is zero.
    #[error("{kind} must be non-zero")]
    ZeroIdentifier {
        /// Numeric type name.
        kind: &'static str,
    },
    /// A checked numeric increment exhausted its durable domain.
    #[error("{kind} is exhausted")]
    IdentifierExhausted {
        /// Numeric type name.
        kind: &'static str,
    },
    /// A UUID-backed identity is nil.
    #[error("{kind} must be a non-nil UUID")]
    NilUuid {
        /// UUID type name.
        kind: &'static str,
    },
    /// A vector setting is outside its strict V2 domain.
    #[error("invalid V2 vector setting {field}: {reason}")]
    InvalidVectorSetting {
        /// Setting name.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A platform-sized runtime setting cannot fit the frozen V2 integer.
    #[error("V2 vector setting {field} exceeds u32")]
    VectorSettingOverflow {
        /// Setting name.
        field: &'static str,
    },
    /// The logical identity does not match the definition.
    #[error("index identity does not match its validated definition")]
    IdentityDefinitionMismatch,
    /// The physical generation family does not match the definition family.
    #[error("physical generation family does not match the index definition")]
    PhysicalDefinitionMismatch,
    /// A vector descriptor does not match its validated definition.
    #[error("vector physical descriptor does not match its validated definition")]
    VectorDescriptorMismatch,
    /// A vector physical layout does not match its tenant-partition setting.
    #[error("vector physical layout does not match its validated definition")]
    VectorLayoutMismatch,
    /// The requested state transition is illegal.
    #[error("illegal index state transition from {from} using {transition}")]
    IllegalStateTransition {
        /// Current state name.
        from: &'static str,
        /// Requested transition name.
        transition: &'static str,
    },
}

/// Non-empty bounded UTF-8 component shared by V2 identities and definitions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexComponent(String);

impl IndexComponent {
    /// Validates one label/property component against the frozen V2 bound.
    pub fn try_new(
        kind: &'static str,
        value: impl Into<String>,
    ) -> Result<Self, IndexV2ModelError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IndexV2ModelError::EmptyComponent { kind });
        }
        if value.len() > INDEX_COMPONENT_MAX_LEN {
            return Err(IndexV2ModelError::OversizedComponent {
                kind,
                actual: value.len(),
                maximum: INDEX_COMPONENT_MAX_LEN,
            });
        }
        Ok(Self(value))
    }

    /// Borrows the validated UTF-8 value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IndexComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

macro_rules! non_zero_u64_type {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Validated non-zero ", $kind, ".")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            #[doc = concat!("Constructs a validated ", $kind, ".")]
            pub fn new(value: u64) -> Result<Self, IndexV2ModelError> {
                NonZeroU64::new(value)
                    .map(Self)
                    .ok_or(IndexV2ModelError::ZeroIdentifier { kind: $kind })
            }

            #[doc = concat!("Returns the raw ", $kind, ".")]
            pub const fn get(self) -> u64 {
                self.0.get()
            }

            #[doc = concat!("Returns the initial ", $kind, " value.")]
            pub const fn initial() -> Self {
                Self(NonZeroU64::MIN)
            }

            #[doc = concat!("Checked-increments this ", $kind, ".")]
            pub fn checked_next(self) -> Result<Self, IndexV2ModelError> {
                let Some(next) = self.get().checked_add(1) else {
                    return Err(IndexV2ModelError::IdentifierExhausted { kind: $kind });
                };
                Self::new(next)
            }
        }
    };
}

non_zero_u64_type!(IndexId, "index ID");
non_zero_u64_type!(IndexRevision, "index revision");
non_zero_u64_type!(IndexOperationRevision, "index operation revision");
non_zero_u64_type!(IndexGenerationId, "index generation ID");
non_zero_u64_type!(VectorPhysicalIndexId, "vector physical index ID");
non_zero_u64_type!(TextIntentRevision, "text intent revision");
non_zero_u64_type!(TextManifestRevision, "text manifest revision");
non_zero_u64_type!(TextLogicalVersion, "text logical version");
non_zero_u64_type!(BlobGcRunRevision, "blob GC run revision");

/// Graph entity identity retained by lifecycle work.
///
/// Unlike lifecycle IDs and revisions, graph node and edge allocators start at
/// zero. This wrapper therefore accepts the complete `u64` graph-ID domain
/// while still preventing entity IDs from being confused with index IDs.
///
/// ```
/// use db::index_v2::IndexEntityId;
///
/// let first_graph_entity = IndexEntityId::new(0);
/// assert_eq!(first_graph_entity.get(), 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexEntityId(u64);

impl IndexEntityId {
    /// Wraps one graph node or edge ID, including the valid first ID `0`.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the graph ID encoded in V2 keys and values.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the first graph entity ID.
    pub const fn initial() -> Self {
        Self(0)
    }
}

macro_rules! non_nil_uuid_type {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Validated non-nil ", $kind, ".")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Uuid);

        impl $name {
            #[doc = concat!("Constructs a validated ", $kind, ".")]
            pub fn new(value: Uuid) -> Result<Self, IndexV2ModelError> {
                if value.is_nil() {
                    return Err(IndexV2ModelError::NilUuid { kind: $kind });
                }
                Ok(Self(value))
            }

            #[doc = concat!("Generates a random version-4 ", $kind, ".")]
            pub fn new_v4() -> Self {
                Self(Uuid::new_v4())
            }

            #[doc = concat!("Decodes the raw bytes of a ", $kind, ".")]
            pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, IndexV2ModelError> {
                Self::new(Uuid::from_bytes(bytes))
            }

            #[doc = concat!("Returns the raw bytes of this ", $kind, ".")]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                self.0.as_bytes()
            }

            #[doc = concat!("Returns the UUID backing this ", $kind, ".")]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }
    };
}

non_nil_uuid_type!(IndexOperationId, "index operation ID");
non_nil_uuid_type!(BlobPublicationPermitId, "blob publication permit ID");
non_nil_uuid_type!(TextUploadIntentId, "text upload intent ID");
non_nil_uuid_type!(WriterEpoch, "writer epoch");
non_nil_uuid_type!(MutationId, "mutation ID");
non_nil_uuid_type!(BlobGcRunId, "blob GC run ID");

/// Element family owned by an index.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexElementKind {
    /// Node-backed index.
    Node = 0x01,
    /// Edge-backed index.
    Edge = 0x02,
}

/// Logical identity lane. Full settings remain in the validated definition.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexIdentityFamily {
    /// Secondary equality lane.
    SecondaryEquality = 0x01,
    /// Secondary range lane.
    SecondaryRange = 0x02,
    /// Vector lane.
    Vector = 0x03,
    /// Text lane.
    Text = 0x04,
}

/// Scoped logical identity used by the canonical index-record key and value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexIdentity {
    family: IndexIdentityFamily,
    element_kind: IndexElementKind,
    label: IndexComponent,
    property: IndexComponent,
}

impl IndexIdentity {
    /// Constructs a logical identity from already validated components.
    pub const fn new(
        family: IndexIdentityFamily,
        element_kind: IndexElementKind,
        label: IndexComponent,
        property: IndexComponent,
    ) -> Self {
        Self {
            family,
            element_kind,
            label,
            property,
        }
    }

    /// Returns the logical family lane.
    pub const fn family(&self) -> IndexIdentityFamily {
        self.family
    }

    /// Returns the node/edge kind.
    pub const fn element_kind(&self) -> IndexElementKind {
        self.element_kind
    }

    /// Returns the validated label.
    pub const fn label(&self) -> &IndexComponent {
        &self.label
    }

    /// Returns the validated property.
    pub const fn property(&self) -> &IndexComponent {
        &self.property
    }
}

/// Canonical V2 secondary-index definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValidatedSecondaryIndexDefinition {
    /// Node equality lane, optionally unique.
    NodeEquality {
        /// Label scope.
        label: IndexComponent,
        /// Indexed property.
        property: IndexComponent,
        /// Whether values must be unique.
        unique: bool,
    },
    /// Node range lane.
    NodeRange {
        /// Label scope.
        label: IndexComponent,
        /// Indexed property.
        property: IndexComponent,
        /// Physical ordering.
        direction: RangeIndexDirection,
    },
    /// Edge equality lane.
    EdgeEquality {
        /// Label scope.
        label: IndexComponent,
        /// Indexed property.
        property: IndexComponent,
    },
    /// Edge range lane.
    EdgeRange {
        /// Label scope.
        label: IndexComponent,
        /// Indexed property.
        property: IndexComponent,
        /// Physical ordering.
        direction: RangeIndexDirection,
    },
}

impl ValidatedSecondaryIndexDefinition {
    /// Returns the canonical logical identity.
    pub fn identity(&self) -> IndexIdentity {
        let (family, element_kind, label, property) = match self {
            Self::NodeEquality {
                label, property, ..
            } => (
                IndexIdentityFamily::SecondaryEquality,
                IndexElementKind::Node,
                label,
                property,
            ),
            Self::NodeRange {
                label, property, ..
            } => (
                IndexIdentityFamily::SecondaryRange,
                IndexElementKind::Node,
                label,
                property,
            ),
            Self::EdgeEquality { label, property } => (
                IndexIdentityFamily::SecondaryEquality,
                IndexElementKind::Edge,
                label,
                property,
            ),
            Self::EdgeRange {
                label, property, ..
            } => (
                IndexIdentityFamily::SecondaryRange,
                IndexElementKind::Edge,
                label,
                property,
            ),
        };
        IndexIdentity::new(family, element_kind, label.clone(), property.clone())
    }

    /// Returns the node/edge kind.
    pub const fn element_kind(&self) -> IndexElementKind {
        match self {
            Self::NodeEquality { .. } | Self::NodeRange { .. } => IndexElementKind::Node,
            Self::EdgeEquality { .. } | Self::EdgeRange { .. } => IndexElementKind::Edge,
        }
    }

    /// Returns the logical equality/range family lane.
    pub const fn identity_family(&self) -> IndexIdentityFamily {
        match self {
            Self::NodeEquality { .. } | Self::EdgeEquality { .. } => {
                IndexIdentityFamily::SecondaryEquality
            }
            Self::NodeRange { .. } | Self::EdgeRange { .. } => IndexIdentityFamily::SecondaryRange,
        }
    }

    /// Returns the validated label.
    pub const fn label(&self) -> &IndexComponent {
        match self {
            Self::NodeEquality { label, .. }
            | Self::NodeRange { label, .. }
            | Self::EdgeEquality { label, .. }
            | Self::EdgeRange { label, .. } => label,
        }
    }

    /// Returns the validated property.
    pub const fn property(&self) -> &IndexComponent {
        match self {
            Self::NodeEquality { property, .. }
            | Self::NodeRange { property, .. }
            | Self::EdgeEquality { property, .. }
            | Self::EdgeRange { property, .. } => property,
        }
    }

    /// Returns whether this is a unique node-equality definition.
    pub const fn unique(&self) -> bool {
        matches!(self, Self::NodeEquality { unique: true, .. })
    }

    /// Returns range ordering; equality definitions have canonical ascending ordering.
    pub const fn direction(&self) -> RangeIndexDirection {
        match self {
            Self::NodeRange { direction, .. } | Self::EdgeRange { direction, .. } => *direction,
            Self::NodeEquality { .. } | Self::EdgeEquality { .. } => RangeIndexDirection::Asc,
        }
    }

    /// Converts the durable semantic value into the non-persisted runtime adapter.
    pub fn to_runtime(&self) -> SecondaryIndexDefinition {
        match self {
            Self::NodeEquality {
                label,
                property,
                unique: false,
            } => SecondaryIndexDefinition::node_equality(label.as_str(), property.as_str())
                .expect("V2 components satisfy runtime validation"),
            Self::NodeEquality {
                label,
                property,
                unique: true,
            } => SecondaryIndexDefinition::node_unique_equality(label.as_str(), property.as_str())
                .expect("V2 components satisfy runtime validation"),
            Self::NodeRange {
                label,
                property,
                direction,
            } => SecondaryIndexDefinition::node_range_with_direction(
                label.as_str(),
                property.as_str(),
                *direction,
            )
            .expect("V2 components satisfy runtime validation"),
            Self::EdgeEquality { label, property } => {
                SecondaryIndexDefinition::edge_equality(label.as_str(), property.as_str())
                    .expect("V2 components satisfy runtime validation")
            }
            Self::EdgeRange {
                label,
                property,
                direction,
            } => SecondaryIndexDefinition::edge_range_with_direction(
                label.as_str(),
                property.as_str(),
                *direction,
            )
            .expect("V2 components satisfy runtime validation"),
        }
    }
}

impl TryFrom<SecondaryIndexDefinition> for ValidatedSecondaryIndexDefinition {
    type Error = IndexV2ModelError;

    fn try_from(value: SecondaryIndexDefinition) -> Result<Self, Self::Error> {
        Ok(match value {
            SecondaryIndexDefinition::NodeEquality {
                label,
                property,
                unique,
            } => Self::NodeEquality {
                label: IndexComponent::try_new("label", label)?,
                property: IndexComponent::try_new("property", property)?,
                unique,
            },
            SecondaryIndexDefinition::NodeRange {
                label,
                property,
                direction,
            } => Self::NodeRange {
                label: IndexComponent::try_new("label", label)?,
                property: IndexComponent::try_new("property", property)?,
                direction,
            },
            SecondaryIndexDefinition::EdgeEquality { label, property } => Self::EdgeEquality {
                label: IndexComponent::try_new("label", label)?,
                property: IndexComponent::try_new("property", property)?,
            },
            SecondaryIndexDefinition::EdgeRange {
                label,
                property,
                direction,
            } => Self::EdgeRange {
                label: IndexComponent::try_new("label", label)?,
                property: IndexComponent::try_new("property", property)?,
                direction,
            },
        })
    }
}

/// Only vector payload codec assigned a V2 production discriminant.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActiveVectorCodecV2 {
    /// Current byte-compatible f32 payload codec.
    F32V1 = 0x01,
}

/// Exact stable score meaning of an active V2 vector generation.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorScoreSemanticV2 {
    /// `(1 - cosine) / 2` over f32 vectors.
    CosineHalfF32V1 = 0x01,
    /// Squared Euclidean distance over f32 vectors.
    SquaredEuclideanF32V1 = 0x02,
    /// Manhattan distance over f32 vectors.
    ManhattanF32V1 = 0x03,
}

/// Metric-specific zero-norm handling contract.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CosineNormPolicyV2 {
    /// The metric does not consume a cosine norm.
    NotApplicable = 0x00,
    /// Reject a zero scaled-L2 norm at the public boundary.
    RejectZeroScaledL2V1 = 0x01,
}

/// Strict canonical V2 vector-index definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedVectorIndexDefinition {
    element_kind: IndexElementKind,
    label: IndexComponent,
    property: IndexComponent,
    tenant_property: Option<IndexComponent>,
    dimension: NonZeroU32,
    metric: VectorDistanceMetric,
    codec: ActiveVectorCodecV2,
    m: NonZeroU32,
    m0: NonZeroU32,
    ef_construction: NonZeroU32,
    ml: f32,
    simhash_threshold: u32,
    sampling_ratio: f32,
    adaptive_enabled: bool,
    adaptive_failure_probability: f32,
}

// Every floating-point field is finite by construction, so structural equality
// is reflexive and the definition satisfies `Eq`'s contract.
impl Eq for ValidatedVectorIndexDefinition {}

impl ValidatedVectorIndexDefinition {
    /// Strictly validates a runtime vector definition without clamping fields.
    pub fn try_from_runtime(value: &VectorIndexDefinition) -> Result<Self, IndexV2ModelError> {
        let dimension = u32::try_from(value.dimension())
            .map_err(|_| IndexV2ModelError::VectorSettingOverflow { field: "dimension" })?;
        let m = u32::try_from(value.m())
            .map_err(|_| IndexV2ModelError::VectorSettingOverflow { field: "m" })?;
        let m0 = u32::try_from(value.m0())
            .map_err(|_| IndexV2ModelError::VectorSettingOverflow { field: "m0" })?;
        let ef_construction = u32::try_from(value.ef_construction()).map_err(|_| {
            IndexV2ModelError::VectorSettingOverflow {
                field: "ef_construction",
            }
        })?;
        let simhash_threshold = u32::try_from(value.simhash_threshold()).map_err(|_| {
            IndexV2ModelError::VectorSettingOverflow {
                field: "simhash_threshold",
            }
        })?;
        Self::try_new(
            match value.element_type() {
                VectorElementType::Node => IndexElementKind::Node,
                VectorElementType::Edge => IndexElementKind::Edge,
            },
            value.label(),
            value.property(),
            value.tenant_property(),
            dimension,
            value.metric(),
            m,
            m0,
            ef_construction,
            value.ml(),
            simhash_threshold,
            value.sampling_ratio(),
            value.adaptive_enabled(),
            value.adaptive_failure_prob(),
        )
    }

    /// Constructs a strict f32 V2 vector definition.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        element_kind: IndexElementKind,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
        dimension: u32,
        metric: VectorDistanceMetric,
        m: u32,
        m0: u32,
        ef_construction: u32,
        ml: f32,
        simhash_threshold: u32,
        sampling_ratio: f32,
        adaptive_enabled: bool,
        adaptive_failure_probability: f32,
    ) -> Result<Self, IndexV2ModelError> {
        let dimension =
            NonZeroU32::new(dimension).ok_or(IndexV2ModelError::InvalidVectorSetting {
                field: "dimension",
                reason: "must be non-zero",
            })?;
        let m = NonZeroU32::new(m).ok_or(IndexV2ModelError::InvalidVectorSetting {
            field: "m",
            reason: "must be non-zero",
        })?;
        let m0 = NonZeroU32::new(m0).ok_or(IndexV2ModelError::InvalidVectorSetting {
            field: "m0",
            reason: "must be non-zero",
        })?;
        let ef_construction =
            NonZeroU32::new(ef_construction).ok_or(IndexV2ModelError::InvalidVectorSetting {
                field: "ef_construction",
                reason: "must be non-zero",
            })?;
        if !ml.is_finite() || ml <= 0.0 {
            return Err(IndexV2ModelError::InvalidVectorSetting {
                field: "ml",
                reason: "must be finite and positive",
            });
        }
        if simhash_threshold as usize > SIMHASH_BITS {
            return Err(IndexV2ModelError::InvalidVectorSetting {
                field: "simhash_threshold",
                reason: "exceeds SimHash bit width",
            });
        }
        if !sampling_ratio.is_finite() || !(0.0..=1.0).contains(&sampling_ratio) {
            return Err(IndexV2ModelError::InvalidVectorSetting {
                field: "sampling_ratio",
                reason: "must be finite and within [0, 1]",
            });
        }
        if !adaptive_failure_probability.is_finite()
            || !(1e-6..=0.999_999).contains(&adaptive_failure_probability)
        {
            return Err(IndexV2ModelError::InvalidVectorSetting {
                field: "adaptive_failure_probability",
                reason: "must be finite and within [1e-6, 0.999999]",
            });
        }
        Ok(Self {
            element_kind,
            label: IndexComponent::try_new("label", label)?,
            property: IndexComponent::try_new("property", property)?,
            tenant_property: tenant_property
                .map(|property| IndexComponent::try_new("tenant property", property))
                .transpose()?,
            dimension,
            metric,
            codec: ActiveVectorCodecV2::F32V1,
            m,
            m0,
            ef_construction,
            ml,
            simhash_threshold,
            sampling_ratio,
            adaptive_enabled,
            adaptive_failure_probability,
        })
    }

    /// Returns the canonical logical identity.
    pub fn identity(&self) -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Vector,
            self.element_kind,
            self.label.clone(),
            self.property.clone(),
        )
    }

    /// Returns the node/edge kind.
    pub const fn element_kind(&self) -> IndexElementKind {
        self.element_kind
    }

    /// Returns the validated label.
    pub const fn label(&self) -> &IndexComponent {
        &self.label
    }

    /// Returns the validated property.
    pub const fn property(&self) -> &IndexComponent {
        &self.property
    }

    /// Returns the optional tenant partition property.
    pub fn tenant_property(&self) -> Option<&IndexComponent> {
        self.tenant_property.as_ref()
    }

    /// Returns the non-zero dimension.
    pub const fn dimension(&self) -> u32 {
        self.dimension.get()
    }

    /// Returns the distance metric.
    pub const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Returns the only active production codec.
    pub const fn codec(&self) -> ActiveVectorCodecV2 {
        self.codec
    }

    /// Returns HNSW M.
    pub const fn m(&self) -> u32 {
        self.m.get()
    }

    /// Returns HNSW M0.
    pub const fn m0(&self) -> u32 {
        self.m0.get()
    }

    /// Returns the construction beam width.
    pub const fn ef_construction(&self) -> u32 {
        self.ef_construction.get()
    }

    /// Returns the level multiplier.
    pub const fn ml(&self) -> f32 {
        self.ml
    }

    /// Returns the SimHash collision threshold.
    pub const fn simhash_threshold(&self) -> u32 {
        self.simhash_threshold
    }

    /// Returns the layer-zero sampling ratio.
    pub const fn sampling_ratio(&self) -> f32 {
        self.sampling_ratio
    }

    /// Returns whether adaptive traversal is active.
    pub const fn adaptive_enabled(&self) -> bool {
        self.adaptive_enabled
    }

    /// Returns the adaptive failure probability.
    pub const fn adaptive_failure_probability(&self) -> f32 {
        self.adaptive_failure_probability
    }

    /// Converts the durable semantic value into the non-persisted runtime adapter.
    pub fn to_runtime(&self) -> VectorIndexDefinition {
        let definition = match self.element_kind {
            IndexElementKind::Node => VectorIndexDefinition::new_node(
                self.label.as_str(),
                self.property.as_str(),
                self.dimension.get() as usize,
                self.metric,
            ),
            IndexElementKind::Edge => VectorIndexDefinition::new_edge(
                self.label.as_str(),
                self.property.as_str(),
                self.dimension.get() as usize,
                self.metric,
            ),
        }
        .expect("V2 vector components satisfy runtime validation");
        let definition = definition
            .with_m0(self.m0.get() as usize)
            .expect("V2 layer-0 connections satisfy runtime validation");
        let definition = definition
            .with_ef_construction(self.ef_construction.get() as usize)
            .expect("V2 construction beam satisfies runtime validation");
        let definition = definition
            .with_m(self.m.get() as usize)
            .expect("V2 connections satisfy runtime validation");
        let definition = definition
            .with_ml(self.ml)
            .expect("V2 layer multiplier satisfies runtime validation");
        let definition = definition
            .with_simhash_threshold(self.simhash_threshold as usize)
            .expect("V2 SimHash threshold satisfies runtime validation");
        let definition = definition
            .with_sampling_ratio(self.sampling_ratio)
            .expect("V2 sampling ratio satisfies runtime validation")
            .with_adaptive_enabled(self.adaptive_enabled);
        let definition = definition
            .with_adaptive_failure_prob(self.adaptive_failure_probability)
            .expect("V2 failure probability satisfies runtime validation");
        definition
            .with_tenant_property_option(
                self.tenant_property
                    .as_ref()
                    .map(|property| property.as_str()),
            )
            .expect("V2 tenant property satisfies runtime validation")
    }
}

/// Canonical V2 text-index definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidatedTextIndexDefinition {
    element_kind: IndexElementKind,
    label: IndexComponent,
    property: IndexComponent,
    tenant_property: Option<IndexComponent>,
    analyzer: TextAnalyzerKind,
    positions_enabled: bool,
}

impl ValidatedTextIndexDefinition {
    /// Strictly validates a runtime text definition.
    pub fn try_from_runtime(value: &TextIndexDefinition) -> Result<Self, IndexV2ModelError> {
        Ok(Self {
            element_kind: match value.element_type() {
                TextElementType::Node => IndexElementKind::Node,
                TextElementType::Edge => IndexElementKind::Edge,
            },
            label: IndexComponent::try_new("label", value.label())?,
            property: IndexComponent::try_new("property", value.property())?,
            tenant_property: value
                .tenant_property()
                .map(|property| IndexComponent::try_new("tenant property", property))
                .transpose()?,
            analyzer: value.analyzer(),
            positions_enabled: value.positions_enabled(),
        })
    }

    /// Constructs a strict V2 text definition.
    pub fn try_new(
        element_kind: IndexElementKind,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
        analyzer: TextAnalyzerKind,
        positions_enabled: bool,
    ) -> Result<Self, IndexV2ModelError> {
        Ok(Self {
            element_kind,
            label: IndexComponent::try_new("label", label)?,
            property: IndexComponent::try_new("property", property)?,
            tenant_property: tenant_property
                .map(|property| IndexComponent::try_new("tenant property", property))
                .transpose()?,
            analyzer,
            positions_enabled,
        })
    }

    /// Returns the canonical logical identity.
    pub fn identity(&self) -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Text,
            self.element_kind,
            self.label.clone(),
            self.property.clone(),
        )
    }

    /// Returns the node/edge kind.
    pub const fn element_kind(&self) -> IndexElementKind {
        self.element_kind
    }

    /// Returns the validated label.
    pub const fn label(&self) -> &IndexComponent {
        &self.label
    }

    /// Returns the validated property.
    pub const fn property(&self) -> &IndexComponent {
        &self.property
    }

    /// Returns the optional tenant property.
    pub fn tenant_property(&self) -> Option<&IndexComponent> {
        self.tenant_property.as_ref()
    }

    /// Returns the analyzer preset.
    pub const fn analyzer(&self) -> TextAnalyzerKind {
        self.analyzer
    }

    /// Returns whether positions are recorded.
    pub const fn positions_enabled(&self) -> bool {
        self.positions_enabled
    }

    /// Converts the durable semantic value into the non-persisted runtime adapter.
    pub fn to_runtime(&self) -> TextIndexDefinition {
        let definition = match self.element_kind {
            IndexElementKind::Node => {
                TextIndexDefinition::new_node(self.label.as_str(), self.property.as_str())
            }
            IndexElementKind::Edge => {
                TextIndexDefinition::new_edge(self.label.as_str(), self.property.as_str())
            }
        }
        .expect("V2 text components satisfy runtime validation")
        .with_analyzer(self.analyzer)
        .with_positions_enabled(self.positions_enabled);
        definition
            .with_tenant_property_option(
                self.tenant_property
                    .as_ref()
                    .map(|property| property.as_str()),
            )
            .expect("V2 tenant property satisfies runtime validation")
    }
}

/// Only definitions accepted by canonical V2 persistence.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidatedDynamicIndexDefinition {
    /// Secondary equality/range definition.
    Secondary(ValidatedSecondaryIndexDefinition),
    /// Strict f32 vector definition.
    Vector(ValidatedVectorIndexDefinition),
    /// Text definition.
    Text(ValidatedTextIndexDefinition),
}

impl ValidatedDynamicIndexDefinition {
    /// Returns the canonical identity derived from the definition itself.
    pub fn identity(&self) -> IndexIdentity {
        match self {
            Self::Secondary(definition) => definition.identity(),
            Self::Vector(definition) => definition.identity(),
            Self::Text(definition) => definition.identity(),
        }
    }

    /// Returns the family of the physical generation this definition requires.
    pub const fn family(&self) -> IndexDefinitionFamily {
        match self {
            Self::Secondary(_) => IndexDefinitionFamily::Secondary,
            Self::Vector(_) => IndexDefinitionFamily::Vector,
            Self::Text(_) => IndexDefinitionFamily::Text,
        }
    }
}

impl TryFrom<SecondaryIndexDefinition> for ValidatedDynamicIndexDefinition {
    type Error = IndexV2ModelError;

    fn try_from(value: SecondaryIndexDefinition) -> Result<Self, Self::Error> {
        Ok(Self::Secondary(value.try_into()?))
    }
}

impl TryFrom<VectorIndexDefinition> for ValidatedDynamicIndexDefinition {
    type Error = IndexV2ModelError;

    fn try_from(value: VectorIndexDefinition) -> Result<Self, Self::Error> {
        Ok(Self::Vector(
            ValidatedVectorIndexDefinition::try_from_runtime(&value)?,
        ))
    }
}

impl TryFrom<TextIndexDefinition> for ValidatedDynamicIndexDefinition {
    type Error = IndexV2ModelError;

    fn try_from(value: TextIndexDefinition) -> Result<Self, Self::Error> {
        Ok(Self::Text(ValidatedTextIndexDefinition::try_from_runtime(
            &value,
        )?))
    }
}

/// Three physical index families.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexDefinitionFamily {
    /// Secondary physical rows.
    Secondary = 0x01,
    /// Vector physical rows.
    Vector = 0x02,
    /// Text physical rows.
    Text = 0x03,
}

/// Exact metric/codec/score contract of one vector physical generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorGenerationDescriptor {
    dimension: NonZeroU32,
    metric: VectorDistanceMetric,
    codec: ActiveVectorCodecV2,
    score_semantic: VectorScoreSemanticV2,
    cosine_norm_policy: CosineNormPolicyV2,
}

impl VectorGenerationDescriptor {
    /// Derives the only legal active descriptor for a validated definition.
    pub fn for_definition(definition: &ValidatedVectorIndexDefinition) -> Self {
        let (score_semantic, cosine_norm_policy) = match definition.metric() {
            VectorDistanceMetric::Cosine => (
                VectorScoreSemanticV2::CosineHalfF32V1,
                CosineNormPolicyV2::RejectZeroScaledL2V1,
            ),
            VectorDistanceMetric::Euclidean => (
                VectorScoreSemanticV2::SquaredEuclideanF32V1,
                CosineNormPolicyV2::NotApplicable,
            ),
            VectorDistanceMetric::Manhattan => (
                VectorScoreSemanticV2::ManhattanF32V1,
                CosineNormPolicyV2::NotApplicable,
            ),
        };
        Self {
            dimension: definition.dimension,
            metric: definition.metric(),
            codec: definition.codec(),
            score_semantic,
            cosine_norm_policy,
        }
    }

    /// Validates a decoded descriptor tuple without accepting future codecs.
    pub fn try_new(
        dimension: u32,
        metric: VectorDistanceMetric,
        codec: ActiveVectorCodecV2,
        score_semantic: VectorScoreSemanticV2,
        cosine_norm_policy: CosineNormPolicyV2,
    ) -> Result<Self, IndexV2ModelError> {
        let dimension =
            NonZeroU32::new(dimension).ok_or(IndexV2ModelError::InvalidVectorSetting {
                field: "descriptor dimension",
                reason: "must be non-zero",
            })?;
        let legal = matches!(
            (metric, score_semantic, cosine_norm_policy),
            (
                VectorDistanceMetric::Cosine,
                VectorScoreSemanticV2::CosineHalfF32V1,
                CosineNormPolicyV2::RejectZeroScaledL2V1
            ) | (
                VectorDistanceMetric::Euclidean,
                VectorScoreSemanticV2::SquaredEuclideanF32V1,
                CosineNormPolicyV2::NotApplicable
            ) | (
                VectorDistanceMetric::Manhattan,
                VectorScoreSemanticV2::ManhattanF32V1,
                CosineNormPolicyV2::NotApplicable
            )
        );
        if !legal {
            return Err(IndexV2ModelError::VectorDescriptorMismatch);
        }
        Ok(Self {
            dimension,
            metric,
            codec,
            score_semantic,
            cosine_norm_policy,
        })
    }

    /// Returns the dimension.
    pub const fn dimension(self) -> u32 {
        self.dimension.get()
    }

    /// Returns the metric.
    pub const fn metric(self) -> VectorDistanceMetric {
        self.metric
    }

    /// Returns the codec.
    pub const fn codec(self) -> ActiveVectorCodecV2 {
        self.codec
    }

    /// Returns the stable score meaning.
    pub const fn score_semantic(self) -> VectorScoreSemanticV2 {
        self.score_semantic
    }

    /// Returns the cosine norm policy.
    pub const fn cosine_norm_policy(self) -> CosineNormPolicyV2 {
        self.cosine_norm_policy
    }
}

/// Closed physical generation descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PhysicalGeneration {
    /// Generation-qualified secondary rows.
    Secondary {
        /// Logical generation.
        generation: IndexGenerationId,
    },
    /// Generation-qualified vector rows with closed physical ownership.
    Vector {
        /// Logical generation.
        generation: IndexGenerationId,
        /// Unpartitioned or tenant-partitioned physical ownership.
        layout: VectorPhysicalLayout,
        /// Exact vector semantics.
        descriptor: VectorGenerationDescriptor,
    },
    /// Generation-qualified text rows.
    Text {
        /// Logical generation.
        generation: IndexGenerationId,
    },
}

/// Physical ownership allowed by a validated vector definition.
///
/// Unpartitioned generations own one namespace directly. Partitioned
/// generations resolve each canonical tenant value through a durable V2
/// mapping, so no meaningless generation-wide namespace can be constructed.
///
/// ```
/// use db::index_v2::{VectorPhysicalIndexId, VectorPhysicalLayout};
///
/// let layout = VectorPhysicalLayout::Unpartitioned {
///     physical_index_id: VectorPhysicalIndexId::initial(),
/// };
/// assert_eq!(layout.physical_index_id(), Some(VectorPhysicalIndexId::initial()));
/// assert_eq!(VectorPhysicalLayout::Partitioned.physical_index_id(), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorPhysicalLayout {
    /// One physical HNSW namespace owned by the complete generation.
    Unpartitioned {
        /// Fresh collision-free vector row namespace.
        physical_index_id: VectorPhysicalIndexId,
    },
    /// One durable mapping and physical namespace per canonical tenant value.
    Partitioned,
}

impl VectorPhysicalLayout {
    /// Returns the directly owned namespace only for an unpartitioned layout.
    pub const fn physical_index_id(self) -> Option<VectorPhysicalIndexId> {
        match self {
            Self::Unpartitioned { physical_index_id } => Some(physical_index_id),
            Self::Partitioned => None,
        }
    }
}

impl PhysicalGeneration {
    /// Returns the logical generation.
    pub const fn generation(&self) -> IndexGenerationId {
        match self {
            Self::Secondary { generation }
            | Self::Vector { generation, .. }
            | Self::Text { generation } => *generation,
        }
    }

    /// Returns the physical family.
    pub const fn family(&self) -> IndexDefinitionFamily {
        match self {
            Self::Secondary { .. } => IndexDefinitionFamily::Secondary,
            Self::Vector { .. } => IndexDefinitionFamily::Vector,
            Self::Text { .. } => IndexDefinitionFamily::Text,
        }
    }
}

/// Canonical persisted lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexStateV2 {
    /// Hidden physical construction is in progress.
    Building {
        /// Candidate generation.
        physical: PhysicalGeneration,
        /// Owning build operation.
        build_operation_id: IndexOperationId,
    },
    /// The exact physical generation is planner-visible.
    Active {
        /// Active generation.
        physical: PhysicalGeneration,
        /// Retained completed build operation.
        completed_build_operation_id: IndexOperationId,
    },
    /// A build is being rolled back.
    Aborting {
        /// Candidate generation being removed.
        physical: PhysicalGeneration,
        /// Original build operation, now executing abort progress.
        build_operation_id: IndexOperationId,
    },
    /// An active generation is draining and being removed.
    Dropping {
        /// Active generation being removed.
        physical: PhysicalGeneration,
        /// Owning drop operation.
        drop_operation_id: IndexOperationId,
    },
    /// No physical generation is visible or retained.
    Dropped {
        /// Last generation, retained for checked recreation.
        last_generation: IndexGenerationId,
        /// Completed abort/drop operation.
        completed_operation_id: IndexOperationId,
    },
}

impl IndexStateV2 {
    /// Returns a stable state name for diagnostics.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Building { .. } => "building",
            Self::Active { .. } => "active",
            Self::Aborting { .. } => "aborting",
            Self::Dropping { .. } => "dropping",
            Self::Dropped { .. } => "dropped",
        }
    }

    /// Returns the current or last logical generation.
    pub const fn generation(&self) -> IndexGenerationId {
        match self {
            Self::Building { physical, .. }
            | Self::Active { physical, .. }
            | Self::Aborting { physical, .. }
            | Self::Dropping { physical, .. } => physical.generation(),
            Self::Dropped {
                last_generation, ..
            } => *last_generation,
        }
    }

    /// Returns the physical generation when one still exists.
    pub const fn physical(&self) -> Option<&PhysicalGeneration> {
        match self {
            Self::Building { physical, .. }
            | Self::Active { physical, .. }
            | Self::Aborting { physical, .. }
            | Self::Dropping { physical, .. } => Some(physical),
            Self::Dropped { .. } => None,
        }
    }
}

/// Explicit legal state transition request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexStateTransition {
    /// Publish a successfully built generation.
    Activate,
    /// Convert build progress into abort cleanup.
    BeginAbort,
    /// Complete build-abort cleanup.
    CompleteAbort,
    /// Begin dropping an active generation.
    BeginDrop {
        /// Newly allocated drop operation.
        drop_operation_id: IndexOperationId,
    },
    /// Complete drop cleanup.
    CompleteDrop,
    /// Recreate a dropped logical index with the next generation.
    Recreate {
        /// Newly constructed physical generation.
        physical: PhysicalGeneration,
        /// Newly allocated build operation.
        build_operation_id: IndexOperationId,
    },
}

impl IndexStateTransition {
    const fn name(&self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::BeginAbort => "begin_abort",
            Self::CompleteAbort => "complete_abort",
            Self::BeginDrop { .. } => "begin_drop",
            Self::CompleteDrop => "complete_drop",
            Self::Recreate { .. } => "recreate",
        }
    }
}

/// The only canonical persisted logical index record.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexRecordV2 {
    index_id: IndexId,
    identity: IndexIdentity,
    definition: ValidatedDynamicIndexDefinition,
    revision: IndexRevision,
    state: IndexStateV2,
}

impl IndexRecordV2 {
    /// Constructs a hidden generation and derives its identity from the definition.
    pub fn building(
        index_id: IndexId,
        definition: ValidatedDynamicIndexDefinition,
        revision: IndexRevision,
        physical: PhysicalGeneration,
        build_operation_id: IndexOperationId,
    ) -> Result<Self, IndexV2ModelError> {
        let identity = definition.identity();
        Self::try_new(
            index_id,
            identity,
            definition,
            revision,
            IndexStateV2::Building {
                physical,
                build_operation_id,
            },
        )
    }

    /// Validates a fully decoded canonical record.
    pub fn try_new(
        index_id: IndexId,
        identity: IndexIdentity,
        definition: ValidatedDynamicIndexDefinition,
        revision: IndexRevision,
        state: IndexStateV2,
    ) -> Result<Self, IndexV2ModelError> {
        if identity != definition.identity() {
            return Err(IndexV2ModelError::IdentityDefinitionMismatch);
        }
        let Some(physical) = state.physical() else {
            return Ok(Self {
                index_id,
                identity,
                definition,
                revision,
                state,
            });
        };
        if physical.family() != definition.family() {
            return Err(IndexV2ModelError::PhysicalDefinitionMismatch);
        }
        let vector = match (&definition, physical) {
            (
                ValidatedDynamicIndexDefinition::Vector(definition),
                PhysicalGeneration::Vector {
                    layout, descriptor, ..
                },
            ) => Some((definition, layout, descriptor)),
            _ => None,
        };
        let Some((vector_definition, layout, descriptor)) = vector else {
            return Ok(Self {
                index_id,
                identity,
                definition,
                revision,
                state,
            });
        };
        let layout_matches = matches!(
            (vector_definition.tenant_property(), layout),
            (None, VectorPhysicalLayout::Unpartitioned { .. })
                | (Some(_), VectorPhysicalLayout::Partitioned)
        );
        if !layout_matches {
            return Err(IndexV2ModelError::VectorLayoutMismatch);
        }
        if *descriptor != VectorGenerationDescriptor::for_definition(vector_definition) {
            return Err(IndexV2ModelError::VectorDescriptorMismatch);
        }
        Ok(Self {
            index_id,
            identity,
            definition,
            revision,
            state,
        })
    }

    /// Returns the logical index ID.
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the logical identity.
    pub const fn identity(&self) -> &IndexIdentity {
        &self.identity
    }

    /// Returns the validated full definition.
    pub const fn definition(&self) -> &ValidatedDynamicIndexDefinition {
        &self.definition
    }

    /// Returns the compare-and-swap revision.
    pub const fn revision(&self) -> IndexRevision {
        self.revision
    }

    /// Returns the lifecycle state.
    pub const fn state(&self) -> &IndexStateV2 {
        &self.state
    }

    /// Applies one legal transition and checked-increments the record revision.
    pub fn transition(&self, transition: IndexStateTransition) -> Result<Self, IndexV2ModelError> {
        let next_state = match (&self.state, &transition) {
            (
                IndexStateV2::Building {
                    physical,
                    build_operation_id,
                },
                IndexStateTransition::Activate,
            ) => IndexStateV2::Active {
                physical: physical.clone(),
                completed_build_operation_id: *build_operation_id,
            },
            (
                IndexStateV2::Building {
                    physical,
                    build_operation_id,
                },
                IndexStateTransition::BeginAbort,
            ) => IndexStateV2::Aborting {
                physical: physical.clone(),
                build_operation_id: *build_operation_id,
            },
            (
                IndexStateV2::Aborting {
                    physical,
                    build_operation_id,
                },
                IndexStateTransition::CompleteAbort,
            ) => IndexStateV2::Dropped {
                last_generation: physical.generation(),
                completed_operation_id: *build_operation_id,
            },
            (
                IndexStateV2::Active { physical, .. },
                IndexStateTransition::BeginDrop { drop_operation_id },
            ) => IndexStateV2::Dropping {
                physical: physical.clone(),
                drop_operation_id: *drop_operation_id,
            },
            (
                IndexStateV2::Dropping {
                    physical,
                    drop_operation_id,
                },
                IndexStateTransition::CompleteDrop,
            ) => IndexStateV2::Dropped {
                last_generation: physical.generation(),
                completed_operation_id: *drop_operation_id,
            },
            (
                IndexStateV2::Dropped {
                    last_generation, ..
                },
                IndexStateTransition::Recreate {
                    physical,
                    build_operation_id,
                },
            ) if physical.generation() == last_generation.checked_next()? => {
                IndexStateV2::Building {
                    physical: physical.clone(),
                    build_operation_id: *build_operation_id,
                }
            }
            _ => {
                return Err(IndexV2ModelError::IllegalStateTransition {
                    from: self.state.name(),
                    transition: transition.name(),
                });
            }
        };
        Self::try_new(
            self.index_id,
            self.identity.clone(),
            self.definition.clone(),
            self.revision.checked_next()?,
            next_state,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secondary_definition() -> ValidatedDynamicIndexDefinition {
        SecondaryIndexDefinition::node_equality("User", "email")
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn building_record() -> IndexRecordV2 {
        IndexRecordV2::building(
            IndexId::initial(),
            secondary_definition(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([1; 16]).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn identifiers_reject_zero_nil_and_checked_exhaustion() {
        assert!(matches!(
            IndexId::new(0),
            Err(IndexV2ModelError::ZeroIdentifier { .. })
        ));
        assert!(matches!(
            IndexRevision::new(u64::MAX).unwrap().checked_next(),
            Err(IndexV2ModelError::IdentifierExhausted { .. })
        ));
        assert!(matches!(
            IndexOperationId::from_bytes([0; 16]),
            Err(IndexV2ModelError::NilUuid { .. })
        ));
    }

    #[test]
    fn components_enforce_non_empty_and_frozen_byte_bound() {
        assert!(matches!(
            IndexComponent::try_new("label", ""),
            Err(IndexV2ModelError::EmptyComponent { .. })
        ));
        assert!(IndexComponent::try_new("label", "x".repeat(INDEX_COMPONENT_MAX_LEN)).is_ok());
        assert!(matches!(
            IndexComponent::try_new("label", "x".repeat(INDEX_COMPONENT_MAX_LEN + 1)),
            Err(IndexV2ModelError::OversizedComponent { .. })
        ));
    }

    #[test]
    fn vector_validation_rejects_instead_of_clamping() {
        let error = ValidatedVectorIndexDefinition::try_new(
            IndexElementKind::Node,
            "Doc",
            "embedding",
            None::<String>,
            3,
            VectorDistanceMetric::Cosine,
            16,
            32,
            200,
            0.36,
            SIMHASH_BITS as u32 + 1,
            0.8,
            true,
            0.1,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            IndexV2ModelError::InvalidVectorSetting {
                field: "simhash_threshold",
                ..
            }
        ));
    }

    #[test]
    fn vector_descriptor_only_accepts_metric_specific_score_and_norm_tuple() {
        assert!(VectorGenerationDescriptor::try_new(
            3,
            VectorDistanceMetric::Cosine,
            ActiveVectorCodecV2::F32V1,
            VectorScoreSemanticV2::CosineHalfF32V1,
            CosineNormPolicyV2::RejectZeroScaledL2V1,
        )
        .is_ok());
        assert!(matches!(
            VectorGenerationDescriptor::try_new(
                3,
                VectorDistanceMetric::Cosine,
                ActiveVectorCodecV2::F32V1,
                VectorScoreSemanticV2::CosineHalfF32V1,
                CosineNormPolicyV2::NotApplicable,
            ),
            Err(IndexV2ModelError::VectorDescriptorMismatch)
        ));
    }

    #[test]
    fn vector_layout_must_match_the_validated_tenant_partition_setting() {
        let definition = |tenant_property: Option<&str>| {
            ValidatedVectorIndexDefinition::try_new(
                IndexElementKind::Node,
                "Doc",
                "embedding",
                tenant_property,
                3,
                VectorDistanceMetric::Cosine,
                16,
                32,
                64,
                0.5,
                4,
                0.75,
                false,
                0.25,
            )
            .unwrap()
        };
        let unpartitioned = definition(None);
        let partitioned = definition(Some("tenant_id"));
        let physical =
            |definition: &ValidatedVectorIndexDefinition, layout| PhysicalGeneration::Vector {
                generation: IndexGenerationId::initial(),
                layout,
                descriptor: VectorGenerationDescriptor::for_definition(definition),
            };
        let record = |definition: ValidatedVectorIndexDefinition, physical| {
            IndexRecordV2::building(
                IndexId::initial(),
                ValidatedDynamicIndexDefinition::Vector(definition),
                IndexRevision::initial(),
                physical,
                IndexOperationId::from_bytes([9; 16]).unwrap(),
            )
        };

        assert!(record(
            unpartitioned.clone(),
            physical(&unpartitioned, VectorPhysicalLayout::Partitioned),
        )
        .is_err_and(|error| error == IndexV2ModelError::VectorLayoutMismatch));
        assert!(record(
            partitioned.clone(),
            physical(
                &partitioned,
                VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::initial(),
                },
            ),
        )
        .is_err_and(|error| error == IndexV2ModelError::VectorLayoutMismatch));
        assert!(record(
            unpartitioned.clone(),
            physical(
                &unpartitioned,
                VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::initial(),
                },
            ),
        )
        .is_ok());
        assert!(record(
            partitioned.clone(),
            physical(&partitioned, VectorPhysicalLayout::Partitioned),
        )
        .is_ok());
    }

    #[test]
    fn every_legal_state_transition_is_exhaustive_and_revisioned() {
        let building = building_record();
        let active = building.transition(IndexStateTransition::Activate).unwrap();
        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::from_bytes([2; 16]).unwrap(),
            })
            .unwrap();
        let dropped = dropping
            .transition(IndexStateTransition::CompleteDrop)
            .unwrap();
        let recreated = dropped
            .transition(IndexStateTransition::Recreate {
                physical: PhysicalGeneration::Secondary {
                    generation: IndexGenerationId::new(2).unwrap(),
                },
                build_operation_id: IndexOperationId::from_bytes([3; 16]).unwrap(),
            })
            .unwrap();
        assert!(matches!(recreated.state(), IndexStateV2::Building { .. }));
        assert_eq!(recreated.revision().get(), 5);

        let aborting = building
            .transition(IndexStateTransition::BeginAbort)
            .unwrap();
        let aborted = aborting
            .transition(IndexStateTransition::CompleteAbort)
            .unwrap();
        assert!(matches!(aborted.state(), IndexStateV2::Dropped { .. }));
    }

    #[test]
    fn illegal_or_skipped_generation_transition_is_rejected() {
        let building = building_record();
        assert!(matches!(
            building.transition(IndexStateTransition::CompleteDrop),
            Err(IndexV2ModelError::IllegalStateTransition { .. })
        ));
        let dropped = building
            .transition(IndexStateTransition::BeginAbort)
            .unwrap()
            .transition(IndexStateTransition::CompleteAbort)
            .unwrap();
        assert!(matches!(
            dropped.transition(IndexStateTransition::Recreate {
                physical: PhysicalGeneration::Secondary {
                    generation: IndexGenerationId::new(3).unwrap(),
                },
                build_operation_id: IndexOperationId::from_bytes([4; 16]).unwrap(),
            }),
            Err(IndexV2ModelError::IllegalStateTransition { .. })
        ));
    }
}
