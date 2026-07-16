//! Element KV key contracts.

use serde::{Deserialize, Serialize};

use crate::properties;

/// Primary element record keyspaces used by native executable point reads.
///
/// IDs are encoded as big-endian bytes inside the keyspace. Keeping the
/// keyspace as an ADT makes node/edge key mixing unrepresentable for native
/// element point reads.
///
/// ```
/// use helix_planner::exec::ElementKeyspace;
///
/// let key = ElementKeyspace::NodeProperty.point_key(7);
/// assert_eq!(key.keyspace(), ElementKeyspace::NodeProperty);
/// assert_eq!(key.id(), 7);
/// assert_eq!(key.bytes(), &7_u64.to_be_bytes());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElementKeyspace {
    /// Node property record keyspace.
    NodeProperty,
    /// Edge endpoint record keyspace.
    EdgeEndpoints,
}

impl ElementKeyspace {
    /// Stable keyspace name used in executable plans.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeProperty => "node_property",
            Self::EdgeEndpoints => "edge_endpoints",
        }
    }

    /// Element kind represented by this keyspace.
    pub const fn element(self) -> properties::ElementKind {
        match self {
            Self::NodeProperty => properties::ElementKind::Node,
            Self::EdgeEndpoints => properties::ElementKind::Edge,
        }
    }

    /// Encode one concrete element ID as a point-read key.
    pub fn point_key(self, id: u64) -> KvKey {
        KvKey::from_id(self, id)
    }
}

impl std::fmt::Display for ElementKeyspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Element point key with a typed keyspace and fixed-width encoded ID.
///
/// ```
/// use helix_planner::exec::{ElementKeyspace, KvKey};
///
/// assert!(KvKey::new(ElementKeyspace::NodeProperty, Vec::new()).is_none());
/// assert!(KvKey::new(ElementKeyspace::NodeProperty, vec![1]).is_none());
/// assert_eq!(
///     KvKey::new(ElementKeyspace::NodeProperty, 1_u64.to_be_bytes().to_vec()).unwrap().id(),
///     1
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvKey {
    keyspace: ElementKeyspace,
    bytes: [u8; 8],
}

impl KvKey {
    /// Build an element point key from already encoded 8-byte ID bytes.
    pub fn new(keyspace: ElementKeyspace, bytes: Vec<u8>) -> Option<Self> {
        Some(Self {
            keyspace,
            bytes: bytes.try_into().ok()?,
        })
    }

    /// Build an element point key from an element ID.
    pub fn from_id(keyspace: ElementKeyspace, id: u64) -> Self {
        Self {
            keyspace,
            bytes: id.to_be_bytes(),
        }
    }

    /// Keyspace.
    pub const fn keyspace(&self) -> ElementKeyspace {
        self.keyspace
    }

    /// Element ID encoded by this point key.
    pub fn id(&self) -> u64 {
        u64::from_be_bytes(self.bytes)
    }

    /// Encoded big-endian element ID bytes.
    pub const fn bytes(&self) -> &[u8; 8] {
        &self.bytes
    }
}

/// Keyspace-free element bound key used inside a typed range scan.
///
/// The enclosing `KvReadPlan::RangeScan` owns the `ElementKeyspace`, so range
/// bounds cannot disagree with the scan keyspace.
///
/// ```
/// use helix_planner::exec::KvBoundKey;
///
/// assert!(KvBoundKey::new(Vec::new()).is_none());
/// assert!(KvBoundKey::new(vec![1]).is_none());
/// assert_eq!(KvBoundKey::from_id(9).id(), 9);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvBoundKey {
    bytes: [u8; 8],
}

impl KvBoundKey {
    /// Build a bound key from already encoded 8-byte ID bytes.
    pub fn new(bytes: Vec<u8>) -> Option<Self> {
        Some(Self {
            bytes: bytes.try_into().ok()?,
        })
    }

    /// Build a bound key from an element ID.
    pub fn from_id(id: u64) -> Self {
        Self {
            bytes: id.to_be_bytes(),
        }
    }

    /// Element ID encoded by this bound key.
    pub fn id(&self) -> u64 {
        u64::from_be_bytes(self.bytes)
    }

    /// Encoded big-endian element ID bytes.
    pub const fn bytes(&self) -> &[u8; 8] {
        &self.bytes
    }
}

/// Bound for executable range scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KvKeyBound {
    /// Open bound.
    Unbounded,
    /// Inclusive encoded key.
    Included(KvBoundKey),
    /// Exclusive encoded key.
    Excluded(KvBoundKey),
}

impl KvKeyBound {
    /// Inclusive bound from an element ID.
    pub fn included_id(id: u64) -> Self {
        Self::Included(KvBoundKey::from_id(id))
    }

    /// Exclusive bound from an element ID.
    pub fn excluded_id(id: u64) -> Self {
        Self::Excluded(KvBoundKey::from_id(id))
    }
}
