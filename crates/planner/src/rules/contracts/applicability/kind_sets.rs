//! Canonical non-empty scheduler kind-set wrappers.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ir, logical};

/// Non-empty canonical set of top-level logical expression families.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleLogicalKinds {
    kinds: CanonicalKindSet<logical::LogicalExprKind>,
}

impl RuleLogicalKinds {
    /// Build a canonical non-empty kind set.
    pub fn new(kinds: ir::AtLeast<logical::LogicalExprKind, 1>) -> Self {
        Self {
            kinds: CanonicalKindSet::new(kinds),
        }
    }

    /// Build a set with one top-level logical expression kind.
    pub fn one(kind: logical::LogicalExprKind) -> Self {
        Self {
            kinds: CanonicalKindSet::one(kind),
        }
    }

    /// Return the canonical kind slice.
    pub fn as_slice(&self) -> &[logical::LogicalExprKind] {
        self.kinds.as_slice()
    }

    /// True when the set contains `kind`.
    pub fn contains(&self, kind: logical::LogicalExprKind) -> bool {
        self.kinds.contains(kind)
    }
}

impl Serialize for RuleLogicalKinds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.kinds.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RuleLogicalKinds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ir::AtLeast::<logical::LogicalExprKind, 1>::deserialize(deserializer).map(Self::new)
    }
}

/// Non-empty canonical set of pure logical operation families.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulePureOpKinds {
    kinds: CanonicalKindSet<logical::PureLogicalOpKind>,
}

impl RulePureOpKinds {
    /// Build a canonical non-empty pure-operation kind set.
    pub fn new(kinds: ir::AtLeast<logical::PureLogicalOpKind, 1>) -> Self {
        Self {
            kinds: CanonicalKindSet::new(kinds),
        }
    }

    /// Build a set with one pure operation kind.
    pub fn one(kind: logical::PureLogicalOpKind) -> Self {
        Self {
            kinds: CanonicalKindSet::one(kind),
        }
    }

    /// Return the canonical kind slice.
    pub fn as_slice(&self) -> &[logical::PureLogicalOpKind] {
        self.kinds.as_slice()
    }

    /// True when the set contains `kind`.
    pub fn contains(&self, kind: logical::PureLogicalOpKind) -> bool {
        self.kinds.contains(kind)
    }
}

impl Serialize for RulePureOpKinds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.kinds.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RulePureOpKinds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ir::AtLeast::<logical::PureLogicalOpKind, 1>::deserialize(deserializer).map(Self::new)
    }
}

/// Non-empty canonical set of stream-pipeline operator families.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleStreamPipelineOpKinds {
    kinds: CanonicalKindSet<logical::StreamPipelineOpKind>,
}

impl RuleStreamPipelineOpKinds {
    /// Build a canonical non-empty stream-pipeline operator kind set.
    pub fn new(kinds: ir::AtLeast<logical::StreamPipelineOpKind, 1>) -> Self {
        Self {
            kinds: CanonicalKindSet::new(kinds),
        }
    }

    /// Build a set with one stream-pipeline operator kind.
    pub fn one(kind: logical::StreamPipelineOpKind) -> Self {
        Self {
            kinds: CanonicalKindSet::one(kind),
        }
    }

    /// Return the canonical kind slice.
    pub fn as_slice(&self) -> &[logical::StreamPipelineOpKind] {
        self.kinds.as_slice()
    }

    /// True when the set contains `kind`.
    pub fn contains(&self, kind: logical::StreamPipelineOpKind) -> bool {
        self.kinds.contains(kind)
    }
}

impl Serialize for RuleStreamPipelineOpKinds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.kinds.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RuleStreamPipelineOpKinds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ir::AtLeast::<logical::StreamPipelineOpKind, 1>::deserialize(deserializer).map(Self::new)
    }
}

/// Non-empty canonical set of access-source families.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleAccessSourceKinds {
    kinds: CanonicalKindSet<logical::AccessSourceKind>,
}

impl RuleAccessSourceKinds {
    /// Build a canonical non-empty access-source kind set.
    pub fn new(kinds: ir::AtLeast<logical::AccessSourceKind, 1>) -> Self {
        Self {
            kinds: CanonicalKindSet::new(kinds),
        }
    }

    /// Build a set with one access-source kind.
    pub fn one(kind: logical::AccessSourceKind) -> Self {
        Self {
            kinds: CanonicalKindSet::one(kind),
        }
    }

    /// Return the canonical kind slice.
    pub fn as_slice(&self) -> &[logical::AccessSourceKind] {
        self.kinds.as_slice()
    }

    /// True when the set contains `kind`.
    pub fn contains(&self, kind: logical::AccessSourceKind) -> bool {
        self.kinds.contains(kind)
    }
}

impl Serialize for RuleAccessSourceKinds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.kinds.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RuleAccessSourceKinds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ir::AtLeast::<logical::AccessSourceKind, 1>::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalKindSet<T> {
    kinds: ir::AtLeast<T, 1>,
}

impl<T> CanonicalKindSet<T>
where
    T: Ord,
{
    fn new(kinds: ir::AtLeast<T, 1>) -> Self {
        Self {
            kinds: kinds.sorted_dedup(),
        }
    }

    fn one(kind: T) -> Self {
        Self {
            kinds: ir::AtLeast::<_, 1>::from_one(kind),
        }
    }
}

impl<T> CanonicalKindSet<T> {
    fn as_slice(&self) -> &[T] {
        self.kinds.as_ref()
    }

    fn contains(&self, kind: T) -> bool
    where
        T: PartialEq,
    {
        self.kinds.as_ref().contains(&kind)
    }
}

impl<T> Serialize for CanonicalKindSet<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.kinds.serialize(serializer)
    }
}
