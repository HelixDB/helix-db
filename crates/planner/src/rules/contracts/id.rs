use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

use super::KnownRuleId;
use crate::ir;

/// Stable optimizer rule ID.
///
/// ```
/// use helix_planner::rules::{KnownRuleId, RuleId};
///
/// assert!(RuleId::new("").is_none());
/// assert_eq!(RuleId::new("filter_pushdown").unwrap().as_ref(), "filter_pushdown");
/// assert_eq!(RuleId::known(KnownRuleId::SeedStream).as_ref(), "seed_stream");
/// assert_eq!(
///     RuleId::known(KnownRuleId::SeedStream).to_non_empty_string().as_ref(),
///     "seed_stream"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleId(RuleIdKind);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RuleIdKind {
    Known(KnownRuleId),
    Custom(ir::NonEmptyString),
}

impl RuleId {
    /// Build a production rule ID from the closed rule inventory.
    pub const fn known(id: KnownRuleId) -> Self {
        Self(RuleIdKind::Known(id))
    }

    /// Build a rule ID, rejecting empty names.
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        KnownRuleId::from_name(&value).map(Self::known).or_else(|| {
            ir::NonEmptyString::new(value)
                .map(RuleIdKind::Custom)
                .map(Self)
        })
    }

    /// Convert this validated rule ID into a validated string value.
    pub fn to_non_empty_string(&self) -> ir::NonEmptyString {
        match &self.0 {
            RuleIdKind::Known(id) => ir::NonEmptyString::from_static(id.as_str()),
            RuleIdKind::Custom(id) => id.clone(),
        }
    }

    /// Return the closed production-rule inventory value when this is a known rule.
    pub const fn known_inventory(&self) -> Option<KnownRuleId> {
        match &self.0 {
            RuleIdKind::Known(id) => Some(*id),
            RuleIdKind::Custom(_) => None,
        }
    }
}

impl From<KnownRuleId> for RuleId {
    fn from(value: KnownRuleId) -> Self {
        Self::known(value)
    }
}

impl AsRef<str> for RuleId {
    fn as_ref(&self) -> &str {
        match self {
            Self(RuleIdKind::Known(id)) => id.as_ref(),
            Self(RuleIdKind::Custom(id)) => id.as_ref(),
        }
    }
}

impl PartialOrd for RuleId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RuleId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl Serialize for RuleId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for RuleId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| D::Error::custom("expected non-empty rule ID"))
    }
}
