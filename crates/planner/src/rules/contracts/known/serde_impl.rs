//! Serde contract for closed production rule IDs.

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

use super::KnownRuleId;

impl Serialize for KnownRuleId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for KnownRuleId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_name(&value).ok_or_else(|| D::Error::custom("expected known rule ID"))
    }
}
