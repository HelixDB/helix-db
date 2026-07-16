//! Serde boundary for lower-bound collections.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::AtLeast;

impl<T, const MIN: usize> Serialize for AtLeast<T, MIN>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.items.serialize(serializer)
    }
}

fn item_plural_suffix(count: usize) -> &'static str {
    if count == 1 {
        ""
    } else {
        "s"
    }
}

impl<'de, T, const MIN: usize> Deserialize<'de> for AtLeast<T, MIN>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let items = Vec::<T>::deserialize(deserializer)?;
        Self::try_from_vec(items).ok_or_else(|| {
            D::Error::custom(format!(
                "expected at least {MIN} item{}",
                item_plural_suffix(MIN)
            ))
        })
    }
}
