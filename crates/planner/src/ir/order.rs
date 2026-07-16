use std::collections::BTreeSet;

use helix_ast::traversal::Order;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{AtLeast, NonEmptyString};

/// Order key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderKey {
    /// Property.
    pub property: NonEmptyString,
    /// Direction.
    pub order: Order,
}

/// Invalid explicit sort key payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderKeysError {
    /// More than one sort key references the same property.
    DuplicateProperty {
        /// Duplicate sort property.
        property: NonEmptyString,
    },
}

/// Non-empty explicit sort keys with unique properties.
///
/// ```
/// use helix_ast::traversal::Order;
/// use helix_planner::ir::{AtLeast, NonEmptyString, OrderKey, OrderKeys, OrderKeysError};
///
/// let age = NonEmptyString::new("age").unwrap();
/// let keys = OrderKeys::new(AtLeast::<_, 1>::from_one(OrderKey {
///     property: age.clone(),
///     order: Order::Asc,
/// })).unwrap();
/// assert_eq!(serde_json::to_string(&keys).unwrap(), r#"[{"property":"age","order":"asc"}]"#);
///
/// let duplicate = AtLeast::<_, 1>::from_one_and_rest(
///     OrderKey { property: NonEmptyString::new("age").unwrap(), order: Order::Asc },
///     vec![OrderKey { property: NonEmptyString::new("age").unwrap(), order: Order::Desc }],
/// );
/// assert!(matches!(
///     OrderKeys::new(duplicate),
///     Err(OrderKeysError::DuplicateProperty { .. })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKeys {
    keys: AtLeast<OrderKey, 1>,
}

impl OrderKeys {
    /// Build explicit sort keys, returning an error for duplicate properties.
    pub fn new(keys: AtLeast<OrderKey, 1>) -> Result<Self, OrderKeysError> {
        let mut seen = BTreeSet::new();
        for key in &keys {
            if !seen.insert(key.property.clone()) {
                return Err(OrderKeysError::DuplicateProperty {
                    property: key.property.clone(),
                });
            }
        }
        Ok(Self { keys })
    }
}

impl AsRef<[OrderKey]> for OrderKeys {
    fn as_ref(&self) -> &[OrderKey] {
        self.keys.as_ref()
    }
}

impl From<OrderKey> for OrderKeys {
    fn from(key: OrderKey) -> Self {
        Self {
            keys: AtLeast::<_, 1>::from_one(key),
        }
    }
}

impl Serialize for OrderKeys {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.keys.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OrderKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let keys = AtLeast::<OrderKey, 1>::deserialize(deserializer)?;
        Self::new(keys).map_err(|err| match err {
            OrderKeysError::DuplicateProperty { property } => {
                D::Error::custom(format!("duplicate order key `{property}`"))
            }
        })
    }
}

/// Order execution plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderPlan {
    /// Executor must sort by one or more keys.
    ExplicitSort(OrderKeys),
    /// Access path already produces the order for one key.
    RangeIndex {
        /// Ordered key produced by the index.
        key: OrderKey,
        /// Index ID.
        index_id: NonEmptyString,
    },
}
