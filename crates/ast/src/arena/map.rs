//! A string-keyed map in the arena, mirroring `BTreeMap<String, V>`.

use std::marker::PhantomData;

use serde::de::{MapAccess, Visitor};
use serde::Deserializer;

use super::{allocation_failed, ArenaDeserialize, Bump, Seed};

/// The arena mirror of `BTreeMap<String, V>`: entries sorted by key with each
/// key once, so lookup is a binary search and iteration is in key order.
///
/// Only deserialization builds one, and it keeps the last value of a repeated
/// key, as inserting into a `BTreeMap` does.
///
/// ```
/// use helix_ast::arena::{self, IntoOwned};
/// use helix_ast::value::PropertyValue;
///
/// let bump = arena::Bump::new();
/// let mut deserializer =
///     sonic_rs::Deserializer::from_str(r#"{"object":{"b":{"i64":1},"a":{"null":null},"b":{"i64":2}}}"#);
/// let value: arena::PropertyValue<'_> =
///     serde::de::DeserializeSeed::deserialize(arena::Seed::new(&bump), &mut deserializer).unwrap();
/// let arena::PropertyValue::Object(map) = value else { panic!("object") };
/// assert_eq!(map.iter().map(|(key, _)| key).collect::<Vec<_>>(), ["a", "b"]);
/// assert_eq!(map.get("b"), Some(&arena::PropertyValue::I64(2)));
/// let owned: PropertyValue = value.into_owned();
/// assert_eq!(owned.as_object().unwrap()["b"], PropertyValue::I64(2));
/// ```
pub struct Map<'a, V> {
    entries: &'a [(&'a str, V)],
}

impl<'a, V> Map<'a, V> {
    /// The value under `key`.
    pub fn get(&self, key: &str) -> Option<&'a V> {
        self.entries
            .binary_search_by(|(entry, _)| (*entry).cmp(key))
            .ok()
            .map(|index| &self.entries[index].1)
    }

    /// Entries in key order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&'a str, &'a V)> + use<'a, V> {
        self.entries.iter().map(|(key, value)| (*key, value))
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// Manual impls: the entries are a shared slice, which is `Copy` whatever `V`
// is, and deriving would require `V: Copy` and `V: Clone` bounds.
impl<V> Clone for Map<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Map<'_, V> {}

impl<V: PartialEq> PartialEq for Map<'_, V> {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl<V: std::fmt::Debug> std::fmt::Debug for Map<'_, V> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

/// serde's `BTreeMap` visitor, collecting in the arena.
impl<'a, V: ArenaDeserialize<'a> + Copy + 'a> ArenaDeserialize<'a> for Map<'a, V> {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct MapVisitor<'a, V> {
            bump: &'a Bump,
            value: PhantomData<fn() -> V>,
        }

        impl<'a, 'de, V: ArenaDeserialize<'a> + Copy + 'a> Visitor<'de> for MapVisitor<'a, V> {
            type Value = Map<'a, V>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Map<'a, V>, A::Error> {
                let mut entries = bumpalo::collections::Vec::new_in(self.bump);
                while let Some(key) = map.next_key_seed(Seed::<&'a str>::new(self.bump))? {
                    let value = map.next_value_seed(Seed::<V>::new(self.bump))?;
                    entries.try_reserve(1).map_err(allocation_failed)?;
                    entries.push((key, value));
                }
                // Reversing first makes the stable sort put the last value of
                // each key first, which the deduplication keeps.
                entries.reverse();
                entries.sort_by_key(|(key, _)| *key);
                entries.dedup_by(|(later, _), (earlier, _)| later == earlier);
                Ok(Map {
                    entries: entries.into_bump_slice(),
                })
            }
        }

        deserializer.deserialize_map(MapVisitor {
            bump,
            value: PhantomData,
        })
    }
}
