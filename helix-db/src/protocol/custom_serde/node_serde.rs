use crate::utils::{
    items::Node,
    properties::{ImmutablePropertiesMap, ImmutablePropertiesMapDeSeed},
};
use serde::de::{DeserializeSeed, Visitor};
use std::fmt;

/// Helper DeserializeSeed for Option<ImmutablePropertiesMap>
/// This is needed because we can't use next_element::<Option<T>>() with custom DeserializeSeed
struct OptionPropertiesMapDeSeed<'arena> {
    arena: &'arena bumpalo::Bump,
}

impl<'de, 'arena> DeserializeSeed<'de> for OptionPropertiesMapDeSeed<'arena> {
    type Value = Option<ImmutablePropertiesMap<'arena>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OptVisitor<'arena> {
            arena: &'arena bumpalo::Bump,
        }

        impl<'de, 'arena> Visitor<'de> for OptVisitor<'arena> {
            type Value = Option<ImmutablePropertiesMap<'arena>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Option<ImmutablePropertiesMap>")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                ImmutablePropertiesMapDeSeed { arena: self.arena }
                    .deserialize(deserializer)
                    .map(Some)
            }
        }

        deserializer.deserialize_option(OptVisitor { arena: self.arena })
    }
}

/// DeserializeSeed for Node that allocates label and properties into the arena
pub struct NodeDeSeed<'arena> {
    pub arena: &'arena bumpalo::Bump,
    pub id: u128,
}

impl<'de, 'arena> serde::de::DeserializeSeed<'de> for NodeDeSeed<'arena> {
    type Value = Node<'arena>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct NodeVisitor<'arena> {
            arena: &'arena bumpalo::Bump,
            id: u128,
        }

        impl<'de, 'arena> serde::de::Visitor<'de> for NodeVisitor<'arena> {
            type Value = Node<'arena>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Node")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let label_string: &'de str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let label = self.arena.alloc_str(label_string);

                let version: u8 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;

                // Bincode serializes Option<T> as ONE field: 0x00 (None) or 0x01+data (Some)
                // Use our custom DeserializeSeed that handles the Option wrapper
                let properties: Option<ImmutablePropertiesMap<'arena>> = seq
                    .next_element_seed(OptionPropertiesMapDeSeed { arena: self.arena })?
                    .unwrap_or(None);

                Ok(Node {
                    id: self.id,
                    label,
                    version,
                    properties,
                })
            }
        }

        // Match the serialize_struct call in Node's Serialize implementation
        deserializer.deserialize_struct(
            "Node",
            &["label", "version", "properties"],
            NodeVisitor {
                arena: self.arena,
                id: self.id,
            },
        )
    }
}
