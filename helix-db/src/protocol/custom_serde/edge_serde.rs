use crate::{
    helix_engine::storage_core::Arena,
    utils::{
        items::Edge,
        properties::{ImmutablePropertiesMap, ImmutablePropertiesMapDeSeed},
    },
};
use serde::de::{DeserializeSeed, Visitor};
use std::fmt;

/// Helper DeserializeSeed for Option<ImmutablePropertiesMap>
struct OptionPropertiesMapDeSeed<'arena> {
    arena: Arena<'arena>,
}

impl<'de, 'arena> DeserializeSeed<'de> for OptionPropertiesMapDeSeed<'arena> {
    type Value = Option<ImmutablePropertiesMap<'arena>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OptVisitor<'arena> {
            arena: Arena<'arena>,
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
pub struct EdgeDeSeed<'arena> {
    pub arena: Arena<'arena>,
    pub id: u128,
}

impl<'de, 'arena> serde::de::DeserializeSeed<'de> for EdgeDeSeed<'arena> {
    type Value = Edge<'arena>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct EdgeVisitor<'arena> {
            arena: Arena<'arena>,
            id: u128,
        }

        impl<'de, 'arena> serde::de::Visitor<'de> for EdgeVisitor<'arena> {
            type Value = Edge<'arena>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Edge")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let label_string: &'de str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let label = self.arena.alloc_str(label_string);

                let version: u8 = seq.next_element()?.unwrap_or(0);

                let from_node: u128 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;

                let to_node: u128 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;

                // Use our custom DeserializeSeed that handles the Option wrapper
                let properties: Option<ImmutablePropertiesMap<'arena>> = seq
                    .next_element_seed(OptionPropertiesMapDeSeed { arena: self.arena })?
                    .ok_or_else(|| serde::de::Error::custom("Expected properties field"))?;

                Ok(Edge {
                    id: self.id,
                    label,
                    version,
                    from_node,
                    to_node,
                    properties,
                })
            }
        }

        // Match the serialize_struct call in Edge's Serialize implementation
        deserializer.deserialize_struct(
            "Edge",
            &["label", "version", "from_node", "to_node", "properties"],
            EdgeVisitor {
                arena: self.arena,
                id: self.id,
            },
        )
    }
}
