//! Arena-backed native requests.
//!
//! [`crate::query::ArenaQueryRequest`] parses a request body into a tree that
//! lives entirely in one growable [`Bump`] arena: every node, string, list and
//! map is a bump allocation, so parsing makes a handful of large allocations
//! instead of one per node, and freeing the tree is freeing (or resetting) the
//! arena's few chunks instead of walking and freeing every node.
//!
//! Each owned request type `X` has a mirror `ArenaX<'a>` generated beside it
//! by `#[derive(ArenaMirror)]` and re-exported here under the owned name, so
//! `arena::AstNode<'a>` mirrors [`crate::traversal::AstNode`]. Mirrors are
//! `Copy`, hold `&'a str`, `&'a T`, `&'a [T]` and [`Map`] where the owned
//! types hold `String`, `Box<T>`, `Vec<T>` and `BTreeMap<String, T>`, and
//! never need dropping: nothing in the arena owns heap memory, which the
//! `Copy` bounds on [`ArenaDeserialize`]'s reference impls enforce.
//!
//! Contract:
//!
//! - Parsing into the arena accepts exactly the JSON the owned parse accepts
//!   and reports the same errors, and [`IntoOwned`] converts a mirror into the
//!   owned value that parse would have produced.
//! - Every arena allocation is fallible: with an allocation limit set on the
//!   [`Bump`] (see [`PoolConfig::allocation_limit`]), parsing past it returns
//!   an error instead of panicking.
//! - Mirrors are `Send + Sync`, so an async task may hold the tree across
//!   awaits while it owns the arena; the [`Bump`] itself is only borrowed
//!   during the synchronous parse.
//!
//! ```
//! use helix_ast::arena::{self, IntoOwned};
//! use helix_ast::graph::NodeRef;
//!
//! let bump = arena::Bump::new();
//! let mut deserializer = sonic_rs::Deserializer::from_str(r#"{"ids":[1,2,3]}"#);
//! let node_ref: arena::NodeRef<'_> =
//!     serde::de::DeserializeSeed::deserialize(arena::Seed::new(&bump), &mut deserializer).unwrap();
//! assert_eq!(node_ref, arena::NodeRef::Ids(&[1, 2, 3]));
//! assert_eq!(node_ref.into_owned(), NodeRef::ids([1, 2, 3]));
//! ```

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use serde::de::{DeserializeSeed, Error as _, SeqAccess, Unexpected, Visitor};
use serde::{Deserialize, Deserializer};

pub use bumpalo::Bump;

pub use crate::batch::{
    ArenaBatchCondition as BatchCondition, ArenaBatchEntry as BatchEntry,
    ArenaBatchQuery as BatchQuery, ArenaNamedQuery as NamedQuery, ArenaReadBatch as ReadBatch,
    ArenaWriteBatch as WriteBatch,
};
pub use crate::expr::{
    ArenaExpr as Expr, ArenaPredicate as Predicate, ArenaSourcePredicate as SourcePredicate,
    ArenaStreamBound as StreamBound, ArenaWhenThen as WhenThen,
};
pub use crate::graph::{ArenaEdgeRef as EdgeRef, ArenaNodeRef as NodeRef};
pub use crate::index::ArenaIndexSpec as IndexSpec;
pub use crate::projection::{
    ArenaBindingProjection as BindingProjection, ArenaBindingTarget as BindingTarget,
    ArenaBindingValueRef as BindingValueRef, ArenaExprProjection as ExprProjection,
    ArenaProjection as Projection, ArenaPropertyProjection as PropertyProjection,
};
pub use crate::query::{ArenaQueryRequest as QueryRequest, ArenaQueryValue as QueryValue};
pub use crate::traversal::{
    ArenaAstNode as AstNode, ArenaRepeatConfig as RepeatConfig, ArenaSubTraversal as SubTraversal,
};
pub use crate::value::{ArenaPropertyInput as PropertyInput, ArenaPropertyValue as PropertyValue};

mod map;
mod pool;

pub use map::Map;
pub use pool::{Pool, PoolConfig, PooledBump};

#[cfg(test)]
mod tests;

/// The error every arena allocation reports when the arena's allocation
/// limit (or the system allocator) refuses it.
pub const ALLOCATION_LIMIT_EXCEEDED: &str = "arena allocation limit exceeded";

/// Deserialize `Self` with every allocation in `bump`.
///
/// serde's `Deserialize` cannot carry an arena, so arena types deserialize
/// through this trait and [`Seed`], serde's `DeserializeSeed` adapter.
pub trait ArenaDeserialize<'a>: Sized {
    /// Deserialize from `deserializer`, allocating in `bump`.
    ///
    /// # Errors
    ///
    /// Returns the deserializer's errors, and [`ALLOCATION_LIMIT_EXCEEDED`]
    /// when `bump` refuses an allocation.
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error>;
}

/// A `DeserializeSeed` that deserializes a `T` into an arena.
pub struct Seed<'a, T> {
    bump: &'a Bump,
    target: PhantomData<fn() -> T>,
}

impl<'a, T> Seed<'a, T> {
    /// A seed allocating in `bump`.
    pub fn new(bump: &'a Bump) -> Self {
        Self {
            bump,
            target: PhantomData,
        }
    }
}

impl<'a, 'de, T: ArenaDeserialize<'a>> DeserializeSeed<'de> for Seed<'a, T> {
    type Value = T;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<T, D::Error> {
        T::deserialize_in(self.bump, deserializer)
    }
}

/// Convert an arena value into the owned value it mirrors.
pub trait IntoOwned<T> {
    /// The owned value, copying every borrowed string, list and map.
    fn into_owned(self) -> T;
}

fn allocation_failed<E: serde::de::Error>(_: impl Sized) -> E {
    E::custom(ALLOCATION_LIMIT_EXCEEDED)
}

/// Types that mirror themselves: `Copy`, own no heap, and deserialize with
/// serde's own impls, so number coercions and errors are serde's.
macro_rules! plain {
    ($($ty:ty),* $(,)?) => {$(
        impl<'a> ArenaDeserialize<'a> for $ty {
            fn deserialize_in<'de, D: Deserializer<'de>>(
                _bump: &'a Bump,
                deserializer: D,
            ) -> Result<Self, D::Error> {
                <$ty as Deserialize<'de>>::deserialize(deserializer)
            }
        }

        impl IntoOwned<$ty> for $ty {
            fn into_owned(self) -> $ty {
                self
            }
        }
    )*};
}

plain!(
    bool,
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize,
    f32,
    f64,
    NonZeroUsize
);

/// Mirrors `String`: serde's `String` visitor, copying into the arena.
impl<'a> ArenaDeserialize<'a> for &'a str {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct StrVisitor<'a> {
            bump: &'a Bump,
        }

        impl<'a> Visitor<'_> for StrVisitor<'a> {
            type Value = &'a str;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<&'a str, E> {
                self.bump
                    .try_alloc_str(value)
                    .map(|value| &*value)
                    .map_err(allocation_failed)
            }

            fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<&'a str, E> {
                let Ok(text) = std::str::from_utf8(value) else {
                    return Err(E::invalid_value(Unexpected::Bytes(value), &self));
                };
                self.visit_str(text)
            }
        }

        deserializer.deserialize_string(StrVisitor { bump })
    }
}

impl IntoOwned<String> for &str {
    fn into_owned(self) -> String {
        self.to_owned()
    }
}

/// Mirrors `Vec<T>`: serde's sequence visitor, collecting in the arena.
impl<'a, T: ArenaDeserialize<'a> + Copy + 'a> ArenaDeserialize<'a> for &'a [T] {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct SliceVisitor<'a, T> {
            bump: &'a Bump,
            element: PhantomData<fn() -> T>,
        }

        impl<'a, 'de, T: ArenaDeserialize<'a> + Copy + 'a> Visitor<'de> for SliceVisitor<'a, T> {
            type Value = &'a [T];

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<&'a [T], A::Error> {
                // Trust a size hint only as far as serde does, so a hostile
                // hint cannot reserve more than 1 MiB up front.
                const MAX_PREALLOCATED_BYTES: usize = 1024 * 1024;
                let reserve = seq
                    .size_hint()
                    .unwrap_or(0)
                    .min(MAX_PREALLOCATED_BYTES / size_of::<T>().max(1));
                let mut values = bumpalo::collections::Vec::new_in(self.bump);
                values.try_reserve(reserve).map_err(allocation_failed)?;
                while let Some(value) = seq.next_element_seed(Seed::<T>::new(self.bump))? {
                    values.try_reserve(1).map_err(allocation_failed)?;
                    values.push(value);
                }
                Ok(values.into_bump_slice())
            }
        }

        deserializer.deserialize_seq(SliceVisitor {
            bump,
            element: PhantomData,
        })
    }
}

impl<T: IntoOwned<O> + Copy, O> IntoOwned<Vec<O>> for &[T] {
    fn into_owned(self) -> Vec<O> {
        self.iter()
            .map(|value| <T as IntoOwned<O>>::into_owned(*value))
            .collect()
    }
}

/// Mirrors `Box<T>`: the value, moved into the arena.
impl<'a, T: ArenaDeserialize<'a> + Copy + 'a> ArenaDeserialize<'a> for &'a T {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let value = T::deserialize_in(bump, deserializer)?;
        bump.try_alloc(value)
            .map(|value| &*value)
            .map_err(allocation_failed)
    }
}

impl<T: IntoOwned<O> + Copy, O> IntoOwned<Box<O>> for &T {
    fn into_owned(self) -> Box<O> {
        Box::new((*self).into_owned())
    }
}

/// Mirrors `Option<T>`: serde's option visitor.
impl<'a, T: ArenaDeserialize<'a>> ArenaDeserialize<'a> for Option<T> {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct OptionVisitor<'a, T> {
            bump: &'a Bump,
            value: PhantomData<fn() -> T>,
        }

        impl<'a, 'de, T: ArenaDeserialize<'a>> Visitor<'de> for OptionVisitor<'a, T> {
            type Value = Option<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("option")
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Option<T>, E> {
                Ok(None)
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Option<T>, E> {
                Ok(None)
            }

            fn visit_some<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Option<T>, D::Error> {
                T::deserialize_in(self.bump, deserializer).map(Some)
            }
        }

        deserializer.deserialize_option(OptionVisitor {
            bump,
            value: PhantomData,
        })
    }
}

impl<T: IntoOwned<O>, O> IntoOwned<Option<O>> for Option<T> {
    fn into_owned(self) -> Option<O> {
        self.map(IntoOwned::into_owned)
    }
}

/// Mirrors `(A, B)`: serde's two-tuple visitor.
impl<'a, A: ArenaDeserialize<'a>, B: ArenaDeserialize<'a>> ArenaDeserialize<'a> for (A, B) {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct PairVisitor<'a, A, B> {
            bump: &'a Bump,
            pair: PhantomData<fn() -> (A, B)>,
        }

        impl<'a, 'de, A: ArenaDeserialize<'a>, B: ArenaDeserialize<'a>> Visitor<'de>
            for PairVisitor<'a, A, B>
        {
            type Value = (A, B);

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a tuple of size 2")
            }

            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(A, B), S::Error> {
                let Some(first) = seq.next_element_seed(Seed::<A>::new(self.bump))? else {
                    return Err(S::Error::invalid_length(0, &self));
                };
                let Some(second) = seq.next_element_seed(Seed::<B>::new(self.bump))? else {
                    return Err(S::Error::invalid_length(1, &self));
                };
                Ok((first, second))
            }
        }

        deserializer.deserialize_tuple(
            2,
            PairVisitor {
                bump,
                pair: PhantomData,
            },
        )
    }
}

impl<A: IntoOwned<OA>, B: IntoOwned<OB>, OA, OB> IntoOwned<(OA, OB)> for (A, B) {
    fn into_owned(self) -> (OA, OB) {
        (self.0.into_owned(), self.1.into_owned())
    }
}

impl<V: IntoOwned<O> + Copy, O> IntoOwned<BTreeMap<String, O>> for Map<'_, V> {
    fn into_owned(self) -> BTreeMap<String, O> {
        self.iter()
            .map(|(key, value)| (key.to_owned(), <V as IntoOwned<O>>::into_owned(*value)))
            .collect()
    }
}
