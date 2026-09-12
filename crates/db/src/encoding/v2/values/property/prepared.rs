//! Resource-aware access to the unchanged property-row format.
//!
//! Callers admit `data.len()` bytes before constructing `Archive`, then admit
//! `Prepared::owned_bytes()` before decoding. The immutable prepared selection
//! ties the allocation bound to exactly the properties that will be decoded.
//! Unselected values are validated but their owned payloads are never created.
use super::{
    property::Property,
    property_value::{ArchivedPropertyValue as V, PropertyValue},
};
use crate::encoding::error::EncodingError;
use std::{collections::BTreeSet, ops::ControlFlow};

#[cfg(test)]
mod tests;
mod validation;

/// Immutable selection; no user callback can change its answer between
/// allocation preflight and decoding. Names serve small native field sets;
/// sorted keys serve graph projection demand without a quadratic field search.
#[derive(Clone, Copy)]
pub(crate) enum Selection<'a> {
    All,
    Names(&'a [&'a str]),
    Keys {
        names: &'a BTreeSet<String>,
        required: &'a str,
    },
}
impl Selection<'_> {
    pub(crate) fn contains(self, name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Names(names) => names.contains(&name),
            Self::Keys { names, required } => name == required || names.contains(name),
        }
    }
}

/// Owns the exactly sized aligned copy. This type does not yet claim that the
/// bytes are a valid archive; only `prepare` can produce that capability.
pub(crate) struct Archive(rkyv::util::AlignedVec<{ super::row::PROPERTY_ALIGNMENT }>);
impl Archive {
    pub(crate) fn new(data: &[u8]) -> Self {
        let mut bytes = rkyv::util::AlignedVec::with_capacity(data.len());
        bytes.extend_from_slice(data);
        Self(bytes)
    }

    pub(crate) fn prepare<'a>(
        &'a self,
        selection: Selection<'a>,
    ) -> Result<Prepared<'a>, EncodingError> {
        if self.0.is_empty() {
            return Ok(Prepared(Ready::Empty));
        }
        let properties = validation::properties(&self.0)?;
        let (count, owned_bytes) = properties
            .iter()
            .filter(|property| selection.contains(property.name.as_str()))
            .fold((0_usize, 0_usize), |(count, bytes), property| {
                (
                    count + 1,
                    bytes
                        .saturating_add(size_of::<Property>())
                        .saturating_add(property.name.len())
                        .saturating_add(value_bytes(&property.value)),
                )
            });
        Ok(Prepared(Ready::Properties {
            properties,
            selection,
            count,
            owned_bytes,
        }))
    }
}

pub(crate) struct Prepared<'a>(Ready<'a>);
enum Ready<'a> {
    Empty,
    Properties {
        properties: &'a rkyv::Archived<Vec<Property>>,
        selection: Selection<'a>,
        count: usize,
        owned_bytes: usize,
    },
}
impl Prepared<'_> {
    /// Upper bound for the decoded vector, strings, arrays and map nodes.
    /// The aligned archive and any storage-owned bytes are separate live owners.
    pub(crate) fn owned_bytes(&self) -> usize {
        match self.0 {
            Ready::Empty => 0,
            Ready::Properties { owned_bytes, .. } => owned_bytes,
        }
    }

    pub(crate) fn decode(self) -> Result<Vec<Property>, EncodingError> {
        let Ready::Properties {
            properties,
            selection,
            count,
            ..
        } = self.0
        else {
            return Ok(Vec::new());
        };
        let mut values = Vec::with_capacity(count);
        for property in properties
            .iter()
            .filter(|property| selection.contains(property.name.as_str()))
        {
            values.push(
                rkyv::deserialize::<Property, rkyv::rancor::Error>(property)
                    .map_err(|error| EncodingError::Rkyv(error.to_string()))?,
            );
        }
        assert_eq!(
            values.len(),
            count,
            "prepared selection cannot change during decode"
        );
        Ok(values)
    }
}

fn value_bytes(value: &V) -> usize {
    match value {
        V::Null | V::Bool(_) | V::I64(_) | V::DateTime(_) | V::F64(_) | V::F32(_) => 0,
        V::String(value) => value.len(),
        V::Bytes(value) => value.len(),
        V::I64Array(values) => values.len().saturating_mul(size_of::<i64>()),
        V::F64Array(values) => values.len().saturating_mul(size_of::<f64>()),
        V::F32Array(values) => values.len().saturating_mul(size_of::<f32>()),
        V::StringArray(values) => values.iter().fold(
            values.len().saturating_mul(size_of::<String>()),
            |bytes, value| bytes.saturating_add(value.len()),
        ),
        V::Array(values) => values.iter().fold(
            values.len().saturating_mul(size_of::<PropertyValue>()),
            |bytes, value| bytes.saturating_add(value_bytes(value)),
        ),
        V::Object(values) => {
            if values.is_empty() {
                return 0;
            }
            let mut bytes = helix_planner::relational::allocation::btree_bytes::<
                String,
                PropertyValue,
            >(values.len());
            // The pinned archived-map iterator allocates a traversal vector.
            // Its visitor walks the already depth-validated tree without one.
            values.visit(|key, value| {
                bytes = bytes
                    .saturating_add(key.len())
                    .saturating_add(value_bytes(value));
                ControlFlow::<()>::Continue(())
            });
            bytes
        }
    }
}
