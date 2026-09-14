//! Allocation preflight and fallible serialization for the existing row format.
//! The serializer uses the same derived archives and `AsVec` root as `row`.
//! Scratch space is supplied by the caller; serialization never opens an arena.
use super::{property_value::PropertyValue, Property};
use crate::encoding::error::EncodingError;
use rkyv::{rancor, ser, with, Archive};
use std::mem::MaybeUninit;

// Bound recursive cloning and serialization on the ordinary execution stack.
// Native callers without query admission retain their existing codec behavior.
pub(crate) const MAX_WRITE_DEPTH: usize = 48;

/// A borrowed, depth-validated row with immutable allocation bounds.
pub(crate) struct Prepared<'a> {
    properties: &'a [Property],
    scratch_bytes: usize,
    clone_bytes: usize,
    retained_bytes: usize,
}
impl<'a> Prepared<'a> {
    pub(crate) fn new(properties: &'a [Property]) -> Result<Self, EncodingError> {
        let mut result = Self {
            properties,
            scratch_bytes: scratch::<Property>(properties.len()),
            clone_bytes: properties.len().saturating_mul(size_of::<Property>()),
            retained_bytes: 0,
        };
        for property in properties {
            let allocation = value(&property.value, 0)?;
            result.scratch_bytes = result.scratch_bytes.saturating_add(allocation.scratch);
            result.clone_bytes = result
                .clone_bytes
                .saturating_add(property.name.len())
                .saturating_add(allocation.cloned);
            result.retained_bytes = result
                .retained_bytes
                .saturating_add(property.name.capacity())
                .saturating_add(allocation.retained);
        }
        Ok(result)
    }

    pub(crate) const fn scratch_bytes(&self) -> usize {
        self.scratch_bytes
    }
    pub(crate) const fn clone_bytes(&self) -> usize {
        self.clone_bytes
    }
    /// Heap payload plus the supplied capacity of the owned outer vector.
    pub(crate) fn retained_bytes(&self, capacity: usize) -> usize {
        assert!(capacity >= self.properties.len());
        self.retained_bytes
            .saturating_add(capacity.saturating_mul(size_of::<Property>()))
    }

    /// Measure with the canonical serializer, reusing admitted scratch. No
    /// property payload is copied by this pass; bulk byte writes only add their
    /// length. The borrowed row cannot change between measuring and encoding.
    pub(crate) fn encoded_len(
        &self,
        scratch: &mut [MaybeUninit<u8>],
    ) -> Result<usize, rancor::Failure> {
        self.encode(Measure(0), scratch).map(|measure| measure.0)
    }

    pub(crate) fn encode<W: ser::Writer<rancor::Failure>>(
        &self,
        writer: W,
        scratch: &mut [MaybeUninit<u8>],
    ) -> Result<W, rancor::Failure> {
        if self.properties.is_empty() {
            return Ok(writer);
        }
        let properties = with::With::<&[Property], with::AsVec>::cast(&self.properties);
        let mut serializer = ser::Serializer::new(
            writer,
            ser::allocator::SubAllocator::new(scratch),
            ser::sharing::Unshare,
        );
        rkyv::api::serialize_using::<_, rancor::Failure>(properties, &mut serializer)?;
        Ok(serializer.into_raw_parts().0)
    }
}

struct Measure(usize);
impl ser::Positional for Measure {
    fn pos(&self) -> usize {
        self.0
    }
}
impl ser::Writer<rancor::Failure> for Measure {
    fn write(&mut self, bytes: &[u8]) -> Result<(), rancor::Failure> {
        use rancor::Source;
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| rancor::Failure::new(OutputLengthOverflow))?;
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
#[error("encoded property row length overflowed")]
struct OutputLengthOverflow;

struct Allocation {
    cloned: usize,
    retained: usize,
    scratch: usize,
}

// Each SerVec requests resolvers and at most alignment - 1 bytes of padding.
// Sum all requests, including sequential ones, so SubAllocator's retained
// alignment padding is covered without relying on an arena's growth policy.
fn scratch<T: Archive>(count: usize) -> usize {
    if count == 0 || size_of::<T::Resolver>() == 0 {
        return 0;
    }
    count
        .saturating_mul(size_of::<T::Resolver>())
        .saturating_add(align_of::<T::Resolver>() - 1)
}

fn value(value: &PropertyValue, depth: usize) -> Result<Allocation, EncodingError> {
    if depth >= MAX_WRITE_DEPTH {
        return Err(EncodingError::PropertyNestingLimit);
    }
    let (cloned, retained, temporary) = match value {
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_) => (0, 0, 0),
        PropertyValue::String(s) => (s.len(), s.capacity(), 0),
        PropertyValue::Bytes(v) => (v.len(), v.capacity(), 0),
        PropertyValue::I64Array(v) => (
            v.len().saturating_mul(size_of::<i64>()),
            v.capacity().saturating_mul(size_of::<i64>()),
            0,
        ),
        PropertyValue::F64Array(v) => (
            v.len().saturating_mul(size_of::<f64>()),
            v.capacity().saturating_mul(size_of::<f64>()),
            0,
        ),
        PropertyValue::F32Array(v) => (
            v.len().saturating_mul(size_of::<f32>()),
            v.capacity().saturating_mul(size_of::<f32>()),
            0,
        ),
        PropertyValue::StringArray(v) => (
            v.iter()
                .fold(v.len().saturating_mul(size_of::<String>()), |n, s| {
                    n.saturating_add(s.len())
                }),
            v.iter()
                .fold(v.capacity().saturating_mul(size_of::<String>()), |n, s| {
                    n.saturating_add(s.capacity())
                }),
            scratch::<String>(v.len()),
        ),
        PropertyValue::Array(v) => {
            let mut allocation = Allocation {
                cloned: v.len().saturating_mul(size_of::<PropertyValue>()),
                retained: v.capacity().saturating_mul(size_of::<PropertyValue>()),
                scratch: scratch::<PropertyValue>(v.len()),
            };
            for child in v {
                let child = self::value(child, depth + 1)?;
                allocation.cloned = allocation.cloned.saturating_add(child.cloned);
                allocation.retained = allocation.retained.saturating_add(child.retained);
                allocation.scratch = allocation.scratch.saturating_add(child.scratch);
            }
            return Ok(allocation);
        }
        PropertyValue::Object(v) => {
            let tree = helix_planner::relational::allocation::btree_bytes::<String, PropertyValue>(
                v.len(),
            );
            // The pinned rkyv B-tree serializes an array of open inner nodes.
            // Each is an InlineVec of (key reference, value reference, child
            // position). Tree height is at most log2(entries) + 1. Leaf and
            // per-node resolvers stay on the stack. The type equality below
            // makes a dependency change to the node capacity a compile error.
            const _: Option<
                rkyv::collections::btree_map::ArchivedBTreeMap<
                    rkyv::Archived<String>,
                    rkyv::Archived<PropertyValue>,
                    5,
                >,
            > = None::<rkyv::Archived<std::collections::BTreeMap<String, PropertyValue>>>;
            type Inner<'a> =
                rkyv::util::InlineVec<(&'a String, &'a PropertyValue, Option<usize>), 5>;
            let mut allocation = Allocation {
                cloned: tree,
                retained: tree,
                scratch: v.len().checked_ilog2().map_or(0, |height| {
                    (height as usize)
                        .saturating_mul(size_of::<Inner<'_>>())
                        .saturating_add(align_of::<Inner<'_>>() - 1)
                }),
            };
            for (key, child) in v {
                let child = self::value(child, depth + 1)?;
                allocation.cloned = allocation
                    .cloned
                    .saturating_add(key.len())
                    .saturating_add(child.cloned);
                allocation.retained = allocation
                    .retained
                    .saturating_add(key.capacity())
                    .saturating_add(child.retained);
                allocation.scratch = allocation.scratch.saturating_add(child.scratch);
            }
            return Ok(allocation);
        }
    };
    Ok(Allocation {
        cloned,
        retained,
        scratch: temporary,
    })
}

#[cfg(test)]
mod tests;
