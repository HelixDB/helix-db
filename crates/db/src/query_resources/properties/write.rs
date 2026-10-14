//! Canonical write bytes and their query reservation have separate lifetimes.
//! The transaction retains `memory` through commit; shared storage caches may
//! retain the bytes afterwards without retaining the request or its budget.
use super::super::{Budget, Reservation};
use crate::{
    encoding::v2::values::property,
    error::{HelixDbError, Result},
};
use bytes::Bytes;
use rkyv::ser;
use std::{mem::MaybeUninit, sync::Arc};

#[derive(Clone)]
pub(crate) struct Encoded {
    pub(crate) bytes: Bytes,
    pub(crate) memory: Option<Arc<Reservation>>,
}
impl Encoded {
    pub(crate) fn native(bytes: Bytes) -> Self {
        Self {
            bytes,
            memory: None,
        }
    }

    pub(crate) fn new(prepared: &property::write::Prepared<'_>, budget: &Budget) -> Result<Self> {
        let _scratch_memory = budget.reserve(prepared.scratch_bytes())?;
        let mut scratch = vec![MaybeUninit::uninit(); prepared.scratch_bytes()];
        let len = prepared.encoded_len(&mut scratch).map_err(|_| {
            HelixDbError::InvariantViolation(
                "canonical property measurement exceeded its prepared scratch bound".into(),
            )
        })?;
        if len == 0 {
            return Ok(Self::native(Bytes::new()));
        }
        // Bytes may allocate a Shared header on its first clone. The separate
        // reservation Arc adds its owner and two counters. The exact output
        // allocation never grows or coexists with a second output buffer.
        let memory = budget.reserve(len.saturating_add(
            size_of::<Reservation>() + 2 * size_of::<usize>() + 4 * size_of::<usize>(),
        ))?;
        let mut bytes = vec![0; len];
        let written = prepared
            .encode(
                ser::writer::Buffer::from(bytes.as_mut_slice()),
                &mut scratch,
            )
            .map_err(|_| {
                HelixDbError::InvariantViolation(
                    "canonical property encoding disagreed with its measured bounds".into(),
                )
            })?;
        assert_eq!(
            written.len(),
            len,
            "an immutable row must match its measured encoding"
        );
        Ok(Self {
            bytes: Bytes::from(bytes.into_boxed_slice()),
            memory: Some(Arc::new(memory)),
        })
    }
}

impl std::fmt::Debug for Encoded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bytes.fmt(f)
    }
}
impl PartialEq for Encoded {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

#[cfg(test)]
mod tests;
