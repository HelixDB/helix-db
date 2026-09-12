//! Bound structural recursion before bytecheck or deserialization descends.
//! This is an archive-structure limit, not the frontend expression-depth limit.
use super::{EncodingError, Property};
use rkyv::{
    rancor,
    validation::{archive::ArchiveValidator, ArchiveContext},
};
use std::{alloc::Layout, ops::Range};

// Keep recursive validation and typed deserialization within the normal test
// stack, including nested map visitors. Boundary tests exercise both shapes.
pub(super) const MAX_ARCHIVE_DEPTH: usize = 256;

struct Validator<'a> {
    inner: ArchiveValidator<'a>,
    remaining: usize,
    exceeded: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("stored property archive nesting limit exceeded")]
struct DepthLimit;

pub(super) fn properties(bytes: &[u8]) -> Result<&rkyv::Archived<Vec<Property>>, EncodingError> {
    let mut validator = Validator {
        inner: ArchiveValidator::new(bytes),
        remaining: MAX_ARCHIVE_DEPTH,
        exceeded: false,
    };
    // Failure discards bytecheck's recursive diagnostic trace. In debug builds
    // rancor::Error allocates one trace per level, before decode admission.
    rkyv::api::access_with_context::<_, _, rancor::Failure>(bytes, &mut validator).map_err(|_| {
        if validator.exceeded {
            EncodingError::PropertyNestingLimit
        } else {
            EncodingError::Rkyv("invalid stored property archive".into())
        }
    })
}

// SAFETY: Every pointer and range check delegates to the canonical validator.
// The additional counter only rejects descent; it never admits extra memory.
unsafe impl<E: rancor::Source> ArchiveContext<E> for Validator<'_> {
    fn check_subtree_ptr(&mut self, ptr: *const u8, layout: &Layout) -> Result<(), E> {
        self.inner.check_subtree_ptr(ptr, layout)
    }

    unsafe fn push_subtree_range(
        &mut self,
        root: *const u8,
        end: *const u8,
    ) -> Result<Range<usize>, E> {
        if self.remaining == 0 {
            self.exceeded = true;
            rancor::fail!(DepthLimit);
        }
        // SAFETY: The caller supplies the canonical trait's checked range.
        let range = unsafe { self.inner.push_subtree_range(root, end) }?;
        self.remaining -= 1;
        Ok(range)
    }

    unsafe fn pop_subtree_range(&mut self, range: Range<usize>) -> Result<(), E> {
        assert!(
            self.remaining < MAX_ARCHIVE_DEPTH,
            "a subtree pop must match an admitted push"
        );
        // SAFETY: The range is passed unchanged to the validator that issued it.
        unsafe { self.inner.pop_subtree_range(range) }?;
        self.remaining += 1;
        Ok(())
    }
}
