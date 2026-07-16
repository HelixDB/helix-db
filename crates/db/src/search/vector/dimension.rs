//! Checked vector-dimension contracts for distance kernels.
//!
//! Persisted metadata and query inputs begin as unrelated lengths. This module
//! converts them into non-zero dimensions and same-dimension proofs before a
//! distance kernel runs, so kernels cannot silently truncate through `zip`.
//! Callers validate metadata once, bind each vector through [`VectorRef`], then
//! construct [`SameDimensionPair`] for pairwise computation.

use std::num::NonZeroUsize;

use super::unaligned_vector::UnalignedVector;

/// A validated, non-zero vector dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorDimension(NonZeroUsize);

impl VectorDimension {
    /// Validate a vector dimension.
    ///
    /// ```
    /// use db::search::vector::VectorDimension;
    ///
    /// assert_eq!(VectorDimension::try_new(3).unwrap().get(), 3);
    /// assert!(VectorDimension::try_new(0).is_err());
    /// ```
    pub fn try_new(dimension: usize) -> Result<Self, VectorDimensionError> {
        let Some(dimension) = NonZeroUsize::new(dimension) else {
            return Err(VectorDimensionError::ZeroDimension);
        };
        Ok(Self(dimension))
    }

    /// Validate a vector dimension against an engine or configuration limit.
    pub fn try_new_with_max(
        dimension: usize,
        maximum: NonZeroUsize,
    ) -> Result<Self, VectorDimensionError> {
        let dimension = Self::try_new(dimension)?;
        if dimension.get() > maximum.get() {
            return Err(VectorDimensionError::ExceedsMaximum {
                maximum: maximum.get(),
                actual: dimension.get(),
            });
        }
        Ok(dimension)
    }

    /// Return the validated element count.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// An unaligned f32 vector proven to have an expected dimension.
#[derive(Debug, Clone, Copy)]
pub struct VectorRef<'a> {
    values: &'a UnalignedVector<f32>,
    dimension: VectorDimension,
}

impl<'a> VectorRef<'a> {
    /// Check a borrowed vector against an authoritative dimension.
    ///
    /// ```
    /// use db::search::vector::{VectorDimension, VectorRef};
    /// use db::search::vector::unaligned_vector::UnalignedVector;
    ///
    /// let values = UnalignedVector::from_slice(&[1.0_f32, 2.0, 3.0]);
    /// let dimension = VectorDimension::try_new(3)?;
    /// let checked = VectorRef::try_new(&values, dimension)?;
    /// assert_eq!(checked.dimension(), dimension);
    /// # Ok::<(), db::search::vector::VectorDimensionError>(())
    /// ```
    pub fn try_new(
        values: &'a UnalignedVector<f32>,
        expected: VectorDimension,
    ) -> Result<Self, VectorDimensionError> {
        if values.len() != expected.get() {
            return Err(VectorDimensionError::DimensionMismatch {
                expected: expected.get(),
                actual: values.len(),
            });
        }
        Ok(Self {
            values,
            dimension: expected,
        })
    }

    /// Return the checked unaligned vector.
    pub const fn values(self) -> &'a UnalignedVector<f32> {
        self.values
    }

    /// Return the authoritative dimension.
    pub const fn dimension(self) -> VectorDimension {
        self.dimension
    }
}

/// Two f32 vectors proven to share one non-zero dimension.
#[derive(Debug, Clone, Copy)]
pub struct SameDimensionPair<'a> {
    left: VectorRef<'a>,
    right: VectorRef<'a>,
}

impl<'a> SameDimensionPair<'a> {
    /// Validate both vectors and bind their shared dimension.
    ///
    /// ```
    /// use db::search::vector::SameDimensionPair;
    /// use db::search::vector::unaligned_vector::UnalignedVector;
    ///
    /// let left = UnalignedVector::from_slice(&[1.0_f32, 2.0]);
    /// let right = UnalignedVector::from_slice(&[3.0_f32, 4.0]);
    /// let pair = SameDimensionPair::try_new(&left, &right)?;
    /// assert_eq!(pair.dimension().get(), 2);
    /// # Ok::<(), db::search::vector::VectorDimensionError>(())
    /// ```
    pub fn try_new(
        left: &'a UnalignedVector<f32>,
        right: &'a UnalignedVector<f32>,
    ) -> Result<Self, VectorDimensionError> {
        let dimension = VectorDimension::try_new(left.len())?;
        let left = VectorRef::try_new(left, dimension)?;
        let right = VectorRef::try_new(right, dimension)?;
        Ok(Self { left, right })
    }

    /// Return the checked left vector.
    pub const fn left(self) -> VectorRef<'a> {
        self.left
    }

    /// Return the checked right vector.
    pub const fn right(self) -> VectorRef<'a> {
        self.right
    }

    /// Return the shared non-zero dimension.
    pub const fn dimension(self) -> VectorDimension {
        self.left.dimension()
    }
}

/// Invalid vector-dimension input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VectorDimensionError {
    /// A distance kernel cannot operate on a zero-dimensional vector.
    #[error("vector dimension must be non-zero")]
    ZeroDimension,
    /// The dimension exceeds the configured engine limit.
    #[error("vector dimension {actual} exceeds maximum {maximum}")]
    ExceedsMaximum {
        /// Configured maximum element count.
        maximum: usize,
        /// Requested element count.
        actual: usize,
    },
    /// A vector did not match the authoritative dimension.
    #[error("vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Authoritative element count.
        expected: usize,
        /// Observed element count.
        actual: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_ref_checks_authoritative_dimension() {
        let values = UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
        let dimension = VectorDimension::try_new(3).unwrap();
        let checked = VectorRef::try_new(&values, dimension).unwrap();
        assert_eq!(checked.dimension(), dimension);
        assert_eq!(checked.values().to_vec(), vec![1.0, 2.0, 3.0]);

        assert_eq!(
            VectorRef::try_new(&values, VectorDimension::try_new(2).unwrap()).unwrap_err(),
            VectorDimensionError::DimensionMismatch {
                expected: 2,
                actual: 3,
            }
        );
    }

    #[test]
    fn vector_dimension_checks_configured_maximum() {
        let maximum = NonZeroUsize::new(3).unwrap();
        assert_eq!(
            VectorDimension::try_new_with_max(3, maximum).unwrap().get(),
            3
        );
        assert_eq!(
            VectorDimension::try_new_with_max(4, maximum).unwrap_err(),
            VectorDimensionError::ExceedsMaximum {
                maximum: 3,
                actual: 4,
            }
        );
    }

    #[test]
    fn same_dimension_pair_rejects_zero_and_mismatch() {
        let empty = UnalignedVector::<f32>::from_slice(&[]);
        assert_eq!(
            SameDimensionPair::try_new(&empty, &empty).unwrap_err(),
            VectorDimensionError::ZeroDimension
        );

        let left = UnalignedVector::<f32>::from_slice(&[1.0, 2.0]);
        let right = UnalignedVector::<f32>::from_slice(&[1.0]);
        assert_eq!(
            SameDimensionPair::try_new(&left, &right).unwrap_err(),
            VectorDimensionError::DimensionMismatch {
                expected: 2,
                actual: 1,
            }
        );
        assert_eq!(
            SameDimensionPair::try_new(&empty, &right).unwrap_err(),
            VectorDimensionError::ZeroDimension
        );
        assert_eq!(
            SameDimensionPair::try_new(&left, &empty).unwrap_err(),
            VectorDimensionError::DimensionMismatch {
                expected: 2,
                actual: 0,
            }
        );
    }
}
