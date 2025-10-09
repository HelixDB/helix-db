use half::f16;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents vector data with different floating-point precisions
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum VectorData {
    F64(Vec<f64>),
    F32(Vec<f32>),
    F16(Vec<f16>),
}

impl VectorData {
    /// Returns the number of elements in the vector
    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            VectorData::F64(v) => v.len(),
            VectorData::F32(v) => v.len(),
            VectorData::F16(v) => v.len(),
        }
    }

    /// Returns true if the vector is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the precision type as a string
    pub fn precision(&self) -> &'static str {
        match self {
            VectorData::F64(_) => "f64",
            VectorData::F32(_) => "f32",
            VectorData::F16(_) => "f16",
        }
    }

    /// Creates VectorData from f64 slice
    #[inline(always)]
    pub fn from_f64_slice(data: &[f64]) -> Self {
        VectorData::F64(data.to_vec())
    }

    /// Creates VectorData from f32 slice
    #[inline(always)]
    pub fn from_f32_slice(data: &[f32]) -> Self {
        VectorData::F32(data.to_vec())
    }

    /// Creates VectorData from f16 slice
    #[inline(always)]
    pub fn from_f16_slice(data: &[f16]) -> Self {
        VectorData::F16(data.to_vec())
    }
}

impl fmt::Debug for VectorData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorData::F64(v) => write!(f, "f64({v:?})"),
            VectorData::F32(v) => write!(f, "f32({v:?})"),
            VectorData::F16(v) => write!(f, "f16({v:?})"),
        }
    }
}
