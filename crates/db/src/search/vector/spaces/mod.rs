//! Architecture-dispatched vector arithmetic kernels.
//!
//! Public distance types enter through checked dimension proofs in [`simple`].
//! Architecture-specific implementations are private dispatch targets and do
//! not define persistence or descriptor identity.

pub mod simple;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub(super) mod simple_sse;

#[cfg(target_arch = "x86_64")]
pub(super) mod simple_avx;

#[cfg(target_arch = "aarch64")]
pub(super) mod simple_neon;
