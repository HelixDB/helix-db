pub mod simple;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simple_sse;

#[cfg(target_arch = "x86_64")]
mod simple_avx;

#[cfg(target_arch = "aarch64")]
mod simple_neon;
