//! Deprecated V1 paths retained for migrations and compatibility checks.

#![allow(deprecated, unused_imports)]

#[deprecated(note = "use encoding::v2::keys::indexes")]
pub mod indexes;
#[deprecated(note = "use encoding::v2::keys")]
pub mod keys;
#[deprecated(note = "use encoding::v2::values::property")]
pub mod property;
#[deprecated(note = "use encoding::v2::values")]
pub mod values;

#[deprecated(note = "use encoding::v2::keys::codec::read_u64")]
pub(crate) use crate::encoding::v2::keys::codec::read_u64;
