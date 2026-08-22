//! Deprecated property-value paths.

#![allow(clippy::module_inception, deprecated, unused_imports)]

#[deprecated(note = "use encoding::v2::values::property::canonical_number")]
pub(crate) mod canonical_number {
    #[deprecated(note = "use encoding::v2::values::property::canonical_number")]
    pub(crate) use crate::encoding::v2::values::property::canonical_number::*;
}

#[deprecated(note = "use encoding::v2::values::property::equality_index_value")]
pub(crate) mod equality_value {
    #[deprecated(note = "use encoding::v2::values::property::equality_index_value")]
    pub(crate) use crate::encoding::v2::values::property::equality_index_value::*;
}

#[deprecated(note = "use encoding::v2::values::property::property")]
pub(crate) mod property {
    #[deprecated(note = "use encoding::v2::values::property::property")]
    pub use crate::encoding::v2::values::property::property::*;
}

#[deprecated(note = "use encoding::v2::values::property::property_value")]
pub(crate) mod property_value {
    #[deprecated(note = "use encoding::v2::values::property::property_value")]
    pub(crate) use crate::encoding::v2::values::property::property_value::*;
}

#[deprecated(note = "use encoding::v2::values::property::range_index_value")]
pub(crate) mod range_value {
    #[deprecated(note = "use encoding::v2::values::property::range_index_value")]
    pub(crate) use crate::encoding::v2::values::property::range_index_value::*;
}

#[deprecated(note = "use encoding::v2::values::property")]
pub use crate::encoding::v2::values::property::Property;
#[deprecated(note = "use encoding::v2::values::property")]
pub(crate) use crate::encoding::v2::values::property::{
    decode_properties, encode_index_partition_value, encode_properties,
};
