//! Shared lifecycle value discriminators.

pub(crate) const INDEX_RECORD_KIND: u8 = 0x01;
pub(crate) const OPERATION_RECORD_KIND: u8 = 0x02;
pub(crate) const BUILD_DELTA_KIND: u8 = 0x03;
pub(crate) const APPLIED_STATE_KIND: u8 = 0x04;
