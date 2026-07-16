//! LSM-aware key-value read contracts for native executable plans.

mod batch;
mod key;
mod read;

pub use self::batch::{
    coalesce_multi_get_batches, coalesce_non_empty_multi_get_batches, KvMultiGetKeys,
    KvMultiGetPlan,
};
pub use self::key::{ElementKeyspace, KvBoundKey, KvKey, KvKeyBound};
pub use self::read::KvReadPlan;
