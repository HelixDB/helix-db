//! Unit-style coverage for the complete encoding tree in isolation.
//!
//! The target includes production encoding sources by path while re-exporting
//! their owning DTO modules from `db`. This exercises private codec contracts
//! without copying persisted types or counting test code as production coverage.

#![allow(dead_code, unused_imports)]

// New typed encoding modules intentionally refer to their owning production
// DTOs. Re-export those crate modules so this path-included test target keeps
// exercising the real encoding sources without creating test-only DTO copies.
pub use db::{config, error, search};

#[path = "../src/index_lifecycle/model.rs"]
mod index_lifecycle_model;
pub(crate) use index_lifecycle_model::*;
#[path = "../src/index_lifecycle/metadata.rs"]
mod index_lifecycle_metadata;
#[path = "../src/index_lifecycle/operation.rs"]
mod index_lifecycle_operation;
pub(crate) use index_lifecycle_operation::*;
#[path = "../src/index_lifecycle/work.rs"]
mod index_lifecycle_work;
pub(crate) use index_lifecycle_work::*;

// The path-included encoding tree resolves its owning DTOs through the same
// module shape as the production crate. Keeping the DTO sources path-included
// preserves private codec coverage without publishing persistence internals.
mod index_lifecycle {
    pub(crate) use crate::index_lifecycle_metadata::*;
    pub(crate) use crate::index_lifecycle_model::*;
    pub(crate) use crate::index_lifecycle_operation::*;
    pub(crate) use crate::index_lifecycle_work::TextPartition;

    pub(crate) mod work {
        pub(crate) use crate::index_lifecycle_work::*;
    }
}

#[path = "../src/encoding/mod.rs"]
mod encoding;
