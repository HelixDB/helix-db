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

#[path = "../src/index_v2/model.rs"]
mod index_v2_model;
pub(crate) use index_v2_model::*;
#[path = "../src/index_v2/metadata.rs"]
mod index_v2_metadata;
#[path = "../src/index_v2/operation.rs"]
mod index_v2_operation;
pub(crate) use index_v2_operation::*;
#[path = "../src/index_v2/work.rs"]
mod index_v2_work;
pub(crate) use index_v2_work::*;

// The path-included encoding tree resolves its owning DTOs through the same
// module shape as the production crate. Keeping the DTO sources path-included
// preserves private codec coverage without publishing persistence internals.
mod index_v2 {
    pub(crate) use crate::index_v2_metadata::*;
    pub(crate) use crate::index_v2_model::*;
    pub(crate) use crate::index_v2_operation::*;
    pub(crate) use crate::index_v2_work::TextPartition;

    pub(crate) mod work {
        pub(crate) use crate::index_v2_work::*;
    }
}

#[path = "../src/encoding/mod.rs"]
mod encoding;
