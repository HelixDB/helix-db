//! Access-stream and stream-pipeline selected lowering.
//!
//! This layer owns the executable translation for selected access contracts and
//! scalar stream operators above already-lowered inputs. Submodules keep the
//! contract explicit: leaf access allocation, access-path matching, access
//! stream wrappers, and reusable stream-pipeline operators are separate units.

use super::contracts::*;
use super::*;

mod bounds;
mod leaf;
mod native;
mod path;
mod pipeline;
mod stream;

pub(in crate::exec::selected::lowering) use bounds::{WindowAccessReadPlan, WindowSuffix};
