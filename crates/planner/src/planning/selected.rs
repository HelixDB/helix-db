//! Selected Cascades lowering boundary.
//!
//! This module owns request-scoped conversion from native logical roots into
//! selected executable IR roots. It may run Cascades selection, cache selected
//! logical roots within one planning request, and merge planner metrics. It
//! must not construct executable steps; those stay on the adjacent executable
//! IR boundary.
//!
//! Cache hits are validated with full logical-expression equality after digest
//! lookup, and recursive child planning work is merged without double-counting
//! selected execution cost already carried by the selected parent alternative.

mod cache;
mod control;
mod lowering;
mod metrics;
mod native;
mod rejection;
mod root;
mod session;
mod trace;

use self::session::SelectedCascadesPlanner;

pub(super) use self::native::{
    cascades_batch_entries_from_ast, cascades_batch_entries_from_ast_entries,
};
pub(super) use self::trace::append_selected_trace;
