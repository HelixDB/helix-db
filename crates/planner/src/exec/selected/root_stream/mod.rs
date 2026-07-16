//! Selected root-stream and terminal contracts.
//!
//! Stream payloads keep selected recursive inputs explicit. Access and variable
//! sources are described by their logical payloads; any stream-producing root
//! that needs child execution carries a selected subplan instead.

mod input;
mod pipeline;
mod prefix;
mod terminal;

#[cfg(test)]
mod tests;

pub use self::input::SelectedRootStreamInput;
pub use self::pipeline::SelectedRootPipeline;
pub use self::terminal::{SelectedRootTerminal, SelectedRootTerminalPlan};
