//! Static stream-window composition rule facade.
//!
//! The rule wrapper, composition algorithm, and static-window ADTs are split so
//! rewrite policy can evolve without mixing optimizer wiring and range
//! arithmetic.

mod compose;
mod contracts;
mod rule;

pub use rule::StreamCompositionRule;
