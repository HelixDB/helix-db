//! Planner-owned optimizer coverage checklist for semantic lowering helpers
//! that still exist outside Cascades rules.

mod batch_controls;
mod chosen_plans;
mod control_flow;
mod empty_sources;
mod mutation_ddl;
mod parameter_specialization;
mod predicate_limits;
mod runtime_feedback;
mod search_limits;
mod terminal_roots;
