//! Query planner for Helix AST.
//!
//! The planner is pure: callers pass a [`context::PlannerContext`] snapshot,
//! and the public planning entrypoints return a validated
//! [`exec::ExecutablePlan`] plus trace and metrics. It never reads SlateDB or
//! materializes index result sets.
//!
//! Public contract modules:
//!
//! - [`context`] defines the immutable request-time planner inputs.
//! - [`catalog`] defines index metadata keyed for efficient lookup.
//! - [`cost`] defines tunable LSM/object-storage cost contracts.
//! - [`digest`] defines stable planner digests for deterministic optimization.
//! - [`experiments`] defines shared scalability fixtures and metric thresholds.
//! - [`feedback`] defines optional immutable runtime feedback snapshots.
//! - [`exec`] defines the executable DAG contract.
//! - [`ir`] defines shared planner IR contracts and operator payload ADTs.
//! - [`logical`], [`memo`], [`optimizer`], [`physical`], [`properties`], and
//!   [`rules`] define the modular optimizer architecture.
//! - [`planning`] contains the entrypoints and orchestration.
//! - [`trace`] records explicit, testable optimization decisions.

#![deny(unsafe_code)]

mod analysis;

pub mod catalog;
pub mod context;
pub mod cost;
pub mod digest;
pub mod error;
pub mod exec;
pub mod experiments;
pub mod feedback;
pub mod ir;
pub mod logical;
pub mod memo;
pub mod optimizer;
pub mod physical;
pub mod planning;
pub mod properties;
pub mod rules;
pub mod trace;
