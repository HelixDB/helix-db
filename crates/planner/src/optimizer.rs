//! Cascades optimizer public contract.
//!
//! The optimizer facade re-exports the stable ADTs used by rule
//! implementations, planning entry points, and tests. Implementation details
//! live in contract-focused submodules so the exploration driver does not own
//! config, rule result, provenance, or physical selection APIs.

mod config;
mod driver;
mod memoize;
mod ordering;
mod provenance;
mod registry;
mod result;
mod rule;

pub use self::{
    config::OptimizerConfig,
    driver::CascadesOptimizer,
    provenance::RuleProvenance,
    registry::{OptimizerRuleRegistry, OptimizerRuleRegistryError},
    result::{
        GroupAlternatives, OptimizationResult, OptimizerGuardrail, PhysicalAlternativeEntry,
        RootSelectionFailure, RootSelectionSummary, SelectedPhysicalAlternative, SelectionError,
        SelectionSession,
    },
    rule::{OptimizerRule, RuleEffect, RuleInput, RuleResult},
};

#[cfg(test)]
mod tests;
