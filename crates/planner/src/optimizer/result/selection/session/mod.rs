//! Reusable recursive physical-selection session.

mod api;
mod cache;
mod candidate;
mod cost;
mod source;

use std::collections::{BTreeMap, BTreeSet};

use crate::{cost as planner_cost, memo};

use super::super::OptimizationResult;
use super::super::PhysicalAlternativeEntry;

/// Reusable recursive physical-selection session for one optimization result.
///
/// The session keeps the default selected-alternative cache across multiple
/// root lookups. This is the right boundary for selected batch extraction,
/// where independently requested roots can share recursive memo-child groups.
#[derive(Debug)]
pub struct SelectionSession<'a> {
    pub(super) result: &'a OptimizationResult,
    pub(super) default_selection_cache: BTreeMap<memo::MemoGroupId, CachedDefaultSelection<'a>>,
    pub(super) visiting: BTreeSet<memo::MemoGroupId>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CachedDefaultSelection<'a> {
    pub(super) entry: &'a PhysicalAlternativeEntry,
    pub(super) selected_cost: planner_cost::CostVector,
}

impl<'a> SelectionSession<'a> {
    pub(super) fn new(result: &'a OptimizationResult) -> Self {
        Self {
            result,
            default_selection_cache: BTreeMap::new(),
            visiting: BTreeSet::new(),
        }
    }
}
