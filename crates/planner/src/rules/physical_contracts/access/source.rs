//! Source access physical contracts.
//!
//! This facade keeps the rule-facing access contract API stable while shared
//! source costing and node/edge source adapters evolve independently.

mod edge;
mod node;
mod shared;

use crate::{context, cost, logical};

use super::contract::AccessPhysicalContract;

pub(in crate::rules) fn access_path_contract(
    access: &logical::AccessPath,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> AccessPhysicalContract {
    match access {
        logical::AccessPath::Node(path) => {
            node::node_access_contract(path.source().as_ref(), storage, stats)
        }
        logical::AccessPath::Edge(path) => {
            edge::edge_access_contract(path.source().as_ref(), storage, stats)
        }
    }
}

#[cfg(test)]
pub(in crate::rules) use edge::edge_access_contract;
#[cfg(test)]
pub(in crate::rules) use node::node_access_contract;
