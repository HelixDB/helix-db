//! Access-backed root-stream contract adaptation.

use crate::{context, cost, logical, physical};

use super::contract::RootStreamPipelineContract;
use crate::rules::physical_contracts::{access as access_contracts, support};

pub(super) fn access_stream_pipeline_contract(
    input: &logical::AccessStream,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> RootStreamPipelineContract {
    match input {
        logical::AccessStream::Path(access) => {
            let contract = access_contracts::access_path_contract(access, storage, stats);
            RootStreamPipelineContract::new(
                vec![support::access_pipeline_op(access, contract.access)],
                contract.delivered,
                contract.cost,
            )
        }
        logical::AccessStream::Filter(filter) => {
            let (pipeline, delivered, cost) =
                access_contracts::access_filter_pipeline_contract(filter, storage, stats);
            from_pipeline(pipeline, delivered, cost)
        }
        logical::AccessStream::Window(window) => {
            let (pipeline, delivered, cost) =
                access_contracts::access_window_pipeline_contract(window, storage, stats);
            from_pipeline(pipeline, delivered, cost)
        }
        logical::AccessStream::Order(order) => {
            let (pipeline, delivered, cost) =
                access_contracts::access_order_pipeline_contract(order, storage, stats);
            from_pipeline(pipeline, delivered, cost)
        }
        logical::AccessStream::Distinct(distinct) => {
            let (pipeline, delivered, cost) =
                access_contracts::access_distinct_pipeline_contract(distinct, storage, stats);
            from_pipeline(pipeline, delivered, cost)
        }
        logical::AccessStream::Pipeline(access_pipeline) => {
            let (pipeline, delivered, cost) = access_contracts::access_pipeline_physical_contract(
                access_pipeline,
                storage,
                stats,
            );
            from_pipeline(pipeline, delivered, cost)
        }
    }
}

fn from_pipeline(
    pipeline: physical::PhysicalPipeline,
    delivered: crate::properties::DeliveredProperties,
    cost: cost::CostVector,
) -> RootStreamPipelineContract {
    RootStreamPipelineContract::new(pipeline.ops().to_vec(), delivered, cost)
}
