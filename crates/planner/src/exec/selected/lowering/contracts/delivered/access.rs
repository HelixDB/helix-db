//! Delivered properties for selected access streams.

use super::super::*;
use super::pipeline;

pub(in crate::exec::selected::lowering) fn selected_access_stream_delivered_properties(
    input: &logical::AccessStream,
) -> properties::DeliveredProperties {
    match input {
        logical::AccessStream::Path(access) => selected_access_path_delivered_properties(access),
        logical::AccessStream::Filter(filter) => filtered_delivered_properties(
            selected_access_path_delivered_properties(filter.access()),
        ),
        logical::AccessStream::Window(window) => {
            let delivered = selected_access_path_delivered_properties(window.access());
            match window.window().end() {
                Some(end) => {
                    range_delivered_properties(delivered, Some((window.window().start(), end)))
                }
                None if window.window().start() > 0 => {
                    skip_delivered_properties(delivered, Some(window.window().start()))
                }
                None => delivered,
            }
        }
        logical::AccessStream::Order(order) => ordered_delivered_properties(
            materialized_delivered_properties(selected_access_path_delivered_properties(
                order.access(),
            )),
            properties::DeliveredOrdering::ByKeys(order.ordering().clone()),
        ),
        logical::AccessStream::Distinct(distinct) => {
            materialized_delivered_properties(filtered_delivered_properties(
                selected_access_path_delivered_properties(distinct.access()),
            ))
        }
        logical::AccessStream::Pipeline(pipeline) => pipeline.ops().iter().fold(
            selected_access_path_delivered_properties(pipeline.access()),
            pipeline::selected_stream_pipeline_delivered_properties,
        ),
    }
}

pub(in crate::exec::selected::lowering) fn selected_access_path_delivered_properties(
    access: &logical::AccessPath,
) -> properties::DeliveredProperties {
    match access {
        logical::AccessPath::Node(path) => node_access_delivered_properties(path.source().as_ref()),
        logical::AccessPath::Edge(path) => edge_access_delivered_properties(path.source().as_ref()),
    }
}
