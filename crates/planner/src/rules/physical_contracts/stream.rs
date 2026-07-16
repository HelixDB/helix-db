mod contracts;

pub(in crate::rules) use self::contracts::StreamPhysicalContract;
use self::contracts::{StreamPhysicalContractRejection, StreamPhysicalImplementation};
use super::support::*;
use crate::{cost, logical, physical, properties};

pub(in crate::rules) fn stream_physical_contract(
    op: &logical::PureLogicalOp,
    storage: &cost::StorageCostProfile,
) -> StreamPhysicalContract {
    stream_physical_contract_for_rows(op, storage, storage.default_unknown_scan_rows)
}

pub(in crate::rules) fn stream_physical_contract_for_rows(
    op: &logical::PureLogicalOp,
    storage: &cost::StorageCostProfile,
    rows: cost::EstimatedRows,
) -> StreamPhysicalContract {
    let implementation = match op {
        logical::PureLogicalOp::Limit { count } => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Limit,
            bounded_delivered(stream_bound_upper(count)),
            storage.stream_operator(estimated_rows_bounded_by(rows, stream_bound_upper(count))),
        ),
        logical::PureLogicalOp::Skip { .. } => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Skip,
            properties::DeliveredProperties::default(),
            storage.stream_operator(rows),
        ),
        logical::PureLogicalOp::Range { range } => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Range,
            bounded_delivered(stream_range_upper(range)),
            storage.stream_operator(estimated_rows_bounded_by(rows, stream_range_upper(range))),
        ),
        logical::PureLogicalOp::Distinct => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Distinct,
            materialized_delivered(),
            storage.explicit_sort(rows),
        ),
        logical::PureLogicalOp::Expand { element } => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Expand,
            properties::DeliveredProperties {
                element: Some(*element),
                ..properties::DeliveredProperties::default()
            },
            storage.stream_operator(rows),
        ),
        logical::PureLogicalOp::Project => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Project,
            properties::DeliveredProperties {
                element: None,
                ..properties::DeliveredProperties::default()
            },
            storage.stream_operator(rows),
        ),
        logical::PureLogicalOp::Aggregate => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Aggregate,
            properties::DeliveredProperties {
                element: None,
                materialization: properties::Materialization::Materialized,
                ..properties::DeliveredProperties::default()
            },
            storage.explicit_sort(rows),
        ),
        logical::PureLogicalOp::Variable => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Variable,
            properties::DeliveredProperties::default(),
            storage.source_inject(),
        ),
        logical::PureLogicalOp::Reserved => StreamPhysicalImplementation::new(
            physical::PhysicalStreamOp::Reserved,
            properties::DeliveredProperties::default(),
            storage.stream_operator(rows),
        ),
        logical::PureLogicalOp::Source { .. }
        | logical::PureLogicalOp::NoOp
        | logical::PureLogicalOp::Empty
        | logical::PureLogicalOp::Filter { .. }
        | logical::PureLogicalOp::Order { .. } => {
            return StreamPhysicalContract::Unsupported(
                StreamPhysicalContractRejection::UnsupportedPureOp(op.kind()),
            );
        }
    };
    StreamPhysicalContract::Implemented(implementation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ir, properties};

    #[test]
    fn stream_physical_contract_reports_unsupported_pure_op_kinds() {
        let storage = cost::StorageCostProfile::default();
        let unsupported = logical::PureLogicalOp::Filter {
            predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        };

        assert_eq!(
            stream_physical_contract(&unsupported, &storage),
            StreamPhysicalContract::Unsupported(
                StreamPhysicalContractRejection::UnsupportedPureOp(
                    logical::PureLogicalOpKind::Filter
                )
            )
        );
    }

    #[test]
    fn stream_physical_contract_carries_implemented_parts() {
        let storage = cost::StorageCostProfile {
            stream_operator_eval: cost::LatencyEstimate::micros(3),
            default_unknown_scan_rows: cost::EstimatedRows::rows(10),
            ..cost::StorageCostProfile::default()
        };

        let StreamPhysicalContract::Implemented(implementation) = stream_physical_contract(
            &logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Literal(4),
            },
            &storage,
        ) else {
            panic!("limit is a standalone stream op");
        };
        let (op, delivered, cost) = implementation.into_parts();

        assert_eq!(op, physical::PhysicalStreamOp::Limit);
        assert_eq!(delivered.cardinality.upper(), Some(4));
        assert_eq!(cost.latency, cost::LatencyEstimate::micros(12));
    }

    #[test]
    fn stream_physical_contract_tracks_expand_element_delivery() {
        let storage = cost::StorageCostProfile::default();
        let StreamPhysicalContract::Implemented(implementation) = stream_physical_contract(
            &logical::PureLogicalOp::Expand {
                element: properties::ElementKind::Edge,
            },
            &storage,
        ) else {
            panic!("expand is a standalone stream op");
        };
        let (_, delivered, _) = implementation.into_parts();

        assert_eq!(delivered.element, Some(properties::ElementKind::Edge));
    }
}
