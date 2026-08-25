use super::*;

#[test]
fn access_path_contract_covers_edge_source_families() {
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(11),
        range_next: cost::LatencyEstimate::micros(2),
        source_inject_overhead: cost::LatencyEstimate::micros(5),
        cpu_predicate_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(50),
        ..cost::StorageCostProfile::default()
    };
    let label = name("LIKES");
    let edge_status = catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap();
    let edge_weight = catalog::ScopedPropertyDirectionKey::try_new(
        "LIKES",
        "weight",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .unwrap();
    let stats = context::StatsSnapshot::default()
        .with_edge_label_cardinality(label.clone(), 4)
        .with_edge_eq_cardinality(edge_status.clone(), 6)
        .with_edge_range_cardinality(edge_weight.clone(), 8);

    let empty = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::Empty),
        &storage,
        &stats,
    );
    let points = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![4, 2]),
        }),
        &storage,
        &stats,
    );
    let runtime_param = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::FromParam {
            param: name("edges"),
        }),
        &storage,
        &stats,
    );
    let runtime_var = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::FromVar {
            variable: name("edges"),
        }),
        &storage,
        &stats,
    );
    let all_scan = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::AllScan),
        &storage,
        &stats,
    );
    let label_scan = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::LabelScan {
            label: label.clone(),
        }),
        &storage,
        &stats,
    );
    let equality = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::EqualityIndex {
            index: catalog::EdgeEqualityIndexMeta::try_new("edge_status").unwrap(),
            key: edge_status,
            value: equality_literal(1),
        }),
        &storage,
        &stats,
    );
    let range = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::try_new("edge_weight").unwrap(),
            key: edge_weight,
            range: lower_range(1),
        }),
        &storage,
        &stats,
    );
    let vector = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::VectorSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
            index: ir::SearchIndexPlan {
                index_id: name("likes_embedding"),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                helix_ast::value::PropertyValue::F32Array(vec![0.5]),
            ))
            .unwrap(),
            k: search_limit(3),
        }),
        &storage,
        &stats,
    );
    let text = access_path_contract(
        &edge_access_path(edge_text_search(search_limit(2))),
        &storage,
        &stats,
    );
    let union = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
                ids: element_ids(vec![10, 20]),
            })
            .unwrap(),
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
        ))),
        &storage,
        &stats,
    );
    let intersection = access_path_contract(
        &edge_access_path(ir::EdgeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(
                ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
                    ids: element_ids(vec![10, 20]),
                })
                .unwrap(),
                ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
            ),
        )),
        &storage,
        &stats,
    );
    let filtered = edge_access_contract(
        &ir::EdgeAccessPlan::ScanThenFilter {
            source: ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan { label }).unwrap(),
            residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        },
        &storage,
        &stats,
    );

    assert_eq!(empty.estimated_rows, cost::EstimatedRows::ZERO);
    assert_eq!(points.estimated_rows, cost::EstimatedRows::rows(2));
    assert_eq!(runtime_param.cost, storage.source_inject());
    assert_eq!(runtime_var.cost, storage.source_inject());
    assert_eq!(
        all_scan.cost,
        storage.range_scan(storage.default_unknown_scan_rows)
    );
    assert_eq!(
        label_scan.cost,
        storage.range_scan(cost::EstimatedRows::rows(4))
    );
    assert_eq!(
        equality.cost,
        storage
            .bitmap_equality_lookup(cost::EstimatedRows::rows(6))
            .serial(storage.secondary_row_materialization(cost::EstimatedRows::rows(6)))
    );
    assert_eq!(
        range.cost,
        storage
            .secondary_range_lookup(cost::EstimatedRows::rows(8))
            .serial(storage.secondary_row_materialization(cost::EstimatedRows::rows(8)))
    );
    assert!(matches!(
        range.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        vector.cost,
        storage.range_scan(cost::EstimatedRows::rows(3))
    );
    assert_eq!(text.cost, storage.range_scan(cost::EstimatedRows::rows(2)));
    assert_eq!(union.estimated_rows, cost::EstimatedRows::rows(2));
    assert_eq!(intersection.estimated_rows, cost::EstimatedRows::ZERO);
    assert_eq!(
        filtered.cost,
        storage
            .range_scan(cost::EstimatedRows::rows(4))
            .serial(storage.predicate_eval(cost::EstimatedRows::rows(4)))
    );
}
