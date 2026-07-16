use super::*;

#[test]
fn access_window_range_composition_covers_bounded_cases() {
    let limited = AccessWindowRange::identity().then_limit(5);

    assert_eq!(limited, AccessWindowRange::new(0, Some(5)).unwrap());
    assert_eq!(
        limited
            .bounded_stream_range()
            .map(|range| (range.start(), range.end())),
        Some((0, 5))
    );
    assert_eq!(
        limited.then_skip(2),
        AccessWindowRange::new(2, Some(5)).unwrap()
    );
    assert_eq!(
        limited.then_skip(10),
        AccessWindowRange::new(5, Some(5)).unwrap()
    );
    assert_eq!(
        limited.then_range(&ir::StreamLiteralRange::new(1, 4).unwrap()),
        AccessWindowRange::new(1, Some(4)).unwrap()
    );
    assert_eq!(
        limited.then_range(&ir::StreamLiteralRange::new(10, 12).unwrap()),
        AccessWindowRange::new(5, Some(5)).unwrap()
    );
    assert!(limited.fully_contains_bounded_prefix(5));
    assert!(limited.fully_contains_bounded_prefix(0));
    assert!(!limited.fully_contains_bounded_prefix(6));
    assert!(!AccessWindowRange::new(1, Some(5))
        .unwrap()
        .fully_contains_bounded_prefix(5));
}

#[test]
fn access_window_range_rejects_inverted_deserialization() {
    assert!(
        serde_json::from_value::<AccessWindowRange>(serde_json::json!({
            "start": 8,
            "end": 2,
        }))
        .is_err()
    );

    let open = AccessWindowRange::new(3, None).unwrap();
    assert!(open.bounded_stream_range().is_none());
}

#[test]
fn access_pipeline_constructor_rejects_non_canonical_windows() {
    let access = node_access_path(ir::NodeAccessPlan::AllScan);
    let identity = StreamPipelineOp::Window {
        window: AccessWindowRange::new(0, None).unwrap(),
    };
    let first = StreamPipelineOp::Window {
        window: AccessWindowRange::new(0, Some(5)).unwrap(),
    };
    let second = StreamPipelineOp::Window {
        window: AccessWindowRange::new(1, Some(3)).unwrap(),
    };

    assert!(AccessPipeline::new(access.clone(), ir::AtLeast::<_, 1>::from_one(identity)).is_none());
    assert!(AccessPipeline::new(
        access,
        ir::AtLeast::<_, 1>::from_one_and_rest(first, vec![second]),
    )
    .is_none());
}
