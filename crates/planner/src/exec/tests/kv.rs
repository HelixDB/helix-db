use super::*;

#[test]
fn element_keyspaces_expose_stable_names_and_element_kinds() {
    let cases = [
        (
            ElementKeyspace::NodeProperty,
            "node_property",
            properties::ElementKind::Node,
        ),
        (
            ElementKeyspace::EdgeEndpoints,
            "edge_endpoints",
            properties::ElementKind::Edge,
        ),
    ];

    for (keyspace, name, element) in cases {
        assert_eq!(keyspace.as_str(), name);
        assert_eq!(keyspace.to_string(), name);
        assert_eq!(keyspace.element(), element);
        assert_eq!(
            serde_json::from_str::<ElementKeyspace>(&format!(r#""{name}""#)).unwrap(),
            keyspace
        );
        assert_eq!(serde_json::to_value(keyspace).unwrap(), name);
    }
}

#[test]
fn point_keys_are_fixed_width_big_endian_element_ids() {
    for invalid in [Vec::new(), vec![1; 7], vec![1; 9]] {
        assert!(KvKey::new(ElementKeyspace::NodeProperty, invalid).is_none());
    }

    let node = ElementKeyspace::NodeProperty.point_key(0x0102_0304_0506_0708);
    assert_eq!(node.keyspace(), ElementKeyspace::NodeProperty);
    assert_eq!(node.id(), 0x0102_0304_0506_0708);
    assert_eq!(node.bytes(), &0x0102_0304_0506_0708_u64.to_be_bytes());

    let edge = KvKey::new(
        ElementKeyspace::EdgeEndpoints,
        u64::MAX.to_be_bytes().to_vec(),
    )
    .unwrap();
    assert_eq!(edge.keyspace(), ElementKeyspace::EdgeEndpoints);
    assert_eq!(edge.id(), u64::MAX);
    assert_eq!(edge.bytes(), &u64::MAX.to_be_bytes());
}

#[test]
fn range_bound_keys_are_keyspace_free_fixed_width_ids() {
    for invalid in [Vec::new(), vec![1; 7], vec![1; 9]] {
        assert!(KvBoundKey::new(invalid).is_none());
    }

    let bound = KvBoundKey::from_id(42);
    assert_eq!(bound.id(), 42);
    assert_eq!(bound.bytes(), &42_u64.to_be_bytes());

    assert!(matches!(
        KvKeyBound::included_id(7),
        KvKeyBound::Included(key) if key.id() == 7
    ));
    assert!(matches!(
        KvKeyBound::excluded_id(9),
        KvKeyBound::Excluded(key) if key.id() == 9
    ));
    assert_eq!(
        serde_json::to_value(KvKeyBound::Unbounded).unwrap(),
        "unbounded"
    );
}

#[test]
fn range_and_prefix_scan_serde_preserves_lsm_key_contracts() {
    let range = KvReadPlan::RangeScan {
        keyspace: ElementKeyspace::NodeProperty,
        start: KvKeyBound::included_id(1),
        end: KvKeyBound::excluded_id(9),
        limit: Some(properties::PositiveUsize::new(3).unwrap()),
    };
    let range_json = serde_json::to_value(&range).unwrap();
    assert_eq!(
        serde_json::from_value::<KvReadPlan>(range_json).unwrap(),
        range
    );

    let prefix = KvReadPlan::PrefixScan {
        keyspace: ElementKeyspace::EdgeEndpoints,
        prefix: ir::AtLeast::<_, 1>::from_one_and_rest(0xAA, vec![0xBB]),
        limit: None,
    };
    let prefix_json = serde_json::to_value(&prefix).unwrap();
    assert_eq!(
        serde_json::from_value::<KvReadPlan>(prefix_json).unwrap(),
        prefix
    );
}

#[test]
fn multi_get_plan_sorts_keys_and_records_original_positions() {
    let plan = KvMultiGetPlan::new(
        vec![
            key(ElementKeyspace::NodeProperty, 3),
            key(ElementKeyspace::NodeProperty, 1),
            key(ElementKeyspace::NodeProperty, 2),
        ],
        properties::KeyLocality::Close,
        properties::PositiveUsize::new(3).unwrap(),
    )
    .unwrap();

    assert_eq!(
        plan.keys().iter().map(KvKey::id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(plan.original_positions(), &[1, 2, 0]);
}

#[test]
fn multi_get_plan_accessors_preserve_locality_bounds_and_position_pairs() {
    let plan = KvMultiGetPlan::new(
        vec![
            key(ElementKeyspace::EdgeEndpoints, 9),
            key(ElementKeyspace::EdgeEndpoints, 7),
        ],
        properties::KeyLocality::Sparse,
        properties::PositiveUsize::new(4).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.keyspace(), ElementKeyspace::EdgeEndpoints);
    assert_eq!(plan.len(), 2);
    assert!(!plan.is_empty());
    assert_eq!(plan.locality(), properties::KeyLocality::Sparse);
    assert_eq!(plan.max_batch_size().get(), 4);
    assert_eq!(
        plan.keyed_original_positions()
            .map(|(key, position)| (key.id(), position))
            .collect::<Vec<_>>(),
        vec![(7, 1), (9, 0)]
    );
}

#[test]
fn multi_get_prefix_by_original_position_keeps_logical_prefix() {
    let plan = KvMultiGetPlan::new(
        vec![
            key(ElementKeyspace::NodeProperty, 30),
            key(ElementKeyspace::NodeProperty, 10),
            key(ElementKeyspace::NodeProperty, 20),
        ],
        properties::KeyLocality::Close,
        properties::PositiveUsize::new(4).unwrap(),
    )
    .unwrap();

    let prefix = plan
        .prefix_by_original_position(properties::PositiveUsize::new(2).unwrap())
        .unwrap();

    assert_eq!(
        prefix.keys().iter().map(KvKey::id).collect::<Vec<_>>(),
        vec![10, 30]
    );
    assert_eq!(prefix.original_positions(), &[1, 0]);
    assert_eq!(
        prefix
            .keyed_original_positions()
            .map(|(key, position)| (key.id(), position))
            .collect::<Vec<_>>(),
        vec![(10, 1), (30, 0)]
    );
    assert_eq!(
        plan.prefix_by_original_position(properties::PositiveUsize::new(4).unwrap())
            .unwrap(),
        plan
    );
}

#[test]
fn multi_get_rejects_mixed_keyspaces_and_oversized_batches() {
    assert!(matches!(
        KvMultiGetPlan::new(
            Vec::new(),
            properties::KeyLocality::Close,
            properties::PositiveUsize::new(4).unwrap(),
        ),
        Err(ExecPlanError::EmptyMultiGet)
    ));
    assert!(matches!(
        KvMultiGetPlan::new(
            vec![
                key(ElementKeyspace::NodeProperty, 1),
                key(ElementKeyspace::EdgeEndpoints, 2)
            ],
            properties::KeyLocality::Close,
            properties::PositiveUsize::new(4).unwrap(),
        ),
        Err(ExecPlanError::MixedMultiGetKeyspace { .. })
    ));
    assert!(matches!(
        KvMultiGetPlan::new(
            vec![
                key(ElementKeyspace::NodeProperty, 1),
                key(ElementKeyspace::NodeProperty, 2)
            ],
            properties::KeyLocality::Close,
            properties::PositiveUsize::new(1).unwrap(),
        ),
        Err(ExecPlanError::MultiGetBatchTooLarge { .. })
    ));
}

#[test]
fn coalesce_multi_get_batches_groups_by_keyspace_and_locality_cap() {
    let profile = cost::StorageCostProfile {
        close_key_multi_get_batch: properties::PositiveUsize::new(2).unwrap(),
        ..cost::StorageCostProfile::default()
    };
    let batches = coalesce_multi_get_batches(
        vec![
            key(ElementKeyspace::NodeProperty, 3),
            key(ElementKeyspace::EdgeEndpoints, 1),
            key(ElementKeyspace::NodeProperty, 1),
            key(ElementKeyspace::NodeProperty, 2),
        ],
        properties::KeyLocality::Close,
        &profile,
    )
    .unwrap();

    assert_eq!(batches.len(), 3);
    assert_eq!(
        batches
            .iter()
            .map(|batch| (batch.keyspace(), batch.len()))
            .collect::<Vec<_>>(),
        vec![
            (ElementKeyspace::NodeProperty, 2),
            (ElementKeyspace::NodeProperty, 1),
            (ElementKeyspace::EdgeEndpoints, 1),
        ]
    );
}

#[test]
fn coalesce_non_empty_multi_get_batches_preserves_non_empty_output() {
    let profile = cost::StorageCostProfile::default();
    let batches = coalesce_non_empty_multi_get_batches(
        ir::AtLeast::<_, 1>::from_one(key(ElementKeyspace::NodeProperty, 7)),
        properties::KeyLocality::Close,
        &profile,
    )
    .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches.as_ref()[0].keys().len(), 1);
}

#[test]
fn prepared_multi_get_keys_encode_keyspace_and_batch_bound() {
    let ids = ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(3, vec![1, 2])).unwrap();

    assert!(KvMultiGetKeys::from_element_ids(
        ElementKeyspace::NodeProperty,
        &ids,
        properties::PositiveUsize::new(2).unwrap(),
    )
    .is_none());

    let keys = KvMultiGetKeys::from_element_ids(
        ElementKeyspace::NodeProperty,
        &ids,
        properties::PositiveUsize::new(3).unwrap(),
    )
    .unwrap();
    assert_eq!(keys.keyspace(), ElementKeyspace::NodeProperty);
    assert_eq!(keys.len(), 3);
    assert!(!keys.is_empty());
    assert_eq!(keys.max_batch_size().get(), 3);

    let plan = KvMultiGetPlan::from_keys(keys, properties::KeyLocality::Close);
    assert_eq!(
        plan.keys().iter().map(KvKey::id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(plan.original_positions(), &[1, 2, 0]);
}

#[test]
fn coalesce_multi_get_batches_chunks_by_original_order_inside_keyspace() {
    let profile = cost::StorageCostProfile {
        close_key_multi_get_batch: properties::PositiveUsize::new(2).unwrap(),
        ..cost::StorageCostProfile::default()
    };
    let batches = coalesce_multi_get_batches(
        vec![
            key(ElementKeyspace::NodeProperty, 4),
            key(ElementKeyspace::NodeProperty, 1),
            key(ElementKeyspace::NodeProperty, 3),
            key(ElementKeyspace::NodeProperty, 2),
        ],
        properties::KeyLocality::Close,
        &profile,
    )
    .unwrap();

    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches[0].keys().iter().map(KvKey::id).collect::<Vec<_>>(),
        vec![1, 4]
    );
    assert_eq!(batches[0].original_positions(), &[1, 0]);
    assert_eq!(
        batches[1].keys().iter().map(KvKey::id).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(batches[1].original_positions(), &[3, 2]);
}

#[test]
fn multi_get_deserialization_rejects_broken_invariants() {
    let json = serde_json::json!({
        "keyspace": "node_property",
        "keys": [
            { "keyspace": "node_property", "bytes": 2_u64.to_be_bytes() },
            { "keyspace": "node_property", "bytes": 1_u64.to_be_bytes() }
        ],
        "original_positions": [0, 1],
        "locality": "close",
        "max_batch_size": 2
    });

    assert!(serde_json::from_value::<KvMultiGetPlan>(json).is_err());
}

#[test]
fn multi_get_deserialization_rejects_duplicate_original_positions() {
    let json = serde_json::json!({
        "keyspace": "node_property",
        "keys": [
            { "keyspace": "node_property", "bytes": 1_u64.to_be_bytes() },
            { "keyspace": "node_property", "bytes": 2_u64.to_be_bytes() }
        ],
        "original_positions": [0, 0],
        "locality": "close",
        "max_batch_size": 2
    });

    let error = serde_json::from_value::<KvMultiGetPlan>(json).unwrap_err();
    assert!(error
        .to_string()
        .contains("multi_get original input position 0 appears more than once"));
}

#[test]
fn multi_get_deserialization_rejects_length_mismatch() {
    let json = serde_json::json!({
        "keyspace": "node_property",
        "keys": [
            { "keyspace": "node_property", "bytes": 1_u64.to_be_bytes() },
            { "keyspace": "node_property", "bytes": 2_u64.to_be_bytes() }
        ],
        "original_positions": [0],
        "locality": "close",
        "max_batch_size": 2
    });

    let error = serde_json::from_value::<KvMultiGetPlan>(json).unwrap_err();
    assert!(error
        .to_string()
        .contains("multi_get keys/original_positions length mismatch: 2 != 1"));
}

#[test]
fn multi_get_deserialization_rejects_oversized_wire_batches() {
    let json = serde_json::json!({
        "keyspace": "node_property",
        "keys": [
            { "keyspace": "node_property", "bytes": 1_u64.to_be_bytes() },
            { "keyspace": "node_property", "bytes": 2_u64.to_be_bytes() }
        ],
        "original_positions": [0, 1],
        "locality": "close",
        "max_batch_size": 1
    });

    let error = serde_json::from_value::<KvMultiGetPlan>(json).unwrap_err();
    assert!(error
        .to_string()
        .contains("multi_get has 2 keys but max_batch_size is 1"));
}

#[test]
fn multi_get_deserialization_rejects_mixed_keyspace_wire_batches() {
    let json = serde_json::json!({
        "keyspace": "node_property",
        "keys": [
            { "keyspace": "node_property", "bytes": 1_u64.to_be_bytes() },
            { "keyspace": "edge_endpoints", "bytes": 2_u64.to_be_bytes() }
        ],
        "original_positions": [0, 1],
        "locality": "close",
        "max_batch_size": 2
    });

    let error = serde_json::from_value::<KvMultiGetPlan>(json).unwrap_err();
    assert!(error
        .to_string()
        .contains("multi_get keyspace mismatch: expected node_property, got edge_endpoints"));
}

#[test]
fn multi_get_duplicate_position_error_is_stable() {
    assert_eq!(
        ExecPlanError::DuplicateMultiGetOriginalPosition { position: 2 }.to_string(),
        "multi_get original input position 2 appears more than once"
    );
}

#[test]
fn point_key_deserialization_rejects_raw_keyspace_and_short_ids() {
    let raw_keyspace = serde_json::json!({
        "get": {
            "key": {
                "keyspace": "raw_bytes",
                "bytes": 1_u64.to_be_bytes()
            }
        }
    });
    assert!(serde_json::from_value::<KvReadPlan>(raw_keyspace).is_err());

    let short_id = serde_json::json!({
        "get": {
            "key": {
                "keyspace": "node_property",
                "bytes": [1]
            }
        }
    });
    assert!(serde_json::from_value::<KvReadPlan>(short_id).is_err());
}

#[test]
fn scan_deserialization_rejects_unknown_element_keyspace() {
    let json = serde_json::json!({
        "range_scan": {
            "keyspace": "raw_bytes",
            "start": "unbounded",
            "end": "unbounded",
            "limit": null
        }
    });

    assert!(serde_json::from_value::<KvReadPlan>(json).is_err());
}

#[test]
fn range_bound_deserialization_rejects_short_ids() {
    let json = serde_json::json!({
        "range_scan": {
            "keyspace": "node_property",
            "start": { "included": { "bytes": [1] } },
            "end": "unbounded",
            "limit": null
        }
    });

    assert!(serde_json::from_value::<KvReadPlan>(json).is_err());
}
