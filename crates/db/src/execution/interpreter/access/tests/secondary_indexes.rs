//! Secondary-index access, lease, and canonical-row integration tests.

use super::support::*;
use crate::config::SecondaryIndexDefinition;
use crate::encoding::indexes::hash_property_value;
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
use crate::encoding::v1::keys::index_v2::{
    CanonicalSecondaryValue, IndexV2Key, SecondaryEntryKey, SecondaryEntryLane,
};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::index_v2::{
    encode_index_record, encode_work_value, IndexV2WorkValue,
};
use crate::error::{HelixDbError, IndexFamily, IndexLifecycleUnavailableReason};
use crate::index_v2::reader_lease::LeaseGenerationKey;
use crate::index_v2::work::SecondaryEntryValue;
use crate::index_v2::{
    IndexEntityId, IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
    IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition,
};

/// Seeds an Active generation and registers its exact reader-lease identity.
async fn seed_active_secondary_generation(
    db: &HelixDB,
    definition: SecondaryIndexDefinition,
    index_id: u64,
    rows: &[(&str, u64)],
) -> (LeaseGenerationKey, crate::index_v2::IndexIdentity) {
    let definition = ValidatedDynamicIndexDefinition::try_from(definition)
        .expect("managed secondary fixture definition validates");
    let identity = definition.identity();
    let index_id = IndexId::new(index_id).expect("managed fixture index ID is positive");
    let generation = IndexGenerationId::initial();
    let building = IndexRecordV2::building(
        index_id,
        definition,
        IndexRevision::initial(),
        PhysicalGeneration::Secondary { generation },
        IndexOperationId::new_v4(),
    )
    .expect("managed secondary fixture starts building");
    let active = building
        .transition(IndexStateTransition::Activate)
        .expect("managed secondary fixture activates");
    let handle =
        crate::index_v2::ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("managed secondary fixture projects an Active handle");
    db.inner_db()
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::IndexV2(IndexV2Key::index_record(identity.clone())),
            }
            .to_bytes(),
            encode_index_record(&active),
        )
        .await
        .expect("managed secondary Active record persists");

    let definition = handle.secondary_definition().unwrap();
    let lane = match definition {
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: false, .. } => {
            SecondaryEntryLane::NodeEquality
        }
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. } => {
            SecondaryEntryLane::NodeUniqueEquality
        }
        ValidatedSecondaryIndexDefinition::NodeRange {
            direction: crate::config::RangeIndexDirection::Asc,
            ..
        } => SecondaryEntryLane::NodeRangeAscending,
        ValidatedSecondaryIndexDefinition::NodeRange {
            direction: crate::config::RangeIndexDirection::Desc,
            ..
        } => SecondaryEntryLane::NodeRangeDescending,
        ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => SecondaryEntryLane::EdgeEquality,
        ValidatedSecondaryIndexDefinition::EdgeRange {
            direction: crate::config::RangeIndexDirection::Asc,
            ..
        } => SecondaryEntryLane::EdgeRangeAscending,
        ValidatedSecondaryIndexDefinition::EdgeRange {
            direction: crate::config::RangeIndexDirection::Desc,
            ..
        } => SecondaryEntryLane::EdgeRangeDescending,
    };
    for (value, entity_id) in rows {
        let canonical = match definition {
            ValidatedSecondaryIndexDefinition::NodeEquality { .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
                CanonicalSecondaryValue::equality(hash_property_value(value))
            }
            ValidatedSecondaryIndexDefinition::NodeRange { direction, .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { direction, .. } => {
                let direction = match direction {
                    crate::config::RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
                    crate::config::RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
                };
                CanonicalSecondaryValue::range(direction, value)
            }
        };
        let entity_id = IndexEntityId::new(*entity_id);
        let key = SecondaryEntryKey::try_new(
            index_id,
            generation,
            lane,
            canonical,
            (!lane.is_unique()).then_some(entity_id),
        )
        .expect("managed secondary entry key validates");
        db.inner_db()
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::SecondaryEntry(key)),
                }
                .to_bytes(),
                encode_work_value(&IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
                    index_id,
                    generation,
                    lane,
                    entity_id,
                })),
            )
            .await
            .expect("managed secondary entry persists");
    }
    let lease_generation = LeaseGenerationKey::new(DataScope::LegacyUnscoped, index_id, generation);
    db.reader_lease_coordinator()
        .expect("process-local managed fixture has reader coordination")
        .register_generation(lease_generation)
        .await
        .expect("managed secondary generation registers");
    (lease_generation, identity)
}

#[tokio::test]
async fn managed_secondary_access_uses_leased_v2_rows_and_releases_before_drain() {
    let db = test_support::open_db("access-managed-secondary-v2").await;
    let active_one = test_support::add_user(&db, "active-one").await;
    let inactive = test_support::add_user(&db, "inactive").await;
    let active_two = test_support::add_user(&db, "active-two").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let range_a = test_support::add_edge(&db, from, to, "FOLLOWS").await;
    let range_aa = test_support::add_edge(&db, from, to, "FOLLOWS").await;
    let range_b = test_support::add_edge(&db, from, to, "FOLLOWS").await;
    let (equality_generation, equality_identity) = seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
        41,
        &[
            ("active", active_one),
            ("inactive", inactive),
            ("active", active_two),
        ],
    )
    .await;
    let (range_generation, _) = seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "weight").unwrap(),
        42,
        &[("a", range_a), ("aa", range_aa), ("b", range_b)],
    )
    .await;

    let equality_plan = exec::ExecNodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
        value: ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    };
    let mut active_ids = vec![active_one, active_two];
    active_ids.sort_unstable();
    assert_eq!(
        run_node_access(&db, equality_plan.clone()).await,
        ExecutionValue::Scalars(
            active_ids
                .into_iter()
                .map(ExecutionScalar::NodeId)
                .collect(),
        )
    );
    assert_eq!(
        run_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::RangeIndex {
                index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                    "edge_range:FOLLOWS:weight:desc",
                )),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "FOLLOWS",
                    "weight",
                    helix_ast::index::RangeIndexDirection::Desc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            },
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(range_b),
            ExecutionScalar::EdgeId(range_aa),
            ExecutionScalar::EdgeId(range_a),
        ])
    );

    let coordinator = db.reader_lease_coordinator().unwrap();
    for generation in [equality_generation, range_generation] {
        let fence = coordinator
            .begin_drain(generation, None)
            .await
            .expect("completed request permits generation drain");
        assert!(coordinator.check_drained(&fence).await.unwrap());
    }

    let record = crate::index_v2::repository::load_index_record(
        db.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &equality_identity,
    )
    .await
    .unwrap()
    .unwrap();
    let dropping = record
        .transition(IndexStateTransition::BeginDrop {
            drop_operation_id: IndexOperationId::new_v4(),
        })
        .unwrap();
    db.inner_db()
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::IndexV2(IndexV2Key::index_record(equality_identity)),
            }
            .to_bytes(),
            encode_index_record(&dropping),
        )
        .await
        .unwrap();
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(equality_plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));
}

#[tokio::test]
async fn edge_equality_access_uses_global_label_scoped_index() {
    let config = test_support::in_memory_config("access-edge-equality-index")
        .with_edge_equality_index("FOLLOWS", "status");
    let db = test_support::open_db_with_config(config).await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let active_one = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "FOLLOWS",
        vec![("status", PropertyValue::from("active"))],
    )
    .await;
    let _inactive = test_support::add_edge_with_properties(
        &db,
        bob,
        carol,
        "FOLLOWS",
        vec![("status", PropertyValue::from("inactive"))],
    )
    .await;
    let active_two = test_support::add_edge_with_properties(
        &db,
        carol,
        alice,
        "FOLLOWS",
        vec![("status", PropertyValue::from("active"))],
    )
    .await;
    let _different_label = test_support::add_edge_with_properties(
        &db,
        alice,
        carol,
        "LIKES",
        vec![("status", PropertyValue::from("active"))],
    )
    .await;

    let value = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::EqualityIndex {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                "edge_eq:FOLLOWS:status",
            )),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").expect("valid key"),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("active"))
                    .expect("indexable value"),
            ),
        },
    )
    .await;

    assert_eq!(
        value,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(active_one),
            ExecutionScalar::EdgeId(active_two),
        ])
    );
}

#[tokio::test]
async fn node_range_access_uses_configured_directional_index() {
    let config = test_support::in_memory_config("access-node-range-index")
        .with_range_index("User", "score")
        .with_range_desc_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let low = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("low")),
            ("score", PropertyValue::I64(10)),
        ],
    )
    .await;
    let medium = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("medium")),
            ("score", PropertyValue::I64(20)),
        ],
    )
    .await;
    let _high = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("high")),
            ("score", PropertyValue::I64(30)),
        ],
    )
    .await;

    let range = ir::IndexRange::Upper {
        upper: ir::IndexBound::Exclusive(
            ir::RangeIndexValue::literal(PropertyValue::I64(25)).expect("range value is indexable"),
        ),
    };
    let asc = run_node_access(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
    )
    .await;
    let desc = run_node_access(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
    )
    .await;

    assert_eq!(
        asc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(low),
            ExecutionScalar::NodeId(medium),
        ])
    );
    assert_eq!(
        desc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(medium),
            ExecutionScalar::NodeId(low),
        ])
    );

    let limited_asc_all = run_limited_node_access(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: ir::IndexRange::All,
        },
        2,
    )
    .await;
    let limited_desc_upper = run_limited_node_access(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
        1,
    )
    .await;

    assert_eq!(
        limited_asc_all,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(low),
            ExecutionScalar::NodeId(medium),
        ])
    );
    assert_eq!(
        limited_desc_upper,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(medium)])
    );

    let exclusive_between = exclusive_i64_between(10, 30);
    let asc_between = run_node_access(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: exclusive_between.clone(),
        },
    )
    .await;
    let desc_between = run_node_access(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: exclusive_between,
        },
    )
    .await;

    assert_eq!(
        asc_between,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(medium)])
    );
    assert_eq!(
        desc_between,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(medium)])
    );
}

#[tokio::test]
async fn node_range_access_resolves_runtime_parameter_bounds() {
    let config = test_support::in_memory_config("access-node-range-params")
        .with_range_index("User", "score")
        .with_range_desc_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let _low = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("low")),
            ("score", PropertyValue::I64(10)),
        ],
    )
    .await;
    let medium = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("medium")),
            ("score", PropertyValue::I64(20)),
        ],
    )
    .await;
    let high = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("high")),
            ("score", PropertyValue::I64(30)),
        ],
    )
    .await;

    let min = test_support::name("min_score");
    let max = test_support::name("max_score");
    let params = context::ParamBindings::default()
        .with_value(min.clone(), PropertyValue::I64(20))
        .with_value(max.clone(), PropertyValue::I64(40));
    let range = parameterized_i64_between(min, max);

    let asc = run_node_access_with_params(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
        params.clone(),
    )
    .await;
    let desc = run_node_access_with_params(
        &db,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range,
        },
        params,
    )
    .await;

    assert_eq!(
        asc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(medium),
            ExecutionScalar::NodeId(high),
        ])
    );
    assert_eq!(
        desc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(high),
            ExecutionScalar::NodeId(medium),
        ])
    );
}

#[tokio::test]
async fn edge_range_access_uses_global_ordered_index() {
    let config = test_support::in_memory_config("access-edge-range-index")
        .with_edge_range_index("FOLLOWS", "weight")
        .with_edge_range_desc_index("FOLLOWS", "weight");
    let db = test_support::open_db_with_config(config).await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let light = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(10))],
    )
    .await;
    let heavy = test_support::add_edge_with_properties(
        &db,
        alice,
        carol,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(30))],
    )
    .await;
    let medium = test_support::add_edge_with_properties(
        &db,
        bob,
        carol,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(20))],
    )
    .await;

    let asc_all = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: ir::IndexRange::All,
        },
    )
    .await;
    let desc_all = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: ir::IndexRange::All,
        },
    )
    .await;

    let range = ir::IndexRange::Upper {
        upper: ir::IndexBound::Exclusive(
            ir::RangeIndexValue::literal(PropertyValue::I64(25)).expect("range value is indexable"),
        ),
    };
    let asc = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
    )
    .await;
    let desc = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
    )
    .await;

    assert_eq!(
        asc_all,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(light),
            ExecutionScalar::EdgeId(medium),
            ExecutionScalar::EdgeId(heavy),
        ])
    );
    assert_eq!(
        desc_all,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(heavy),
            ExecutionScalar::EdgeId(medium),
            ExecutionScalar::EdgeId(light),
        ])
    );
    assert_eq!(
        asc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(light),
            ExecutionScalar::EdgeId(medium),
        ])
    );
    assert_eq!(
        desc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(medium),
            ExecutionScalar::EdgeId(light),
        ])
    );

    let limited_asc_all = run_limited_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: ir::IndexRange::All,
        },
        2,
    )
    .await;
    let limited_desc_upper = run_limited_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
        1,
    )
    .await;

    assert_eq!(
        limited_asc_all,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(light),
            ExecutionScalar::EdgeId(medium),
        ])
    );
    assert_eq!(
        limited_desc_upper,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(medium)])
    );

    let exclusive_between = exclusive_i64_between(10, 30);
    let asc_between = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: exclusive_between.clone(),
        },
    )
    .await;
    let desc_between = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range: exclusive_between,
        },
    )
    .await;

    assert_eq!(
        asc_between,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(medium)])
    );
    assert_eq!(
        desc_between,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(medium)])
    );
}

#[tokio::test]
async fn edge_range_access_resolves_runtime_parameter_bounds() {
    let config = test_support::in_memory_config("access-edge-range-params")
        .with_edge_range_index("FOLLOWS", "weight")
        .with_edge_range_desc_index("FOLLOWS", "weight");
    let db = test_support::open_db_with_config(config).await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let _light = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(10))],
    )
    .await;
    let heavy = test_support::add_edge_with_properties(
        &db,
        alice,
        carol,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(30))],
    )
    .await;
    let medium = test_support::add_edge_with_properties(
        &db,
        bob,
        carol,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(20))],
    )
    .await;

    let min = test_support::name("min_weight");
    let max = test_support::name("max_weight");
    let params = context::ParamBindings::default()
        .with_value(min.clone(), PropertyValue::I64(20))
        .with_value(max.clone(), PropertyValue::I64(40));
    let range = parameterized_i64_between(min, max);

    let asc = run_edge_access_with_params(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .expect("valid key"),
            range: range.clone(),
        },
        params.clone(),
    )
    .await;
    let desc = run_edge_access_with_params(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                helix_ast::index::RangeIndexDirection::Desc,
            )
            .expect("valid key"),
            range,
        },
        params,
    )
    .await;

    assert_eq!(
        asc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(medium),
            ExecutionScalar::EdgeId(heavy),
        ])
    );
    assert_eq!(
        desc,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(heavy),
            ExecutionScalar::EdgeId(medium),
        ])
    );
}

#[tokio::test]
async fn reader_range_access_covers_node_and_edge_bound_shapes() {
    let config = test_support::in_memory_config("access-reader-range-indexes")
        .with_range_index("User", "score")
        .with_edge_range_index("FOLLOWS", "weight");
    let writer = test_support::open_db_with_config(config.clone()).await;
    let low = test_support::add_node_with_properties(
        &writer,
        "User",
        vec![("score", PropertyValue::I64(10))],
    )
    .await;
    let high = test_support::add_node_with_properties(
        &writer,
        "User",
        vec![("score", PropertyValue::I64(20))],
    )
    .await;
    let light = test_support::add_edge_with_properties(
        &writer,
        low,
        high,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(10))],
    )
    .await;
    let heavy = test_support::add_edge_with_properties(
        &writer,
        high,
        low,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(20))],
    )
    .await;
    drop(writer);
    let reader = test_support::open_reader_with_config(config).await;
    let node_key = catalog::ScopedPropertyDirectionKey::try_new(
        "User",
        "score",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .expect("valid node range key");
    let edge_key = catalog::ScopedPropertyDirectionKey::try_new(
        "FOLLOWS",
        "weight",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .expect("valid edge range key");

    let all_nodes = run_node_access(
        &reader,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: node_key.clone(),
            range: ir::IndexRange::All,
        },
    )
    .await;
    let inclusive_lower_nodes = run_node_access(
        &reader,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: node_key.clone(),
            range: ir::IndexRange::Lower {
                lower: ir::IndexBound::Inclusive(
                    ir::RangeIndexValue::literal(PropertyValue::I64(20))
                        .expect("range value is indexable"),
                ),
            },
        },
    )
    .await;
    let exclusive_lower_nodes = run_node_access(
        &reader,
        exec::ExecNodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: node_key,
            range: ir::IndexRange::Lower {
                lower: ir::IndexBound::Exclusive(
                    ir::RangeIndexValue::literal(PropertyValue::I64(10))
                        .expect("range value is indexable"),
                ),
            },
        },
    )
    .await;
    let all_edges = run_edge_access(
        &reader,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: edge_key.clone(),
            range: ir::IndexRange::All,
        },
    )
    .await;
    let inclusive_upper_edges = run_edge_access(
        &reader,
        exec::ExecEdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight:asc",
            )),
            key: edge_key,
            range: ir::IndexRange::Upper {
                upper: ir::IndexBound::Inclusive(
                    ir::RangeIndexValue::literal(PropertyValue::I64(10))
                        .expect("range value is indexable"),
                ),
            },
        },
    )
    .await;

    assert_eq!(
        all_nodes,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(low),
            ExecutionScalar::NodeId(high),
        ])
    );
    assert_eq!(
        inclusive_lower_nodes,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(high)])
    );
    assert_eq!(
        exclusive_lower_nodes,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(high)])
    );
    assert_eq!(
        all_edges,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(light),
            ExecutionScalar::EdgeId(heavy),
        ])
    );
    assert_eq!(
        inclusive_upper_edges,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(light)])
    );
}
