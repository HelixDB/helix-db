//! Secondary-index access and canonical-row integration tests.

use std::collections::HashMap;

use super::support::*;
use crate::config::SecondaryIndexDefinition;
use crate::encoding::indexes::equality::{EqualityIndexKey, GlobalEdgeEqualityIndexKey};
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
use crate::encoding::indexes::{hash_property_name, hash_property_value, PropertyIndexKey};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey as ManagedKey;
use crate::encoding::v2::keys::{
    CanonicalSecondaryValue, ScopedKey, SecondaryEntryKey, SecondaryEntryLane,
    SecondaryEqualityBitmapKey,
};
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::values::{
    encode_index_record, encode_secondary_entry, SecondaryEqualityBitmapValue,
};
use crate::error::{HelixDbError, IndexFamily, IndexLifecycleUnavailableReason};
use crate::execution::interpreter::ExecutionContext;
use crate::index_lifecycle::work::SecondaryEntryValue;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2,
    IndexRevision, IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition,
};

macro_rules! node_equality {
    (index: $index:expr, key: $key:expr, value: $value:expr $(,)?) => {
        exec::ExecNodeAccessPlan::exact_equality($index, $key, $value)
    };
}

macro_rules! edge_equality {
    (index: $index:expr, key: $key:expr, value: $value:expr $(,)?) => {
        exec::ExecEdgeAccessPlan::exact_equality($index, $key, $value)
    };
}

macro_rules! node_set_equalities {
    (index: $index:expr, key: $key:expr, $values:ident $(,)?) => {
        exec::ExecNodeSecondarySetPlan::exact_equalities($index, $key, $values)
    };
    (index: $index:expr, key: $key:expr, values: $values:expr $(,)?) => {
        exec::ExecNodeSecondarySetPlan::exact_equalities($index, $key, $values)
    };
}

macro_rules! edge_set_equalities {
    (index: $index:expr, key: $key:expr, values: $values:expr $(,)?) => {
        exec::ExecEdgeSecondarySetPlan::exact_equalities($index, $key, $values)
    };
}

/// Seeds one Active secondary generation.
async fn seed_active_secondary_generation(
    db: &HelixDB,
    definition: SecondaryIndexDefinition,
    index_id: u64,
    rows: &[(&str, u64)],
) -> crate::index_lifecycle::IndexIdentity {
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
    let handle = crate::index_lifecycle::ActiveIndexHandle::try_from_record(
        DataScope::LegacyUnscoped,
        &active,
    )
    .expect("managed secondary fixture projects an Active handle");
    db.inner_db()
        .put(
            ManagedKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: ScopedKey::index_record(identity.clone()),
            }
            .to_bytes(),
            encode_index_record(&active),
        )
        .await
        .expect("managed secondary Active record persists");

    let definition = handle.secondary_definition().unwrap();
    let equality_element_kind = match definition {
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: false, .. } => {
            Some(IndexElementKind::Node)
        }
        ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => Some(IndexElementKind::Edge),
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. }
        | ValidatedSecondaryIndexDefinition::NodeRange { .. }
        | ValidatedSecondaryIndexDefinition::EdgeRange { .. } => None,
    };
    if let Some(element_kind) = equality_element_kind {
        let mut bitmaps = HashMap::new();
        for (value, entity_id) in rows {
            let CanonicalSecondaryValue::Equality(value) =
                CanonicalSecondaryValue::equality_string(value)
            else {
                unreachable!("string equality fixtures always produce equality values")
            };
            bitmaps
                .entry(value)
                .or_insert_with(roaring::RoaringTreemap::new)
                .insert(*entity_id);
        }
        for (value, ids) in bitmaps {
            let key =
                SecondaryEqualityBitmapKey::try_new(index_id, generation, element_kind, value)
                    .expect("managed equality bitmap key validates");
            db.inner_db()
                .put(
                    ManagedKey::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: ScopedKey::SecondaryEqualityBitmap(key),
                    }
                    .to_bytes(),
                    SecondaryEqualityBitmapValue::new(ids).encode(),
                )
                .await
                .expect("managed equality bitmap persists");
        }
        return identity;
    }
    let lane = match definition {
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
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: false, .. }
        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
            unreachable!("nonunique equality fixtures return after writing their bitmap")
        }
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
                CanonicalSecondaryValue::equality_string(value)
            }
            ValidatedSecondaryIndexDefinition::NodeRange { direction, .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { direction, .. } => {
                let direction = match direction {
                    crate::config::RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
                    crate::config::RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
                };
                CanonicalSecondaryValue::range_string(direction, value)
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
                ManagedKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::SecondaryEntry(key),
                }
                .to_bytes(),
                encode_secondary_entry(&SecondaryEntryValue {
                    index_id,
                    generation,
                    lane,
                    entity_id,
                }),
            )
            .await
            .expect("managed secondary entry persists");
    }
    identity
}

fn node_range_plan(
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
    range: ir::IndexRange,
) -> exec::ExecNodeAccessPlan {
    let suffix = match direction {
        helix_ast::index::RangeIndexDirection::Asc => "asc",
        helix_ast::index::RangeIndexDirection::Desc => "desc",
    };
    exec::ExecNodeAccessPlan::RangeIndex {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::NodeRangeIndexMeta::new(test_support::name(&format!(
            "node_range:User:{property}:{suffix}"
        ))),
        key: catalog::ScopedPropertyDirectionKey::try_new("User", property, direction)
            .expect("valid node range key"),
        range,
    }
}

fn edge_range_plan(
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
    range: ir::IndexRange,
) -> exec::ExecEdgeAccessPlan {
    let suffix = match direction {
        helix_ast::index::RangeIndexDirection::Asc => "asc",
        helix_ast::index::RangeIndexDirection::Desc => "desc",
    };
    exec::ExecEdgeAccessPlan::RangeIndex {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::EdgeRangeIndexMeta::new(test_support::name(&format!(
            "edge_range:FOLLOWS:{property}:{suffix}"
        ))),
        key: catalog::ScopedPropertyDirectionKey::try_new("FOLLOWS", property, direction)
            .expect("valid edge range key"),
        range,
    }
}

#[tokio::test]
async fn managed_secondary_access_uses_active_v2_rows() {
    let db = test_support::open_db("access-managed-secondary-v2").await;
    let active_one = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("status", PropertyValue::from("active")),
            ("score", PropertyValue::from("a")),
        ],
    )
    .await;
    let inactive = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("status", PropertyValue::from("inactive")),
            ("score", PropertyValue::from("aa")),
        ],
    )
    .await;
    let active_two = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("status", PropertyValue::from("active")),
            ("score", PropertyValue::from("b")),
        ],
    )
    .await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let range_a = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![("weight", PropertyValue::from("a"))],
    )
    .await;
    let range_aa = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![("weight", PropertyValue::from("aa"))],
    )
    .await;
    let range_b = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![("weight", PropertyValue::from("b"))],
    )
    .await;
    let equality_identity = seed_active_secondary_generation(
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
    let node_range_identity = seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_range("User", "score").unwrap(),
        42,
        &[("a", active_one), ("aa", inactive), ("b", active_two)],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "weight").unwrap(),
        43,
        &[("a", range_a), ("aa", range_aa), ("b", range_b)],
    )
    .await;

    let equality_plan = node_equality! {
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
    let node_range_plan = exec::ExecNodeAccessPlan::RangeIndex {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:score:asc")),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "User",
            "score",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };
    assert_eq!(
        run_node_access(&db, node_range_plan.clone()).await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(active_one),
            ExecutionScalar::NodeId(inactive),
            ExecutionScalar::NodeId(active_two),
        ])
    );
    let direction_mismatch = db
        .execute(
            &node_access_ids_plan(exec::ExecNodeAccessPlan::RangeIndex {
                iteration: helix_planner::ir::RangeScanIteration::Forward,
                index: catalog::NodeRangeIndexMeta::new(test_support::name(
                    "node_range:User:score:desc",
                )),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "score",
                    helix_ast::index::RangeIndexDirection::Desc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            }),
            context::ParamBindings::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        direction_mismatch,
        HelixDbError::IndexCatalogCorruption(message)
            if message.contains("direction disagrees")
    ));
    assert_eq!(
        run_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::RangeIndex {
                iteration: helix_planner::ir::RangeScanIteration::Forward,
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

    let node_range_record = crate::index_lifecycle::repository::load_index_record(
        db.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &node_range_identity,
    )
    .await
    .unwrap()
    .unwrap();
    let dropping_node_range = node_range_record
        .transition(IndexStateTransition::BeginDrop {
            drop_operation_id: IndexOperationId::new_v4(),
        })
        .unwrap();
    db.inner_db()
        .put(
            ManagedKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: ScopedKey::index_record(node_range_identity),
            }
            .to_bytes(),
            encode_index_record(&dropping_node_range),
        )
        .await
        .unwrap();
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(node_range_plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));

    let record = crate::index_lifecycle::repository::load_index_record(
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
            ManagedKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: ScopedKey::index_record(equality_identity),
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
async fn secondary_set_keeps_dynamic_equality_outside_literal_batches() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-secondary-set-batch").await;
            let active = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            let paused = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("paused"))],
            )
            .await;
            let inactive = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("inactive"))],
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                51,
                &[
                    ("active", active),
                    ("paused", paused),
                    ("inactive", inactive),
                ],
            )
            .await;
            let values = ir::AtLeast::<_, 1>::try_from_vec(vec![
                ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
                ),
                ir::IndexValue::Param(test_support::name("selected_status")),
            ])
            .unwrap();
            crate::index_lifecycle::secondary::reset_equality_read_metrics();

            let actual = run_node_access_with_params(
                &db,
                exec::ExecNodeAccessPlan::SecondarySet {
                    set: node_set_equalities! {
                        index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                            "node_eq:User:status",
                        )),
                        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                        values,
                    },
                },
                context::ParamBindings::default().with_value(
                    test_support::name("selected_status"),
                    PropertyValue::from("paused"),
                ),
            )
            .await;

            let mut expected = vec![active, paused];
            expected.sort_unstable();
            assert_eq!(
                actual,
                ExecutionValue::Scalars(
                    expected.into_iter().map(ExecutionScalar::NodeId).collect()
                )
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            // Each independently encoded child resolves its catalog and then performs
            // its one selected point primitive. The executor must not batch across the
            // explicit dynamic-equality boundary.
            assert_eq!(metrics.point_reads, 4);
            assert_eq!(metrics.multi_get_calls, 0);
            assert_eq!(metrics.graph_reads, 0);
        })
        .await
}

#[tokio::test]
async fn secondary_set_literal_batch_issues_one_multi_get_without_graph_hydration() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-secondary-set-literal-batch").await;
            let active = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            let paused = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("paused"))],
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                61,
                &[("active", active), ("paused", paused)],
            )
            .await;
            let values = ir::AtLeast::<_, 1>::try_from_vec(vec![
                ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
                ),
                ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(PropertyValue::from("paused")).unwrap(),
                ),
            ])
            .unwrap();
            crate::index_lifecycle::secondary::reset_equality_read_metrics();

            let mut expected = vec![active, paused];
            expected.sort_unstable();
            assert_eq!(
                run_node_access(
                    &db,
                    exec::ExecNodeAccessPlan::SecondarySet {
                        set: exec::ExecNodeSecondarySetPlan::exact_equalities(
                            catalog::NodeEqualityIndexMeta::new(test_support::name(
                                "node_eq:User:status",
                            )),
                            catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                            values,
                        ),
                    },
                )
                .await,
                ExecutionValue::Scalars(
                    expected.into_iter().map(ExecutionScalar::NodeId).collect()
                )
            );
            assert_eq!(
                crate::index_lifecycle::secondary::equality_read_metrics(),
                crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                    point_reads: 3,
                    multi_get_calls: 1,
                    scans: 0,
                    graph_reads: 0,
                }
            );
        })
        .await
}

#[tokio::test]
async fn unordered_node_secondary_sets_combine_ids_before_materialization() {
    let db = test_support::open_db("access-unordered-node-secondary-set").await;
    let active_admin = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("status", PropertyValue::from("active")),
            ("role", PropertyValue::from("admin")),
            ("rank", PropertyValue::from("a")),
        ],
    )
    .await;
    let active_member = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("status", PropertyValue::from("active")),
            ("role", PropertyValue::from("member")),
            ("rank", PropertyValue::from("b")),
        ],
    )
    .await;
    let paused_admin = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("status", PropertyValue::from("paused")),
            ("role", PropertyValue::from("admin")),
            ("rank", PropertyValue::from("c")),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
        54,
        &[
            ("active", active_admin),
            ("active", active_member),
            ("paused", paused_admin),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "role").unwrap(),
        55,
        &[
            ("admin", active_admin),
            ("member", active_member),
            ("admin", paused_admin),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        58,
        &[
            ("a", active_admin),
            ("b", active_member),
            ("c", paused_admin),
        ],
    )
    .await;

    let status = |value: &'static str| {
        node_set_equalities! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            values: ir::AtLeast::from_one(ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
            )),
        }
    };
    let role = |value: &'static str| {
        node_set_equalities! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:role")),
            key: catalog::ScopedPropertyKey::try_new("User", "role").unwrap(),
            values: ir::AtLeast::from_one(ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
            )),
        }
    };
    let range = || exec::ExecNodeSecondaryRangePlan {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:asc")),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "User",
            "rank",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };
    let plan = exec::ExecNodeAccessPlan::SecondarySet {
        set: exec::ExecNodeSecondarySetPlan::Intersect {
            driver: Box::new(exec::ExecNodeSecondarySetPlan::Union {
                driver: Box::new(status("active")),
                rest: ir::AtLeast::from_one(status("paused")),
            }),
            rest: ir::AtLeast::from_one(role("admin")),
        },
    };

    let mut expected = vec![active_admin, paused_admin];
    expected.sort_unstable();
    assert_eq!(
        run_node_access(&db, plan).await,
        ExecutionValue::Scalars(expected.into_iter().map(ExecutionScalar::NodeId).collect())
    );
    assert_eq!(
        run_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Intersect {
                    driver: Box::new(exec::ExecNodeSecondarySetPlan::Range(range())),
                    rest: ir::AtLeast::from_one(role("admin")),
                },
            },
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(active_admin),
            ExecutionScalar::NodeId(paused_admin),
        ])
    );
    assert_eq!(
        run_limited_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                    driver: range(),
                    filters: ir::AtLeast::<_, 1>::try_from_vec(vec![
                        status("active"),
                        role("admin"),
                    ])
                    .unwrap(),
                },
            },
            1,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(active_admin)])
    );
    assert_eq!(
        run_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Empty,
            },
        )
        .await,
        ExecutionValue::Scalars(Vec::new())
    );
}

#[tokio::test]
async fn exact_unique_row_access_verifies_present_missing_and_corrupt_owners() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-exact-unique-owner").await;
            let alice = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("email", PropertyValue::from("alice@example.com"))],
            )
            .await;
            let bob = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("email", PropertyValue::from("bob@example.com"))],
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap(),
                59,
                &[("alice@example.com", alice), ("corrupt@example.com", bob)],
            )
            .await;
            let plan = |value: &'static str| {
                exec::ExecNodeAccessPlan::exact_equality(
                    catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:email"))
                        .with_uniqueness(catalog::IndexUniqueness::Unique),
                    catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                    ir::IndexValue::Literal(
                        ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
                    ),
                )
            };

            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access(&db, plan("alice@example.com")).await,
                ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(alice)])
            );
            assert_eq!(
                crate::index_lifecycle::secondary::equality_read_metrics(),
                crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                    point_reads: 2,
                    multi_get_calls: 0,
                    scans: 0,
                    graph_reads: 1,
                }
            );

            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access(&db, plan("missing@example.com")).await,
                ExecutionValue::Scalars(Vec::new())
            );
            assert_eq!(
                crate::index_lifecycle::secondary::equality_read_metrics(),
                crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                    point_reads: 2,
                    multi_get_calls: 0,
                    scans: 0,
                    graph_reads: 0,
                }
            );

            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            let error = db
                .execute(
                    &node_access_ids_plan(plan("corrupt@example.com")),
                    context::ParamBindings::default(),
                )
                .await
                .expect_err("a stale unique owner is physical corruption");
            assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
            assert_eq!(
                crate::index_lifecycle::secondary::equality_read_metrics(),
                crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                    point_reads: 2,
                    multi_get_calls: 0,
                    scans: 0,
                    graph_reads: 1,
                }
            );
        })
        .await
}

#[tokio::test]
async fn unique_union_batches_owners_and_rejects_stale_graph_rows() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("unique-union-batch").await;
            let first = test_support::add_node_with_properties(
                &db,
                "Fixture",
                vec![("key", PropertyValue::from("first"))],
            )
            .await;
            let second = test_support::add_node_with_properties(
                &db,
                "Fixture",
                vec![("key", PropertyValue::from("second"))],
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_unique_equality("Fixture", "key").unwrap(),
                79,
                &[("first", first), ("second", second), ("stale", second)],
            )
            .await;
            let plan = |values: &[&str]| exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::UniqueUnion {
                    index: exec::ExecNodeUniqueEqualityIndex::try_from(
                        catalog::NodeEqualityIndexMeta::new(test_support::name(
                            "node_eq:Fixture:key",
                        ))
                        .with_uniqueness(catalog::IndexUniqueness::Unique),
                    )
                    .unwrap(),
                    key: catalog::ScopedPropertyKey::try_new("Fixture", "key").unwrap(),
                    values: ir::AtLeast::try_from_vec(
                        values
                            .iter()
                            .map(|value| {
                                exec::ExecIndexedEqualityValue::try_from(
                                    ir::SecondaryIndexLiteral::new(PropertyValue::from(*value))
                                        .unwrap(),
                                )
                                .unwrap()
                            })
                            .collect(),
                    )
                    .unwrap(),
                },
            };
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            let result = run_node_access(&db, plan(&["second", "missing", "first", "first"])).await;
            let mut ids = vec![first, second];
            ids.sort_unstable();
            assert_eq!(
                result,
                ExecutionValue::Scalars(ids.into_iter().map(ExecutionScalar::NodeId).collect())
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            // One multi-get reads the owners and one verifies their records, never
            // one get per owner.
            assert_eq!(metrics.multi_get_calls, 2);
            assert_eq!(metrics.scans, 0);
            assert_eq!(metrics.graph_reads, 3);
            // Six hundred values are three owner batches; only the two holding an
            // owner need a verification batch.
            let wide = core::iter::once("first".to_string())
                .chain((0..598).map(|n| format!("missing-{n}")))
                .chain(core::iter::once("second".to_string()))
                .collect::<Vec<_>>();
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            let mut ids = vec![first, second];
            ids.sort_unstable();
            assert_eq!(
                run_node_access(
                    &db,
                    plan(&wide.iter().map(String::as_str).collect::<Vec<_>>())
                )
                .await,
                ExecutionValue::Scalars(ids.into_iter().map(ExecutionScalar::NodeId).collect())
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.multi_get_calls, 3 + 2);
            assert_eq!(metrics.graph_reads, 2);
            assert_eq!(
                run_node_access(&db, plan(&["missing", "absent"])).await,
                ExecutionValue::Scalars(Vec::new())
            );
            let error = db
                .execute(
                    &node_access_ids_plan(plan(&["first", "stale"])),
                    context::ParamBindings::default(),
                )
                .await
                .expect_err("stale owner must fail closed");
            assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
            let key = ManagedKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: ScopedKey::SecondaryEntry(
                    SecondaryEntryKey::try_new(
                        IndexId::new(79).unwrap(),
                        IndexGenerationId::initial(),
                        SecondaryEntryLane::NodeUniqueEquality,
                        CanonicalSecondaryValue::equality_string("first"),
                        None,
                    )
                    .unwrap(),
                ),
            }
            .to_bytes();
            db.inner_db()
                .put(key, bytes::Bytes::from_static(b"malformed-owner"))
                .await
                .unwrap();
            assert!(
                db.execute(
                    &node_access_ids_plan(plan(&["first", "second"])),
                    context::ParamBindings::default()
                )
                .await
                .is_err(),
                "malformed owners must not become misses"
            );

            // Literal admission precedes every owner read: a budget that
            // cannot hold the requested values fails before any lookup.
            let exec::ExecNodeAccessPlan::SecondarySet { set } = plan(&["first", "second"]) else {
                panic!("unique union fixture is a secondary set");
            };
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.row_memory = Some(crate::query_resources::Budget::new(1));
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert!(matches!(
                ctx.node_secondary_set_ids(&set, None).await,
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
            assert_eq!(
                crate::index_lifecycle::secondary::equality_read_metrics().multi_get_calls,
                0
            );
            assert_eq!(ctx.row_memory.as_ref().unwrap().available(), 1);
        })
        .await
}

#[tokio::test]
async fn exact_null_and_nan_row_access_never_enter_bitmap_dispatch() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-exact-null-and-nan").await;
            let explicit_null = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            let absent = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
            test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            test_support::add_node_with_properties(
                &db,
                "Other",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            let index =
                catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status"));
            let key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();

            let mut expected = vec![explicit_null, absent];
            expected.sort_unstable();
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access(
                    &db,
                    exec::ExecNodeAccessPlan::exact_equality(
                        index.clone(),
                        key.clone(),
                        ir::IndexValue::Literal(
                            ir::SecondaryIndexLiteral::new(PropertyValue::Null).unwrap(),
                        ),
                    ),
                )
                .await,
                ExecutionValue::Scalars(
                    expected.into_iter().map(ExecutionScalar::NodeId).collect()
                )
            );
            // No Active generation: the three `User` rows are the candidates, and
            // the `Other` row is never read.
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.scans, 0);
            assert_eq!(metrics.graph_reads, 3);

            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            for nan in [PropertyValue::F32(f32::NAN), PropertyValue::F64(f64::NAN)] {
                assert_eq!(
                    run_node_access(
                        &db,
                        exec::ExecNodeAccessPlan::exact_equality(
                            index.clone(),
                            key.clone(),
                            ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(nan).unwrap()),
                        ),
                    )
                    .await,
                    ExecutionValue::Scalars(Vec::new())
                );
            }
            assert_eq!(
                crate::index_lifecycle::secondary::equality_read_metrics(),
                crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics::default()
            );
        })
        .await
}

#[tokio::test]
async fn dynamic_equality_is_the_only_runtime_classifier_for_null_nan_and_indexed_values() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-explicit-dynamic-equality").await;
            let active = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            let null = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                60,
                &[("active", active)],
            )
            .await;
            let param = test_support::name("late_status");
            let plan = exec::ExecNodeAccessPlan::DynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: param.clone(),
            };

            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan.clone(),
                    context::ParamBindings::default()
                        .with_value(param.clone(), PropertyValue::from("active")),
                )
                .await,
                ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(active)])
            );
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan.clone(),
                    context::ParamBindings::default()
                        .with_value(param.clone(), PropertyValue::Null),
                )
                .await,
                ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(null)])
            );
            // A null binding reads the lane once and verifies only the label row
            // outside it.
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.scans, 1);
            assert_eq!(metrics.graph_reads, 1);
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan,
                    context::ParamBindings::default()
                        .with_value(param, PropertyValue::F64(f64::NAN)),
                )
                .await,
                ExecutionValue::Scalars(Vec::new())
            );
        })
        .await;
}

#[tokio::test]
async fn dynamic_membership_batches_safe_values_and_falls_back_authoritatively() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-dynamic-membership").await;
            let active = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            let paused = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("paused"))],
            )
            .await;
            let explicit_null = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            let missing = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
            let _wrong_label = test_support::add_node_with_properties(
                &db,
                "Account",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                61,
                &[("active", active), ("paused", paused)],
            )
            .await;
            let param = test_support::name("late_statuses");
            let plan = exec::ExecNodeAccessPlan::DynamicMembership {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                values: ir::RuntimeEqualitySet::new(
                    param.clone(),
                    std::num::NonZeroUsize::new(2).unwrap(),
                ),
            };

            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan.clone(),
                    context::ParamBindings::default()
                        .with_value(param.clone(), PropertyValue::StringArray(Vec::new())),
                )
                .await,
                ExecutionValue::Scalars(Vec::new())
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.multi_get_calls, 0);
            assert_eq!(metrics.scans, 0);

            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan.clone(),
                    context::ParamBindings::default().with_value(
                        param.clone(),
                        PropertyValue::StringArray(vec![
                            "active".to_owned(),
                            "paused".to_owned(),
                            "active".to_owned(),
                        ]),
                    ),
                )
                .await,
                ExecutionValue::Scalars(vec![
                    ExecutionScalar::NodeId(active),
                    ExecutionScalar::NodeId(paused),
                ])
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.multi_get_calls, 1);
            assert_eq!(metrics.scans, 0);

            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan.clone(),
                    context::ParamBindings::default().with_value(
                        param.clone(),
                        PropertyValue::Array(vec![
                            PropertyValue::from("active"),
                            PropertyValue::Null
                        ]),
                    ),
                )
                .await,
                ExecutionValue::Scalars(vec![
                    ExecutionScalar::NodeId(active),
                    ExecutionScalar::NodeId(explicit_null),
                    ExecutionScalar::NodeId(missing),
                ])
            );

            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan,
                    context::ParamBindings::default().with_value(
                        param,
                        PropertyValue::StringArray(vec![
                            "active".to_owned(),
                            "paused".to_owned(),
                            "absent".to_owned(),
                        ]),
                    ),
                )
                .await,
                ExecutionValue::Scalars(vec![
                    ExecutionScalar::NodeId(active),
                    ExecutionScalar::NodeId(paused),
                ])
            );
        })
        .await
}

#[tokio::test]
async fn null_equality_reads_only_label_rows_outside_the_lane() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-null-equality-label-complement").await;
            let mut active = Vec::new();
            for _ in 0..6 {
                active.push(
                    test_support::add_node_with_properties(
                        &db,
                        "User",
                        vec![("status", PropertyValue::from("active"))],
                    )
                    .await,
                );
            }
            let explicit_null = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            let missing = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
            for _ in 0..5 {
                test_support::add_node_with_properties(
                    &db,
                    "Other",
                    vec![("status", PropertyValue::Null)],
                )
                .await;
            }
            let from = active[0];
            let to = active[1];
            let mut active_edges = Vec::new();
            for _ in 0..3 {
                active_edges.push(
                    test_support::add_edge_with_properties(
                        &db,
                        from,
                        to,
                        "FOLLOWS",
                        vec![("status", PropertyValue::from("active"))],
                    )
                    .await,
                );
            }
            let null_edge = test_support::add_edge_with_properties(
                &db,
                from,
                to,
                "FOLLOWS",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            for _ in 0..4 {
                test_support::add_edge_with_properties(
                    &db,
                    from,
                    to,
                    "LIKES",
                    vec![("status", PropertyValue::Null)],
                )
                .await;
            }
            let node_rows = active.iter().map(|id| ("active", *id)).collect::<Vec<_>>();
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                70,
                &node_rows,
            )
            .await;
            let edge_rows = active_edges
                .iter()
                .map(|id| ("active", *id))
                .collect::<Vec<_>>();
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::edge_equality("FOLLOWS", "status").unwrap(),
                71,
                &edge_rows,
            )
            .await;

            let null = || {
                ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(PropertyValue::Null).unwrap(),
                )
            };
            let param = test_support::name("late_status");
            let node_index =
                catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status"));
            let node_key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
            let mut with_nulls = vec![explicit_null, missing];
            with_nulls.sort_unstable();
            let mut active_or_null = active
                .iter()
                .copied()
                .chain(with_nulls.clone())
                .collect::<Vec<_>>();
            active_or_null.sort_unstable();
            // A literal, a bound parameter (a `ForEach` field binds the same runtime
            // equality), an `IN` list with null, and a parameter list with null.
            let node_cases = [
                (
                    exec::ExecNodeAccessPlan::exact_equality(
                        node_index.clone(),
                        node_key.clone(),
                        null(),
                    ),
                    context::ParamBindings::default(),
                    with_nulls.clone(),
                ),
                (
                    exec::ExecNodeAccessPlan::DynamicEquality {
                        index: node_index.clone(),
                        key: node_key.clone(),
                        param: param.clone(),
                    },
                    context::ParamBindings::default()
                        .with_value(param.clone(), PropertyValue::Null),
                    with_nulls.clone(),
                ),
                (
                    exec::ExecNodeAccessPlan::SecondarySet {
                        set: exec::ExecNodeSecondarySetPlan::exact_equalities(
                            node_index.clone(),
                            node_key.clone(),
                            ir::AtLeast::try_from_vec(vec![
                                null(),
                                ir::IndexValue::Literal(
                                    ir::SecondaryIndexLiteral::new(PropertyValue::from("active"))
                                        .unwrap(),
                                ),
                            ])
                            .unwrap(),
                        ),
                    },
                    context::ParamBindings::default(),
                    active_or_null.clone(),
                ),
                (
                    exec::ExecNodeAccessPlan::DynamicMembership {
                        index: node_index.clone(),
                        key: node_key.clone(),
                        values: ir::RuntimeEqualitySet::new(
                            param.clone(),
                            std::num::NonZeroUsize::new(2).unwrap(),
                        ),
                    },
                    context::ParamBindings::default().with_value(
                        param.clone(),
                        PropertyValue::Array(vec![
                            PropertyValue::Null,
                            PropertyValue::from("active"),
                        ]),
                    ),
                    active_or_null,
                ),
            ];
            for (plan, params, expected) in node_cases {
                crate::index_lifecycle::secondary::reset_equality_read_metrics();
                assert_eq!(
                    run_node_access_with_params(&db, plan, params).await,
                    ExecutionValue::Scalars(
                        expected.into_iter().map(ExecutionScalar::NodeId).collect()
                    )
                );
                let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
                // One lane scan, and only the two label rows outside it are read:
                // never a row of another label.
                assert_eq!(metrics.scans, 1);
                assert_eq!(metrics.graph_reads, 2);
            }

            let edge_index =
                catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:FOLLOWS:status"));
            let edge_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
            for (plan, params) in [
                (
                    exec::ExecEdgeAccessPlan::exact_equality(
                        edge_index.clone(),
                        edge_key.clone(),
                        null(),
                    ),
                    context::ParamBindings::default(),
                ),
                (
                    exec::ExecEdgeAccessPlan::DynamicEquality {
                        index: edge_index,
                        key: edge_key,
                        param: param.clone(),
                    },
                    context::ParamBindings::default()
                        .with_value(param.clone(), PropertyValue::Null),
                ),
            ] {
                crate::index_lifecycle::secondary::reset_equality_read_metrics();
                assert_eq!(
                    run_edge_access_with_params(&db, plan, params).await,
                    ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(null_edge)])
                );
                let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
                assert_eq!(metrics.scans, 1);
                assert_eq!(metrics.graph_reads, 1);
            }

            // Without an Active generation every label row is a candidate, still
            // never a row of another label.
            let unindexed = test_support::open_db("access-null-equality-without-lane").await;
            test_support::add_node_with_properties(
                &unindexed,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            let mut expected = vec![
                test_support::add_node_with_properties(
                    &unindexed,
                    "User",
                    vec![("status", PropertyValue::Null)],
                )
                .await,
                test_support::add_node_with_properties(&unindexed, "User", Vec::new()).await,
            ];
            expected.sort_unstable();
            test_support::add_node_with_properties(
                &unindexed,
                "Other",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access(
                    &unindexed,
                    exec::ExecNodeAccessPlan::exact_equality(node_index, node_key, null()),
                )
                .await,
                ExecutionValue::Scalars(
                    expected.into_iter().map(ExecutionScalar::NodeId).collect()
                )
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.scans, 0);
            assert_eq!(metrics.graph_reads, 3);
        })
        .await;
}

#[tokio::test]
async fn unsupported_and_oversized_equality_values_return_verified_label_rows() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-unencodable-equality-values").await;
            let active = test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await;
            test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::Null)],
            )
            .await;
            test_support::add_node_with_properties(&db, "Other", Vec::new()).await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                72,
                &[("active", active)],
            )
            .await;
            let param = test_support::name("late_status");
            let plan = exec::ExecNodeAccessPlan::DynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:status",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: param.clone(),
            };
            for value in [
        PropertyValue::Object(Default::default()),
        PropertyValue::Array(vec![PropertyValue::I64(1)]),
        PropertyValue::String("x".repeat(
            crate::encoding::v2::values::property::equality_index_value::MAX_EQUALITY_CANONICAL_LEN,
        )),
    ] {
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        // No stored row can hold a value no lane can encode, so the verified
        // label rows outside the lane answer it: empty, and never an error.
        assert_eq!(
            run_node_access_with_params(
                &db,
                plan.clone(),
                context::ParamBindings::default().with_value(param.clone(), value),
            )
            .await,
            ExecutionValue::Scalars(Vec::new())
        );
        let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
        assert_eq!(metrics.scans, 1);
        assert_eq!(metrics.graph_reads, 1);
    }
        })
        .await;
}

#[tokio::test]
async fn label_rows_outside_the_lane_stop_at_the_request_deadline() {
    let db = test_support::open_db("access-null-equality-deadline").await;
    let mut active = Vec::new();
    for _ in 0..3 {
        active.push(
            test_support::add_node_with_properties(
                &db,
                "User",
                vec![("status", PropertyValue::from("active"))],
            )
            .await,
        );
    }
    // More null rows than one record batch holds: verification reads two.
    let mut nulls = Vec::new();
    for _ in 0..helix_planner::cost::RECORD_BATCH_ROWS + 1 {
        nulls.push(test_support::add_node_with_properties(&db, "User", Vec::new()).await);
    }
    let rows = active.iter().map(|id| ("active", *id)).collect::<Vec<_>>();
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
        74,
        &rows,
    )
    .await;
    let key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let mut expired = 0;
    for checks in 0.. {
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.fail_deadline_after(checks);
        match ctx
            .null_equality_rows(IndexElementKind::Node, &key, None)
            .await
        {
            Err(HelixDbError::QueryDeadlineExceeded) => expired += 1,
            Err(error) => panic!("unexpected error {error:?}"),
            Ok(rows) => {
                assert_eq!(
                    rows.into_unbudgeted(),
                    roaring::RoaringTreemap::from_iter(nulls)
                );
                break;
            }
        }
    }
    // The request deadline is checked before every lane row (the one `active`
    // bitmap, then the end of the lane) and before every record batch (two,
    // then the end of the candidates), so an expired request stops at each.
    assert_eq!(expired, 5);
}

#[tokio::test]
async fn runtime_lists_over_the_bound_use_chunked_multi_gets() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            let db = test_support::open_db("access-runtime-lists-over-the-bound").await;
            let mut matching = Vec::new();
            for value in 0..10 {
                matching.push(
                    test_support::add_node_with_properties(
                        &db,
                        "User",
                        vec![("status", PropertyValue::from(format!("s{value}")))],
                    )
                    .await,
                );
            }
            let values = (0..1_000)
                .map(|value| format!("s{value}"))
                .collect::<Vec<_>>();
            let rows = matching
                .iter()
                .enumerate()
                .map(|(value, id)| (values[value].as_str(), *id))
                .collect::<Vec<_>>();
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                73,
                &rows,
            )
            .await;
            let index =
                catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status"));
            let key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
            let expected = ExecutionValue::Scalars(
                matching
                    .iter()
                    .copied()
                    .map(ExecutionScalar::NodeId)
                    .collect(),
            );

            // A runtime list of a thousand values is read as unions of at most 64
            // values each: one multi-get per union, never a scan.
            let param = test_support::name("statuses");
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    exec::ExecNodeAccessPlan::DynamicMembership {
                        index: index.clone(),
                        key: key.clone(),
                        values: ir::RuntimeEqualitySet::new(
                            param.clone(),
                            std::num::NonZeroUsize::new(64).unwrap(),
                        ),
                    },
                    context::ParamBindings::default()
                        .with_value(param, PropertyValue::StringArray(values.clone())),
                )
                .await,
                expected
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.multi_get_calls, 1_000_u64.div_ceil(64));
            assert_eq!(metrics.scans, 0);
            assert_eq!(metrics.graph_reads, 0);

            // A literal list of the same size is one batched read, in multi-gets of
            // one record batch each.
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            assert_eq!(
                run_node_access(
                    &db,
                    exec::ExecNodeAccessPlan::exact_equality(
                        index,
                        key,
                        ir::IndexValue::LiteralSet(
                            ir::AtLeast::try_from_vec(
                                values
                                    .into_iter()
                                    .map(|value| {
                                        ir::SecondaryIndexLiteral::new(PropertyValue::from(value))
                                            .unwrap()
                                    })
                                    .collect(),
                            )
                            .unwrap(),
                        ),
                    ),
                )
                .await,
                expected
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(
                metrics.multi_get_calls,
                1_000_u64.div_ceil(helix_planner::cost::RECORD_BATCH_ROWS)
            );
            assert_eq!(metrics.scans, 0);
            assert_eq!(metrics.graph_reads, 0);
        })
        .await;
}

#[tokio::test]
async fn unordered_edge_secondary_sets_remain_edge_scoped() {
    let db = test_support::open_db("access-unordered-edge-secondary-set").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let active_friend = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![
            ("status", PropertyValue::from("active")),
            ("kind", PropertyValue::from("friend")),
        ],
    )
    .await;
    let paused_friend = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![
            ("status", PropertyValue::from("paused")),
            ("kind", PropertyValue::from("friend")),
        ],
    )
    .await;
    let active_colleague = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![
            ("status", PropertyValue::from("active")),
            ("kind", PropertyValue::from("colleague")),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "status").unwrap(),
        56,
        &[
            ("active", active_friend),
            ("paused", paused_friend),
            ("active", active_colleague),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind").unwrap(),
        57,
        &[
            ("friend", active_friend),
            ("friend", paused_friend),
            ("colleague", active_colleague),
        ],
    )
    .await;

    let status = |value: &'static str| {
        edge_set_equalities! {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:FOLLOWS:status")),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            values: ir::AtLeast::from_one(ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
            )),
        }
    };
    let friend = edge_set_equalities! {
        index: catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:FOLLOWS:kind")),
        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "kind").unwrap(),
        values: ir::AtLeast::from_one(ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("friend")).unwrap(),
        )),
    };
    let plan = exec::ExecEdgeAccessPlan::SecondarySet {
        set: exec::ExecEdgeSecondarySetPlan::Intersect {
            driver: Box::new(exec::ExecEdgeSecondarySetPlan::Union {
                driver: Box::new(status("active")),
                rest: ir::AtLeast::from_one(status("paused")),
            }),
            rest: ir::AtLeast::from_one(friend),
        },
    };

    let mut expected = vec![active_friend, paused_friend];
    expected.sort_unstable();
    assert_eq!(
        run_edge_access(&db, plan).await,
        ExecutionValue::Scalars(expected.into_iter().map(ExecutionScalar::EdgeId).collect())
    );
    assert_eq!(
        run_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::Empty,
            },
        )
        .await,
        ExecutionValue::Scalars(Vec::new())
    );
}

#[tokio::test]
async fn ordered_edge_secondary_intersection_filters_before_applying_limit() {
    let db = test_support::open_db("access-ordered-edge-secondary-set").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let active_low = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![
            ("status", PropertyValue::from("active")),
            ("weight", PropertyValue::from("a")),
        ],
    )
    .await;
    let inactive_middle = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![
            ("status", PropertyValue::from("inactive")),
            ("weight", PropertyValue::from("b")),
        ],
    )
    .await;
    let active_high = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![
            ("status", PropertyValue::from("active")),
            ("weight", PropertyValue::from("c")),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "status").unwrap(),
        52,
        &[
            ("active", active_low),
            ("inactive", inactive_middle),
            ("active", active_high),
        ],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_range("FOLLOWS", "weight").unwrap(),
        53,
        &[
            ("a", active_low),
            ("b", inactive_middle),
            ("c", active_high),
        ],
    )
    .await;
    let equality = edge_set_equalities! {
        index: catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:FOLLOWS:status")),
        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
        values: ir::AtLeast::from_one(ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        )),
    };
    let range = || exec::ExecEdgeSecondaryRangePlan {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::EdgeRangeIndexMeta::new(test_support::name(
            "edge_range:FOLLOWS:weight:asc",
        )),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "FOLLOWS",
            "weight",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };
    assert_eq!(
        run_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::SecondarySet {
                set: exec::ExecEdgeSecondarySetPlan::Range(range()),
            },
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(active_low),
            ExecutionScalar::EdgeId(inactive_middle),
            ExecutionScalar::EdgeId(active_high),
        ])
    );
    let plan = exec::ExecEdgeAccessPlan::SecondarySet {
        set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect {
            driver: range(),
            filters: ir::AtLeast::<_, 1>::try_from_vec(vec![equality.clone(), equality]).unwrap(),
        },
    };

    assert_eq!(
        run_limited_edge_access(&db, plan, 2).await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(active_low),
            ExecutionScalar::EdgeId(active_high),
        ])
    );
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
        edge_equality! {
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
async fn regression_equality_lookup_uses_the_full_canonical_value_key() {
    let db = test_support::open_db("access-equality-digest-collision").await;
    let matching = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("status", PropertyValue::from("needle"))],
    )
    .await;
    let collision = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("status", PropertyValue::from("different canonical value"))],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
        44,
        &[
            ("needle", matching),
            ("different canonical value", collision),
        ],
    )
    .await;

    let actual = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("needle")).unwrap(),
            ),
        },
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(matching)]),
        "managed equality lookup must address the complete canonical value"
    );
}

#[tokio::test]
async fn regression_edge_equality_lookup_uses_the_full_canonical_value_key() {
    let db = test_support::open_db("access-edge-equality-digest-collision").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let matching = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![("status", PropertyValue::from("needle"))],
    )
    .await;
    let collision = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "FOLLOWS",
        vec![("status", PropertyValue::from("different canonical value"))],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "status").unwrap(),
        45,
        &[
            ("needle", matching),
            ("different canonical value", collision),
        ],
    )
    .await;

    let actual = run_edge_access(
        &db,
        edge_equality! {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                "edge_eq:FOLLOWS:status",
            )),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("needle")).unwrap(),
            ),
        },
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(matching)]),
        "managed edge equality lookup must address the complete canonical value"
    );
}

#[tokio::test]
async fn regression_equality_lookup_matches_cross_numeric_full_scan_semantics() {
    let config = test_support::in_memory_config("access-cross-numeric-equality")
        .with_equality_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let expected = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("score", PropertyValue::I64(42))],
    )
    .await;

    let actual = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:score")),
            key: catalog::ScopedPropertyKey::try_new("User", "score").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::F64(42.0)).unwrap(),
            ),
        },
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(expected)]),
        "indexed equality must match the independent full-scan numeric equality contract"
    );
}

#[tokio::test]
async fn regression_equality_lookup_treats_positive_and_negative_zero_as_equal() {
    let config = test_support::in_memory_config("access-signed-zero-equality")
        .with_equality_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let expected = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("score", PropertyValue::F64(-0.0))],
    )
    .await;

    let actual = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:score")),
            key: catalog::ScopedPropertyKey::try_new("User", "score").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::F64(0.0)).unwrap(),
            ),
        },
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(expected)]),
        "indexed equality must match full-scan equality for signed zero"
    );
}

#[tokio::test]
async fn regression_equality_lookup_keeps_nan_non_reflexive_like_a_full_scan() {
    let config =
        test_support::in_memory_config("access-nan-equality").with_equality_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    test_support::add_node_with_properties(
        &db,
        "User",
        vec![("score", PropertyValue::F64(f64::NAN))],
    )
    .await;

    let actual = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:score")),
            key: catalog::ScopedPropertyKey::try_new("User", "score").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::F64(f64::NAN)).unwrap(),
            ),
        },
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(Vec::new()),
        "NaN is not equal to itself under the independent full-scan predicate"
    );
}

#[tokio::test]
async fn regression_unique_equality_allows_distinct_typed_values_with_the_same_text() {
    let config = test_support::in_memory_config("access-typed-unique-equality")
        .with_unique_equality_index("User", "external_id");
    let db = test_support::open_db_with_config(config).await;
    test_support::add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::Bool(true))],
    )
    .await;

    let second = test_support::try_add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::String("true".into()))],
    )
    .await;

    assert!(
        second.is_ok(),
        "distinct Bool(true) and String(\"true\") values must not violate uniqueness: {second:?}"
    );
}

#[tokio::test]
async fn regression_unique_equality_uses_exact_cross_numeric_semantics_above_two_to_the_53() {
    let config = test_support::in_memory_config("access-exact-cross-numeric-unique")
        .with_unique_equality_index("User", "external_id");
    let db = test_support::open_db_with_config(config).await;
    let exact_owner = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::I64(9_007_199_254_740_992))],
    )
    .await;
    let equal_float = test_support::try_add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::F64(9_007_199_254_740_992.0))],
    )
    .await;
    assert!(matches!(
        equal_float,
        Err(HelixDbError::UniqueConstraintViolation {
            existing_node_id,
            ..
        }) if existing_node_id == exact_owner
    ));

    let reverse_config = test_support::in_memory_config("access-exact-cross-numeric-distinct")
        .with_unique_equality_index("User", "external_id");
    let reverse_db = test_support::open_db_with_config(reverse_config).await;
    test_support::add_node_with_properties(
        &reverse_db,
        "User",
        vec![("external_id", PropertyValue::F64(9_007_199_254_740_992.0))],
    )
    .await;
    let distinct = test_support::try_add_node_with_properties(
        &reverse_db,
        "User",
        vec![("external_id", PropertyValue::I64(9_007_199_254_740_993))],
    )
    .await;
    assert!(
        distinct.is_ok(),
        "the adjacent integer must remain a distinct unique value: {distinct:?}"
    );
}

#[tokio::test]
async fn regression_equality_distinguishes_distinct_same_length_arrays() {
    let config = test_support::in_memory_config("access-array-equality")
        .with_equality_index("User", "external_id");
    let db = test_support::open_db_with_config(config).await;
    let expected = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::I64Array(vec![1, 2]))],
    )
    .await;
    test_support::add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::I64Array(vec![8, 9]))],
    )
    .await;

    let actual = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                "node_eq:User:external_id",
            )),
            key: catalog::ScopedPropertyKey::try_new("User", "external_id").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::I64Array(vec![1, 2])).unwrap(),
            ),
        },
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(expected)]),
        "equality lookup must compare array contents, not only array length"
    );
}

#[tokio::test]
async fn regression_null_equality_uses_authoritative_missing_and_null_semantics() {
    let config = test_support::in_memory_config("access-null-equality")
        .with_equality_index("User", "external_id");
    let db = test_support::open_db_with_config(config).await;
    let missing = test_support::add_node_with_properties(&db, "User", Vec::new()).await;
    let explicit_null = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::Null)],
    )
    .await;
    let string_null = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("external_id", PropertyValue::String("null".to_string()))],
    )
    .await;
    let access = |value| {
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:external_id")),
            key: catalog::ScopedPropertyKey::try_new("User", "external_id").unwrap(),
            value: ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value).unwrap()),
        }
    };

    assert_eq!(
        run_node_access(&db, access(PropertyValue::Null)).await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(missing),
            ExecutionScalar::NodeId(explicit_null),
        ])
    );
    assert_eq!(
        run_node_access(&db, access(PropertyValue::String("null".to_string()))).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(string_null)])
    );
}

#[tokio::test]
async fn regression_node_dynamic_equality_never_falls_back_to_colliding_legacy_property_rows() {
    let db = test_support::open_db("access-node-property-hash-collision").await;
    let first_property = crate::config::scoped_secondary_index_property("User", "property_16755");
    let second_property = crate::config::scoped_secondary_index_property("User", "property_36911");
    assert_eq!(
        hash_property_name(&first_property),
        hash_property_name(&second_property)
    );

    let entity_id = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("property_16755", PropertyValue::from("collision"))],
    )
    .await;
    let transaction = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::add_to_equality_index_scoped(
        &transaction,
        &first_property,
        "collision",
        entity_id,
        DataScope::LegacyUnscoped,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let result = db
        .execute(
            &node_access_ids_plan(node_equality! {
                index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                    "node_eq:User:property_36911",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "property_36911").unwrap(),
                value: ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(PropertyValue::from("collision")).unwrap(),
                ),
            }),
            context::ParamBindings::default(),
        )
        .await;

    assert!(matches!(
        result,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));
}

#[tokio::test]
async fn regression_edge_dynamic_equality_never_falls_back_to_colliding_legacy_property_rows() {
    let db = test_support::open_db("access-edge-property-hash-collision").await;
    let first_property = crate::config::scoped_secondary_index_property("User", "property_16755");
    let second_property = crate::config::scoped_secondary_index_property("User", "property_36911");
    assert_eq!(
        hash_property_name(&first_property),
        hash_property_name(&second_property)
    );

    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let entity_id = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "User",
        vec![("property_16755", PropertyValue::from("collision"))],
    )
    .await;
    let transaction = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::add_to_edge_equality_index_scoped(
        &transaction,
        from,
        to,
        entity_id,
        &first_property,
        "collision",
        DataScope::LegacyUnscoped,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let result = db
        .execute(
            &edge_access_ids_plan(edge_equality! {
                index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                    "edge_eq:User:property_36911",
                )),
                key: catalog::ScopedPropertyKey::try_new("User", "property_36911").unwrap(),
                value: ir::IndexValue::Literal(
                    ir::SecondaryIndexLiteral::new(PropertyValue::from("collision")).unwrap(),
                ),
            }),
            context::ParamBindings::default(),
        )
        .await;

    assert!(matches!(
        result,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));
}

#[tokio::test]
async fn regression_colliding_property_names_keep_independent_managed_node_and_edge_indexes() {
    let db = test_support::open_db("access-managed-property-hash-isolation").await;
    let first_node = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("property_16755", PropertyValue::from("first"))],
    )
    .await;
    let second_node = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("property_36911", PropertyValue::from("second"))],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "property_16755").unwrap(),
        80,
        &[("first", first_node)],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_equality("User", "property_36911").unwrap(),
        81,
        &[("second", second_node)],
    )
    .await;

    let first_result = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                "node_eq:User:property_16755",
            )),
            key: catalog::ScopedPropertyKey::try_new("User", "property_16755").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("first")).unwrap(),
            ),
        },
    )
    .await;
    let second_result = run_node_access(
        &db,
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name(
                "node_eq:User:property_36911",
            )),
            key: catalog::ScopedPropertyKey::try_new("User", "property_36911").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("second")).unwrap(),
            ),
        },
    )
    .await;
    assert_eq!(
        first_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(first_node)])
    );
    assert_eq!(
        second_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(second_node)])
    );

    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let first_edge = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "User",
        vec![("property_16755", PropertyValue::from("first"))],
    )
    .await;
    let second_edge = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "User",
        vec![("property_36911", PropertyValue::from("second"))],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_equality("User", "property_16755").unwrap(),
        82,
        &[("first", first_edge)],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_equality("User", "property_36911").unwrap(),
        83,
        &[("second", second_edge)],
    )
    .await;

    let first_result = run_edge_access(
        &db,
        edge_equality! {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                "edge_eq:User:property_16755",
            )),
            key: catalog::ScopedPropertyKey::try_new("User", "property_16755").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("first")).unwrap(),
            ),
        },
    )
    .await;
    let second_result = run_edge_access(
        &db,
        edge_equality! {
            index: catalog::EdgeEqualityIndexMeta::new(test_support::name(
                "edge_eq:User:property_36911",
            )),
            key: catalog::ScopedPropertyKey::try_new("User", "property_36911").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("second")).unwrap(),
            ),
        },
    )
    .await;
    assert_eq!(
        first_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(first_edge)])
    );
    assert_eq!(
        second_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(second_edge)])
    );
}

#[tokio::test]
async fn regression_dynamic_range_never_falls_back_to_colliding_legacy_property_rows() {
    let db = test_support::open_db("access-range-property-hash-collision").await;
    let first_node_property =
        crate::config::scoped_secondary_index_property("User", "property_16755");
    let second_node_property =
        crate::config::scoped_secondary_index_property("User", "property_36911");
    let node_id = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("property_16755", PropertyValue::I64(7))],
    )
    .await;
    let from = test_support::add_user(&db, "from-range").await;
    let to = test_support::add_user(&db, "to-range").await;
    let edge_id = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "User",
        vec![("property_16755", PropertyValue::I64(7))],
    )
    .await;
    let first_edge_property =
        crate::config::scoped_secondary_index_property("User", "property_16755");
    let transaction = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::add_to_range_index_with_direction_scoped(
        &transaction,
        &first_node_property,
        "7",
        node_id,
        StorageRangeIndexDirection::Asc,
        DataScope::LegacyUnscoped,
    )
    .await
    .unwrap();
    crate::search::add_to_edge_range_index_with_direction_scoped(
        &transaction,
        from,
        to,
        edge_id,
        &first_edge_property,
        "7",
        StorageRangeIndexDirection::Asc,
        DataScope::LegacyUnscoped,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    assert_eq!(
        hash_property_name(&first_node_property),
        hash_property_name(&second_node_property)
    );
    let node_result = db
        .execute(
            &node_access_ids_plan(node_range_plan(
                "property_36911",
                helix_ast::index::RangeIndexDirection::Asc,
                ir::IndexRange::All,
            )),
            context::ParamBindings::default(),
        )
        .await;
    assert!(matches!(
        node_result,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));

    let edge_result = db
        .execute(
            &edge_access_ids_plan(exec::ExecEdgeAccessPlan::RangeIndex {
                iteration: helix_planner::ir::RangeScanIteration::Forward,
                index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                    "edge_range:User:property_36911:asc",
                )),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "property_36911",
                    helix_ast::index::RangeIndexDirection::Asc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            }),
            context::ParamBindings::default(),
        )
        .await;
    assert!(matches!(
        edge_result,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));
}

#[tokio::test]
async fn dynamic_equality_ignores_legacy_bitmaps_while_builtin_label_scan_remains_fail_closed() {
    let db = test_support::open_db("access-corrupt-equality-bitmaps").await;
    let node_property = crate::config::scoped_secondary_index_property("User", "status");
    let edge_property = crate::config::scoped_secondary_index_property("FOLLOWS", "status");
    for key in [
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
                hash_property_name("$label"),
                hash_property_value("User"),
            ))),
        }
        .to_bytes(),
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
                hash_property_name(&node_property),
                hash_property_value("active"),
            ))),
        }
        .to_bytes(),
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeEquality(
                GlobalEdgeEqualityIndexKey::new(
                    hash_property_name(&edge_property),
                    hash_property_value("active"),
                ),
            )),
        }
        .to_bytes(),
    ] {
        db.inner_db()
            .put(key, bytes::Bytes::from_static(b"corrupt bitmap"))
            .await
            .expect("corrupt equality bitmap writes");
    }

    let node = node_equality! {
        index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
        key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
        value: ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(node),
            context::ParamBindings::default(),
        )
        .await,
        Err(HelixDbError::IndexLifecycleUnavailable {
            family: IndexFamily::Secondary,
            reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        })
    ));

    assert!(matches!(
        db.execute(
            &node_access_ids_plan(exec::ExecNodeAccessPlan::LabelScan {
                label: test_support::name("User"),
            }),
            context::ParamBindings::default(),
        )
        .await,
        Err(HelixDbError::Encoding(_))
    ));

    let edge = edge_equality! {
        index: catalog::EdgeEqualityIndexMeta::new(test_support::name("edge_eq:FOLLOWS:status")),
        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
        value: ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    };
    assert!(matches!(
        db.execute(
            &edge_access_ids_plan(edge),
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
async fn managed_secondary_access_propagates_corrupt_canonical_records() {
    let config = test_support::in_memory_config("access-corrupt-secondary-records")
        .with_equality_index("User", "status")
        .with_range_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    for definition in [
        SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
        SecondaryIndexDefinition::node_range("User", "score").unwrap(),
    ] {
        let identity = ValidatedDynamicIndexDefinition::try_from(definition)
            .expect("secondary definition validates")
            .identity();
        db.inner_db()
            .put(
                ManagedKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::index_record(identity),
                }
                .to_bytes(),
                bytes::Bytes::from_static(b"corrupt canonical record"),
            )
            .await
            .expect("corrupt canonical record writes");
    }

    for plan in [
        node_equality! {
            index: catalog::NodeEqualityIndexMeta::new(test_support::name("node_eq:User:status")),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
            ),
        },
        exec::ExecNodeAccessPlan::RangeIndex {
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score",
                helix_ast::index::RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        },
    ] {
        assert!(matches!(
            db.execute(
                &node_access_ids_plan(plan),
                context::ParamBindings::default(),
            )
            .await,
            Err(HelixDbError::Encoding(_))
        ));
    }
}

#[tokio::test]
async fn range_access_rejects_oversized_identity_components() {
    let db = test_support::open_db("access-oversized-range-identity").await;
    let oversized = "x".repeat(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN + 1);
    let node = exec::ExecNodeAccessPlan::RangeIndex {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::NodeRangeIndexMeta::new(test_support::name("oversized-node-range")),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            oversized.clone(),
            "score",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };
    let edge = exec::ExecEdgeAccessPlan::RangeIndex {
        iteration: helix_planner::ir::RangeScanIteration::Forward,
        index: catalog::EdgeRangeIndexMeta::new(test_support::name("oversized-edge-range")),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            oversized,
            "weight",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };

    assert!(matches!(
        db.execute(
            &node_access_ids_plan(node),
            context::ParamBindings::default(),
        )
        .await,
        Err(HelixDbError::InvalidIndexV2Model(_))
    ));
    assert!(matches!(
        db.execute(
            &edge_access_ids_plan(edge),
            context::ParamBindings::default(),
        )
        .await,
        Err(HelixDbError::InvalidIndexV2Model(_))
    ));
}

#[tokio::test]
async fn regression_ascending_node_range_matches_typed_oracle_across_signed_extremes() {
    let config = test_support::in_memory_config("access-signed-node-range-ascending")
        .with_range_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let mut expected = Vec::new();
    for score in [i64::MIN, -10, -2, -1, 0, 1, 2, 10, i64::MAX] {
        expected.push(
            test_support::add_node_with_properties(
                &db,
                "User",
                vec![("score", PropertyValue::I64(score))],
            )
            .await,
        );
    }

    let actual = run_node_access(
        &db,
        node_range_plan(
            "score",
            helix_ast::index::RangeIndexDirection::Asc,
            ir::IndexRange::All,
        ),
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(expected.into_iter().map(ExecutionScalar::NodeId).collect()),
        "ascending index order must equal independent signed-i64 order"
    );
}

#[tokio::test]
async fn regression_descending_node_range_matches_typed_oracle_across_signed_extremes() {
    let config = test_support::in_memory_config("access-signed-node-range-descending")
        .with_range_desc_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let mut expected = Vec::new();
    for score in [i64::MIN, -10, -2, -1, 0, 1, 2, 10, i64::MAX] {
        expected.push(
            test_support::add_node_with_properties(
                &db,
                "User",
                vec![("score", PropertyValue::I64(score))],
            )
            .await,
        );
    }
    expected.reverse();

    let actual = run_node_access(
        &db,
        node_range_plan(
            "score",
            helix_ast::index::RangeIndexDirection::Desc,
            ir::IndexRange::All,
        ),
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(expected.into_iter().map(ExecutionScalar::NodeId).collect()),
        "descending index order must reverse independent signed-i64 order"
    );
}

#[tokio::test]
async fn regression_signed_node_range_bounds_and_limit_match_typed_oracle() {
    let config = test_support::in_memory_config("access-signed-node-range-bounds")
        .with_range_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    let mut ids = Vec::new();
    for score in [-100, -10, -2, -1, 0, 1] {
        ids.push(
            test_support::add_node_with_properties(
                &db,
                "User",
                vec![("score", PropertyValue::I64(score))],
            )
            .await,
        );
    }
    let range = ir::IndexRange::Between(
        ir::IndexBetweenRange::new(
            ir::IndexBound::Inclusive(
                ir::RangeIndexValue::literal(PropertyValue::I64(-10)).unwrap(),
            ),
            ir::IndexBound::Exclusive(ir::RangeIndexValue::literal(PropertyValue::I64(0)).unwrap()),
        )
        .unwrap(),
    );

    let actual = run_limited_node_access(
        &db,
        node_range_plan("score", helix_ast::index::RangeIndexDirection::Asc, range),
        2,
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(ids[1]),
            ExecutionScalar::NodeId(ids[2]),
        ]),
        "bounds must be applied semantically before LIMIT"
    );
}

#[tokio::test]
async fn regression_edge_range_matches_typed_oracle_for_negative_values() {
    let config = test_support::in_memory_config("access-signed-edge-range")
        .with_edge_range_index("FOLLOWS", "weight");
    let db = test_support::open_db_with_config(config).await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let mut expected = Vec::new();
    for weight in [-10, -2, -1, 0, 1] {
        expected.push(
            test_support::add_edge_with_properties(
                &db,
                from,
                to,
                "FOLLOWS",
                vec![("weight", PropertyValue::I64(weight))],
            )
            .await,
        );
    }

    let actual = run_edge_access(
        &db,
        edge_range_plan(
            "weight",
            helix_ast::index::RangeIndexDirection::Asc,
            ir::IndexRange::All,
        ),
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(expected.into_iter().map(ExecutionScalar::EdgeId).collect()),
        "edge range order must equal independent signed-i64 order"
    );
}

#[tokio::test]
async fn regression_node_range_remains_correct_after_reopen() {
    let config = test_support::in_memory_config("access-signed-node-range-reopen")
        .with_range_index("User", "score");
    let writer = test_support::open_db_with_config(config.clone()).await;
    let lower = test_support::add_node_with_properties(
        &writer,
        "User",
        vec![("score", PropertyValue::I64(-10))],
    )
    .await;
    let higher = test_support::add_node_with_properties(
        &writer,
        "User",
        vec![("score", PropertyValue::I64(-2))],
    )
    .await;
    writer.close().await.expect("writer closes");
    let reader = test_support::open_db_with_config(config).await;

    let actual = run_node_access(
        &reader,
        node_range_plan(
            "score",
            helix_ast::index::RangeIndexDirection::Asc,
            ir::IndexRange::All,
        ),
    )
    .await;

    assert_eq!(
        actual,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(lower),
            ExecutionScalar::NodeId(higher),
        ]),
        "persisted range rows must preserve semantic order after reopen"
    );
}

#[tokio::test]
async fn node_range_access_uses_configured_directional_index() {
    let config = test_support::in_memory_config("access-node-range-index")
        .with_range_index("User", "score_asc")
        .with_range_desc_index("User", "score_desc");
    let db = test_support::open_db_with_config(config).await;
    let low = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("low")),
            ("score_asc", PropertyValue::I64(10)),
            ("score_desc", PropertyValue::I64(10)),
        ],
    )
    .await;
    let medium = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("medium")),
            ("score_asc", PropertyValue::I64(20)),
            ("score_desc", PropertyValue::I64(20)),
        ],
    )
    .await;
    let _high = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("high")),
            ("score_asc", PropertyValue::I64(30)),
            ("score_desc", PropertyValue::I64(30)),
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_desc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_desc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_desc",
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
        .with_range_index("User", "score_asc")
        .with_range_desc_index("User", "score_desc");
    let db = test_support::open_db_with_config(config).await;
    let _low = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("low")),
            ("score_asc", PropertyValue::I64(10)),
            ("score_desc", PropertyValue::I64(10)),
        ],
    )
    .await;
    let medium = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("medium")),
            ("score_asc", PropertyValue::I64(20)),
            ("score_desc", PropertyValue::I64(20)),
        ],
    )
    .await;
    let high = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("high")),
            ("score_asc", PropertyValue::I64(30)),
            ("score_desc", PropertyValue::I64(30)),
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::NodeRangeIndexMeta::new(test_support::name(
                "node_range:User:score_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "score_desc",
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
        .with_edge_range_index("FOLLOWS", "weight_asc")
        .with_edge_range_desc_index("FOLLOWS", "weight_desc");
    let db = test_support::open_db_with_config(config).await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let light = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "FOLLOWS",
        vec![
            ("weight_asc", PropertyValue::I64(10)),
            ("weight_desc", PropertyValue::I64(10)),
        ],
    )
    .await;
    let heavy = test_support::add_edge_with_properties(
        &db,
        alice,
        carol,
        "FOLLOWS",
        vec![
            ("weight_asc", PropertyValue::I64(30)),
            ("weight_desc", PropertyValue::I64(30)),
        ],
    )
    .await;
    let medium = test_support::add_edge_with_properties(
        &db,
        bob,
        carol,
        "FOLLOWS",
        vec![
            ("weight_asc", PropertyValue::I64(20)),
            ("weight_desc", PropertyValue::I64(20)),
        ],
    )
    .await;

    let asc_all = run_edge_access(
        &db,
        exec::ExecEdgeAccessPlan::RangeIndex {
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_desc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_desc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_desc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_desc",
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
        .with_edge_range_index("FOLLOWS", "weight_asc")
        .with_edge_range_desc_index("FOLLOWS", "weight_desc");
    let db = test_support::open_db_with_config(config).await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let _light = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "FOLLOWS",
        vec![
            ("weight_asc", PropertyValue::I64(10)),
            ("weight_desc", PropertyValue::I64(10)),
        ],
    )
    .await;
    let heavy = test_support::add_edge_with_properties(
        &db,
        alice,
        carol,
        "FOLLOWS",
        vec![
            ("weight_asc", PropertyValue::I64(30)),
            ("weight_desc", PropertyValue::I64(30)),
        ],
    )
    .await;
    let medium = test_support::add_edge_with_properties(
        &db,
        bob,
        carol,
        "FOLLOWS",
        vec![
            ("weight_asc", PropertyValue::I64(20)),
            ("weight_desc", PropertyValue::I64(20)),
        ],
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_asc:asc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_asc",
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
            index: catalog::EdgeRangeIndexMeta::new(test_support::name(
                "edge_range:FOLLOWS:weight_desc:desc",
            )),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight_desc",
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
async fn direct_range_access_covers_writer_reader_and_active_transaction_views() {
    let config = test_support::in_memory_config("access-direct-range-views")
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
    let edge = test_support::add_edge_with_properties(
        &writer,
        low,
        high,
        "FOLLOWS",
        vec![("weight", PropertyValue::I64(30))],
    )
    .await;
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

    let writer_context = ExecutionContext::new(&writer, context::ParamBindings::default());
    assert_eq!(
        writer_context
            .node_range_index_ids(&node_key, &ir::IndexRange::All, None)
            .await
            .expect("direct writer range access succeeds"),
        vec![low, high]
    );
    assert_eq!(
        writer_context
            .edge_range_index_ids(&edge_key, &ir::IndexRange::All, None)
            .await
            .expect("direct writer edge range access succeeds"),
        vec![edge]
    );

    let mut transaction_context = ExecutionContext::new(&writer, context::ParamBindings::default());
    transaction_context
        .enable_request_write_scope()
        .await
        .expect("request transaction opens");
    assert_eq!(
        transaction_context
            .node_range_index_ids(&node_key, &ir::IndexRange::All, None)
            .await
            .expect("transaction-owned node range access succeeds"),
        vec![low, high]
    );
    assert_eq!(
        transaction_context
            .edge_range_index_ids(&edge_key, &ir::IndexRange::All, None)
            .await
            .expect("transaction-owned edge range access succeeds"),
        vec![edge]
    );
    assert_eq!(
        transaction_context
            .node_range_index_count_with_membership(&node_key, &ir::IndexRange::All, &[], None,)
            .await
            .expect("transaction-owned node range count succeeds"),
        2
    );
    assert_eq!(
        transaction_context
            .edge_range_index_count_with_membership(&edge_key, &ir::IndexRange::All, &[], None,)
            .await
            .expect("transaction-owned edge range count succeeds"),
        1
    );
    assert_eq!(
        transaction_context
            .node_range_index_count_with_membership(
                &node_key,
                &ir::IndexRange::Lower {
                    lower: ir::IndexBound::Inclusive(
                        ir::RangeIndexValue::literal(PropertyValue::I64(15)).unwrap(),
                    ),
                },
                &[],
                None,
            )
            .await
            .expect("transaction-owned bounded node range count succeeds"),
        1
    );
    let absent_key = catalog::ScopedPropertyDirectionKey::try_new(
        "Missing",
        "score",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .unwrap();
    assert!(transaction_context
        .node_range_index_count_with_membership(&absent_key, &ir::IndexRange::All, &[], None,)
        .await
        .is_err());
    transaction_context.abort_request_write_scope();

    drop(writer);
    let reader = test_support::open_reader_with_config(config).await;
    let reader_context = ExecutionContext::new(&reader, context::ParamBindings::default());
    assert_eq!(
        reader_context
            .node_range_index_ids(&node_key, &ir::IndexRange::All, None)
            .await
            .expect("direct reader node range access succeeds"),
        vec![low, high]
    );
    assert_eq!(
        reader_context
            .edge_range_index_ids(&edge_key, &ir::IndexRange::All, None)
            .await
            .expect("direct reader edge range access succeeds"),
        vec![edge]
    );
    for (element, key, expected) in [
        (IndexElementKind::Node, &node_key, vec![low, high]),
        (IndexElementKind::Edge, &edge_key, vec![edge]),
    ] {
        let mut cursor = reader_context
            .open_range_cursor(
                element,
                key,
                &ir::IndexRange::All,
                ir::RangeScanIteration::Forward,
            )
            .await
            .unwrap();
        for id in expected {
            assert_eq!(
                reader_context.next_range_cursor(&mut cursor).await.unwrap(),
                Some(id)
            );
        }
        assert!(reader_context
            .next_range_cursor(&mut cursor)
            .await
            .unwrap()
            .is_none());
    }
    reader.close().await.unwrap();
}

#[tokio::test]
async fn exact_range_count_rejects_missing_bounds_and_oversized_identity_components() {
    let db = test_support::open_db("access-exact-range-count-invalid-inputs").await;
    let context = ExecutionContext::new(&db, context::ParamBindings::default());
    let valid_label = ir::NonEmptyString::new("User").unwrap();
    let valid_property = ir::NonEmptyString::new("score").unwrap();
    let direction = helix_ast::index::RangeIndexDirection::Asc;
    let valid_key = catalog::ScopedPropertyDirectionKey::new(
        valid_label.clone(),
        valid_property.clone(),
        direction,
    );
    let missing_bound = ir::IndexRange::Lower {
        lower: ir::IndexBound::Inclusive(ir::RangeIndexValue::Param(
            ir::NonEmptyString::new("missing").unwrap(),
        )),
    };
    assert!(context
        .node_range_index_count_with_membership(&valid_key, &missing_bound, &[], None)
        .await
        .is_err());

    let oversized = ir::NonEmptyString::new("x".repeat(u16::MAX as usize + 1)).unwrap();
    for key in [
        catalog::ScopedPropertyDirectionKey::new(oversized.clone(), valid_property, direction),
        catalog::ScopedPropertyDirectionKey::new(valid_label, oversized, direction),
    ] {
        assert!(context
            .node_range_index_count_with_membership(&key, &ir::IndexRange::All, &[], None)
            .await
            .is_err());
    }
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
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
            iteration: helix_planner::ir::RangeScanIteration::Forward,
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

/// Users with Active `status`, `role` and `tier` equality indexes and a `rank`
/// range index: `[active admin gold, active member gold, paused admin silver]`.
async fn concurrent_set_fixture(name: &str) -> (HelixDB, [u64; 3]) {
    let db = test_support::open_db(name).await;
    let mut users = [0; 3];
    for (user, (status, role, tier, rank)) in users.iter_mut().zip([
        ("active", "admin", "gold", "a"),
        ("active", "member", "gold", "b"),
        ("paused", "admin", "silver", "c"),
    ]) {
        *user = test_support::add_node_with_properties(
            &db,
            "User",
            vec![
                ("status", PropertyValue::from(status)),
                ("role", PropertyValue::from(role)),
                ("tier", PropertyValue::from(tier)),
                ("rank", PropertyValue::from(rank)),
            ],
        )
        .await;
    }
    let [active_admin, active_member, paused_admin] = users;
    for (definition, index_id, rows) in [
        (
            SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
            54,
            [
                ("active", active_admin),
                ("active", active_member),
                ("paused", paused_admin),
            ],
        ),
        (
            SecondaryIndexDefinition::node_equality("User", "role").unwrap(),
            55,
            [
                ("admin", active_admin),
                ("member", active_member),
                ("admin", paused_admin),
            ],
        ),
        (
            SecondaryIndexDefinition::node_equality("User", "tier").unwrap(),
            56,
            [
                ("gold", active_admin),
                ("gold", active_member),
                ("silver", paused_admin),
            ],
        ),
        (
            SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
            58,
            [
                ("a", active_admin),
                ("b", active_member),
                ("c", paused_admin),
            ],
        ),
    ] {
        seed_active_secondary_generation(&db, definition, index_id, &rows).await;
    }
    (db, users)
}

/// One exact equality leaf over `User.property` resolved through `index`.
fn user_equality(index: &str, property: &str, value: &str) -> exec::ExecNodeSecondarySetPlan {
    node_set_equalities! {
        index: catalog::NodeEqualityIndexMeta::new(test_support::name(index)),
        key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
        values: ir::AtLeast::from_one(ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
        )),
    }
}

#[tokio::test]
async fn set_children_are_read_concurrently_up_to_the_bound() {
    use std::sync::atomic::Ordering;

    let (db, [active_admin, _, _]) =
        concurrent_set_fixture("access-concurrent-secondary-children").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context.enable_request_read_view().await.unwrap();
    let peak = &db.inner.peak_index_child_reads;

    peak.store(0, Ordering::SeqCst);
    let ids = context
        .node_secondary_set_bitmap(&exec::ExecNodeSecondarySetPlan::Intersect {
            driver: Box::new(user_equality("node_eq:User:status", "status", "active")),
            rest: ir::AtLeast::try_from_vec(vec![
                user_equality("node_eq:User:role", "role", "admin"),
                user_equality("node_eq:User:tier", "tier", "gold"),
            ])
            .unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![active_admin]);
    assert_eq!(peak.load(Ordering::SeqCst), 3);

    let point = |property: &str, value: &str| exec::ExecNodeBitmapExpr::PointRead {
        index: exec::ExecNodeNonUniqueEqualityIndex::try_from(catalog::NodeEqualityIndexMeta::new(
            test_support::name(&format!("node_eq:User:{property}")),
        ))
        .unwrap(),
        key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
        value: exec::ExecIndexedEqualityValue::try_from(
            ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
        )
        .unwrap(),
    };
    peak.store(0, Ordering::SeqCst);
    let ids = context
        .node_bitmap(
            &exec::ExecNodeBitmapExpr::Intersect {
                driver: Box::new(point("status", "active")),
                rest: ir::AtLeast::try_from_vec(vec![
                    point("role", "admin"),
                    point("tier", "gold"),
                ])
                .unwrap(),
            },
            crate::execution::interpreter::access::PARALLEL_INDEX_READS,
        )
        .await
        .unwrap();
    assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![active_admin]);
    assert_eq!(peak.load(Ordering::SeqCst), 3);
    context.close_request_read_view().unwrap();

    peak.store(0, Ordering::SeqCst);
    assert_eq!(
        run_limited_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::OrderedIntersect {
                    driver: exec::ExecNodeSecondaryRangePlan {
                        iteration: helix_planner::ir::RangeScanIteration::Forward,
                        index: catalog::NodeRangeIndexMeta::new(test_support::name(
                            "node_range:User:rank:asc",
                        )),
                        key: catalog::ScopedPropertyDirectionKey::try_new(
                            "User",
                            "rank",
                            helix_ast::index::RangeIndexDirection::Asc,
                        )
                        .unwrap(),
                        range: ir::IndexRange::All,
                    },
                    filters: ir::AtLeast::<_, 1>::try_from_vec(vec![
                        user_equality("node_eq:User:status", "status", "active"),
                        user_equality("node_eq:User:role", "role", "admin"),
                    ])
                    .unwrap(),
                },
            },
            1,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(active_admin)])
    );
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn disjoint_ordered_filters_skip_the_range_driver() {
    use std::sync::atomic::Ordering;

    let (db, _) = concurrent_set_fixture("access-ordered-disjoint-filters").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context.enable_request_read_view().await.unwrap();
    let ordered = |filters: Vec<exec::ExecNodeSecondarySetPlan>| {
        exec::ExecNodeSecondarySetPlan::OrderedIntersect {
            driver: exec::ExecNodeSecondaryRangePlan {
                iteration: helix_planner::ir::RangeScanIteration::Forward,
                index: catalog::NodeRangeIndexMeta::new(test_support::name(
                    "node_range:User:rank:asc",
                )),
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "rank",
                    helix_ast::index::RangeIndexDirection::Asc,
                )
                .unwrap(),
                range: ir::IndexRange::All,
            },
            filters: ir::AtLeast::<_, 1>::try_from_vec(filters).unwrap(),
        }
    };

    // `paused` and `gold` each match a user, but never the same one.
    let disjoint = ordered(vec![
        user_equality("node_eq:User:status", "status", "paused"),
        user_equality("node_eq:User:tier", "tier", "gold"),
    ]);
    assert_eq!(
        context
            .node_secondary_set_ids(&disjoint, None)
            .await
            .unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(context.range_reads.entries.load(Ordering::Relaxed), 0);

    // Overlapping filters still drive the range in order.
    let overlapping = ordered(vec![
        user_equality("node_eq:User:status", "status", "active"),
        user_equality("node_eq:User:tier", "tier", "gold"),
    ]);
    assert_eq!(
        context
            .node_secondary_set_ids(&overlapping, None)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(context.range_reads.entries.load(Ordering::Relaxed) > 0);
    context.close_request_read_view().unwrap();
}

#[tokio::test(start_paused = true)]
async fn read_children_yields_results_and_the_first_error_in_plan_order() {
    use futures::{FutureExt, TryStreamExt};

    let db = test_support::open_db("access-read-children-plan-order").await;
    let context = ExecutionContext::new(&db, context::ParamBindings::default());
    let read = |children: &'static [(u64, Result<u64, &'static str>)]| {
        context
            .read_children(
                children.iter().collect(),
                crate::execution::interpreter::access::PARALLEL_INDEX_READS,
                |(delay, result), _| {
                    async move {
                        tokio::time::sleep(std::time::Duration::from_millis(*delay)).await;
                        result.map_err(|message| HelixDbError::Query(message.to_string()))
                    }
                    .boxed()
                },
            )
            .try_collect::<Vec<_>>()
    };

    let error = read(&[(10, Err("a")), (1, Err("b"))]).await.unwrap_err();
    assert!(
        matches!(&error, HelixDbError::Query(message) if message == "a"),
        "{error:?}"
    );
    let error = read(&[(10, Ok(1)), (1, Err("b"))]).await.unwrap_err();
    assert!(
        matches!(&error, HelixDbError::Query(message) if message == "b"),
        "{error:?}"
    );
    assert_eq!(
        read(&[(10, Ok(1)), (1, Ok(2)), (5, Ok(3))]).await.unwrap(),
        vec![1, 2, 3]
    );
}

/// A synthetic set whose leaves each sleep one millisecond while counted live.
enum ReadTree {
    Leaf,
    Set(Vec<ReadTree>),
}

/// Leaf reads live now, and the most seen live at once.
#[derive(Default)]
struct LiveLeaves {
    live: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

/// Read `tree` the way a nested secondary set reads its children.
fn read_tree<'a>(
    context: &'a ExecutionContext<'_>,
    tree: &'a ReadTree,
    reads: core::num::NonZeroUsize,
    leaves: &'a LiveLeaves,
) -> futures::future::BoxFuture<'a, crate::error::Result<()>> {
    use futures::{FutureExt, TryStreamExt};
    use std::sync::atomic::Ordering;

    async move {
        match tree {
            ReadTree::Leaf => {
                let live = leaves.live.fetch_add(1, Ordering::SeqCst) + 1;
                leaves.peak.fetch_max(live, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                leaves.live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
            ReadTree::Set(children) => context
                .read_children(children.iter().collect(), reads, |child, reads| {
                    read_tree(context, child, reads, leaves)
                })
                .try_collect::<Vec<()>>()
                .await
                .map(drop),
        }
    }
    .boxed()
}

#[tokio::test(start_paused = true)]
async fn nested_sets_never_exceed_the_read_budget() {
    let db = test_support::open_db("access-read-children-budget").await;
    let context = ExecutionContext::new(&db, context::ParamBindings::default());
    let leaves = |count| ReadTree::Set((0..count).map(|_| ReadTree::Leaf).collect());
    let nested = || ReadTree::Set(vec![leaves(12), leaves(12)]);
    for (tree, reads, expected) in [
        (nested(), 16, 16),
        (leaves(20), 16, 16),
        (leaves(3), 16, 3),
        (nested(), 1, 1),
    ] {
        let live = LiveLeaves::default();
        let reads = core::num::NonZeroUsize::new(reads).unwrap();
        read_tree(&context, &tree, reads, &live).await.unwrap();
        assert_eq!(
            live.peak.load(std::sync::atomic::Ordering::SeqCst),
            expected,
            "reads={reads}"
        );
    }
}

/// Sets resolved at once, by one step or by parallel steps sharing the
/// request's pool, add at most `PARALLEL_INDEX_READS - 1` reads beyond one
/// each, and the pool refills when they finish.
#[tokio::test(start_paused = true)]
async fn concurrent_sets_share_one_request_read_budget() {
    let db = test_support::open_db("access-read-children-request-budget").await;
    let context = ExecutionContext::new(&db, context::ParamBindings::default());
    let wide = ReadTree::Set((0..20).map(|_| ReadTree::Leaf).collect());
    let reads = crate::execution::interpreter::access::PARALLEL_INDEX_READS;
    let live = LiveLeaves::default();
    futures::try_join!(
        read_tree(&context, &wide, reads, &live),
        read_tree(&context, &wide, reads, &live),
        read_tree(&context, &wide, reads, &live),
    )
    .unwrap();
    assert_eq!(
        live.peak.load(std::sync::atomic::Ordering::SeqCst),
        reads.get() + 2
    );

    let live = LiveLeaves::default();
    read_tree(&context, &wide, reads, &live).await.unwrap();
    assert_eq!(
        live.peak.load(std::sync::atomic::Ordering::SeqCst),
        reads.get()
    );
}

#[tokio::test]
async fn set_errors_surface_in_plan_order_and_empty_children_do_not_short_circuit() {
    let (db, _) = concurrent_set_fixture("access-concurrent-secondary-errors").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context.enable_request_read_view().await.unwrap();
    let corrupt = || user_equality("not-a-planner-identity", "status", "active");
    let unavailable = || user_equality("node_eq:User:color", "color", "red");
    let intersect = |driver, rest| exec::ExecNodeSecondarySetPlan::Intersect {
        driver: Box::new(driver),
        rest: ir::AtLeast::from_one(rest),
    };

    let error = context
        .node_secondary_set_bitmap(&intersect(corrupt(), unavailable()))
        .await
        .unwrap_err();
    assert!(
        matches!(error, HelixDbError::IndexCatalogCorruption(_)),
        "{error:?}"
    );
    let error = context
        .node_secondary_set_bitmap(&intersect(unavailable(), corrupt()))
        .await
        .unwrap_err();
    assert!(
        matches!(error, HelixDbError::IndexLifecycleUnavailable { .. }),
        "{error:?}"
    );
    for set in [
        intersect(exec::ExecNodeSecondarySetPlan::Empty, corrupt()),
        exec::ExecNodeSecondarySetPlan::Union {
            driver: Box::new(exec::ExecNodeSecondarySetPlan::Empty),
            rest: ir::AtLeast::from_one(corrupt()),
        },
    ] {
        let error = context.node_secondary_set_bitmap(&set).await.unwrap_err();
        assert!(
            matches!(error, HelixDbError::IndexCatalogCorruption(_)),
            "{error:?}"
        );
    }
    context.close_request_read_view().unwrap();
}

#[tokio::test]
async fn range_bounds_without_an_encoding_are_empty() {
    let config = test_support::in_memory_config("access-range-unencodable-bounds")
        .with_range_index("User", "score");
    let db = test_support::open_db_with_config(config).await;
    for score in [10, 20, 30] {
        test_support::add_node_with_properties(
            &db,
            "User",
            vec![("score", PropertyValue::I64(score))],
        )
        .await;
    }
    let min = test_support::name("min_score");
    let max = test_support::name("max_score");
    let key = catalog::ScopedPropertyDirectionKey::try_new(
        "User",
        "score",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .expect("valid key");
    let plan = |range: ir::IndexRange, iteration| exec::ExecNodeAccessPlan::RangeIndex {
        iteration,
        index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:score:asc")),
        key: key.clone(),
        range,
    };
    let lower = ir::IndexRange::Lower {
        lower: ir::IndexBound::Inclusive(ir::RangeIndexValue::Param(min.clone())),
    };
    // No stored value orders against null, bool or NaN, nor between bounds
    // of different domains: every such range is empty, never an error.
    for (low, high) in [
        (PropertyValue::Null, PropertyValue::I64(40)),
        (PropertyValue::Bool(true), PropertyValue::I64(40)),
        (PropertyValue::F64(f64::NAN), PropertyValue::I64(40)),
        (PropertyValue::I64(20), PropertyValue::from("z")),
    ] {
        let params = context::ParamBindings::default()
            .with_value(min.clone(), low.clone())
            .with_value(max.clone(), high);
        for iteration in [
            helix_planner::ir::RangeScanIteration::Forward,
            helix_planner::ir::RangeScanIteration::Reverse,
        ] {
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan(
                        parameterized_i64_between(min.clone(), max.clone()),
                        iteration
                    ),
                    params.clone(),
                )
                .await,
                ExecutionValue::Scalars(Vec::new()),
                "{low:?}"
            );
            assert_eq!(
                run_limited_node_access_with_params(
                    &db,
                    plan(
                        parameterized_i64_between(min.clone(), max.clone()),
                        iteration
                    ),
                    2,
                    params.clone(),
                )
                .await,
                ExecutionValue::Scalars(Vec::new()),
                "{low:?}"
            );
        }
        if low.as_i64().is_none() {
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    plan(
                        lower.clone(),
                        helix_planner::ir::RangeScanIteration::Forward
                    ),
                    params,
                )
                .await,
                ExecutionValue::Scalars(Vec::new()),
                "{low:?}"
            );
        }
    }
}

#[tokio::test]
async fn intersections_verify_label_rows_only_against_their_other_children() {
    crate::index_lifecycle::secondary::EqualityReadObserver::default()
        .scope(async {
            // Null equality, and runtime equality or domains binding null, verify
            // label records. Inside an intersection they read only the rows the
            // other children keep: no label bitmap, no lane scan, and one record per
            // kept row, however many label rows have the property null.
            let db = test_support::open_db("access-null-intersections-verify-within").await;
            let mut active = Vec::new();
            for deleted_at in [
                Some(PropertyValue::Null),
                None,
                Some(PropertyValue::from("2024")),
                Some(PropertyValue::from("2025")),
            ] {
                active.push(
                    test_support::add_node_with_properties(
                        &db,
                        "User",
                        core::iter::once(("status", PropertyValue::from("active")))
                            .chain(deleted_at.map(|value| ("deleted_at", value)))
                            .collect(),
                    )
                    .await,
                );
            }
            let mut paused = Vec::new();
            for _ in 0..30 {
                paused.push(
                    test_support::add_node_with_properties(
                        &db,
                        "User",
                        vec![("status", PropertyValue::from("paused"))],
                    )
                    .await,
                );
            }
            let status_rows = active
                .iter()
                .map(|id| ("active", *id))
                .chain(paused.iter().map(|id| ("paused", *id)))
                .collect::<Vec<_>>();
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
                80,
                &status_rows,
            )
            .await;
            seed_active_secondary_generation(
                &db,
                SecondaryIndexDefinition::node_equality("User", "deleted_at").unwrap(),
                81,
                &[("2024", active[2]), ("2025", active[3])],
            )
            .await;

            let index = |property: &str| {
                catalog::NodeEqualityIndexMeta::new(test_support::name(&format!(
                    "node_eq:User:{property}"
                )))
            };
            let key =
                |property: &str| catalog::ScopedPropertyKey::try_new("User", property).unwrap();
            let active_status = || exec::ExecNodeBitmapExpr::PointRead {
                index: exec::ExecNodeNonUniqueEqualityIndex::try_from(index("status")).unwrap(),
                key: key("status"),
                value: exec::ExecIndexedEqualityValue::try_from(
                    ir::SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
                )
                .unwrap(),
            };
            let param = test_support::name("deleted_at");
            let values =
                ir::RuntimeEqualitySet::new(param.clone(), std::num::NonZeroUsize::new(2).unwrap());
            let null_param =
                || context::ParamBindings::default().with_value(param.clone(), PropertyValue::Null);
            let null_list = || {
                context::ParamBindings::default().with_value(
                    param.clone(),
                    PropertyValue::Array(vec![PropertyValue::Null, PropertyValue::from("1999")]),
                )
            };
            let mut expected = [active[0], active[1]];
            expected.sort_unstable();

            let row_cases = [
                (
                    exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                        exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                            key: key("deleted_at"),
                        },
                    ),
                    context::ParamBindings::default(),
                ),
                (
                    exec::ExecNodeSecondarySetPlan::DynamicEquality {
                        index: index("deleted_at"),
                        key: key("deleted_at"),
                        param: param.clone(),
                    },
                    null_param(),
                ),
                (
                    exec::ExecNodeSecondarySetPlan::DynamicMembership {
                        index: index("deleted_at"),
                        key: key("deleted_at"),
                        values: values.clone(),
                    },
                    null_list(),
                ),
            ];
            for (late, params) in row_cases {
                crate::index_lifecycle::secondary::reset_equality_read_metrics();
                assert_eq!(
                    run_node_access_with_params(
                        &db,
                        exec::ExecNodeAccessPlan::SecondarySet {
                            // The late leaf comes first; it is still read last.
                            set: exec::ExecNodeSecondarySetPlan::Intersect {
                                driver: Box::new(late),
                                rest: ir::AtLeast::from_one(
                                    exec::ExecNodeSecondarySetPlan::Bitmap(active_status(),)
                                ),
                            },
                        },
                        params,
                    )
                    .await,
                    ExecutionValue::Scalars(
                        expected
                            .iter()
                            .copied()
                            .map(ExecutionScalar::NodeId)
                            .collect()
                    )
                );
                let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
                assert_eq!(metrics.scans, 0);
                assert_eq!(metrics.graph_reads, active.len() as u64);
            }

            let count_cases = [
                (
                    exec::ExecCountCursorPlan::NodeAuthoritativeScan(
                        exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                            key: key("deleted_at"),
                        },
                    ),
                    context::ParamBindings::default(),
                ),
                (
                    exec::ExecCountCursorPlan::NodeDynamicEquality {
                        index: index("deleted_at"),
                        key: key("deleted_at"),
                        param: param.clone(),
                    },
                    null_param(),
                ),
                (
                    exec::ExecCountCursorPlan::NodeDynamicMembership {
                        index: index("deleted_at"),
                        key: key("deleted_at"),
                        values,
                    },
                    null_list(),
                ),
            ];
            for (late, params) in count_cases {
                let mut execution = ExecutionContext::new(&db, params);
                execution.enable_request_read_view().await.unwrap();
                crate::index_lifecycle::secondary::reset_equality_read_metrics();
                assert_eq!(
                    execution
                        .pull_count_cardinality(
                            &exec::ExecCountCursorPlan::Intersect {
                                driver: Box::new(late),
                                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::NodeBitmap(
                                    active_status(),
                                )),
                            },
                            &mut None,
                            0,
                            None,
                        )
                        .await
                        .unwrap(),
                    expected.len()
                );
                let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
                assert_eq!(metrics.scans, 0);
                assert_eq!(metrics.graph_reads, active.len() as u64);
                execution.close_request_read_view().unwrap();
            }

            // Alone, a null runtime equality still reads the label rows outside the
            // lane: one lane scan, every row without the property verified.
            crate::index_lifecycle::secondary::reset_equality_read_metrics();
            let mut alone = expected
                .iter()
                .copied()
                .chain(paused.iter().copied())
                .collect::<Vec<_>>();
            alone.sort_unstable();
            assert_eq!(
                run_node_access_with_params(
                    &db,
                    exec::ExecNodeAccessPlan::DynamicEquality {
                        index: index("deleted_at"),
                        key: key("deleted_at"),
                        param: param.clone(),
                    },
                    null_param(),
                )
                .await,
                ExecutionValue::Scalars(alone.into_iter().map(ExecutionScalar::NodeId).collect())
            );
            let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
            assert_eq!(metrics.scans, 1);
            assert_eq!(metrics.graph_reads, 2 + paused.len() as u64);
        })
        .await;
}

mod nested;
