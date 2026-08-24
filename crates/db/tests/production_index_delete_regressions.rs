//! Production-linked regressions for deleting graph entities omitted from active indexes.
//!
//! Every fixture creates graph data before its dynamic index, waits for retroactive
//! activation, and then deletes a label-matching entity that has no indexed property.

use std::{num::NonZeroUsize, time::Duration};

use db::{HelixDB, HelixDbSource};
use helix_ast::batch;
use helix_ast::expr::Predicate;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index::{self, IndexSpec, RangeIndexDirection};
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use helix_ast::value::{PropertyInput, PropertyValue};
use serde_json::Value;

const NODE_LABEL: &str = "Resource";
const EDGE_LABEL: &str = "RELATED";
const INDEX_PROPERTY: &str = "name_embedding";
const TENANT_PROPERTY: &str = "tenant";
const TARGET_TENANT: &str = "target-tenant";
const CONTROL_TENANT: &str = "control-tenant";
const BULK_DELETE_TARGETS: usize = 513;
const BULK_DELETE_PROPERTY: &str = "delete_group";
const BULK_DELETE_VALUE: &str = "bulk";
const BULK_SEED_BATCH: usize = 64;

/// Every valid public index shape with a behaviorally distinct delete path.
#[derive(Debug, Clone, Copy)]
enum DeleteCase {
    NodeEquality { unique: bool },
    EdgeEquality,
    NodeRange { direction: RangeIndexDirection },
    EdgeRange { direction: RangeIndexDirection },
    NodeVector { tenant_partitioned: bool },
    EdgeVector { tenant_partitioned: bool },
    NodeText { tenant_partitioned: bool },
    EdgeText { tenant_partitioned: bool },
}

impl DeleteCase {
    fn name(self) -> &'static str {
        match self {
            Self::NodeEquality { unique: false } => "node-equality",
            Self::NodeEquality { unique: true } => "node-unique-equality",
            Self::EdgeEquality => "edge-equality",
            Self::NodeRange {
                direction: RangeIndexDirection::Asc,
            } => "node-range-ascending",
            Self::NodeRange {
                direction: RangeIndexDirection::Desc,
            } => "node-range-descending",
            Self::EdgeRange {
                direction: RangeIndexDirection::Asc,
            } => "edge-range-ascending",
            Self::EdgeRange {
                direction: RangeIndexDirection::Desc,
            } => "edge-range-descending",
            Self::NodeVector {
                tenant_partitioned: false,
            } => "node-vector-unscoped",
            Self::NodeVector {
                tenant_partitioned: true,
            } => "node-vector-tenant",
            Self::EdgeVector {
                tenant_partitioned: false,
            } => "edge-vector-unscoped",
            Self::EdgeVector {
                tenant_partitioned: true,
            } => "edge-vector-tenant",
            Self::NodeText {
                tenant_partitioned: false,
            } => "node-text-unscoped",
            Self::NodeText {
                tenant_partitioned: true,
            } => "node-text-tenant",
            Self::EdgeText {
                tenant_partitioned: false,
            } => "edge-text-unscoped",
            Self::EdgeText {
                tenant_partitioned: true,
            } => "edge-text-tenant",
        }
    }

    const fn is_node(self) -> bool {
        matches!(
            self,
            Self::NodeEquality { .. }
                | Self::NodeRange { .. }
                | Self::NodeVector { .. }
                | Self::NodeText { .. }
        )
    }

    const fn tenant_partitioned(self) -> bool {
        matches!(
            self,
            Self::NodeVector {
                tenant_partitioned: true
            } | Self::EdgeVector {
                tenant_partitioned: true
            } | Self::NodeText {
                tenant_partitioned: true
            } | Self::EdgeText {
                tenant_partitioned: true
            }
        )
    }

    const fn vector_dimension(self) -> Option<usize> {
        match self {
            Self::NodeVector {
                tenant_partitioned: true,
            } => Some(512),
            Self::NodeVector { .. } | Self::EdgeVector { .. } => Some(2),
            _ => None,
        }
    }

    fn index_spec(self) -> IndexSpec {
        match self {
            Self::NodeEquality { unique: false } => {
                IndexSpec::node_equality(NODE_LABEL, INDEX_PROPERTY)
            }
            Self::NodeEquality { unique: true } => {
                IndexSpec::node_unique_equality(NODE_LABEL, INDEX_PROPERTY)
            }
            Self::EdgeEquality => IndexSpec::edge_equality(EDGE_LABEL, INDEX_PROPERTY),
            Self::NodeRange { direction } => {
                IndexSpec::node_range_with_direction(NODE_LABEL, INDEX_PROPERTY, direction)
            }
            Self::EdgeRange { direction } => {
                IndexSpec::edge_range_with_direction(EDGE_LABEL, INDEX_PROPERTY, direction)
            }
            Self::NodeVector { tenant_partitioned } => IndexSpec::node_vector(
                NODE_LABEL,
                INDEX_PROPERTY,
                NonZeroUsize::new(
                    self.vector_dimension()
                        .expect("node vector case has a dimension"),
                )
                .expect("fixture vector dimension is nonzero"),
                index::VectorDistanceMetric::Cosine,
                tenant_partitioned.then_some(TENANT_PROPERTY),
            ),
            Self::EdgeVector { tenant_partitioned } => IndexSpec::edge_vector(
                EDGE_LABEL,
                INDEX_PROPERTY,
                NonZeroUsize::new(
                    self.vector_dimension()
                        .expect("edge vector case has a dimension"),
                )
                .expect("fixture vector dimension is nonzero"),
                index::VectorDistanceMetric::Cosine,
                tenant_partitioned.then_some(TENANT_PROPERTY),
            ),
            Self::NodeText { tenant_partitioned } => IndexSpec::node_text(
                NODE_LABEL,
                INDEX_PROPERTY,
                tenant_partitioned.then_some(TENANT_PROPERTY),
            ),
            Self::EdgeText { tenant_partitioned } => IndexSpec::edge_text(
                EDGE_LABEL,
                INDEX_PROPERTY,
                tenant_partitioned.then_some(TENANT_PROPERTY),
            ),
        }
    }

    fn control_properties(self) -> Vec<(&'static str, PropertyInput)> {
        let indexed_value = match self {
            Self::NodeEquality { .. } | Self::EdgeEquality => PropertyInput::from("control"),
            Self::NodeRange { .. } | Self::EdgeRange { .. } => PropertyInput::from(7_i64),
            Self::NodeVector { .. } | Self::EdgeVector { .. } => {
                let dimension = self
                    .vector_dimension()
                    .expect("vector case has a dimension");
                let mut vector = vec![0.0_f32; dimension];
                vector[0] = 1.0;
                PropertyInput::from(vector)
            }
            Self::NodeText { .. } | Self::EdgeText { .. } => {
                PropertyInput::from("control searchable token")
            }
        };
        let mut properties = vec![("name", PropertyInput::from("control"))];
        properties.push((INDEX_PROPERTY, indexed_value));
        if self.tenant_partitioned() {
            properties.push((TENANT_PROPERTY, PropertyInput::from(CONTROL_TENANT)));
        }
        properties
    }

    fn target_properties(self) -> Vec<(&'static str, PropertyInput)> {
        let mut properties = vec![("name", PropertyInput::from("target"))];
        if self.tenant_partitioned() {
            properties.push((TENANT_PROPERTY, PropertyInput::from(TARGET_TENANT)));
        }
        properties
    }
}

#[derive(Debug, Clone, Copy)]
struct SeededEntities {
    target_id: u64,
    control_id: u64,
}

fn first_id(response: &Value, binding: &str) -> u64 {
    response[binding][0]
        .as_u64()
        .unwrap_or_else(|| panic!("{binding} returns one numeric ID: {response}"))
}

async fn seed_case(db: &HelixDB, case: DeleteCase) -> SeededEntities {
    let response = if case.is_node() {
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "target",
                    traversal::g()
                        .add_n(NODE_LABEL, case.target_properties())
                        .id(),
                )
                .var_as(
                    "control",
                    traversal::g()
                        .add_n(NODE_LABEL, case.control_properties())
                        .id(),
                )
                .returning(["target", "control"]),
        ))
        .await
    } else {
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "target_from",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "target_to",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "control_from",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "control_to",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "target",
                    traversal::g()
                        .n(NodeRef::var("target_from"))
                        .add_e(
                            EDGE_LABEL,
                            NodeRef::var("target_to"),
                            case.target_properties(),
                        )
                        .id(),
                )
                .var_as(
                    "control",
                    traversal::g()
                        .n(NodeRef::var("control_from"))
                        .add_e(
                            EDGE_LABEL,
                            NodeRef::var("control_to"),
                            case.control_properties(),
                        )
                        .id(),
                )
                .returning(["target", "control"]),
        ))
        .await
    }
    .unwrap_or_else(|error| panic!("{} fixture seeds: {error}", case.name()));

    SeededEntities {
        target_id: first_id(&response, "target"),
        control_id: first_id(&response, "control"),
    }
}

async fn await_index_operation_success(db: &HelixDB, operation_id: &str, description: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = db
                .query(QueryRequest::read(
                    batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(operation_id))
                        .returning(["status"]),
                ))
                .await
                .unwrap_or_else(|error| panic!("{description} status reads: {error}"));
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::task::yield_now().await,
                state => panic!("{description} operation reached unexpected state {state:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{description} activates within thirty seconds"));
}

async fn create_index(db: &HelixDB, case: DeleteCase) {
    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(case.index_spec()),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap_or_else(|error| panic!("{} index creation succeeds: {error}", case.name()));
    assert_eq!(receipt["operation"]["kind"], "accepted", "{}", case.name());
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .unwrap_or_else(|| panic!("{} accepted operation has an ID", case.name()));
    await_index_operation_success(db, operation_id, case.name()).await;
}

async fn control_ids(db: &HelixDB, case: DeleteCase) -> Vec<u64> {
    let request = match case {
        DeleteCase::NodeEquality { .. } => QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .n_with_label_where(NODE_LABEL, Predicate::eq(INDEX_PROPERTY, "control"))
                        .id(),
                )
                .returning(["ids"]),
        ),
        DeleteCase::EdgeEquality => QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .e_with_label_where(EDGE_LABEL, Predicate::eq(INDEX_PROPERTY, "control"))
                        .id(),
                )
                .returning(["ids"]),
        ),
        DeleteCase::NodeRange { .. } => QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .n_with_label_where(NODE_LABEL, Predicate::gte(INDEX_PROPERTY, 7_i64))
                        .id(),
                )
                .returning(["ids"]),
        ),
        DeleteCase::EdgeRange { .. } => QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .e_with_label_where(EDGE_LABEL, Predicate::gte(INDEX_PROPERTY, 7_i64))
                        .id(),
                )
                .returning(["ids"]),
        ),
        DeleteCase::NodeVector { .. } => {
            let mut vector = vec![0.0_f32; case.vector_dimension().unwrap()];
            vector[0] = 1.0;
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "ids",
                        traversal::g()
                            .vector_search_nodes_with(
                                NODE_LABEL,
                                INDEX_PROPERTY,
                                PropertyInput::from(vector),
                                8_usize,
                                case.tenant_partitioned()
                                    .then(|| PropertyInput::from(CONTROL_TENANT)),
                            )
                            .id(),
                    )
                    .returning(["ids"]),
            )
        }
        DeleteCase::EdgeVector { .. } => {
            let mut vector = vec![0.0_f32; case.vector_dimension().unwrap()];
            vector[0] = 1.0;
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "ids",
                        traversal::g()
                            .vector_search_edges_with(
                                EDGE_LABEL,
                                INDEX_PROPERTY,
                                PropertyInput::from(vector),
                                8_usize,
                                case.tenant_partitioned()
                                    .then(|| PropertyInput::from(CONTROL_TENANT)),
                            )
                            .id(),
                    )
                    .returning(["ids"]),
            )
        }
        DeleteCase::NodeText { .. } => QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_nodes(
                            NODE_LABEL,
                            INDEX_PROPERTY,
                            "searchable",
                            8,
                            case.tenant_partitioned()
                                .then(|| PropertyValue::from(CONTROL_TENANT)),
                        )
                        .id(),
                )
                .returning(["ids"]),
        ),
        DeleteCase::EdgeText { .. } => QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_edges(
                            EDGE_LABEL,
                            INDEX_PROPERTY,
                            "searchable",
                            8,
                            case.tenant_partitioned()
                                .then(|| PropertyValue::from(CONTROL_TENANT)),
                        )
                        .id(),
                )
                .returning(["ids"]),
        ),
    };
    let response = db
        .query(request)
        .await
        .unwrap_or_else(|error| panic!("{} control query succeeds: {error}", case.name()));
    response["ids"]
        .as_array()
        .unwrap_or_else(|| panic!("{} returns an ID array: {response}", case.name()))
        .iter()
        .map(|value| {
            value
                .as_u64()
                .unwrap_or_else(|| panic!("{} returns numeric IDs: {response}", case.name()))
        })
        .collect()
}

async fn entity_ids(db: &HelixDB, case: DeleteCase, id: u64) -> Vec<u64> {
    let request = if case.is_node() {
        QueryRequest::read(
            batch::read_batch()
                .var_as("ids", traversal::g().n(NodeRef::id(id)).id())
                .returning(["ids"]),
        )
    } else {
        QueryRequest::read(
            batch::read_batch()
                .var_as("ids", traversal::g().e(EdgeRef::id(id)).id())
                .returning(["ids"]),
        )
    };
    let response = db.query(request).await.unwrap();
    let Some(ids) = response["ids"].as_array() else {
        return Vec::new();
    };
    ids.iter()
        .map(|value| value.as_u64().expect("point lookup returns numeric IDs"))
        .collect()
}

async fn seed_unindexed_text_nodes(db: &HelixDB) {
    for batch_start in (0..BULK_DELETE_TARGETS).step_by(BULK_SEED_BATCH) {
        let mut seed = batch::write_batch();
        for target in batch_start..(batch_start + BULK_SEED_BATCH).min(BULK_DELETE_TARGETS) {
            seed = seed.var_as(
                &format!("target-{target}"),
                traversal::g().add_n(
                    NODE_LABEL,
                    vec![(BULK_DELETE_PROPERTY, PropertyInput::from(BULK_DELETE_VALUE))],
                ),
            );
        }
        db.query(QueryRequest::write(seed.returning(Vec::<String>::new())))
            .await
            .expect("unindexed text nodes seed before index creation");
    }
}

async fn seed_unindexed_text_edges(db: &HelixDB, from: u64, to: u64) {
    for batch_start in (0..BULK_DELETE_TARGETS).step_by(BULK_SEED_BATCH) {
        let mut seed = batch::write_batch();
        for target in batch_start..(batch_start + BULK_SEED_BATCH).min(BULK_DELETE_TARGETS) {
            seed = seed.var_as(
                &format!("target-{target}"),
                traversal::g().n(NodeRef::id(from)).add_e(
                    EDGE_LABEL,
                    NodeRef::id(to),
                    vec![
                        (BULK_DELETE_PROPERTY, PropertyInput::from(BULK_DELETE_VALUE)),
                        (TENANT_PROPERTY, PropertyInput::from(TARGET_TENANT)),
                    ],
                ),
            );
        }
        db.query(QueryRequest::write(seed.returning(Vec::<String>::new())))
            .await
            .expect("unindexed text edges seed before index creation");
    }
}

async fn bulk_delete_target_count(db: &HelixDB, nodes: bool) -> u64 {
    let count = if nodes {
        traversal::g()
            .n_with_label_where(
                NODE_LABEL,
                Predicate::eq(BULK_DELETE_PROPERTY, BULK_DELETE_VALUE),
            )
            .count()
    } else {
        traversal::g()
            .e_with_label_where(
                EDGE_LABEL,
                Predicate::eq(BULK_DELETE_PROPERTY, BULK_DELETE_VALUE),
            )
            .count()
    };
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("count", count)
                .returning(["count"]),
        ))
        .await
        .expect("bulk-delete target count reads");
    response["count"]
        .as_u64()
        .expect("bulk-delete target count is numeric")
}

async fn run_direct_delete_case(case: DeleteCase) {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: format!("production-index-delete-{}", case.name()),
    })
    .await
    .unwrap_or_else(|error| panic!("{} database opens: {error}", case.name()));
    let seeded = seed_case(&db, case).await;
    create_index(&db, case).await;
    assert_eq!(control_ids(&db, case).await, vec![seeded.control_id]);

    let delete = if case.is_node() {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "deleted",
                    traversal::g().n(NodeRef::id(seeded.target_id)).drop(),
                )
                .returning(Vec::<String>::new()),
        )
    } else {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "deleted",
                    traversal::g().drop_edge_by_id(EdgeRef::id(seeded.target_id)),
                )
                .returning(Vec::<String>::new()),
        )
    };
    db.query(delete).await.unwrap_or_else(|error| {
        panic!("{} missing-property delete succeeds: {error}", case.name())
    });

    assert!(entity_ids(&db, case, seeded.target_id).await.is_empty());
    assert_eq!(control_ids(&db, case).await, vec![seeded.control_id]);
    db.close()
        .await
        .unwrap_or_else(|error| panic!("{} database closes: {error}", case.name()));
}

#[tokio::test]
async fn deletes_node_omitted_from_non_unique_equality_index() {
    run_direct_delete_case(DeleteCase::NodeEquality { unique: false }).await;
}

#[tokio::test]
async fn deletes_node_omitted_from_unique_equality_index() {
    run_direct_delete_case(DeleteCase::NodeEquality { unique: true }).await;
}

#[tokio::test]
async fn deletes_edge_omitted_from_equality_index() {
    run_direct_delete_case(DeleteCase::EdgeEquality).await;
}

#[tokio::test]
async fn deletes_node_omitted_from_ascending_range_index() {
    run_direct_delete_case(DeleteCase::NodeRange {
        direction: RangeIndexDirection::Asc,
    })
    .await;
}

#[tokio::test]
async fn deletes_node_omitted_from_descending_range_index() {
    run_direct_delete_case(DeleteCase::NodeRange {
        direction: RangeIndexDirection::Desc,
    })
    .await;
}

#[tokio::test]
async fn deletes_edge_omitted_from_ascending_range_index() {
    run_direct_delete_case(DeleteCase::EdgeRange {
        direction: RangeIndexDirection::Asc,
    })
    .await;
}

#[tokio::test]
async fn deletes_edge_omitted_from_descending_range_index() {
    run_direct_delete_case(DeleteCase::EdgeRange {
        direction: RangeIndexDirection::Desc,
    })
    .await;
}

#[tokio::test]
async fn deletes_node_omitted_from_unscoped_vector_index() {
    run_direct_delete_case(DeleteCase::NodeVector {
        tenant_partitioned: false,
    })
    .await;
}

#[tokio::test]
async fn deletes_node_without_mapping_from_tenant_vector_index() {
    run_direct_delete_case(DeleteCase::NodeVector {
        tenant_partitioned: true,
    })
    .await;
}

#[tokio::test]
async fn deletes_edge_omitted_from_unscoped_vector_index() {
    run_direct_delete_case(DeleteCase::EdgeVector {
        tenant_partitioned: false,
    })
    .await;
}

#[tokio::test]
async fn deletes_edge_without_mapping_from_tenant_vector_index() {
    run_direct_delete_case(DeleteCase::EdgeVector {
        tenant_partitioned: true,
    })
    .await;
}

#[tokio::test]
async fn deletes_node_omitted_from_unscoped_text_index() {
    run_direct_delete_case(DeleteCase::NodeText {
        tenant_partitioned: false,
    })
    .await;
}

#[tokio::test]
async fn deletes_node_omitted_from_tenant_text_index() {
    run_direct_delete_case(DeleteCase::NodeText {
        tenant_partitioned: true,
    })
    .await;
}

#[tokio::test]
async fn deletes_edge_omitted_from_unscoped_text_index() {
    run_direct_delete_case(DeleteCase::EdgeText {
        tenant_partitioned: false,
    })
    .await;
}

#[tokio::test]
async fn deletes_edge_omitted_from_tenant_text_index() {
    run_direct_delete_case(DeleteCase::EdgeText {
        tenant_partitioned: true,
    })
    .await;
}

#[tokio::test]
async fn active_node_text_index_ignores_unindexed_bulk_delete_limit() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-index-delete-text-node-bulk".to_owned(),
    })
    .await
    .expect("bulk node database opens");
    let text_case = DeleteCase::NodeText {
        tenant_partitioned: false,
    };
    seed_unindexed_text_nodes(&db).await;
    let control = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "control",
                    traversal::g()
                        .add_n(NODE_LABEL, text_case.control_properties())
                        .id(),
                )
                .returning(["control"]),
        ))
        .await
        .expect("indexed text node control seeds");
    let control_id = first_id(&control, "control");
    create_index(&db, text_case).await;
    assert_eq!(control_ids(&db, text_case).await, vec![control_id]);
    assert_eq!(
        bulk_delete_target_count(&db, true).await,
        BULK_DELETE_TARGETS as u64
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "deleted",
                traversal::g()
                    .n_with_label_where(
                        NODE_LABEL,
                        Predicate::eq(BULK_DELETE_PROPERTY, BULK_DELETE_VALUE),
                    )
                    .drop(),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("active text index ignores unindexed nodes at the mutation limit");

    assert_eq!(bulk_delete_target_count(&db, true).await, 0);
    assert_eq!(control_ids(&db, text_case).await, vec![control_id]);
    db.close().await.expect("bulk node database closes");
}

#[tokio::test]
async fn active_tenant_edge_text_index_ignores_unindexed_bulk_delete_limit() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-index-delete-text-edge-bulk".to_owned(),
    })
    .await
    .expect("bulk edge database opens");
    let text_case = DeleteCase::EdgeText {
        tenant_partitioned: true,
    };
    let endpoints = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "from",
                    traversal::g()
                        .add_n("Endpoint", Vec::<(&str, PropertyInput)>::new())
                        .id(),
                )
                .var_as(
                    "to",
                    traversal::g()
                        .add_n("Endpoint", Vec::<(&str, PropertyInput)>::new())
                        .id(),
                )
                .returning(["from", "to"]),
        ))
        .await
        .expect("bulk edge endpoints seed");
    let from = first_id(&endpoints, "from");
    let to = first_id(&endpoints, "to");
    seed_unindexed_text_edges(&db, from, to).await;
    let control = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "control",
                    traversal::g()
                        .n(NodeRef::id(from))
                        .add_e(EDGE_LABEL, NodeRef::id(to), text_case.control_properties())
                        .id(),
                )
                .returning(["control"]),
        ))
        .await
        .expect("indexed text edge control seeds");
    let control_id = first_id(&control, "control");
    create_index(&db, text_case).await;
    assert_eq!(control_ids(&db, text_case).await, vec![control_id]);
    assert_eq!(
        bulk_delete_target_count(&db, false).await,
        BULK_DELETE_TARGETS as u64
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "targets",
                traversal::g().e_with_label_where(
                    EDGE_LABEL,
                    Predicate::eq(BULK_DELETE_PROPERTY, BULK_DELETE_VALUE),
                ),
            )
            .var_as(
                "deleted",
                traversal::g().drop_edge_by_id(EdgeRef::var("targets")),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("active text index ignores unindexed edges at the mutation limit");

    assert_eq!(bulk_delete_target_count(&db, false).await, 0);
    assert_eq!(control_ids(&db, text_case).await, vec![control_id]);
    db.close().await.expect("bulk edge database closes");
}

#[tokio::test]
async fn node_delete_cascades_to_unmapped_tenant_vector_edge() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-index-delete-vector-edge-cascade".to_owned(),
    })
    .await
    .expect("cascade database opens");
    let vector_case = DeleteCase::EdgeVector {
        tenant_partitioned: true,
    };
    let response = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "center",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "target_to",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "control_from",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "control_to",
                    traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as(
                    "target_edge",
                    traversal::g()
                        .n(NodeRef::var("center"))
                        .add_e(
                            EDGE_LABEL,
                            NodeRef::var("target_to"),
                            vector_case.target_properties(),
                        )
                        .id(),
                )
                .var_as(
                    "control_edge",
                    traversal::g()
                        .n(NodeRef::var("control_from"))
                        .add_e(
                            EDGE_LABEL,
                            NodeRef::var("control_to"),
                            vector_case.control_properties(),
                        )
                        .id(),
                )
                .var_as("center_id", traversal::g().n(NodeRef::var("center")).id())
                .var_as(
                    "target_to_id",
                    traversal::g().n(NodeRef::var("target_to")).id(),
                )
                .returning(["center_id", "target_to_id", "target_edge", "control_edge"]),
        ))
        .await
        .expect("cascade fixture seeds");
    let center_id = first_id(&response, "center_id");
    let target_to_id = first_id(&response, "target_to_id");
    let target_edge_id = first_id(&response, "target_edge");
    let control_edge_id = first_id(&response, "control_edge");

    create_index(&db, vector_case).await;
    assert_eq!(control_ids(&db, vector_case).await, vec![control_edge_id]);
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("deleted", traversal::g().n(NodeRef::id(center_id)).drop())
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("node delete cascades through the missing-property incident edge");

    assert!(
        entity_ids(&db, DeleteCase::NodeEquality { unique: false }, center_id)
            .await
            .is_empty()
    );
    assert_eq!(
        entity_ids(
            &db,
            DeleteCase::NodeEquality { unique: false },
            target_to_id,
        )
        .await,
        vec![target_to_id]
    );
    assert!(entity_ids(&db, vector_case, target_edge_id)
        .await
        .is_empty());
    assert_eq!(control_ids(&db, vector_case).await, vec![control_edge_id]);
    db.close().await.expect("cascade database closes");
}
