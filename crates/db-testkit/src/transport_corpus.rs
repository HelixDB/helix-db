//! Shared public-query corpus for embedded, service, and transport adapters.

use helix_ast::batch;
use helix_ast::expr::{Expr, Predicate, StreamBound};
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::traversal;
use helix_ast::value::{PropertyInput, PropertyValue};

use crate::fixtures::QueryCorpusAdapter;
use crate::Result;

/// One named request whose response is compared across every adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct TransportCorpusStep {
    name: &'static str,
    request: QueryRequest,
}

impl TransportCorpusStep {
    /// Returns the stable diagnostic name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Clones the public query request for one isolated adapter run.
    pub fn request(&self) -> QueryRequest {
        self.request.clone()
    }
}

/// Returns the deterministic graph mutation/read corpus shared by all adapters.
pub fn transport_query_corpus() -> Vec<TransportCorpusStep> {
    vec![
        TransportCorpusStep {
            name: "insert",
            request: QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g().add_n(
                            "Document",
                            vec![
                                ("name", PropertyInput::from("alice")),
                                ("rank", PropertyInput::from(1_i64)),
                            ],
                        ),
                    )
                    .var_as("created_id", traversal::g().n(NodeRef::var("created")).id())
                    .returning(["created", "created_id"]),
            ),
        },
        TransportCorpusStep {
            name: "point_projection_aggregate",
            request: QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "point",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .values(vec!["name", "rank"]),
                    )
                    .var_as(
                        "projection",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .value_map(Some(vec!["name", "rank"])),
                    )
                    .var_as("visible_path", traversal::g().n(NodeRef::id(0)).path())
                    .var_as(
                        "visible_sack",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .with_sack(PropertyValue::from(7_i64))
                            .sack_get(),
                    )
                    .var_as("count", traversal::g().n(NodeRef::all()).count())
                    .returning([
                        "point",
                        "projection",
                        "visible_path",
                        "visible_sack",
                        "count",
                    ]),
            ),
        },
        TransportCorpusStep {
            name: "update",
            request: QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "updated",
                        traversal::g().n(NodeRef::id(0)).set_property("rank", 2_i64),
                    )
                    .returning(Vec::<String>::new()),
            ),
        },
        TransportCorpusStep {
            name: "range_after_update",
            request: QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "range",
                        traversal::g()
                            .n(NodeRef::all())
                            .range(0_usize, 8_usize)
                            .values(vec!["rank"]),
                    )
                    .var_as("count", traversal::g().n(NodeRef::all()).count())
                    .returning(["range", "count"]),
            ),
        },
        TransportCorpusStep {
            name: "insert_cleanup_candidates",
            request: QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "first_candidate",
                        traversal::g().add_n(
                            "Resource",
                            vec![
                                ("tenant", PropertyInput::from("tenant-a")),
                                ("type", PropertyInput::from("job")),
                            ],
                        ),
                    )
                    .var_as(
                        "second_candidate",
                        traversal::g().add_n(
                            "Resource",
                            vec![
                                ("tenant", PropertyInput::from("tenant-a")),
                                ("type", PropertyInput::from("job")),
                            ],
                        ),
                    )
                    .var_as(
                        "third_candidate",
                        traversal::g().add_n(
                            "Resource",
                            vec![
                                ("tenant", PropertyInput::from("tenant-a")),
                                ("type", PropertyInput::from("job")),
                            ],
                        ),
                    )
                    .var_as(
                        "other_tenant",
                        traversal::g().add_n(
                            "Resource",
                            vec![
                                ("tenant", PropertyInput::from("tenant-b")),
                                ("type", PropertyInput::from("job")),
                            ],
                        ),
                    )
                    .returning(Vec::<String>::new()),
            ),
        },
        TransportCorpusStep {
            name: "direct_n_where_limit_drop",
            request: QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "dropped",
                        traversal::g()
                            .n_where(Predicate::and(vec![
                                Predicate::eq("$label", "Resource"),
                                Predicate::eq_param("tenant", "tenant"),
                                Predicate::eq_param("type", "type"),
                            ]))
                            .limit(StreamBound::expr(Expr::param("limit")))
                            .drop(),
                    )
                    .returning(["dropped"]),
            )
            .with_parameter_value("tenant", QueryValue::String("tenant-a".to_owned()))
            .with_parameter_value("type", QueryValue::String("job".to_owned()))
            .with_parameter_value("limit", QueryValue::I64(2)),
        },
        TransportCorpusStep {
            name: "verify_direct_n_where_limit_drop",
            request: QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "candidate_count",
                        traversal::g()
                            .n_where(Predicate::and(vec![
                                Predicate::eq("$label", "Resource"),
                                Predicate::eq("tenant", "tenant-a"),
                                Predicate::eq("type", "job"),
                            ]))
                            .count(),
                    )
                    .var_as(
                        "other_tenant_count",
                        traversal::g()
                            .n_where(Predicate::and(vec![
                                Predicate::eq("$label", "Resource"),
                                Predicate::eq("tenant", "tenant-b"),
                                Predicate::eq("type", "job"),
                            ]))
                            .count(),
                    )
                    .var_as("total_count", traversal::g().n(NodeRef::all()).count())
                    .returning(["candidate_count", "other_tenant_count", "total_count"]),
            ),
        },
    ]
}

/// Executes the shared corpus in order and returns each JSON observation.
pub async fn execute_transport_corpus(
    adapter: &mut impl QueryCorpusAdapter,
) -> Result<Vec<serde_json::Value>> {
    let mut observations = Vec::new();
    for step in transport_query_corpus() {
        observations.push(adapter.execute_query(step.request()).await?);
    }
    Ok(observations)
}

/// Independent expected results for [`transport_query_corpus`].
pub fn expected_transport_observations() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "created": [{ "$id": 0 }],
            "created_id": [0],
        }),
        serde_json::json!({
            "count": 1,
            "point": [{ "name": "alice", "rank": 1 }],
            "projection": [{ "name": "alice", "rank": 1 }],
            "visible_path": [{
                "current": { "node": 0 },
                "bindings": {},
                "path": [{ "node": 0 }],
            }],
            "visible_sack": [{
                "current": { "node": 0 },
                "bindings": {},
                "sack": 7,
            }],
        }),
        serde_json::json!({}),
        serde_json::json!({
            "count": 1,
            "range": [{ "rank": 2 }],
        }),
        serde_json::json!({}),
        serde_json::json!({ "dropped": [] }),
        serde_json::json!({
            "candidate_count": 1,
            "other_tenant_count": 1,
            "total_count": 3,
        }),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;
    use helix_ast::query::QueryRequestType;

    use super::*;
    use crate::TestkitError;

    struct RecordingAdapter {
        responses: VecDeque<serde_json::Value>,
        request_types: Vec<helix_ast::query::QueryRequestType>,
        closed: bool,
    }

    #[async_trait]
    impl QueryCorpusAdapter for RecordingAdapter {
        async fn execute_query(&mut self, request: QueryRequest) -> Result<serde_json::Value> {
            self.request_types.push(request.request_type());
            self.responses
                .pop_front()
                .ok_or_else(|| TestkitError::Adapter("missing expected response".to_string()))
        }

        async fn close(&mut self) -> Result<()> {
            self.closed = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn corpus_names_request_modes_and_observations_are_frozen() {
        let steps = transport_query_corpus();
        assert_eq!(
            steps
                .iter()
                .map(TransportCorpusStep::name)
                .collect::<Vec<_>>(),
            [
                "insert",
                "point_projection_aggregate",
                "update",
                "range_after_update",
                "insert_cleanup_candidates",
                "direct_n_where_limit_drop",
                "verify_direct_n_where_limit_drop",
            ]
        );

        let expected = expected_transport_observations();
        let mut adapter = RecordingAdapter {
            responses: expected.clone().into(),
            request_types: Vec::new(),
            closed: false,
        };
        assert_eq!(
            execute_transport_corpus(&mut adapter).await.unwrap(),
            expected
        );
        assert_eq!(
            adapter.request_types,
            [
                QueryRequestType::Write,
                QueryRequestType::Read,
                QueryRequestType::Write,
                QueryRequestType::Read,
                QueryRequestType::Write,
                QueryRequestType::Write,
                QueryRequestType::Read,
            ]
        );
        adapter.close().await.unwrap();
        assert!(adapter.closed);
    }
}
