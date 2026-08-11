from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PYTHON_ROOT = Path(__file__).resolve().parents[1]
SDKS_ROOT = PYTHON_ROOT.parent
sys.path.insert(0, str(PYTHON_ROOT / "src"))

from parity_runtime_fixtures import (  # noqa: E402
    base_runtime_fixtures,
    node_permutation_fixtures,
)

from helixdb import (  # noqa: E402
    AggregateFunction,
    BatchCondition,
    BindingProjection,
    CompareOp,
    EdgeRef,
    Expr,
    IndexSpec,
    NodeRef,
    Order,
    Predicate,
    Projection,
    PropertyInput,
    PropertyValue,
    QueryParamType,
    QueryValue,
    RepeatConfig,
    ShortestPathDirection,
    Step,
    StreamBound,
    Traversal,
    VectorDistanceMetric,
    WhenThen,
    g,
    read_batch,
    sub,
    write_batch,
)
from helixdb import (
    QueryRequest as QueryRequest,
)

OUTPUT_ROOT = SDKS_ROOT / "tests" / "parity" / "generated" / "python"


@dataclass(frozen=True)
class Fixture:
    name: str
    request: QueryRequest


def json_only(name: str, request: QueryRequest) -> Fixture:
    return Fixture(name, request)


def with_params(
    request: QueryRequest,
    values: list[tuple[str, Any]],
    types: list[tuple[str, QueryParamType]],
) -> QueryRequest:
    if len(values) != len(types):
        raise TypeError("fixture schema/value mismatch")
    for (value_name, value), (type_name, param_type) in zip(values, types):
        if value_name != type_name:
            raise TypeError("fixture parameter name mismatch")
        request.insert_typed_parameter(value_name, param_type, value)
    return request


def nested_metadata(external_id: str, score: int) -> PropertyValue:
    return PropertyValue.from_value(
        {"externalID": external_id, "score": score, "tags": ["alpha", 7]}
    )


def raw_read_steps() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "raw_nodes",
            Traversal.from_steps(
                [
                    Step.n(NodeRef.param("node_ids")),
                    Step.has("name", "Alice"),
                    Step.where(Predicate.contains_param("bio", "needle")),
                    Step.limit(StreamBound.expr(Expr.param("limit"))),
                    Step.skip(StreamBound.expr(Expr.param("skip"))),
                    Step.range(
                        StreamBound.literal(0),
                        StreamBound.expr(Expr.param("end")),
                    ),
                    Step.as_("a"),
                    Step.store("stored"),
                    Step.select("stored"),
                    Step.dedup(),
                    Step.within("stored"),
                    Step.without("missing"),
                    Step.fold(),
                    Step.unfold(),
                    Step.path(),
                    Step.simple_path(),
                    Step.with_sack(0),
                    Step.sack_set("score"),
                    Step.sack_add("score"),
                    Step.sack_get(),
                    Step.project(
                        [
                            Projection.property("externalId", "externalId"),
                            Projection.expr("neg_age", Expr.prop("age").neg()),
                        ]
                    ),
                ],
                "nodes",
                "read",
            ),
        )
        .var_as(
            "raw_edges",
            Traversal.from_steps(
                [
                    Step.e(EdgeRef.param("edge_ids")),
                    Step.where(
                        Predicate.or_(
                            [
                                Predicate.has_key("since"),
                                Predicate.starts_with("note", "Alice"),
                            ]
                        )
                    ),
                    Step.edge_has("weight", PropertyInput.value(PropertyValue.f64(1.0))),
                    Step.edge_has_label("FOLLOWS"),
                    Step.order_by("weight", Order.DESC),
                    Step.edge_properties(),
                ],
                "edges",
                "read",
            ),
        )
        .var_as(
            "index_operation",
            g().get_index_operation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"),
        )
        .returning(["raw_nodes", "raw_edges", "index_operation"])
    )
    return json_only(
        "900-exhaustive-raw-read-steps",
        with_params(
            request,
            [
                ("node_ids", [1, 2]),
                ("edge_ids", [1]),
                ("needle", "graph"),
                ("limit", 10),
                ("skip", 0),
                ("end", 10),
            ],
            [
                ("node_ids", QueryParamType.array(QueryParamType.i64())),
                ("edge_ids", QueryParamType.array(QueryParamType.i64())),
                ("needle", QueryParamType.string()),
                ("limit", QueryParamType.i64()),
                ("skip", QueryParamType.i64()),
                ("end", QueryParamType.i64()),
            ],
        ),
    )


def raw_write_steps() -> Fixture:
    request = QueryRequest.write(
        write_batch()
        .var_as(
            "raw_unique_index",
            g().create_index_if_not_exists(
                IndexSpec.node_unique_equality("ParityUser", "externalId")
            ),
        )
        .var_as(
            "raw_drop_range_index",
            g().drop_index(IndexSpec.node_range("ParityUser", "age")),
        )
        .var_as(
            "raw_node_vector_index",
            g().create_vector_index_nodes(
                "ParityUser",
                "embedding",
                3,
                VectorDistanceMetric.COSINE,
                "tenantId",
            ),
        )
        .var_as(
            "raw_edge_vector_index",
            g().create_vector_index_edges(
                "FOLLOWS",
                "embedding",
                2,
                VectorDistanceMetric.COSINE,
                "tenantId",
            ),
        )
        .var_as(
            "raw_node_text_index",
            g().create_text_index_nodes("ParityUser", "bio", "tenantId"),
        )
        .var_as(
            "raw_edge_text_index",
            g().create_text_index_edges("FOLLOWS", "note", "tenantId"),
        )
        .var_as(
            "raw_mutations",
            Traversal.from_steps(
                [
                    Step.add_n("RawNode", [("name", PropertyInput.value("raw"))]),
                    Step.add_e(
                        "RAW_EDGE",
                        NodeRef.var("raw_mutations"),
                        [("weight", PropertyInput.value(1))],
                    ),
                    Step.set_property("name", PropertyInput.expr(Expr.param("name"))),
                    Step.remove_property("old"),
                    Step.drop_edge(NodeRef.ids([999_999])),
                    Step.drop_edge_labeled(NodeRef.ids([999_999]), "RAW_EDGE"),
                    Step.drop_edge_by_id(EdgeRef.ids([999_999])),
                    Step.drop(),
                ],
                "nodes",
                "write",
            ),
        )
        .var_as(
            "retry_index_operation",
            g().retry_index_operation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"),
        )
        .var_as(
            "abort_index_operation",
            g().abort_index_operation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"),
        )
        .returning(
            [
                "raw_unique_index",
                "raw_drop_range_index",
                "raw_node_vector_index",
                "raw_edge_vector_index",
                "raw_node_text_index",
                "raw_edge_text_index",
                "raw_mutations",
                "retry_index_operation",
                "abort_index_operation",
            ]
        )
    )
    return json_only("901-exhaustive-raw-write-steps", request)


def query_value_shapes() -> Fixture:
    request = QueryRequest.read(
        read_batch().var_as("empty", g().n_with_label("Missing").count()).returning(["empty"])
    )
    return json_only(
        "902-query-value-and-param-type-shapes",
        with_params(
            request,
            [
                ("null", QueryValue.null()),
                ("bool", QueryValue.bool(True)),
                ("i64", QueryValue.i64(9_223_372_036_854_775_807)),
                ("f64", QueryValue.f64(1.25)),
                ("f32", QueryValue.f32(1.5)),
                ("string", QueryValue.string("value")),
                ("array", QueryValue.array([1, "two"])),
                ("object", QueryValue.object({"nested": True})),
            ],
            [
                ("null", QueryParamType.value()),
                ("bool", QueryParamType.bool()),
                ("i64", QueryParamType.i64()),
                ("f64", QueryParamType.f64()),
                ("f32", QueryParamType.f32()),
                ("string", QueryParamType.string()),
                ("array", QueryParamType.array(QueryParamType.value())),
                ("object", QueryParamType.object()),
            ],
        ),
    )


def runtime_input_shapes() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "vector_nodes",
            g().vector_search_nodes_with(
                "ParityUser",
                "embedding",
                PropertyInput.param("query_vector"),
                Expr.param("limit"),
                PropertyInput.param("tenant"),
            ),
        )
        .var_as(
            "text_nodes",
            g().text_search_nodes_with(
                "ParityUser",
                "bio",
                PropertyInput.param("query_text"),
                Expr.param("limit"),
                PropertyInput.param("tenant"),
            ),
        )
        .returning(["vector_nodes", "text_nodes"])
    )
    return json_only(
        "903-empty-source-vector-text-runtime-inputs",
        with_params(
            request,
            [
                ("query_vector", [1.0, 0.0, 0.0]),
                ("query_text", "graph"),
                ("limit", 5),
                ("tenant", "tenant-a"),
            ],
            [
                ("query_vector", QueryParamType.array(QueryParamType.f64())),
                ("query_text", QueryParamType.string()),
                ("limit", QueryParamType.i64()),
                ("tenant", QueryParamType.string()),
            ],
        ),
    )


def reference_shapes() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "all_nodes",
            Traversal.from_steps([Step.n(NodeRef.all()), Step.count()]),
        )
        .var_as(
            "node_ids",
            Traversal.from_steps([Step.n(NodeRef.ids([1, 2])), Step.id()]),
        )
        .var_as(
            "node_var",
            Traversal.from_steps([Step.n(NodeRef.var("all_nodes")), Step.label()]),
        )
        .var_as(
            "edge_ids",
            Traversal.from_steps([Step.e(EdgeRef.ids([1, 2])), Step.id()], "edges"),
        )
        .var_as(
            "edge_var",
            Traversal.from_steps([Step.e(EdgeRef.var("edge_ids")), Step.label()], "edges"),
        )
        .returning(["all_nodes", "node_ids", "node_var", "edge_ids", "edge_var"])
    )
    return json_only("904-empty-query-and-node-edge-ref-shapes", request)


def source_mutators() -> Fixture:
    request = QueryRequest.write(
        write_batch()
        .var_as("inject", Traversal.new().inject("some_var").count())
        .var_as("drop_edge_by_id", g().drop_edge_by_id(EdgeRef.id(123_456)).count())
        .returning(["inject", "drop_edge_by_id"])
    )
    return json_only("905-empty-traversal-source-mutators", request)


def nested_write_shapes() -> Fixture:
    request = QueryRequest.write(
        write_batch()
        .var_as(
            "created",
            g().add_n(
                "ParityNested",
                [
                    ("name", PropertyInput.value("nested")),
                    (
                        "metadata",
                        PropertyInput.value(nested_metadata("some_id", 20)),
                    ),
                ],
            ),
        )
        .var_as(
            "updated",
            g()
            .n(NodeRef.var("created"))
            .set_property("metadata", PropertyInput.param("metadata"))
            .value_map(["metadata.externalID"]),
        )
        .var_as(
            "target",
            g().add_n("ParityNestedTarget", [("name", PropertyInput.value("target"))]),
        )
        .var_as(
            "edge",
            g()
            .n(NodeRef.var("created"))
            .add_e(
                "NESTED_LINK",
                NodeRef.var("target"),
                [
                    (
                        "metadata",
                        PropertyInput.value(nested_metadata("edge_id", 5)),
                    )
                ],
            )
            .count(),
        )
        .returning(["created", "updated", "edge"])
    )
    return json_only(
        "906-nested-query-property-write-shapes",
        with_params(
            request,
            [
                (
                    "metadata",
                    {"externalID": "param_id", "score": 22, "tags": ["alpha", 7]},
                )
            ],
            [("metadata", QueryParamType.object())],
        ),
    )


def nested_read_shapes() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "nested_users",
            g()
            .n_where(
                Predicate.and_(
                    [
                        Predicate.eq("$label", "ParityNested"),
                        Predicate.eq("metadata.externalID", Expr.param("external_id")),
                    ]
                )
            )
            .where(Predicate.compare(Expr.prop("metadata.score"), CompareOp.GT, Expr.val(10)))
            .order_by_multiple([("metadata.score", Order.DESC), ("name", Order.ASC)])
            .project(
                [
                    Projection.property("metadata.externalID", "external_id"),
                    Projection.expr("score_copy", Expr.prop("metadata.score")),
                ]
            ),
        )
        .var_as(
            "nested_values",
            g().n_with_label("ParityNested").values(["metadata.externalID"]),
        )
        .var_as(
            "nested_map",
            g().n_with_label("ParityNested").value_map(["metadata.externalID", "metadata.score"]),
        )
        .var_as(
            "nested_edges",
            g()
            .e_where(
                Predicate.and_(
                    [
                        Predicate.eq("$label", "NESTED_LINK"),
                        Predicate.eq("metadata.externalID", "edge_id"),
                    ]
                )
            )
            .edge_has("metadata.externalID", PropertyInput.value("edge_id"))
            .edge_properties(),
        )
        .returning(["nested_users", "nested_values", "nested_map", "nested_edges"])
    )
    return json_only(
        "907-nested-query-property-read-shapes",
        with_params(
            request,
            [("external_id", "param_id")],
            [("external_id", QueryParamType.string())],
        ),
    )


def endpoint_projection() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "endpoints",
            g()
            .e_with_label("FOLLOWS")
            .project(
                [
                    Projection.from_endpoint("externalId", "from_id"),
                    Projection.to_endpoint("externalId", "to_id"),
                    Projection.property("$id", "edge_id"),
                ]
            ),
        )
        .returning(["endpoints"])
    )
    return json_only("908-edge-endpoint-projection", request)


def binding_projection() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "bindings",
            g()
            .n_with_label("ParityService")
            .bind("service")
            .project_bindings(
                [
                    BindingProjection.binding("service", "$id", "service_id"),
                    BindingProjection.current("metadata.name", "current_name"),
                    BindingProjection.binding(
                        "missing_binding", "externalId", "missing_external_id"
                    ),
                ]
            ),
        )
        .returning(["bindings"])
    )
    return json_only("909-row-binding-basic-projection", request)


def branch_binding_projection() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "workloads",
            g()
            .n_with_label("ParityService")
            .bind("service")
            .out("ROUTES_TO")
            .bind("pod")
            .optional(sub().in_("CREATES").bind("deployment"))
            .union(
                [
                    sub().in_("MANAGES").bind("owner"),
                    sub().out("ROUTES_TO").bind("workload"),
                ]
            )
            .project_distinct_bindings(
                [
                    BindingProjection.binding("service", "$id", "service_id"),
                    BindingProjection.coalesce(
                        [
                            BindingProjection.binding_ref("deployment", "$id"),
                            BindingProjection.binding_ref("owner", "$id"),
                            BindingProjection.binding_ref("workload", "$id"),
                        ],
                        "workload_id",
                    ),
                ]
            ),
        )
        .returning(["workloads"])
    )
    return json_only("910-row-binding-branch-distinct-projection", request)


def range_index_direction() -> Fixture:
    request = QueryRequest.write(
        write_batch()
        .var_as(
            "node_desc",
            g().create_index_if_not_exists(IndexSpec.node_range_desc("ParityUser", "age")),
        )
        .var_as(
            "edge_desc",
            g().create_index_if_not_exists(IndexSpec.edge_range_desc("FOLLOWS", "weight")),
        )
        .var_as(
            "node_asc",
            g().create_index_if_not_exists(IndexSpec.node_range("ParityUser", "score")),
        )
        .returning(["node_desc", "edge_desc", "node_asc"])
    )
    return json_only("911-range-index-direction", request)


def shortest_path() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "path",
            g().shortest_path(
                NodeRef.id(1),
                NodeRef.param("target"),
                5,
                label="FOLLOWS",
                direction=ShortestPathDirection.BOTH,
            ),
        )
        .returning(["path"])
    )
    return json_only(
        "912-shortest-path-terminal",
        with_params(
            request,
            [("target", 3)],
            [("target", QueryParamType.i64())],
        ),
    )


def remaining_read_contract() -> Fixture:
    comparisons = Predicate.and_(
        [
            Predicate.neq("neq", 1),
            Predicate.gt("gt", 1),
            Predicate.gte("gte", 1),
            Predicate.lt("lt", 1),
            Predicate.lte("lte", 1),
            Predicate.between("between", 1, 3),
            Predicate.ends_with("suffix", "end"),
            Predicate.is_in("status", ["active", "inactive"]),
            Predicate.is_null("missing"),
            Predicate.is_not_null("present"),
            Predicate.not_(Predicate.eq("disabled", True)),
            Predicate.compare(Expr.id(), CompareOp.EQ, Expr.val(1)),
            Predicate.compare(Expr.id(), CompareOp.NEQ, Expr.val(1)),
            Predicate.compare(Expr.id(), CompareOp.GT, Expr.val(1)),
            Predicate.compare(Expr.id(), CompareOp.GTE, Expr.val(1)),
            Predicate.compare(Expr.id(), CompareOp.LT, Expr.val(1)),
            Predicate.compare(Expr.id(), CompareOp.LTE, Expr.val(1)),
        ]
    )
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "expressions_and_predicates",
            g()
            .n(NodeRef.all())
            .where(comparisons)
            .project(
                [
                    Projection.expr("id", Expr.id()),
                    Projection.expr("timestamp", Expr.timestamp()),
                    Projection.expr("datetime", Expr.datetime()),
                    Projection.expr("null", Expr.val(PropertyValue.null())),
                    Projection.expr(
                        "date_value",
                        Expr.val(PropertyValue.date_time(1_777_000_000_000)),
                    ),
                    Projection.expr("f32", Expr.val(PropertyValue.f32(1.25))),
                    Projection.expr("bytes", Expr.val(PropertyValue.bytes([1, 2, 3]))),
                    Projection.expr("i64_array", Expr.val(PropertyValue.i64_array([1, 2, 3]))),
                    Projection.expr("f64_array", Expr.val(PropertyValue.f64_array([1.25, 2.5]))),
                    Projection.expr("add", Expr.val(4).add(Expr.val(1))),
                    Projection.expr("sub", Expr.val(4).sub(Expr.val(1))),
                    Projection.expr("mul", Expr.val(4).mul(Expr.val(2))),
                    Projection.expr("div", Expr.val(4).div(Expr.val(2))),
                    Projection.expr("mod", Expr.val(5).modulo(Expr.val(2))),
                    Projection.expr(
                        "case",
                        Expr.case(
                            [
                                WhenThen(
                                    Predicate.eq("status", "active"),
                                    Expr.val("enabled"),
                                )
                            ],
                            Expr.val("disabled"),
                        ),
                    ),
                ]
            ),
        )
        .var_as("both", g().n(NodeRef.id(1)).both().count())
        .var_as("in_e", g().n(NodeRef.id(1)).in_e().edge_properties())
        .var_as("out_e", g().n(NodeRef.id(1)).out_e().edge_properties())
        .var_as("both_e", g().n(NodeRef.id(1)).both_e().edge_properties())
        .var_as("in_n", g().e(EdgeRef.all()).in_n().value_map())
        .var_as("out_n", g().e(EdgeRef.all()).out_n().value_map())
        .var_as("other_n", g().e(EdgeRef.all()).other_n().value_map())
        .var_as("direct_has_key", g().n(NodeRef.all()).has_key("externalId").count())
        .var_as("has_label", g().n(NodeRef.all()).has_label("ParityUser").count())
        .var_as("exists", g().n(NodeRef.all()).exists())
        .var_as(
            "choose",
            g()
            .n(NodeRef.all())
            .choose(Predicate.is_not_null("status"), sub().out(), sub().in_())
            .count(),
        )
        .var_as(
            "coalesce",
            g().n(NodeRef.all()).coalesce([sub().out(), sub().in_()]).count(),
        )
        .var_as("group", g().n(NodeRef.all()).group("status"))
        .var_as("group_count", g().n(NodeRef.all()).group_count("status"))
        .var_as(
            "aggregate_count",
            g().n(NodeRef.all()).aggregate_by(AggregateFunction.COUNT, "age"),
        )
        .var_as(
            "aggregate_sum",
            g().n(NodeRef.all()).aggregate_by(AggregateFunction.SUM, "age"),
        )
        .var_as(
            "aggregate_min",
            g().n(NodeRef.all()).aggregate_by(AggregateFunction.MIN, "age"),
        )
        .var_as(
            "aggregate_max",
            g().n(NodeRef.all()).aggregate_by(AggregateFunction.MAX, "age"),
        )
        .var_as(
            "aggregate_mean",
            g().n(NodeRef.all()).aggregate_by(AggregateFunction.MEAN, "age"),
        )
        .var_as(
            "repeat_none",
            g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out())).count(),
        )
        .var_as(
            "repeat_before",
            g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out()).emit_before()).count(),
        )
        .var_as(
            "repeat_after",
            g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out()).emit_after()).count(),
        )
        .var_as(
            "repeat_all",
            g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out()).emit_all()).count(),
        )
        .var_as(
            "shortest_out",
            g().shortest_path(
                NodeRef.id(1),
                NodeRef.id(2),
                5,
                direction=ShortestPathDirection.OUT,
            ),
        )
        .var_as(
            "shortest_in",
            g().shortest_path(
                NodeRef.id(1),
                NodeRef.id(2),
                5,
                direction=ShortestPathDirection.IN,
            ),
        )
        .var_as(
            "vector_edges",
            g().vector_search_edges("FOLLOWS", "embedding", [1.0, 0.0], 5).edge_properties(),
        )
        .var_as(
            "vector_nodes_within",
            g()
            .n_with_label("ParityUser")
            .vector_search("ParityUser", "embedding", [1.0, 0.0, 0.0], 5),
        )
        .var_as(
            "vector_edges_within",
            g()
            .e(EdgeRef.all())
            .has_label("FOLLOWS")
            .vector_search("FOLLOWS", "embedding", [1.0, 0.0], 5),
        )
        .var_as(
            "text_edges",
            g().text_search_edges("FOLLOWS", "note", "graph", 5).edge_properties(),
        )
        .var_as_if(
            "previous",
            BatchCondition.prev_not_empty(),
            g().n(NodeRef.all()).count(),
        )
        .var_as_if(
            "not_empty",
            BatchCondition.var_not_empty("expressions_and_predicates"),
            g().n(NodeRef.all()).count(),
        )
        .var_as_if(
            "empty",
            BatchCondition.var_empty("missing"),
            g().n(NodeRef.all()).count(),
        )
        .var_as_if(
            "min_size",
            BatchCondition.var_min_size("expressions_and_predicates", 1),
            g().n(NodeRef.all()).count(),
        )
        .for_each_param(
            "rows",
            read_batch().var_as("foreach", g().n(NodeRef.all()).count()),
        )
        .returning(
            [
                "expressions_and_predicates",
                "both",
                "in_e",
                "out_e",
                "both_e",
                "in_n",
                "out_n",
                "other_n",
                "direct_has_key",
                "has_label",
                "exists",
                "choose",
                "coalesce",
                "group",
                "group_count",
                "aggregate_count",
                "aggregate_sum",
                "aggregate_min",
                "aggregate_max",
                "aggregate_mean",
                "repeat_none",
                "repeat_before",
                "repeat_after",
                "repeat_all",
                "shortest_out",
                "shortest_in",
                "vector_edges",
                "vector_nodes_within",
                "vector_edges_within",
                "text_edges",
                "previous",
                "not_empty",
                "empty",
                "min_size",
                "foreach",
            ]
        )
    )
    request.insert_typed_parameter(
        "date_time", QueryParamType.date_time(), "2026-01-01T00:00:00.000Z"
    )
    return json_only("913-remaining-read-contract", request)


def remaining_write_contract() -> Fixture:
    request = QueryRequest.write(
        write_batch()
        .var_as(
            "edge_equality",
            g().create_index_if_not_exists(IndexSpec.edge_equality("FOLLOWS", "since")),
        )
        .var_as(
            "node_euclidean",
            g().create_index_if_not_exists(
                IndexSpec.node_vector(
                    "ParityUser",
                    "euclidean_embedding",
                    4,
                    VectorDistanceMetric.EUCLIDEAN,
                )
            ),
        )
        .var_as(
            "edge_manhattan",
            g().create_index_if_not_exists(
                IndexSpec.edge_vector(
                    "FOLLOWS",
                    "manhattan_embedding",
                    4,
                    VectorDistanceMetric.MANHATTAN,
                )
            ),
        )
        .returning(["edge_equality", "node_euclidean", "edge_manhattan"])
    )
    return json_only("914-remaining-write-contract", request)


def neo4j_migration_movie_details() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "movie_node",
            g().n_with_label("Movie").where(Predicate.eq_param("movieId", "movie_id")),
        )
        .var_as(
            "movie",
            g()
            .n(NodeRef.var("movie_node"))
            .project(
                [
                    Projection.property("movieId", "movieId"),
                    Projection.property("title", "title"),
                    Projection.property("released", "released"),
                    Projection.property("tagline", "tagline"),
                ]
            ),
        )
        .var_as(
            "cast",
            g()
            .n(NodeRef.var("movie_node"))
            .in_e("ACTED_IN")
            .project(
                [
                    Projection.from_endpoint("personId", "personId"),
                    Projection.from_endpoint("name", "name"),
                    Projection.from_endpoint("born", "born"),
                    Projection.property("roles", "roles"),
                ]
            ),
        )
        .var_as(
            "directors",
            g()
            .n(NodeRef.var("movie_node"))
            .in_e("DIRECTED")
            .project(
                [
                    Projection.from_endpoint("personId", "personId"),
                    Projection.from_endpoint("name", "name"),
                    Projection.from_endpoint("born", "born"),
                ]
            ),
        )
        .var_as(
            "producers",
            g()
            .n(NodeRef.var("movie_node"))
            .in_e("PRODUCED")
            .project(
                [
                    Projection.from_endpoint("personId", "personId"),
                    Projection.from_endpoint("name", "name"),
                    Projection.from_endpoint("born", "born"),
                ]
            ),
        )
        .returning(["movie", "cast", "directors", "producers"]),
        "movie_details",
    )
    return json_only(
        "915-neo4j-migration-movie-details",
        with_params(
            request,
            [("movie_id", "m-matrix")],
            [("movie_id", QueryParamType.string())],
        ),
    )


def neo4j_migration_filmography() -> Fixture:
    request = QueryRequest.read(
        read_batch()
        .var_as(
            "person_node",
            g().n_with_label("Person").where(Predicate.eq_param("personId", "person_id")),
        )
        .var_as(
            "person",
            g()
            .n(NodeRef.var("person_node"))
            .project(
                [
                    Projection.property("personId", "personId"),
                    Projection.property("name", "name"),
                    Projection.property("born", "born"),
                ]
            ),
        )
        .var_as(
            "movies",
            g()
            .n(NodeRef.var("person_node"))
            .out_e("ACTED_IN")
            .bind("credit")
            .out_n()
            .bind("movie")
            .project_bindings(
                [
                    BindingProjection.binding("movie", "movieId", "movieId"),
                    BindingProjection.binding("movie", "title", "title"),
                    BindingProjection.binding("movie", "released", "released"),
                    BindingProjection.binding("movie", "tagline", "tagline"),
                    BindingProjection.binding("credit", "roles", "roles"),
                ]
            ),
        )
        .returning(["person", "movies"]),
        "person_filmography",
    )
    return json_only(
        "916-neo4j-migration-person-filmography",
        with_params(
            request,
            [("person_id", "p-keanu")],
            [("person_id", QueryParamType.string())],
        ),
    )


def main() -> None:
    runtime_fixtures = [*base_runtime_fixtures(), *node_permutation_fixtures()]
    json_only_fixtures = [
        raw_read_steps(),
        raw_write_steps(),
        query_value_shapes(),
        runtime_input_shapes(),
        reference_shapes(),
        source_mutators(),
        nested_write_shapes(),
        nested_read_shapes(),
        endpoint_projection(),
        binding_projection(),
        branch_binding_projection(),
        range_index_direction(),
        shortest_path(),
        remaining_read_contract(),
        remaining_write_contract(),
        neo4j_migration_movie_details(),
        neo4j_migration_filmography(),
    ]
    if len(runtime_fixtures) != 233:
        raise RuntimeError(f"generated {len(runtime_fixtures)} runtime fixtures, expected 233")
    if len(json_only_fixtures) != 17:
        raise RuntimeError(f"generated {len(json_only_fixtures)} JSON-only fixtures, expected 17")
    names = [name for name, _ in runtime_fixtures]
    names.extend(fixture.name for fixture in json_only_fixtures)
    if len(set(names)) != len(names):
        raise RuntimeError("duplicate Python parity fixture name")

    shutil.rmtree(OUTPUT_ROOT, ignore_errors=True)
    runtime_output = OUTPUT_ROOT / "runtime"
    json_only_output = OUTPUT_ROOT / "json-only"
    runtime_output.mkdir(parents=True)
    json_only_output.mkdir(parents=True)
    for name, request in runtime_fixtures:
        (runtime_output / f"{name}.json").write_text(request.to_json_string(), encoding="utf-8")
    for fixture in json_only_fixtures:
        (json_only_output / f"{fixture.name}.json").write_text(
            fixture.request.to_json_string(), encoding="utf-8"
        )


if __name__ == "__main__":
    main()
