from __future__ import annotations

from typing import Any

from helixdb import (
    AggregateFunction,
    BatchCondition,
    CompareOp,
    DateTime,
    Expr,
    IndexSpec,
    NodeRef,
    Order,
    Predicate,
    Projection,
    PropertyInput,
    PropertyValue,
    QueryParamType,
    QueryRequest,
    RepeatConfig,
    SourcePredicate,
    Traversal,
    VectorDistanceMetric,
    WhenThen,
    g,
    read_batch,
    sub,
    write_batch,
)

RuntimeFixture = tuple[str, QueryRequest]


def _with_params(
    request: QueryRequest,
    values: list[tuple[str, Any]],
    types: list[tuple[str, QueryParamType]],
) -> QueryRequest:
    for name, value in values:
        request.insert_parameter_value(name, value)
    for name, param_type in types:
        request.insert_parameter_type(name, param_type)
    return request


def _user_props(
    external_id: str,
    name: str,
    age: int,
    score: float,
    status: str,
    city: str,
    bio: str,
    embedding: list[float],
) -> list[tuple[str, PropertyInput]]:
    return [
        ("externalId", PropertyInput.value(external_id)),
        ("name", PropertyInput.value(name)),
        ("age", PropertyInput.value(age)),
        ("score", PropertyInput.value(PropertyValue.f64(score))),
        ("status", PropertyInput.value(status)),
        ("tenantId", PropertyInput.value("tenant-a")),
        ("city", PropertyInput.value(city)),
        ("bio", PropertyInput.value(bio)),
        ("createdAt", PropertyInput.value(DateTime.from_millis(1_776_000_000_000))),
        ("embedding", PropertyInput.value(PropertyValue.f32_array(embedding))),
    ]


def base_runtime_fixtures() -> list[RuntimeFixture]:
    return [
        (
            "001-write-seed-core",
            QueryRequest.write(
                write_batch()
                .var_as(
                    "alice",
                    g().add_n(
                        "ParityUser",
                        _user_props(
                            "user-alice",
                            "Alice",
                            31,
                            90.5,
                            "active",
                            "London",
                            "Alice writes graph database tests",
                            [1.0, 0.0, 0.0],
                        ),
                    ),
                )
                .var_as(
                    "bob",
                    g().add_n(
                        "ParityUser",
                        _user_props(
                            "user-bob",
                            "Bob",
                            27,
                            72.25,
                            "active",
                            "Paris",
                            "Bob likes traversal testing",
                            [0.9, 0.1, 0.0],
                        ),
                    ),
                )
                .var_as(
                    "carol",
                    g().add_n(
                        "ParityUser",
                        _user_props(
                            "user-carol",
                            "Carol",
                            42,
                            64.0,
                            "inactive",
                            "Berlin",
                            "Carol archives old records",
                            [0.0, 1.0, 0.0],
                        ),
                    ),
                )
                .var_as(
                    "alice_follows_bob",
                    g()
                    .n(NodeRef.var("alice"))
                    .add_e(
                        "FOLLOWS",
                        NodeRef.var("bob"),
                        [
                            ("weight", PropertyInput.value(PropertyValue.f64(1.0))),
                            ("since", PropertyInput.value("2024-01-01")),
                            ("note", PropertyInput.value("Alice follows Bob")),
                            (
                                "embedding",
                                PropertyInput.value(PropertyValue.f32_array([1.0, 0.0])),
                            ),
                        ],
                    ),
                )
                .var_as(
                    "bob_follows_carol",
                    g()
                    .n(NodeRef.var("bob"))
                    .add_e(
                        "FOLLOWS",
                        NodeRef.var("carol"),
                        [
                            ("weight", PropertyInput.value(PropertyValue.f64(0.5))),
                            ("since", PropertyInput.value("2024-02-01")),
                            ("note", PropertyInput.value("Bob follows Carol")),
                            (
                                "embedding",
                                PropertyInput.value(PropertyValue.f32_array([0.0, 1.0])),
                            ),
                        ],
                    ),
                )
                .returning(
                    [
                        "alice",
                        "bob",
                        "carol",
                        "alice_follows_bob",
                        "bob_follows_carol",
                    ]
                )
            ),
        ),
        (
            "002-read-count-all-users",
            QueryRequest.read(
                read_batch()
                .var_as("user_count", g().n_with_label("ParityUser").count())
                .returning(["user_count"])
            ),
        ),
        (
            "003-read-source-predicate-and-count",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "active_adults",
                    g()
                    .n_with_label_where(
                        "ParityUser",
                        SourcePredicate.and_(
                            [
                                SourcePredicate.eq("status", "active"),
                                SourcePredicate.gte("age", 30),
                            ]
                        ),
                    )
                    .count(),
                )
                .returning(["active_adults"])
            ),
        ),
        (
            "004-read-value-map-projection",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "alice",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-alice"))
                    .project(
                        [
                            Projection.property("externalId", "id"),
                            Projection.property("name", "name"),
                            Projection.expr(
                                "score_plus_one",
                                Expr.prop("score").add(
                                    Expr.val(PropertyValue.f64(1.0))
                                ),
                            ),
                            Projection.expr(
                                "status_label",
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
                .returning(["alice"])
            ),
        ),
        (
            "005-read-order-range-values",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "ordered",
                    g()
                    .n_with_label("ParityUser")
                    .order_by_multiple(
                        [("status", Order.ASC), ("age", Order.DESC)]
                    )
                    .range(0, 2)
                    .value_map(["externalId", "age", "status"]),
                )
                .returning(["ordered"])
            ),
        ),
        (
            "006-read-edge-count",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "edge_count",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-alice"))
                    .out_e("FOLLOWS")
                    .count(),
                )
                .returning(["edge_count"])
            ),
        ),
        (
            "007-read-edge-properties",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "edges",
                    g()
                    .e_with_label("FOLLOWS")
                    .edge_has("weight", PropertyInput.value(PropertyValue.f64(1.0)))
                    .edge_properties(),
                )
                .returning(["edges"])
            ),
        ),
        (
            "008-read-edge-endpoints",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "from_nodes",
                    g()
                    .e_with_label("FOLLOWS")
                    .edge_has_label("FOLLOWS")
                    .in_n()
                    .value_map(["externalId", "name"]),
                )
                .var_as(
                    "to_nodes",
                    g()
                    .e_with_label("FOLLOWS")
                    .out_n()
                    .value_map(["externalId", "name"]),
                )
                .returning(["from_nodes", "to_nodes"])
            ),
        ),
        (
            "009-read-conditional-var-not-empty",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "alice",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-alice")),
                )
                .var_as_if(
                    "friends",
                    BatchCondition.var_not_empty("alice"),
                    g()
                    .n(NodeRef.var("alice"))
                    .out("FOLLOWS")
                    .value_map(["externalId", "name"]),
                )
                .returning(["alice", "friends"])
            ),
        ),
        (
            "010-read-conditional-var-empty",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "missing",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "missing-user")),
                )
                .var_as_if(
                    "fallback",
                    BatchCondition.var_empty("missing"),
                    g().n_with_label("ParityUser").limit(1).value_map(["externalId"]),
                )
                .returning(["missing", "fallback"])
            ),
        ),
        (
            "011-read-conditional-var-min-size-prev",
            QueryRequest.read(
                read_batch()
                .var_as("users", g().n_with_label("ParityUser").limit(3))
                .var_as_if(
                    "min_two",
                    BatchCondition.var_min_size("users", 2),
                    g().n(NodeRef.var("users")).count(),
                )
                .var_as_if(
                    "prev_ok",
                    BatchCondition.prev_not_empty(),
                    g().n(NodeRef.var("users")).exists(),
                )
                .returning(["min_two", "prev_ok"])
            ),
        ),
        (
            "012-read-foreach-param",
            _with_params(
                QueryRequest.read(
                    read_batch()
                    .for_each_param(
                        "lookups",
                        read_batch().var_as(
                            "matched",
                            g()
                            .n_with_label("ParityUser")
                            .where(Predicate.eq_param("externalId", "externalId"))
                            .value_map(["externalId", "name"]),
                        ),
                    )
                    .returning(["matched"])
                ),
                [
                    (
                        "lookups",
                        [
                            {"externalId": "user-alice"},
                            {"externalId": "user-carol"},
                        ],
                    )
                ],
                [("lookups", QueryParamType.array(QueryParamType.object()))],
            ),
        ),
        (
            "013-write-foreach-param-create",
            _with_params(
                QueryRequest.write(
                    write_batch()
                    .for_each_param(
                        "rows",
                        write_batch().var_as(
                            "created",
                            g().add_n(
                                "ParityEvent",
                                [
                                    ("eventId", PropertyInput.param("eventId")),
                                    ("kind", PropertyInput.param("kind")),
                                    ("score", PropertyInput.param("score")),
                                ],
                            ),
                        ),
                    )
                    .returning(["created"])
                ),
                [
                    (
                        "rows",
                        [
                            {"eventId": "event-1", "kind": "click", "score": 10},
                            {"eventId": "event-2", "kind": "view", "score": 5},
                        ],
                    )
                ],
                [("rows", QueryParamType.array(QueryParamType.object()))],
            ),
        ),
        (
            "014-read-after-foreach-param",
            QueryRequest.read(
                read_batch()
                .var_as("event_count", g().n_with_label("ParityEvent").count())
                .returning(["event_count"])
            ),
        ),
        (
            "015-write-set-remove-properties",
            QueryRequest.write(
                write_batch()
                .var_as(
                    "updated",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-bob"))
                    .set_property("status", PropertyInput.value("inactive"))
                    .set_property(
                        "updatedAt",
                        PropertyInput.value(DateTime.from_millis(1_777_000_000_000)),
                    )
                    .remove_property("city")
                    .count(),
                )
                .returning(["updated"])
            ),
        ),
        (
            "016-read-updated-properties",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "bob",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-bob"))
                    .value_map(["externalId", "status", "updatedAt", "city"]),
                )
                .returning(["bob"])
            ),
        ),
        (
            "017-read-repeat-union",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "walked",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-alice"))
                    .repeat(
                        RepeatConfig.new(sub().out("FOLLOWS"))
                        .times(2)
                        .emit_all()
                        .max_depth(4)
                    )
                    .union([sub().out("FOLLOWS"), sub().in_("FOLLOWS")])
                    .dedup()
                    .value_map(["externalId", "name"]),
                )
                .returning(["walked"])
            ),
        ),
        (
            "018-read-choose-coalesce-optional",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "branched",
                    g()
                    .n_with_label("ParityUser")
                    .where(Predicate.eq("externalId", "user-alice"))
                    .choose(
                        Predicate.eq("status", "active"),
                        sub().out("FOLLOWS"),
                        sub().in_("FOLLOWS"),
                    )
                    .coalesce([sub().out("FOLLOWS"), sub().in_("FOLLOWS")])
                    .optional(sub().out("FOLLOWS"))
                    .dedup()
                    .value_map(["externalId", "name"]),
                )
                .returning(["branched"])
            ),
        ),
        (
            "019-read-aggregations",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "by_status", g().n_with_label("ParityUser").group_count("status")
                )
                .var_as(
                    "mean_score",
                    g()
                    .n_with_label("ParityUser")
                    .aggregate_by(AggregateFunction.MEAN, "score"),
                )
                .var_as(
                    "max_age",
                    g()
                    .n_with_label("ParityUser")
                    .aggregate_by(AggregateFunction.MAX, "age"),
                )
                .returning(["by_status", "mean_score", "max_age"])
            ),
        ),
        (
            "020-write-index-create",
            QueryRequest.write(
                write_batch()
                .var_as(
                    "node_eq",
                    g().create_index_if_not_exists(
                        IndexSpec.node_equality("ParityUser", "externalId")
                    ),
                )
                .var_as(
                    "node_range",
                    g().create_index_if_not_exists(
                        IndexSpec.node_range("ParityUser", "age")
                    ),
                )
                .var_as(
                    "edge_eq",
                    g().create_index_if_not_exists(
                        IndexSpec.edge_equality("FOLLOWS", "since")
                    ),
                )
                .var_as(
                    "edge_range",
                    g().create_index_if_not_exists(
                        IndexSpec.edge_range("FOLLOWS", "weight")
                    ),
                )
                .returning(["node_eq", "node_range", "edge_eq", "edge_range"])
            ),
        ),
        (
            "021-read-parameter-types",
            _with_params(
                QueryRequest.read(
                    read_batch()
                    .var_as(
                        "matches",
                        g()
                        .n_with_label("ParityUser")
                        .where(Predicate.is_in_param("status", "statuses"))
                        .where(Predicate.gte_param("createdAt", "created_after"))
                        .limit(Expr.param("limit"))
                        .value_map(["externalId", "status"]),
                    )
                    .returning(["matches"])
                ),
                [
                    ("statuses", ["active", "inactive"]),
                    ("created_after", "2026-01-01T00:00:00.000Z"),
                    ("limit", 5),
                ],
                [
                    ("statuses", QueryParamType.array(QueryParamType.string())),
                    ("created_after", QueryParamType.date_time()),
                    ("limit", QueryParamType.i64()),
                ],
            ),
        ),
        (
            "022-write-property-value-variants",
            QueryRequest.write(
                write_batch()
                .var_as(
                    "variant_node",
                    g().add_n(
                        "ParityVariant",
                        [
                            ("nullValue", PropertyInput.value(PropertyValue.null())),
                            ("boolValue", PropertyInput.value(True)),
                            (
                                "i64Value",
                                PropertyInput.value(
                                    PropertyValue.i64(9_223_372_036_854_775_000)
                                ),
                            ),
                            (
                                "dateTimeValue",
                                PropertyInput.value(DateTime.from_millis(-1)),
                            ),
                            ("f64Value", PropertyInput.value(3.25)),
                            (
                                "f32Value",
                                PropertyInput.value(PropertyValue.f32(1.5)),
                            ),
                            ("stringValue", PropertyInput.value("variant")),
                            (
                                "bytesValue",
                                PropertyInput.value(PropertyValue.bytes([1, 2, 3])),
                            ),
                            (
                                "i64Array",
                                PropertyInput.value(PropertyValue.i64_array([1, 2, 3])),
                            ),
                            (
                                "f64Array",
                                PropertyInput.value(PropertyValue.f64_array([1.0, 2.0])),
                            ),
                            (
                                "f32Array",
                                PropertyInput.value(PropertyValue.f32_array([1.0, 2.0])),
                            ),
                            (
                                "stringArray",
                                PropertyInput.value(
                                    PropertyValue.string_array(["a", "b"])
                                ),
                            ),
                        ],
                    ),
                )
                .returning(["variant_node"])
            ),
        ),
        (
            "023-read-property-value-variants",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "variant", g().n_with_label("ParityVariant").value_map(None)
                )
                .returning(["variant"])
            ),
        ),
        (
            "024-write-text-vector-indexes",
            QueryRequest.write(
                write_batch()
                .var_as(
                    "node_text",
                    g().create_text_index_nodes("ParityUser", "bio", None),
                )
                .var_as(
                    "node_vector",
                    g().create_vector_index_nodes(
                        "ParityUser",
                        "embedding",
                        3,
                        VectorDistanceMetric.COSINE,
                        None,
                    ),
                )
                .var_as(
                    "edge_text",
                    g().create_text_index_edges("FOLLOWS", "note", None),
                )
                .var_as(
                    "edge_vector",
                    g().create_vector_index_edges(
                        "FOLLOWS",
                        "embedding",
                        2,
                        VectorDistanceMetric.COSINE,
                        None,
                    ),
                )
                .returning(["node_text", "node_vector", "edge_text", "edge_vector"])
            ),
        ),
        (
            "025-read-text-search-nodes",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "text_hits",
                    g()
                    .text_search_nodes("ParityUser", "bio", "graph", 5, None)
                    .value_map(["externalId", "bio", "$distance"]),
                )
                .returning(["text_hits"])
            ),
        ),
        (
            "026-read-vector-search-nodes",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "vector_hits",
                    g()
                    .vector_search_nodes(
                        "ParityUser", "embedding", [1.0, 0.0, 0.0], 3, None
                    )
                    .project(
                        [
                            Projection.property("externalId", "externalId"),
                            Projection.property("$distance", "distance"),
                        ]
                    ),
                )
                .returning(["vector_hits"])
            ),
        ),
        (
            "027-read-text-search-edges",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "edge_text_hits",
                    g()
                    .text_search_edges("FOLLOWS", "note", "follows", 5, None)
                    .edge_properties(),
                )
                .returning(["edge_text_hits"])
            ),
        ),
        (
            "028-read-vector-search-edges",
            QueryRequest.read(
                read_batch()
                .var_as(
                    "edge_vector_hits",
                    g()
                    .vector_search_edges(
                        "FOLLOWS", "embedding", [1.0, 0.0], 5, None
                    )
                    .edge_properties(),
                )
                .returning(["edge_vector_hits"])
            ),
        ),
        (
            "029-write-drop-temp-node",
            QueryRequest.write(
                write_batch()
                .var_as(
                    "temp",
                    g().add_n(
                        "ParityTemp", [("name", PropertyInput.value("temp"))]
                    ),
                )
                .var_as(
                    "dropped",
                    g().n(NodeRef.var("temp")).drop().count(),
                )
                .returning(["dropped"])
            ),
        ),
        (
            "030-read-final-counts",
            QueryRequest.read(
                read_batch()
                .var_as("users", g().n_with_label("ParityUser").count())
                .var_as("events", g().n_with_label("ParityEvent").count())
                .var_as("variants", g().n_with_label("ParityVariant").count())
                .returning(["users", "events", "variants"])
            ),
        ),
        (
            "031-read-source-predicate-eq-param",
            _with_params(
                QueryRequest.read(
                    read_batch()
                    .var_as(
                        "user",
                        g()
                        .n_where(
                            SourcePredicate.and_(
                                [
                                    SourcePredicate.eq("$label", "ParityUser"),
                                    SourcePredicate.eq("name", Expr.param("name")),
                                ]
                            )
                        )
                        .value_map(["externalId", "name"]),
                    )
                    .returning(["user"])
                ),
                [("name", "Alice")],
                [("name", QueryParamType.string())],
            ),
        ),
        (
            "032-read-source-predicate-between-param",
            _with_params(
                QueryRequest.read(
                    read_batch()
                    .var_as(
                        "adults",
                        g()
                        .n_where(
                            SourcePredicate.and_(
                                [
                                    SourcePredicate.eq("$label", "ParityUser"),
                                    SourcePredicate.between(
                                        "age", Expr.param("min_age"), 65
                                    ),
                                ]
                            )
                        )
                        .value_map(["externalId", "age"]),
                    )
                    .returning(["adults"])
                ),
                [("min_age", 30)],
                [("min_age", QueryParamType.i64())],
            ),
        ),
    ]


def node_permutation_fixtures() -> list[RuntimeFixture]:
    fixtures: list[RuntimeFixture] = []
    index = 100
    for source in ("label", "where", "all"):
        for filter_name in ("none", "has", "logic", "expr"):
            for bound in ("none", "limit", "skip", "range"):
                for terminal in ("count", "exists", "value_map", "project"):
                    fixtures.append(
                        (
                            f"{index:03d}-combo-node-{source}-{filter_name}-{bound}-{terminal}",
                            QueryRequest.read(
                                _node_combo_batch(source, filter_name, bound, terminal)
                            ),
                        )
                    )
                    index += 1
    return fixtures


def _node_combo_batch(source: str, filter_name: str, bound: str, terminal: str):
    traversal = _apply_node_bound(
        _apply_node_filter(_node_source(source), filter_name), bound
    ).order_by("externalId", Order.ASC)
    if terminal == "count":
        terminal_traversal = traversal.count()
    elif terminal == "exists":
        terminal_traversal = traversal.exists()
    elif terminal == "value_map":
        terminal_traversal = traversal.value_map(
            ["externalId", "name", "age", "status"]
        )
    elif terminal == "project":
        terminal_traversal = traversal.project(
            [
                Projection.property("externalId", "externalId"),
                Projection.property("status", "status"),
                Projection.expr("age_plus_two", Expr.prop("age").add(Expr.val(2))),
            ]
        )
    else:
        raise ValueError(f"unknown terminal {terminal}")
    return read_batch().var_as("result", terminal_traversal).returning(["result"])


def _node_source(source: str) -> Traversal:
    if source == "label":
        return g().n_with_label("ParityUser")
    if source == "where":
        return g().n_where(SourcePredicate.eq("$label", "ParityUser"))
    if source == "all":
        return g().n(NodeRef.all()).has_label("ParityUser")
    raise ValueError(f"unknown source {source}")


def _apply_node_filter(traversal: Traversal, filter_name: str) -> Traversal:
    if filter_name == "none":
        return traversal
    if filter_name == "has":
        return traversal.has("status", "active")
    if filter_name == "logic":
        return traversal.where(
            Predicate.and_(
                [
                    Predicate.has_key("externalId"),
                    Predicate.or_(
                        [
                            Predicate.starts_with("name", "A"),
                            Predicate.ends_with("name", "b"),
                        ]
                    ),
                    Predicate.not_(Predicate.is_null("age")),
                ]
            )
        )
    if filter_name == "expr":
        return traversal.where(
            Predicate.compare(
                Expr.prop("score").add(Expr.val(PropertyValue.f64(1.0))),
                CompareOp.GT,
                Expr.val(PropertyValue.f64(65.0)),
            )
        )
    raise ValueError(f"unknown filter {filter_name}")


def _apply_node_bound(traversal: Traversal, bound: str) -> Traversal:
    if bound == "none":
        return traversal
    if bound == "limit":
        return traversal.limit(2)
    if bound == "skip":
        return traversal.skip(1)
    if bound == "range":
        return traversal.range(0, 2)
    raise ValueError(f"unknown bound {bound}")
