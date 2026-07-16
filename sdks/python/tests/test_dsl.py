from __future__ import annotations

import json
import unittest

from helixdb import (
    BatchCondition,
    BindingProjection,
    DateTime,
    QueryError,
    QueryValue,
    Expr,
    IndexSpec,
    NodeRef,
    Order,
    Predicate,
    Projection,
    PropertyInput,
    PropertyProjection,
    PropertyValue,
    QueryParamType,
    RangeIndexDirection,
    RepeatConfig,
    ShortestPathDirection,
    SourcePredicate,
    VectorDistanceMetric,
    WhenThen,
    define_params,
    g,
    param,
    parse_index_ddl_receipt,
    parse_index_operation_status,
    read_batch,
    stringify_json,
    structural_json_equal,
    sub,
    write_batch,
)


def parsed(value: object) -> object:
    return json.loads(stringify_json(value))


class DslAstTests(unittest.TestCase):
    def test_index_lifecycle_response_decoders_are_strict_and_additive(self) -> None:
        operation_id = "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"
        receipt = parse_index_ddl_receipt({
            "kind": "accepted",
            "operation_id": operation_id,
            "index_id": "42",
            "generation": "3",
            "future": True,
        })
        self.assertEqual(receipt.kind, "accepted")
        status = parse_index_operation_status({
            "status": "blocked",
            "operation_id": operation_id,
            "index_id": "42",
            "generation": "3",
            "operation_kind": "build",
            "family": "secondary",
            "stage": "scan",
            "attempt": 2,
            "progress": {
                "entities": "9",
                "input_bytes": "10",
                "output_operations": "11",
                "output_bytes": "12",
                "future": True,
            },
            "blocker_code": "uniqueness_violation",
            "future": True,
        })
        self.assertEqual(status.status, "blocked")
        for stage in ("await_upload", "validate_manifests"):
            status = parse_index_operation_status({
                "status": "queued",
                "operation_id": operation_id,
                "index_id": "42",
                "generation": "3",
                "operation_kind": "build",
                "family": "text",
                "stage": stage,
                "attempt": 0,
                "progress": {
                    "entities": "0",
                    "input_bytes": "0",
                    "output_operations": "0",
                    "output_bytes": "0",
                },
            })
            self.assertEqual(status.common.stage, stage)
        with self.assertRaises(ValueError):
            parse_index_ddl_receipt({"kind": "future"})
        with self.assertRaises(ValueError):
            parse_index_operation_status({
                "status": "queued",
                "operation_id": operation_id.upper(),
                "index_id": "42",
                "generation": "3",
                "operation_kind": "build",
                "family": "secondary",
                "stage": "scan",
                "attempt": 0,
                "progress": {
                    "entities": "0",
                    "input_bytes": "0",
                    "output_operations": "0",
                    "output_bytes": "0",
                },
            })

    def test_values_exprs_and_predicates_use_ast_shape(self) -> None:
        self.assertTrue(structural_json_equal(b'{"n":9223372036854775807}', b'{"n":9223372036854775807}'))
        self.assertEqual(parsed(PropertyValue.null()), "null")
        self.assertEqual(parsed(PropertyValue.bytes(b"\x01\x02")), {"bytes": [1, 2]})
        self.assertEqual(parsed(PropertyInput.param("limit")), {"expr": {"param": "limit"}})
        self.assertEqual(parsed(NodeRef.param("node_ids")), {"param": "node_ids"})
        self.assertEqual(parsed(QueryParamType.array(QueryParamType.array(QueryParamType.f64()))), {"array": {"array": "f64"}})
        self.assertEqual(PropertyValue.string("x").as_str(), "x")
        self.assertEqual(DateTime.parse_rfc3339("1969-12-31T23:59:59.999-00:00").to_rfc3339(), "1969-12-31T23:59:59.999Z")

        self.assertEqual(
            parsed(Expr.prop("a").add(Expr.val(1)).neg()),
            {"neg": {"expr": {"add": {"left": {"property": "a"}, "right": {"constant": {"i64": 1}}}}}},
        )
        self.assertEqual(
            parsed(
                Expr.case(
                    [WhenThen(Predicate.is_not_null("email"), Expr.prop("email"))],
                    Expr.val("missing"),
                )
            ),
            {
                "case": {
                    "when_then": [
                        {
                            "when": {"is_not_null": {"property": "email"}},
                            "then": {"property": "email"},
                        }
                    ],
                    "else_expr": {"constant": {"string": "missing"}},
                }
            },
        )
        self.assertEqual(
            parsed(Predicate.eq("username", Expr.param("name"))),
            {"eq": {"left": {"property": "username"}, "right": {"param": "name"}}},
        )
        self.assertEqual(
            parsed(SourcePredicate.or_([SourcePredicate.has_key("name"), SourcePredicate.starts_with("name", "A")])),
            {
                "or": {
                    "predicates": [
                        {"has_key": {"property": "name"}},
                        {"starts_with": {"value": {"property": "name"}, "prefix": {"constant": {"string": "A"}}}},
                    ]
                }
            },
        )

    def test_row_binding_projection_ast_shape(self) -> None:
        traversal = (
            g()
            .n_with_label("Service")
            .bind("service")
            .optional(sub().in_("CREATES").bind("deployment"))
            .union([sub().in_("MANAGES").bind("owner"), sub().out("ROUTES_TO").bind("workload")])
            .project_distinct_bindings(
                [
                    BindingProjection.binding("service", "$id", "service_id"),
                    BindingProjection.current("$id", "current_id"),
                    BindingProjection.coalesce(
                        [
                            BindingProjection.binding_ref("deployment", "$id"),
                            BindingProjection.binding_ref("owner", "$id"),
                            BindingProjection.binding_ref("workload", "$id"),
                        ],
                        "workload_id",
                    ),
                ]
            )
        )
        root = parsed(traversal)["root"]
        self.assertTrue(traversal.has_terminal())
        self.assertEqual(root["project_bindings"]["distinct"], True)
        self.assertEqual(
            root["project_bindings"]["projections"][0],
            {"property": {"target": {"binding": "service"}, "source": "$id", "alias": "service_id"}},
        )
        self.assertEqual(
            root["project_bindings"]["projections"][1],
            {"property": {"target": "current", "source": "$id", "alias": "current_id"}},
        )
        self.assertEqual(
            root["project_bindings"]["projections"][2],
            {
                "coalesce": {
                    "refs": [
                        {"target": {"binding": "deployment"}, "source": "$id"},
                        {"target": {"binding": "owner"}, "source": "$id"},
                        {"target": {"binding": "workload"}, "source": "$id"},
                    ],
                    "alias": "workload_id",
                }
            },
        )

    def test_row_binding_builders_reject_invalid_contracts(self) -> None:
        with self.assertRaises(ValueError):
            g().n_with_label("Service").bind("")
        with self.assertRaises(ValueError):
            BindingProjection.binding("service", "$id", "")
        with self.assertRaises(ValueError):
            BindingProjection.coalesce([], "workload_id")
        with self.assertRaises(ValueError):
            g().n_with_label("Service").project_bindings([])

    def test_batches_emit_entries_and_nested_roots(self) -> None:
        read = (
            read_batch()
            .var_as("user", g().n_where(SourcePredicate.eq("username", "alice")))
            .var_as("friends", g().n(NodeRef.var("user")).out("FOLLOWS").dedup().limit(100))
            .returning(["user", "friends"])
        )
        self.assertEqual(
            parsed(read),
            {
                "entries": [
                    {
                        "query": {
                            "name": "user",
                            "root": {
                                "nodes_where": {
                                    "predicate": {
                                        "eq": {
                                            "left": {"property": "username"},
                                            "right": {"constant": {"string": "alice"}},
                                        }
                                    }
                                }
                            },
                        }
                    },
                    {
                        "query": {
                            "name": "friends",
                            "root": {
                                "limit": {
                                    "input": {
                                        "dedup": {
                                            "input": {
                                                "out": {
                                                    "input": {"nodes": {"reference": {"var": "user"}}},
                                                    "label": "FOLLOWS",
                                                }
                                            }
                                        }
                                    },
                                    "count": {"literal": 100},
                                }
                            },
                        }
                    },
                ],
                "returns": ["user", "friends"],
            },
        )

        conditional = (
            read_batch()
            .var_as("user", g().n_with_label("User"))
            .var_as_if("posts", BatchCondition.var_not_empty("user"), g().n(NodeRef.var("user")).out("POSTED"))
        )
        self.assertEqual(parsed(conditional)["entries"][1]["query"]["condition"], {"var_not_empty": "user"})

        shortest_path = (
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
        self.assertEqual(
            parsed(shortest_path)["entries"][0]["query"]["root"],
            {
                "shortest_path": {
                    "source": {"ids": [1]},
                    "target": {"param": "target"},
                    "label": "FOLLOWS",
                    "direction": "both",
                    "max_depth": 5,
                }
            },
        )

    def test_writes_searches_indexes_and_nested_properties(self) -> None:
        write = (
            write_batch()
            .var_as("alice", g().add_n("User", {"name": "Alice", "tier": "pro"}))
            .var_as("linked", g().n(NodeRef.var("alice")).add_e("FOLLOWS", NodeRef.var("bob"), {"since": "2026-01-01"}).count())
            .returning(["alice", "linked"])
        )
        write_json = parsed(write)
        self.assertEqual(write_json["entries"][0]["query"]["root"]["add_n"]["label"], "User")
        self.assertEqual(
            write_json["entries"][0]["query"]["root"]["add_n"]["properties"][0],
            ["name", {"value": {"string": "Alice"}}],
        )

        vector = read_batch().var_as(
            "hits",
            g()
            .vector_search_nodes("Doc", "embedding", [1, 0, 0], 5)
            .project([PropertyProjection.renamed("$id", "doc_id")]),
        )
        self.assertEqual(
            parsed(vector)["entries"][0]["query"]["root"]["project"]["input"]["vector_search_nodes"],
            {
                "label": "Doc",
                "property": "embedding",
                "query_vector": {"value": {"f32_array": [1, 0, 0]}},
                "k": {"literal": 5},
            },
        )

        nested = (
            write_batch()
            .var_as(
                "updated",
                g()
                .add_n("User", {"metadata": {"externalID": "some_id", "score": 20}})
                .set_property("metadata", PropertyInput.param("metadata"))
                .value_map(["metadata.externalID"]),
            )
            .returning(["updated"])
        )
        root = parsed(nested)["entries"][0]["query"]["root"]
        self.assertEqual(root["value_map"]["properties"], ["metadata.externalID"])
        self.assertEqual(root["value_map"]["input"]["set_property"]["value"], {"expr": {"param": "metadata"}})

        index = write_batch().var_as(
            "idx",
            g().create_vector_index_nodes(
                "Doc", "embedding", 3, VectorDistanceMetric.COSINE, "tenant_id"
            ),
        )
        self.assertEqual(
            parsed(index)["entries"][0]["query"]["root"],
            {
                "create_index": {
                    "spec": {
                        "node_vector": {
                            "label": "Doc",
                            "property": "embedding",
                            "dimension": 3,
                            "metric": "cosine",
                            "tenant_property": "tenant_id",
                        }
                    },
                    "if_not_exists": True,
                }
            },
        )

        with self.assertRaises(ValueError):
            IndexSpec.node_vector("Doc", "embedding", 0, VectorDistanceMetric.COSINE)

    def test_repeat_projection_and_index_metadata(self) -> None:
        traversal = (
            g()
            .n([1, 2])
            .repeat(RepeatConfig.new(sub().out()).times(2))
            .union([sub().out("FOLLOWS")])
            .coalesce([sub().out("LIKES")])
            .optional(sub().out("POSTED"))
        )
        repeat_node = parsed(traversal)["root"]["optional"]["input"]["coalesce"]["input"]["union"]["input"]["repeat"]
        self.assertEqual(
            repeat_node,
            {
                "input": {"nodes": {"reference": {"ids": [1, 2]}}},
                "config": {
                    "traversal": {"root": {"out": {"input": "context"}}},
                    "times": 2,
                    "emit": "none",
                    "max_depth": 100,
                },
            },
        )
        self.assertEqual(
            parsed(Projection.expr("score_copy", Expr.prop("metadata.score"))),
            {"expr": {"alias": "score_copy", "expr": {"property": "metadata.score"}}},
        )
        self.assertEqual(
            parsed(IndexSpec.node_range("User", "age")),
            {"node_range": {"label": "User", "property": "age", "direction": "asc"}},
        )
        self.assertEqual(
            parsed(IndexSpec.node_range_with_direction("User", "age", RangeIndexDirection.ASC)),
            {"node_range": {"label": "User", "property": "age", "direction": "asc"}},
        )
        self.assertEqual(
            parsed(IndexSpec.edge_range_desc("FOLLOWS", "weight")),
            {"edge_range": {"label": "FOLLOWS", "property": "weight", "direction": "desc"}},
        )

    def test_query_requests(self) -> None:
        params = define_params({"tenant_id": param.string(), "limit": param.i64()})
        write_params = define_params({"data": param.array(param.object(param.value()))})

        def read_query(p):
            return (
                read_batch()
                .var_as(
                    "users",
                    g().n_with_label("User").where(Predicate.eq("tenantId", p.tenant_id)).limit(p.limit).value_map(["$id"]),
                )
                .returning(["users"])
            )

        def write_query(p):
            return (
                write_batch()
                .for_each_param("data", write_batch().var_as("created", g().add_n("User", {"payload": p.data})))
                .returning(["created"])
            )

        request = read_query(params).to_query_request(
            params,
            {"tenant_id": "acme", "limit": 25},
            query_name="read_query",
        )
        request_json = json.loads(request.to_json_string())
        self.assertEqual(request_json["request_type"], "read")
        self.assertEqual(request_json["query_name"], "read_query")
        self.assertEqual(set(request_json["query"]), {"read"})
        self.assertEqual(request_json["query"]["read"]["returns"], ["users"])
        self.assertEqual(request_json["parameters"], {"tenant_id": "acme", "limit": 25})
        self.assertEqual(request_json["parameter_types"]["limit"], "i64")

        write_request = write_query(write_params).to_query_request(
            write_params, {"data": [{"name": "Alice"}]}
        )
        write_request_json = json.loads(write_request.to_json_string())
        self.assertEqual(write_request_json["request_type"], "write")
        self.assertEqual(set(write_request_json["query"]), {"write"})
        self.assertEqual(write_request_json["query"]["write"]["returns"], ["created"])
        self.assertEqual(QueryValue.i64(9223372036854775807), 9223372036854775807)

        bytes_params = define_params({"payload": param.bytes()})
        with self.assertRaises(QueryError):
            read_batch().to_query_json(bytes_params, {"payload": b"abc"})
        with self.assertRaises(TypeError):
            read_batch().var_as("bad", g().add_n("User", {"name": "Alice"}))


if __name__ == "__main__":
    unittest.main()
