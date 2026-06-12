from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from helixdb import (
    BatchCondition,
    DateTime,
    DynamicQueryRequest,
    DynamicQueryValue,
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
    RangeIndexDirection,
    RepeatConfig,
    SourcePredicate,
    Step,
    StreamBound,
    Traversal,
    define_params,
    define_queries,
    g,
    param,
    read_batch,
    register_read,
    register_write,
    serialize_query_bundle,
    structural_json_equal,
    sub,
    write_batch,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "sdks" / "tests" / "parity" / "generated" / "typescript"


def fixture(bucket: str, name: str) -> str:
    return (FIXTURES / bucket / name).read_text(encoding="utf-8")


class DslParityTests(unittest.TestCase):
    def assert_json_equal(self, actual: str, expected: str) -> None:
        self.assertTrue(structural_json_equal(actual, expected), f"\nactual:   {actual}\nexpected: {expected}")

    def test_read_count_matches_typescript_fixture(self) -> None:
        request = DynamicQueryRequest.read(
            read_batch()
            .var_as("user_count", g().n_with_label("ParityUser").count())
            .returning(["user_count"])
        )

        self.assertEqual(
            request.to_json_string(),
            fixture("runtime", "002-read-count-all-users.json"),
        )

    def test_projection_expr_case_matches_typescript_fixture(self) -> None:
        request = DynamicQueryRequest.read(
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
                            Expr.prop("score").add(Expr.val(PropertyValue.f64(1.0))),
                        ),
                        Projection.expr(
                            "status_label",
                            Expr.case(
                                [(Predicate.eq("status", "active"), Expr.val("enabled"))],
                                Expr.val("disabled"),
                            ),
                        ),
                    ]
                ),
            )
            .returning(["alice"])
        )

        self.assert_json_equal(
            request.to_json_string(),
            fixture("runtime", "004-read-value-map-projection.json"),
        )

    def test_repeat_union_matches_typescript_fixture(self) -> None:
        request = DynamicQueryRequest.read(
            read_batch()
            .var_as(
                "walked",
                g()
                .n_with_label("ParityUser")
                .where(Predicate.eq("externalId", "user-alice"))
                .repeat(RepeatConfig.new(sub().out("FOLLOWS")).times(2).emit_all().max_depth(4))
                .union([sub().out("FOLLOWS"), sub().in_("FOLLOWS")])
                .dedup()
                .value_map(["externalId", "name"]),
            )
            .returning(["walked"])
        )

        self.assert_json_equal(
            request.to_json_string(),
            fixture("runtime", "017-read-repeat-union.json"),
        )

    def test_dynamic_params_match_typescript_fixture(self) -> None:
        params = define_params(
            {
                "statuses": param.array(param.string()),
                "created_after": param.date_time(),
                "limit": param.i64(),
            }
        )

        query = (
            read_batch()
            .var_as(
                "matches",
                g()
                .n_with_label("ParityUser")
                .where(Predicate.is_in_expr("status", params.statuses))
                .where(Predicate.gte("createdAt", params.created_after))
                .limit(params.limit)
                .value_map(["externalId", "status"]),
            )
            .returning(["matches"])
        )

        actual = query.to_dynamic_json(
            params,
            {
                "statuses": ["active", "inactive"],
                "created_after": DateTime.parse_rfc3339("2026-01-01T00:00:00Z"),
                "limit": 5,
            },
        )

        self.assertEqual(actual, fixture("runtime", "021-read-parameter-types.json"))

    def test_raw_read_step_shapes_match_typescript_fixture(self) -> None:
        request = DynamicQueryRequest.read(
            read_batch()
            .var_as(
                "raw_nodes",
                g()
                .n(NodeRef.param("node_ids"))
                .has("name", "Alice")
                .where(Predicate.contains_expr("bio", Expr.param("needle")))
                .limit(Expr.param("limit"))
                .skip(Expr.param("skip"))
                .range(StreamBound.literal(0), StreamBound.expr(Expr.param("end")))
                .as_("a")
                .store("stored")
                .select("stored")
                .dedup()
                .within("stored")
                .without("missing")
                .fold()
                .unfold()
                .path()
                .simple_path()
                .with_sack(0)
                .sack_set("score")
                .sack_add("score")
                .sack_get()
                .project(
                    [
                        Projection.property("externalId"),
                        Projection.expr("neg_age", Expr.prop("age").neg()),
                    ]
                ),
            )
            .var_as(
                "raw_edges",
                g()
                .e(EdgeRef.param("edge_ids"))
                .e_where(
                    SourcePredicate.or_(
                        [SourcePredicate.has_key("since"), SourcePredicate.starts_with("note", "Alice")]
                    )
                )
                .out_n()
                .in_n()
                .other_n()
                .edge_has("weight", PropertyInput.value(PropertyValue.f64(1.0)))
                .edge_has_label("FOLLOWS")
                .order_by("weight", Order.DESC)
                .edge_properties(),
            )
            .returning(["raw_nodes", "raw_edges"])
        )
        for name, value in {
            "node_ids": [1, 2],
            "edge_ids": [1],
            "needle": "graph",
            "limit": 10,
            "skip": 0,
            "end": 10,
        }.items():
            request.insert_parameter_value(name, value)
        for name, ty in {
            "node_ids": QueryParamType.array(QueryParamType.i64()),
            "edge_ids": QueryParamType.array(QueryParamType.i64()),
            "needle": QueryParamType.string(),
            "limit": QueryParamType.i64(),
            "skip": QueryParamType.i64(),
            "end": QueryParamType.i64(),
        }.items():
            request.insert_parameter_type(name, ty)

        self.assert_json_equal(
            request.to_json_string(),
            fixture("json-only", "900-exhaustive-raw-read-steps.json"),
        )

    def test_raw_write_step_shapes_match_typescript_fixture(self) -> None:
        request = DynamicQueryRequest.write(
            write_batch()
            .var_as(
                "raw_indexes",
                Traversal.from_steps(
                    [
                        Step.create_index(IndexSpec.node_unique_equality("ParityUser", "externalId"), True),
                        Step.drop_index(IndexSpec.node_range("ParityUser", "age")),
                        Step.create_vector_index_nodes("ParityUser", "embedding", "tenantId"),
                        Step.create_vector_index_edges("FOLLOWS", "embedding", "tenantId"),
                        Step.create_text_index_nodes("ParityUser", "bio", "tenantId"),
                        Step.create_text_index_edges("FOLLOWS", "note", "tenantId"),
                    ],
                    state="terminal",
                    mode="write",
                ),
            )
            .var_as(
                "raw_mutations",
                g()
                .add_n("RawNode", {"name": "raw"})
                .add_e("RAW_EDGE", NodeRef.var("raw_mutations"), {"weight": 1})
                .set_property("name", PropertyInput.param("name"))
                .remove_property("old")
                .drop_edge(NodeRef.id(999999))
                .drop_edge_labeled(NodeRef.id(999999), "RAW_EDGE")
                .drop_edge_by_id(EdgeRef.id(999999))
                .drop(),
            )
            .returning(["raw_indexes", "raw_mutations"])
        )

        self.assert_json_equal(
            request.to_json_string(),
            fixture("json-only", "901-exhaustive-raw-write-steps.json"),
        )

    def test_range_index_direction_serialization(self) -> None:
        self.assertEqual(
            IndexSpec.node_range("User", "age").to_json(),
            {"NodeRange": {"label": "User", "property": "age"}},
        )
        self.assertEqual(
            IndexSpec.node_range_with_direction("User", "age", RangeIndexDirection.ASC).to_json(),
            {"NodeRange": {"label": "User", "property": "age"}},
        )
        self.assertEqual(
            IndexSpec.node_range_desc("User", "age").to_json(),
            {"NodeRange": {"label": "User", "property": "age", "direction": "Desc"}},
        )
        self.assertEqual(
            IndexSpec.edge_range_desc("FOLLOWS", "weight").to_json(),
            {"EdgeRange": {"label": "FOLLOWS", "property": "weight", "direction": "Desc"}},
        )

    def test_value_params_reject_bytes_for_dynamic_json(self) -> None:
        params = define_params({"payload": param.value()})
        query = read_batch().returning([])
        with self.assertRaises(ValueError):
            query.to_dynamic_json(params, {"payload": b"abc"})

    def test_read_batch_rejects_write_traversal(self) -> None:
        with self.assertRaises(TypeError):
            read_batch().var_as("bad", g().add_n("User", {"name": "Alice"}))

    def test_define_queries_builds_callable_bundle_and_request(self) -> None:
        params = define_params({"tenant_id": param.string()})
        queries = define_queries(
            {
                "read": {
                    "find_users": register_read(
                        lambda p: read_batch()
                        .var_as(
                            "users",
                            g()
                            .n_with_label("User")
                            .where(Predicate.eq("tenantId", p.tenant_id)),
                        )
                        .returning(["users"]),
                        params,
                    )
                },
                "write": {
                    "add_user": register_write(
                        lambda p: write_batch()
                        .var_as("user", g().add_n("User", {"tenantId": p.tenant_id}))
                        .returning(["user"]),
                        params,
                    )
                },
            }
        )

        request = queries.call.find_users({"tenant_id": "acme"})
        self.assertEqual(request.query_name, "find_users")
        self.assertEqual(request.parameters, {"tenant_id": "acme"})
        self.assertEqual(request.parameter_types, {"tenant_id": QueryParamType.string()})

        bundle = queries.build_query_bundle()
        serialized = serialize_query_bundle(bundle)
        parsed = json.loads(serialized)
        self.assertEqual(parsed["version"], 4)
        self.assertEqual(list(parsed["read_routes"]), ["find_users"])
        self.assertEqual(list(parsed["write_routes"]), ["add_user"])

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "queries.json"
            self.assertEqual(queries.generate(path), str(path))
            self.assertTrue(path.exists())

    def test_dynamic_query_value_helpers(self) -> None:
        self.assertEqual(DynamicQueryValue.i64(9223372036854775807), 9223372036854775807)
        self.assertEqual(DynamicQueryValue.array([1, "two"]), [1, "two"])


if __name__ == "__main__":
    unittest.main()
