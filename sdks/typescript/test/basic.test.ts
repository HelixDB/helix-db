import assert from "node:assert/strict";
import {
  BatchCondition,
  BindingProjection,
  DateTime,
  EdgeRef,
  QueryError,
  QueryValue,
  Expr,
  GraphSelection,
  IndexSpec,
  NodeRef,
  Predicate,
  PropertyInput,
  PropertyProjection,
  PropertyValue,
  QueryParamType,
  RepeatConfig,
  ShortestPathDirection,
  SourcePredicate,
  VectorDistanceMetric,
  WhenThen,
  defineParams,
  g,
  param,
  parseIndexDdlReceipt,
  parseIndexOperationStatus,
  readBatch,
  stringifyJson,
  structuralJsonEqual,
  parseJson,
  sub,
  writeBatch,
} from "../src/index.js";

function parsed(value: unknown) {
  return JSON.parse(stringifyJson(value));
}

const operationId = "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001";
assert.equal(
  parseIndexDdlReceipt({ kind: "accepted", operation_id: operationId, index_id: "42", generation: "3", future: true }).kind,
  "accepted",
);
const parsedOperationStatus = parseIndexOperationStatus({
  status: "blocked",
  operation_id: operationId,
  index_id: "42",
  generation: "3",
  operation_kind: "build",
  family: "secondary",
  stage: "scan",
  attempt: 2,
  progress: { entities: "9", input_bytes: "10", output_operations: "11", output_bytes: "12", future: true },
  blocker_code: "uniqueness_violation",
  future: true,
});
assert.equal(parsedOperationStatus.status, "blocked");
for (const stage of ["await_upload", "validate_manifests"] as const) {
  const status = parseIndexOperationStatus({
    status: "queued",
    operation_id: operationId,
    index_id: "42",
    generation: "3",
    operation_kind: "build",
    family: "text",
    stage,
    attempt: 0,
    progress: { entities: "0", input_bytes: "0", output_operations: "0", output_bytes: "0" },
  });
  assert.equal(status.stage, stage);
}
assert.throws(() => parseIndexDdlReceipt({ kind: "future" }), TypeError);
assert.throws(
  () =>
    parseIndexOperationStatus({
      ...parsedOperationStatus,
      operation_id: operationId.toUpperCase(),
    }),
  TypeError,
);

assert.equal(structuralJsonEqual('{"n":9223372036854775807}', '{"n":9223372036854775807}'), true);
assert.equal(structuralJsonEqual('{"n":9223372036854775807}', '{"n":9223372036854775806}'), false);
assert.deepEqual(parseJson('{"n":9223372036854775807,"nested":[-9223372036854775808]}'), {
  n: 9223372036854775807n,
  nested: [-9223372036854775808n],
});

assert.deepEqual(parsed(PropertyValue.null()), "null");
assert.deepEqual(parsed(PropertyValue.bytes(new Uint8Array([1, 2]))), { bytes: [1, 2] });
assert.deepEqual(parsed(PropertyInput.param("limit")), { expr: { param: "limit" } });
assert.deepEqual(parsed(NodeRef.param("node_ids")), { param: "node_ids" });
assert.deepEqual(parsed(QueryParamType.array(QueryParamType.array(QueryParamType.f64()))), { array: { array: "f64" } });
assert.equal(PropertyValue.string("x").asStr(), "x");
assert.equal(PropertyValue.i64(1n).asI64(), 1n);
assert.equal(DateTime.parseRfc3339("1969-12-31T23:59:59.999-00:00").toRfc3339(), "1969-12-31T23:59:59.999Z");

assert.deepEqual(parsed(Expr.prop("a").add(Expr.val(1)).neg()), {
  neg: { expr: { add: { left: { property: "a" }, right: { constant: { i64: 1 } } } } },
});

assert.deepEqual(parsed(Predicate.eq("username", Expr.param("name"))), {
  eq: { left: { property: "username" }, right: { param: "name" } },
});
assert.deepEqual(parsed(Predicate.between("age", Expr.param("min_age"), 65)), {
  between: { value: { property: "age" }, min: { param: "min_age" }, max: { constant: { i64: 65 } } },
});
assert.deepEqual(parsed(SourcePredicate.or([SourcePredicate.hasKey("name"), SourcePredicate.startsWith("name", "A")])), {
  or: {
    predicates: [
      { has_key: { property: "name" } },
      { starts_with: { value: { property: "name" }, prefix: { constant: { string: "A" } } } },
    ],
  },
});

const rowBindingTraversal = g()
  .nWithLabel("Service")
  .bind("service")
  .optional(sub().in("CREATES").bind("deployment"))
  .union([sub().in("MANAGES").bind("owner"), sub().out("ROUTES_TO").bind("workload")])
  .projectDistinctBindings([
    BindingProjection.binding("service", "$id", "service_id"),
    BindingProjection.current("$id", "current_id"),
    BindingProjection.coalesce(
      [
        BindingProjection.bindingRef("deployment", "$id"),
        BindingProjection.bindingRef("owner", "$id"),
        BindingProjection.bindingRef("workload", "$id"),
      ],
      "workload_id",
    ),
  ]);
const rowBindingJson = parsed(rowBindingTraversal);
assert.equal(rowBindingTraversal.hasTerminal(), true);
assert.equal(rowBindingJson.root.project_bindings.distinct, true);
assert.deepEqual(rowBindingJson.root.project_bindings.projections[0], {
  property: { target: { binding: "service" }, source: "$id", alias: "service_id" },
});

const read = readBatch()
  .varAs("user", g().nWhere(SourcePredicate.eq("username", "alice")))
  .varAs("friends", g().n(NodeRef.var("user")).out("FOLLOWS").dedup().limit(100))
  .returning(["user", "friends"]);
assert.deepEqual(parsed(read), {
  entries: [
    {
      query: {
        name: "user",
        root: {
          nodes_where: { predicate: { eq: { left: { property: "username" }, right: { constant: { string: "alice" } } } } },
        },
      },
    },
    {
      query: {
        name: "friends",
        root: {
          limit: {
            input: {
              dedup: {
                input: {
                  out: {
                    input: { nodes: { reference: { var: "user" } } },
                    label: "FOLLOWS",
                  },
                },
              },
            },
            count: { literal: 100 },
          },
        },
      },
    },
  ],
  returns: ["user", "friends"],
});

const write = writeBatch()
  .varAs("alice", g().addN("User", { name: "Alice", tier: "pro" }))
  .varAs("bob", g().addN("User", [["name", "Bob"]]))
  .varAs("linked", g().n(NodeRef.var("alice")).addE("FOLLOWS", NodeRef.var("bob"), { since: "2026-01-01" }).count())
  .returning(["alice", "bob", "linked"]);
const writeJson = parsed(write);
assert.equal(writeJson.entries[0].query.root.add_n.label, "User");
assert.deepEqual(writeJson.entries[0].query.root.add_n.properties[0], ["name", { value: { string: "Alice" } }]);

const conditional = readBatch()
  .varAs("user", g().nWithLabel("User"))
  .varAsIf("posts", BatchCondition.varNotEmpty("user"), g().n(NodeRef.var("user")).out("POSTED"));
assert.deepEqual(parsed(conditional).entries[1].query.condition, { var_not_empty: "user" });

const shortestPath = readBatch()
  .varAs(
    "path",
    g().shortestPath(NodeRef.id(1n), NodeRef.param("target"), 5, {
      label: "FOLLOWS",
      direction: ShortestPathDirection.Both,
    }),
  )
  .returning(["path"]);
assert.deepEqual(parsed(shortestPath).entries[0].query.root, {
  shortest_path: {
    source: { ids: [1] },
    target: { param: "target" },
    label: "FOLLOWS",
    direction: "both",
    max_depth: 5,
  },
});

const graphSelection = new GraphSelection({
  nodeTraversal: g().nWhere(SourcePredicate.hasKey("$id")),
  edgeTraversal: g().eWhere(SourcePredicate.hasKey("$id")),
  direction: "directed",
  nodeProperties: ["path"],
  edgeProperties: ["line"],
  externalIdentityProperty: "external_id",
  graphifyEdgeKeyProperty: "key",
  weightProperty: "weight",
  maxNodes: 2,
  maxEdges: 3,
  allowFullScan: true,
});
const graphQuery = parsed(graphSelection.toQueryRequest());
assert.deepEqual(graphQuery.query.read.returns, ["nodes", "edges"]);
assert.equal(JSON.stringify(graphQuery).includes("__helix_graph_external_id"), true);
assert.equal(JSON.stringify(graphQuery).includes("__helix_graph_edge_source"), true);
assert.throws(
  () =>
    new GraphSelection({
      nodeTraversal: g().nWhere(SourcePredicate.hasKey("$id")),
      edgeTraversal: g().eWhere(SourcePredicate.hasKey("$id")),
      nodeProperties: ["__helix_graph_collision"],
    }),
  Error,
);
assert.throws(
  () =>
    new GraphSelection({
      nodeTraversal: g().n(NodeRef.all()).hasLabel("File"),
      edgeTraversal: g().e(EdgeRef.all()).hasLabel("DEPENDS_ON"),
    }),
  /allowFullScan/,
);

const vector = readBatch().varAs(
  "hits",
  g()
    .vectorSearchNodes("Doc", "embedding", [1, 0, 0], 5, null)
    .project([PropertyProjection.renamed("$id", "doc_id"), PropertyProjection.renamed("$distance", "score")]),
);
assert.deepEqual(parsed(vector).entries[0].query.root.project.input.vector_search_nodes, {
  label: "Doc",
  property: "embedding",
  query_vector: { value: { f32_array: [1, 0, 0] } },
  k: { literal: 5 },
});

const index = writeBatch().varAs("idx", g().createVectorIndexNodes("Doc", "embedding", 3, VectorDistanceMetric.Cosine, "tenant_id"));
assert.deepEqual(parsed(index).entries[0].query.root, {
  create_index: {
    spec: { node_vector: { label: "Doc", property: "embedding", dimension: 3, metric: "cosine", tenant_property: "tenant_id" } },
    if_not_exists: true,
  },
});
assert.throws(() => IndexSpec.nodeVector("Doc", "embedding", 0, VectorDistanceMetric.Cosine), TypeError);

const params = defineParams({
  tenant_id: param.string(),
  limit: param.i64(),
  created_after: param.dateTime(),
  labels: param.object(param.string()),
});

function readQuery(p: typeof params) {
  return readBatch()
    .varAs(
      "users",
      g()
        .nWithLabel("User")
        .where(Predicate.eqParam("tenantId", "tenant_id"))
        .where(Predicate.gteParam("created_at", "created_after"))
        .limit(p.limit)
        .valueMap(["$id", "name", "tenantId"]),
    )
    .returning(["users"]);
}

const writeParams = defineParams({
  data: param.array(param.object(param.value())),
});

function writeQuery(p: typeof writeParams) {
  return writeBatch()
    .forEachParam("data", writeBatch().varAs("created", g().addN("User", { name: PropertyInput.param("name"), payload: p.data })))
    .returning(["created"]);
}

const request = readQuery(params).toQueryRequest(
  params,
  {
    tenant_id: "acme",
    limit: 25n,
    created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
    labels: { status: "active" },
  },
  { queryName: "read_query" },
);
const requestJson = JSON.parse(request.toJsonString());
assert.equal(requestJson.request_type, "read");
assert.equal(requestJson.query_name, "read_query");
assert.deepEqual(Object.keys(requestJson.query), ["read"]);
assert.deepEqual(requestJson.query.read.returns, ["users"]);
assert.deepEqual(requestJson.parameters, {
  tenant_id: "acme",
  limit: 25,
  created_after: "2026-04-05T10:34:56.789Z",
  labels: { status: "active" },
});
assert.deepEqual(requestJson.parameter_types.limit, "i64");
const writeRequestJson = JSON.parse(writeQuery(writeParams).toQueryJson(writeParams, { data: [{ name: "Alice" }] }));
assert.deepEqual(Object.keys(writeRequestJson.query), ["write"]);
assert.deepEqual(writeRequestJson.query.write.returns, ["created"]);
assert.deepEqual(JSON.parse(readQuery(params).toJsonString()).entries[0].query.name, "users");
assert.equal(writeQuery(writeParams).toQueryBytes(writeParams, { data: [{ name: "Alice" }] }) instanceof Uint8Array, true);
assert.equal(readBatch().varAs("count", g().nWithLabel("User").count()).toQueryJson().includes('"request_type":"read"'), true);
assert.equal(write.toJsonString(), stringifyJson(write));
assert.equal(write.toJsonBytes() instanceof Uint8Array, true);
assert.throws(
  () =>
    readQuery(params).toQueryJson(params, {
      tenant_id: "acme",
      limit: 25n,
      created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
      labels: { status: 1 },
    } as never),
  TypeError,
);

const bytesParams = defineParams({ payload: param.bytes() });
assert.throws(() => readBatch().toQueryJson(bytesParams, { payload: new Uint8Array([1, 2, 3]) }), QueryError);

assert.equal(stringifyJson(PropertyValue.i64(9223372036854775807n)), '{"i64":9223372036854775807}');
assert.equal(stringifyJson(QueryValue.i64(9223372036854775807n)), "9223372036854775807");

assert.deepEqual(parsed(Expr.case([WhenThen(Predicate.isNotNull("email"), Expr.prop("email"))], Expr.val("missing"))), {
  case: {
    when_then: [{ when: { is_not_null: { property: "email" } }, then: { property: "email" } }],
    else_expr: { constant: { string: "missing" } },
  },
});

assert.deepEqual(
  parsed(
    g()
      .n([1n, 2n])
      .repeat(RepeatConfig.new(sub().out()).times(2))
      .union([sub().out("FOLLOWS")])
      .coalesce([sub().out("LIKES")])
      .optional(sub().out("POSTED")),
  ).root.optional.input.coalesce.input.union.input.repeat,
  {
    input: { nodes: { reference: { ids: [1, 2] } } },
    config: {
      traversal: { root: { out: { input: "context" } } },
      times: 2,
      emit: "none",
      max_depth: 100,
    },
  },
);
