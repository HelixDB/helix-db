import assert from "node:assert/strict";
import {
  BatchCondition,
  BindingProjection,
  DateTime,
  DynamicQueryError,
  DynamicQueryValue,
  EdgeRef,
  Expr,
  GenerateError,
  IndexSpec,
  NodeRef,
  Order,
  Predicate,
  PropertyInput,
  PropertyProjection,
  PropertyValue,
  Projection,
  QueryParamType,
  RepeatConfig,
  SourcePredicate,
  Step,
  StreamBound,
  LEGACY_QUERY_BUNDLE_VERSION_V4,
  QUERY_BUNDLE_VERSION,
  deserializeQueryBundle,
  defineParams,
  defineQueries,
  g,
  param,
  readBatch,
  registerRead,
  registerWrite,
  serializeQueryBundle,
  stringifyJson,
  structuralJsonEqual,
  sub,
  writeBatch,
} from "../src/index.js";

function parsed(value: unknown) {
  return JSON.parse(stringifyJson(value));
}

assert.equal(structuralJsonEqual('{"n":9223372036854775807}', '{"n":9223372036854775807}'), true);
assert.equal(structuralJsonEqual('{"n":9223372036854775807}', '{"n":9223372036854775806}'), false);
assert.equal(structuralJsonEqual('{"n":9223372036854775807}', '{"n":"9223372036854775807"}'), false);

const values = [
  PropertyValue.null(),
  PropertyValue.bool(true),
  PropertyValue.i64(1n),
  PropertyValue.dateTime(DateTime.fromMillis(-1)),
  PropertyValue.f64(1.5),
  PropertyValue.f32(1.25),
  PropertyValue.string("x"),
  PropertyValue.bytes(new Uint8Array([1, 2])),
  PropertyValue.i64Array([1, 2n]),
  PropertyValue.f64Array([1.5]),
  PropertyValue.f32Array([1.25]),
  PropertyValue.stringArray(["a"]),
  PropertyValue.array(["a", 1]),
  PropertyValue.object({ nested: true }),
].map((value) => parsed(value));
assert.deepEqual(values[0], "Null");
assert.deepEqual(values[7], { Bytes: [1, 2] });
assert.equal(PropertyValue.string("x").asStr(), "x");
assert.equal(PropertyValue.i64(1n).asI64(), 1n);
assert.equal(PropertyValue.datetimeMillis(-1).asDatetimeMillis(), -1);
assert.equal(PropertyValue.f32(1.25).asF64(), 1.25);
assert.equal(PropertyValue.bool(true).asBool(), true);
assert.equal(PropertyValue.array(["a"]).asArray()?.length, 1);
assert.equal(PropertyValue.object({ a: 1 }).asObject()?.a.asI64(), 1);

assert.equal(DateTime.fromMillis(-1).millis(), -1n);
assert.equal(DateTime.parseRfc3339("1969-12-31T23:59:59.999-00:00").toRfc3339(), "1969-12-31T23:59:59.999Z");

assert.deepEqual(parsed(PropertyInput.expr(Expr.id())), { Expr: "Id" });
assert.deepEqual(parsed(PropertyInput.param("limit")), { Expr: { Param: "limit" } });

assert.deepEqual(parsed(NodeRef.all()), "All");
assert.deepEqual(parsed(NodeRef.param("node_ids")), { Param: "node_ids" });
assert.deepEqual(parsed(EdgeRef.param("edge_ids")), { Param: "edge_ids" });
assert.deepEqual(parsed(StreamBound.from(-1)), { Expr: { Constant: { I64: -1 } } });

assert.deepEqual(parsed(Expr.prop("a").add(Expr.val(1)).sub(Expr.val(2)).mul(Expr.val(3)).div(Expr.val(4)).modulo(Expr.val(5)).neg()), {
  Neg: {
    Mod: [
      {
        Div: [
          { Mul: [{ Sub: [{ Add: [{ Property: "a" }, { Constant: { I64: 1 } }] }, { Constant: { I64: 2 } }] }, { Constant: { I64: 3 } }] },
          { Constant: { I64: 4 } },
        ],
      },
      { Constant: { I64: 5 } },
    ],
  },
});

assert.deepEqual(
  parsed(
    Predicate.and([
      Predicate.neq("a", 1),
      Predicate.gt("b", 2),
      Predicate.lt("c", 3),
      Predicate.lte("d", 4),
      Predicate.between("e", 1, 5),
      Predicate.startsWith("f", "pre"),
      Predicate.endsWith("g", "post"),
      Predicate.containsParam("h", "needle"),
      Predicate.isIn("i", [1, 2]),
      Predicate.isInExpr("j", Expr.param("list")),
      Predicate.isInParam("k", "list"),
      Predicate.or([Predicate.hasKey("l")]),
      Predicate.not(Predicate.isNull("m")),
    ]),
  ),
  {
    And: [
      { Neq: ["a", { I64: 1 }] },
      { Gt: ["b", { I64: 2 }] },
      { Lt: ["c", { I64: 3 }] },
      { Lte: ["d", { I64: 4 }] },
      { Between: ["e", { I64: 1 }, { I64: 5 }] },
      { StartsWith: ["f", "pre"] },
      { EndsWith: ["g", "post"] },
      { ContainsExpr: ["h", { Param: "needle" }] },
      { IsIn: ["i", { I64Array: [1, 2] }] },
      { IsInExpr: ["j", { Param: "list" }] },
      { IsInExpr: ["k", { Param: "list" }] },
      { Or: [{ HasKey: "l" }] },
      { Not: { IsNull: "m" } },
    ],
  },
);

assert.deepEqual(parsed(Predicate.eq("param_field", Expr.param("param_value"))), { EqExpr: ["param_field", { Param: "param_value" }] });
assert.deepEqual(parsed(Predicate.gteParam("created_at", "created_after")), { GteExpr: ["created_at", { Param: "created_after" }] });
assert.deepEqual(parsed(Predicate.between("age", Expr.param("min_age"), 65)), {
  BetweenExpr: ["age", { Param: "min_age" }, { Constant: { I64: 65 } }],
});

assert.deepEqual(parsed(SourcePredicate.or([SourcePredicate.hasKey("name"), SourcePredicate.startsWith("name", "A")]).toPredicate()), {
  Or: [{ HasKey: "name" }, { StartsWith: ["name", "A"] }],
});

// SourcePredicate params: literals keep the existing variant (JSON unchanged), Expr/params route to *Expr.
assert.deepEqual(parsed(SourcePredicate.eq("username", "alice")), { Eq: ["username", { String: "alice" }] });
assert.deepEqual(parsed(SourcePredicate.gt("score", 10)), { Gt: ["score", { I64: 10 }] });
assert.deepEqual(parsed(SourcePredicate.between("age", 18, 65)), { Between: ["age", { I64: 18 }, { I64: 65 }] });
assert.deepEqual(parsed(SourcePredicate.eq("username", Expr.param("name"))), { EqExpr: ["username", { Param: "name" }] });
assert.deepEqual(parsed(SourcePredicate.lte("score", Expr.param("max"))), { LteExpr: ["score", { Param: "max" }] });
assert.deepEqual(parsed(SourcePredicate.between("age", Expr.param("lo"), 65)), {
  BetweenExpr: ["age", { Param: "lo" }, { Constant: { I64: 65 } }],
});
// toPredicate() preserves the *Expr variant shape.
assert.deepEqual(parsed(SourcePredicate.eq("username", Expr.param("name")).toPredicate()), {
  EqExpr: ["username", { Param: "name" }],
});

assert.deepEqual(parsed(Projection.property("name", "display_name")), { source: "name", alias: "display_name" });
assert.deepEqual(parsed(Projection.fromEndpoint("resource_id", "from_id")), { source: "$from.resource_id", alias: "from_id" });
assert.deepEqual(parsed(Projection.toEndpoint("resource_id", "to_id")), { source: "$to.resource_id", alias: "to_id" });
assert.deepEqual(parsed(Projection.expr("age2", Expr.prop("age").add(Expr.val(1)))), {
  alias: "age2",
  expr: { Add: [{ Property: "age" }, { Constant: { I64: 1 } }] },
});
assert.deepEqual(parsed(Step.bind("service")), { Bind: "service" });
assert.deepEqual(
  parsed(
    Step.projectBindings(
      [
        BindingProjection.binding("service", "$id", "service_id"),
        BindingProjection.coalesce(
          [BindingProjection.bindingRef("deployment", "$id"), BindingProjection.bindingRef("owner", "$id")],
          "workload_id",
        ),
      ],
      true,
    ),
  ),
  {
    ProjectBindings: {
      projections: [
        { kind: "Property", target: { Binding: "service" }, source: "$id", alias: "service_id" },
        {
          kind: "Coalesce",
          refs: [
            { target: { Binding: "deployment" }, source: "$id" },
            { target: { Binding: "owner" }, source: "$id" },
          ],
          alias: "workload_id",
        },
      ],
      distinct: true,
    },
  },
);

const rowBindingTraversal = g()
  .nWithLabel("Service")
  .bind("service")
  .optional(sub().in("CREATES").bind("deployment"))
  .union([sub().in("MANAGES").bind("owner"), sub().out("ROUTES_TO").bind("workload")])
  .projectDistinctBindings([
    BindingProjection.binding("service", "$id", "service_id"),
    BindingProjection.current("$id", "current_id"),
    BindingProjection.binding("missing_binding", "externalId", "missing_external_id"),
    BindingProjection.coalesce(
      [
        BindingProjection.bindingRef("deployment", "$id"),
        BindingProjection.bindingRef("owner", "$id"),
        BindingProjection.bindingRef("workload", "$id"),
      ],
      "workload_id",
    ),
  ]);
assert.equal(rowBindingTraversal.hasTerminal(), true);
assert.deepEqual(parsed(rowBindingTraversal).steps[1], { Bind: "service" });
assert.equal(parsed(rowBindingTraversal).steps.at(-1).ProjectBindings.distinct, true);

const legacyBundle = JSON.stringify({
  version: LEGACY_QUERY_BUNDLE_VERSION_V4,
  read_routes: {},
  write_routes: {},
  read_parameters: {},
  write_parameters: {},
});
assert.equal((deserializeQueryBundle(legacyBundle) as { version: number }).version, LEGACY_QUERY_BUNDLE_VERSION_V4);
assert.equal(QUERY_BUNDLE_VERSION, 5);

const read = readBatch()
  .varAs("user", g().nWhere(SourcePredicate.eq("username", "alice")))
  .varAs("friends", g().n(NodeRef.var("user")).out("FOLLOWS").dedup().limit(100))
  .returning(["user", "friends"]);

assert.deepEqual(parsed(read), {
  queries: [
    {
      Query: {
        name: "user",
        steps: [{ NWhere: { Eq: ["username", { String: "alice" }] } }],
        condition: null,
      },
    },
    {
      Query: {
        name: "friends",
        steps: [{ N: { Var: "user" } }, { Out: "FOLLOWS" }, "Dedup", { Limit: 100 }],
        condition: null,
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
assert.equal(writeJson.queries[0].Query.steps[0].AddN.label, "User");
assert.deepEqual(writeJson.queries[0].Query.steps[0].AddN.properties[0], ["name", { Value: { String: "Alice" } }]);

const conditional = readBatch()
  .varAs("user", g().nWithLabel("User"))
  .varAsIf("posts", BatchCondition.varNotEmpty("user"), g().n(NodeRef.var("user")).out("POSTED"));
assert.deepEqual(parsed(conditional).queries[1].Query.condition, { VarNotEmpty: "user" });

const vector = readBatch().varAs(
  "hits",
  g()
    .vectorSearchNodes("Doc", "embedding", [1, 0, 0], 5, null)
    .project([PropertyProjection.renamed("$id", "doc_id"), PropertyProjection.renamed("$distance", "score")]),
);
assert.deepEqual(parsed(vector).queries[0].Query.steps[0], {
  VectorSearchNodes: {
    label: "Doc",
    property: "embedding",
    query_vector: { Value: { F32Array: [1, 0, 0] } },
    k: { Literal: 5 },
  },
});

const index = writeBatch().varAs("idx", g().createVectorIndexNodes("Doc", "embedding", "tenant_id"));
assert.deepEqual(parsed(index).queries[0].Query.steps[0], {
  CreateIndex: {
    spec: { NodeVector: { label: "Doc", property: "embedding", tenant_property: "tenant_id" } },
    if_not_exists: true,
  },
});

const params = defineParams({
  tenant_id: param.string(),
  limit: param.i64(),
  created_after: param.dateTime(),
  labels: param.object(param.string()),
});

function registeredRead(p: typeof params) {
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

function registeredWrite(p: typeof writeParams) {
  return writeBatch()
    .forEachParam("data", writeBatch().varAs("created", g().addN("User", { name: PropertyInput.param("name"), payload: p.data })))
    .returning(["created"]);
}

const queries = defineQueries({
  read: { registered_read: registerRead(registeredRead, params) },
  write: { registered_write: registerWrite(registeredWrite, writeParams) },
});

const bundle = JSON.parse(serializeQueryBundle(queries.buildQueryBundle()));
assert.equal(bundle.version, QUERY_BUNDLE_VERSION);
assert.deepEqual(bundle.read_parameters.registered_read, [
  { name: "tenant_id", ty: "String" },
  { name: "limit", ty: "I64" },
  { name: "created_after", ty: "DateTime" },
  { name: "labels", ty: "Object" },
]);
assert.deepEqual(bundle.write_parameters.registered_write, [{ name: "data", ty: { Array: "Object" } }]);

const request = queries.call.registered_read({
  tenant_id: "acme",
  limit: 25n,
  created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
  labels: { status: "active" },
});
const requestJson = JSON.parse(request.toJsonString());
assert.equal(requestJson.request_type, "read");
assert.equal(requestJson.query_name, "registered_read");
assert.deepEqual(requestJson.parameters, {
  tenant_id: "acme",
  limit: 25,
  created_after: "2026-04-05T10:34:56.789Z",
  labels: { status: "active" },
});
assert.deepEqual(requestJson.parameter_types.limit, "I64");
assert.deepEqual(JSON.parse(registeredRead(params).toJsonString()).queries[0].Query.name, "users");
assert.deepEqual(
  JSON.parse(
    registeredRead(params).toDynamicJson(
      params,
      {
        tenant_id: "acme",
        limit: 25n,
        created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
        labels: { status: "active" },
      },
      { queryName: "registered_read" },
    ),
  ),
  requestJson,
);
const directRequest = registeredRead(params).toDynamicRequest(params, {
  tenant_id: "acme",
  limit: 25n,
  created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
  labels: { status: "active" },
});
assert.equal(directRequest.requestType, "read");
assert.equal(directRequest.queryName, null);
assert.equal(
  registeredRead(params).toDynamicRequest(
    params,
    {
      tenant_id: "acme",
      limit: 25n,
      created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
      labels: { status: "active" },
    },
    { queryName: "direct_registered_read" },
  ).queryName,
  "direct_registered_read",
);
assert.equal(registeredWrite(writeParams).toDynamicBytes(writeParams, { data: [{ name: "Alice" }] }) instanceof Uint8Array, true);
assert.equal(readBatch().varAs("count", g().nWithLabel("User").count()).toDynamicJson().includes('"request_type":"read"'), true);
assert.equal(readBatch().varAs("count", g().nWithLabel("User").count()).toDynamicJson().includes('"query_name":null'), true);
assert.equal(
  readBatch()
    .varAs("count", g().nWithLabel("User").count())
    .toDynamicJson({ queryName: "count_users" })
    .includes('"query_name":"count_users"'),
  true,
);
assert.equal(write.toJsonString(), stringifyJson(write));
assert.equal(write.toJsonBytes() instanceof Uint8Array, true);
assert.throws(() => registeredRead(params).toDynamicJson(params as never, undefined as never));
assert.throws(() =>
  queries.call.registered_read({
    tenant_id: "acme",
    limit: 25n,
    created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
    labels: { status: 1 },
  } as never),
);
assert.throws(() =>
  queries.call.registered_read({
    tenant_id: "acme",
    limit: 25n,
    created_after: DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00"),
    labels: { status: "active" },
    unexpected: true,
  } as never),
);
assert.throws(
  () =>
    defineQueries({
      read: { duplicate: registerRead(registeredRead, params) },
      write: { duplicate: registerWrite(registeredWrite, writeParams) },
    }),
  GenerateError,
);

const bytesParams = defineParams({ payload: param.bytes() });
const bytesQueries = defineQueries({ read: { bytes_route: registerRead(() => readBatch(), bytesParams) } });
assert.throws(() => bytesQueries.call.bytes_route({ payload: new Uint8Array([1, 2, 3]) }), DynamicQueryError);
assert.throws(() => readBatch().toDynamicJson(bytesParams, { payload: new Uint8Array([1, 2, 3]) }), DynamicQueryError);

const large = stringifyJson(PropertyValue.i64(9223372036854775807n));
assert.equal(large, '{"I64":9223372036854775807}');
assert.equal(stringifyJson(DynamicQueryValue.i64(9223372036854775807n)), "9223372036854775807");

assert.deepEqual(parsed(Expr.case([[Predicate.isNotNull("email"), Expr.prop("email")]], Expr.val("missing"))), {
  Case: {
    when_then: [[{ IsNotNull: "email" }, { Property: "email" }]],
    else_expr: { Constant: { String: "missing" } },
  },
});

assert.deepEqual(parsed(QueryParamType.array(QueryParamType.array(QueryParamType.f64()))), { Array: { Array: "F64" } });
assert.deepEqual(
  parsed(
    g()
      .n([1n, 2n])
      .repeat(RepeatConfig.new(sub().out()).times(2))
      .union([sub().out("FOLLOWS")])
      .coalesce([sub().out("LIKES")])
      .optional(sub().out("POSTED")),
  ),
  {
    steps: [
      { N: { Ids: [1, 2] } },
      { Repeat: { traversal: { steps: [{ Out: null }] }, times: 2, until: null, emit: "None", emit_predicate: null, max_depth: 100 } },
      { Union: [{ steps: [{ Out: "FOLLOWS" }] }] },
      { Coalesce: [{ steps: [{ Out: "LIKES" }] }] },
      { Optional: { steps: [{ Out: "POSTED" }] } },
    ],
  },
);

const rawSteps = [
  Step.e(EdgeRef.var("edges")),
  Step.eWhere(SourcePredicate.gt("weight", 0.5)),
  Step.vectorSearchEdges("Rel", "embedding", PropertyInput.value(PropertyValue.f32Array([0.1, 0.2])), StreamBound.literal(5)),
  Step.textSearchNodes("Doc", "body", PropertyInput.param("q"), StreamBound.expr(Expr.param("k"))),
  Step.textSearchEdges("Rel", "body", PropertyInput.param("q"), StreamBound.expr(Expr.param("k"))),
  Step.in("FOLLOWS"),
  Step.both("FOLLOWS"),
  Step.outE("FOLLOWS"),
  Step.inE("FOLLOWS"),
  Step.bothE("FOLLOWS"),
  Step.outN(),
  Step.inN(),
  Step.otherN(),
  Step.has("name", "Alice"),
  Step.hasLabel("User"),
  Step.hasKey("email"),
  Step.where(Predicate.contains("name", "A")),
  Step.edgeHas("weight", PropertyInput.value(1)),
  Step.edgeHasLabel("FOLLOWS"),
  Step.skip(StreamBound.expr(Expr.param("skip"))),
  Step.range(StreamBound.literal(1), StreamBound.expr(Expr.param("end"))),
  Step.as("x"),
  Step.store("x"),
  Step.select("x"),
  Step.values(["name"]),
  Step.valueMap(null),
  Step.edgeProperties(),
  Step.createVectorIndexNodes("Doc", "embedding", "tenant_id"),
  Step.createVectorIndexEdges("Rel", "embedding", "tenant_id"),
  Step.createTextIndexNodes("Doc", "body", "tenant_id"),
  Step.createTextIndexEdges("Rel", "body", "tenant_id"),
  Step.setProperty("name", PropertyInput.value("Alice")),
  Step.removeProperty("name"),
  Step.drop(),
  Step.dropEdge(NodeRef.var("target")),
  Step.dropEdgeLabeled(NodeRef.var("target"), "FOLLOWS"),
  Step.dropEdgeById(EdgeRef.id(1)),
  Step.orderBy("name", Order.Asc),
  Step.orderByMultiple([["name", Order.Asc]]),
  Step.choose(Predicate.hasKey("email"), sub().out(), null),
  Step.group("status"),
  Step.groupCount("status"),
  Step.fold(),
  Step.unfold(),
  Step.path(),
  Step.simplePath(),
  Step.withSack(0),
  Step.sackSet("score"),
  Step.sackAdd("score"),
  Step.sackGet(),
  Step.inject("x"),
];
assert.equal(rawSteps.length > 40, true);
const rawStepJson = rawSteps.map((step) => parsed(step));
assert.deepEqual(rawStepJson[27], { CreateVectorIndexNodes: { label: "Doc", property: "embedding", tenant_property: "tenant_id" } });
assert.deepEqual(rawStepJson.at(-1), { Inject: "x" });

void IndexSpec;

// ── StreamBound.literal ─────────────────────────────────────────

// Upper bound: bigint above MAX_SAFE_INTEGER should throw
assert.throws(
  () => StreamBound.literal(BigInt(Number.MAX_SAFE_INTEGER) + 1n),
  TypeError,
  "should reject bigint above MAX_SAFE_INTEGER",
);

// Lower bound: bigint below MIN_SAFE_INTEGER should throw
assert.throws(
  () => StreamBound.literal(BigInt(Number.MIN_SAFE_INTEGER) - 1n),
  TypeError,
  "should reject bigint below MIN_SAFE_INTEGER",
);

// Boundary: exact MAX_SAFE_INTEGER as bigint should NOT throw
assert.doesNotThrow(
  () => StreamBound.literal(BigInt(Number.MAX_SAFE_INTEGER)),
  "should accept bigint equal to MAX_SAFE_INTEGER",
);

// Boundary: exact MIN_SAFE_INTEGER as bigint should NOT throw
assert.doesNotThrow(
  () => StreamBound.literal(BigInt(Number.MIN_SAFE_INTEGER)),
  "should accept bigint equal to MIN_SAFE_INTEGER",
);

// Regular number should NOT throw
assert.doesNotThrow(
  () => StreamBound.literal(42),
  "should accept regular numbers",
);

console.log("All StreamBound tests passed.");
