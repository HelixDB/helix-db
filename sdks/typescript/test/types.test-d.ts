import {
  BindingProjection,
  Client,
  CypherExecutionRequest,
  CypherExplanation,
  CypherResponse,
  DateTime,
  ParamSchema,
  QueryParamType,
  QueryRequest,
  ReadBatch,
  WriteBatch,
  defineParams,
  g,
  param,
  readBatch,
  sub,
  writeBatch,
} from "../src/index.js";

const readParams = defineParams({
  tenant: param.string(),
  limit: param.i64(),
  createdAfter: param.dateTime(),
  scores: param.array(param.f64()),
  labels: param.object(param.string()),
});

const writeParams = defineParams({
  values: param.array(param.object(param.value())),
});

function directFindUsers(p: typeof readParams) {
  return readBatch().varAs("users", g().nWithLabel("User").limit(p.limit));
}

const directBatch = directFindUsers(readParams);

directBatch.toJsonString();
directBatch.toQueryRequest({ queryName: "find_users" });
directBatch.toQueryRequest(readParams, {
  tenant: "acme",
  limit: 10n,
  createdAfter: DateTime.fromMillis(0),
  scores: [1, 2],
  labels: { status: "active" },
});
directBatch.toQueryJson(
  readParams,
  {
    tenant: "acme",
    limit: 10,
    createdAfter: "2026-01-01T00:00:00Z",
    scores: [1, 2],
    labels: { status: "active" },
  },
  {
    queryName: "find_users",
  },
);
writeBatch()
  .varAs("users", g().addN("User", { payload: writeParams.values }))
  .toQueryBytes(writeParams, {
    values: [{ id: 1, nested: { ok: true } }],
  });
readBatch().varAs("count", g().nWithLabel("User").count()).toQueryJson();
readBatch()
  .varAs(
    "bindings",
    g()
      .nWithLabel("Service")
      .bind("service")
      .optional(sub().in("CREATES").bind("deployment"))
      .projectDistinctBindings([
        BindingProjection.binding("service", "$id", "service_id"),
        BindingProjection.coalesce(
          [BindingProjection.bindingRef("deployment", "$id"), BindingProjection.bindingRef("service", "$id")],
          "workload_id",
        ),
      ]),
  )
  .toQueryJson();

// @ts-expect-error missing direct query parameters
directBatch.toQueryJson(readParams, { tenant: "acme" });

// @ts-expect-error unknown direct query parameter
directBatch.toQueryJson(readParams, { tenant: "acme", limit: 10, createdAfter: 0, scores: [], labels: {}, extra: true });

// @ts-expect-error wrong direct query object parameter
directBatch.toQueryJson(readParams, { tenant: "acme", limit: 10, createdAfter: 0, scores: [], labels: { status: 1 } });

// @ts-expect-error wrong direct query array parameter
directBatch.toQueryJson(readParams, { tenant: "acme", limit: 10, createdAfter: 0, scores: ["bad"], labels: {} });

// @ts-expect-error write traversal is rejected by read batches
readBatch().varAs("created", g().addN("User", {}));

// @ts-expect-error parameter schemas are factory-created closed values
new ParamSchema("Array");

// @ts-expect-error wire parameter types are factory-created closed values
new QueryParamType("Array");

// @ts-expect-error read batches cannot be directly constructed
new ReadBatch();

// @ts-expect-error write batches cannot be directly constructed
new WriteBatch();

// @ts-expect-error read and write batches are nominally distinct
const writeFromRead: WriteBatch = readBatch();
void writeFromRead;

// @ts-expect-error read requests require a nominal read batch
QueryRequest.read(writeBatch());

// @ts-expect-error write requests require a nominal write batch
QueryRequest.write(readBatch());

export async function cypherApi(client: Client) {
  const request: CypherExecutionRequest = client
    .requestBuilder()
    .writerOnly()
    .shouldAwaitDurability(false)
    .cypher("RETURN $x AS x", { x: 1n });
  const response: CypherResponse = await request.send();
  // A builder explanation is absent after a Helix Cloud warm-only 204.
  const warmed: CypherExplanation | undefined = await client.requestBuilder().warmOnly().cypher("MATCH (n) RETURN n").explain();
  // @ts-expect-error builder explanations must be checked before use
  const unchecked: CypherExplanation = await client.requestBuilder().cypher("MATCH (n) RETURN n").explain();
  const explanation: CypherExplanation = await client.explainCypher("MATCH (n) RETURN n");
  const effect: "Read" | "Write" = explanation.effect;
  const planner: unknown = explanation.planner;
  const direct: [CypherResponse, CypherExplanation] = [
    await client.cypher("RETURN 1 AS x"),
    await client.explainCypher("RETURN 1 AS x", {}, "name"),
  ];

  // @ts-expect-error Cypher parameters are a map of names to values
  client.cypher("RETURN $x AS x", "x");

  // @ts-expect-error the query name is a string
  client.explainCypher("RETURN 1 AS x", {}, 1);

  return { response, warmed, unchecked, effect, planner, direct };
}
