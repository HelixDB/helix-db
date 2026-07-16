import {
  BindingProjection,
  DateTime,
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
