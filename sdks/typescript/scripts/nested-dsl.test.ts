import assert from "node:assert/strict";
import {
  Expr,
  IndexSpec,
  Order,
  Predicate,
  Projection,
  PropertyInput,
  RangeIndexDirection,
  SourcePredicate,
  g,
  readBatch,
  stringifyJson,
  writeBatch,
} from "../src/index.js";

function parsed(value: unknown) {
  return JSON.parse(stringifyJson(value));
}

const nestedWrite = writeBatch()
  .varAs(
    "updated",
    g()
      .addN("User", { name: "john", metadata: { externalID: "some_id", score: 20, tags: ["alpha", 7] } })
      .setProperty("metadata", PropertyInput.param("metadata"))
      .valueMap(["metadata.externalID"]),
  )
  .returning(["updated"]);
const nestedWriteJson = parsed(nestedWrite);
assert.deepEqual(nestedWriteJson.queries[0].Query.steps[0].AddN.properties[1], [
  "metadata",
  {
    Value: {
      Object: {
        externalID: { String: "some_id" },
        score: { I64: 20 },
        tags: { Array: [{ String: "alpha" }, { I64: 7 }] },
      },
    },
  },
]);
assert.deepEqual(nestedWriteJson.queries[0].Query.steps[1], {
  SetProperty: ["metadata", { Expr: { Param: "metadata" } }],
});
assert.deepEqual(nestedWriteJson.queries[0].Query.steps[2], { ValueMap: ["metadata.externalID"] });

const nestedRead = readBatch()
  .varAs(
    "users",
    g()
      .nWhere(SourcePredicate.and([SourcePredicate.eq("name", "john"), SourcePredicate.eq("metadata.externalID", "some_id")]))
      .orderBy("metadata.score", Order.Desc)
      .project([Projection.property("metadata.externalID", "external_id"), Projection.expr("score_copy", Expr.prop("metadata.score"))]),
  )
  .varAs("external_ids", g().nWithLabel("User").values(["metadata.externalID"]))
  .returning(["users", "external_ids"]);
const nestedReadJson = parsed(nestedRead);
assert.deepEqual(nestedReadJson.queries[0].Query.steps[0], {
  NWhere: { And: [{ Eq: ["name", { String: "john" }] }, { Eq: ["metadata.externalID", { String: "some_id" }] }] },
});
assert.deepEqual(nestedReadJson.queries[0].Query.steps[1], { OrderBy: ["metadata.score", "Desc"] });
assert.deepEqual(nestedReadJson.queries[0].Query.steps[2], {
  Project: [
    { source: "metadata.externalID", alias: "external_id" },
    { alias: "score_copy", expr: { Property: "metadata.score" } },
  ],
});
assert.deepEqual(nestedReadJson.queries[1].Query.steps.at(-1), { Values: ["metadata.externalID"] });

const genericEdgeFilters = g()
  .n([1])
  .outE("FOLLOWS")
  .has("status", "active")
  .hasLabel("FOLLOWS")
  .hasKey("weight")
  .where(Predicate.gt("weight", 5))
  .edgeProperties();
assert.deepEqual(parsed(genericEdgeFilters).steps, [
  { N: { Ids: [1] } },
  { OutE: "FOLLOWS" },
  { Has: ["status", { String: "active" }] },
  { HasLabel: "FOLLOWS" },
  { HasKey: "weight" },
  { Where: { Gt: ["weight", { I64: 5 }] } },
  "EdgeProperties",
]);

assert.deepEqual(parsed(IndexSpec.nodeRange("User", "age")), {
  NodeRange: { label: "User", property: "age" },
});
assert.deepEqual(parsed(IndexSpec.nodeRangeWithDirection("User", "age", RangeIndexDirection.Asc)), {
  NodeRange: { label: "User", property: "age" },
});
assert.deepEqual(parsed(IndexSpec.nodeRangeDesc("User", "age")), {
  NodeRange: { label: "User", property: "age", direction: "Desc" },
});
assert.deepEqual(parsed(IndexSpec.edgeRangeDesc("FOLLOWS", "weight")), {
  EdgeRange: { label: "FOLLOWS", property: "weight", direction: "Desc" },
});

console.log("nested-dsl.test.ts passed");
