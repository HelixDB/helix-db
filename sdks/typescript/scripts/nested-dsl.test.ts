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
const nestedWriteRoot = parsed(nestedWrite).entries[0].query.root;
assert.deepEqual(nestedWriteRoot.value_map.input.set_property.input.add_n.properties[1], [
  "metadata",
  {
    value: {
      object: {
        externalID: { string: "some_id" },
        score: { i64: 20 },
        tags: { array: [{ string: "alpha" }, { i64: 7 }] },
      },
    },
  },
]);
assert.deepEqual(nestedWriteRoot.value_map.input.set_property, {
  input: nestedWriteRoot.value_map.input.set_property.input,
  name: "metadata",
  value: { expr: { param: "metadata" } },
});
assert.deepEqual(nestedWriteRoot.value_map.properties, ["metadata.externalID"]);

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
assert.deepEqual(nestedReadJson.entries[0].query.root.project.input.order_by.input.nodes_where, {
  predicate: {
    and: {
      predicates: [
        { eq: { left: { property: "name" }, right: { constant: { string: "john" } } } },
        { eq: { left: { property: "metadata.externalID" }, right: { constant: { string: "some_id" } } } },
      ],
    },
  },
});
assert.deepEqual(nestedReadJson.entries[0].query.root.project.input.order_by, {
  input: nestedReadJson.entries[0].query.root.project.input.order_by.input,
  property: "metadata.score",
  order: "desc",
});
assert.deepEqual(nestedReadJson.entries[0].query.root.project.projections, [
  { property: { source: "metadata.externalID", alias: "external_id" } },
  { expr: { alias: "score_copy", expr: { property: "metadata.score" } } },
]);
assert.deepEqual(nestedReadJson.entries[1].query.root.values.properties, ["metadata.externalID"]);

const genericEdgeFilters = g()
  .n([1])
  .outE("FOLLOWS")
  .has("status", "active")
  .hasLabel("FOLLOWS")
  .hasKey("weight")
  .where(Predicate.gt("weight", 5))
  .edgeProperties();
assert.deepEqual(parsed(genericEdgeFilters).root.edge_properties.input.where.predicate, {
  gt: { left: { property: "weight" }, right: { constant: { i64: 5 } } },
});
assert.deepEqual(parsed(genericEdgeFilters).root.edge_properties.input.where.input.has_key.input.has_label.input.has, {
  input: { out_e: { input: { nodes: { reference: { ids: [1] } } }, label: "FOLLOWS" } },
  property: "status",
  value: { string: "active" },
});

assert.deepEqual(parsed(IndexSpec.nodeRange("User", "age")), {
  node_range: { label: "User", property: "age", direction: "asc" },
});
assert.deepEqual(parsed(IndexSpec.nodeRangeWithDirection("User", "age", RangeIndexDirection.Asc)), {
  node_range: { label: "User", property: "age", direction: "asc" },
});
assert.deepEqual(parsed(IndexSpec.nodeRangeDesc("User", "age")), {
  node_range: { label: "User", property: "age", direction: "desc" },
});
assert.deepEqual(parsed(IndexSpec.edgeRangeDesc("FOLLOWS", "weight")), {
  edge_range: { label: "FOLLOWS", property: "weight", direction: "desc" },
});

console.log("nested-dsl.test.ts passed");
