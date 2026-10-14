import assert from "node:assert/strict";
import { Client, g, NodeRef, readBatch, writeBatch } from "../src/index.js";

// Run against a disposable server, then rerun with --verify after restarting it.
const client = Client.server(process.env.HELIX_DROP_TEST_URL ?? "http://127.0.0.1:16972");
const read = async () =>
  client
    .query(
      readBatch()
        .varAs("nodes", g().nWithLabel("DropRegressionNode").count())
        .varAs("edges", g().eWithLabel("DROP_REGRESSION_EDGE").count())
        .returning(["nodes", "edges"])
        .toQueryRequest(),
    )
    .send();
if (!process.argv.includes("--verify")) {
  await client
    .requestBuilder()
    .shouldAwaitDurability(true)
    .query(
      writeBatch()
        .varAs("a", g().addN("DropRegressionNode", { key: "a" }))
        .varAs("b", g().addN("DropRegressionNode", { key: "b" }))
        .varAs("e", g().n(NodeRef.var("a")).addE("DROP_REGRESSION_EDGE", NodeRef.var("b")))
        .returning([])
        .toQueryRequest(),
    )
    .send();
  assert.deepEqual(await read(), { nodes: 2, edges: 1 });
  for (let i = 0; i < 3; i++) {
    await client
      .requestBuilder()
      .shouldAwaitDurability(true)
      .query(
        writeBatch()
          .varAs("a", g().nWithLabel("DropRegressionNode").has("key", "a"))
          .varAs("b", g().nWithLabel("DropRegressionNode").has("key", "b"))
          .varAs("gone", g().n(NodeRef.var("a")).outE("DROP_REGRESSION_EDGE").drop())
          .varAs("new", g().n(NodeRef.var("a")).addE("DROP_REGRESSION_EDGE", NodeRef.var("b")))
          .returning([])
          .toQueryRequest(),
      )
      .send();
    assert.deepEqual(await read(), { nodes: 2, edges: 1 });
  }
  await client
    .requestBuilder()
    .shouldAwaitDurability(true)
    .query(writeBatch().varAs("gone", g().eWithLabel("DROP_REGRESSION_EDGE").drop()).returning([]).toQueryRequest())
    .send();
}
assert.deepEqual(await read(), { nodes: 2, edges: 0 });
console.log("Edge drop HTTP regression passed.");
