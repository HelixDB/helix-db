import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  Client,
  IndexSpec,
  Projection,
  PropertyValue,
  SourcePredicate,
  VectorDistanceMetric,
  g,
  readBatch,
  writeBatch,
} from "@helix-db/helix-db";

const DATABASE = "node-package-disk-smoke";
const NODE_COUNT = 100;
const EDGE_COUNT = 200;
const BATCH_SIZE = 25;
const VECTOR_RESULT_COUNT = 1;
const VECTOR_SCORE_TOLERANCE = 1e-6;
const root = await mkdtemp(join(tmpdir(), "helixdb-node-package-smoke-"));
const source = { kind: "disk", root, database: DATABASE };

try {
  const writer = await Client.embedded(source);
  const nodeIds = [];
  try {
    const indexNames = [
      "node_equality",
      "node_range",
      "edge_equality",
      "edge_range",
      "node_text",
      "edge_text",
      "node_vector",
      "edge_vector",
    ];
    const indexResponse = await execute(
      writer,
      writeBatch()
        .varAs(
          "node_equality",
          g().createIndexIfNotExists(
            IndexSpec.nodeEquality("Document", "category"),
          ),
        )
        .varAs(
          "node_range",
          g().createIndexIfNotExists(IndexSpec.nodeRange("Document", "rank")),
        )
        .varAs(
          "edge_equality",
          g().createIndexIfNotExists(
            IndexSpec.edgeEquality("REFERENCES", "kind"),
          ),
        )
        .varAs(
          "edge_range",
          g().createIndexIfNotExists(
            IndexSpec.edgeRange("REFERENCES", "weight"),
          ),
        )
        .varAs(
          "node_text",
          g().createIndexIfNotExists(IndexSpec.nodeText("Document", "body")),
        )
        .varAs(
          "edge_text",
          g().createIndexIfNotExists(IndexSpec.edgeText("REFERENCES", "note")),
        )
        .varAs(
          "node_vector",
          g().createIndexIfNotExists(
            IndexSpec.nodeVector(
              "Document",
              "embedding",
              4,
              VectorDistanceMetric.Cosine,
            ),
          ),
        )
        .varAs(
          "edge_vector",
          g().createIndexIfNotExists(
            IndexSpec.edgeVector(
              "REFERENCES",
              "embedding",
              4,
              VectorDistanceMetric.Cosine,
            ),
          ),
        )
        .returning(indexNames),
      "create_package_smoke_indexes",
    );
    for (const name of indexNames) {
      await awaitIndexOperation(writer, indexResponse[name]);
    }

    for (let start = 0; start < NODE_COUNT; start += BATCH_SIZE) {
      let batch = writeBatch();
      const names = [];
      for (
        let index = start;
        index < Math.min(start + BATCH_SIZE, NODE_COUNT);
        index += 1
      ) {
        const name = `document_${index}`;
        const category = ["science", "history", "engineering", "literature"][
          index % 4
        ];
        const body =
          index % 10 === 0
            ? `Helix graph database vector search document ${index}`
            : index % 2 === 0
              ? `Distributed graph storage and indexing document ${index}`
              : `Transactional database systems document ${index}`;
        batch = batch.varAs(
          name,
          g()
            .addN("Document", {
              body,
              category,
              doc_id: `doc-${String(index).padStart(3, "0")}`,
              embedding: PropertyValue.f32Array(oneHot(index)),
              rank: BigInt(index),
            })
            .valueMap(["$id"]),
        );
        names.push(name);
      }
      const response = await execute(
        writer,
        batch.returning(names),
        `insert_package_smoke_nodes_${start}`,
      );
      for (const name of names) {
        const row = Array.isArray(response[name])
          ? response[name][0]
          : response[name];
        assert.notEqual(
          row?.$id,
          undefined,
          `${name} should return its node ID`,
        );
        nodeIds.push(row.$id);
      }
    }

    for (let index = 0; index < NODE_COUNT; index += 1) {
      await execute(
        writer,
        writeBatch()
          .varAs(
            `reference_${index}`,
            g()
              .n(nodeIds[index])
              .addE("REFERENCES", nodeIds[(index + 1) % NODE_COUNT], {
                embedding: PropertyValue.f32Array(oneHot(index)),
                kind: index % 2 === 0 ? "citation" : "reference",
                note:
                  index % 10 === 0
                    ? "helix graph connection"
                    : "database relation",
                weight: BigInt(index),
              }),
          )
          .returning([]),
        `insert_package_smoke_reference_edge_${index}`,
      );
      await execute(
        writer,
        writeBatch()
          .varAs(
            `similar_${index}`,
            g()
              .n(nodeIds[index])
              .addE("REFERENCES", nodeIds[(index + 7) % NODE_COUNT], {
                embedding: PropertyValue.f32Array(oneHot(index + 1)),
                kind: "similar",
                note:
                  index % 10 === 0
                    ? "semantic vector neighbor"
                    : "related document",
                weight: BigInt(100 + index),
              }),
          )
          .returning([]),
        `insert_package_smoke_similar_edge_${index}`,
      );
    }
  } finally {
    await writer.close();
  }

  const reader = await Client.embeddedReader(source);
  try {
    const response = await execute(
      reader,
      readBatch()
        .varAs("node_count", g().nWithLabel("Document").count())
        .varAs("edge_count", g().eWithLabel("REFERENCES").count())
        .varAs(
          "node_equality",
          g()
            .nWithLabelWhere(
              "Document",
              SourcePredicate.eq("category", "science"),
            )
            .count(),
        )
        .varAs(
          "node_range",
          g()
            .nWithLabelWhere("Document", SourcePredicate.gte("rank", 90n))
            .count(),
        )
        .varAs(
          "edge_equality",
          g()
            .eWithLabelWhere(
              "REFERENCES",
              SourcePredicate.eq("kind", "citation"),
            )
            .count(),
        )
        .varAs(
          "edge_range",
          g()
            .eWithLabelWhere("REFERENCES", SourcePredicate.gte("weight", 190n))
            .count(),
        )
        .varAs(
          "node_text",
          g()
            .textSearchNodes("Document", "body", "helix graph", 10)
            .valueMap(["doc_id", "$distance"]),
        )
        .varAs(
          "edge_text",
          g()
            .textSearchEdges("REFERENCES", "note", "helix graph", 10)
            .edgeProperties(),
        )
        .returning([
          "node_count",
          "edge_count",
          "node_equality",
          "node_range",
          "edge_equality",
          "edge_range",
          "node_text",
          "edge_text",
        ]),
      "search_package_smoke_indexes_after_reopen",
    );
    assert.equal(Number(response.node_count), NODE_COUNT);
    assert.equal(Number(response.edge_count), EDGE_COUNT);
    assert.equal(Number(response.node_equality), 25);
    assert.equal(Number(response.node_range), 10);
    assert.equal(Number(response.edge_equality), 50);
    assert.equal(Number(response.edge_range), 10);
    assert.equal(response.node_text.length, 10);
    assert.equal(response.edge_text.length, 10);

    for (let dimension = 0; dimension < 4; dimension += 1) {
      const query = oneHot(dimension);
      const vectorResponse = await execute(
        reader,
        readBatch()
          .varAs(
            "nodes",
            g()
              .vectorSearchNodes(
                "Document",
                "embedding",
                query,
                VECTOR_RESULT_COUNT,
              )
              .project([
                Projection.property("embedding", "embedding"),
                Projection.property("$distance", "distance"),
              ]),
          )
          .varAs(
            "edges",
            g()
              .vectorSearchEdges(
                "REFERENCES",
                "embedding",
                query,
                VECTOR_RESULT_COUNT,
              )
              .project([
                Projection.property("embedding", "embedding"),
                Projection.property("$distance", "distance"),
              ]),
          )
          .returning(["nodes", "edges"]),
        `search_package_smoke_vector_basis_${dimension}`,
      );
      for (const kind of ["nodes", "edges"]) {
        assert.equal(
          vectorResponse[kind].length,
          VECTOR_RESULT_COUNT,
          `${kind} basis ${dimension} should return the requested hits`,
        );
        for (const hit of vectorResponse[kind]) {
          assert.equal(
            hit.embedding.length,
            query.length,
            `${kind} basis ${dimension} should preserve vector dimensions`,
          );
          assert.ok(
            Math.abs(hit.distance - halfCosineScore(query, hit.embedding)) <
              VECTOR_SCORE_TOLERANCE,
            `${kind} basis ${dimension} should return the projected vector's half-cosine score`,
          );
        }
      }
    }
  } finally {
    await reader.close();
  }
} finally {
  await rm(root, { force: true, recursive: true });
}

function oneHot(index) {
  return Array.from({ length: 4 }, (_, dimension) =>
    dimension === index % 4 ? 1 : 0,
  );
}

function halfCosineScore(left, right) {
  const dot = left.reduce((sum, value, index) => sum + value * right[index], 0);
  const leftMagnitude = Math.sqrt(
    left.reduce((sum, value) => sum + value * value, 0),
  );
  const rightMagnitude = Math.sqrt(
    right.reduce((sum, value) => sum + value * value, 0),
  );
  return (1 - dot / (leftMagnitude * rightMagnitude)) / 2;
}

async function execute(client, batch, queryName) {
  for (let attempt = 1; attempt <= 8; attempt += 1) {
    try {
      return await client.query(batch.toQueryRequest({ queryName })).send();
    } catch (error) {
      const retryableConflict =
        error?.kind === "Embedded" &&
        error?.details?.includes("Transaction error: transaction conflict");
      if (!retryableConflict || attempt === 8) throw error;
      await new Promise((resolve) => setTimeout(resolve, attempt * 25));
    }
  }
  throw new Error("embedded conflict retry loop exhausted");
}

async function awaitIndexOperation(client, receipt) {
  assert.equal(
    typeof receipt?.operation_id,
    "string",
    "new index should return an operation ID",
  );
  const deadline = Date.now() + 60_000;
  for (;;) {
    const response = await execute(
      client,
      readBatch()
        .varAs("status", g().getIndexOperation(receipt.operation_id))
        .returning(["status"]),
      "poll_package_smoke_index",
    );
    if (response.status.status === "succeeded") return;
    assert.ok(
      response.status.status === "queued" ||
        response.status.status === "running",
      `index operation reached ${String(response.status.status)}`,
    );
    assert.ok(
      Date.now() < deadline,
      "index operation should finish within 60 seconds",
    );
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
