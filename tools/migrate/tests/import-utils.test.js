const test = require("node:test");
const assert = require("node:assert/strict");

const {
  extractId,
  buildCompositeKeyFromColumns,
} = require("../dist/import-data.js");

test("extractId finds nested id values", () => {
  const response = {
    node: {
      created: {
        id: "node-123",
      },
    },
  };

  assert.equal(extractId(response), "node-123");
});

test("extractId can read ids from arrays", () => {
  const response = [{ edge: { id: "edge-1" } }];
  assert.equal(extractId(response), "edge-1");
});

test("buildCompositeKeyFromColumns is deterministic", () => {
  const row = { a: 1, b: "x" };
  const key = buildCompositeKeyFromColumns(row, ["a", "b"]);
  assert.equal(key, JSON.stringify([1, "x"]));
});

test("buildCompositeKeyFromColumns returns null for missing values", () => {
  const row = { a: 1, b: null };
  const key = buildCompositeKeyFromColumns(row, ["a", "b"]);
  assert.equal(key, null);
});
