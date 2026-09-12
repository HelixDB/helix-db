import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  cypherFixturePath,
  normalizeGraphIds,
  parseCypherCases,
  readCypherCases,
  verifyCypherCase,
  verifyCypherResults,
  type CypherCase,
} from "./cypher-results.js";

const cases = await readCypherCases();
const scalar: CypherCase = {
  name: "comparison",
  query: "RETURN 1",
  parameters: {},
  afterDiskReopen: false,
  expectation: {
    kind: "result",
    normalizeGraphIds: false,
    value: {
      columns: ["a", "b"],
      rows: [
        [1, null],
        [1, null],
        [2, false],
      ],
    },
  },
};

test("corpus rejects missing cases, unsafe filenames and incompatible assertions", async () => {
  const original = JSON.parse(await readFile(cypherFixturePath, "utf8"));
  assert.equal(parseCypherCases(original).length, 10);
  const changes = [
    (v: typeof original) => {
      v.schema_version = 2;
    },
    (v: typeof original) => {
      v.cases.pop();
    },
    (v: typeof original) => {
      v.cases[0].name = "../escape";
    },
    (v: typeof original) => {
      v.cases.reverse();
    },
    (v: typeof original) => {
      v.cases[6].after_disk_reopen = false;
    },
    (v: typeof original) => {
      v.cases[0].name = v.cases[1].name;
    },
    (v: typeof original) => {
      v.cases[0].parameters = [];
    },
    (v: typeof original) => {
      v.cases[0].query = "";
    },
    (v: typeof original) => {
      v.cases[0].after_disk_reopen = 1;
    },
    (v: typeof original) => {
      v.cases[0].expected_error_detail = "Oops";
    },
    (v: typeof original) => {
      v.cases[3].normalize_graph_ids = true;
    },
  ];
  for (const change of changes) {
    const corrupted = structuredClone(original);
    change(corrupted);
    assert.throws(() => parseCypherCases(corrupted));
  }
});

test("result comparison preserves order, duplicates, nulls and rectangularity", () => {
  assert.equal(scalar.expectation.kind, "result");
  if (scalar.expectation.kind !== "result") throw new Error("wrong fixture");
  verifyCypherCase(scalar, { result: scalar.expectation.value });
  for (const result of [
    {
      columns: ["b", "a"],
      rows: [
        [1, null],
        [1, null],
        [2, false],
      ],
    },
    {
      columns: ["a", "b"],
      rows: [
        [1, null],
        [2, false],
      ],
    },
    {
      columns: ["a", "b"],
      rows: [
        [2, false],
        [1, null],
        [1, null],
      ],
    },
    { columns: ["a", "b"], rows: [[1], [1, null], [2, false]] },
    { ...scalar.expectation.value, diagnostics: {} },
    { ...scalar.expectation.value, resources: {} },
  ])
    assert.throws(() => verifyCypherCase(scalar, { result }));
  assert.throws(() => verifyCypherCase(scalar, { error: "UnsupportedFeature" }));
});

test("large integers and escaped maps cannot be flattened by a driver", () => {
  const fixture = cases[0]!;
  assert.equal(fixture.expectation.kind, "result");
  if (fixture.expectation.kind !== "result") throw new Error("wrong fixture");
  verifyCypherCase(fixture, { result: structuredClone(fixture.expectation.value) });
  assert.deepStrictEqual(normalizeGraphIds(fixture.expectation.value), fixture.expectation.value);
  const corrupted = structuredClone(fixture.expectation.value);
  corrupted.rows[0]![0] = Number("9223372036854775807");
  assert.throws(() => verifyCypherCase(fixture, { result: corrupted }));
  const escaped = { $type: "map", value: { $type: "node", id: "literal", properties: null } };
  assert.deepStrictEqual(normalizeGraphIds(escaped), escaped);
  assert.throws(() => normalizeGraphIds({ $type: "unknown" }));
});

const a = { $type: "node", id: "18446744073709551615", labels: ["CypherParity"], properties: { key: "a" } };
const b = { $type: "node", id: "0", labels: ["CypherParity"], properties: { key: "b" } };
const edge = { $type: "relationship", id: "0", type: "CYPHER_PARITY", start: a.id, end: b.id, properties: { tag: "edge" } };

test("graph IDs are lossless, namespace-aware and consistent across path appearances", () => {
  const input = [a, edge, { $type: "path", nodes: [a, b], relationships: [edge] }];
  const nodeA = { ...a, id: "node:a" };
  const nodeB = { ...b, id: "node:b" };
  const relationship = { ...edge, id: "relationship:edge", start: "node:a", end: "node:b" };
  assert.deepStrictEqual(normalizeGraphIds(input), [
    nodeA,
    relationship,
    { $type: "path", nodes: [nodeA, nodeB], relationships: [relationship] },
  ]);
  for (const id of [1, -1, "-1", "01", "1.0", "18446744073709551616", "999999999999999999999"]) {
    assert.throws(() => normalizeGraphIds([{ ...a, id }]));
  }
  assert.throws(() => normalizeGraphIds([a, { ...b, id: a.id }]));
  assert.throws(() => normalizeGraphIds([a, { ...a, id: "4" }]));
  assert.throws(() => normalizeGraphIds([a, edge]));
  assert.throws(() => normalizeGraphIds([{ ...a, properties: { key: "unknown" } }]));
  assert.throws(() => normalizeGraphIds({ $type: "path", nodes: null, relationships: [] }));
  const fixture: CypherCase = {
    ...scalar,
    expectation: {
      kind: "result",
      normalizeGraphIds: true,
      value: { columns: ["path"], rows: [[{ $type: "path", nodes: [nodeA, nodeB], relationships: [relationship] }]] },
    },
  };
  verifyCypherCase(fixture, { result: { columns: ["path"], rows: [[{ $type: "path", nodes: [a, b], relationships: [edge] }]] } });
  assert.throws(() =>
    verifyCypherCase(fixture, { result: { columns: ["path"], rows: [[{ $type: "path", nodes: [b, a], relationships: [edge] }]] } }),
  );
  assert.throws(() =>
    verifyCypherCase(fixture, {
      result: { columns: ["path"], rows: [[{ $type: "path", nodes: [a, b], relationships: [{ ...edge, start: b.id, end: a.id }] }]] },
    }),
  );
});

test("negative case requires its specific error and excludes a result", () => {
  const fixture = cases[3]!;
  verifyCypherCase(fixture, { error: "cannot delete connected node", code: "ConstraintVerificationFailed:Runtime:DeleteConnectedNode" });
  verifyCypherCase(fixture, {
    error: "cannot delete connected node",
    code: "ConstraintVerificationFailed",
    details: { phase: "runtime", detail: "DeleteConnectedNode" },
  });
  for (const output of [
    { error: "UnsupportedFeature" },
    { error: "SyntaxError" },
    { error: "DeleteConnectedNode" },
    { error: "DeleteConnectedNode", code: "UnsupportedFeature:Runtime:DeleteConnectedNode" },
    { error: "DeleteConnectedNode", code: "ConstraintVerificationFailed:Parse:DeleteConnectedNode" },
    { error: "DeleteConnectedNode", code: "ConstraintVerificationFailed:Runtime:WrongDetail" },
    { error: "DeleteConnectedNode", code: "ConstraintVerificationFailed:DeleteConnectedNode" },
    { error: "DeleteConnectedNode", code: "ConstraintVerificationFailed", details: { phase: 1, detail: "DeleteConnectedNode" } },
    { error: "DeleteConnectedNode", code: "ConstraintVerificationFailed", details: null },
    { result: {} },
    { error: "DeleteConnectedNode", result: {} },
  ]) {
    assert.throws(() => verifyCypherCase(fixture, output));
  }
});

test("missing and extra output files fail the denominator gate", async () => {
  const root = await mkdtemp(join(tmpdir(), "helix-cypher-comparator-"));
  try {
    await assert.rejects(verifyCypherResults(root, [scalar]));
    if (scalar.expectation.kind !== "result") throw new Error("wrong fixture");
    await writeFile(join(root, "comparison.json"), JSON.stringify({ result: scalar.expectation.value }));
    await verifyCypherResults(root, [scalar]);
    await writeFile(join(root, "extra.json"), "{}");
    await assert.rejects(verifyCypherResults(root, [scalar]));
    await assert.rejects(verifyCypherResults(join(root, "missing"), [scalar]));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
