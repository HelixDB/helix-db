import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { workspaceRoot } from "./paths.js";

type ObjectValue = Record<string, unknown>;
type ResultValue = { columns: string[]; rows: unknown[][] };
export type CypherCase = {
  name: string;
  query: string;
  parameters: ObjectValue;
  afterDiskReopen: boolean;
  expectation:
    | { kind: "result"; value: ResultValue; normalizeGraphIds: boolean }
    | { kind: "error"; detail: string; category: string; phase: string };
};

export const cypherFixturePath = join(workspaceRoot, "sdks", "tests", "cypher", "runtime.json");

function object(value: unknown): ObjectValue {
  assert(value !== null && typeof value === "object" && !Array.isArray(value), "expected a JSON object");
  return value as ObjectValue;
}

function result(value: unknown): ResultValue {
  const record = object(value);
  assert(Array.isArray(record.columns) && record.columns.every((column: unknown) => typeof column === "string"));
  assert(Array.isArray(record.rows));
  const width = record.columns.length;
  assert(
    record.rows.every((row: unknown) => Array.isArray(row) && row.length === width),
    "nonrectangular Cypher rows",
  );
  assert(!("diagnostics" in record) && !("resources" in record), "planner/resource diagnostics leaked into result data");
  return { columns: record.columns as string[], rows: record.rows as unknown[][] };
}

/** Resolve fixture input into a closed success/error assertion contract. */
export function parseCypherCases(value: unknown): CypherCase[] {
  const corpus = object(value);
  assert.equal(corpus.schema_version, 1);
  assert(Array.isArray(corpus.cases) && corpus.cases.length === 10, "missing Cypher runtime cases");
  const names = new Set<string>();
  const cases: CypherCase[] = corpus.cases.map((value: unknown) => {
    const entry = object(value);
    assert(
      typeof entry.name === "string" && /^[a-z0-9-]+$/.test(entry.name) && !names.has(entry.name),
      "invalid or duplicate fixture name",
    );
    names.add(entry.name);
    assert(typeof entry.query === "string" && entry.query.length > 0);
    assert(entry.after_disk_reopen === undefined || typeof entry.after_disk_reopen === "boolean");
    assert(entry.normalize_graph_ids === undefined || typeof entry.normalize_graph_ids === "boolean");
    const base = {
      name: entry.name,
      query: entry.query,
      parameters: entry.parameters === undefined ? {} : object(entry.parameters),
      afterDiskReopen: entry.after_disk_reopen === true,
    };
    if ("expected_error_detail" in entry) {
      assert(!("expected" in entry) && entry.normalize_graph_ids !== true && !base.afterDiskReopen);
      assert(typeof entry.expected_error_detail === "string" && entry.expected_error_detail.length > 0);
      assert(typeof entry.expected_error_category === "string" && entry.expected_error_category.length > 0);
      assert(typeof entry.expected_error_phase === "string" && /^(parse|bind|plan|runtime)$/.test(entry.expected_error_phase));
      return {
        ...base,
        expectation: {
          kind: "error",
          detail: entry.expected_error_detail,
          category: entry.expected_error_category,
          phase: entry.expected_error_phase,
        },
      };
    }
    return {
      ...base,
      expectation: { kind: "result", value: result(entry.expected), normalizeGraphIds: entry.normalize_graph_ids === true },
    };
  });
  assert.deepStrictEqual(
    cases.map((fixture) => fixture.name),
    [
      "scalar-values",
      "create",
      "graph-values",
      "delete-rollback",
      "rollback-observation",
      "update",
      "persistent-read",
      "match-after-distinct",
      "detach-delete",
      "deleted-observation",
    ],
    "Cypher runtime case order or denominator changed",
  );
  assert.deepStrictEqual(
    cases.filter((fixture) => fixture.afterDiskReopen).map((fixture) => fixture.name),
    ["persistent-read"],
    "missing disk restart boundary",
  );
  return cases;
}

export async function readCypherCases(): Promise<CypherCase[]> {
  return parseCypherCases(JSON.parse(await readFile(cypherFixturePath, "utf8")) as unknown);
}

/** Traverse wire values without treating an escaped literal map as a graph element. */
function mapWire(value: unknown, graph: (value: ObjectValue) => ObjectValue): unknown {
  if (Array.isArray(value)) return value.map((value: unknown) => mapWire(value, graph));
  if (value === null || typeof value !== "object") return value;
  const record = object(value);
  const properties = (value: unknown): ObjectValue =>
    Object.fromEntries(Object.entries(object(value)).map(([key, value]) => [key, mapWire(value, graph)]));
  switch (record.$type) {
    case undefined:
      return properties(record);
    case "integer":
    case "float":
      return record;
    case "map":
      return { ...record, value: properties(record.value) };
    case "node":
    case "relationship":
      return graph({ ...record, properties: properties(record.properties) });
    case "path": {
      assert(Array.isArray(record.nodes) && Array.isArray(record.relationships), "invalid path arrays");
      return graph({ ...record, nodes: mapWire(record.nodes, graph), relationships: mapWire(record.relationships, graph) });
    }
    default:
      throw new Error("unknown unescaped wire-value tag");
  }
}

function graphId(value: unknown): string {
  assert(typeof value === "string" && value.length <= 20 && /^(0|[1-9][0-9]*)$/.test(value), "graph IDs must be canonical decimal strings");
  assert(BigInt(value) <= 18446744073709551615n, "graph ID overflow");
  return value;
}

/** Normalize only verified fixture identities, keeping endpoint and path direction. */
export function normalizeGraphIds(value: unknown): unknown {
  const nodes = new Map<string, string>();
  const relationships = new Map<string, string>();
  const names = new Map<string, string>();
  mapWire(value, (element) => {
    if (element.$type === "path") return element;
    const props = object(element.properties);
    const name = element.$type === "node" ? `node:${String(props.key)}` : `relationship:${String(props.tag)}`;
    assert(["node:a", "node:b", "relationship:edge"].includes(name), "unexpected fixture graph identity");
    const id = graphId(element.id);
    const identities = element.$type === "node" ? nodes : relationships;
    assert(identities.get(id) === undefined || identities.get(id) === name, "one graph ID describes different elements");
    assert(names.get(name) === undefined || names.get(name) === id, "one element has inconsistent graph IDs");
    identities.set(id, name);
    names.set(name, id);
    return element;
  });
  return mapWire(value, (element) => {
    switch (element.$type) {
      case "node":
        return { ...element, id: nodes.get(graphId(element.id)) };
      case "relationship": {
        const start = nodes.get(graphId(element.start));
        const end = nodes.get(graphId(element.end));
        assert(start !== undefined && end !== undefined, "relationship endpoints lack matching node values");
        return { ...element, id: relationships.get(graphId(element.id)), start, end };
      }
      default:
        return element;
    }
  });
}

export function verifyCypherCase(fixture: CypherCase, output: unknown): void {
  const actual = object(output);
  if (fixture.expectation.kind === "error") {
    assert(!("result" in actual) && typeof actual.error === "string", `${fixture.name}: expected an error`);
    assert(typeof actual.code === "string", `${fixture.name}: missing structured error code`);
    const code = actual.code.split(":");
    let category: unknown;
    let phase: unknown;
    let detail: unknown;
    if (code.length === 3) {
      [category, phase, detail] = code;
    } else {
      assert.equal(code.length, 1, "malformed Cypher error code");
      category = actual.code;
      const details = object(actual.details);
      phase = details.phase;
      detail = details.detail;
    }
    assert(typeof phase === "string");
    assert.deepStrictEqual(
      { category, phase: phase.toLowerCase(), detail },
      {
        category: fixture.expectation.category,
        phase: fixture.expectation.phase,
        detail: fixture.expectation.detail,
      },
      `${fixture.name}: wrong error classification`,
    );
    return;
  }
  assert(!("error" in actual), `${fixture.name}: unexpected error: ${String(actual.error)}`);
  let value: unknown = result(actual.result);
  if (fixture.expectation.normalizeGraphIds) value = normalizeGraphIds(value);
  assert.deepStrictEqual(value, fixture.expectation.value, fixture.name);
}

/** Missing/extra files and any individual mismatch fail the SDK runtime gate. */
export async function verifyCypherResults(root: string, cases: CypherCase[]): Promise<void> {
  const files = (await readdir(root, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
    .map((entry) => entry.name)
    .sort();
  assert.deepStrictEqual(
    files,
    cases.map((fixture) => `${fixture.name}.json`).sort(),
    "Cypher result denominator differs from the fixture corpus",
  );
  for (const fixture of cases) {
    verifyCypherCase(fixture, JSON.parse(await readFile(join(root, `${fixture.name}.json`), "utf8")) as unknown);
  }
}
