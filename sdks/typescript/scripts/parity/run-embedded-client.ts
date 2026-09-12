import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Client, g, HelixError, QueryRequest, readBatch, stringifyJson, structuralJsonEqual, type HelixDbSource } from "../../src/index.js";
import { nodePermutationFixtures, runtimeFixtures, type Fixture } from "./generate-fixtures.js";
import { runCypherEmbedded } from "./cypher-client.js";
import { parityProgress } from "./progress.js";

const TRANSACTION_CONFLICT_ATTEMPTS = 8;
const TRANSACTION_CONFLICT_MESSAGE = "Storage error: Transaction error: transaction conflict";

const results = process.env.HELIX_EMBEDDED_PARITY_RESULTS;
if (results === undefined) throw new Error("HELIX_EMBEDDED_PARITY_RESULTS is required");

await rm(results, { recursive: true, force: true });
await mkdir(results, { recursive: true });

const database = process.env.HELIX_EMBEDDED_PARITY_DATABASE ?? "typescript-sdk-embedded-parity";
const storage = process.env.HELIX_EMBEDDED_PARITY_STORAGE ?? "memory";
const source = (): HelixDbSource => {
  if (storage === "memory") return { kind: "inMemory", database };
  if (storage !== "disk") throw new Error(`unsupported embedded parity storage ${storage}`);
  const root = process.env.HELIX_EMBEDDED_PARITY_DISK_ROOT;
  if (root === undefined) throw new Error("HELIX_EMBEDDED_PARITY_DISK_ROOT is required for disk parity");
  return { kind: "disk", root, database };
};
const cache = { vectorMemoryBytes: 256 * 1024 * 1024, mode: { kind: "memory" as const } };
const fixtures = [...runtimeFixtures(), ...nodePermutationFixtures()].sort((left, right) => left.name.localeCompare(right.name));
parityProgress(`DSL ${storage}: opening writer`);
let client = await Client.embedded(source(), cache);
try {
  for (const fixture of fixtures) {
    parityProgress(`DSL ${storage}: ${fixture.name}`);
    if (storage === "disk" && fixture.name === "900-write-active-text-items") {
      await client.close();
      const reader = await Client.embeddedReader(source(), cache);
      try {
        for (const searchName of ["025-read-text-search-nodes", "027-read-text-search-edges"]) {
          const search = requiredFixture(fixtures, searchName);
          const actual = await executeQuery(reader, search.request);
          const expected = await readFile(join(results, `${search.name}.json`), "utf8");
          if (!structuralJsonEqual(expected, stringifyJson(actual))) {
            throw new Error(`${search.name} changed after reopening a disk reader`);
          }
        }
      } finally {
        await reader.close();
      }
      client = await Client.embedded(source(), cache);
    }
    const response = await executeQuery(client, fixture.request);
    await awaitIndexOperations(client, response);
    await writeFile(join(results, `${fixture.name}.json`), stringifyJson(normalizeOperationIds(response)));
  }
  for (const searchName of ["025-read-text-search-nodes", "027-read-text-search-edges"]) {
    const search = requiredFixture(fixtures, searchName);
    try {
      await executeQuery(client, search.request);
      throw new Error(`${search.name} unexpectedly succeeded after index DROP`);
    } catch (error) {
      if (error instanceof Error && error.message.includes("unexpectedly succeeded")) throw error;
      if (!(error instanceof Error) || !error.message.includes("index_not_found")) {
        throw new Error(`${search.name} returned the wrong post-DROP error: ${String(error)}`);
      }
    }
  }
} finally {
  parityProgress(`DSL ${storage}: closing writer`);
  await client.close();
}
parityProgress(`DSL ${storage}: closed writer`);
await runCypherEmbedded(source(), cache, results);
parityProgress(`${storage}: client complete`);

function requiredFixture(fixtures: Fixture[], name: string): Fixture {
  const fixture = fixtures.find((candidate) => candidate.name === name);
  if (fixture === undefined) throw new Error(`missing fixture ${name}`);
  return fixture;
}

async function executeQuery<R = unknown>(client: Client, request: QueryRequest): Promise<R> {
  for (let attempt = 0; attempt < TRANSACTION_CONFLICT_ATTEMPTS; attempt += 1) {
    try {
      return await client.query<R>(request).send();
    } catch (error) {
      const isTransactionConflict =
        error instanceof HelixError && error.kind === "Embedded" && error.details?.includes(TRANSACTION_CONFLICT_MESSAGE) === true;
      if (!isTransactionConflict || attempt + 1 === TRANSACTION_CONFLICT_ATTEMPTS) throw error;
      // Embedded storage reports retryable transaction conflicts in the error
      // details; the losing transaction did not commit.
      await new Promise((resolve) => setTimeout(resolve, 10 * 2 ** attempt));
    }
  }
  throw new Error("transaction conflict retry loop exhausted without an error");
}

/** Wait for asynchronous DDL receipts before a later embedded fixture uses the index. */
async function awaitIndexOperations(client: Client, response: unknown): Promise<void> {
  for (const operationId of collectOperationIds(response)) {
    const deadline = Date.now() + 60_000;
    for (;;) {
      const statusResponse = await executeQuery(
        client,
        QueryRequest.read(readBatch().varAs("status", g().getIndexOperation(operationId)).returning(["status"])),
      );
      const status = objectField(objectField(statusResponse, "status"), "status");
      if (status === "succeeded") break;
      if (status !== "queued" && status !== "running") {
        throw new Error(`operation ${operationId} reached unexpected status ${String(status)}: ${stringifyJson(statusResponse)}`);
      }
      if (Date.now() >= deadline) throw new Error(`operation ${operationId} did not finish within 60s`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
}

/** Return operation IDs only from DDL receipt objects. */
function collectOperationIds(value: unknown, ids = new Set<string>()): Set<string> {
  if (Array.isArray(value)) {
    for (const entry of value) collectOperationIds(entry, ids);
  } else if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    if ((object.kind === "accepted" || object.kind === "existing_operation") && typeof object.operation_id === "string") {
      ids.add(object.operation_id);
    }
    for (const entry of Object.values(object)) collectOperationIds(entry, ids);
  }
  return ids;
}

/** Replace random operation UUIDs while retaining the receipt contract under comparison. */
function normalizeOperationIds(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeOperationIds);
  if (value === null || typeof value !== "object") return value;
  const normalized = Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, normalizeOperationIds(entry)]));
  if ((normalized.kind === "accepted" || normalized.kind === "existing_operation") && "operation_id" in normalized) {
    normalized.operation_id = "<operation-id>";
  }
  return normalized;
}

/** Read one required object field without accepting malformed native responses. */
function objectField(value: unknown, field: string): unknown {
  if (value === null || typeof value !== "object" || !(field in value)) throw new Error(`embedded response is missing ${field}`);
  return (value as Record<string, unknown>)[field];
}
