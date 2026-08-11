import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { configFromEnv } from "../src/config.js";
import { checksum, chunks, readDump, serializeDump, writeDump } from "../src/dump.js";
import { parseMovieGraphDump, type MovieGraphDumpV1 } from "../src/model.js";

const fixtureUrl = new URL("../fixtures/movie-graph.v1.json", import.meta.url);

async function fixture(): Promise<MovieGraphDumpV1> {
  return parseMovieGraphDump(JSON.parse(await readFile(fixtureUrl, "utf8")) as unknown);
}

test("validates and normalizes a versioned dump deterministically", async () => {
  const dump = await fixture();
  const reversed = { ...dump, nodes: [...dump.nodes].reverse(), relationships: [...dump.relationships].reverse() };
  assert.equal(serializeDump(reversed), await readFile(fixtureUrl, "utf8"));
  assert.equal(checksum(serializeDump(reversed)), "a0456df5ad9b925836b19226a5c5a6c5e782750314731cd64e811d0175800343");
});

test("rejects unsupported versions and malformed fields", async () => {
  const dump = await fixture();
  assert.throws(() => parseMovieGraphDump({ ...dump, version: 2 }), /Invalid input/);
  assert.throws(() => parseMovieGraphDump({ ...dump, extra: true }), /Unrecognized key/);
  const malformed = structuredClone(dump) as unknown as { nodes: Array<Record<string, unknown>> };
  malformed.nodes[0]!.released = "1999";
  assert.throws(() => parseMovieGraphDump(malformed), /expected number/);
});

test("rejects duplicate logical identifiers", async () => {
  const dump = await fixture();
  assert.throws(() => parseMovieGraphDump({ ...dump, nodes: [...dump.nodes, dump.nodes[0]] }), /duplicate Movie.movieId/);
  const person = dump.nodes.find((node) => node.kind === "person")!;
  assert.throws(() => parseMovieGraphDump({ ...dump, nodes: [...dump.nodes, person] }), /duplicate Person.personId/);
  assert.throws(
    () => parseMovieGraphDump({ ...dump, relationships: [...dump.relationships, dump.relationships[0]] }),
    /duplicate relationshipId/,
  );
});

test("rejects orphan endpoints and duplicate acting roles", async () => {
  const dump = await fixture();
  const relationship = dump.relationships[0]!;
  assert.throws(
    () => parseMovieGraphDump({ ...dump, relationships: [{ ...relationship, fromPersonId: "missing" }, ...dump.relationships.slice(1)] }),
    /missing Person/,
  );
  assert.throws(
    () => parseMovieGraphDump({ ...dump, relationships: [{ ...relationship, toMovieId: "missing" }, ...dump.relationships.slice(1)] }),
    /missing Movie/,
  );
  assert.equal(relationship.kind, "acted_in");
  if (relationship.kind === "acted_in") {
    assert.throws(
      () => parseMovieGraphDump({ ...dump, relationships: [{ ...relationship, roles: ["Neo", "Neo"] }, ...dump.relationships.slice(1)] }),
      /duplicate role Neo/,
    );
  }
});

test("writes and verifies the dump checksum", async () => {
  const directory = await mkdtemp(join(tmpdir(), "neo4j-migration-dump-"));
  const path = join(directory, "dump.json");
  try {
    const dump = await fixture();
    const digest = await writeDump(path, dump);
    assert.equal(digest, checksum(await readFile(path, "utf8")));
    assert.deepEqual(await readDump(path), dump);
    await writeFile(path, "{}\n", "utf8");
    await assert.rejects(readDump(path), /checksum mismatch/);
    await writeFile(`${path}.sha256`, `${digest}  other.json\n`, "utf8");
    await assert.rejects(readDump(path), /invalid checksum file/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("splits bounded batches and validates configuration", () => {
  assert.deepEqual(chunks([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(chunks([], 2), []);
  assert.throws(() => chunks([1], 0), /positive integer/);
  assert.equal(configFromEnv({}).batchSize, 100);
  assert.equal(configFromEnv({ MIGRATION_BATCH_SIZE: "7" }).batchSize, 7);
  assert.throws(() => configFromEnv({ MIGRATION_BATCH_SIZE: "0" }), /positive integer/);
});
