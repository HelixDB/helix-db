import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import type { QueryRequest } from "@helix-db/helix-db";
import { HelixHttpClient } from "../src/helix-http.js";
import { ensureIndexes, loadDump } from "../src/loader.js";
import { parseMovieGraphDump } from "../src/model.js";
import { normalizeMovieDetails, normalizePersonFilmography } from "../src/repositories.js";
import { expectedCounts } from "../src/verification.js";

const fixtureUrl = new URL("../fixtures/movie-graph.v1.json", import.meta.url);

async function fixture() {
  return parseMovieGraphDump(JSON.parse(await readFile(fixtureUrl, "utf8")) as unknown);
}

function clientWith(handler: (request: QueryRequest) => unknown | Promise<unknown>): HelixHttpClient {
  return {
    execute: (request: QueryRequest) => Promise.resolve(handler(request)),
  } as unknown as HelixHttpClient;
}

test("stops cleanly after nodes and resumes with bounded relationship batches", async () => {
  const dump = await fixture();
  const names: string[] = [];
  const client = clientWith((request) => {
    const body = JSON.parse(request.toJsonString()) as { query_name: string };
    names.push(body.query_name);
    return body.query_name === "bootstrap_movie_graph_indexes"
      ? { indexes: [{ kind: "already_active", index_id: "1", generation: "1" }] }
      : {};
  });
  const stopped = await loadDump(dump, client, { batchSize: 3, stopAfterNodes: true });
  assert.deepEqual(stopped, { people: 5, movies: 2, relationships: 0, stoppedAfterNodes: true });
  assert.deepEqual(names, ["bootstrap_movie_graph_indexes", "upsert_people", "upsert_people", "upsert_movies"]);

  names.length = 0;
  const completed = await loadDump(dump, client, { batchSize: 3 });
  assert.deepEqual(completed, { people: 5, movies: 2, relationships: 10, stoppedAfterNodes: false });
  assert.deepEqual(names, [
    "bootstrap_movie_graph_indexes",
    "upsert_people",
    "upsert_people",
    "upsert_movies",
    "replace_acted_in_relationships",
    "replace_acted_in_relationships",
    "replace_directed_relationships",
    "replace_produced_relationships",
  ]);
});

test("waits for accepted indexes and rejects blocked or timed-out operations", async () => {
  const operationId = "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001";
  const receipt = { kind: "accepted", operation_id: operationId, index_id: "1", generation: "1" };
  const progress = { entities: "0", input_bytes: "0", output_operations: "0", output_bytes: "0" };
  const status = (state: "queued" | "succeeded" | "blocked") => ({
    status: state,
    operation_id: operationId,
    index_id: "1",
    generation: "1",
    operation_kind: "build",
    family: "secondary",
    stage: "scan",
    attempt: 0,
    progress,
    ...(state === "blocked" ? { blocker_code: "invalid_source_data" } : {}),
  });
  let polls = 0;
  await ensureIndexes(
    clientWith((request) => {
      const name = (JSON.parse(request.toJsonString()) as { query_name: string }).query_name;
      if (name === "bootstrap_movie_graph_indexes") return { index: receipt };
      polls += 1;
      return { status: status(polls === 1 ? "queued" : "succeeded") };
    }),
    1_000,
    0,
  );
  assert.equal(polls, 2);

  await assert.rejects(
    ensureIndexes(clientWith((request) => ((JSON.parse(request.toJsonString()) as { query_name: string }).query_name === "bootstrap_movie_graph_indexes" ? { index: receipt } : { status: status("blocked") })), 100, 0),
    /ended with blocked/,
  );
  await assert.rejects(
    ensureIndexes(clientWith((request) => ((JSON.parse(request.toJsonString()) as { query_name: string }).query_name === "bootstrap_movie_graph_indexes" ? { index: receipt } : { status: status("queued") })), 0, 0),
    /did not finish/,
  );
});

test("normalizes Neo4j objects and Helix result arrays to one contract", () => {
  const movie = { movieId: "m-1", title: "Movie", released: 2000, tagline: "Tagline" };
  const person = { personId: "p-1", name: "Person", born: 1970 };
  const expectedDetails = { movie, cast: [{ ...person, roles: ["Lead"] }], directors: [person], producers: [person] };
  assert.deepEqual(
    normalizeMovieDetails({ movie: [movie], cast: [{ ...person, roles: ["Lead"] }], directors: [person], producers: [person] }),
    expectedDetails,
  );
  assert.deepEqual(normalizeMovieDetails({ movie, cast: expectedDetails.cast, directors: [person], producers: [person] }), expectedDetails);
  assert.equal(normalizeMovieDetails({ movie: [] }), null);
  assert.throws(() => normalizeMovieDetails([]), /must be an object/);

  const older = { ...movie, movieId: "m-old", title: "Older", released: 1999, roles: ["Lead"] };
  const newer = { ...movie, movieId: "m-new", title: "Newer", released: 2003, roles: ["Lead"] };
  assert.deepEqual(normalizePersonFilmography({ person: [person], movies: [older, newer] }), { person, movies: [newer, older] });
  assert.equal(normalizePersonFilmography({ person: [] }), null);
});

test("derives expected counts from the validated dump", async () => {
  assert.deepEqual(expectedCounts(await fixture()), { people: 5, movies: 2, acted_in: 6, directed: 2, produced: 2 });
});
