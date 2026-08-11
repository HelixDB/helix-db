import assert from "node:assert/strict";
import test from "node:test";
import { QueryRequest, g, readBatch } from "@helix-db/helix-db";
import { HelixHttpClient, HelixHttpError } from "../src/helix-http.js";
import {
  bootstrapIndexesRequest,
  graphCountsRequest,
  movieDetailsRequest,
  personFilmographyRequest,
  replaceRelationshipsRequest,
  upsertMoviesRequest,
  upsertPeopleRequest,
} from "../src/helix-queries.js";

const countRequest = QueryRequest.read(readBatch().varAs("count", g().nWithLabel("Movie").count()).returning(["count"]));

test("retries 409 responses with bounded exponential backoff", async () => {
  const responses = [new Response("conflict", { status: 409 }), new Response("conflict", { status: 409 }), new Response('{"count":2}', { status: 200 })];
  const delays: number[] = [];
  const calls: RequestInit[] = [];
  const client = new HelixHttpClient(
    "http://localhost:16969",
    async (_input, init) => {
      calls.push(init!);
      return responses.shift()!;
    },
    async (milliseconds) => {
      delays.push(milliseconds);
    },
  );
  assert.deepEqual(await client.execute(countRequest, { attempts: 3, baseDelayMs: 5, awaitDurability: true }), { count: 2 });
  assert.deepEqual(delays, [5, 10]);
  assert.equal(calls.length, 3);
  assert.equal((calls[0]!.headers as Record<string, string>)["x-helix-await-durable"], "true");
  assert.equal(calls[0]!.body, calls[2]!.body);
});

test("does not retry permanent errors or an exhausted conflict", async () => {
  const permanent = new HelixHttpClient("http://localhost:16969", async () => new Response("bad query", { status: 400 }));
  await assert.rejects(permanent.execute(countRequest), (error) => error instanceof HelixHttpError && error.status === 400);
  const conflict = new HelixHttpClient("http://localhost:16969", async () => new Response("conflict", { status: 409 }), async () => {});
  await assert.rejects(conflict.execute(countRequest, { attempts: 2 }), (error) => error instanceof HelixHttpError && error.status === 409);
  await assert.rejects(conflict.execute(countRequest, { attempts: 0 }), /positive integer/);
  await assert.rejects(conflict.execute(countRequest, { baseDelayMs: -1 }), /non-negative integer/);
});

test("returns undefined for an empty successful response and rejects malformed JSON", async () => {
  const empty = new HelixHttpClient("http://localhost:16969", async () => new Response(null, { status: 200 }));
  assert.equal(await empty.execute(countRequest), undefined);
  const malformed = new HelixHttpClient("http://localhost:16969", async () => new Response("not-json", { status: 200 }));
  await assert.rejects(malformed.execute(countRequest), SyntaxError);
});

test("serializes both migration reads as parameterized direct requests", () => {
  const movie = JSON.parse(movieDetailsRequest("m-matrix").toJsonString()) as Record<string, unknown>;
  assert.equal(movie.request_type, "read");
  assert.equal(movie.query_name, "movie_details");
  assert.deepEqual(movie.parameters, { movie_id: "m-matrix" });
  assert.deepEqual(movie.parameter_types, { movie_id: "string" });
  assert.match(JSON.stringify(movie), /\"in_e\"/);
  assert.match(JSON.stringify(movie), /\$from.personId/);

  const filmography = JSON.parse(personFilmographyRequest("p-keanu").toJsonString()) as Record<string, unknown>;
  assert.equal(filmography.query_name, "person_filmography");
  assert.deepEqual(filmography.parameters, { person_id: "p-keanu" });
  assert.match(JSON.stringify(filmography), /project_bindings/);
  assert.match(JSON.stringify(filmography), /\"binding\":\"credit\"/);
});

test("serializes replay-safe writes, indexes, and count verification", () => {
  const person = { kind: "person" as const, personId: "p-1", name: "Person", born: 1970 };
  const movie = { kind: "movie" as const, movieId: "m-1", title: "Movie", released: 2000, tagline: "Tagline" };
  const relationship = {
    kind: "acted_in" as const,
    relationshipId: "r-1",
    fromPersonId: "p-1",
    toMovieId: "m-1",
    roles: ["Lead"],
  };
  const requests = [
    bootstrapIndexesRequest(),
    upsertPeopleRequest([person]),
    upsertMoviesRequest([movie]),
    replaceRelationshipsRequest("acted_in", [relationship]),
    graphCountsRequest(),
  ];
  for (const request of requests) assert.doesNotThrow(() => JSON.parse(request.toJsonString()));
  assert.match(requests[1]!.toJsonString(), /for_each/);
  assert.match(requests[3]!.toJsonString(), /var_not_empty/);
  assert.match(requests[3]!.toJsonString(), /drop/);
});
