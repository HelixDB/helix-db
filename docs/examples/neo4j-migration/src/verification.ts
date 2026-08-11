import assert from "node:assert/strict";
import type { Driver } from "neo4j-driver";
import { HelixHttpClient } from "./helix-http.js";
import { graphCountsRequest } from "./helix-queries.js";
import type { MovieGraphDumpV1 } from "./model.js";
import { HelixMovieRepository, Neo4jMovieRepository } from "./repositories.js";

export type GraphCounts = {
  people: number;
  movies: number;
  acted_in: number;
  directed: number;
  produced: number;
};

function counts(value: unknown): GraphCounts {
  if (value === null || typeof value !== "object")
    throw new TypeError("count response must be an object");
  const record = value as Record<string, unknown>;
  const output = {
    people: record.people,
    movies: record.movies,
    acted_in: record.acted_in,
    directed: record.directed,
    produced: record.produced,
  };
  for (const [name, count] of Object.entries(output)) {
    if (!Number.isSafeInteger(count) || (count as number) < 0)
      throw new TypeError(`${name} count is invalid: ${String(count)}`);
  }
  return output as GraphCounts;
}

export async function helixCounts(
  client: HelixHttpClient,
): Promise<GraphCounts> {
  return counts(await client.execute(graphCountsRequest()));
}

export function expectedCounts(dump: MovieGraphDumpV1): GraphCounts {
  return {
    people: dump.nodes.filter((node) => node.kind === "person").length,
    movies: dump.nodes.filter((node) => node.kind === "movie").length,
    acted_in: dump.relationships.filter(
      (relationship) => relationship.kind === "acted_in",
    ).length,
    directed: dump.relationships.filter(
      (relationship) => relationship.kind === "directed",
    ).length,
    produced: dump.relationships.filter(
      (relationship) => relationship.kind === "produced",
    ).length,
  };
}

export async function verifyMigration(
  driver: Driver,
  database: string,
  client: HelixHttpClient,
  dump: MovieGraphDumpV1,
): Promise<void> {
  assert.deepEqual(await helixCounts(client), expectedCounts(dump));
  const neo4jRepository = new Neo4jMovieRepository(driver, database);
  const helixRepository = new HelixMovieRepository(client);
  assert.deepEqual(
    await helixRepository.movieDetails("m-matrix"),
    await neo4jRepository.movieDetails("m-matrix"),
  );
  assert.deepEqual(
    await helixRepository.personFilmography("p-keanu"),
    await neo4jRepository.personFilmography("p-keanu"),
  );
}
