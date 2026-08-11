import {
  QueryRequest,
  g,
  parseIndexDdlReceipt,
  parseIndexOperationStatus,
  readBatch,
  type IndexOperationId,
} from "@helix-db/helix-db";
import { chunks } from "./dump.js";
import { HelixHttpClient } from "./helix-http.js";
import {
  bootstrapIndexesRequest,
  replaceRelationshipsRequest,
  upsertMoviesRequest,
  upsertPeopleRequest,
} from "./helix-queries.js";
import type {
  ActedInRelationship,
  DirectedRelationship,
  MovieGraphDumpV1,
  MovieNode,
  PersonNode,
  ProducedRelationship,
} from "./model.js";

export type LoadOptions = {
  batchSize: number;
  stopAfterNodes?: boolean;
  indexTimeoutMs?: number;
  indexPollMs?: number;
};

export type LoadProgress = {
  people: number;
  movies: number;
  relationships: number;
  stoppedAfterNodes: boolean;
};

function collectReceipts(
  value: unknown,
  operationIds = new Set<IndexOperationId>(),
): Set<IndexOperationId> {
  if (Array.isArray(value)) {
    for (const entry of value) collectReceipts(entry, operationIds);
    return operationIds;
  }
  if (value === null || typeof value !== "object") return operationIds;
  const record = value as Record<string, unknown>;
  if (
    record.kind === "accepted" ||
    record.kind === "existing_operation" ||
    record.kind === "already_active"
  ) {
    const receipt = parseIndexDdlReceipt(record);
    if (receipt.kind !== "already_active")
      operationIds.add(receipt.operation_id);
  }
  for (const entry of Object.values(record))
    collectReceipts(entry, operationIds);
  return operationIds;
}

async function awaitIndex(
  client: HelixHttpClient,
  operationId: IndexOperationId,
  timeoutMs: number,
  pollMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const request = QueryRequest.read(
      readBatch()
        .varAs("status", g().getIndexOperation(operationId))
        .returning(["status"]),
      "index_operation_status",
    );
    const response = await client.execute(request);
    if (
      response === null ||
      typeof response !== "object" ||
      !("status" in response)
    ) {
      throw new TypeError(
        `index operation response is missing status for ${operationId}`,
      );
    }
    const status = parseIndexOperationStatus(
      (response as Record<string, unknown>).status,
    );
    if (status.status === "succeeded") return;
    if (status.status === "blocked" || status.status === "aborted") {
      throw new Error(
        `index operation ${operationId} ended with ${status.status}`,
      );
    }
    if (Date.now() >= deadline)
      throw new Error(
        `index operation ${operationId} did not finish within ${timeoutMs}ms`,
      );
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}

export async function ensureIndexes(
  client: HelixHttpClient,
  timeoutMs = 60_000,
  pollMs = 25,
): Promise<void> {
  const response = await client.execute(bootstrapIndexesRequest(), {
    awaitDurability: true,
  });
  for (const operationId of collectReceipts(response))
    await awaitIndex(client, operationId, timeoutMs, pollMs);
}

async function loadBatches<T>(
  items: readonly T[],
  batchSize: number,
  request: (batch: readonly T[]) => QueryRequest,
  client: HelixHttpClient,
) {
  for (const batch of chunks(items, batchSize))
    await client.execute(request(batch), { awaitDurability: true });
}

export async function loadDump(
  dump: MovieGraphDumpV1,
  client: HelixHttpClient,
  options: LoadOptions,
): Promise<LoadProgress> {
  await ensureIndexes(client, options.indexTimeoutMs, options.indexPollMs);

  const people = dump.nodes.filter(
    (node): node is PersonNode => node.kind === "person",
  );
  const movies = dump.nodes.filter(
    (node): node is MovieNode => node.kind === "movie",
  );
  await loadBatches(people, options.batchSize, upsertPeopleRequest, client);
  await loadBatches(movies, options.batchSize, upsertMoviesRequest, client);

  if (options.stopAfterNodes === true) {
    return {
      people: people.length,
      movies: movies.length,
      relationships: 0,
      stoppedAfterNodes: true,
    };
  }

  const actedIn = dump.relationships.filter(
    (relationship): relationship is ActedInRelationship =>
      relationship.kind === "acted_in",
  );
  const directed = dump.relationships.filter(
    (relationship): relationship is DirectedRelationship =>
      relationship.kind === "directed",
  );
  const produced = dump.relationships.filter(
    (relationship): relationship is ProducedRelationship =>
      relationship.kind === "produced",
  );
  await loadBatches(
    actedIn,
    options.batchSize,
    (batch) => replaceRelationshipsRequest("acted_in", batch),
    client,
  );
  await loadBatches(
    directed,
    options.batchSize,
    (batch) => replaceRelationshipsRequest("directed", batch),
    client,
  );
  await loadBatches(
    produced,
    options.batchSize,
    (batch) => replaceRelationshipsRequest("produced", batch),
    client,
  );

  return {
    people: people.length,
    movies: movies.length,
    relationships: dump.relationships.length,
    stoppedAfterNodes: false,
  };
}
