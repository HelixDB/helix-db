import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server } from "node:http";
import { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { stringifyJson } from "../src/dsl.js";
import { Client, QueryRequest, HelixError, SourcePredicate, g, readBatch, writeBatch } from "../src/index.js";

interface CapturedRequest {
  method: string;
  path: string;
  headers: IncomingMessage["headers"];
  body: string;
}

interface CaptureServer {
  base: string;
  captured: Promise<CapturedRequest>;
  requestCount: () => number;
  close: () => Promise<void>;
}

/**
 * Spawn a one-shot HTTP server on a random port that captures the first request
 * and replies with the supplied status/body. Analogue of the Rust
 * `spawn_capture_server` helper in `lib.rs`.
 */
function spawnCaptureServer(response: { status?: number; body?: string; dropResponse?: boolean } = {}): Promise<CaptureServer> {
  return new Promise((resolveServer) => {
    let requestCount = 0;
    const server: Server = createServer((req, res) => {
      requestCount += 1;
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        resolveCaptured({
          method: req.method ?? "",
          path: req.url ?? "",
          headers: req.headers,
          body: Buffer.concat(chunks).toString("utf8"),
        });
        if (response.dropResponse) {
          res.destroy();
          return;
        }
        res.writeHead(response.status ?? 200, { "Content-Type": "application/json" });
        res.end(response.body ?? "{}");
      });
    });

    let resolveCaptured!: (value: CapturedRequest) => void;
    const captured = new Promise<CapturedRequest>((resolve) => {
      resolveCaptured = resolve;
    });

    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      resolveServer({
        base: `http://127.0.0.1:${port}`,
        captured,
        requestCount: () => requestCount,
        close: () => new Promise<void>((resolve) => server.close(() => resolve())),
      });
    });
  });
}

function sampleRequest(): QueryRequest {
  return QueryRequest.read(
    readBatch()
      .varAs("user", g().nWhere(SourcePredicate.eq("username", "alice")))
      .returning(["user"]),
  );
}

function sampleWriteRequest(): QueryRequest {
  return QueryRequest.write(
    writeBatch()
      .varAs("created", g().addN("User", { name: "Ada" }))
      .returning(["created"]),
  );
}

async function remoteError(status: number, body: string): Promise<HelixError> {
  const server = await spawnCaptureServer({ status, body });
  const client = new Client(server.base);
  try {
    await client.query(sampleRequest()).send();
    assert.fail("non-success response should return a remote error");
  } catch (error) {
    assert.ok(error instanceof HelixError);
    return error;
  } finally {
    await server.close();
  }
}

async function withFakeNativeModule<T>(run: (moduleUrl: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "helixdb-ts-native-"));
  const modulePath = join(dir, "native.mjs");
  await writeFile(
    modulePath,
    `
export const calls = [];
export const queryBodies = [];
export const cypherBodies = [];
let closed = false;
let queryError;
const missingMethods = new Set();
export const wasClosed = () => closed;
export const setQueryError = (error, msg) => {
  queryError = { error, msg };
};
// Simulate an older native package; applies to handles opened afterwards.
export const removeMethod = (name) => missingMethods.add(name);

export const HelixDbSource = {
  InMemory: (database) => ({ variant: "InMemory", database }),
  Disk: (root, database) => ({ variant: "Disk", root, database }),
  ObjectStorage: (database, bucket, region, endpoint, allowHttp) => ({
    variant: "ObjectStorage",
    database,
    bucket,
    region,
    endpoint,
    allowHttp,
  }),
};

export const EmbeddedCacheMode = {
  VectorMemoryOnly: () => ({ variant: "VectorMemoryOnly" }),
  Memory: () => ({ variant: "Memory" }),
  Hybrid: (slateMemoryBytes, slateDiskPath, slateDiskBytes, objectStoreDiskPath, objectStoreDiskBytes) => ({
    variant: "Hybrid",
    slateMemoryBytes,
    slateDiskPath,
    slateDiskBytes,
    objectStoreDiskPath,
    objectStoreDiskBytes,
  }),
};

function handle() {
  const native = {
    async query_json(request) {
      queryBodies.push(new TextDecoder().decode(request));
      if (queryError !== undefined) throw Object.assign(new Error(queryError.msg), queryError);
      return new TextEncoder().encode('{"users":0}');
    },
    // Both Cypher methods read instance state through \`this\`, like UniFFI handles.
    marker: "native-handle",
    async cypher_json(request) {
      cypherBodies.push(["cypher", this.marker, new TextDecoder().decode(request)]);
      if (queryError !== undefined) throw Object.assign(new Error(queryError.msg), queryError);
      return new TextEncoder().encode('{"columns":["x"],"rows":[[1]]}');
    },
    async explain_cypher_json(request) {
      cypherBodies.push(["explain", this.marker, new TextDecoder().decode(request)]);
      if (queryError !== undefined) throw Object.assign(new Error(queryError.msg), queryError);
      return new TextEncoder().encode('{"effect":"Write","operators":[{"position":0,"rows":-9007199254740993}],"notices":[]}');
    },
    async close() {
      closed = true;
    },
  };
  for (const name of missingMethods) delete native[name];
  return native;
}

export const HelixDB = {
  async open(source) {
    calls.push(["open", source]);
    return handle();
  },
  async open_reader(source) {
    calls.push(["open_reader", source]);
    return handle();
  },
  async open_with_config(source, cache) {
    calls.push(["open_with_config", source, cache]);
    return handle();
  },
  async open_reader_with_config(source, cache) {
    calls.push(["open_reader_with_config", source, cache]);
    return handle();
  },
};
`,
  );
  const previous = process.env.HELIXDB_EMBEDDED_NODE_PACKAGE;
  const previousLegacy = process.env.HELIXDB_UNIFFI_NODE_PACKAGE;
  process.env.HELIXDB_EMBEDDED_NODE_PACKAGE = pathToFileURL(modulePath).href;
  process.env.HELIXDB_UNIFFI_NODE_PACKAGE = pathToFileURL(join(dir, "missing-legacy.mjs")).href;
  try {
    return await run(process.env.HELIXDB_EMBEDDED_NODE_PACKAGE);
  } finally {
    if (previous === undefined) delete process.env.HELIXDB_EMBEDDED_NODE_PACKAGE;
    else process.env.HELIXDB_EMBEDDED_NODE_PACKAGE = previous;
    if (previousLegacy === undefined) delete process.env.HELIXDB_UNIFFI_NODE_PACKAGE;
    else process.env.HELIXDB_UNIFFI_NODE_PACKAGE = previousLegacy;
    await rm(dir, { recursive: true, force: true });
  }
}

// ---- Client construction ----------------------------------------------------

{
  const client = new Client();
  assert.equal(client.baseUrl, "http://localhost:6969/");
}

{
  const client = new Client("https://cluster.helix-db.com");
  assert.equal(client.baseUrl, "https://cluster.helix-db.com/");
}

assert.throws(
  () => new Client("not a url"),
  (error: unknown) => error instanceof HelixError && error.kind === "InvalidUrl",
);

// ---- Request routing + headers ----------------------------------------------

{
  const server = await spawnCaptureServer();
  const client = new Client(server.base).withApiKey("hx_secret");
  const result = await client.requestBuilder<Record<string, unknown>>().warmOnly().writerOnly().query(sampleRequest()).send();

  const req = await server.captured;
  await server.close();

  assert.equal(req.method, "POST");
  assert.equal(req.path, "/v2/query");
  assert.equal(req.headers["content-type"], "application/json");
  assert.equal(req.headers["authorization"], "Bearer hx_secret");
  assert.equal(req.headers["x-helix-warm"], "true");
  assert.equal(req.headers["x-helix-require-writer"], "true");
  assert.equal(req.body, sampleRequest().toJsonString());
  assert.deepEqual(result, {});
}

// ---- Durability header -------------------------------------------------------

{
  const server = await spawnCaptureServer({ body: '{"ok":true}' });
  const client = new Client(server.base);
  const result = await client.requestBuilder<Record<string, unknown>>().shouldAwaitDurability(false).query(sampleRequest()).send();

  const req = await server.captured;
  await server.close();

  assert.equal(req.path, "/v2/query");
  assert.equal(req.headers["x-helix-await-durable"], "false");
  assert.equal(req.headers["authorization"], undefined);
  assert.equal(req.body, sampleRequest().toJsonString());
  assert.deepEqual(result, { ok: true });
}

// ---- Cloud warm 204 is a successful empty response -------------------------

{
  const server = await spawnCaptureServer({ status: 204, body: "" });
  const client = new Client(server.base);
  const result = await client.requestBuilder<void>().warmOnly().query(sampleRequest()).send();

  const req = await server.captured;
  await server.close();

  assert.equal(req.headers["x-helix-warm"], "true");
  assert.equal(result, undefined);
}

// ---- Empty HTTP 200 still fails response deserialization -------------------

{
  const server = await spawnCaptureServer({ status: 200, body: "" });
  const client = new Client(server.base);

  await assert.rejects(
    client.query(sampleRequest()).send(),
    (error: unknown) => error instanceof HelixError && error.kind === "Serialization",
  );
  await server.close();
}

// ---- Other non-success responses surface a remote error --------------------

{
  const body = '{"error":"write conflict","code":"write_conflict","details":{"retryable":true}}';
  const error = await remoteError(409, body);

  assert.equal(error.kind, "Remote");
  assert.equal(error.statusCode, 409);
  assert.equal(error.code, "write_conflict");
  assert.equal(error.serverMessage, "write conflict");
  assert.deepEqual(error.serverDetails, { retryable: true });
  assert.equal(error.rawBody, body);
  assert.equal(error.details, "write conflict");
  assert.equal(error.isConflict(), true);
  assert.equal(error.isRateLimited(), false);
  assert.equal(error.retryable, undefined);
  assert.equal(error.isRetryable(), false);
}

for (const message of ["db error: Storage error: Transaction error: transaction conflict", "The asset changed concurrently"]) {
  const body = JSON.stringify({ error: "transaction_conflict", msg: message });
  const server = await spawnCaptureServer({ status: 409, body });
  try {
    await assert.rejects(
      new Client(server.base).query(sampleWriteRequest()).send(),
      (error: unknown) =>
        error instanceof HelixError &&
        error.kind === "Remote" &&
        error.statusCode === 409 &&
        error.code === "transaction_conflict" &&
        error.serverMessage === message &&
        error.rawBody === body &&
        error.isConflict(),
    );
    assert.equal(server.requestCount(), 1);
  } finally {
    await server.close();
  }
}

{
  // The server consumed the mutation before the connection disappeared. The
  // client cannot infer an abort or safely replay the write from this failure.
  const server = await spawnCaptureServer({ dropResponse: true });
  try {
    await assert.rejects(
      new Client(server.base).query(sampleWriteRequest()).send(),
      (error: unknown) =>
        error instanceof HelixError &&
        error.kind === "Network" &&
        error.statusCode === undefined &&
        error.code === undefined &&
        !error.isConflict() &&
        !error.isRetryable(),
    );
    assert.equal(server.requestCount(), 1);
  } finally {
    await server.close();
  }
}

for (const [body, expectedCode] of [
  [
    '{"error":"writer_fenced_commit_outcome_unknown","msg":"write outcome is unknown","retryable":false}',
    "writer_fenced_commit_outcome_unknown",
  ],
  ['{"error":"write_outcome_unknown","msg":"write outcome is unknown; the operation was not replayed","retryable":false}', "write_outcome_unknown"],
  // Helix Cloud gateways before the shared envelope.
  ['{"code":"WRITE_OUTCOME_UNKNOWN","error":"write outcome is unknown","retryable":false}', "WRITE_OUTCOME_UNKNOWN"],
] as const) {
  const server = await spawnCaptureServer({ status: 503, body });
  const client = new Client(server.base);
  await assert.rejects(
    client.query(sampleWriteRequest()).send(),
    (error: unknown) =>
      error instanceof HelixError &&
      error.code === expectedCode &&
      error.retryable === false &&
      !error.isConflict() &&
      !error.isRetryable(),
  );
  await server.captured;
  assert.equal(server.requestCount(), 1);
  await server.close();
}

for (const [retryable, expected] of [
  [true, true],
  [false, false],
  ["true", false],
  [undefined, false],
] as const) {
  const body = JSON.stringify({ error: "classified", ...(retryable === undefined ? {} : { retryable }) });
  const error = await remoteError(503, body);
  assert.equal(error.isRetryable(), expected);
}

{
  const body = '{"error":"index_backpressure","msg":"index backpressure","retryable":true}';
  const error = await remoteError(429, body);

  assert.equal(error.code, "index_backpressure");
  assert.equal(error.isRateLimited(), true);
  assert.equal(error.isRetryable(), true);
  assert.equal(error.isIndexBackpressure(), true);
  assert.equal(error.isConflict(), false);
  assert.equal(HelixError.embedded("index backpressure", "index_backpressure").isRetryable(), true);
  assert.equal(HelixError.embedded("bad input", "invalid_query").isRetryable(), false);
}

for (const status of [400, 401, 403, 409, 429, 503]) {
  const body = JSON.stringify({ message: `status ${status}`, code: "test_error" });
  const error = await remoteError(status, body);

  assert.equal(error.statusCode, status);
  assert.equal(error.code, "test_error");
  assert.equal(error.serverMessage, `status ${status}`);
  assert.equal(error.rawBody, body);
  assert.equal(error.isConflict(), status === 409);
  assert.equal(error.isRateLimited(), status === 429);
}

{
  const error = await remoteError(500, "upstream failed");

  assert.equal(error.statusCode, 500);
  assert.equal(error.code, undefined);
  assert.equal(error.serverMessage, "upstream failed");
  assert.equal(error.serverDetails, undefined);
  assert.equal(error.rawBody, "upstream failed");
  assert.equal(error.details, "upstream failed");
}

{
  const body = '{"message":"write conflict","code":42,"details":null}';
  const error = await remoteError(409, body);

  assert.equal(error.code, undefined);
  assert.equal(error.serverMessage, "write conflict");
  assert.equal(error.serverDetails, null);
  assert.equal(error.rawBody, body);
}

{
  const error = await remoteError(503, "");

  assert.equal(error.serverMessage, "Service Unavailable");
  assert.equal(error.rawBody, "");
  assert.equal(error.details, "Service Unavailable");
}

{
  const error = HelixError.remote("legacy remote error", "legacy_code");

  assert.equal(error.kind, "Remote");
  assert.equal(error.details, "legacy remote error");
  assert.equal(error.code, "legacy_code");
  assert.equal(error.statusCode, undefined);
}

for (const testCase of [
  {
    body: '{"error":"index_not_found","msg":"missing index"}',
    code: "index_not_found",
    details: "missing index",
  },
  {
    body: '{"error":"legacy message","code":"index_not_found"}',
    code: "index_not_found",
    details: "legacy message",
  },
  {
    body: '{"error":"legacy message","code":"index_not_found","message":"generic message"}',
    code: "index_not_found",
    details: "legacy message",
  },
  {
    body: '{"error":"future_code","msg":"future message"}',
    code: "future_code",
    details: "future message",
  },
  { body: '{"error":"message without code"}', code: undefined, details: "message without code" },
  { body: "not JSON", code: undefined, details: "not JSON" },
]) {
  const server = await spawnCaptureServer({ status: 500, body: testCase.body });
  const client = new Client(server.base);
  await assert.rejects(
    client.query(sampleRequest()).send(),
    (error: unknown) =>
      error instanceof HelixError && error.kind === "Remote" && error.code === testCase.code && error.details === testCase.details,
  );
  await server.close();
}

// ---- Unreachable server surfaces an actionable network error ----------------

{
  const client = new Client("http://127.0.0.1:1");
  await assert.rejects(
    client.query(sampleRequest()).send(),
    (error: unknown) =>
      error instanceof HelixError &&
      error.kind === "Network" &&
      error.message.includes("http://127.0.0.1:1/v2/query") &&
      error.message.includes("helix start"),
  );
}

// ---- Embedded execution -----------------------------------------------------

await withFakeNativeModule(async (moduleUrl) => {
  const client = await Client.embedded({ kind: "inMemory", database: "ts-sdk-embedded" });
  const result = await client
    .query<{ users: number }>(QueryRequest.read(readBatch().varAs("users", g().nWithLabel("Missing").count()).returning(["users"])))
    .send();
  await client.close();
  const native = (await import(moduleUrl)) as {
    calls: unknown[];
    queryBodies: string[];
    wasClosed: () => boolean;
  };

  assert.deepEqual(result, { users: 0 });
  assert.deepEqual(native.calls[0], ["open", { variant: "InMemory", database: "ts-sdk-embedded" }]);
  assert.equal(JSON.parse(native.queryBodies[0]).request_type, "read");
  assert.equal(native.wasClosed(), true);
});

await withFakeNativeModule(async (moduleUrl) => {
  const client = await Client.embedded({ kind: "inMemory", database: "ts-sdk-embedded-error" });
  const native = (await import(moduleUrl)) as { setQueryError: (error: string, msg: string) => void };
  native.setQueryError("index_not_found", "missing text index");

  await assert.rejects(
    client.query(sampleRequest()).send(),
    (error: unknown) =>
      error instanceof HelixError &&
      error.kind === "Embedded" &&
      error.code === "index_not_found" &&
      error.details === "missing text index",
  );
});

await withFakeNativeModule(async (moduleUrl) => {
  const client = await Client.embedded(
    { kind: "inMemory", database: "ts-sdk-hybrid" },
    {
      vectorMemoryBytes: 1024,
      mode: {
        kind: "hybrid",
        slateMemoryBytes: 2048,
        slateDiskPath: "/tmp/slate",
        slateDiskBytes: 4096,
        objectStoreDiskPath: "/tmp/object",
        objectStoreDiskBytes: 8192,
      },
    },
  );
  const native = (await import(moduleUrl)) as { calls: unknown[] };
  assert.deepEqual(native.calls[0], [
    "open_with_config",
    { variant: "InMemory", database: "ts-sdk-hybrid" },
    {
      vector_memory_bytes: 1024,
      mode: {
        variant: "Hybrid",
        slateMemoryBytes: 2048,
        slateDiskPath: "/tmp/slate",
        slateDiskBytes: 4096,
        objectStoreDiskPath: "/tmp/object",
        objectStoreDiskBytes: 8192,
      },
    },
  ]);
  await client.close();
});

await withFakeNativeModule(async (moduleUrl) => {
  const client = await Client.embeddedReader({ kind: "disk", root: "/tmp/helix", database: "ts-sdk-reader" });
  const native = (await import(moduleUrl)) as { calls: unknown[] };

  assert.deepEqual(native.calls[0], ["open_reader", { variant: "Disk", root: "/tmp/helix", database: "ts-sdk-reader" }]);
  await client.close();
});

await withFakeNativeModule(async () => {
  const client = await Client.embedded({ kind: "objectStorage", database: "ts-sdk-os", bucket: "bucket", region: "region" });

  await assert.rejects(
    client.requestBuilder().warmOnly().query(sampleRequest()).send(),
    (error: unknown) => error instanceof HelixError && error.kind === "InvalidRequest" && error.details?.includes("x-helix-warm") === true,
  );
});

// ---- Cypher over HTTP -------------------------------------------------------

{
  const result = { columns: ["x"], rows: [[{ $type: "integer", value: "9223372036854775807" }]] };
  const server = await spawnCaptureServer({ body: JSON.stringify(result) });
  try {
    const client = new Client(server.base).withApiKey("local-test").withDatabaseId("db_cypher");
    assert.deepEqual(await client.cypher("RETURN $x AS x", { x: 9223372036854775807n, f: Infinity }, "parameter"), result);
    const request = await server.captured;
    assert.equal(request.method, "POST");
    assert.equal(request.path, "/v2/cypher");
    assert.equal(request.headers["content-type"], "application/json");
    assert.equal(request.headers.authorization, "Bearer local-test");
    assert.equal(request.headers["x-helix-database-id"], "db_cypher");
    for (const option of ["x-helix-warm", "x-helix-require-writer", "x-helix-await-durable"]) {
      assert.equal(request.headers[option], undefined);
    }
    assert.deepEqual(JSON.parse(request.body), {
      query: "RETURN $x AS x",
      parameters: { x: { $type: "integer", value: "9223372036854775807" }, f: { $type: "float", value: "Infinity" } },
      query_name: "parameter",
    });
    assert.equal(server.requestCount(), 1);
  } finally {
    await server.close();
  }
}

{
  const result = { columns: ["n"], rows: [[{ $type: "node", id: "1", labels: ["User"], properties: { name: "Ada" } }]] };
  const server = await spawnCaptureServer({ body: JSON.stringify(result) });
  try {
    const client = new Client(server.base).withApiKey("hx_secret").withDatabaseId("db_writer");
    const response = await client
      .requestBuilder()
      .writerOnly()
      .shouldAwaitDurability(true)
      .cypher("CREATE (n:User {name: $name}) RETURN n", { name: "Ada" }, "create_user")
      .send();
    const request = await server.captured;
    assert.deepEqual(response, result);
    assert.equal(request.path, "/v2/cypher");
    assert.equal(request.headers.authorization, "Bearer hx_secret");
    assert.equal(request.headers["x-helix-database-id"], "db_writer");
    assert.equal(request.headers["x-helix-require-writer"], "true");
    assert.equal(request.headers["x-helix-await-durable"], "true");
    assert.equal(request.headers["x-helix-warm"], undefined);
    assert.deepEqual(JSON.parse(request.body), {
      query: "CREATE (n:User {name: $name}) RETURN n",
      parameters: { name: "Ada" },
      query_name: "create_user",
    });
  } finally {
    await server.close();
  }
}

{
  // The explanation is returned as the server encoded it, including untyped planner fields.
  const explanation = {
    effect: "Read",
    bindings: [{ name: "n" }],
    returns: [["n", 0]],
    operators: [{ position: 0, blocking: [] }],
    planner: { candidates: 1, estimated_rows: 9007199254740993n },
    notices: [{ kind: "buffered_response" }],
  };
  // Raw planner integers beyond Number.MAX_SAFE_INTEGER must survive as bigint.
  const server = await spawnCaptureServer({ body: stringifyJson(explanation) });
  try {
    const client = new Client(server.base).withApiKey("hx_secret").withDatabaseId("db_reader");
    const response = await client
      .requestBuilder()
      .warmOnly()
      .cypher("MATCH (n:User) WHERE n.score > $min RETURN n", { min: 9007199254740993n }, "explain_users")
      .explain();
    const request = await server.captured;
    assert.deepEqual(response, explanation);
    assert.equal(response.effect, "Read");
    assert.equal(request.method, "POST");
    assert.equal(request.path, "/v2/cypher/explain");
    assert.equal(request.headers["content-type"], "application/json");
    assert.equal(request.headers.authorization, "Bearer hx_secret");
    assert.equal(request.headers["x-helix-database-id"], "db_reader");
    assert.equal(request.headers["x-helix-warm"], "true");
    assert.deepEqual(JSON.parse(request.body), {
      query: "MATCH (n:User) WHERE n.score > $min RETURN n",
      parameters: { min: { $type: "integer", value: "9007199254740993" } },
      query_name: "explain_users",
    });
    assert.equal(server.requestCount(), 1);
  } finally {
    await server.close();
  }
}

{
  const explanation = { effect: "Write", operators: [] };
  const server = await spawnCaptureServer({ body: JSON.stringify(explanation) });
  try {
    const client = new Client(server.base).withDatabaseId("db_plain");
    assert.deepEqual(await client.explainCypher("CREATE (:User)"), explanation);
    const request = await server.captured;
    assert.equal(request.path, "/v2/cypher/explain");
    assert.equal(request.headers.authorization, undefined);
    assert.equal(request.headers["x-helix-database-id"], "db_plain");
    // Omitted parameters default to an empty map and an absent name is not sent.
    assert.deepEqual(JSON.parse(request.body), { query: "CREATE (:User)", parameters: {} });
  } finally {
    await server.close();
  }
}

// ---- Cloud warm 204 is a successful Cypher response without a payload -------

{
  const server = await spawnCaptureServer({ status: 204, body: "" });
  try {
    const client = new Client(server.base).withDatabaseId("db_warm");
    const response = await client.requestBuilder().warmOnly().cypher("MATCH (u:User) RETURN u").send();
    const request = await server.captured;
    assert.deepEqual(response, { columns: [], rows: [] });
    assert.equal(request.path, "/v2/cypher");
    assert.equal(request.headers["x-helix-warm"], "true");
    assert.equal(request.headers["x-helix-database-id"], "db_warm");
  } finally {
    await server.close();
  }
}

{
  const server = await spawnCaptureServer({ status: 204, body: "" });
  try {
    const client = new Client(server.base).withDatabaseId("db_warm");
    const response = await client.requestBuilder().warmOnly().cypher("MATCH (u:User) RETURN u").explain();
    const request = await server.captured;
    assert.equal(response, undefined);
    assert.equal(request.path, "/v2/cypher/explain");
    assert.equal(request.headers["x-helix-warm"], "true");
  } finally {
    await server.close();
  }
}

{
  // `explainCypher` sends no warm option, so a payload-less 204 breaks the explain contract.
  const server = await spawnCaptureServer({ status: 204, body: "" });
  try {
    await assert.rejects(
      new Client(server.base).explainCypher("MATCH (u:User) RETURN u"),
      (error: unknown) => error instanceof HelixError && error.kind === "Remote" && error.statusCode === 204,
    );
    assert.equal((await server.captured).headers["x-helix-warm"], undefined);
  } finally {
    await server.close();
  }
}

for (const explain of [false, true]) {
  const body = JSON.stringify({
    error: "syntax_error",
    msg: "parameter name cannot be empty",
    details: { detail: "invalid_parameter", phase: "compile", span: null },
  });
  const server = await spawnCaptureServer({ status: 400, body });
  try {
    const request = new Client(server.base).requestBuilder().cypher("RETURN $x AS x", { "": 1 });
    await assert.rejects(
      explain ? request.explain() : request.send(),
      (error: unknown) =>
        error instanceof HelixError &&
        error.kind === "Remote" &&
        error.statusCode === 400 &&
        error.code === "syntax_error" &&
        error.serverMessage === "parameter name cannot be empty" &&
        error.rawBody === body &&
        !error.isRetryable(),
    );
    assert.equal((await server.captured).path, explain ? "/v2/cypher/explain" : "/v2/cypher");
    assert.equal(server.requestCount(), 1);
  } finally {
    await server.close();
  }
}

{
  const body = '{"error":"tenant_id_required","msg":"x-helix-database-id is required"}';
  const server = await spawnCaptureServer({ status: 400, body });
  try {
    await assert.rejects(
      new Client(server.base).explainCypher("RETURN 1 AS x"),
      (error: unknown) => error instanceof HelixError && error.kind === "Remote" && error.code === "tenant_id_required",
    );
  } finally {
    await server.close();
  }
}

{
  const server = await spawnCaptureServer({ body: "not JSON" });
  try {
    await assert.rejects(
      new Client(server.base).explainCypher("RETURN 1 AS x"),
      (error: unknown) => error instanceof HelixError && error.kind === "Serialization",
    );
  } finally {
    await server.close();
  }
}

{
  const client = new Client("http://127.0.0.1:1");
  await assert.rejects(
    client.explainCypher("RETURN 1 AS x"),
    (error: unknown) =>
      error instanceof HelixError && error.kind === "Network" && error.message.includes("http://127.0.0.1:1/v2/cypher/explain"),
  );

  // Request serialization fails before any transport is attempted.
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  for (const run of [() => client.cypher("RETURN $x AS x", { x: cyclic }), () => client.explainCypher("RETURN $x AS x", { x: cyclic })]) {
    await assert.rejects(run(), (error: unknown) => error instanceof HelixError && error.kind === "Serialization");
  }
}

// ---- Cypher embedded --------------------------------------------------------

type FakeNative = {
  cypherBodies: [string, string, string][];
  setQueryError: (error: string, msg: string) => void;
  removeMethod: (name: string) => void;
};

await withFakeNativeModule(async (moduleUrl) => {
  const native = (await import(moduleUrl)) as FakeNative;
  const client = await Client.embedded({ kind: "inMemory", database: "ts-sdk-cypher" });

  assert.deepEqual(await client.cypher("RETURN $x AS x", { x: 1 }, "embedded"), { columns: ["x"], rows: [[1]] });
  assert.deepEqual(await client.requestBuilder().cypher("CREATE (:User)", { big: -9007199254740993n }).explain(), {
    effect: "Write",
    operators: [{ position: 0, rows: -9007199254740993n }],
    notices: [],
  });
  assert.deepEqual(await client.explainCypher("RETURN 1 AS x"), {
    effect: "Write",
    operators: [{ position: 0, rows: -9007199254740993n }],
    notices: [],
  });
  assert.deepEqual(
    native.cypherBodies.map(([method, marker, body]) => [method, marker, JSON.parse(body)]),
    [
      ["cypher", "native-handle", { query: "RETURN $x AS x", parameters: { x: 1 }, query_name: "embedded" }],
      ["explain", "native-handle", { query: "CREATE (:User)", parameters: { big: { $type: "integer", value: "-9007199254740993" } } }],
      ["explain", "native-handle", { query: "RETURN 1 AS x", parameters: {} }],
    ],
  );

  // Server request options are rejected before reaching the native handle.
  for (const [builder, option] of [
    [client.requestBuilder().writerOnly(), "x-helix-require-writer"],
    [client.requestBuilder().warmOnly(), "x-helix-warm"],
    [client.requestBuilder().shouldAwaitDurability(false), "x-helix-await-durable"],
  ] as const) {
    for (const run of [() => builder.cypher("RETURN 1 AS x").send(), () => builder.cypher("RETURN 1 AS x").explain()]) {
      await assert.rejects(
        run(),
        (error: unknown) =>
          error instanceof HelixError &&
          error.kind === "InvalidRequest" &&
          error.details === `embedded queries do not support server request options: ${option}`,
      );
    }
  }
  assert.equal(native.cypherBodies.length, 3);

  native.setQueryError("syntax_error:compile:unexpected_end", "unexpected token");
  for (const run of [() => client.cypher("RETURN"), () => client.explainCypher("RETURN")]) {
    await assert.rejects(
      run(),
      (error: unknown) =>
        error instanceof HelixError && error.kind === "Embedded" &&
        error.code === "syntax_error:compile:unexpected_end" &&
        error.details === "unexpected token",
    );
  }
  await client.close();
});

await withFakeNativeModule(async (moduleUrl) => {
  const native = (await import(moduleUrl)) as FakeNative;
  native.removeMethod("explain_cypher_json");
  const client = await Client.embedded({ kind: "inMemory", database: "ts-sdk-cypher-no-explain" });

  assert.deepEqual(await client.cypher("RETURN 1 AS x"), { columns: ["x"], rows: [[1]] });
  await assert.rejects(
    client.explainCypher("RETURN 1 AS x"),
    (error: unknown) =>
      error instanceof HelixError &&
      error.kind === "EmbeddedUnavailable" &&
      error.details === "rebuild native bindings with Cypher explain support",
  );
  await client.close();
});

await withFakeNativeModule(async (moduleUrl) => {
  const native = (await import(moduleUrl)) as FakeNative;
  native.removeMethod("cypher_json");
  native.removeMethod("explain_cypher_json");
  const client = await Client.embedded({ kind: "inMemory", database: "ts-sdk-no-cypher" });

  await assert.rejects(
    client.cypher("RETURN 1 AS x"),
    (error: unknown) =>
      error instanceof HelixError &&
      error.kind === "EmbeddedUnavailable" &&
      error.details === "rebuild native bindings with Cypher support",
  );
  assert.deepEqual(native.cypherBodies, []);
  await client.close();
});

console.log("client.test.ts passed");
