import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server } from "node:http";
import { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { Client, QueryRequest, HelixError, SourcePredicate, g, readBatch } from "../src/index.js";

interface CapturedRequest {
  method: string;
  path: string;
  headers: IncomingMessage["headers"];
  body: string;
}

interface CaptureServer {
  base: string;
  captured: Promise<CapturedRequest>;
  close: () => Promise<void>;
}

/**
 * Spawn a one-shot HTTP server on a random port that captures the first request
 * and replies with the supplied status/body. Analogue of the Rust
 * `spawn_capture_server` helper in `lib.rs`.
 */
function spawnCaptureServer(response: { status?: number; body?: string } = {}): Promise<CaptureServer> {
  return new Promise((resolveServer) => {
    const server: Server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        resolveCaptured({
          method: req.method ?? "",
          path: req.url ?? "",
          headers: req.headers,
          body: Buffer.concat(chunks).toString("utf8"),
        });
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
let closed = false;
let queryError;
export const wasClosed = () => closed;
export const setQueryError = (error, msg) => {
  queryError = { error, msg };
};

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
  return {
    async query_json(request) {
      queryBodies.push(new TextDecoder().decode(request));
      if (queryError !== undefined) throw Object.assign(new Error(queryError.msg), queryError);
      return new TextEncoder().encode('{"users":0}');
    },
    async close() {
      closed = true;
    },
  };
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

for (const databaseId of ["", "   "]) {
  assert.throws(
    () => Client.managed("https://cluster.helix-db.com", databaseId),
    (error: unknown) => error instanceof HelixError && error.kind === "InvalidRequest",
  );
}

// ---- Request routing + headers ----------------------------------------------

{
  const server = await spawnCaptureServer();
  const client = Client.managed(server.base, "db_123").withApiKey("hx_secret");
  const result = await client.requestBuilder<Record<string, unknown>>().warmOnly().writerOnly().query(sampleRequest()).send();

  const req = await server.captured;
  await server.close();

  assert.equal(req.method, "POST");
  assert.equal(req.path, "/v2/query");
  assert.equal(req.headers["content-type"], "application/json");
  assert.equal(req.headers["authorization"], "Bearer hx_secret");
  assert.equal(req.headers["x-helix-database-id"], "db_123");
  assert.equal(req.headers["x-helix-warm"], "true");
  assert.equal(req.headers["x-helix-require-writer"], "true");
  assert.equal(req.body, sampleRequest().toJsonString());
  assert.deepEqual(result, {});
}

{
  const server = await spawnCaptureServer();
  const client = new Client(server.base).withDatabaseId("db_123");
  await client.query(sampleRequest()).send();
  const req = await server.captured;
  await server.close();
  assert.equal(req.headers["x-helix-database-id"], "db_123");
}

{
  const server = await spawnCaptureServer();
  const client = Client.managed(server.base, "db_initial");
  const request = client.query(sampleRequest());
  client.withDatabaseId("db_later");
  await request.send();
  const req = await server.captured;
  await server.close();
  assert.equal(req.headers["x-helix-database-id"], "db_initial");
}

{
  const server = await spawnCaptureServer();
  const client = new Client(server.base).withDatabaseId(" ");
  await assert.rejects(
    client.query(sampleRequest()).send(),
    (error: unknown) => error instanceof HelixError && error.kind === "InvalidRequest",
  );
  await server.close();
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

// ---- Non-200 response surfaces a remote error -------------------------------

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

console.log("client.test.ts passed");
