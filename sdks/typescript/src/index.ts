// Public entry point for the Helix TypeScript SDK.
//
// The query DSL lives in `./dsl.ts` and is re-exported wholesale here. This file
// adds the network client (`Client`, `QueryBuilder`, `QueryExecutionRequest`,
// `CypherExecutionRequest`, `HelixError`), mirroring the Rust SDK layout where the
// DSL lives in `dsl.rs` and the client in `lib.rs`.

export * from "./dsl.js";
export * from "./graph.js";

import { QueryRequest, parseJson } from "./dsl.js";
import { GraphSelection, NativeGraph, loadGraph } from "./graph.js";

const DEFAULT_URL = "http://localhost:6969";
const QUERY_PATH = "/v2/query";

/** Graph IDs and large integers are lossless tagged values within each row. */
export interface CypherResponse {
  columns: string[];
  rows: unknown[][];
}

/**
 * Planning diagnostics from `POST /v2/cypher/explain`; the statement is not executed.
 *
 * Only the top-level effect and operator list are typed. Operator contracts,
 * bindings, returns, planner metrics, and notices are planner diagnostics whose
 * shape may grow between server versions.
 */
export interface CypherExplanation {
  effect: "Read" | "Write";
  operators: unknown[];
  [field: string]: unknown;
}

/** A Cypher route: its server path, and the native method serving it in embedded mode. */
interface CypherRoute {
  path: string;
  native: "cypher_json" | "explain_cypher_json";
  unavailable: string;
}

const CYPHER_EXECUTE: CypherRoute = {
  path: "/v2/cypher",
  native: "cypher_json",
  unavailable: "rebuild native bindings with Cypher support",
};
const CYPHER_EXPLAIN: CypherRoute = {
  path: "/v2/cypher/explain",
  native: "explain_cypher_json",
  unavailable: "rebuild native bindings with Cypher explain support",
};

/**
 * Error raised by the network {@link Client}.
 *
 * Strict port of the Rust `HelixError` enum:
 * - `Network` ↔ `ReqwestError` (transport failure; a write may already have committed)
 * - `Remote` ↔ `RemoteError` (the server returned neither query success `200`
 *   nor Cloud warm success `204`)
 * - `Serialization` ↔ `SerializationError` (request/response (de)serialization failed)
 * - `InvalidUrl` ↔ `InvalidURL` (the client URL could not be parsed)
 * - `InvalidRequest` ↔ `InvalidRequest` (server-only options were used in embedded mode)
 */
export class HelixError extends Error {
  readonly kind: "Network" | "Remote" | "Serialization" | "InvalidUrl" | "InvalidRequest" | "EmbeddedUnavailable" | "Embedded";
  readonly details?: string;
  readonly statusCode?: number;
  readonly code?: string;
  readonly serverMessage?: string;
  readonly retryable?: boolean;
  readonly serverDetails?: unknown;
  readonly rawBody?: string;

  private constructor(
    kind: HelixError["kind"],
    message: string,
    details?: string,
    code?: string,
    remote?: {
      statusCode: number;
      serverMessage: string;
      retryable?: boolean;
      serverDetails?: unknown;
      rawBody: string;
    },
  ) {
    super(message);
    this.name = "HelixError";
    this.kind = kind;
    this.details = details;
    this.statusCode = remote?.statusCode;
    this.code = code;
    this.serverMessage = remote?.serverMessage;
    this.retryable = remote?.retryable;
    this.serverDetails = remote?.serverDetails;
    this.rawBody = remote?.rawBody;
  }

  static network(message: string, url?: string): HelixError {
    const hint = url
      ? ` Cannot reach Helix at ${url} — start a local instance with \`helix start\`, or pass the URL of a running instance to \`new Client(url)\`.`
      : "";
    return new HelixError("Network", `error communicating with server: ${message}.${hint}`, message);
  }

  static remote(details: string, code?: string): HelixError;
  static remote(statusCode: number, rawBody: string, statusText: string): HelixError;
  static remote(statusCodeOrDetails: number | string, rawBodyOrCode = "", statusText = ""): HelixError {
    if (typeof statusCodeOrDetails === "string") {
      return new HelixError("Remote", `got error from server: ${statusCodeOrDetails}`, statusCodeOrDetails, rawBodyOrCode || undefined);
    }

    const statusCode = statusCodeOrDetails;
    const rawBody = rawBodyOrCode;
    let code: string | undefined;
    let serverMessage: string | undefined;
    let retryable: boolean | undefined;
    let serverDetails: unknown;
    try {
      const parsed: unknown = JSON.parse(rawBody);
      if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
        const body = parsed as Record<string, unknown>;
        const errorField = typeof body.error === "string" && body.error.length > 0 ? body.error : undefined;
        const msgField = typeof body.msg === "string" && body.msg.length > 0 ? body.msg : undefined;
        const codeField = typeof body.code === "string" && body.code.length > 0 ? body.code : undefined;
        const messageField = typeof body.message === "string" && body.message.length > 0 ? body.message : undefined;
        if (errorField !== undefined && msgField !== undefined) {
          code = errorField;
          serverMessage = msgField;
        } else if (errorField !== undefined && codeField !== undefined) {
          code = codeField;
          serverMessage = errorField;
        } else {
          code = codeField;
          serverMessage = messageField ?? errorField;
        }
        if (Object.prototype.hasOwnProperty.call(body, "details")) serverDetails = body.details;
        if (typeof body.retryable === "boolean") retryable = body.retryable;
      }
    } catch {}

    serverMessage = (serverMessage ?? rawBody) || statusText || `unknown error with code: ${statusCode}`;
    const details = serverMessage;
    return new HelixError("Remote", `got error from server: ${serverMessage}`, details, code, {
      statusCode,
      serverMessage,
      retryable,
      serverDetails,
      rawBody,
    });
  }

  static serialization(message: string): HelixError {
    return new HelixError("Serialization", `error serializing data: ${message}`, message);
  }

  static invalidUrl(message: string): HelixError {
    return new HelixError("InvalidUrl", `invalid url: ${message}`, message);
  }

  static invalidRequest(message: string): HelixError {
    return new HelixError("InvalidRequest", `invalid request: ${message}`, message, "invalid_request");
  }

  static embeddedUnavailable(message: string): HelixError {
    return new HelixError("EmbeddedUnavailable", `embedded bindings unavailable: ${message}`, message);
  }

  static embedded(message: string, code?: string): HelixError {
    return new HelixError("Embedded", `embedded HelixDB error: ${message}`, message, code);
  }

  isConflict(): boolean {
    return this.kind === "Remote" && this.statusCode === 409;
  }

  isRateLimited(): boolean {
    return this.kind === "Remote" && this.statusCode === 429;
  }

  /**
   * Returns true when the failure is explicitly retryable.
   *
   * Remote failures are retryable only when the server says so. Embedded
   * `index_backpressure` failures are retryable because the whole request was
   * rejected without effect: a write before commit, or a strong search.
   */
  isRetryable(): boolean {
    if (this.kind === "Embedded") return this.isIndexBackpressure();
    return this.kind === "Remote" && this.retryable === true;
  }

  /**
   * Returns whether asynchronous vector/text index work rejected the request.
   *
   * Either a write was rejected before commit because the index backlog is
   * full, or a strong search's answer lies behind more than 800 unpublished
   * changes; eventual searches are never rejected this way. The whole request
   * was rejected (HTTP 429, gRPC resource-exhausted); retry it unchanged after
   * a backoff.
   */
  isIndexBackpressure(): boolean {
    return (this.kind === "Remote" || this.kind === "Embedded") && this.code === "index_backpressure";
  }
}

function embeddedError(error: unknown): HelixError {
  if (typeof error === "object" && error !== null) {
    const fields = error as { error?: unknown; msg?: unknown };
    if (typeof fields.error === "string" && typeof fields.msg === "string") {
      return HelixError.embedded(fields.msg, fields.error);
    }
  }
  return HelixError.embedded(error instanceof Error ? error.message : String(error));
}

function remoteError(body: string, fallback: string, statusCode: number): HelixError {
  return HelixError.remote(statusCode, body, fallback);
}

type ClientBackend = { kind: "server"; url: URL; apiKey?: string; databaseId?: string } | { kind: "embedded"; native: NativeHelixDB };

/** Complete query request handed from {@link QueryBuilder} to {@link QueryExecutionRequest}. */
interface RequestParts {
  backend: ClientBackend;
  headers: Record<string, string>;
  query: QueryRequest;
}

/** Complete Cypher request handed from {@link QueryBuilder} to {@link CypherExecutionRequest}. */
interface CypherRequestParts {
  backend: ClientBackend;
  headers: Record<string, string>;
  query: string;
  parameters: Record<string, unknown>;
  queryName?: string;
}

interface QueryResponse {
  status: number;
  body: Uint8Array;
}

export type HelixDbSource =
  | { kind: "inMemory"; database: string }
  | { kind: "disk"; root: string; database: string }
  | { kind: "objectStorage"; database: string; bucket: string; region: string; endpoint?: string | null; allowHttp?: boolean };

/** Cache profile fixed for the lifetime of an embedded database handle. */
export type EmbeddedCacheConfig = {
  vectorMemoryBytes: number;
  mode:
    | { kind: "vectorMemoryOnly" }
    | { kind: "memory" }
    | {
        kind: "hybrid";
        slateMemoryBytes: number;
        slateDiskPath: string;
        slateDiskBytes: number;
        objectStoreDiskPath: string;
        objectStoreDiskBytes: number;
      };
};

type NativeHelixDB = {
  query_json(request: Uint8Array): Promise<Uint8Array>;
  cypher_json?(request: Uint8Array): Promise<Uint8Array>;
  explain_cypher_json?(request: Uint8Array): Promise<Uint8Array>;
  graph?(request: Uint8Array, spec: unknown): Promise<unknown>;
  close(): Promise<void>;
};

type NativeHelixDBConstructor = {
  open(source: unknown): Promise<NativeHelixDB>;
  open_with_config(source: unknown, config: unknown): Promise<NativeHelixDB>;
  open_reader(source: unknown): Promise<NativeHelixDB>;
  open_reader_with_config(source: unknown, config: unknown): Promise<NativeHelixDB>;
};

type NativeHelixDbSourceConstructor = {
  InMemory(database: string): unknown;
  Disk(root: string, database: string): unknown;
  ObjectStorage(database: string, bucket: string, region: string, endpoint: string | undefined, allowHttp: boolean): unknown;
};

type NativeEmbeddedCacheModeConstructor = {
  VectorMemoryOnly(): unknown;
  Memory(): unknown;
  Hybrid(
    slateMemoryBytes: number,
    slateDiskPath: string,
    slateDiskBytes: number,
    objectStoreDiskPath: string,
    objectStoreDiskBytes: number,
  ): unknown;
};

type NativeModule = {
  HelixDB?: NativeHelixDBConstructor;
  HelixDbSource?: NativeHelixDbSourceConstructor;
  EmbeddedCacheMode?: NativeEmbeddedCacheModeConstructor;
};

const DEFAULT_NATIVE_PACKAGE = "@helix-db/helix-db-embedded";
const dynamicImport = new Function("specifier", "return import(specifier)") as (specifier: string) => Promise<NativeModule>;

/**
 * Async HTTP client for running queries against a Helix instance.
 *
 * Strict port of the Rust `helix_db::Client`. Uses the built-in global `fetch`,
 * so the package stays dependency-free.
 *
 * ```ts
 * const client = new Client().withApiKey("hx_secret");
 * const result = await client.query<MyRow[]>(request).send();
 * ```
 */
export class Client {
  private backend: ClientBackend;

  constructor(url?: string | null) {
    try {
      this.backend = { kind: "server", url: new URL(url ?? DEFAULT_URL) };
    } catch (error) {
      throw HelixError.invalidUrl(error instanceof Error ? error.message : String(error));
    }
  }

  private static fromBackend(backend: ClientBackend): Client {
    const client = new Client();
    client.backend = backend;
    return client;
  }

  static server(url?: string | null): Client {
    return new Client(url);
  }

  static async embedded(source: HelixDbSource, cache?: EmbeddedCacheConfig): Promise<Client> {
    const native = await loadNativeHelixDB();
    try {
      const nativeSource = toNativeSource(native.HelixDbSource, source);
      return Client.fromBackend({
        kind: "embedded",
        native:
          cache === undefined
            ? await native.HelixDB.open(nativeSource)
            : await native.HelixDB.open_with_config(nativeSource, toNativeCacheConfig(native.EmbeddedCacheMode, cache)),
      });
    } catch (error) {
      throw embeddedError(error);
    }
  }

  static async embeddedReader(source: HelixDbSource, cache?: EmbeddedCacheConfig): Promise<Client> {
    const native = await loadNativeHelixDB();
    try {
      const nativeSource = toNativeSource(native.HelixDbSource, source);
      return Client.fromBackend({
        kind: "embedded",
        native:
          cache === undefined
            ? await native.HelixDB.open_reader(nativeSource)
            : await native.HelixDB.open_reader_with_config(nativeSource, toNativeCacheConfig(native.EmbeddedCacheMode, cache)),
      });
    } catch (error) {
      throw embeddedError(error);
    }
  }

  /** Set (or, with `null`/`undefined`, clear) the bearer API key sent on every request. */
  withApiKey(apiKey?: string | null): Client {
    if (this.backend.kind === "server") this.backend.apiKey = apiKey ?? undefined;
    return this;
  }

  /** Set (or, with `null`/`undefined`, clear) the database ID header sent on every request. */
  withDatabaseId(databaseId?: string | null): Client {
    if (this.backend.kind === "server") this.backend.databaseId = databaseId ?? undefined;
    return this;
  }

  /** Execute an SDK-built query. */
  query<R = unknown>(request: QueryRequest): QueryExecutionRequest<R> {
    return new QueryBuilder<R>(this.backend).query(request);
  }

  /** Execute Cypher, retaining tagged lossless values in the response. */
  cypher(query: string, parameters: Record<string, unknown> = {}, queryName?: string): Promise<CypherResponse> {
    return this.requestBuilder().cypher(query, parameters, queryName).send();
  }

  /** Plan Cypher without executing it, including modifying statements. */
  async explainCypher(query: string, parameters: Record<string, unknown> = {}, queryName?: string): Promise<CypherExplanation> {
    const explanation = await this.requestBuilder().cypher(query, parameters, queryName).explain();
    // Only a warm-only request succeeds with 204 No Content, and this one sends no options.
    if (explanation === undefined) throw HelixError.remote(204, "", "No Content");
    return explanation;
  }

  /** Begin building an advanced server request whose 200 response body deserializes into `R`. */
  requestBuilder<R = unknown>(): QueryBuilder<R> {
    return new QueryBuilder<R>(this.backend);
  }

  /** The client base URL (origin + path), e.g. `http://localhost:6969/`. */
  get baseUrl(): string {
    return this.backend.kind === "server" ? this.backend.url.toString() : "embedded://helixdb";
  }

  /** Load one immutable native graph with one ordinary read request. */
  async graph(selection: GraphSelection): Promise<NativeGraph> {
    return loadGraph(this, selection);
  }

  /** @internal Raw response path used only by the native graph adapter. */
  async _graphResponse(request: QueryRequest, nativeSpec: unknown): Promise<Uint8Array | Record<string, any>> {
    if (this.backend.kind === "embedded" && this.backend.native.graph !== undefined) {
      try {
        return (await this.backend.native.graph(request.toJsonBytes(), nativeSpec)) as Record<string, any>;
      } catch (error) {
        throw embeddedError(error);
      }
    }
    return this.requestBuilder<Uint8Array>().query(request).sendBytes();
  }

  async close(): Promise<void> {
    if (this.backend.kind === "embedded") {
      try {
        await this.backend.native.close();
      } catch (error) {
        throw embeddedError(error);
      }
    }
  }
}

export class QueryBuilder<R = unknown> {
  private readonly headers: Record<string, string> = { "Content-Type": "application/json" };

  constructor(private readonly backend: ClientBackend) {}

  /** Require this request to be served by a writer node (`x-helix-require-writer: true`). */
  writerOnly(): this {
    this.headers["x-helix-require-writer"] = "true";
    return this;
  }

  /** Mark this request as warm-only (`x-helix-warm: true`). */
  warmOnly(): this {
    this.headers["x-helix-warm"] = "true";
    return this;
  }

  /** Control whether the request waits for durability (`x-helix-await-durable`). */
  shouldAwaitDurability(should: boolean): this {
    this.headers["x-helix-await-durable"] = should ? "true" : "false";
    return this;
  }

  /** Attach a query and target `POST /v2/query`. */
  query(query: QueryRequest): QueryExecutionRequest<R> {
    return new QueryExecutionRequest<R>({
      backend: this.backend,
      headers: { ...this.headers },
      query,
    });
  }

  /** Attach one Cypher statement and target `POST /v2/cypher` or `POST /v2/cypher/explain`. */
  cypher(query: string, parameters: Record<string, unknown> = {}, queryName?: string): CypherExecutionRequest {
    return new CypherExecutionRequest({
      backend: this.backend,
      headers: { ...this.headers },
      query,
      parameters,
      queryName,
    });
  }
}

export class QueryExecutionRequest<R = unknown> {
  constructor(private readonly parts: RequestParts) {}

  private async execute(): Promise<QueryResponse> {
    const { backend, headers, query } = this.parts;

    if (backend.kind === "embedded") {
      const serverOptions = Object.keys(headers).filter((name) => name.toLowerCase() !== "content-type");
      if (serverOptions.length > 0) {
        throw HelixError.invalidRequest(`embedded queries do not support server request options: ${serverOptions.join(", ")}`);
      }
      let response: Uint8Array;
      try {
        response = await backend.native.query_json(query.toJsonBytes());
      } catch (error) {
        throw embeddedError(error);
      }
      return { status: 200, body: response };
    }

    let url: string;
    try {
      url = new URL(QUERY_PATH, backend.url).toString();
    } catch (error) {
      throw HelixError.invalidUrl(error instanceof Error ? error.message : String(error));
    }

    const requestHeaders: Record<string, string> = { ...headers };
    if (backend.apiKey !== undefined) requestHeaders["Authorization"] = `Bearer ${backend.apiKey}`;
    if (backend.databaseId !== undefined) requestHeaders["x-helix-database-id"] = backend.databaseId;

    let response: Response;
    try {
      response = await fetch(url, { method: "POST", headers: requestHeaders, body: query.toJsonString() });
    } catch (error) {
      throw HelixError.network(error instanceof Error ? error.message : String(error), url);
    }

    if (response.status === 200 || response.status === 204) {
      return { status: response.status, body: new Uint8Array(await response.arrayBuffer()) };
    }

    let body: string;
    try {
      body = await response.text();
    } catch {
      body = "";
    }
    throw remoteError(body, response.statusText || `unknown error with code: ${response.status}`, response.status);
  }

  async sendBytes(): Promise<Uint8Array> {
    return (await this.execute()).body;
  }

  async send(): Promise<R> {
    const response = await this.execute();
    if (response.status === 204) return undefined as R;
    try {
      return parseJson(new TextDecoder().decode(response.body)) as R;
    } catch (error) {
      throw HelixError.serialization(error instanceof Error ? error.message : String(error));
    }
  }
}

/**
 * One Cypher statement with the request options of the {@link QueryBuilder} that created it.
 *
 * ```ts
 * const rows = await client.requestBuilder().writerOnly().cypher("CREATE (n:User {name: $name}) RETURN n", { name: "Ada" }).send();
 * const plan = await client.requestBuilder().cypher("MATCH (n:User) RETURN n").explain();
 * ```
 */
export class CypherExecutionRequest {
  constructor(private readonly parts: CypherRequestParts) {}

  /**
   * Execute the statement (`POST /v2/cypher`), retaining tagged lossless values in the response.
   * A Helix Cloud warm-only read answers `204 No Content`, which resolves to no columns and no rows.
   */
  async send(): Promise<CypherResponse> {
    const response = await this.execute(CYPHER_EXECUTE);
    return response === undefined ? { columns: [], rows: [] } : (response as CypherResponse);
  }

  /**
   * Plan the statement without executing it (`POST /v2/cypher/explain`). The server applies options as a read.
   * Resolves to `undefined` only when a Helix Cloud warm-only request answers `204 No Content`.
   */
  async explain(): Promise<CypherExplanation | undefined> {
    return (await this.execute(CYPHER_EXPLAIN)) as CypherExplanation | undefined;
  }

  /** Resolves to the decoded JSON body, or `undefined` for a `204 No Content` warm success. */
  private async execute(route: CypherRoute): Promise<unknown> {
    const { backend, headers, query, parameters, queryName } = this.parts;

    // Integers outside the JavaScript safe range and non-finite floats have no
    // plain JSON form, so they travel as the server's lossless tagged values.
    let body: string;
    try {
      body = JSON.stringify({ query, parameters, query_name: queryName }, (_key, value: unknown) =>
        typeof value === "bigint"
          ? { $type: "integer", value: value.toString() }
          : typeof value === "number" && !Number.isFinite(value)
            ? { $type: "float", value: String(value) }
            : value,
      );
    } catch (error) {
      throw HelixError.serialization(String(error));
    }

    if (backend.kind === "embedded") {
      const serverOptions = Object.keys(headers).filter((name) => name.toLowerCase() !== "content-type");
      if (serverOptions.length > 0) {
        throw HelixError.invalidRequest(`embedded queries do not support server request options: ${serverOptions.join(", ")}`);
      }
      // Older native packages predate these methods; call through the handle to keep its `this`.
      const run = backend.native[route.native];
      if (run === undefined) throw HelixError.embeddedUnavailable(route.unavailable);
      try {
        return JSON.parse(new TextDecoder().decode(await run.call(backend.native, new TextEncoder().encode(body))));
      } catch (error) {
        throw embeddedError(error);
      }
    }

    const url = new URL(route.path, backend.url);
    const requestHeaders: Record<string, string> = { ...headers };
    if (backend.apiKey !== undefined) requestHeaders["Authorization"] = `Bearer ${backend.apiKey}`;
    if (backend.databaseId !== undefined) requestHeaders["x-helix-database-id"] = backend.databaseId;
    let response: Response;
    try {
      response = await fetch(url, { method: "POST", headers: requestHeaders, body });
    } catch (error) {
      throw HelixError.network(String(error), url.toString());
    }
    const text = await response.text();
    if (response.status === 204) return undefined;
    if (response.status !== 200) throw HelixError.remote(response.status, text, response.statusText);
    try {
      return JSON.parse(text);
    } catch (error) {
      throw HelixError.serialization(String(error));
    }
  }
}

async function loadNativeHelixDB(): Promise<{
  HelixDB: NativeHelixDBConstructor;
  HelixDbSource: NativeHelixDbSourceConstructor;
  EmbeddedCacheMode?: NativeEmbeddedCacheModeConstructor;
}> {
  const packageName = process.env.HELIXDB_EMBEDDED_NODE_PACKAGE ?? process.env.HELIXDB_UNIFFI_NODE_PACKAGE ?? DEFAULT_NATIVE_PACKAGE;
  let module: NativeModule;
  try {
    module = await dynamicImport(packageName);
  } catch (error) {
    throw HelixError.embeddedUnavailable(error instanceof Error ? error.message : String(error));
  }
  if (module.HelixDB === undefined) throw HelixError.embeddedUnavailable(`${packageName} does not export HelixDB`);
  if (module.HelixDbSource === undefined) throw HelixError.embeddedUnavailable(`${packageName} does not export HelixDbSource`);
  return { HelixDB: module.HelixDB, HelixDbSource: module.HelixDbSource, EmbeddedCacheMode: module.EmbeddedCacheMode };
}

function toNativeCacheConfig(native: NativeEmbeddedCacheModeConstructor | undefined, cache: EmbeddedCacheConfig): unknown {
  if (native === undefined) throw HelixError.embeddedUnavailable("native package does not export EmbeddedCacheMode");
  const mode = cache.mode;
  const nativeMode =
    mode.kind === "vectorMemoryOnly"
      ? native.VectorMemoryOnly()
      : mode.kind === "memory"
        ? native.Memory()
        : native.Hybrid(
            mode.slateMemoryBytes,
            mode.slateDiskPath,
            mode.slateDiskBytes,
            mode.objectStoreDiskPath,
            mode.objectStoreDiskBytes,
          );
  return { vector_memory_bytes: cache.vectorMemoryBytes, mode: nativeMode };
}

function toNativeSource(native: NativeHelixDbSourceConstructor, source: HelixDbSource): unknown {
  switch (source.kind) {
    case "inMemory":
      return native.InMemory(source.database);
    case "disk":
      return native.Disk(source.root, source.database);
    case "objectStorage":
      return native.ObjectStorage(source.database, source.bucket, source.region, source.endpoint ?? undefined, source.allowHttp ?? false);
  }
}
