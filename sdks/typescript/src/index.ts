// Public entry point for the Helix TypeScript SDK.
//
// The query DSL lives in `./dsl.ts` and is re-exported wholesale here. This file
// adds the network client (`Client`, `QueryBuilder`, `QueryExecutionRequest`, `HelixError`),
// mirroring the Rust SDK layout where the DSL lives in `dsl.rs` and the client in
// `lib.rs`.

export * from "./dsl.js";
export * from "./graph.js";

import { QueryRequest, parseJson } from "./dsl.js";
import { GraphSelection, NativeGraph, loadGraph } from "./graph.js";

const DEFAULT_URL = "http://localhost:6969";
const QUERY_PATH = "/v1/query";

/**
 * Error raised by the network {@link Client}.
 *
 * Strict port of the Rust `HelixError` enum:
 * - `Network` ↔ `ReqwestError` (the request failed to reach the server)
 * - `Remote` ↔ `RemoteError` (the server returned a non-200 response)
 * - `Serialization` ↔ `SerializationError` (request/response (de)serialization failed)
 * - `InvalidUrl` ↔ `InvalidURL` (the client URL could not be parsed)
 * - `InvalidRequest` ↔ `InvalidRequest` (server-only options were used in embedded mode)
 */
export class HelixError extends Error {
  readonly kind: "Network" | "Remote" | "Serialization" | "InvalidUrl" | "InvalidRequest" | "EmbeddedUnavailable" | "Embedded";
  readonly details?: string;

  private constructor(kind: HelixError["kind"], message: string, details?: string) {
    super(message);
    this.name = "HelixError";
    this.kind = kind;
    this.details = details;
  }

  static network(message: string, url?: string): HelixError {
    const hint = url
      ? ` Cannot reach Helix at ${url} — start a local instance with \`helix start\`, or pass the URL of a running instance to \`new Client(url)\`.`
      : "";
    return new HelixError("Network", `error communicating with server: ${message}.${hint}`, message);
  }

  static remote(details: string): HelixError {
    return new HelixError("Remote", `got error from server: ${details}`, details);
  }

  static serialization(message: string): HelixError {
    return new HelixError("Serialization", `error serializing data: ${message}`, message);
  }

  static invalidUrl(message: string): HelixError {
    return new HelixError("InvalidUrl", `invalid url: ${message}`, message);
  }

  static invalidRequest(message: string): HelixError {
    return new HelixError("InvalidRequest", `invalid request: ${message}`, message);
  }

  static embeddedUnavailable(message: string): HelixError {
    return new HelixError("EmbeddedUnavailable", `embedded bindings unavailable: ${message}`, message);
  }

  static embedded(message: string): HelixError {
    return new HelixError("Embedded", `embedded HelixDB error: ${message}`, message);
  }
}

type ClientBackend = { kind: "server"; url: URL; apiKey?: string } | { kind: "embedded"; native: NativeHelixDB };

/** Complete query request handed from {@link QueryBuilder} to {@link QueryExecutionRequest}. */
interface RequestParts {
  backend: ClientBackend;
  headers: Record<string, string>;
  query: QueryRequest;
}

export type HelixDbSource =
  | { kind: "inMemory"; database: string }
  | { kind: "disk"; root: string; database: string }
  | { kind: "objectStorage"; database: string; bucket: string; region: string; endpoint?: string | null; allowHttp?: boolean };

type NativeHelixDB = {
  query_json(request: Uint8Array): Promise<Uint8Array>;
  graph?(request: Uint8Array, spec: unknown): Promise<unknown>;
  close(): Promise<void>;
};

type NativeHelixDBConstructor = {
  open(source: unknown): Promise<NativeHelixDB>;
  open_reader(source: unknown): Promise<NativeHelixDB>;
};

type NativeHelixDbSourceConstructor = {
  InMemory(database: string): unknown;
  Disk(root: string, database: string): unknown;
  ObjectStorage(database: string, bucket: string, region: string, endpoint: string | undefined, allowHttp: boolean): unknown;
};

type NativeModule = {
  HelixDB?: NativeHelixDBConstructor;
  HelixDbSource?: NativeHelixDbSourceConstructor;
};

const DEFAULT_NATIVE_PACKAGE = "@helix-db/uniffi";
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

  static async embedded(source: HelixDbSource): Promise<Client> {
    const native = await loadNativeHelixDB();
    try {
      return Client.fromBackend({
        kind: "embedded",
        native: await native.HelixDB.open(toNativeSource(native.HelixDbSource, source)),
      });
    } catch (error) {
      throw HelixError.embedded(error instanceof Error ? error.message : String(error));
    }
  }

  static async embeddedReader(source: HelixDbSource): Promise<Client> {
    const native = await loadNativeHelixDB();
    try {
      return Client.fromBackend({
        kind: "embedded",
        native: await native.HelixDB.open_reader(toNativeSource(native.HelixDbSource, source)),
      });
    } catch (error) {
      throw HelixError.embedded(error instanceof Error ? error.message : String(error));
    }
  }

  /** Set (or, with `null`/`undefined`, clear) the bearer API key sent on every request. */
  withApiKey(apiKey?: string | null): Client {
    if (this.backend.kind === "server") this.backend.apiKey = apiKey ?? undefined;
    return this;
  }

  /** Execute an SDK-built query. */
  query<R = unknown>(request: QueryRequest): QueryExecutionRequest<R> {
    return new QueryBuilder<R>(this.backend).query(request);
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
        throw HelixError.embedded(error instanceof Error ? error.message : String(error));
      }
    }
    return this.requestBuilder<Uint8Array>().query(request).sendBytes();
  }

  async close(): Promise<void> {
    if (this.backend.kind === "embedded") {
      try {
        await this.backend.native.close();
      } catch (error) {
        throw HelixError.embedded(error instanceof Error ? error.message : String(error));
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

  /** Attach a query and target `POST /v1/query`. */
  query(query: QueryRequest): QueryExecutionRequest<R> {
    return new QueryExecutionRequest<R>({
      backend: this.backend,
      headers: { ...this.headers },
      query,
    });
  }
}

export class QueryExecutionRequest<R = unknown> {
  constructor(private readonly parts: RequestParts) {}

  async sendBytes(): Promise<Uint8Array> {
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
        throw HelixError.embedded(error instanceof Error ? error.message : String(error));
      }
      return response;
    }

    let url: string;
    try {
      url = new URL(QUERY_PATH, backend.url).toString();
    } catch (error) {
      throw HelixError.invalidUrl(error instanceof Error ? error.message : String(error));
    }

    const requestHeaders: Record<string, string> = { ...headers };
    if (backend.apiKey !== undefined) requestHeaders["Authorization"] = `Bearer ${backend.apiKey}`;

    let response: Response;
    try {
      response = await fetch(url, { method: "POST", headers: requestHeaders, body: query.toJsonString() });
    } catch (error) {
      throw HelixError.network(error instanceof Error ? error.message : String(error), url);
    }

    // Mirror the Rust client: only HTTP 200 is treated as success.
    if (response.status === 200) {
      return new Uint8Array(await response.arrayBuffer());
    }

    let details: string;
    try {
      details = await response.text();
    } catch {
      details = response.statusText;
    }
    if (details.length === 0) details = response.statusText || `unknown error with code: ${response.status}`;
    throw HelixError.remote(details);
  }

  async send(): Promise<R> {
    const response = await this.sendBytes();
    try {
      return parseJson(new TextDecoder().decode(response)) as R;
    } catch (error) {
      throw HelixError.serialization(error instanceof Error ? error.message : String(error));
    }
  }
}

async function loadNativeHelixDB(): Promise<{ HelixDB: NativeHelixDBConstructor; HelixDbSource: NativeHelixDbSourceConstructor }> {
  const packageName = process.env.HELIXDB_UNIFFI_NODE_PACKAGE ?? DEFAULT_NATIVE_PACKAGE;
  let module: NativeModule;
  try {
    module = await dynamicImport(packageName);
  } catch (error) {
    throw HelixError.embeddedUnavailable(error instanceof Error ? error.message : String(error));
  }
  if (module.HelixDB === undefined) throw HelixError.embeddedUnavailable(`${packageName} does not export HelixDB`);
  if (module.HelixDbSource === undefined) throw HelixError.embeddedUnavailable(`${packageName} does not export HelixDbSource`);
  return { HelixDB: module.HelixDB, HelixDbSource: module.HelixDbSource };
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
