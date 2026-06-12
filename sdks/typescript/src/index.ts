// Public entry point for the Helix TypeScript SDK.
//
// The query DSL lives in `./dsl.ts` and is re-exported wholesale here. This file
// adds the network client (`Client`, `QueryBuilder`, `QueryRequest`, `HelixError`),
// mirroring the Rust SDK layout where the DSL lives in `dsl.rs` and the client in
// `lib.rs`.

export * from "./dsl.js";

import { DynamicQueryRequest, parseJsonStructural, stringifyJson } from "./dsl.js";

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
 */
export class HelixError extends Error {
  readonly kind: "Network" | "Remote" | "Serialization" | "InvalidUrl";
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
}

type QueryType = { kind: "stored"; name: string } | { kind: "dynamic"; query: DynamicQueryRequest } | { kind: "empty" };

/** Snapshot handed from {@link QueryBuilder} to {@link QueryRequest} at `stored()`/`dynamic()` time. */
interface RequestParts {
  base: URL;
  apiKey?: string;
  headers: Record<string, string>;
  body?: string;
  queryType: QueryType;
}

/**
 * Async HTTP client for running queries against a Helix instance.
 *
 * Strict port of the Rust `helix_db::Client`. Uses the built-in global `fetch`,
 * so the package stays dependency-free.
 *
 * ```ts
 * const client = new Client().withApiKey("hx_secret");
 * const result = await client.query<MyRow[]>().dynamic(request).send();
 * ```
 */
export class Client {
  private readonly url: URL;
  private apiKey?: string;

  constructor(url?: string | null) {
    try {
      this.url = new URL(url ?? DEFAULT_URL);
    } catch (error) {
      throw HelixError.invalidUrl(error instanceof Error ? error.message : String(error));
    }
  }

  /** Set (or, with `null`/`undefined`, clear) the bearer API key sent on every request. */
  withApiKey(apiKey?: string | null): Client {
    this.apiKey = apiKey ?? undefined;
    return this;
  }

  /** Begin building a query whose 200 response body deserializes into `R`. */
  query<R = unknown>(): QueryBuilder<R> {
    return new QueryBuilder<R>(this.url, this.apiKey);
  }

  /** The client base URL (origin + path), e.g. `http://localhost:6969/`. */
  get baseUrl(): string {
    return this.url.toString();
  }
}

export class QueryBuilder<R = unknown> {
  private readonly headers: Record<string, string> = { "Content-Type": "application/json" };
  private bodyData?: string;

  constructor(
    private readonly base: URL,
    private readonly apiKey?: string,
  ) {}

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

  /**
   * Attach a JSON request body (only used by {@link QueryBuilder.stored}; dynamic
   * requests always send the serialized query). Serialized with the SDK's
   * bigint-safe `stringifyJson`.
   */
  body(data: unknown): this {
    try {
      this.bodyData = stringifyJson(data);
    } catch (error) {
      throw HelixError.serialization(error instanceof Error ? error.message : String(error));
    }
    return this;
  }

  /** Target a stored query route (`POST /v1/query/{name}`). */
  stored(queryName: string): QueryRequest<R> {
    return new QueryRequest<R>(this.parts({ kind: "stored", name: queryName }));
  }

  /** Target the dynamic query route (`POST /v1/query`). */
  dynamic(query: DynamicQueryRequest): QueryRequest<R> {
    return new QueryRequest<R>(this.parts({ kind: "dynamic", query }));
  }

  private parts(queryType: QueryType): RequestParts {
    return {
      base: this.base,
      apiKey: this.apiKey,
      headers: { ...this.headers },
      body: this.bodyData,
      queryType,
    };
  }
}

export class QueryRequest<R = unknown> {
  constructor(private readonly parts: RequestParts) {}

  async send(): Promise<R> {
    const { base, apiKey, headers, body, queryType } = this.parts;

    let path: string;
    let payload: string | undefined;
    switch (queryType.kind) {
      case "dynamic":
        path = QUERY_PATH;
        payload = queryType.query.toJsonString();
        break;
      case "stored":
        path = `${QUERY_PATH}/${queryType.name}`;
        payload = body;
        break;
      case "empty":
        throw new Error("send() is only reachable after stored() or dynamic()");
    }

    let url: string;
    try {
      url = new URL(path, base).toString();
    } catch (error) {
      throw HelixError.invalidUrl(error instanceof Error ? error.message : String(error));
    }

    const requestHeaders: Record<string, string> = { ...headers };
    if (apiKey !== undefined) requestHeaders["Authorization"] = `Bearer ${apiKey}`;

    let response: Response;
    try {
      response = await fetch(url, { method: "POST", headers: requestHeaders, body: payload });
    } catch (error) {
      throw HelixError.network(error instanceof Error ? error.message : String(error), url);
    }

    // Mirror the Rust client: only HTTP 200 is treated as success.
    if (response.status === 200) {
      const text = await response.text();
      try {
        return parseJsonStructural(text) as R;
      } catch (error) {
        throw HelixError.serialization(error instanceof Error ? error.message : String(error));
      }
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
}
