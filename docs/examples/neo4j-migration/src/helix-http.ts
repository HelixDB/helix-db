import { QueryRequest } from "@helix-db/helix-db";

export type HelixRequestOptions = {
  awaitDurability?: boolean;
  attempts?: number;
  baseDelayMs?: number;
};

export type FetchLike = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;
export type Sleep = (milliseconds: number) => Promise<void>;

export class HelixHttpError extends Error {
  constructor(
    readonly status: number,
    readonly details: string,
  ) {
    super(`HelixDB returned HTTP ${status}: ${details}`);
    this.name = "HelixHttpError";
  }
}

export class HelixHttpClient {
  private readonly queryUrl: URL;

  constructor(
    baseUrl: string,
    private readonly fetchImpl: FetchLike = fetch,
    private readonly sleep: Sleep = (milliseconds) =>
      new Promise((resolve) => setTimeout(resolve, milliseconds)),
  ) {
    this.queryUrl = new URL("/v2/query", baseUrl);
  }

  async execute(
    request: QueryRequest,
    options: HelixRequestOptions = {},
  ): Promise<unknown> {
    const attempts = options.attempts ?? 8;
    const baseDelayMs = options.baseDelayMs ?? 10;
    if (!Number.isSafeInteger(attempts) || attempts <= 0)
      throw new TypeError(`attempts must be a positive integer: ${attempts}`);
    if (!Number.isSafeInteger(baseDelayMs) || baseDelayMs < 0)
      throw new TypeError(
        `baseDelayMs must be a non-negative integer: ${baseDelayMs}`,
      );

    const headers: Record<string, string> = {
      "content-type": "application/json",
    };
    if (options.awaitDurability === true)
      headers["x-helix-await-durable"] = "true";
    const body = request.toJsonString();

    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const response = await this.fetchImpl(this.queryUrl, {
        method: "POST",
        headers,
        body,
        signal: AbortSignal.timeout(120_000),
      });
      const responseBody = await response.text();
      if (response.ok)
        return responseBody.length === 0
          ? undefined
          : (JSON.parse(responseBody) as unknown);
      if (response.status === 409 && attempt + 1 < attempts) {
        await this.sleep(baseDelayMs * 2 ** attempt);
        continue;
      }
      throw new HelixHttpError(
        response.status,
        responseBody || response.statusText,
      );
    }
    throw new Error("HelixDB conflict retry loop exhausted unexpectedly");
  }
}

export async function waitForHelix(
  url: string,
  attempts = 120,
  fetchImpl: FetchLike = fetch,
): Promise<void> {
  const readyUrl = new URL("/readyz", url);
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetchImpl(readyUrl, {
        signal: AbortSignal.timeout(1_000),
      });
      if (response.ok) return;
    } catch {
      // The container is still starting.
    }
    if (attempt + 1 < attempts)
      await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error(`HelixDB did not become ready at ${readyUrl}`);
}
