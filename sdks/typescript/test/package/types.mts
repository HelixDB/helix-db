import { Client, HelixError, g, writeBatch } from "@helix-db/helix-db";

// Compile against the installed declarations, not the repository's source.
export async function updateAsset(client: Client, id: bigint) {
  try {
    return await client.query(writeBatch().varAs("asset", g().n(id).setProperty("name", "updated")).toQueryRequest()).send();
  } catch (error) {
    if (error instanceof HelixError) {
      const code: string | undefined = error.code;
      const status: number | undefined = error.statusCode;
      const diagnostic: string | undefined = error.serverMessage;
      const retryable: boolean | undefined = error.retryable;
      return { code, status, diagnostic, retryable, conflict: error.isConflict(), explicitlyRetryable: error.isRetryable() };
    }
    throw error;
  }
}
