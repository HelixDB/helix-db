import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Client, HelixError, type HelixDbSource, type EmbeddedCacheConfig } from "../../src/index.js";
import { readCypherCases, type CypherCase } from "./cypher-results.js";
import { parityProgress } from "./progress.js";

/** Record actual public SDK responses. Mutations are executed exactly once. */
export async function writeCypherCase(client: Client, fixture: CypherCase, root: string): Promise<void> {
  parityProgress(`${fixture.statement.language === "sql" ? "SQL" : "Cypher"}: ${fixture.name}`);
  const { statement } = fixture;
  let output: unknown;
  try {
    output = {
      result:
        statement.language === "sql"
          ? await client.sql(fixture.query, statement.parameters, statement.mappings, fixture.name)
          : await client.cypher(fixture.query, statement.parameters, fixture.name),
    };
  } catch (error) {
    output =
      error instanceof HelixError ? { error: String(error), code: error.code, details: error.serverDetails } : { error: String(error) };
  }
  await writeFile(join(root, `${fixture.name}.json`), JSON.stringify(output));
}

export async function runCypherEmbedded(source: HelixDbSource, cache: EmbeddedCacheConfig, results: string): Promise<void> {
  const cases = await readCypherCases();
  const root = join(results, "cypher");
  await mkdir(root, { recursive: true });
  parityProgress(`Cypher ${source.kind}: opening writer`);
  let client = await Client.embedded(source, cache);
  try {
    for (const fixture of cases) {
      if (source.kind === "disk" && fixture.afterDiskReopen) {
        parityProgress(`Cypher disk: closing writer before ${fixture.name}`);
        await client.close();
        parityProgress(`Cypher disk: opening reader before ${fixture.name}`);
        const reader = await Client.embeddedReader(source, cache);
        try {
          await writeCypherCase(reader, fixture, root);
        } finally {
          parityProgress(`Cypher disk: closing reader after ${fixture.name}`);
          await reader.close();
        }
        parityProgress(`Cypher disk: reopening writer after ${fixture.name}`);
        client = await Client.embedded(source, cache);
      } else {
        await writeCypherCase(client, fixture, root);
      }
    }
  } finally {
    parityProgress(`Cypher ${source.kind}: closing writer`);
    await client.close();
  }
  parityProgress(`Cypher ${source.kind}: closed writer`);
}
