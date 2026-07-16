import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Client, stringifyJson } from "../../src/index.js";
import { nodePermutationFixtures, runtimeFixtures } from "./generate-fixtures.js";

const results = process.env.HELIX_EMBEDDED_PARITY_RESULTS;
if (results === undefined) throw new Error("HELIX_EMBEDDED_PARITY_RESULTS is required");

await rm(results, { recursive: true, force: true });
await mkdir(results, { recursive: true });

const client = await Client.embedded({
  kind: "inMemory",
  database: process.env.HELIX_EMBEDDED_PARITY_DATABASE ?? "typescript-sdk-embedded-parity",
});
try {
  for (const fixture of [...runtimeFixtures(), ...nodePermutationFixtures()]) {
    const response = await client.query(fixture.request).send();
    await writeFile(join(results, `${fixture.name}.json`), stringifyJson(response));
  }
} finally {
  await client.close();
}
