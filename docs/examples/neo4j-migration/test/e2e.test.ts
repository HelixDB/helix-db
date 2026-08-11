import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import test from "node:test";
import { configFromEnv } from "../src/config.js";
import { readDump, serializeDump, writeDump } from "../src/dump.js";
import { HelixHttpClient, waitForHelix } from "../src/helix-http.js";
import { loadDump } from "../src/loader.js";
import { createNeo4jDriver, waitForNeo4j } from "../src/neo4j.js";
import { exportNeo4jSnapshot } from "../src/neo4j-exporter.js";
import { seedNeo4j } from "../src/seed.js";
import {
  expectedCounts,
  helixCounts,
  verifyMigration,
} from "../src/verification.js";

const execute = promisify(execFile);
const exampleRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const fixtureUrl = new URL("../fixtures/movie-graph.v1.json", import.meta.url);
const seedUrl = new URL("../fixtures/seed.cypher", import.meta.url);

async function compose(...args: string[]): Promise<void> {
  await execute("docker", ["compose", ...args], {
    cwd: exampleRoot,
    timeout: 180_000,
  });
}

test(
  "migrates Neo4j data, resumes after interruption, and replays without duplicates",
  { timeout: 300_000 },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "neo4j-to-helix-e2e-"));
    await compose("down", "--volumes", "--remove-orphans").catch(
      () => undefined,
    );
    await compose("up", "-d", "--force-recreate");
    const config = configFromEnv({});
    const driver = createNeo4jDriver(config);
    try {
      await Promise.all([waitForNeo4j(driver), waitForHelix(config.helixUrl)]);
      await seedNeo4j(driver, config.neo4jDatabase, seedUrl);

      const dump = await exportNeo4jSnapshot(
        driver,
        config.neo4jDatabase,
        "2026-08-11T00:00:00.000Z",
      );
      assert.equal(serializeDump(dump), await readFile(fixtureUrl, "utf8"));
      const dumpPath = join(directory, "movie-graph.v1.json");
      await writeDump(dumpPath, dump);
      const validatedDump = await readDump(dumpPath);

      const client = new HelixHttpClient(config.helixUrl);
      await loadDump(validatedDump, client, {
        batchSize: 2,
        stopAfterNodes: true,
      });
      assert.deepEqual(await helixCounts(client), {
        ...expectedCounts(validatedDump),
        acted_in: 0,
        directed: 0,
        produced: 0,
      });

      await loadDump(validatedDump, client, { batchSize: 2 });
      await verifyMigration(
        driver,
        config.neo4jDatabase,
        client,
        validatedDump,
      );

      await loadDump(validatedDump, client, { batchSize: 2 });
      await verifyMigration(
        driver,
        config.neo4jDatabase,
        client,
        validatedDump,
      );
    } finally {
      await driver.close();
      await compose("down", "--volumes", "--remove-orphans");
      await rm(directory, { recursive: true, force: true });
    }
  },
);
