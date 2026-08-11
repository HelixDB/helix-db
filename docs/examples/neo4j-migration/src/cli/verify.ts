import { resolve } from "node:path";
import { configFromEnv } from "../config.js";
import { readDump } from "../dump.js";
import { HelixHttpClient, waitForHelix } from "../helix-http.js";
import { createNeo4jDriver, waitForNeo4j } from "../neo4j.js";
import { verifyMigration } from "../verification.js";

const dumpPath = resolve(process.argv[2] ?? "movie-graph.v1.json");
const config = configFromEnv();
const dump = await readDump(dumpPath);
const driver = createNeo4jDriver(config);
try {
  await Promise.all([waitForNeo4j(driver), waitForHelix(config.helixUrl)]);
  await verifyMigration(
    driver,
    config.neo4jDatabase,
    new HelixHttpClient(config.helixUrl),
    dump,
  );
  console.log("Migration verified: counts and repository results match.");
} finally {
  await driver.close();
}
