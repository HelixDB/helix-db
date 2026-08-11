import { resolve } from "node:path";
import { configFromEnv } from "../config.js";
import { writeDump } from "../dump.js";
import { createNeo4jDriver, waitForNeo4j } from "../neo4j.js";
import { exportNeo4jSnapshot } from "../neo4j-exporter.js";

const outputPath = resolve(process.argv[2] ?? "movie-graph.v1.json");
const config = configFromEnv();
const driver = createNeo4jDriver(config);
try {
  await waitForNeo4j(driver);
  const dump = await exportNeo4jSnapshot(driver, config.neo4jDatabase, process.env.MIGRATION_EXPORTED_AT);
  const digest = await writeDump(outputPath, dump);
  console.log(`Exported ${dump.nodes.length} nodes and ${dump.relationships.length} relationships to ${outputPath}.`);
  console.log(`SHA-256: ${digest}`);
} finally {
  await driver.close();
}
