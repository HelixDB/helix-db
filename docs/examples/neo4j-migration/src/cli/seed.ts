import { configFromEnv } from "../config.js";
import { createNeo4jDriver, waitForNeo4j } from "../neo4j.js";
import { seedNeo4j } from "../seed.js";

const config = configFromEnv();
const driver = createNeo4jDriver(config);
try {
  await waitForNeo4j(driver);
  await seedNeo4j(
    driver,
    config.neo4jDatabase,
    new URL("../../fixtures/seed.cypher", import.meta.url),
  );
  console.log("Seeded Neo4j with 5 people, 2 movies, and 10 relationships.");
} finally {
  await driver.close();
}
