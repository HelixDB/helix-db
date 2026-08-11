import neo4j, { type Driver } from "neo4j-driver";
import type { MigrationConfig } from "./config.js";

export function createNeo4jDriver(config: MigrationConfig): Driver {
  return neo4j.driver(
    config.neo4jUri,
    neo4j.auth.basic(config.neo4jUser, config.neo4jPassword),
    {
      disableLosslessIntegers: true,
    },
  );
}

export async function waitForNeo4j(
  driver: Driver,
  attempts = 60,
): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      await driver.verifyConnectivity();
      return;
    } catch (error) {
      if (attempt + 1 === attempts) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  }
}
