export type MigrationConfig = {
  neo4jUri: string;
  neo4jUser: string;
  neo4jPassword: string;
  neo4jDatabase: string;
  helixUrl: string;
  batchSize: number;
};

function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value ?? fallback);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`MIGRATION_BATCH_SIZE must be a positive integer, received ${value ?? fallback}`);
  }
  return parsed;
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): MigrationConfig {
  return {
    neo4jUri: env.NEO4J_URI ?? "bolt://localhost:17687",
    neo4jUser: env.NEO4J_USER ?? "neo4j",
    neo4jPassword: env.NEO4J_PASSWORD ?? "migration-password",
    neo4jDatabase: env.NEO4J_DATABASE ?? "neo4j",
    helixUrl: env.HELIX_URL ?? "http://localhost:16969",
    batchSize: positiveInteger(env.MIGRATION_BATCH_SIZE, 100),
  };
}
