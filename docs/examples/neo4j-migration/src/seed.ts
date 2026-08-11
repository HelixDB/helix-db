import { readFile } from "node:fs/promises";
import type { Driver } from "neo4j-driver";

const constraints = [
  "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (person:Person) REQUIRE person.personId IS UNIQUE",
  "CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (movie:Movie) REQUIRE movie.movieId IS UNIQUE",
];

function statements(cypher: string): string[] {
  return cypher
    .split(/;\s*(?:\n|$)/)
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}

export async function seedNeo4j(driver: Driver, database: string, seedPath: URL): Promise<void> {
  const cypher = await readFile(seedPath, "utf8");
  const session = driver.session({ database, defaultAccessMode: "WRITE" });
  try {
    for (const constraint of constraints) await session.run(constraint);
    for (const statement of statements(cypher)) await session.run(statement);
  } finally {
    await session.close();
  }
}
