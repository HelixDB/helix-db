import type { Driver, QueryResult, RecordShape } from "neo4j-driver";
import { parseMovieGraphDump, type MovieGraphDumpV1 } from "./model.js";

export const exportQueries = {
  people: `
    MATCH (person:Person)
    RETURN person.personId AS personId, person.name AS name, person.born AS born
    ORDER BY personId
  `,
  movies: `
    MATCH (movie:Movie)
    RETURN movie.movieId AS movieId, movie.title AS title,
           movie.released AS released, movie.tagline AS tagline
    ORDER BY movieId
  `,
  actedIn: `
    MATCH (person:Person)-[relationship:ACTED_IN]->(movie:Movie)
    RETURN relationship.relationshipId AS relationshipId,
           person.personId AS fromPersonId, movie.movieId AS toMovieId,
           relationship.roles AS roles
    ORDER BY relationshipId
  `,
  directed: `
    MATCH (person:Person)-[relationship:DIRECTED]->(movie:Movie)
    RETURN relationship.relationshipId AS relationshipId,
           person.personId AS fromPersonId, movie.movieId AS toMovieId
    ORDER BY relationshipId
  `,
  produced: `
    MATCH (person:Person)-[relationship:PRODUCED]->(movie:Movie)
    RETURN relationship.relationshipId AS relationshipId,
           person.personId AS fromPersonId, movie.movieId AS toMovieId
    ORDER BY relationshipId
  `,
} as const;

type ExportResults = {
  people: QueryResult<RecordShape>;
  movies: QueryResult<RecordShape>;
  actedIn: QueryResult<RecordShape>;
  directed: QueryResult<RecordShape>;
  produced: QueryResult<RecordShape>;
};

function field<T>(record: QueryResult<RecordShape>["records"][number], name: string): T {
  return record.get(name) as T;
}

export function buildDump(results: ExportResults, exportedAt: string): MovieGraphDumpV1 {
  return parseMovieGraphDump({
    version: 1,
    exportedAt,
    nodes: [
      ...results.people.records.map((record) => ({
        kind: "person" as const,
        personId: field<string>(record, "personId"),
        name: field<string>(record, "name"),
        born: field<number>(record, "born"),
      })),
      ...results.movies.records.map((record) => ({
        kind: "movie" as const,
        movieId: field<string>(record, "movieId"),
        title: field<string>(record, "title"),
        released: field<number>(record, "released"),
        tagline: field<string>(record, "tagline"),
      })),
    ],
    relationships: [
      ...results.actedIn.records.map((record) => ({
        kind: "acted_in" as const,
        relationshipId: field<string>(record, "relationshipId"),
        fromPersonId: field<string>(record, "fromPersonId"),
        toMovieId: field<string>(record, "toMovieId"),
        roles: field<string[]>(record, "roles"),
      })),
      ...results.directed.records.map((record) => ({
        kind: "directed" as const,
        relationshipId: field<string>(record, "relationshipId"),
        fromPersonId: field<string>(record, "fromPersonId"),
        toMovieId: field<string>(record, "toMovieId"),
      })),
      ...results.produced.records.map((record) => ({
        kind: "produced" as const,
        relationshipId: field<string>(record, "relationshipId"),
        fromPersonId: field<string>(record, "fromPersonId"),
        toMovieId: field<string>(record, "toMovieId"),
      })),
    ],
  });
}

export async function exportNeo4jSnapshot(driver: Driver, database: string, exportedAt = new Date().toISOString()): Promise<MovieGraphDumpV1> {
  const session = driver.session({ database, defaultAccessMode: "READ" });
  const transaction = session.beginTransaction();
  try {
    const results: ExportResults = {
      people: await transaction.run(exportQueries.people),
      movies: await transaction.run(exportQueries.movies),
      actedIn: await transaction.run(exportQueries.actedIn),
      directed: await transaction.run(exportQueries.directed),
      produced: await transaction.run(exportQueries.produced),
    };
    await transaction.commit();
    return buildDump(results, exportedAt);
  } catch (error) {
    await transaction.rollback();
    throw error;
  } finally {
    await session.close();
  }
}
