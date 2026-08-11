import { z } from "zod";

const identifier = z.string().trim().min(1);
const year = z.number().int().safe();

const personNodeSchema = z
  .object({
    kind: z.literal("person"),
    personId: identifier,
    name: z.string().trim().min(1),
    born: year,
  })
  .strict();

const movieNodeSchema = z
  .object({
    kind: z.literal("movie"),
    movieId: identifier,
    title: z.string().trim().min(1),
    released: year,
    tagline: z.string().trim().min(1),
  })
  .strict();

const actedInSchema = z
  .object({
    kind: z.literal("acted_in"),
    relationshipId: identifier,
    fromPersonId: identifier,
    toMovieId: identifier,
    roles: z.array(z.string().trim().min(1)).min(1),
  })
  .strict();

const directedSchema = z
  .object({
    kind: z.literal("directed"),
    relationshipId: identifier,
    fromPersonId: identifier,
    toMovieId: identifier,
  })
  .strict();

const producedSchema = z
  .object({
    kind: z.literal("produced"),
    relationshipId: identifier,
    fromPersonId: identifier,
    toMovieId: identifier,
  })
  .strict();

export const movieGraphDumpV1Schema = z
  .object({
    version: z.literal(1),
    exportedAt: z.iso.datetime({ offset: true }),
    nodes: z.array(z.discriminatedUnion("kind", [personNodeSchema, movieNodeSchema])),
    relationships: z.array(z.discriminatedUnion("kind", [actedInSchema, directedSchema, producedSchema])),
  })
  .strict();

export type PersonNode = z.infer<typeof personNodeSchema>;
export type MovieNode = z.infer<typeof movieNodeSchema>;
export type MovieGraphNode = PersonNode | MovieNode;
export type ActedInRelationship = z.infer<typeof actedInSchema>;
export type DirectedRelationship = z.infer<typeof directedSchema>;
export type ProducedRelationship = z.infer<typeof producedSchema>;
export type MovieGraphRelationship = ActedInRelationship | DirectedRelationship | ProducedRelationship;
export type MovieGraphDumpV1 = z.infer<typeof movieGraphDumpV1Schema>;

function duplicate(values: readonly string[]): string | undefined {
  const seen = new Set<string>();
  return values.find((value) => {
    if (seen.has(value)) return true;
    seen.add(value);
    return false;
  });
}

export function parseMovieGraphDump(input: unknown): MovieGraphDumpV1 {
  const dump = movieGraphDumpV1Schema.parse(input);
  const people = dump.nodes.filter((node): node is PersonNode => node.kind === "person");
  const movies = dump.nodes.filter((node): node is MovieNode => node.kind === "movie");

  const duplicatePerson = duplicate(people.map((person) => person.personId));
  if (duplicatePerson !== undefined) throw new TypeError(`duplicate Person.personId: ${duplicatePerson}`);
  const duplicateMovie = duplicate(movies.map((movie) => movie.movieId));
  if (duplicateMovie !== undefined) throw new TypeError(`duplicate Movie.movieId: ${duplicateMovie}`);
  const duplicateRelationship = duplicate(dump.relationships.map((relationship) => relationship.relationshipId));
  if (duplicateRelationship !== undefined) throw new TypeError(`duplicate relationshipId: ${duplicateRelationship}`);

  const personIds = new Set(people.map((person) => person.personId));
  const movieIds = new Set(movies.map((movie) => movie.movieId));
  for (const relationship of dump.relationships) {
    if (!personIds.has(relationship.fromPersonId)) {
      throw new TypeError(`${relationship.relationshipId} references missing Person ${relationship.fromPersonId}`);
    }
    if (!movieIds.has(relationship.toMovieId)) {
      throw new TypeError(`${relationship.relationshipId} references missing Movie ${relationship.toMovieId}`);
    }
    if (relationship.kind === "acted_in") {
      const duplicateRole = duplicate(relationship.roles);
      if (duplicateRole !== undefined) throw new TypeError(`${relationship.relationshipId} contains duplicate role ${duplicateRole}`);
    }
  }

  return dump;
}
