import type { Driver } from "neo4j-driver";
import { z } from "zod";
import { HelixHttpClient } from "./helix-http.js";
import {
  movieDetailsRequest,
  personFilmographyRequest,
} from "./helix-queries.js";

const personSchema = z.object({
  personId: z.string(),
  name: z.string(),
  born: z.number().int(),
});
const movieSchema = z.object({
  movieId: z.string(),
  title: z.string(),
  released: z.number().int(),
  tagline: z.string(),
});
const castMemberSchema = personSchema.extend({ roles: z.array(z.string()) });
const filmographyMovieSchema = movieSchema.extend({
  roles: z.array(z.string()),
});

export type Person = z.infer<typeof personSchema>;
export type Movie = z.infer<typeof movieSchema>;
export type CastMember = z.infer<typeof castMemberSchema>;
export type FilmographyMovie = z.infer<typeof filmographyMovieSchema>;
export type MovieDetails = {
  movie: Movie;
  cast: CastMember[];
  directors: Person[];
  producers: Person[];
};
export type PersonFilmography = { person: Person; movies: FilmographyMovie[] };

export interface MovieRepository {
  movieDetails(movieId: string): Promise<MovieDetails | null>;
  personFilmography(personId: string): Promise<PersonFilmography | null>;
}

export const cypherQueries = {
  movieDetails: `
    MATCH (movie:Movie {movieId: $movieId})
    RETURN movie{.movieId, .title, .released, .tagline} AS movie,
           [(person:Person)-[credit:ACTED_IN]->(movie) |
             person{.personId, .name, .born, roles: credit.roles}] AS cast,
           [(person:Person)-[:DIRECTED]->(movie) |
             person{.personId, .name, .born}] AS directors,
           [(person:Person)-[:PRODUCED]->(movie) |
             person{.personId, .name, .born}] AS producers
  `,
  personFilmography: `
    MATCH (person:Person {personId: $personId})
    OPTIONAL MATCH (person)-[credit:ACTED_IN]->(movie:Movie)
    WITH person, credit, movie
    ORDER BY movie.released DESC, movie.title ASC
    RETURN person{.personId, .name, .born} AS person,
           collect(CASE WHEN movie IS NULL THEN null
             ELSE movie{.movieId, .title, .released, .tagline, roles: credit.roles}
           END) AS movies
  `,
} as const;

function one(value: unknown): unknown | undefined {
  if (Array.isArray(value)) return value[0];
  return value;
}

function responseObject(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("query response must be an object");
  return value as Record<string, unknown>;
}

export function normalizeMovieDetails(value: unknown): MovieDetails | null {
  const response = responseObject(value);
  const movie = one(response.movie);
  if (movie === undefined || movie === null) return null;
  return {
    movie: movieSchema.parse(movie),
    cast: z
      .array(castMemberSchema)
      .parse(response.cast ?? [])
      .sort((left, right) => left.personId.localeCompare(right.personId)),
    directors: z
      .array(personSchema)
      .parse(response.directors ?? [])
      .sort((left, right) => left.personId.localeCompare(right.personId)),
    producers: z
      .array(personSchema)
      .parse(response.producers ?? [])
      .sort((left, right) => left.personId.localeCompare(right.personId)),
  };
}

export function normalizePersonFilmography(
  value: unknown,
): PersonFilmography | null {
  const response = responseObject(value);
  const person = one(response.person);
  if (person === undefined || person === null) return null;
  return {
    person: personSchema.parse(person),
    movies: z
      .array(filmographyMovieSchema)
      .parse(response.movies ?? [])
      .sort(
        (left, right) =>
          right.released - left.released ||
          left.title.localeCompare(right.title),
      ),
  };
}

export class Neo4jMovieRepository implements MovieRepository {
  constructor(
    private readonly driver: Driver,
    private readonly database: string,
  ) {}

  async movieDetails(movieId: string): Promise<MovieDetails | null> {
    const result = await this.driver.executeQuery(
      cypherQueries.movieDetails,
      { movieId },
      { database: this.database, routing: "READ" },
    );
    const record = result.records[0];
    if (record === undefined) return null;
    return normalizeMovieDetails({
      movie: record.get("movie"),
      cast: record.get("cast"),
      directors: record.get("directors"),
      producers: record.get("producers"),
    });
  }

  async personFilmography(personId: string): Promise<PersonFilmography | null> {
    const result = await this.driver.executeQuery(
      cypherQueries.personFilmography,
      { personId },
      { database: this.database, routing: "READ" },
    );
    const record = result.records[0];
    if (record === undefined) return null;
    return normalizePersonFilmography({
      person: record.get("person"),
      movies: record.get("movies"),
    });
  }
}

export class HelixMovieRepository implements MovieRepository {
  constructor(private readonly client: HelixHttpClient) {}

  async movieDetails(movieId: string): Promise<MovieDetails | null> {
    return normalizeMovieDetails(
      await this.client.execute(movieDetailsRequest(movieId)),
    );
  }

  async personFilmography(personId: string): Promise<PersonFilmography | null> {
    return normalizePersonFilmography(
      await this.client.execute(personFilmographyRequest(personId)),
    );
  }
}
