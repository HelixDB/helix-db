import {
  BatchCondition,
  BindingProjection,
  EdgeRef,
  IndexSpec,
  NodeRef,
  Predicate,
  Projection,
  PropertyInput,
  PropertyProjection,
  defineParams,
  g,
  param,
  readBatch,
  writeBatch,
  type QueryRequest,
} from "@helix-db/helix-db";
import type { ActedInRelationship, DirectedRelationship, MovieNode, PersonNode, ProducedRelationship } from "./model.js";

const movieDetailsParams = defineParams({ movie_id: param.string() });
const filmographyParams = defineParams({ person_id: param.string() });
const rowsParams = defineParams({ rows: param.array(param.object(param.value())) });

export function movieDetailsRequest(movieId: string): QueryRequest {
  const query = readBatch()
    .varAs("movie_node", g().nWithLabel("Movie").where(Predicate.eqParam("movieId", "movie_id")))
    .varAs(
      "movie",
      g().n(NodeRef.var("movie_node")).project([
        PropertyProjection.new("movieId"),
        PropertyProjection.new("title"),
        PropertyProjection.new("released"),
        PropertyProjection.new("tagline"),
      ]),
    )
    .varAs(
      "cast",
      g().n(NodeRef.var("movie_node")).inE("ACTED_IN").project([
        Projection.fromEndpoint("personId", "personId"),
        Projection.fromEndpoint("name", "name"),
        Projection.fromEndpoint("born", "born"),
        Projection.property("roles", "roles"),
      ]),
    )
    .varAs(
      "directors",
      g().n(NodeRef.var("movie_node")).inE("DIRECTED").project([
        Projection.fromEndpoint("personId", "personId"),
        Projection.fromEndpoint("name", "name"),
        Projection.fromEndpoint("born", "born"),
      ]),
    )
    .varAs(
      "producers",
      g().n(NodeRef.var("movie_node")).inE("PRODUCED").project([
        Projection.fromEndpoint("personId", "personId"),
        Projection.fromEndpoint("name", "name"),
        Projection.fromEndpoint("born", "born"),
      ]),
    )
    .returning(["movie", "cast", "directors", "producers"]);
  return query.toQueryRequest(movieDetailsParams, { movie_id: movieId }, { queryName: "movie_details" });
}

export function personFilmographyRequest(personId: string): QueryRequest {
  const query = readBatch()
    .varAs("person_node", g().nWithLabel("Person").where(Predicate.eqParam("personId", "person_id")))
    .varAs(
      "person",
      g().n(NodeRef.var("person_node")).project([
        PropertyProjection.new("personId"),
        PropertyProjection.new("name"),
        PropertyProjection.new("born"),
      ]),
    )
    .varAs(
      "movies",
      g()
        .n(NodeRef.var("person_node"))
        .outE("ACTED_IN")
        .bind("credit")
        .outN()
        .bind("movie")
        .projectBindings([
          BindingProjection.binding("movie", "movieId", "movieId"),
          BindingProjection.binding("movie", "title", "title"),
          BindingProjection.binding("movie", "released", "released"),
          BindingProjection.binding("movie", "tagline", "tagline"),
          BindingProjection.binding("credit", "roles", "roles"),
        ]),
    )
    .returning(["person", "movies"]);
  return query.toQueryRequest(filmographyParams, { person_id: personId }, { queryName: "person_filmography" });
}

export function bootstrapIndexesRequest(): QueryRequest {
  return writeBatch()
    .varAs("person_id", g().createIndexIfNotExists(IndexSpec.nodeUniqueEquality("Person", "personId")))
    .varAs("movie_id", g().createIndexIfNotExists(IndexSpec.nodeUniqueEquality("Movie", "movieId")))
    .varAs("acted_in_id", g().createIndexIfNotExists(IndexSpec.edgeEquality("ACTED_IN", "relationshipId")))
    .varAs("directed_id", g().createIndexIfNotExists(IndexSpec.edgeEquality("DIRECTED", "relationshipId")))
    .varAs("produced_id", g().createIndexIfNotExists(IndexSpec.edgeEquality("PRODUCED", "relationshipId")))
    .returning(["person_id", "movie_id", "acted_in_id", "directed_id", "produced_id"])
    .toQueryRequest({ queryName: "bootstrap_movie_graph_indexes" });
}

export function upsertPeopleRequest(people: readonly PersonNode[]): QueryRequest {
  const body = writeBatch()
    .varAs("existing", g().nWithLabel("Person").where(Predicate.eqParam("personId", "personId")))
    .varAsIf(
      "updated",
      BatchCondition.varNotEmpty("existing"),
      g()
        .n(NodeRef.var("existing"))
        .setProperty("name", PropertyInput.param("name"))
        .setProperty("born", PropertyInput.param("born")),
    )
    .varAsIf(
      "created",
      BatchCondition.varEmpty("existing"),
      g().addN("Person", {
        personId: PropertyInput.param("personId"),
        name: PropertyInput.param("name"),
        born: PropertyInput.param("born"),
      }),
    );
  const rows = people.map(({ personId, name, born }) => ({ personId, name, born }));
  return writeBatch().forEachParam("rows", body).returning(["updated", "created"]).toQueryRequest(rowsParams, { rows }, { queryName: "upsert_people" });
}

export function upsertMoviesRequest(movies: readonly MovieNode[]): QueryRequest {
  const body = writeBatch()
    .varAs("existing", g().nWithLabel("Movie").where(Predicate.eqParam("movieId", "movieId")))
    .varAsIf(
      "updated",
      BatchCondition.varNotEmpty("existing"),
      g()
        .n(NodeRef.var("existing"))
        .setProperty("title", PropertyInput.param("title"))
        .setProperty("released", PropertyInput.param("released"))
        .setProperty("tagline", PropertyInput.param("tagline")),
    )
    .varAsIf(
      "created",
      BatchCondition.varEmpty("existing"),
      g().addN("Movie", {
        movieId: PropertyInput.param("movieId"),
        title: PropertyInput.param("title"),
        released: PropertyInput.param("released"),
        tagline: PropertyInput.param("tagline"),
      }),
    );
  const rows = movies.map(({ movieId, title, released, tagline }) => ({ movieId, title, released, tagline }));
  return writeBatch().forEachParam("rows", body).returning(["updated", "created"]).toQueryRequest(rowsParams, { rows }, { queryName: "upsert_movies" });
}

type Relationship = ActedInRelationship | DirectedRelationship | ProducedRelationship;

export function replaceRelationshipsRequest(kind: Relationship["kind"], relationships: readonly Relationship[]): QueryRequest {
  const label = kind === "acted_in" ? "ACTED_IN" : kind === "directed" ? "DIRECTED" : "PRODUCED";
  const properties: Record<string, PropertyInput> = { relationshipId: PropertyInput.param("relationshipId") };
  if (kind === "acted_in") properties.roles = PropertyInput.param("roles");

  const body = writeBatch()
    .varAs("from", g().nWithLabel("Person").where(Predicate.eqParam("personId", "fromPersonId")))
    .varAs("to", g().nWithLabel("Movie").where(Predicate.eqParam("movieId", "toMovieId")))
    .varAs("existing", g().eWithLabel(label).where(Predicate.eqParam("relationshipId", "relationshipId")))
    .varAsIf("removed", BatchCondition.varNotEmpty("existing"), g().dropEdgeById(EdgeRef.var("existing")))
    .varAs("created", g().n(NodeRef.var("from")).addE(label, NodeRef.var("to"), properties));

  const rows = relationships.map((relationship) =>
    relationship.kind === "acted_in"
      ? {
          relationshipId: relationship.relationshipId,
          fromPersonId: relationship.fromPersonId,
          toMovieId: relationship.toMovieId,
          roles: relationship.roles,
        }
      : {
          relationshipId: relationship.relationshipId,
          fromPersonId: relationship.fromPersonId,
          toMovieId: relationship.toMovieId,
        },
  );
  return writeBatch()
    .forEachParam("rows", body)
    .returning(["removed", "created"])
    .toQueryRequest(rowsParams, { rows }, { queryName: `replace_${kind}_relationships` });
}

export function graphCountsRequest(): QueryRequest {
  return readBatch()
    .varAs("people", g().nWithLabel("Person").count())
    .varAs("movies", g().nWithLabel("Movie").count())
    .varAs("acted_in", g().eWithLabel("ACTED_IN").count())
    .varAs("directed", g().eWithLabel("DIRECTED").count())
    .varAs("produced", g().eWithLabel("PRODUCED").count())
    .returning(["people", "movies", "acted_in", "directed", "produced"])
    .toQueryRequest({ queryName: "movie_graph_counts" });
}
