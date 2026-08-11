import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  AggregateFunction,
  BatchCondition,
  CompareOp,
  DateTime,
  QueryRequest,
  QueryValue,
  EdgeRef,
  Expr,
  IndexSpec,
  NodeRef,
  Order,
  Predicate,
  Projection,
  BindingProjection,
  PropertyInput,
  PropertyValue,
  QueryParamType,
  RepeatConfig,
  ShortestPathDirection,
  SourcePredicate,
  Step,
  StreamBound,
  Traversal,
  VectorDistanceMetric,
  g,
  readBatch,
  stringifyJson,
  sub,
  writeBatch,
} from "../../src/index.js";
import { typescriptGeneratedRoot } from "./paths.js";

export type Fixture = {
  bucket: "runtime" | "json-only";
  name: string;
  request: QueryRequest;
};

await resetDir(join(typescriptGeneratedRoot, "runtime"));
await resetDir(join(typescriptGeneratedRoot, "json-only"));

const fixtures = [...runtimeFixtures(), ...nodePermutationFixtures(), ...jsonOnlyFixtures()];
for (const fixture of fixtures) {
  await writeFile(join(typescriptGeneratedRoot, fixture.bucket, `${fixture.name}.json`), fixture.request.toJsonString());
}

async function resetDir(path: string) {
  await rm(path, { recursive: true, force: true });
  await mkdir(path, { recursive: true });
}

function runtime(name: string, request: QueryRequest): Fixture {
  return { bucket: "runtime", name, request };
}

function jsonOnly(name: string, request: QueryRequest): Fixture {
  return { bucket: "json-only", name, request };
}

function withParams(request: QueryRequest, values: [string, QueryValue][], types: [string, QueryParamType][]): QueryRequest {
  if (values.length !== types.length) throw new TypeError("fixture schema/value mismatch");
  values.forEach(([valueName, value], index) => {
    const [typeName, type] = types[index]!;
    if (valueName !== typeName) throw new TypeError("fixture parameter name mismatch");
    request.insertTypedParameter(valueName, type, value);
  });
  return request;
}

function userProps(
  externalId: string,
  name: string,
  age: number,
  score: number,
  status: string,
  city: string,
  bio: string,
  embedding: number[],
): [string, PropertyInput][] {
  return [
    ["externalId", PropertyInput.value(externalId)],
    ["name", PropertyInput.value(name)],
    ["age", PropertyInput.value(age)],
    ["score", PropertyInput.value(PropertyValue.f64(score))],
    ["status", PropertyInput.value(status)],
    ["tenantId", PropertyInput.value("tenant-a")],
    ["city", PropertyInput.value(city)],
    ["bio", PropertyInput.value(bio)],
    ["createdAt", PropertyInput.value(DateTime.fromMillis(1_776_000_000_000))],
    ["embedding", PropertyInput.value(PropertyValue.f32Array(embedding))],
  ];
}

function nestedMetadataProperty(externalID: string, score: number): PropertyValue {
  return PropertyValue.object({ externalID, score, tags: ["alpha", 7] });
}

function nestedMetadataParam(externalID: string, score: number) {
  return { externalID, score, tags: ["alpha", 7] };
}

export function runtimeFixtures(): Fixture[] {
  return [
    runtime(
      "001-write-seed-core",
      QueryRequest.write(
        writeBatch()
          .varAs(
            "alice",
            g().addN(
              "ParityUser",
              userProps("user-alice", "Alice", 31, 90.5, "active", "London", "Alice writes graph database tests", [1.0, 0.0, 0.0]),
            ),
          )
          .varAs(
            "bob",
            g().addN(
              "ParityUser",
              userProps("user-bob", "Bob", 27, 72.25, "active", "Paris", "Bob likes traversal testing", [0.9, 0.1, 0.0]),
            ),
          )
          .varAs(
            "carol",
            g().addN(
              "ParityUser",
              userProps("user-carol", "Carol", 42, 64.0, "inactive", "Berlin", "Carol archives old records", [0.0, 1.0, 0.0]),
            ),
          )
          .varAs(
            "alice_follows_bob",
            g()
              .n(NodeRef.var("alice"))
              .addE("FOLLOWS", NodeRef.var("bob"), [
                ["weight", PropertyInput.value(PropertyValue.f64(1.0))],
                ["since", PropertyInput.value("2024-01-01")],
                ["note", PropertyInput.value("Alice follows Bob")],
                ["embedding", PropertyInput.value(PropertyValue.f32Array([1.0, 0.0]))],
              ]),
          )
          .varAs(
            "bob_follows_carol",
            g()
              .n(NodeRef.var("bob"))
              .addE("FOLLOWS", NodeRef.var("carol"), [
                ["weight", PropertyInput.value(PropertyValue.f64(0.5))],
                ["since", PropertyInput.value("2024-02-01")],
                ["note", PropertyInput.value("Bob follows Carol")],
                ["embedding", PropertyInput.value(PropertyValue.f32Array([0.0, 1.0]))],
              ]),
          )
          .returning(["alice", "bob", "carol", "alice_follows_bob", "bob_follows_carol"]),
      ),
    ),
    runtime(
      "002-read-count-all-users",
      QueryRequest.read(readBatch().varAs("user_count", g().nWithLabel("ParityUser").count()).returning(["user_count"])),
    ),
    runtime(
      "003-read-source-predicate-and-count",
      QueryRequest.read(
        readBatch()
          .varAs(
            "active_adults",
            g()
              .nWithLabelWhere("ParityUser", SourcePredicate.and([SourcePredicate.eq("status", "active"), SourcePredicate.gte("age", 30)]))
              .count(),
          )
          .returning(["active_adults"]),
      ),
    ),
    runtime(
      "004-read-value-map-projection",
      QueryRequest.read(
        readBatch()
          .varAs(
            "alice",
            g()
              .nWithLabel("ParityUser")
              .where(Predicate.eq("externalId", "user-alice"))
              .project([
                Projection.property("externalId", "id"),
                Projection.property("name", "name"),
                Projection.expr("score_plus_one", Expr.prop("score").add(Expr.val(PropertyValue.f64(1.0)))),
                Projection.expr("status_label", Expr.case([[Predicate.eq("status", "active"), Expr.val("enabled")]], Expr.val("disabled"))),
              ]),
          )
          .returning(["alice"]),
      ),
    ),
    runtime(
      "005-read-order-range-values",
      QueryRequest.read(
        readBatch()
          .varAs(
            "ordered",
            g()
              .nWithLabel("ParityUser")
              .orderByMultiple([
                ["status", Order.Asc],
                ["age", Order.Desc],
              ])
              .range(0, 2)
              .valueMap(["externalId", "age", "status"]),
          )
          .returning(["ordered"]),
      ),
    ),
    runtime(
      "006-read-edge-count",
      QueryRequest.read(
        readBatch()
          .varAs("edge_count", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "user-alice")).outE("FOLLOWS").count())
          .returning(["edge_count"]),
      ),
    ),
    runtime(
      "007-read-edge-properties",
      QueryRequest.read(
        readBatch()
          .varAs(
            "edges",
            g()
              .eWithLabel("FOLLOWS")
              .edgeHas("weight", PropertyInput.value(PropertyValue.f64(1.0)))
              .edgeProperties(),
          )
          .returning(["edges"]),
      ),
    ),
    runtime(
      "008-read-edge-endpoints",
      QueryRequest.read(
        readBatch()
          .varAs("from_nodes", g().eWithLabel("FOLLOWS").edgeHasLabel("FOLLOWS").inN().valueMap(["externalId", "name"]))
          .varAs("to_nodes", g().eWithLabel("FOLLOWS").outN().valueMap(["externalId", "name"]))
          .returning(["from_nodes", "to_nodes"]),
      ),
    ),
    runtime(
      "009-read-conditional-var-not-empty",
      QueryRequest.read(
        readBatch()
          .varAs("alice", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "user-alice")))
          .varAsIf(
            "friends",
            BatchCondition.varNotEmpty("alice"),
            g().n(NodeRef.var("alice")).out("FOLLOWS").valueMap(["externalId", "name"]),
          )
          .returning(["alice", "friends"]),
      ),
    ),
    runtime(
      "010-read-conditional-var-empty",
      QueryRequest.read(
        readBatch()
          .varAs("missing", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "missing-user")))
          .varAsIf("fallback", BatchCondition.varEmpty("missing"), g().nWithLabel("ParityUser").limit(1).valueMap(["externalId"]))
          .returning(["missing", "fallback"]),
      ),
    ),
    runtime(
      "011-read-conditional-var-min-size-prev",
      QueryRequest.read(
        readBatch()
          .varAs("users", g().nWithLabel("ParityUser").limit(3))
          .varAsIf("min_two", BatchCondition.varMinSize("users", 2), g().n(NodeRef.var("users")).count())
          .varAsIf("prev_ok", BatchCondition.prevNotEmpty(), g().n(NodeRef.var("users")).exists())
          .returning(["min_two", "prev_ok"]),
      ),
    ),
    runtime(
      "012-read-foreach-param",
      withParams(
        QueryRequest.read(
          readBatch()
            .forEachParam(
              "lookups",
              readBatch().varAs(
                "matched",
                g().nWithLabel("ParityUser").where(Predicate.eqParam("externalId", "externalId")).valueMap(["externalId", "name"]),
              ),
            )
            .returning(["matched"]),
        ),
        [["lookups", [{ externalId: "user-alice" }, { externalId: "user-carol" }]]],
        [["lookups", QueryParamType.array(QueryParamType.object())]],
      ),
    ),
    runtime(
      "013-write-foreach-param-create",
      withParams(
        QueryRequest.write(
          writeBatch()
            .forEachParam(
              "rows",
              writeBatch().varAs(
                "created",
                g().addN("ParityEvent", [
                  ["eventId", PropertyInput.param("eventId")],
                  ["kind", PropertyInput.param("kind")],
                  ["score", PropertyInput.param("score")],
                ]),
              ),
            )
            .returning(["created"]),
        ),
        [
          [
            "rows",
            [
              { eventId: "event-1", kind: "click", score: 10 },
              { eventId: "event-2", kind: "view", score: 5 },
            ],
          ],
        ],
        [["rows", QueryParamType.array(QueryParamType.object())]],
      ),
    ),
    runtime(
      "014-read-after-foreach-param",
      QueryRequest.read(readBatch().varAs("event_count", g().nWithLabel("ParityEvent").count()).returning(["event_count"])),
    ),
    runtime(
      "015-write-set-remove-properties",
      QueryRequest.write(
        writeBatch()
          .varAs(
            "updated",
            g()
              .nWithLabel("ParityUser")
              .where(Predicate.eq("externalId", "user-bob"))
              .setProperty("status", PropertyInput.value("inactive"))
              .setProperty("updatedAt", PropertyInput.value(DateTime.fromMillis(1_777_000_000_000)))
              .removeProperty("city")
              .count(),
          )
          .returning(["updated"]),
      ),
    ),
    runtime(
      "016-read-updated-properties",
      QueryRequest.read(
        readBatch()
          .varAs(
            "bob",
            g()
              .nWithLabel("ParityUser")
              .where(Predicate.eq("externalId", "user-bob"))
              .valueMap(["externalId", "status", "updatedAt", "city"]),
          )
          .returning(["bob"]),
      ),
    ),
    runtime(
      "017-read-repeat-union",
      QueryRequest.read(
        readBatch()
          .varAs(
            "walked",
            g()
              .nWithLabel("ParityUser")
              .where(Predicate.eq("externalId", "user-alice"))
              .repeat(RepeatConfig.new(sub().out("FOLLOWS")).times(2).emitAll().maxDepth(4))
              .union([sub().out("FOLLOWS"), sub().in("FOLLOWS")])
              .dedup()
              .valueMap(["externalId", "name"]),
          )
          .returning(["walked"]),
      ),
    ),
    runtime(
      "018-read-choose-coalesce-optional",
      QueryRequest.read(
        readBatch()
          .varAs(
            "branched",
            g()
              .nWithLabel("ParityUser")
              .where(Predicate.eq("externalId", "user-alice"))
              .choose(Predicate.eq("status", "active"), sub().out("FOLLOWS"), sub().in("FOLLOWS"))
              .coalesce([sub().out("FOLLOWS"), sub().in("FOLLOWS")])
              .optional(sub().out("FOLLOWS"))
              .dedup()
              .valueMap(["externalId", "name"]),
          )
          .returning(["branched"]),
      ),
    ),
    runtime(
      "019-read-aggregations",
      QueryRequest.read(
        readBatch()
          .varAs("by_status", g().nWithLabel("ParityUser").groupCount("status"))
          .varAs("mean_score", g().nWithLabel("ParityUser").aggregateBy(AggregateFunction.Mean, "score"))
          .varAs("max_age", g().nWithLabel("ParityUser").aggregateBy(AggregateFunction.Max, "age"))
          .returning(["by_status", "mean_score", "max_age"]),
      ),
    ),
    runtime(
      "020-write-index-create",
      QueryRequest.write(
        writeBatch()
          .varAs("node_eq", g().createIndexIfNotExists(IndexSpec.nodeEquality("ParityUser", "externalId")))
          .varAs("node_range", g().createIndexIfNotExists(IndexSpec.nodeRange("ParityUser", "age")))
          .varAs("edge_eq", g().createIndexIfNotExists(IndexSpec.edgeEquality("FOLLOWS", "since")))
          .varAs("edge_range", g().createIndexIfNotExists(IndexSpec.edgeRange("FOLLOWS", "weight")))
          .returning(["node_eq", "node_range", "edge_eq", "edge_range"]),
      ),
    ),
    runtime(
      "021-read-parameter-types",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "matches",
              g()
                .nWithLabel("ParityUser")
                .where(Predicate.isInParam("status", "statuses"))
                .where(Predicate.gteParam("createdAt", "created_after"))
                .limit(Expr.param("limit"))
                .valueMap(["externalId", "status"]),
            )
            .returning(["matches"]),
        ),
        [
          ["statuses", ["active", "inactive"]],
          ["created_after", "2026-01-01T00:00:00.000Z"],
          ["limit", 5],
        ],
        [
          ["statuses", QueryParamType.array(QueryParamType.string())],
          ["created_after", QueryParamType.dateTime()],
          ["limit", QueryParamType.i64()],
        ],
      ),
    ),
    runtime(
      "022-write-property-value-variants",
      QueryRequest.write(
        writeBatch()
          .varAs(
            "variant_node",
            g().addN("ParityVariant", [
              ["nullValue", PropertyInput.value(PropertyValue.null())],
              ["boolValue", PropertyInput.value(true)],
              ["i64Value", PropertyInput.value(PropertyValue.i64(9_223_372_036_854_775_000n))],
              ["dateTimeValue", PropertyInput.value(DateTime.fromMillis(-1))],
              ["f64Value", PropertyInput.value(3.25)],
              ["f32Value", PropertyInput.value(PropertyValue.f32(1.5))],
              ["stringValue", PropertyInput.value("variant")],
              ["bytesValue", PropertyInput.value(PropertyValue.bytes([1, 2, 3]))],
              ["i64Array", PropertyInput.value(PropertyValue.i64Array([1, 2, 3]))],
              ["f64Array", PropertyInput.value(PropertyValue.f64Array([1.0, 2.0]))],
              ["f32Array", PropertyInput.value(PropertyValue.f32Array([1.0, 2.0]))],
              ["stringArray", PropertyInput.value(PropertyValue.stringArray(["a", "b"]))],
            ]),
          )
          .returning(["variant_node"]),
      ),
    ),
    runtime(
      "023-read-property-value-variants",
      QueryRequest.read(readBatch().varAs("variant", g().nWithLabel("ParityVariant").valueMap(null)).returning(["variant"])),
    ),
    runtime(
      "024-write-text-vector-indexes",
      QueryRequest.write(
        writeBatch()
          .varAs("node_text", g().createTextIndexNodes("ParityUser", "bio", null))
          .varAs("node_vector", g().createVectorIndexNodes("ParityUser", "embedding", 3, VectorDistanceMetric.Cosine, null))
          .varAs("edge_text", g().createTextIndexEdges("FOLLOWS", "note", null))
          .varAs("edge_vector", g().createVectorIndexEdges("FOLLOWS", "embedding", 2, VectorDistanceMetric.Cosine, null))
          .returning(["node_text", "node_vector", "edge_text", "edge_vector"]),
      ),
    ),
    runtime(
      "025-read-text-search-nodes",
      QueryRequest.read(
        readBatch()
          .varAs("text_hits", g().textSearchNodes("ParityUser", "bio", "graph", 5, null).valueMap(["externalId", "bio", "$distance"]))
          .returning(["text_hits"]),
      ),
    ),
    runtime(
      "026-read-vector-search-nodes",
      QueryRequest.read(
        readBatch()
          .varAs(
            "vector_hits",
            g()
              .vectorSearchNodes("ParityUser", "embedding", [1.0, 0.0, 0.0], 3, null)
              .project([Projection.property("externalId", "externalId"), Projection.property("$distance", "distance")]),
          )
          .returning(["vector_hits"]),
      ),
    ),
    runtime(
      "027-read-text-search-edges",
      QueryRequest.read(
        readBatch()
          .varAs("edge_text_hits", g().textSearchEdges("FOLLOWS", "note", "follows", 5, null).edgeProperties())
          .returning(["edge_text_hits"]),
      ),
    ),
    runtime(
      "028-read-vector-search-edges",
      QueryRequest.read(
        readBatch()
          .varAs("edge_vector_hits", g().vectorSearchEdges("FOLLOWS", "embedding", [1.0, 0.0], 5, null).edgeProperties())
          .returning(["edge_vector_hits"]),
      ),
    ),
    runtime(
      "029-write-drop-temp-node",
      QueryRequest.write(
        writeBatch()
          .varAs("temp", g().addN("ParityTemp", [["name", PropertyInput.value("temp")]]))
          .varAs("dropped", g().n(NodeRef.var("temp")).drop().count())
          .returning(["dropped"]),
      ),
    ),
    runtime(
      "030-read-final-counts",
      QueryRequest.read(
        readBatch()
          .varAs("users", g().nWithLabel("ParityUser").count())
          .varAs("events", g().nWithLabel("ParityEvent").count())
          .varAs("variants", g().nWithLabel("ParityVariant").count())
          .returning(["users", "events", "variants"]),
      ),
    ),
    runtime(
      "031-read-source-predicate-eq-param",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "user",
              g()
                .nWhere(SourcePredicate.and([SourcePredicate.eq("$label", "ParityUser"), SourcePredicate.eq("name", Expr.param("name"))]))
                .valueMap(["externalId", "name"]),
            )
            .returning(["user"]),
        ),
        [["name", "Alice"]],
        [["name", QueryParamType.string()]],
      ),
    ),
    runtime(
      "032-read-source-predicate-between-param",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "adults",
              g()
                .nWhere(
                  SourcePredicate.and([
                    SourcePredicate.eq("$label", "ParityUser"),
                    SourcePredicate.between("age", Expr.param("min_age"), 65),
                  ]),
                )
                .valueMap(["externalId", "age"]),
            )
            .returning(["adults"]),
        ),
        [["min_age", 30]],
        [["min_age", QueryParamType.i64()]],
      ),
    ),
    runtime(
      "900-write-active-text-items",
      QueryRequest.write(
        writeBatch()
          .varAs(
            "source",
            g().addN("ParityUser", [
              ["externalId", PropertyInput.value("active-text-source")],
              ["bio", PropertyInput.value("activeinsertnode")],
            ]),
          )
          .varAs("target", g().addN("ParityUser", [["externalId", PropertyInput.value("active-text-target")]]))
          .varAs(
            "edge",
            g()
              .n(NodeRef.var("source"))
              .addE("FOLLOWS", NodeRef.var("target"), [["note", PropertyInput.value("activeinsertedge")]]),
          )
          .returning(["source", "target", "edge"]),
      ),
    ),
    runtime(
      "901-read-active-text-items",
      QueryRequest.read(
        readBatch()
          .varAs("nodes", g().textSearchNodes("ParityUser", "bio", "activeinsertnode", 5, null).count())
          .varAs("edges", g().textSearchEdges("FOLLOWS", "note", "activeinsertedge", 5, null).count())
          .returning(["nodes", "edges"]),
      ),
    ),
    runtime(
      "902-write-remove-indexed-properties",
      QueryRequest.write(
        writeBatch()
          .varAs(
            "nodes",
            g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "active-text-source")).removeProperty("bio").count(),
          )
          .varAs("edges", g().eWithLabel("FOLLOWS").where(Predicate.eq("note", "activeinsertedge")).removeProperty("note").count())
          .returning(["nodes", "edges"]),
      ),
    ),
    runtime(
      "903-read-removed-indexed-properties",
      QueryRequest.read(
        readBatch()
          .varAs("nodes", g().textSearchNodes("ParityUser", "bio", "activeinsertnode", 5, null).count())
          .varAs("edges", g().textSearchEdges("FOLLOWS", "note", "activeinsertedge", 5, null).count())
          .returning(["nodes", "edges"]),
      ),
    ),
    runtime(
      "904-write-text-drop-candidates",
      QueryRequest.write(
        writeBatch()
          .varAs(
            "source",
            g().addN("ParityUser", [
              ["externalId", PropertyInput.value("drop-text-source")],
              ["bio", PropertyInput.value("dropitemnode")],
            ]),
          )
          .varAs("target", g().addN("ParityUser", [["externalId", PropertyInput.value("drop-text-target")]]))
          .varAs(
            "edge",
            g()
              .n(NodeRef.var("source"))
              .addE("FOLLOWS", NodeRef.var("target"), [["note", PropertyInput.value("dropitemedge")]]),
          )
          .returning(["source", "target", "edge"]),
      ),
    ),
    runtime(
      "905-read-text-drop-candidates",
      QueryRequest.read(
        readBatch()
          .varAs("nodes", g().textSearchNodes("ParityUser", "bio", "dropitemnode", 5, null).count())
          .varAs("edges", g().textSearchEdges("FOLLOWS", "note", "dropitemedge", 5, null).count())
          .returning(["nodes", "edges"]),
      ),
    ),
    runtime(
      "906-write-drop-indexed-items",
      QueryRequest.write(
        writeBatch()
          .varAs("edge_matches", g().eWithLabel("FOLLOWS").where(Predicate.eq("note", "dropitemedge")))
          .varAs("edges", g().dropEdgeById(EdgeRef.var("edge_matches")).count())
          .varAs("source", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "drop-text-source")).drop().count())
          .varAs("target", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "drop-text-target")).drop().count())
          .varAs("active_source", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "active-text-source")).drop().count())
          .varAs("active_target", g().nWithLabel("ParityUser").where(Predicate.eq("externalId", "active-text-target")).drop().count())
          .returning(["edges", "source", "target", "active_source", "active_target"]),
      ),
    ),
    runtime(
      "907-read-dropped-indexed-items",
      QueryRequest.read(
        readBatch()
          .varAs("nodes", g().textSearchNodes("ParityUser", "bio", "dropitemnode", 5, null).count())
          .varAs("edges", g().textSearchEdges("FOLLOWS", "note", "dropitemedge", 5, null).count())
          .returning(["nodes", "edges"]),
      ),
    ),
    runtime(
      "908-write-drop-text-indexes",
      QueryRequest.write(
        writeBatch()
          .varAs("node_text", g().dropIndex(IndexSpec.nodeText("ParityUser", "bio", null)))
          .varAs("edge_text", g().dropIndex(IndexSpec.edgeText("FOLLOWS", "note", null)))
          .returning(["node_text", "edge_text"]),
      ),
    ),
  ];
}

export function nodePermutationFixtures(): Fixture[] {
  const sources = ["label", "where", "all"] as const;
  const filters = ["none", "has", "logic", "expr"] as const;
  const bounds = ["none", "limit", "skip", "range"] as const;
  const terminals = ["count", "exists", "value_map", "project"] as const;
  const fixtures: Fixture[] = [];
  let index = 100;
  for (const source of sources) {
    for (const filter of filters) {
      for (const bound of bounds) {
        for (const terminal of terminals) {
          fixtures.push(
            runtime(
              `${String(index).padStart(3, "0")}-combo-node-${source}-${filter}-${bound}-${terminal}`,
              QueryRequest.read(nodeComboBatch(source, filter, bound, terminal)),
            ),
          );
          index += 1;
        }
      }
    }
  }
  return fixtures;
}

function nodeComboBatch(source: string, filter: string, bound: string, terminal: string) {
  const traversal = applyNodeBound(applyNodeFilter(nodeSource(source), filter), bound).orderBy("externalId", Order.Asc);
  const terminalTraversal =
    terminal === "count"
      ? traversal.count()
      : terminal === "exists"
        ? traversal.exists()
        : terminal === "value_map"
          ? traversal.valueMap(["externalId", "name", "age", "status"])
          : terminal === "project"
            ? traversal.project([
                Projection.property("externalId", "externalId"),
                Projection.property("status", "status"),
                Projection.expr("age_plus_two", Expr.prop("age").add(Expr.val(2))),
              ])
            : (() => {
                throw new Error(`unknown terminal ${terminal}`);
              })();
  return readBatch().varAs("result", terminalTraversal).returning(["result"]);
}

function nodeSource(source: string): Traversal<"nodes", "read"> {
  if (source === "label") return g().nWithLabel("ParityUser");
  if (source === "where") return g().nWhere(SourcePredicate.eq("$label", "ParityUser"));
  if (source === "all") return g().n(NodeRef.all()).hasLabel("ParityUser");
  throw new Error(`unknown source ${source}`);
}

function applyNodeFilter(traversal: Traversal<"nodes", "read">, filter: string): Traversal<"nodes", "read"> {
  if (filter === "none") return traversal;
  if (filter === "has") return traversal.has("status", "active");
  if (filter === "logic") {
    return traversal.where(
      Predicate.and([
        Predicate.hasKey("externalId"),
        Predicate.or([Predicate.startsWith("name", "A"), Predicate.endsWith("name", "b")]),
        Predicate.not(Predicate.isNull("age")),
      ]),
    );
  }
  if (filter === "expr") {
    return traversal.where(
      Predicate.compare(Expr.prop("score").add(Expr.val(PropertyValue.f64(1.0))), CompareOp.Gt, Expr.val(PropertyValue.f64(65.0))),
    );
  }
  throw new Error(`unknown filter ${filter}`);
}

function applyNodeBound(traversal: Traversal<"nodes", "read">, bound: string): Traversal<"nodes", "read"> {
  if (bound === "none") return traversal;
  if (bound === "limit") return traversal.limit(2);
  if (bound === "skip") return traversal.skip(1);
  if (bound === "range") return traversal.range(0, 2);
  throw new Error(`unknown bound ${bound}`);
}

function jsonOnlyFixtures(): Fixture[] {
  return [
    jsonOnly(
      "900-exhaustive-raw-read-steps",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "raw_nodes",
              Traversal.fromSteps(
                [
                  Step.n(NodeRef.param("node_ids")),
                  Step.has("name", "Alice"),
                  Step.where(Predicate.containsParam("bio", "needle")),
                  Step.limit(StreamBound.expr(Expr.param("limit"))),
                  Step.skip(StreamBound.expr(Expr.param("skip"))),
                  Step.range(StreamBound.literal(0), StreamBound.expr(Expr.param("end"))),
                  Step.as("a"),
                  Step.store("stored"),
                  Step.select("stored"),
                  Step.dedup(),
                  Step.within("stored"),
                  Step.without("missing"),
                  Step.fold(),
                  Step.unfold(),
                  Step.path(),
                  Step.simplePath(),
                  Step.withSack(0),
                  Step.sackSet("score"),
                  Step.sackAdd("score"),
                  Step.sackGet(),
                  Step.project([Projection.property("externalId", "externalId"), Projection.expr("neg_age", Expr.prop("age").neg())]),
                ],
                "nodes",
                "read",
              ),
            )
            .varAs(
              "raw_edges",
              Traversal.fromSteps(
                [
                  Step.e(EdgeRef.param("edge_ids")),
                  Step.where(SourcePredicate.or([SourcePredicate.hasKey("since"), SourcePredicate.startsWith("note", "Alice")])),
                  Step.edgeHas("weight", PropertyInput.value(PropertyValue.f64(1.0))),
                  Step.edgeHasLabel("FOLLOWS"),
                  Step.orderBy("weight", Order.Desc),
                  Step.edgeProperties(),
                ],
                "edges",
                "read",
              ),
            )
            .varAs("index_operation", g().getIndexOperation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"))
            .returning(["raw_nodes", "raw_edges", "index_operation"]),
        ),
        [
          ["node_ids", [1, 2]],
          ["edge_ids", [1]],
          ["needle", "graph"],
          ["limit", 10],
          ["skip", 0],
          ["end", 10],
        ],
        [
          ["node_ids", QueryParamType.array(QueryParamType.i64())],
          ["edge_ids", QueryParamType.array(QueryParamType.i64())],
          ["needle", QueryParamType.string()],
          ["limit", QueryParamType.i64()],
          ["skip", QueryParamType.i64()],
          ["end", QueryParamType.i64()],
        ],
      ),
    ),
    jsonOnly(
      "901-exhaustive-raw-write-steps",
      QueryRequest.write(
        writeBatch()
          .varAs("raw_unique_index", g().createIndexIfNotExists(IndexSpec.nodeUniqueEquality("ParityUser", "externalId")))
          .varAs("raw_drop_range_index", g().dropIndex(IndexSpec.nodeRange("ParityUser", "age")))
          .varAs("raw_node_vector_index", g().createVectorIndexNodes("ParityUser", "embedding", 3, VectorDistanceMetric.Cosine, "tenantId"))
          .varAs("raw_edge_vector_index", g().createVectorIndexEdges("FOLLOWS", "embedding", 2, VectorDistanceMetric.Cosine, "tenantId"))
          .varAs("raw_node_text_index", g().createTextIndexNodes("ParityUser", "bio", "tenantId"))
          .varAs("raw_edge_text_index", g().createTextIndexEdges("FOLLOWS", "note", "tenantId"))
          .varAs(
            "raw_mutations",
            Traversal.fromSteps(
              [
                Step.addN("RawNode", [["name", PropertyInput.value("raw")]]),
                Step.addE("RAW_EDGE", NodeRef.var("raw_mutations"), [["weight", PropertyInput.value(1)]]),
                Step.setProperty("name", PropertyInput.expr(Expr.param("name"))),
                Step.removeProperty("old"),
                Step.dropEdge(NodeRef.ids([999_999])),
                Step.dropEdgeLabeled(NodeRef.ids([999_999]), "RAW_EDGE"),
                Step.dropEdgeById(EdgeRef.ids([999_999])),
                Step.drop(),
              ],
              "nodes",
              "write",
            ),
          )
          .varAs("retry_index_operation", g().retryIndexOperation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"))
          .varAs("abort_index_operation", g().abortIndexOperation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"))
          .returning([
            "raw_unique_index",
            "raw_drop_range_index",
            "raw_node_vector_index",
            "raw_edge_vector_index",
            "raw_node_text_index",
            "raw_edge_text_index",
            "raw_mutations",
            "retry_index_operation",
            "abort_index_operation",
          ]),
      ),
    ),
    jsonOnly(
      "902-query-value-and-param-type-shapes",
      withParams(
        QueryRequest.read(readBatch().varAs("empty", g().nWithLabel("Missing").count()).returning(["empty"])),
        [
          ["null", QueryValue.null()],
          ["bool", QueryValue.bool(true)],
          ["i64", QueryValue.i64(9_223_372_036_854_775_807n)],
          ["f64", QueryValue.f64(1.25)],
          ["f32", QueryValue.f32(1.5)],
          ["string", QueryValue.string("value")],
          ["array", QueryValue.array([1, "two"])],
          ["object", QueryValue.object({ nested: true })],
        ],
        [
          ["null", QueryParamType.value()],
          ["bool", QueryParamType.bool()],
          ["i64", QueryParamType.i64()],
          ["f64", QueryParamType.f64()],
          ["f32", QueryParamType.f32()],
          ["string", QueryParamType.string()],
          ["array", QueryParamType.array(QueryParamType.value())],
          ["object", QueryParamType.object()],
        ],
      ),
    ),
    jsonOnly(
      "903-empty-source-vector-text-runtime-inputs",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "vector_nodes",
              g().vectorSearchNodesWith(
                "ParityUser",
                "embedding",
                PropertyInput.param("query_vector"),
                Expr.param("limit"),
                PropertyInput.param("tenant"),
              ),
            )
            .varAs(
              "text_nodes",
              g().textSearchNodesWith(
                "ParityUser",
                "bio",
                PropertyInput.param("query_text"),
                Expr.param("limit"),
                PropertyInput.param("tenant"),
              ),
            )
            .returning(["vector_nodes", "text_nodes"]),
        ),
        [
          ["query_vector", [1.0, 0.0, 0.0]],
          ["query_text", "graph"],
          ["limit", 5],
          ["tenant", "tenant-a"],
        ],
        [
          ["query_vector", QueryParamType.array(QueryParamType.f64())],
          ["query_text", QueryParamType.string()],
          ["limit", QueryParamType.i64()],
          ["tenant", QueryParamType.string()],
        ],
      ),
    ),
    jsonOnly(
      "904-empty-query-and-node-edge-ref-shapes",
      QueryRequest.read(
        readBatch()
          .varAs("all_nodes", Traversal.fromSteps([Step.n(NodeRef.all()), Step.count()], "nodes", "read"))
          .varAs("node_ids", Traversal.fromSteps([Step.n(NodeRef.ids([1, 2])), Step.id()], "nodes", "read"))
          .varAs("node_var", Traversal.fromSteps([Step.n(NodeRef.var("all_nodes")), Step.label()], "nodes", "read"))
          .varAs("edge_ids", Traversal.fromSteps([Step.e(EdgeRef.ids([1, 2])), Step.id()], "edges", "read"))
          .varAs("edge_var", Traversal.fromSteps([Step.e(EdgeRef.var("edge_ids")), Step.label()], "edges", "read"))
          .returning(["all_nodes", "node_ids", "node_var", "edge_ids", "edge_var"]),
      ),
    ),
    jsonOnly(
      "905-empty-traversal-source-mutators",
      QueryRequest.write(
        writeBatch()
          .varAs("inject", Traversal.new().inject("some_var").count())
          .varAs("drop_edge_by_id", g().dropEdgeById(EdgeRef.id(123_456)).count())
          .returning(["inject", "drop_edge_by_id"]),
      ),
    ),
    jsonOnly(
      "906-nested-query-property-write-shapes",
      withParams(
        QueryRequest.write(
          writeBatch()
            .varAs(
              "created",
              g().addN("ParityNested", [
                ["name", PropertyInput.value("nested")],
                ["metadata", PropertyInput.value(nestedMetadataProperty("some_id", 20))],
              ]),
            )
            .varAs(
              "updated",
              g().n(NodeRef.var("created")).setProperty("metadata", PropertyInput.param("metadata")).valueMap(["metadata.externalID"]),
            )
            .varAs("target", g().addN("ParityNestedTarget", [["name", PropertyInput.value("target")]]))
            .varAs(
              "edge",
              g()
                .n(NodeRef.var("created"))
                .addE("NESTED_LINK", NodeRef.var("target"), [["metadata", PropertyInput.value(nestedMetadataProperty("edge_id", 5))]])
                .count(),
            )
            .returning(["created", "updated", "edge"]),
        ),
        [["metadata", nestedMetadataParam("param_id", 22)]],
        [["metadata", QueryParamType.object()]],
      ),
    ),
    jsonOnly(
      "907-nested-query-property-read-shapes",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "nested_users",
              g()
                .nWhere(
                  SourcePredicate.and([
                    SourcePredicate.eq("$label", "ParityNested"),
                    SourcePredicate.eq("metadata.externalID", Expr.param("external_id")),
                  ]),
                )
                .where(Predicate.compare(Expr.prop("metadata.score"), CompareOp.Gt, Expr.val(10)))
                .orderByMultiple([
                  ["metadata.score", Order.Desc],
                  ["name", Order.Asc],
                ])
                .project([
                  Projection.property("metadata.externalID", "external_id"),
                  Projection.expr("score_copy", Expr.prop("metadata.score")),
                ]),
            )
            .varAs("nested_values", g().nWithLabel("ParityNested").values(["metadata.externalID"]))
            .varAs("nested_map", g().nWithLabel("ParityNested").valueMap(["metadata.externalID", "metadata.score"]))
            .varAs(
              "nested_edges",
              g()
                .eWhere(
                  SourcePredicate.and([SourcePredicate.eq("$label", "NESTED_LINK"), SourcePredicate.eq("metadata.externalID", "edge_id")]),
                )
                .edgeHas("metadata.externalID", PropertyInput.value("edge_id"))
                .edgeProperties(),
            )
            .returning(["nested_users", "nested_values", "nested_map", "nested_edges"]),
        ),
        [["external_id", "param_id"]],
        [["external_id", QueryParamType.string()]],
      ),
    ),
    jsonOnly(
      "908-edge-endpoint-projection",
      QueryRequest.read(
        readBatch()
          .varAs(
            "endpoints",
            g()
              .eWithLabel("FOLLOWS")
              .project([
                Projection.fromEndpoint("externalId", "from_id"),
                Projection.toEndpoint("externalId", "to_id"),
                Projection.property("$id", "edge_id"),
              ]),
          )
          .returning(["endpoints"]),
      ),
    ),
    jsonOnly(
      "909-row-binding-basic-projection",
      QueryRequest.read(
        readBatch()
          .varAs(
            "bindings",
            g()
              .nWithLabel("ParityService")
              .bind("service")
              .projectBindings([
                BindingProjection.binding("service", "$id", "service_id"),
                BindingProjection.current("metadata.name", "current_name"),
                BindingProjection.binding("missing_binding", "externalId", "missing_external_id"),
              ]),
          )
          .returning(["bindings"]),
      ),
    ),
    jsonOnly(
      "910-row-binding-branch-distinct-projection",
      QueryRequest.read(
        readBatch()
          .varAs(
            "workloads",
            g()
              .nWithLabel("ParityService")
              .bind("service")
              .out("ROUTES_TO")
              .bind("pod")
              .optional(sub().in("CREATES").bind("deployment"))
              .union([sub().in("MANAGES").bind("owner"), sub().out("ROUTES_TO").bind("workload")])
              .projectDistinctBindings([
                BindingProjection.binding("service", "$id", "service_id"),
                BindingProjection.coalesce(
                  [
                    BindingProjection.bindingRef("deployment", "$id"),
                    BindingProjection.bindingRef("owner", "$id"),
                    BindingProjection.bindingRef("workload", "$id"),
                  ],
                  "workload_id",
                ),
              ]),
          )
          .returning(["workloads"]),
      ),
    ),
    jsonOnly(
      "911-range-index-direction",
      QueryRequest.write(
        writeBatch()
          .varAs("node_desc", g().createIndexIfNotExists(IndexSpec.nodeRangeDesc("ParityUser", "age")))
          .varAs("edge_desc", g().createIndexIfNotExists(IndexSpec.edgeRangeDesc("FOLLOWS", "weight")))
          .varAs("node_asc", g().createIndexIfNotExists(IndexSpec.nodeRange("ParityUser", "score")))
          .returning(["node_desc", "edge_desc", "node_asc"]),
      ),
    ),
    jsonOnly(
      "912-shortest-path-terminal",
      withParams(
        QueryRequest.read(
          readBatch()
            .varAs(
              "path",
              g().shortestPath(NodeRef.id(1n), NodeRef.param("target"), 5, {
                label: "FOLLOWS",
                direction: ShortestPathDirection.Both,
              }),
            )
            .returning(["path"]),
        ),
        [["target", 3]],
        [["target", QueryParamType.i64()]],
      ),
    ),
    remainingReadContractFixture(),
    remainingWriteContractFixture(),
    neo4jMigrationMovieDetailsFixture(),
    neo4jMigrationFilmographyFixture(),
  ];
}

function neo4jMigrationMovieDetailsFixture(): Fixture {
  const request = QueryRequest.read(
    readBatch()
      .varAs("movie_node", g().nWithLabel("Movie").where(Predicate.eqParam("movieId", "movie_id")))
      .varAs(
        "movie",
        g()
          .n(NodeRef.var("movie_node"))
          .project([
            Projection.property("movieId", "movieId"),
            Projection.property("title", "title"),
            Projection.property("released", "released"),
            Projection.property("tagline", "tagline"),
          ]),
      )
      .varAs(
        "cast",
        g()
          .n(NodeRef.var("movie_node"))
          .inE("ACTED_IN")
          .project([
            Projection.fromEndpoint("personId", "personId"),
            Projection.fromEndpoint("name", "name"),
            Projection.fromEndpoint("born", "born"),
            Projection.property("roles", "roles"),
          ]),
      )
      .varAs(
        "directors",
        g()
          .n(NodeRef.var("movie_node"))
          .inE("DIRECTED")
          .project([
            Projection.fromEndpoint("personId", "personId"),
            Projection.fromEndpoint("name", "name"),
            Projection.fromEndpoint("born", "born"),
          ]),
      )
      .varAs(
        "producers",
        g()
          .n(NodeRef.var("movie_node"))
          .inE("PRODUCED")
          .project([
            Projection.fromEndpoint("personId", "personId"),
            Projection.fromEndpoint("name", "name"),
            Projection.fromEndpoint("born", "born"),
          ]),
      )
      .returning(["movie", "cast", "directors", "producers"]),
  ).withQueryName("movie_details");
  return jsonOnly(
    "915-neo4j-migration-movie-details",
    withParams(request, [["movie_id", "m-matrix"]], [["movie_id", QueryParamType.string()]]),
  );
}

function neo4jMigrationFilmographyFixture(): Fixture {
  const request = QueryRequest.read(
    readBatch()
      .varAs("person_node", g().nWithLabel("Person").where(Predicate.eqParam("personId", "person_id")))
      .varAs(
        "person",
        g()
          .n(NodeRef.var("person_node"))
          .project([Projection.property("personId", "personId"), Projection.property("name", "name"), Projection.property("born", "born")]),
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
      .returning(["person", "movies"]),
  ).withQueryName("person_filmography");
  return jsonOnly(
    "916-neo4j-migration-person-filmography",
    withParams(request, [["person_id", "p-keanu"]], [["person_id", QueryParamType.string()]]),
  );
}

function remainingReadContractFixture(): Fixture {
  const comparisons = Predicate.and([
    Predicate.neq("neq", 1),
    Predicate.gt("gt", 1),
    Predicate.gte("gte", 1),
    Predicate.lt("lt", 1),
    Predicate.lte("lte", 1),
    Predicate.between("between", 1, 3),
    Predicate.endsWith("suffix", "end"),
    Predicate.isIn("status", ["active", "inactive"]),
    Predicate.isNull("missing"),
    Predicate.isNotNull("present"),
    Predicate.not(Predicate.eq("disabled", true)),
    Predicate.compare(Expr.id(), CompareOp.Eq, Expr.val(1)),
    Predicate.compare(Expr.id(), CompareOp.Neq, Expr.val(1)),
    Predicate.compare(Expr.id(), CompareOp.Gt, Expr.val(1)),
    Predicate.compare(Expr.id(), CompareOp.Gte, Expr.val(1)),
    Predicate.compare(Expr.id(), CompareOp.Lt, Expr.val(1)),
    Predicate.compare(Expr.id(), CompareOp.Lte, Expr.val(1)),
  ]);
  const request = QueryRequest.read(
    readBatch()
      .varAs(
        "expressions_and_predicates",
        g()
          .n(NodeRef.all())
          .where(comparisons)
          .project([
            Projection.expr("id", Expr.id()),
            Projection.expr("timestamp", Expr.timestamp()),
            Projection.expr("datetime", Expr.datetime()),
            Projection.expr("null", Expr.val(PropertyValue.null())),
            Projection.expr("date_value", Expr.val(PropertyValue.dateTime(1_777_000_000_000))),
            Projection.expr("f32", Expr.val(PropertyValue.f32(1.25))),
            Projection.expr("bytes", Expr.val(PropertyValue.bytes([1, 2, 3]))),
            Projection.expr("i64_array", Expr.val(PropertyValue.i64Array([1, 2, 3]))),
            Projection.expr("f64_array", Expr.val(PropertyValue.f64Array([1.25, 2.5]))),
            Projection.expr("add", Expr.val(4).add(Expr.val(1))),
            Projection.expr("sub", Expr.val(4).sub(Expr.val(1))),
            Projection.expr("mul", Expr.val(4).mul(Expr.val(2))),
            Projection.expr("div", Expr.val(4).div(Expr.val(2))),
            Projection.expr("mod", Expr.val(5).modulo(Expr.val(2))),
            Projection.expr(
              "case",
              Expr.case([{ when: Predicate.eq("status", "active"), then: Expr.val("enabled") }], Expr.val("disabled")),
            ),
          ]),
      )
      .varAs("both", g().n(NodeRef.id(1)).both().count())
      .varAs("in_e", g().n(NodeRef.id(1)).inE().edgeProperties())
      .varAs("out_e", g().n(NodeRef.id(1)).outE().edgeProperties())
      .varAs("both_e", g().n(NodeRef.id(1)).bothE().edgeProperties())
      .varAs("in_n", g().e(EdgeRef.all()).inN().valueMap(null))
      .varAs("out_n", g().e(EdgeRef.all()).outN().valueMap(null))
      .varAs("other_n", g().e(EdgeRef.all()).otherN().valueMap(null))
      .varAs("direct_has_key", g().n(NodeRef.all()).hasKey("externalId").count())
      .varAs("has_label", g().n(NodeRef.all()).hasLabel("ParityUser").count())
      .varAs("exists", g().n(NodeRef.all()).exists())
      .varAs("choose", g().n(NodeRef.all()).choose(Predicate.isNotNull("status"), sub().out(), sub().in()).count())
      .varAs("coalesce", g().n(NodeRef.all()).coalesce([sub().out(), sub().in()]).count())
      .varAs("group", g().n(NodeRef.all()).group("status"))
      .varAs("group_count", g().n(NodeRef.all()).groupCount("status"))
      .varAs("aggregate_count", g().n(NodeRef.all()).aggregateBy(AggregateFunction.Count, "age"))
      .varAs("aggregate_sum", g().n(NodeRef.all()).aggregateBy(AggregateFunction.Sum, "age"))
      .varAs("aggregate_min", g().n(NodeRef.all()).aggregateBy(AggregateFunction.Min, "age"))
      .varAs("aggregate_max", g().n(NodeRef.all()).aggregateBy(AggregateFunction.Max, "age"))
      .varAs("aggregate_mean", g().n(NodeRef.all()).aggregateBy(AggregateFunction.Mean, "age"))
      .varAs("repeat_none", g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out())).count())
      .varAs("repeat_before", g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out()).emitBefore()).count())
      .varAs("repeat_after", g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out()).emitAfter()).count())
      .varAs("repeat_all", g().n(NodeRef.id(1)).repeat(RepeatConfig.new(sub().out()).emitAll()).count())
      .varAs("shortest_out", g().shortestPath(NodeRef.id(1), NodeRef.id(2), 5, { direction: ShortestPathDirection.Out }))
      .varAs("shortest_in", g().shortestPath(NodeRef.id(1), NodeRef.id(2), 5, { direction: ShortestPathDirection.In }))
      .varAs("vector_edges", g().vectorSearchEdges("FOLLOWS", "embedding", [1, 0], 5).edgeProperties())
      .varAs("vector_nodes_within", g().nWithLabel("ParityUser").vectorSearch("ParityUser", "embedding", [1, 0, 0], 5))
      .varAs("vector_edges_within", g().e(EdgeRef.all()).hasLabel("FOLLOWS").vectorSearch("FOLLOWS", "embedding", [1, 0], 5))
      .varAs("text_edges", g().textSearchEdges("FOLLOWS", "note", "graph", 5).edgeProperties())
      .varAsIf("previous", BatchCondition.prevNotEmpty(), g().n(NodeRef.all()).count())
      .varAsIf("not_empty", BatchCondition.varNotEmpty("expressions_and_predicates"), g().n(NodeRef.all()).count())
      .varAsIf("empty", BatchCondition.varEmpty("missing"), g().n(NodeRef.all()).count())
      .varAsIf("min_size", BatchCondition.varMinSize("expressions_and_predicates", 1), g().n(NodeRef.all()).count())
      .forEachParam("rows", readBatch().varAs("foreach", g().n(NodeRef.all()).count()))
      .returning([
        "expressions_and_predicates",
        "both",
        "in_e",
        "out_e",
        "both_e",
        "in_n",
        "out_n",
        "other_n",
        "direct_has_key",
        "has_label",
        "exists",
        "choose",
        "coalesce",
        "group",
        "group_count",
        "aggregate_count",
        "aggregate_sum",
        "aggregate_min",
        "aggregate_max",
        "aggregate_mean",
        "repeat_none",
        "repeat_before",
        "repeat_after",
        "repeat_all",
        "shortest_out",
        "shortest_in",
        "vector_edges",
        "vector_nodes_within",
        "vector_edges_within",
        "text_edges",
        "previous",
        "not_empty",
        "empty",
        "min_size",
        "foreach",
      ]),
  );
  request.insertTypedParameter("date_time", QueryParamType.dateTime(), "2026-01-01T00:00:00.000Z");
  return jsonOnly("913-remaining-read-contract", request);
}

function remainingWriteContractFixture(): Fixture {
  return jsonOnly(
    "914-remaining-write-contract",
    QueryRequest.write(
      writeBatch()
        .varAs("edge_equality", g().createIndexIfNotExists(IndexSpec.edgeEquality("FOLLOWS", "since")))
        .varAs(
          "node_euclidean",
          g().createIndexIfNotExists(IndexSpec.nodeVector("ParityUser", "euclidean_embedding", 4, VectorDistanceMetric.Euclidean)),
        )
        .varAs(
          "edge_manhattan",
          g().createIndexIfNotExists(IndexSpec.edgeVector("FOLLOWS", "manhattan_embedding", 4, VectorDistanceMetric.Manhattan)),
        )
        .returning(["edge_equality", "node_euclidean", "edge_manhattan"]),
    ),
  );
}

void stringifyJson;
