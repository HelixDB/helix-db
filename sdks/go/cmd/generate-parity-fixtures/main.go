package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	helix "github.com/helixdb/helix-db/sdks/go"
)

type fixture struct {
	bucket  string
	name    string
	request helix.Request
}

const transactionConflictAttempts = 8
const transactionConflictMessage = "Storage error: Transaction error: transaction conflict"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	out := "../tests/parity/generated/go"
	if len(os.Args) > 1 {
		out = os.Args[1]
	}

	if err := resetDir(filepath.Join(out, "runtime")); err != nil {
		return err
	}
	if err := resetDir(filepath.Join(out, "json-only")); err != nil {
		return err
	}

	fixtures := append(runtimeFixtures(), nodePermutationFixtures()...)
	fixtures = append(fixtures, jsonOnlyFixtures()...)

	seen := map[string]struct{}{}
	runtimeCount := 0
	jsonOnlyCount := 0
	for _, fixture := range fixtures {
		if fixture.bucket == "runtime" {
			runtimeCount++
		} else if fixture.bucket == "json-only" {
			jsonOnlyCount++
		} else {
			return fmt.Errorf("unknown fixture bucket %q", fixture.bucket)
		}

		rel := filepath.Join(fixture.bucket, fixture.name+".json")
		if _, ok := seen[rel]; ok {
			return fmt.Errorf("duplicate fixture path %s", rel)
		}
		seen[rel] = struct{}{}

		body, err := helix.MarshalRequest(fixture.request)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", rel, err)
		}
		if err := os.WriteFile(filepath.Join(out, rel), body, 0o644); err != nil {
			return err
		}
	}

	if runtimeCount != 233 {
		return fmt.Errorf("generated %d runtime fixtures, expected 233", runtimeCount)
	}
	if jsonOnlyCount != 17 {
		return fmt.Errorf("generated %d json-only fixtures, expected 17", jsonOnlyCount)
	}
	if results := os.Getenv("HELIX_EMBEDDED_PARITY_RESULTS"); results != "" {
		if err := executeEmbeddedFixtures(fixtures, results); err != nil {
			return err
		}
	}

	return nil
}

func executeEmbeddedFixtures(fixtures []fixture, results string) error {
	if err := resetDir(results); err != nil {
		return err
	}
	database := os.Getenv("HELIX_EMBEDDED_PARITY_DATABASE")
	if database == "" {
		database = "go-sdk-embedded-parity"
	}
	storage := os.Getenv("HELIX_EMBEDDED_PARITY_STORAGE")
	if storage == "" {
		storage = "memory"
	}
	var source helix.HelixDbSource
	switch storage {
	case "memory":
		source = helix.InMemorySource{Database: database}
	case "disk":
		root := os.Getenv("HELIX_EMBEDDED_PARITY_DISK_ROOT")
		if root == "" {
			return fmt.Errorf("HELIX_EMBEDDED_PARITY_DISK_ROOT is required for disk parity")
		}
		source = helix.DiskSource{Root: root, Database: database}
	default:
		return fmt.Errorf("unsupported embedded parity storage %s", storage)
	}
	cache := helix.EmbeddedCacheConfig{
		VectorMemoryBytes: 256 * 1024 * 1024,
		Mode:              helix.MemoryCache{},
	}
	client, err := helix.NewEmbeddedClientWithConfig(
		source,
		cache,
	)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	runtimeFixtures := make([]fixture, 0, len(fixtures))
	for _, candidate := range fixtures {
		if candidate.bucket == "runtime" {
			runtimeFixtures = append(runtimeFixtures, candidate)
		}
	}
	sort.Slice(runtimeFixtures, func(left, right int) bool {
		return runtimeFixtures[left].name < runtimeFixtures[right].name
	})
	for _, fixture := range runtimeFixtures {
		if storage == "disk" && fixture.name == "900-write-active-text-items" {
			if err := client.Close(); err != nil {
				return err
			}
			reader, err := helix.NewEmbeddedReaderClientWithConfig(source, cache)
			if err != nil {
				return err
			}
			for _, searchName := range []string{
				"025-read-text-search-nodes",
				"027-read-text-search-edges",
			} {
				search, err := requiredFixture(fixtures, searchName)
				if err != nil {
					_ = reader.Close()
					return err
				}
				var actual any
				if err := execEmbeddedWithRetry(reader, search.request, &actual); err != nil {
					_ = reader.Close()
					return fmt.Errorf("%s after disk reader reopen: %w", search.name, err)
				}
				expected, err := readJSONResult(filepath.Join(results, search.name+".json"))
				if err != nil {
					_ = reader.Close()
					return err
				}
				if !reflect.DeepEqual(actual, expected) {
					_ = reader.Close()
					return fmt.Errorf("%s changed after reopening a disk reader", search.name)
				}
			}
			if err := reader.Close(); err != nil {
				return err
			}
			client, err = helix.NewEmbeddedClientWithConfig(source, cache)
			if err != nil {
				return err
			}
		}
		var result any
		if err := execEmbeddedWithRetry(client, fixture.request, &result); err != nil {
			return fmt.Errorf("%s: %w", fixture.name, err)
		}
		if err := awaitIndexOperations(client, result); err != nil {
			return fmt.Errorf("%s: %w", fixture.name, err)
		}
		normalizeOperationIDs(result)
		body, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("marshal %s result: %w", fixture.name, err)
		}
		if err := os.WriteFile(filepath.Join(results, fixture.name+".json"), body, 0o644); err != nil {
			return err
		}
	}
	for _, searchName := range []string{
		"025-read-text-search-nodes",
		"027-read-text-search-edges",
	} {
		search, err := requiredFixture(fixtures, searchName)
		if err != nil {
			return err
		}
		var result any
		err = execEmbeddedWithRetry(client, search.request, &result)
		if err == nil {
			return fmt.Errorf("%s unexpectedly succeeded after index DROP", search.name)
		}
		if !strings.Contains(err.Error(), "index_not_found") {
			return fmt.Errorf("%s returned the wrong post-DROP error: %w", search.name, err)
		}
	}
	return nil
}

func requiredFixture(fixtures []fixture, name string) (fixture, error) {
	for _, candidate := range fixtures {
		if candidate.name == name {
			return candidate, nil
		}
	}
	return fixture{}, fmt.Errorf("missing fixture %s", name)
}

// execEmbeddedWithRetry retries only conflicts whose embedded storage
// transaction did not commit. Every other error, and exhaustion, is terminal.
func execEmbeddedWithRetry(client *helix.Client, request helix.Request, result any) error {
	for attempt := range transactionConflictAttempts {
		err := client.Exec(context.Background(), request, result)
		if err == nil {
			return nil
		}
		var helixError *helix.HelixError
		isTransactionConflict := errors.As(err, &helixError) &&
			helixError.Kind == helix.ErrorEmbedded &&
			strings.Contains(helixError.Details, transactionConflictMessage)
		if !isTransactionConflict || attempt+1 == transactionConflictAttempts {
			return err
		}
		time.Sleep((10 * time.Millisecond) << attempt)
	}
	panic("transaction conflict retry loop exhausted without an error")
}

func readJSONResult(path string) (any, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return value, nil
}

// awaitIndexOperations waits for asynchronous DDL before later fixtures use an index.
func awaitIndexOperations(client *helix.Client, result any) error {
	operationIDs := make(map[string]struct{})
	collectOperationIDs(result, operationIDs)
	for operationID := range operationIDs {
		deadline := time.Now().Add(60 * time.Second)
		for {
			request := helix.NewReadQueryRequest(
				helix.Read().
					VarAs("status", helix.G().GetIndexOperation(operationID)).
					Returning("status"),
			)
			var statusResult map[string]any
			if err := execEmbeddedWithRetry(client, request, &statusResult); err != nil {
				return fmt.Errorf("operation %s status: %w", operationID, err)
			}
			statusObject, ok := statusResult["status"].(map[string]any)
			if !ok {
				return fmt.Errorf("operation %s returned malformed status: %v", operationID, statusResult)
			}
			status, ok := statusObject["status"].(string)
			if !ok {
				return fmt.Errorf("operation %s returned malformed status: %v", operationID, statusResult)
			}
			if status == "succeeded" {
				break
			}
			if status != "queued" && status != "running" {
				return fmt.Errorf("operation %s reached unexpected status %s: %v", operationID, status, statusResult)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("operation %s did not finish within 60s", operationID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return nil
}

// collectOperationIDs collects operation IDs only from DDL receipt objects.
func collectOperationIDs(value any, ids map[string]struct{}) {
	switch value := value.(type) {
	case []any:
		for _, entry := range value {
			collectOperationIDs(entry, ids)
		}
	case map[string]any:
		kind, _ := value["kind"].(string)
		operationID, _ := value["operation_id"].(string)
		if (kind == "accepted" || kind == "existing_operation") && operationID != "" {
			ids[operationID] = struct{}{}
		}
		for _, entry := range value {
			collectOperationIDs(entry, ids)
		}
	}
}

// normalizeOperationIDs replaces random UUIDs while retaining the receipt shape.
func normalizeOperationIDs(value any) {
	switch value := value.(type) {
	case []any:
		for _, entry := range value {
			normalizeOperationIDs(entry)
		}
	case map[string]any:
		kind, _ := value["kind"].(string)
		if (kind == "accepted" || kind == "existing_operation") && value["operation_id"] != nil {
			value["operation_id"] = "<operation-id>"
		}
		for _, entry := range value {
			normalizeOperationIDs(entry)
		}
	}
}

func resetDir(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return os.MkdirAll(path, 0o755)
}

func runtime(name string, request helix.Request) fixture {
	return fixture{bucket: "runtime", name: name, request: request}
}

func jsonOnly(name string, request helix.Request) fixture {
	return fixture{bucket: "json-only", name: name, request: request}
}

func read() *helix.ReadQueryBuilder { return helix.ReadQuery("") }

func write() *helix.WriteQueryBuilder { return helix.WriteQuery("") }

func userProps(externalID, name string, age int64, score float64, status, city, bio string, embedding []float32) helix.Props {
	return helix.Props{
		helix.Prop("externalId", externalID),
		helix.Prop("name", name),
		helix.Prop("age", age),
		helix.Prop("score", helix.F64(score)),
		helix.Prop("status", status),
		helix.Prop("tenantId", "tenant-a"),
		helix.Prop("city", city),
		helix.Prop("bio", bio),
		helix.Prop("createdAt", helix.DateTimeFromMillis(1_776_000_000_000)),
		helix.Prop("embedding", helix.F32Array(embedding...)),
	}
}

func nestedMetadataProperty(externalID string, score int64) helix.PropertyValue {
	return helix.ObjectFromEntries(
		helix.Entry("externalID", externalID),
		helix.Entry("score", score),
		helix.Entry("tags", helix.Array(helix.String("alpha"), helix.I64(7))),
	)
}

func nestedMetadataParam(externalID string, score int64) map[string]any {
	return map[string]any{"externalID": externalID, "score": score, "tags": []any{"alpha", int64(7)}}
}

func exprPtr(expr helix.Expr) *helix.Expr { return &expr }

func inputPtr(input helix.PropertyInput) *helix.PropertyInput { return &input }

func runtimeFixtures() []fixture {
	return []fixture{
		runtime(
			"001-write-seed-core",
			write().
				VarAs("alice", helix.G().AddN("ParityUser", userProps("user-alice", "Alice", 31, 90.5, "active", "London", "Alice writes graph database tests", []float32{1.0, 0.0, 0.0}))).
				VarAs("bob", helix.G().AddN("ParityUser", userProps("user-bob", "Bob", 27, 72.25, "active", "Paris", "Bob likes traversal testing", []float32{0.9, 0.1, 0.0}))).
				VarAs("carol", helix.G().AddN("ParityUser", userProps("user-carol", "Carol", 42, 64.0, "inactive", "Berlin", "Carol archives old records", []float32{0.0, 1.0, 0.0}))).
				VarAs("alice_follows_bob", helix.G().N(helix.NodeVar("alice")).AddE("FOLLOWS", helix.NodeVar("bob"), helix.Props{
					helix.Prop("weight", helix.F64(1.0)),
					helix.Prop("since", "2024-01-01"),
					helix.Prop("note", "Alice follows Bob"),
					helix.Prop("embedding", helix.F32Array(1.0, 0.0)),
				})).
				VarAs("bob_follows_carol", helix.G().N(helix.NodeVar("bob")).AddE("FOLLOWS", helix.NodeVar("carol"), helix.Props{
					helix.Prop("weight", helix.F64(0.5)),
					helix.Prop("since", "2024-02-01"),
					helix.Prop("note", "Bob follows Carol"),
					helix.Prop("embedding", helix.F32Array(0.0, 1.0)),
				})).
				Returning("alice", "bob", "carol", "alice_follows_bob", "bob_follows_carol"),
		),
		runtime(
			"002-read-count-all-users",
			read().VarAs("user_count", helix.G().NWithLabel("ParityUser").Count()).Returning("user_count"),
		),
		runtime(
			"003-read-source-predicate-and-count",
			read().VarAs("active_adults", helix.G().NWithLabelWhere("ParityUser", helix.SourceAnd(helix.SourceEq("status", "active"), helix.SourceGte("age", int64(30)))).Count()).Returning("active_adults"),
		),
		runtime(
			"004-read-value-map-projection",
			read().VarAs("alice", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-alice")).Project(
				helix.ProjectPropAs("externalId", "id"),
				helix.ProjectPropAs("name", "name"),
				helix.ProjectExpr("score_plus_one", helix.ExprProp("score").Add(helix.ExprVal(helix.F64(1.0)))),
				helix.ProjectExpr("status_label", helix.ExprCase([]helix.WhenThen{{When: helix.PredEq("status", "active"), Then: helix.ExprVal("enabled")}}, exprPtr(helix.ExprVal("disabled")))),
			)).Returning("alice"),
		),
		runtime(
			"005-read-order-range-values",
			read().VarAs("ordered", helix.G().NWithLabel("ParityUser").OrderByMultiple(
				helix.Ordering{Property: "status", Order: helix.OrderAsc},
				helix.Ordering{Property: "age", Order: helix.OrderDesc},
			).Range(0, 2).ValueMap("externalId", "age", "status")).Returning("ordered"),
		),
		runtime(
			"006-read-edge-count",
			read().VarAs("edge_count", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-alice")).OutE("FOLLOWS").Count()).Returning("edge_count"),
		),
		runtime(
			"007-read-edge-properties",
			read().VarAs("edges", helix.G().EWithLabel("FOLLOWS").EdgeHas("weight", helix.F64(1.0)).EdgeProperties()).Returning("edges"),
		),
		runtime(
			"008-read-edge-endpoints",
			read().
				VarAs("from_nodes", helix.G().EWithLabel("FOLLOWS").EdgeHasLabel("FOLLOWS").InN().ValueMap("externalId", "name")).
				VarAs("to_nodes", helix.G().EWithLabel("FOLLOWS").OutN().ValueMap("externalId", "name")).
				Returning("from_nodes", "to_nodes"),
		),
		runtime(
			"009-read-conditional-var-not-empty",
			read().
				VarAs("alice", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-alice"))).
				VarAsIf("friends", helix.VarNotEmpty("alice"), helix.G().N(helix.NodeVar("alice")).Out("FOLLOWS").ValueMap("externalId", "name")).
				Returning("alice", "friends"),
		),
		runtime(
			"010-read-conditional-var-empty",
			read().
				VarAs("missing", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "missing-user"))).
				VarAsIf("fallback", helix.VarEmpty("missing"), helix.G().NWithLabel("ParityUser").Limit(1).ValueMap("externalId")).
				Returning("missing", "fallback"),
		),
		runtime(
			"011-read-conditional-var-min-size-prev",
			read().
				VarAs("users", helix.G().NWithLabel("ParityUser").Limit(3)).
				VarAsIf("min_two", helix.VarMinSize("users", 2), helix.G().N(helix.NodeVar("users")).Count()).
				VarAsIf("prev_ok", helix.PrevNotEmpty(), helix.G().N(helix.NodeVar("users")).Exists()).
				Returning("min_two", "prev_ok"),
		),
		fixtureReadForeachParam(),
		fixtureWriteForeachParamCreate(),
		runtime(
			"014-read-after-foreach-param",
			read().VarAs("event_count", helix.G().NWithLabel("ParityEvent").Count()).Returning("event_count"),
		),
		runtime(
			"015-write-set-remove-properties",
			write().VarAs("updated", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-bob")).SetProperty("status", "inactive").SetProperty("updatedAt", helix.DateTimeFromMillis(1_777_000_000_000)).RemoveProperty("city").Count()).Returning("updated"),
		),
		runtime(
			"016-read-updated-properties",
			read().VarAs("bob", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-bob")).ValueMap("externalId", "status", "updatedAt", "city")).Returning("bob"),
		),
		runtime(
			"017-read-repeat-union",
			read().VarAs("walked", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-alice")).Repeat(helix.Repeat(helix.Sub().Out("FOLLOWS")).WithTimes(2).EmitAll().WithMaxDepth(4)).Union(helix.Sub().Out("FOLLOWS"), helix.Sub().In("FOLLOWS")).Dedup().ValueMap("externalId", "name")).Returning("walked"),
		),
		runtime(
			"018-read-choose-coalesce-optional",
			read().VarAs("branched", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "user-alice")).Choose(helix.PredEq("status", "active"), helix.Sub().Out("FOLLOWS"), helix.Sub().In("FOLLOWS")).Coalesce(helix.Sub().Out("FOLLOWS"), helix.Sub().In("FOLLOWS")).Optional(helix.Sub().Out("FOLLOWS")).Dedup().ValueMap("externalId", "name")).Returning("branched"),
		),
		runtime(
			"019-read-aggregations",
			read().
				VarAs("by_status", helix.G().NWithLabel("ParityUser").GroupCount("status")).
				VarAs("mean_score", helix.G().NWithLabel("ParityUser").AggregateBy(helix.AggregateMean, "score")).
				VarAs("max_age", helix.G().NWithLabel("ParityUser").AggregateBy(helix.AggregateMax, "age")).
				Returning("by_status", "mean_score", "max_age"),
		),
		runtime(
			"020-write-index-create",
			write().
				VarAs("node_eq", helix.G().CreateIndexIfNotExists(helix.NodeEqualityIndex("ParityUser", "externalId"))).
				VarAs("node_range", helix.G().CreateIndexIfNotExists(helix.NodeRangeIndex("ParityUser", "age"))).
				VarAs("edge_eq", helix.G().CreateIndexIfNotExists(helix.EdgeEqualityIndex("FOLLOWS", "since"))).
				VarAs("edge_range", helix.G().CreateIndexIfNotExists(helix.EdgeRangeIndex("FOLLOWS", "weight"))).
				Returning("node_eq", "node_range", "edge_eq", "edge_range"),
		),
		fixtureReadParameterTypes(),
		runtime(
			"022-write-property-value-variants",
			write().VarAs("variant_node", helix.G().AddN("ParityVariant", helix.Props{
				helix.Prop("nullValue", helix.Null()),
				helix.Prop("boolValue", true),
				helix.Prop("i64Value", int64(9_223_372_036_854_775_000)),
				helix.Prop("dateTimeValue", helix.DateTimeFromMillis(-1)),
				helix.Prop("f64Value", helix.F64(3.25)),
				helix.Prop("f32Value", helix.F32(1.5)),
				helix.Prop("stringValue", "variant"),
				helix.Prop("bytesValue", helix.Bytes([]byte{1, 2, 3})),
				helix.Prop("i64Array", helix.I64Array(1, 2, 3)),
				helix.Prop("f64Array", helix.F64Array(1.0, 2.0)),
				helix.Prop("f32Array", helix.F32Array(1.0, 2.0)),
				helix.Prop("stringArray", helix.StringArray("a", "b")),
			})).Returning("variant_node"),
		),
		runtime(
			"023-read-property-value-variants",
			read().VarAs("variant", helix.G().NWithLabel("ParityVariant").ValueMapAll()).Returning("variant"),
		),
		runtime(
			"024-write-text-vector-indexes",
			write().
				VarAs("node_text", helix.G().CreateTextIndexNodes("ParityUser", "bio")).
				VarAs("node_vector", helix.G().CreateVectorIndexNodes("ParityUser", "embedding", 3, helix.VectorDistanceCosine)).
				VarAs("edge_text", helix.G().CreateTextIndexEdges("FOLLOWS", "note")).
				VarAs("edge_vector", helix.G().CreateVectorIndexEdges("FOLLOWS", "embedding", 2, helix.VectorDistanceCosine)).
				Returning("node_text", "node_vector", "edge_text", "edge_vector"),
		),
		runtime(
			"025-read-text-search-nodes",
			read().VarAs("text_hits", helix.G().TextSearchNodes("ParityUser", "bio", "graph", 5).ValueMap("externalId", "bio", "$distance")).Returning("text_hits"),
		),
		runtime(
			"026-read-vector-search-nodes",
			read().VarAs("vector_hits", helix.G().VectorSearchNodes("ParityUser", "embedding", []float32{1.0, 0.0, 0.0}, 3).Project(
				helix.ProjectPropAs("externalId", "externalId"),
				helix.ProjectPropAs("$distance", "distance"),
			)).Returning("vector_hits"),
		),
		runtime(
			"027-read-text-search-edges",
			read().VarAs("edge_text_hits", helix.G().TextSearchEdges("FOLLOWS", "note", "follows", 5).EdgeProperties()).Returning("edge_text_hits"),
		),
		runtime(
			"028-read-vector-search-edges",
			read().VarAs("edge_vector_hits", helix.G().VectorSearchEdges("FOLLOWS", "embedding", []float32{1.0, 0.0}, 5).EdgeProperties()).Returning("edge_vector_hits"),
		),
		runtime(
			"029-write-drop-temp-node",
			write().VarAs("temp", helix.G().AddN("ParityTemp", helix.Props{helix.Prop("name", "temp")})).VarAs("dropped", helix.G().N(helix.NodeVar("temp")).Drop().Count()).Returning("dropped"),
		),
		runtime(
			"030-read-final-counts",
			read().VarAs("users", helix.G().NWithLabel("ParityUser").Count()).VarAs("events", helix.G().NWithLabel("ParityEvent").Count()).VarAs("variants", helix.G().NWithLabel("ParityVariant").Count()).Returning("users", "events", "variants"),
		),
		fixtureReadSourcePredicateEqParam(),
		fixtureReadSourcePredicateBetweenParam(),
		runtime(
			"900-write-active-text-items",
			write().
				VarAs("source", helix.G().AddN("ParityUser", helix.Props{
					helix.Prop("externalId", "active-text-source"),
					helix.Prop("bio", "activeinsertnode"),
				})).
				VarAs("target", helix.G().AddN("ParityUser", helix.Props{
					helix.Prop("externalId", "active-text-target"),
				})).
				VarAs("edge", helix.G().N(helix.NodeVar("source")).AddE("FOLLOWS", helix.NodeVar("target"), helix.Props{
					helix.Prop("note", "activeinsertedge"),
				})).
				Returning("source", "target", "edge"),
		),
		runtime(
			"901-read-active-text-items",
			read().
				VarAs("nodes", helix.G().TextSearchNodes("ParityUser", "bio", "activeinsertnode", 5).Count()).
				VarAs("edges", helix.G().TextSearchEdges("FOLLOWS", "note", "activeinsertedge", 5).Count()).
				Returning("nodes", "edges"),
		),
		runtime(
			"902-write-remove-indexed-properties",
			write().
				VarAs("nodes", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "active-text-source")).RemoveProperty("bio").Count()).
				VarAs("edges", helix.G().EWithLabel("FOLLOWS").Where(helix.PredEq("note", "activeinsertedge")).RemoveProperty("note").Count()).
				Returning("nodes", "edges"),
		),
		runtime(
			"903-read-removed-indexed-properties",
			read().
				VarAs("nodes", helix.G().TextSearchNodes("ParityUser", "bio", "activeinsertnode", 5).Count()).
				VarAs("edges", helix.G().TextSearchEdges("FOLLOWS", "note", "activeinsertedge", 5).Count()).
				Returning("nodes", "edges"),
		),
		runtime(
			"904-write-text-drop-candidates",
			write().
				VarAs("source", helix.G().AddN("ParityUser", helix.Props{
					helix.Prop("externalId", "drop-text-source"),
					helix.Prop("bio", "dropitemnode"),
				})).
				VarAs("target", helix.G().AddN("ParityUser", helix.Props{
					helix.Prop("externalId", "drop-text-target"),
				})).
				VarAs("edge", helix.G().N(helix.NodeVar("source")).AddE("FOLLOWS", helix.NodeVar("target"), helix.Props{
					helix.Prop("note", "dropitemedge"),
				})).
				Returning("source", "target", "edge"),
		),
		runtime(
			"905-read-text-drop-candidates",
			read().
				VarAs("nodes", helix.G().TextSearchNodes("ParityUser", "bio", "dropitemnode", 5).Count()).
				VarAs("edges", helix.G().TextSearchEdges("FOLLOWS", "note", "dropitemedge", 5).Count()).
				Returning("nodes", "edges"),
		),
		runtime(
			"906-write-drop-indexed-items",
			write().
				VarAs("edge_matches", helix.G().EWithLabel("FOLLOWS").Where(helix.PredEq("note", "dropitemedge"))).
				VarAs("edges", helix.G().DropEdgeByID(helix.EdgeVar("edge_matches")).Count()).
				VarAs("source", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "drop-text-source")).Drop().Count()).
				VarAs("target", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "drop-text-target")).Drop().Count()).
				VarAs("active_source", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "active-text-source")).Drop().Count()).
				VarAs("active_target", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", "active-text-target")).Drop().Count()).
				Returning("edges", "source", "target", "active_source", "active_target"),
		),
		runtime(
			"907-read-dropped-indexed-items",
			read().
				VarAs("nodes", helix.G().TextSearchNodes("ParityUser", "bio", "dropitemnode", 5).Count()).
				VarAs("edges", helix.G().TextSearchEdges("FOLLOWS", "note", "dropitemedge", 5).Count()).
				Returning("nodes", "edges"),
		),
		runtime(
			"908-write-drop-text-indexes",
			write().
				VarAs("node_text", helix.G().DropIndex(helix.NodeTextIndex("ParityUser", "bio"))).
				VarAs("edge_text", helix.G().DropIndex(helix.EdgeTextIndex("FOLLOWS", "note"))).
				Returning("node_text", "edge_text"),
		),
	}
}

func fixtureReadForeachParam() fixture {
	q := read()
	q.ParamArray("lookups", []any{map[string]any{"externalId": "user-alice"}, map[string]any{"externalId": "user-carol"}}, helix.ParamTypeObject())
	return runtime(
		"012-read-foreach-param",
		q.ForEachParam("lookups", helix.Read().VarAs("matched", helix.G().NWithLabel("ParityUser").Where(helix.PredEq("externalId", helix.ExprParam("externalId"))).ValueMap("externalId", "name"))).Returning("matched"),
	)
}

func fixtureWriteForeachParamCreate() fixture {
	q := write()
	q.ParamArray("rows", []any{
		map[string]any{"eventId": "event-1", "kind": "click", "score": int64(10)},
		map[string]any{"eventId": "event-2", "kind": "view", "score": int64(5)},
	}, helix.ParamTypeObject())
	return runtime(
		"013-write-foreach-param-create",
		q.ForEachParam("rows", helix.Write().VarAs("created", helix.G().AddN("ParityEvent", helix.Props{
			helix.Prop("eventId", helix.ExprParam("eventId")),
			helix.Prop("kind", helix.ExprParam("kind")),
			helix.Prop("score", helix.ExprParam("score")),
		}))).Returning("created"),
	)
}

func fixtureReadParameterTypes() fixture {
	q := read()
	statuses := q.ParamArray("statuses", []string{"active", "inactive"}, helix.ParamTypeString())
	createdAfter := q.ParamDateTime("created_after", "2026-01-01T00:00:00.000Z")
	limit := q.ParamI64("limit", int64(5))
	return runtime(
		"021-read-parameter-types",
		q.VarAs("matches", helix.G().NWithLabel("ParityUser").Where(helix.PredIsIn("status", statuses)).Where(helix.PredGte("createdAt", createdAfter)).Limit(limit).ValueMap("externalId", "status")).Returning("matches"),
	)
}

func fixtureReadSourcePredicateEqParam() fixture {
	q := read()
	name := q.ParamString("name", "Alice")
	return runtime(
		"031-read-source-predicate-eq-param",
		q.VarAs("user", helix.G().NWhere(helix.SourceAnd(helix.SourceEq("$label", "ParityUser"), helix.SourceEq("name", name))).ValueMap("externalId", "name")).Returning("user"),
	)
}

func fixtureReadSourcePredicateBetweenParam() fixture {
	q := read()
	minAge := q.ParamI64("min_age", int64(30))
	return runtime(
		"032-read-source-predicate-between-param",
		q.VarAs("adults", helix.G().NWhere(helix.SourceAnd(helix.SourceEq("$label", "ParityUser"), helix.SourceBetween("age", minAge, int64(65)))).ValueMap("externalId", "age")).Returning("adults"),
	)
}

func nodePermutationFixtures() []fixture {
	sources := []string{"label", "where", "all"}
	filters := []string{"none", "has", "logic", "expr"}
	bounds := []string{"none", "limit", "skip", "range"}
	terminals := []string{"count", "exists", "value_map", "project"}

	fixtures := make([]fixture, 0, len(sources)*len(filters)*len(bounds)*len(terminals))
	index := 100
	for _, source := range sources {
		for _, filter := range filters {
			for _, bound := range bounds {
				for _, terminal := range terminals {
					name := fmt.Sprintf("%03d-combo-node-%s-%s-%s-%s", index, source, filter, bound, terminal)
					fixtures = append(fixtures, runtime(name, nodeComboBatch(source, filter, bound, terminal)))
					index++
				}
			}
		}
	}
	return fixtures
}

func nodeComboBatch(source, filter, bound, terminal string) helix.Request {
	traversal := applyNodeBound(applyNodeFilter(nodeSource(source), filter), bound).OrderBy("externalId", helix.OrderAsc)
	switch terminal {
	case "count":
		traversal = traversal.Count()
	case "exists":
		traversal = traversal.Exists()
	case "value_map":
		traversal = traversal.ValueMap("externalId", "name", "age", "status")
	case "project":
		traversal = traversal.Project(
			helix.ProjectPropAs("externalId", "externalId"),
			helix.ProjectPropAs("status", "status"),
			helix.ProjectExpr("age_plus_two", helix.ExprProp("age").Add(helix.ExprVal(int64(2)))),
		)
	default:
		panic("unknown terminal " + terminal)
	}
	return read().VarAs("result", traversal).Returning("result")
}

func nodeSource(source string) *helix.Traversal {
	switch source {
	case "label":
		return helix.G().NWithLabel("ParityUser")
	case "where":
		return helix.G().NWhere(helix.SourceEq("$label", "ParityUser"))
	case "all":
		return helix.G().N(helix.AllNodes()).HasLabel("ParityUser")
	default:
		panic("unknown source " + source)
	}
}

func applyNodeFilter(traversal *helix.Traversal, filter string) *helix.Traversal {
	switch filter {
	case "none":
		return traversal
	case "has":
		return traversal.Has("status", "active")
	case "logic":
		return traversal.Where(helix.PredAnd(
			helix.PredHasKey("externalId"),
			helix.PredOr(helix.PredStartsWith("name", "A"), helix.PredEndsWith("name", "b")),
			helix.PredNot(helix.PredIsNull("age")),
		))
	case "expr":
		return traversal.Where(helix.PredCompare(helix.ExprProp("score").Add(helix.ExprVal(helix.F64(1.0))), helix.CompareGt, helix.ExprVal(helix.F64(65.0))))
	default:
		panic("unknown filter " + filter)
	}
}

func applyNodeBound(traversal *helix.Traversal, bound string) *helix.Traversal {
	switch bound {
	case "none":
		return traversal
	case "limit":
		return traversal.Limit(2)
	case "skip":
		return traversal.Skip(1)
	case "range":
		return traversal.Range(0, 2)
	default:
		panic("unknown bound " + bound)
	}
}

func jsonOnlyFixtures() []fixture {
	return []fixture{
		fixtureRawReadSteps(),
		fixtureRawWriteSteps(),
		fixtureQueryValueShapes(),
		fixtureEmptySourceVectorTextRuntimeInputs(),
		jsonOnly(
			"904-empty-query-and-node-edge-ref-shapes",
			read().
				VarAs("all_nodes", helix.G().N(helix.AllNodes()).Count()).
				VarAs("node_ids", helix.G().N(helix.NodeIDs(1, 2)).ID()).
				VarAs("node_var", helix.G().N(helix.NodeVar("all_nodes")).Label()).
				VarAs("edge_ids", helix.G().E(helix.EdgeIDs(1, 2)).ID()).
				VarAs("edge_var", helix.G().E(helix.EdgeVar("edge_ids")).Label()).
				Returning("all_nodes", "node_ids", "node_var", "edge_ids", "edge_var"),
		),
		jsonOnly(
			"905-empty-traversal-source-mutators",
			write().
				VarAs("inject", helix.G().Inject("some_var").Count()).
				VarAs("drop_edge_by_id", helix.G().DropEdgeByID(helix.EdgeID(123_456)).Count()).
				Returning("inject", "drop_edge_by_id"),
		),
		fixtureNestedQueryPropertyWriteShapes(),
		fixtureNestedQueryPropertyReadShapes(),
		jsonOnly(
			"908-edge-endpoint-projection",
			read().VarAs("endpoints", helix.G().EWithLabel("FOLLOWS").Project(
				helix.ProjectFromEndpoint("externalId", "from_id"),
				helix.ProjectToEndpoint("externalId", "to_id"),
				helix.ProjectPropAs("$id", "edge_id"),
			)).Returning("endpoints"),
		),
		jsonOnly(
			"909-row-binding-basic-projection",
			read().VarAs("bindings", helix.G().NWithLabel("ParityService").Bind("service").ProjectBindings(
				helix.ProjectNamedBinding("service", "$id", "service_id"),
				helix.ProjectCurrentBinding("metadata.name", "current_name"),
				helix.ProjectNamedBinding("missing_binding", "externalId", "missing_external_id"),
			)).Returning("bindings"),
		),
		jsonOnly(
			"910-row-binding-branch-distinct-projection",
			read().VarAs("workloads", helix.G().NWithLabel("ParityService").Bind("service").Out("ROUTES_TO").Bind("pod").Optional(helix.Sub().In("CREATES").Bind("deployment")).Union(
				helix.Sub().In("MANAGES").Bind("owner"),
				helix.Sub().Out("ROUTES_TO").Bind("workload"),
			).ProjectDistinctBindings(
				helix.ProjectNamedBinding("service", "$id", "service_id"),
				helix.ProjectBindingCoalesce([]helix.BindingValueRef{
					helix.NamedBindingValue("deployment", "$id"),
					helix.NamedBindingValue("owner", "$id"),
					helix.NamedBindingValue("workload", "$id"),
				}, "workload_id"),
			)).Returning("workloads"),
		),
		jsonOnly(
			"911-range-index-direction",
			write().
				VarAs("node_desc", helix.G().CreateIndexIfNotExists(helix.NodeRangeDescIndex("ParityUser", "age"))).
				VarAs("edge_desc", helix.G().CreateIndexIfNotExists(helix.EdgeRangeDescIndex("FOLLOWS", "weight"))).
				VarAs("node_asc", helix.G().CreateIndexIfNotExists(helix.NodeRangeIndex("ParityUser", "score"))).
				Returning("node_desc", "edge_desc", "node_asc"),
		),
		fixtureShortestPathTerminal(),
		fixtureRemainingReadContract(),
		fixtureRemainingWriteContract(),
		fixtureNeo4jMigrationMovieDetails(),
		fixtureNeo4jMigrationFilmography(),
	}
}

func fixtureNeo4jMigrationMovieDetails() fixture {
	q := helix.ReadQuery("movie_details")
	movieID := q.ParamString("movie_id", "m-matrix")
	return jsonOnly(
		"915-neo4j-migration-movie-details",
		q.
			VarAs("movie_node", helix.G().NWithLabel("Movie").Where(helix.PredEq("movieId", movieID))).
			VarAs("movie", helix.G().N(helix.NodeVar("movie_node")).Project(
				helix.ProjectPropAs("movieId", "movieId"),
				helix.ProjectPropAs("title", "title"),
				helix.ProjectPropAs("released", "released"),
				helix.ProjectPropAs("tagline", "tagline"),
			)).
			VarAs("cast", helix.G().N(helix.NodeVar("movie_node")).InE("ACTED_IN").Project(
				helix.ProjectFromEndpoint("personId", "personId"),
				helix.ProjectFromEndpoint("name", "name"),
				helix.ProjectFromEndpoint("born", "born"),
				helix.ProjectPropAs("roles", "roles"),
			)).
			VarAs("directors", helix.G().N(helix.NodeVar("movie_node")).InE("DIRECTED").Project(
				helix.ProjectFromEndpoint("personId", "personId"),
				helix.ProjectFromEndpoint("name", "name"),
				helix.ProjectFromEndpoint("born", "born"),
			)).
			VarAs("producers", helix.G().N(helix.NodeVar("movie_node")).InE("PRODUCED").Project(
				helix.ProjectFromEndpoint("personId", "personId"),
				helix.ProjectFromEndpoint("name", "name"),
				helix.ProjectFromEndpoint("born", "born"),
			)).
			Returning("movie", "cast", "directors", "producers"),
	)
}

func fixtureNeo4jMigrationFilmography() fixture {
	q := helix.ReadQuery("person_filmography")
	personID := q.ParamString("person_id", "p-keanu")
	return jsonOnly(
		"916-neo4j-migration-person-filmography",
		q.
			VarAs("person_node", helix.G().NWithLabel("Person").Where(helix.PredEq("personId", personID))).
			VarAs("person", helix.G().N(helix.NodeVar("person_node")).Project(
				helix.ProjectPropAs("personId", "personId"),
				helix.ProjectPropAs("name", "name"),
				helix.ProjectPropAs("born", "born"),
			)).
			VarAs("movies", helix.G().N(helix.NodeVar("person_node")).OutE("ACTED_IN").Bind("credit").OutN().Bind("movie").ProjectBindings(
				helix.ProjectNamedBinding("movie", "movieId", "movieId"),
				helix.ProjectNamedBinding("movie", "title", "title"),
				helix.ProjectNamedBinding("movie", "released", "released"),
				helix.ProjectNamedBinding("movie", "tagline", "tagline"),
				helix.ProjectNamedBinding("credit", "roles", "roles"),
			)).
			Returning("person", "movies"),
	)
}

func fixtureRawReadSteps() fixture {
	q := read()
	q.ParamArray("node_ids", []int64{1, 2}, helix.ParamTypeI64())
	q.ParamArray("edge_ids", []int64{1}, helix.ParamTypeI64())
	q.ParamString("needle", "graph")
	q.ParamI64("limit", int64(10))
	q.ParamI64("skip", int64(0))
	q.ParamI64("end", int64(10))
	return jsonOnly(
		"900-exhaustive-raw-read-steps",
		q.
			VarAs("raw_nodes", helix.G().N(helix.NodeParam("node_ids")).Has("name", "Alice").Where(helix.PredContainsExpr("bio", helix.ExprParam("needle"))).Limit(helix.ExprParam("limit")).Skip(helix.ExprParam("skip")).Range(helix.BoundLiteral(0), helix.BoundExpr(helix.ExprParam("end"))).As("a").Store("stored").Select("stored").Dedup().Within("stored").Without("missing").Fold().Unfold().Path().SimplePath().WithSack(int64(0)).SackSet("score").SackAdd("score").SackGet().Project(
				helix.ProjectPropAs("externalId", "externalId"),
				helix.ProjectExpr("neg_age", helix.ExprProp("age").Neg()),
			)).
			VarAs("raw_edges", helix.G().E(helix.EdgeParam("edge_ids")).Where(helix.PredOr(helix.PredHasKey("since"), helix.PredStartsWith("note", "Alice"))).EdgeHas("weight", helix.F64(1.0)).EdgeHasLabel("FOLLOWS").OrderBy("weight", helix.OrderDesc).EdgeProperties()).
			VarAs("index_operation", helix.G().GetIndexOperation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001")).
			Returning("raw_nodes", "raw_edges", "index_operation"),
	)
}

func fixtureRawWriteSteps() fixture {
	return jsonOnly(
		"901-exhaustive-raw-write-steps",
		write().
			VarAs("raw_unique_index", helix.G().CreateIndexIfNotExists(helix.NodeUniqueEqualityIndex("ParityUser", "externalId"))).
			VarAs("raw_drop_range_index", helix.G().DropIndex(helix.NodeRangeIndex("ParityUser", "age"))).
			VarAs("raw_node_vector_index", helix.G().CreateVectorIndexNodes("ParityUser", "embedding", 3, helix.VectorDistanceCosine, "tenantId")).
			VarAs("raw_edge_vector_index", helix.G().CreateVectorIndexEdges("FOLLOWS", "embedding", 2, helix.VectorDistanceCosine, "tenantId")).
			VarAs("raw_node_text_index", helix.G().CreateTextIndexNodes("ParityUser", "bio", "tenantId")).
			VarAs("raw_edge_text_index", helix.G().CreateTextIndexEdges("FOLLOWS", "note", "tenantId")).
			VarAs("raw_mutations", helix.G().AddN("RawNode", helix.Props{helix.Prop("name", "raw")}).AddE("RAW_EDGE", helix.NodeVar("raw_mutations"), helix.Props{helix.Prop("weight", int64(1))}).SetProperty("name", helix.ExprParam("name")).RemoveProperty("old").DropEdge(helix.NodeID(999_999)).DropEdgeLabeled(helix.NodeID(999_999), "RAW_EDGE").DropEdgeByID(helix.EdgeID(999_999)).Drop()).
			VarAs("retry_index_operation", helix.G().RetryIndexOperation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001")).
			VarAs("abort_index_operation", helix.G().AbortIndexOperation("018f0c58-6bc7-7c56-8d3d-9c5f18a0f001")).
			Returning("raw_unique_index", "raw_drop_range_index", "raw_node_vector_index", "raw_edge_vector_index", "raw_node_text_index", "raw_edge_text_index", "raw_mutations", "retry_index_operation", "abort_index_operation"),
	)
}

func fixtureQueryValueShapes() fixture {
	q := read()
	q.ParamValue("null", nil)
	q.ParamBool("bool", true)
	q.ParamI64("i64", int64(9_223_372_036_854_775_807))
	q.ParamF64("f64", 1.25)
	q.ParamF32("f32", float32(1.5))
	q.ParamString("string", "value")
	q.ParamArray("array", []any{int64(1), "two"}, helix.ParamTypeValue())
	q.ParamObject("object", map[string]any{"nested": true})
	return jsonOnly(
		"902-query-value-and-param-type-shapes",
		q.VarAs("empty", helix.G().NWithLabel("Missing").Count()).Returning("empty"),
	)
}

func fixtureEmptySourceVectorTextRuntimeInputs() fixture {
	q := read()
	queryVector := q.ParamArray("query_vector", []float64{1.0, 0.0, 0.0}, helix.ParamTypeF64())
	queryText := q.ParamString("query_text", "graph")
	limit := q.ParamI64("limit", int64(5))
	tenant := q.ParamString("tenant", "tenant-a")
	return jsonOnly(
		"903-empty-source-vector-text-runtime-inputs",
		q.
			VarAs("vector_nodes", helix.G().VectorSearchNodesWith("ParityUser", "embedding", queryVector.Input(), limit.Bound(), inputPtr(tenant.Input()))).
			VarAs("text_nodes", helix.G().TextSearchNodesWith("ParityUser", "bio", queryText.Input(), limit.Bound(), inputPtr(tenant.Input()))).
			Returning("vector_nodes", "text_nodes"),
	)
}

func fixtureNestedQueryPropertyWriteShapes() fixture {
	q := write()
	metadata := q.ParamObject("metadata", nestedMetadataParam("param_id", 22))
	return jsonOnly(
		"906-nested-query-property-write-shapes",
		q.
			VarAs("created", helix.G().AddN("ParityNested", helix.Props{
				helix.Prop("name", "nested"),
				helix.Prop("metadata", nestedMetadataProperty("some_id", 20)),
			})).
			VarAs("updated", helix.G().N(helix.NodeVar("created")).SetProperty("metadata", metadata).ValueMap("metadata.externalID")).
			VarAs("target", helix.G().AddN("ParityNestedTarget", helix.Props{helix.Prop("name", "target")})).
			VarAs("edge", helix.G().N(helix.NodeVar("created")).AddE("NESTED_LINK", helix.NodeVar("target"), helix.Props{helix.Prop("metadata", nestedMetadataProperty("edge_id", 5))}).Count()).
			Returning("created", "updated", "edge"),
	)
}

func fixtureNestedQueryPropertyReadShapes() fixture {
	q := read()
	externalID := q.ParamString("external_id", "param_id")
	return jsonOnly(
		"907-nested-query-property-read-shapes",
		q.
			VarAs("nested_users", helix.G().NWhere(helix.SourceAnd(helix.SourceEq("$label", "ParityNested"), helix.SourceEq("metadata.externalID", externalID))).Where(helix.PredCompare(helix.ExprProp("metadata.score"), helix.CompareGt, helix.ExprVal(int64(10)))).OrderByMultiple(
				helix.Ordering{Property: "metadata.score", Order: helix.OrderDesc},
				helix.Ordering{Property: "name", Order: helix.OrderAsc},
			).Project(
				helix.ProjectPropAs("metadata.externalID", "external_id"),
				helix.ProjectExpr("score_copy", helix.ExprProp("metadata.score")),
			)).
			VarAs("nested_values", helix.G().NWithLabel("ParityNested").Values("metadata.externalID")).
			VarAs("nested_map", helix.G().NWithLabel("ParityNested").ValueMap("metadata.externalID", "metadata.score")).
			VarAs("nested_edges", helix.G().EWhere(helix.SourceAnd(helix.SourceEq("$label", "NESTED_LINK"), helix.SourceEq("metadata.externalID", "edge_id"))).EdgeHas("metadata.externalID", "edge_id").EdgeProperties()).
			Returning("nested_users", "nested_values", "nested_map", "nested_edges"),
	)
}

func fixtureShortestPathTerminal() fixture {
	q := read()
	q.ParamI64("target", int64(3))
	return jsonOnly(
		"912-shortest-path-terminal",
		q.VarAs("path", helix.G().ShortestPath(helix.NodeID(1), helix.NodeParam("target"), 5, helix.ShortestPathOptions{
			Label:     "FOLLOWS",
			Direction: helix.ShortestPathBoth,
		})).Returning("path"),
	)
}

func fixtureRemainingReadContract() fixture {
	comparisons := helix.PredAnd(
		helix.PredNeq("neq", int64(1)),
		helix.PredGt("gt", int64(1)),
		helix.PredGte("gte", int64(1)),
		helix.PredLt("lt", int64(1)),
		helix.PredLte("lte", int64(1)),
		helix.PredBetween("between", int64(1), int64(3)),
		helix.PredEndsWith("suffix", "end"),
		helix.PredIsIn("status", []string{"active", "inactive"}),
		helix.PredIsNull("missing"),
		helix.PredIsNotNull("present"),
		helix.PredNot(helix.PredEq("disabled", true)),
		helix.PredCompare(helix.ExprID(), helix.CompareEq, helix.ExprVal(int64(1))),
		helix.PredCompare(helix.ExprID(), helix.CompareNeq, helix.ExprVal(int64(1))),
		helix.PredCompare(helix.ExprID(), helix.CompareGt, helix.ExprVal(int64(1))),
		helix.PredCompare(helix.ExprID(), helix.CompareGte, helix.ExprVal(int64(1))),
		helix.PredCompare(helix.ExprID(), helix.CompareLt, helix.ExprVal(int64(1))),
		helix.PredCompare(helix.ExprID(), helix.CompareLte, helix.ExprVal(int64(1))),
	)
	q := read().
		WithTypedParameter(
			"date_time",
			helix.ParamTypeDateTime(),
			helix.QueryString("2026-01-01T00:00:00.000Z"),
		).
		VarAs("expressions_and_predicates", helix.G().N(helix.AllNodes()).Where(comparisons).Project(
			helix.ProjectExpr("id", helix.ExprID()),
			helix.ProjectExpr("timestamp", helix.ExprTimestamp()),
			helix.ProjectExpr("datetime", helix.ExprDateTime()),
			helix.ProjectExpr("null", helix.ExprVal(helix.Null())),
			helix.ProjectExpr("date_value", helix.ExprVal(helix.DateTimeMillis(1_777_000_000_000))),
			helix.ProjectExpr("f32", helix.ExprVal(helix.F32(1.25))),
			helix.ProjectExpr("bytes", helix.ExprVal(helix.Bytes([]byte{1, 2, 3}))),
			helix.ProjectExpr("i64_array", helix.ExprVal(helix.I64Array(1, 2, 3))),
			helix.ProjectExpr("f64_array", helix.ExprVal(helix.F64Array(1.25, 2.5))),
			helix.ProjectExpr("add", helix.ExprVal(int64(4)).Add(helix.ExprVal(int64(1)))),
			helix.ProjectExpr("sub", helix.ExprVal(int64(4)).Sub(helix.ExprVal(int64(1)))),
			helix.ProjectExpr("mul", helix.ExprVal(int64(4)).Mul(helix.ExprVal(int64(2)))),
			helix.ProjectExpr("div", helix.ExprVal(int64(4)).Div(helix.ExprVal(int64(2)))),
			helix.ProjectExpr("mod", helix.ExprVal(int64(5)).Mod(helix.ExprVal(int64(2)))),
			helix.ProjectExpr("case", helix.ExprCase(
				[]helix.WhenThen{{When: helix.PredEq("status", "active"), Then: helix.ExprVal("enabled")}},
				exprPtr(helix.ExprVal("disabled")),
			)),
		)).
		VarAs("both", helix.G().N(helix.NodeID(1)).Both().Count()).
		VarAs("in_e", helix.G().N(helix.NodeID(1)).InE().EdgeProperties()).
		VarAs("out_e", helix.G().N(helix.NodeID(1)).OutE().EdgeProperties()).
		VarAs("both_e", helix.G().N(helix.NodeID(1)).BothE().EdgeProperties()).
		VarAs("in_n", helix.G().E(helix.AllEdges()).InN().ValueMapAll()).
		VarAs("out_n", helix.G().E(helix.AllEdges()).OutN().ValueMapAll()).
		VarAs("other_n", helix.G().E(helix.AllEdges()).OtherN().ValueMapAll()).
		VarAs("direct_has_key", helix.G().N(helix.AllNodes()).HasKey("externalId").Count()).
		VarAs("has_label", helix.G().N(helix.AllNodes()).HasLabel("ParityUser").Count()).
		VarAs("exists", helix.G().N(helix.AllNodes()).Exists()).
		VarAs("choose", helix.G().N(helix.AllNodes()).Choose(helix.PredIsNotNull("status"), helix.Sub().Out(), helix.Sub().In()).Count()).
		VarAs("coalesce", helix.G().N(helix.AllNodes()).Coalesce(helix.Sub().Out(), helix.Sub().In()).Count()).
		VarAs("group", helix.G().N(helix.AllNodes()).Group("status")).
		VarAs("group_count", helix.G().N(helix.AllNodes()).GroupCount("status")).
		VarAs("aggregate_count", helix.G().N(helix.AllNodes()).AggregateBy(helix.AggregateCount, "age")).
		VarAs("aggregate_sum", helix.G().N(helix.AllNodes()).AggregateBy(helix.AggregateSum, "age")).
		VarAs("aggregate_min", helix.G().N(helix.AllNodes()).AggregateBy(helix.AggregateMin, "age")).
		VarAs("aggregate_max", helix.G().N(helix.AllNodes()).AggregateBy(helix.AggregateMax, "age")).
		VarAs("aggregate_mean", helix.G().N(helix.AllNodes()).AggregateBy(helix.AggregateMean, "age")).
		VarAs("repeat_none", helix.G().N(helix.NodeID(1)).Repeat(helix.Repeat(helix.Sub().Out())).Count()).
		VarAs("repeat_before", helix.G().N(helix.NodeID(1)).Repeat(helix.Repeat(helix.Sub().Out()).EmitBefore()).Count()).
		VarAs("repeat_after", helix.G().N(helix.NodeID(1)).Repeat(helix.Repeat(helix.Sub().Out()).EmitAfter()).Count()).
		VarAs("repeat_all", helix.G().N(helix.NodeID(1)).Repeat(helix.Repeat(helix.Sub().Out()).EmitAll()).Count()).
		VarAs("shortest_out", helix.G().ShortestPath(helix.NodeID(1), helix.NodeID(2), 5, helix.ShortestPathOptions{Direction: helix.ShortestPathOut})).
		VarAs("shortest_in", helix.G().ShortestPath(helix.NodeID(1), helix.NodeID(2), 5, helix.ShortestPathOptions{Direction: helix.ShortestPathIn})).
		VarAs("vector_edges", helix.G().VectorSearchEdges("FOLLOWS", "embedding", []float32{1, 0}, 5).EdgeProperties()).
		VarAs("vector_nodes_within", helix.G().NWithLabel("ParityUser").VectorSearchNodesWithin("ParityUser", "embedding", []float32{1, 0, 0}, 5)).
		VarAs("vector_edges_within", helix.G().E(helix.AllEdges()).HasLabel("FOLLOWS").VectorSearchEdgesWithin("FOLLOWS", "embedding", []float32{1, 0}, 5)).
		VarAs("text_edges", helix.G().TextSearchEdges("FOLLOWS", "note", "graph", 5).EdgeProperties()).
		VarAsIf("previous", helix.PrevNotEmpty(), helix.G().N(helix.AllNodes()).Count()).
		VarAsIf("not_empty", helix.VarNotEmpty("expressions_and_predicates"), helix.G().N(helix.AllNodes()).Count()).
		VarAsIf("empty", helix.VarEmpty("missing"), helix.G().N(helix.AllNodes()).Count()).
		VarAsIf("min_size", helix.VarMinSize("expressions_and_predicates", 1), helix.G().N(helix.AllNodes()).Count()).
		ForEachParam("rows", helix.Read().VarAs("foreach", helix.G().N(helix.AllNodes()).Count())).
		Returning(
			"expressions_and_predicates", "both", "in_e", "out_e", "both_e", "in_n", "out_n", "other_n",
			"direct_has_key", "has_label", "exists", "choose", "coalesce", "group", "group_count",
			"aggregate_count", "aggregate_sum", "aggregate_min", "aggregate_max", "aggregate_mean",
			"repeat_none", "repeat_before", "repeat_after", "repeat_all",
			"shortest_out", "shortest_in", "vector_edges", "vector_nodes_within", "vector_edges_within", "text_edges", "previous", "not_empty", "empty", "min_size", "foreach",
		)
	return jsonOnly("913-remaining-read-contract", q)
}

func fixtureRemainingWriteContract() fixture {
	return jsonOnly(
		"914-remaining-write-contract",
		write().
			VarAs("edge_equality", helix.G().CreateIndexIfNotExists(helix.EdgeEqualityIndex("FOLLOWS", "since"))).
			VarAs("node_euclidean", helix.G().CreateIndexIfNotExists(helix.NodeVectorIndex("ParityUser", "euclidean_embedding", 4, helix.VectorDistanceEuclidean))).
			VarAs("edge_manhattan", helix.G().CreateIndexIfNotExists(helix.EdgeVectorIndex("FOLLOWS", "manhattan_embedding", 4, helix.VectorDistanceManhattan))).
			Returning("edge_equality", "node_euclidean", "edge_manhattan"),
	)
}
