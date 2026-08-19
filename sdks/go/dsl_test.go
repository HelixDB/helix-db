package helix

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type findUsersResponse struct {
	Users []struct {
		ID   json.Number `json:"$id"`
		Name string      `json:"name"`
	} `json:"users"`
}

func findUsers(tenantID string, limit int64) Request {
	q := ReadQuery("find_users")
	tenant := q.ParamString("tenant_id", tenantID)
	maxRows := q.ParamI64("limit", limit)
	return q.VarAs("users", G().NWithLabel("User").Where(PredEq("tenantId", tenant)).Limit(maxRows).ValueMap("$id", "name", "tenantId")).Returning("users")
}

func TestQueryRequestJSON(t *testing.T) {
	body, err := MarshalRequest(findUsers("acme", 25))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	var query map[string]json.RawMessage
	if err := json.Unmarshal(payload["query"], &query); err != nil {
		t.Fatal(err)
	}
	if _, ok := query["read"]; !ok {
		t.Fatalf("read request should tag query payload as read: %s", body)
	}
	if _, ok := query["write"]; ok {
		t.Fatalf("read request should not include write payload: %s", body)
	}
	jsonText := string(body)
	for _, want := range []string{`"request_type":"read"`, `"query_name":"find_users"`, `"tenant_id":"acme"`, `"limit":25`, `"parameter_types":{"limit":"i64","tenant_id":"string"}`} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("request JSON missing %s in %s", want, jsonText)
		}
	}

	writeBody, err := MarshalRequest(
		WriteQuery("create_user").
			VarAs("created", G().AddN("User", Props{Prop("name", "Alice")})).
			Returning("created"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(writeBody, &payload); err != nil {
		t.Fatal(err)
	}
	query = map[string]json.RawMessage{}
	if err := json.Unmarshal(payload["query"], &query); err != nil {
		t.Fatal(err)
	}
	if _, ok := query["write"]; !ok {
		t.Fatalf("write request should tag query payload as write: %s", writeBody)
	}
	if _, ok := query["read"]; ok {
		t.Fatalf("write request should not include read payload: %s", writeBody)
	}
}

func TestTraversalScopedVectorSearchJSON(t *testing.T) {
	body, err := json.Marshal(Read().VarAs(
		"hits",
		G().NWithLabel("Doc").VectorSearchNodesWithin("Doc", "embedding", []float32{1, 0, 0}, 5),
	))
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, want := range []string{
		`"vector_search_nodes_within"`,
		`"input":{"nodes_where"`,
		`"query_vector":{"value":{"f32_array":[1,0,0]}}`,
		`"k":{"literal":5}`,
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("restricted vector JSON missing %s in %s", want, jsonText)
		}
	}
}

func TestTraversalScopedTextSearchJSON(t *testing.T) {
	tenant := ParamInput("tenant")
	body, err := json.Marshal(Read().
		VarAs("nodes", G().NWithLabel("Doc").TextSearchNodesWithin("Doc", "body", "graph", 5)).
		VarAs("edges", G().E(AllEdges()).TextSearchEdgesWithinWith("MENTIONS", "body", ParamInput("query"), BoundExpr(ExprParam("limit")), &tenant)))
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, want := range []string{
		`"text_search_nodes_within"`,
		`"input":{"nodes_where"`,
		`"query_text":{"value":{"string":"graph"}}`,
		`"text_search_edges_within"`,
		`"query_text":{"expr":{"param":"query"}}`,
		`"k":{"expr":{"param":"limit"}}`,
		`"tenant_value":{"expr":{"param":"tenant"}}`,
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("restricted text JSON missing %s in %s", want, jsonText)
		}
	}
}

func TestVectorIndexSpecRequiresDimensionAndMetric(t *testing.T) {
	spec := NodeVectorIndex("Doc", "embedding", 3, VectorDistanceCosine, "tenant_id")
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"node_vector":{"dimension":3,"label":"Doc","metric":"cosine","property":"embedding","tenant_property":"tenant_id"}}`
	if string(body) != want {
		t.Fatalf("unexpected vector index JSON:\nwant %s\n got %s", want, body)
	}

	for name, build := range map[string]func(){
		"zero dimension": func() { NodeVectorIndex("Doc", "embedding", 0, VectorDistanceCosine) },
		"invalid metric": func() { NodeVectorIndex("Doc", "embedding", 3, VectorDistanceMetric("invalid")) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected vector index construction to panic")
				}
			}()
			build()
		})
	}
}

func TestEdgeEndpointProjectionJSON(t *testing.T) {
	req := ReadQuery("list_relationships_by_type").
		VarAs("relationships", G().EWithLabel("DESCRIBES").Project(
			ProjectFromEndpoint("resource_id", "from_id"),
			ProjectToEndpoint("resource_id", "to_id"),
			ProjectPropAs("$id", "edge_id"),
		)).
		Returning("relationships")
	body, err := MarshalRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, want := range []string{
		`"source":"$from.resource_id","alias":"from_id"`,
		`"source":"$to.resource_id","alias":"to_id"`,
		`"source":"$id","alias":"edge_id"`,
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("request JSON missing %s in %s", want, jsonText)
		}
	}
}

func TestRowBindingProjectionJSON(t *testing.T) {
	req := ReadQuery("service_workloads").
		VarAs("workloads", G().NWithLabel("Service").Bind("service").Out("ROUTES_TO").Bind("pod").Optional(Sub().In("CREATES").Bind("deployment")).Union(
			Sub().In("MANAGES").Bind("owner"),
			Sub().Out("ROUTES_TO").Bind("workload"),
		).ProjectDistinctBindings(
			ProjectNamedBinding("service", "$id", "service_id"),
			ProjectCurrentBinding("$id", "current_id"),
			ProjectNamedBinding("missing_binding", "externalId", "missing_external_id"),
			ProjectBindingCoalesce([]BindingValueRef{
				NamedBindingValue("deployment", "$id"),
				NamedBindingValue("owner", "$id"),
				NamedBindingValue("workload", "$id"),
			}, "workload_id"),
		)).
		Returning("workloads")
	body, err := MarshalRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, want := range []string{
		`"bind":`,
		`"name":"service"`,
		`"name":"deployment"`,
		`"project_bindings":`,
		`"binding":"service"`,
		`"target":"current"`,
		`"coalesce":`,
		`"distinct":true`,
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("request JSON missing %s in %s", want, jsonText)
		}
	}
}

func TestShortestPathJSON(t *testing.T) {
	req := ReadQuery("path").
		VarAs("path", G().ShortestPath(NodeID(1), NodeParam("target"), 5, ShortestPathOptions{
			Label:     "FOLLOWS",
			Direction: ShortestPathBoth,
		})).
		Returning("path")
	body, err := MarshalRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, want := range []string{
		`"shortest_path":`,
		`"source":{"ids":[1]}`,
		`"target":{"param":"target"}`,
		`"label":"FOLLOWS"`,
		`"direction":"both"`,
		`"max_depth":5`,
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("request JSON missing %s in %s", want, jsonText)
		}
	}
}

func TestBindRejectsEmptyName(t *testing.T) {
	if err := G().NWithLabel("Service").Bind("").ProjectBindings(ProjectCurrentBinding("$id", "id")).Validate(); err == nil {
		t.Fatal("expected empty binding name to fail validation")
	}
}

func TestReadQueryRejectsWriteTraversal(t *testing.T) {
	req := ReadQuery("bad").VarAs("created", G().AddN("User", Props{Prop("name", "Alice")})).Returning("created")
	if err := req.Validate(); err == nil {
		t.Fatal("expected read query to reject write traversal")
	}
}

func TestReturningEmptySerializesSequence(t *testing.T) {
	req := ReadQuery("warm_users").VarAs("users", G().NWithLabel("User").Count()).Returning()
	body, err := MarshalRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	if !strings.Contains(jsonText, `"returns":[]`) {
		t.Fatalf("request JSON should serialize empty returns as []: %s", jsonText)
	}
	if strings.Contains(jsonText, `"returns":null`) {
		t.Fatalf("request JSON should not serialize empty returns as null: %s", jsonText)
	}
}

func TestRangeIndexDirectionJSON(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec IndexSpec
		want string
	}{
		{
			name: "node asc",
			spec: NodeRangeIndex("User", "age"),
			want: `{"node_range":{"direction":"asc","label":"User","property":"age"}}`,
		},
		{
			name: "node explicit asc",
			spec: NodeRangeIndexWithDirection("User", "age", RangeIndexAsc),
			want: `{"node_range":{"direction":"asc","label":"User","property":"age"}}`,
		},
		{
			name: "node desc",
			spec: NodeRangeDescIndex("User", "age"),
			want: `{"node_range":{"direction":"desc","label":"User","property":"age"}}`,
		},
		{
			name: "edge desc",
			spec: EdgeRangeDescIndex("FOLLOWS", "weight"),
			want: `{"edge_range":{"direction":"desc","label":"FOLLOWS","property":"weight"}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != tc.want {
				t.Fatalf("unexpected JSON: %s", body)
			}
		})
	}
}

func TestPublicQueryRequestSurface(t *testing.T) {
	emptyBatchJSON, err := json.Marshal(Read())
	if err != nil {
		t.Fatal(err)
	}
	if string(emptyBatchJSON) != `{"entries":[],"returns":[]}` {
		t.Fatalf("empty batch must use canonical empty arrays: %s", emptyBatchJSON)
	}
	elseExpr := ExprVal("disabled")
	batch := Read().
		VarAs("users", G().N(AllNodes()).Project(
			ProjectExpr("status", ExprCase(
				[]WhenThen{{When: PredEq("active", true), Then: ExprVal("enabled")}},
				&elseExpr,
			)),
		)).
		Returning("users")
	request := NewReadQueryRequest(batch).
		WithQueryName("read_users").
		WithTypedParameter("tenant", ParamTypeString(), QueryString("acme"))
	if request.RequestType() != RequestTypeRead {
		t.Fatalf("unexpected request type %q", request.RequestType())
	}
	request.ClearQueryName()
	request.SetQueryName("read_users")
	body, err := MarshalRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"request_type":"read"`,
		`"query_name":"read_users"`,
		`"query":{"read":`,
		`"case":{"else_expr":{"constant":{"string":"disabled"}}`,
	} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("request JSON missing %s in %s", want, body)
		}
	}
	batchJSON, err := json.Marshal(ReadBatchQuery(batch))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(batchJSON), `{"read":`) {
		t.Fatalf("batch query should use the canonical read tag: %s", batchJSON)
	}
	writeRequest := NewWriteQueryRequest(Write().Returning()).WithQueryName("write_empty")
	writeJSON, err := MarshalRequest(writeRequest)
	if err != nil {
		t.Fatal(err)
	}
	if writeRequest.RequestType() != RequestTypeWrite || !strings.Contains(string(writeJSON), `"query":{"write":`) {
		t.Fatalf("write request should preserve its typed batch variant: %s", writeJSON)
	}
	if _, err := json.Marshal(ReadBatchQuery(nil)); err == nil {
		t.Fatal("expected nil read batch to fail")
	}
	if _, err := json.Marshal(WriteBatchQuery(nil)); err == nil {
		t.Fatal("expected nil write batch to fail")
	}
	if _, err := json.Marshal(BatchCondition{}); err == nil {
		t.Fatal("expected zero-value batch condition to fail")
	}
	for _, input := range []string{
		`"unknown"`,
		`{}`,
		`{"var_min_size":["users"]}`,
		`{"var_min_size":["users",-1]}`,
	} {
		var condition BatchCondition
		if err := json.Unmarshal([]byte(input), &condition); err == nil {
			t.Fatalf("expected malformed batch condition to fail: %s", input)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected negative minimum size construction to panic")
		}
	}()
	VarMinSize("users", -1)
}

func TestQueryParamTypesCoverEveryValidAndInvalidState(t *testing.T) {
	valid := []QueryParamType{
		{},
		ParamTypeBool(),
		ParamTypeI64(),
		ParamTypeF64(),
		ParamTypeF32(),
		ParamTypeString(),
		ParamTypeDateTime(),
		ParamTypeBytes(),
		ParamTypeValue(),
		ParamTypeObject(),
		ParamTypeArray(ParamTypeArray(ParamTypeString())),
	}
	for _, parameterType := range valid {
		body, err := json.Marshal(parameterType)
		if err != nil {
			t.Fatalf("valid parameter type %#v failed to marshal: %v", parameterType, err)
		}
		var decoded QueryParamType
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("valid parameter type %s failed to unmarshal: %v", body, err)
		}
		if err := decoded.Validate(); err != nil {
			t.Fatalf("round-tripped parameter type %#v is invalid: %v", decoded, err)
		}
		reencoded, err := json.Marshal(decoded)
		if err != nil {
			t.Fatalf("round-tripped parameter type %#v failed to marshal: %v", decoded, err)
		}
		if string(reencoded) != string(body) {
			t.Fatalf("parameter type changed across round trip: before=%s after=%s", body, reencoded)
		}
	}

	scalarInner := ParamTypeString()
	invalid := []QueryParamType{
		{kind: paramKindBool, inner: &scalarInner},
		{kind: paramKindArray},
		{kind: ParamKind(255)},
		ParamTypeArray(QueryParamType{kind: ParamKind(255)}),
	}
	for _, parameterType := range invalid {
		if err := parameterType.Validate(); err == nil {
			t.Fatalf("invalid parameter type unexpectedly validated: %#v", parameterType)
		}
		if _, err := json.Marshal(parameterType); err == nil {
			t.Fatalf("invalid parameter type unexpectedly marshaled: %#v", parameterType)
		}
	}

	for _, input := range []string{
		`"future"`,
		`{}`,
		`{"array":null}`,
		`{"array":"future"}`,
		`{"future":"i64"}`,
		`[]`,
		`{`,
	} {
		var parameterType QueryParamType
		if err := json.Unmarshal([]byte(input), &parameterType); err == nil {
			t.Fatalf("malformed parameter type unexpectedly decoded: %s", input)
		}
	}
}

func TestAtomicTypedAndExplicitUntypedParameterStates(t *testing.T) {
	request := NewReadQueryRequest(Read())
	valid := []struct {
		name  string
		kind  QueryParamType
		value QueryValue
	}{
		{"flag", ParamTypeBool(), QueryBool(true)},
		{"count", ParamTypeI64(), QueryI64(3)},
		{"f64", ParamTypeF64(), QueryF64(1.25)},
		{"f32", ParamTypeF32(), QueryF64(1.1)},
		{"text", ParamTypeString(), QueryString("x")},
		{"when", ParamTypeDateTime(), QueryString("2026-07-28T12:34:56Z")},
		{"value", ParamTypeValue(), QueryArray(QueryBool(true), QueryI64(1))},
		{"object", ParamTypeObject(), QueryObject(map[string]QueryValue{"ok": QueryBool(true)})},
		{"array", ParamTypeArray(ParamTypeBool()), QueryArray(QueryBool(true), QueryBool(false))},
	}
	for _, test := range valid {
		if err := request.InsertTypedParameter(test.name, test.kind, test.value); err != nil {
			t.Fatalf("valid typed parameter %s failed: %v", test.name, err)
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`"flag":true`,
		`"f32":1.1`,
		`"when":"2026-07-28T12:34:56.000Z"`,
		`"array":{"array":"bool"}`,
	} {
		if !strings.Contains(string(body), expected) {
			t.Fatalf("typed request missing %s: %s", expected, body)
		}
	}

	invalid := []struct {
		name  string
		kind  QueryParamType
		value QueryValue
	}{
		{"bool", ParamTypeBool(), QueryI64(1)},
		{"i64", ParamTypeI64(), QueryF64(1)},
		{"f64", ParamTypeF64(), QueryF64(math.Inf(1))},
		{"f32", ParamTypeF32(), QueryF64(math.MaxFloat64)},
		{"string", ParamTypeString(), QueryBool(true)},
		{"datetime", ParamTypeDateTime(), QueryString("28 July 2026")},
		{"bytes", ParamTypeBytes(), QueryString("AQID")},
		{"object", ParamTypeObject(), QueryArray()},
		{"array", ParamTypeArray(ParamTypeBool()), QueryArray(QueryBool(true), QueryI64(0))},
	}
	for _, test := range invalid {
		candidate := NewReadQueryRequest(Read())
		if err := candidate.InsertTypedParameter(test.name, test.kind, test.value); err == nil {
			t.Fatalf("invalid typed parameter %s unexpectedly succeeded", test.name)
		}
		if len(candidate.parameters) != 0 || len(candidate.types) != 0 {
			t.Fatalf("invalid typed parameter %s partially mutated the request", test.name)
		}
	}

	untyped := NewReadQueryRequest(Read())
	if err := untyped.InsertUntypedParameter("raw", QueryBool(true)); err != nil {
		t.Fatal(err)
	}
	if err := untyped.InsertTypedParameter("typed", ParamTypeBool(), QueryBool(true)); !errors.Is(err, ErrMixedParameterModes) {
		t.Fatalf("typed-after-untyped should fail with mixed mode: %v", err)
	}
	typed := NewReadQueryRequest(Read())
	if err := typed.InsertTypedParameter("typed", ParamTypeBool(), QueryBool(true)); err != nil {
		t.Fatal(err)
	}
	if err := typed.InsertUntypedParameter("raw", QueryBool(true)); !errors.Is(err, ErrMixedParameterModes) {
		t.Fatalf("untyped-after-typed should fail with mixed mode: %v", err)
	}
	if err := typed.InsertTypedParameter("typed", ParamTypeBool(), QueryBool(false)); !errors.Is(err, ErrDuplicateParameter) {
		t.Fatalf("duplicate typed parameter should fail: %v", err)
	}
	if err := NewReadQueryRequest(Read()).InsertTypedParameter("", ParamTypeBool(), QueryBool(true)); !errors.Is(err, ErrEmptyParameterName) {
		t.Fatalf("empty typed parameter name should fail: %v", err)
	}
}

func TestBatchDecodersCoverClosedVariantsAndMalformedShapes(t *testing.T) {
	for _, input := range []string{
		`"prev_not_empty"`,
		`{"var_not_empty":"users"}`,
		`{"var_empty":"users"}`,
		`{"var_min_size":["users",0]}`,
	} {
		var condition BatchCondition
		if err := json.Unmarshal([]byte(input), &condition); err != nil {
			t.Fatalf("valid batch condition %s failed to decode: %v", input, err)
		}
		body, err := json.Marshal(condition)
		if err != nil {
			t.Fatalf("decoded batch condition %s failed to encode: %v", input, err)
		}
		if string(body) != input {
			t.Fatalf("batch condition changed across round trip: before=%s after=%s", input, body)
		}
	}
	for _, input := range []string{
		`{"var_not_empty":"users","var_empty":"users"}`,
		`{"var_not_empty":1}`,
		`{"var_min_size":"users"}`,
		`{"var_min_size":["users","1"]}`,
		`{"future":"users"}`,
		`null`,
		`[]`,
		`{`,
	} {
		var condition BatchCondition
		if err := json.Unmarshal([]byte(input), &condition); err == nil {
			t.Fatalf("malformed batch condition unexpectedly decoded: %s", input)
		}
	}

	for _, input := range []string{
		`{"query":{"name":"users","root":{"nodes":"all"},"condition":"prev_not_empty"}}`,
		`{"for_each":{"param":"items","body":[]}}`,
	} {
		var entry BatchEntry
		if err := json.Unmarshal([]byte(input), &entry); err != nil {
			t.Fatalf("valid batch entry %s failed to decode: %v", input, err)
		}
		body, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("decoded batch entry %s failed to encode: %v", input, err)
		}
		if string(body) != input {
			t.Fatalf("batch entry changed across round trip: before=%s after=%s", input, body)
		}
	}
	for _, input := range []string{
		`{}`,
		`{"query":{},"for_each":{"param":"items","body":[]}}`,
		`{"future":{}}`,
		`{"query":{}}`,
		`{"for_each":{"param":1,"body":[]}}`,
		`[]`,
		`{`,
	} {
		var entry BatchEntry
		if err := json.Unmarshal([]byte(input), &entry); err == nil {
			t.Fatalf("malformed batch entry unexpectedly decoded: %s", input)
		}
	}

	readInput := `{"entries":[{"query":{"name":"users","root":{"nodes":"all"},"condition":"prev_not_empty"}}],"returns":[]}`
	var read ReadBatch
	if err := json.Unmarshal([]byte(readInput), &read); err != nil {
		t.Fatalf("valid read batch failed to decode: %v", err)
	}
	readBody, err := json.Marshal(&read)
	if err != nil {
		t.Fatalf("decoded read batch failed to encode: %v", err)
	}
	if string(readBody) != readInput {
		t.Fatalf("read batch changed across round trip: before=%s after=%s", readInput, readBody)
	}

	writeInput := `{"entries":[{"for_each":{"param":"items","body":[]}}],"returns":["created"]}`
	var write WriteBatch
	if err := json.Unmarshal([]byte(writeInput), &write); err != nil {
		t.Fatalf("valid write batch failed to decode: %v", err)
	}
	writeBody, err := json.Marshal(&write)
	if err != nil {
		t.Fatalf("decoded write batch failed to encode: %v", err)
	}
	if string(writeBody) != writeInput {
		t.Fatalf("write batch changed across round trip: before=%s after=%s", writeInput, writeBody)
	}

	for _, batch := range []any{&ReadBatch{}, &WriteBatch{}} {
		if err := json.Unmarshal([]byte(`{`), batch); err == nil {
			t.Fatalf("malformed batch unexpectedly decoded as %T", batch)
		}
	}
	var nilBatch *batchBase
	if err := nilBatch.Validate(); err == nil {
		t.Fatal("nil batch unexpectedly validated")
	}
	if err := (BatchQuery{}).Validate(); err == nil {
		t.Fatal("zero-value batch query unexpectedly validated")
	}
	if _, err := json.Marshal(BatchQuery{}); err == nil {
		t.Fatal("zero-value batch query unexpectedly marshaled")
	}
}

func TestClientExec(t *testing.T) {
	var capturedPath string
	var capturedAuth string
	var capturedWriter string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		capturedWriter = r.Header.Get("x-helix-require-writer")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"users":[{"$id":9223372036854775807,"name":"Alice"}]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithAPIKey("hx_secret"))
	if err != nil {
		t.Fatal(err)
	}
	var out findUsersResponse
	if err := client.Exec(context.Background(), findUsers("acme", 25), &out, WriterOnly()); err != nil {
		t.Fatal(err)
	}
	if capturedPath != "/v2/query" {
		t.Fatalf("unexpected path %s", capturedPath)
	}
	if capturedAuth != "Bearer hx_secret" || capturedWriter != "true" {
		t.Fatalf("headers not set: auth=%q writer=%q", capturedAuth, capturedWriter)
	}
	if got := out.Users[0].ID.String(); got != "9223372036854775807" {
		t.Fatalf("large id lost precision: %s", got)
	}
}

func TestClientExecConflictError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "conflict", http.StatusConflict)
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	var out findUsersResponse
	err = client.Exec(context.Background(), findUsers("acme", 25), &out)
	if err == nil {
		t.Fatal("expected conflict error")
	}
	var helixErr *HelixError
	if !errors.As(err, &helixErr) {
		t.Fatalf("expected HelixError, got %T", err)
	}
	if helixErr.Kind != ErrorRemote || helixErr.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected error kind/status: kind=%s status=%d", helixErr.Kind, helixErr.StatusCode)
	}
	if !strings.Contains(helixErr.Details, "conflict") {
		t.Fatalf("expected conflict details, got %q", helixErr.Details)
	}
	if !errors.Is(err, ErrConflict) {
		t.Fatal("expected errors.Is to detect ErrConflict")
	}
	if !IsConflict(err) {
		t.Fatal("expected IsConflict to detect HTTP 409")
	}
}

func TestClientExecParsesNewLegacyFutureMissingAndMalformedErrors(t *testing.T) {
	cases := []struct {
		body    string
		code    QueryErrorCode
		details string
	}{
		{`{"error":"index_not_found","msg":"missing index"}`, QueryErrorCode("index_not_found"), "missing index"},
		{`{"error":"legacy message","code":"index_not_found"}`, QueryErrorCode("index_not_found"), "legacy message"},
		{`{"error":"future_code","msg":"future message"}`, QueryErrorCode("future_code"), "future message"},
		{`{"error":"message without code"}`, QueryErrorCode(""), "message without code"},
		{"not JSON", QueryErrorCode(""), "not JSON"},
	}

	for _, testCase := range cases {
		t.Run(testCase.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(testCase.body))
			}))
			defer server.Close()
			client, err := NewClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}

			err = client.Exec(context.Background(), findUsers("acme", 25), nil)
			var helixErr *HelixError
			if !errors.As(err, &helixErr) {
				t.Fatalf("expected HelixError, got %T", err)
			}
			if helixErr.Code != testCase.code || helixErr.Details != testCase.details {
				t.Fatalf("unexpected remote error: code=%q details=%q", helixErr.Code, helixErr.Details)
			}
		})
	}
}

func TestClientAPIKeyMutationIsRaceSafe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"users":[]}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithAPIKey("initial"))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if i%2 == 0 {
				client.WithAPIKey("updated")
			} else {
				client.ClearAPIKey()
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				var out findUsersResponse
				if err := client.Exec(context.Background(), findUsers("acme", 1), &out); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestIndexLifecycleResponseDecoders(t *testing.T) {
	operationID := "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"
	receipt, err := UnmarshalIndexDdlReceipt([]byte(`{"kind":"accepted","operation_id":"` + operationID + `","index_id":"42","generation":"3","future":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := receipt.(IndexDdlAccepted); !ok {
		t.Fatalf("expected accepted receipt, got %T", receipt)
	}
	status, err := UnmarshalIndexOperationStatus([]byte(`{"status":"blocked","operation_id":"` + operationID + `","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"scan","attempt":2,"progress":{"entities":"9","input_bytes":"10","output_operations":"11","output_bytes":"12","future":true},"blocker_code":"uniqueness_violation","future":true}`))
	if err != nil {
		t.Fatal(err)
	}
	blocked, ok := status.(*IndexOperationBlocked)
	if !ok || blocked.BlockerCode != IndexBlockerUniquenessViolation {
		t.Fatalf("expected blocked uniqueness status, got %#v", status)
	}
	for _, testCase := range []struct {
		family string
		stage  string
	}{
		{family: "vector", stage: "validate_legacy_physical"},
		{family: "text", stage: "validate_manifests"},
	} {
		status, err := UnmarshalIndexOperationStatus([]byte(`{"status":"queued","operation_id":"` + operationID + `","index_id":"42","generation":"3","operation_kind":"build","family":"` + testCase.family + `","stage":"` + testCase.stage + `","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`))
		if err != nil {
			t.Fatalf("%s build stage %q failed to decode: %v", testCase.family, testCase.stage, err)
		}
		queued, ok := status.(*IndexOperationQueued)
		if !ok || queued.Stage != testCase.stage {
			t.Fatalf("expected queued %s build stage %q, got %#v", testCase.family, testCase.stage, status)
		}
	}
	if _, err := UnmarshalIndexOperationStatus([]byte(`{"status":"queued","operation_id":"` + operationID + `","index_id":"42","generation":"3","operation_kind":"build","family":"text","stage":"await_upload","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`)); err == nil {
		t.Fatal("removed await_upload stage must fail")
	}
	if _, err := UnmarshalIndexDdlReceipt([]byte(`{"kind":"future"}`)); err == nil {
		t.Fatal("unknown receipt tag must fail")
	}
	if _, err := UnmarshalIndexOperationStatus([]byte(`{"status":"queued","operation_id":"018F0C58-6BC7-7C56-8D3D-9C5F18A0F001","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"scan","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`)); err == nil {
		t.Fatal("noncanonical operation ID must fail")
	}
}

func TestDateTimeAndPropertyValueConversions(t *testing.T) {
	parsed, err := ParseDateTimeRFC3339("2025-02-03T04:05:06.123456789Z")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Millis() != 1738555506123 {
		t.Fatalf("unexpected parsed milliseconds: %d", parsed.Millis())
	}
	formatted, err := parsed.RFC3339()
	if err != nil {
		t.Fatal(err)
	}
	if formatted != "2025-02-03T04:05:06.123Z" {
		t.Fatalf("unexpected RFC3339 value: %s", formatted)
	}
	negative, err := DateTimeFromMillis(-1).RFC3339()
	if err != nil {
		t.Fatal(err)
	}
	if negative != "1969-12-31T23:59:59.999Z" {
		t.Fatalf("unexpected negative timestamp: %s", negative)
	}
	if _, err := ParseDateTimeRFC3339("not-a-timestamp"); err == nil {
		t.Fatal("expected malformed RFC3339 input to fail")
	}

	values := []any{
		nil,
		Null(),
		DateTimeFromMillis(1),
		time.UnixMilli(2),
		"text",
		true,
		int(1),
		int8(2),
		int16(3),
		int32(4),
		int64(5),
		uint(6),
		uint8(7),
		uint16(8),
		uint32(9),
		uint64(10),
		float32(1.25),
		float64(2.5),
		[]byte{0, 127, 255},
		[]int{1, 2},
		[]int64{3, 4},
		[]float64{1.5, 2.5},
		[]float32{3.5, 4.5},
		[]string{"a", "b"},
		[]any{"nested", int64(1)},
		[2]int{11, 12},
		map[string]PropertyValue{"ready": Bool(true)},
		map[string]any{"nested": []any{"value", int64(13)}},
		ObjectFromEntries(Entry("name", "Alice"), Entry("score", 9.5)),
		Array(String("x"), I64(14)),
		F64Array(1.0, 2.0),
		F32Array(3.0, 4.0),
		StringArray("x", "y"),
	}
	for i, value := range values {
		converted, err := PropertyValueOf(value)
		if err != nil {
			t.Fatalf("value %d (%T) failed conversion: %v", i, value, err)
		}
		if _, err := json.Marshal(converted); err != nil {
			t.Fatalf("value %d (%T) failed serialization: %v", i, value, err)
		}
	}

	for name, value := range map[string]any{
		"overflowing uint": uint64(math.MaxInt64) + 1,
		"unsupported":      make(chan int),
		"nested unsupported": map[string]any{
			"bad": make(chan int),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := PropertyValueOf(value)
			if err == nil {
				t.Fatal("expected conversion to fail")
			}
			if err.Error() == "" || errors.Unwrap(err) == nil && strings.Contains(name, "nested") {
				t.Fatalf("expected contextual conversion error, got %v", err)
			}
		})
	}
	for _, value := range []PropertyValue{F64(math.Inf(1)), F32(float32(math.NaN())), MustPropertyValue(make(chan int))} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("expected invalid property value to fail serialization")
		}
	}
}

func TestExpressionReferenceAndPredicateConversions(t *testing.T) {
	marshaled := []any{
		ExprInput(ExprID()),
		ParamInput("tenant"),
		PropertyInput{},
		NodeIDs(1, 2),
		NodeVar("nodes"),
		NodeParam("node"),
		EdgeID(3),
		EdgeIDs(4, 5),
		EdgeVar("edges"),
		EdgeParam("edge"),
		ExprID(),
		ExprTimestamp(),
		ExprDateTime(),
		ExprVal(1).Add(ExprVal(2)),
		ExprVal(3).Sub(ExprVal(1)),
		ExprVal(2).Mul(ExprVal(4)),
		ExprVal(8).Div(ExprVal(2)),
		ExprVal(9).Mod(ExprVal(4)),
		ExprVal(1).Neg(),
		ExprCase([]WhenThen{{When: PredEq("ready", true), Then: ExprVal("yes")}}, nil),
		BoundLiteral(10),
		BoundExpr(ExprParam("limit")),
	}
	predicates := []Predicate{
		PredNeq("state", "deleted"),
		PredGt("score", 1),
		PredGte("score", ExprParam("min")),
		PredLt("score", 10),
		PredLte("score", ExprParam("max")),
		PredHasKey("name"),
		PredIsNull("deleted_at"),
		PredIsNotNull("created_at"),
		PredStartsWith("name", "A"),
		PredEndsWith("name", "z"),
		PredContains("name", "li"),
		PredContainsExpr("name", ExprParam("needle")),
		PredIsIn("status", []string{"open", "closed"}),
		PredIsIn("status", ExprParam("statuses")),
		PredIsInExpr("status", ExprParam("statuses")),
		PredAnd(PredEq("a", 1), PredEq("b", 2)),
		PredOr(PredEq("a", 1), PredEq("b", 2)),
		PredNot(PredEq("disabled", true)),
		PredCompare(ExprProp("left"), CompareNeq, ExprProp("right")),
		PredBetween("score", 1, 10),
		PredBetween("score", ExprParam("min"), 10),
		SourceNeq("state", "deleted"),
		SourceGt("score", 1),
		SourceGte("score", 1),
		SourceLt("score", 10),
		SourceLte("score", 10),
		SourceHasKey("name"),
		SourceStartsWith("name", "A"),
		SourceEndsWith("name", "z"),
		SourceContains("name", "li"),
		SourceContainsExpr("name", ExprParam("needle")),
		SourceIsIn("status", []string{"open"}),
		SourceIsInExpr("status", ExprParam("statuses")),
		SourceIsNull("deleted_at"),
		SourceIsNotNull("created_at"),
		SourceAnd(SourceEq("a", 1), SourceEq("b", 2)),
		SourceOr(SourceEq("a", 1), SourceEq("b", 2)),
		SourceNot(SourceEq("disabled", true)),
		SourceCompare(ExprProp("left"), CompareEq, ExprProp("right")),
		SourceBetween("score", 1, 10),
	}
	for _, predicate := range predicates {
		marshaled = append(marshaled, predicate)
	}
	for i, value := range marshaled {
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("value %d (%T) failed serialization: %v", i, value, err)
		}
		if len(body) == 0 {
			t.Fatalf("value %d (%T) serialized to an empty body", i, value)
		}
	}
}

func TestIndexAndTraversalPublicSurfaceSerializes(t *testing.T) {
	indexes := []IndexSpec{
		NodeEqualityIndex("User", "email"),
		NodeUniqueEqualityIndex("User", "email"),
		EdgeEqualityIndex("FOLLOWS", "since"),
		EdgeRangeIndex("FOLLOWS", "weight"),
		NodeTextIndex("Doc", "body", "tenant_id"),
		EdgeVectorIndex("SIMILAR", "embedding", 3, VectorDistanceEuclidean, "tenant_id"),
		EdgeTextIndex("MENTIONS", "body", "tenant_id"),
	}
	for i, index := range indexes {
		if _, err := json.Marshal(index); err != nil {
			t.Fatalf("index %d failed serialization: %v", i, err)
		}
	}
	steps := []Step{
		CreateVectorIndexNodesStep("Doc", "embedding", 3, VectorDistanceCosine),
		CreateVectorIndexEdgesStep("SIMILAR", "embedding", 3, VectorDistanceManhattan),
		CreateTextIndexNodesStep("Doc", "body"),
		CreateTextIndexEdgesStep("MENTIONS", "body"),
	}
	for i, step := range steps {
		if _, err := json.Marshal(step); err != nil {
			t.Fatalf("step %d failed serialization: %v", i, err)
		}
	}

	repeats := []RepeatConfig{
		Repeat(Sub().Out()).WithTimes(2),
		Repeat(Sub().In()).UntilPred(PredEq("ready", true)).EmitAll().WithMaxDepth(4),
		Repeat(Sub().Both()).EmitAfter(),
		Repeat(Sub().Out()).EmitBefore(),
		Repeat(Sub().In()).EmitIf(PredEq("ready", true)),
	}
	for i, repeat := range repeats {
		if _, err := json.Marshal(repeat); err != nil {
			t.Fatalf("repeat %d failed serialization: %v", i, err)
		}
	}

	operationID := "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001"
	tenant := ValueInput("acme")
	traversals := []struct {
		name      string
		traversal *Traversal
	}{
		{"nodes with label predicate", G().NWithLabelWhere("User", SourceGt("age", 18))},
		{"edges with label predicate", G().EWithLabelWhere("FOLLOWS", SourceGt("weight", 0))},
		{"vector nodes", G().VectorSearchNodes("Doc", "embedding", []float64{1, 2, 3}, ExprParam("k"), "acme")},
		{"vector nodes typed", G().VectorSearchNodesWith("Doc", "embedding", ValueInput(F32Array(1, 2, 3)), BoundLiteral(3), &tenant)},
		{"text nodes", G().TextSearchNodes("Doc", "body", ExprParam("query"), 5)},
		{"text nodes typed", G().TextSearchNodesWith("Doc", "body", ParamInput("query"), BoundExpr(ExprParam("k")), &tenant)},
		{"text nodes within", G().NWithLabel("Doc").TextSearchNodesWithin("Doc", "body", "query", 5)},
		{"text nodes within typed", G().NWithLabel("Doc").TextSearchNodesWithinWith("Doc", "body", ParamInput("query"), BoundExpr(ExprParam("k")), &tenant)},
		{"vector edges", G().VectorSearchEdges("SIMILAR", "embedding", []float32{1, 2, 3}, 5)},
		{"vector edges typed", G().VectorSearchEdgesWith("SIMILAR", "embedding", ValueInput(F32Array(1, 2, 3)), BoundLiteral(3), nil)},
		{"text edges", G().TextSearchEdges("MENTIONS", "body", "query", 5)},
		{"text edges typed", G().TextSearchEdgesWith("MENTIONS", "body", ValueInput("query"), BoundLiteral(3), nil)},
		{"text edges within", G().E(AllEdges()).TextSearchEdgesWithin("MENTIONS", "body", "query", 5)},
		{"text edges within typed", G().E(AllEdges()).TextSearchEdgesWithinWith("MENTIONS", "body", ParamInput("query"), BoundExpr(ExprParam("k")), &tenant)},
		{"in", G().N(NodeID(1)).In("FOLLOWS")},
		{"both", G().N(NodeID(1)).Both()},
		{"out edges", G().N(NodeID(1)).OutE("FOLLOWS")},
		{"in edges", G().N(NodeID(1)).InE()},
		{"both edges", G().N(NodeID(1)).BothE("FOLLOWS")},
		{"out node", G().E(EdgeID(1)).OutN()},
		{"in node", G().E(EdgeID(1)).InN()},
		{"other node", G().E(EdgeID(1)).OtherN()},
		{"has", G().N(AllNodes()).Has("active", true)},
		{"has key", G().N(AllNodes()).HasKey("name")},
		{"dedup", G().N(AllNodes()).Dedup()},
		{"within", G().N(AllNodes()).Within("selected")},
		{"without", G().N(AllNodes()).Without("blocked")},
		{"edge has", G().E(AllEdges()).EdgeHas("weight", ExprParam("weight"))},
		{"edge has label", G().E(AllEdges()).EdgeHasLabel("FOLLOWS")},
		{"skip literal", G().N(AllNodes()).Skip(2)},
		{"skip expression", G().N(AllNodes()).Skip(ExprParam("skip"))},
		{"range literal", G().N(AllNodes()).Range(1, 3)},
		{"range expression", G().N(AllNodes()).Range(ExprParam("start"), ExprParam("end"))},
		{"as", G().N(AllNodes()).As("users")},
		{"store", G().N(AllNodes()).Store("users")},
		{"select", G().N(AllNodes()).Select("users")},
		{"inject", G().Inject("users")},
		{"exists", G().N(AllNodes()).Exists()},
		{"id", G().N(AllNodes()).ID()},
		{"label", G().N(AllNodes()).Label()},
		{"values", G().N(AllNodes()).Values("name", "age")},
		{"value map all", G().N(AllNodes()).ValueMapAll()},
		{"edge properties", G().E(AllEdges()).EdgeProperties()},
		{"add edge", G().N(NodeID(1)).AddE("FOLLOWS", NodeID(2), Props{PropInput("since", ParamInput("since"))})},
		{"set property", G().N(NodeID(1)).SetProperty("name", ExprParam("name"))},
		{"remove property", G().N(NodeID(1)).RemoveProperty("name")},
		{"drop", G().N(NodeID(1)).Drop()},
		{"drop edge", G().N(NodeID(1)).DropEdge(NodeID(2))},
		{"drop labeled edge", G().N(NodeID(1)).DropEdgeLabeled(NodeID(2), "FOLLOWS")},
		{"drop edge by id", G().DropEdgeByID(EdgeID(1))},
		{"order", G().N(AllNodes()).OrderBy("name", OrderAsc)},
		{"orders", G().N(AllNodes()).OrderByMultiple(Ordering{Property: "name", Order: OrderAsc}, Ordering{Property: "age", Order: OrderDesc})},
		{"repeat", G().N(NodeID(1)).Repeat(Repeat(Sub().Out()).WithTimes(2))},
		{"choose", G().N(NodeID(1)).Choose(PredEq("active", true), Sub().Out(), Sub().In())},
		{"coalesce", G().N(NodeID(1)).Coalesce(Sub().Out(), Sub().In())},
		{"group", G().N(AllNodes()).Group("country")},
		{"group count", G().N(AllNodes()).GroupCount("country")},
		{"aggregate", G().N(AllNodes()).AggregateBy(AggregateMean, "score")},
		{"create index", G().CreateIndexIfNotExists(NodeEqualityIndex("User", "email"))},
		{"drop index", G().DropIndex(NodeEqualityIndex("User", "email"))},
		{"get operation", G().GetIndexOperation(operationID)},
		{"retry operation", G().RetryIndexOperation(operationID)},
		{"abort operation", G().AbortIndexOperation(operationID)},
		{"create vector nodes", G().CreateVectorIndexNodes("Doc", "embedding", 3, VectorDistanceCosine)},
		{"create vector edges", G().CreateVectorIndexEdges("SIMILAR", "embedding", 3, VectorDistanceEuclidean)},
		{"create text nodes", G().CreateTextIndexNodes("Doc", "body")},
		{"create text edges", G().CreateTextIndexEdges("MENTIONS", "body")},
		{"fold", G().N(AllNodes()).Fold()},
		{"unfold", G().N(AllNodes()).Unfold()},
		{"path", G().N(NodeID(1)).Path()},
		{"simple path", G().N(NodeID(1)).SimplePath()},
		{"with sack", G().N(NodeID(1)).WithSack(0)},
		{"sack set", G().N(NodeID(1)).SackSet("weight")},
		{"sack add", G().N(NodeID(1)).SackAdd("weight")},
		{"sack get", G().N(NodeID(1)).SackGet()},
	}
	for _, tc := range traversals {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.traversal.Validate(); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(tc.traversal)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), `"root"`) {
				t.Fatalf("traversal did not serialize a root: %s", body)
			}
		})
	}
}
