package helix

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
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
		WithParameterValue("tenant", QueryString("acme")).
		WithParameterType("tenant", ParamTypeString())
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
	if capturedPath != "/v1/query" {
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
	for _, stage := range []string{"await_upload", "validate_manifests"} {
		status, err := UnmarshalIndexOperationStatus([]byte(`{"status":"queued","operation_id":"` + operationID + `","index_id":"42","generation":"3","operation_kind":"build","family":"text","stage":"` + stage + `","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`))
		if err != nil {
			t.Fatalf("text build stage %q failed to decode: %v", stage, err)
		}
		queued, ok := status.(*IndexOperationQueued)
		if !ok || queued.Stage != stage {
			t.Fatalf("expected queued text build stage %q, got %#v", stage, status)
		}
	}
	if _, err := UnmarshalIndexDdlReceipt([]byte(`{"kind":"future"}`)); err == nil {
		t.Fatal("unknown receipt tag must fail")
	}
	if _, err := UnmarshalIndexOperationStatus([]byte(`{"status":"queued","operation_id":"018F0C58-6BC7-7C56-8D3D-9C5F18A0F001","index_id":"42","generation":"3","operation_kind":"build","family":"secondary","stage":"scan","attempt":0,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`)); err == nil {
		t.Fatal("noncanonical operation ID must fail")
	}
}
