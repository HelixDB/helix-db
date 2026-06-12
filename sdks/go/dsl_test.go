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

func TestDynamicRequestJSON(t *testing.T) {
	body, err := MarshalRequest(findUsers("acme", 25))
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, want := range []string{`"request_type":"read"`, `"query_name":"find_users"`, `"tenant_id":"acme"`, `"limit":25`, `"parameter_types":{"limit":"I64","tenant_id":"String"}`} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("request JSON missing %s in %s", want, jsonText)
		}
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
			want: `{"NodeRange":{"label":"User","property":"age"}}`,
		},
		{
			name: "node explicit asc",
			spec: NodeRangeIndexWithDirection("User", "age", RangeIndexAsc),
			want: `{"NodeRange":{"label":"User","property":"age"}}`,
		},
		{
			name: "node desc",
			spec: NodeRangeDescIndex("User", "age"),
			want: `{"NodeRange":{"direction":"Desc","label":"User","property":"age"}}`,
		},
		{
			name: "edge desc",
			spec: EdgeRangeDescIndex("FOLLOWS", "weight"),
			want: `{"EdgeRange":{"direction":"Desc","label":"FOLLOWS","property":"weight"}}`,
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
