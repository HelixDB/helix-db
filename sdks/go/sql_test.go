package helix

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSQLRequestEncodesTheServerContract(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		request SQLRequest
		want    string
	}{
		{name: "bare", request: SQLRequest{Query: "SELECT 1"}, want: `{"query":"SELECT 1","mappings":{}}`},
		{
			name:    "positional",
			request: SQLRequest{Query: "SELECT ?", Args: []any{int64(9223372036854775807)}, QueryName: "q"},
			want:    `{"query":"SELECT ?","parameters":[9223372036854775807],"query_name":"q","mappings":{}}`,
		},
		{name: "empty positional", request: SQLRequest{Query: "SELECT 1", Args: []any{}}, want: `{"query":"SELECT 1","parameters":[],"mappings":{}}`},
		{
			name: "named with mappings",
			request: SQLRequest{
				Query:      "SELECT p.name FROM people p WHERE p.age = :age",
				Parameters: map[string]any{"age": 30},
				Mappings:   SQLMappings{Tables: map[string]string{"people": "Person"}, JoinColumns: map[string]string{"people.team_id": "MEMBER_OF"}},
			},
			want: `{"query":"SELECT p.name FROM people p WHERE p.age = :age","parameters":{"age":30},"mappings":{"tables":{"people":"Person"},"join_columns":{"people.team_id":"MEMBER_OF"}}}`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body, err := json.Marshal(testCase.request)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != testCase.want {
				t.Fatalf("body = %s, want %s", body, testCase.want)
			}
		})
	}

	_, err := json.Marshal(SQLRequest{Query: "SELECT ?", Args: []any{1}, Parameters: map[string]any{"a": 1}})
	if err == nil {
		t.Fatal("both parameter forms must be rejected")
	}
}

func TestSQLRoutesHeadersAndResults(t *testing.T) {
	var paths, bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		if r.Header.Get("x-helix-database-id") != "db-1" || r.Header.Get("x-helix-require-writer") != "true" {
			t.Errorf("missing client or option headers: %v", r.Header)
		}
		if r.URL.Path == "/v2/sql/explain" {
			_, _ = w.Write([]byte(`{"effect":"Write","operators":[{"rows":9007199254740993}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"columns":["score"],"rows":[[{"$type":"integer","value":"9223372036854775807"}]]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithDatabaseID("db-1"))
	if err != nil {
		t.Fatal(err)
	}
	query := SQLRequest{Query: "INSERT INTO people (score) VALUES (?) RETURNING score", Args: []any{"x"}}
	result, err := client.SQL(context.Background(), query, WriterOnly())
	if err != nil {
		t.Fatal(err)
	}
	if result.Columns[0] != "score" || result.Rows[0][0].(map[string]any)["value"] != "9223372036854775807" {
		t.Fatalf("invalid lossless result: %#v", result)
	}
	plan, err := client.ExplainSQL(context.Background(), query, WriterOnly())
	if err != nil {
		t.Fatal(err)
	}
	if plan["operators"].([]any)[0].(map[string]any)["rows"] != json.Number("9007199254740993") {
		t.Fatalf("plan numbers must stay exact: %#v", plan)
	}
	want := `{"query":"INSERT INTO people (score) VALUES (?) RETURNING score","parameters":["x"],"mappings":{}}`
	if len(paths) != 2 || paths[0] != "/v2/sql" || paths[1] != "/v2/sql/explain" || bodies[0] != want || bodies[1] != want {
		t.Fatalf("unexpected requests: %q %q", paths, bodies)
	}
}

func TestSQLRemoteDiagnosticsAndInvalidRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"unsupported_feature","msg":"no subqueries","details":{"detail":"procedures_and_subqueries","phase":"compile"}}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SQL(context.Background(), SQLRequest{Query: "SELECT (SELECT 1)"})
	var helixErr *HelixError
	if !errors.As(err, &helixErr) || helixErr.Kind != ErrorRemote || helixErr.StatusCode != http.StatusBadRequest || helixErr.Code != QueryErrorCode("unsupported_feature") {
		t.Fatalf("expected a remote diagnostic, got %T %v", err, err)
	}
	_, err = client.ExplainSQL(context.Background(), SQLRequest{Query: "SELECT ?", Args: []any{1}, Parameters: map[string]any{}})
	if !errors.As(err, &helixErr) || helixErr.Kind != ErrorSerialization {
		t.Fatalf("expected a serialization error, got %T %v", err, err)
	}
}

// fakeSQLDB is a native build with both SQL methods.
type fakeSQLDB struct {
	*fakeNativeDB
	explains [][]byte
}

func (f *fakeSQLDB) SqlJson(request []byte) ([]byte, error) { return f.QueryJson(request) }

func (f *fakeSQLDB) ExplainSqlJson(request []byte) ([]byte, error) {
	f.explains = append(f.explains, append([]byte(nil), request...))
	if f.err != nil {
		return nil, f.err
	}
	return []byte(`{"effect":"Read","operators":[]}`), nil
}

func TestEmbeddedSQLUsesMatchingBindingMethods(t *testing.T) {
	native := &fakeSQLDB{fakeNativeDB: &fakeNativeDB{response: []byte(`{"columns":["name"],"rows":[["Ada"]]}`)}}
	client := &Client{embedded: native}
	query := SQLRequest{Query: "SELECT p.name FROM people p", Mappings: SQLMappings{Tables: map[string]string{"people": "Person"}}}

	result, err := client.SQL(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0][0] != "Ada" {
		t.Fatalf("unexpected result: %#v", result)
	}
	plan, err := client.ExplainSQL(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan["effect"] != "Read" {
		t.Fatalf("unexpected plan: %#v", plan)
	}
	want := `{"query":"SELECT p.name FROM people p","mappings":{"tables":{"people":"Person"}}}`
	if len(native.requests) != 1 || string(native.requests[0]) != want || len(native.explains) != 1 || string(native.explains[0]) != want {
		t.Fatalf("unexpected native requests: execute=%q explain=%q", native.requests, native.explains)
	}

	for _, opt := range []ExecOption{WriterOnly(), WarmOnly(), AwaitDurability(false)} {
		_, sqlErr := client.SQL(context.Background(), query, opt)
		_, explainErr := client.ExplainSQL(context.Background(), query, opt)
		for _, err := range []error{sqlErr, explainErr} {
			var helixErr *HelixError
			if !errors.As(err, &helixErr) || helixErr.Kind != ErrorInvalidRequest {
				t.Fatalf("expected invalid request HelixError, got %T %v", err, err)
			}
		}
	}
	if len(native.requests) != 1 || len(native.explains) != 1 {
		t.Fatal("rejected options reached the native binding")
	}

	native.err = &fakeQueryError{code: QueryErrorCode("unsupported_feature:compile:procedures_and_subqueries"), msg: "no subqueries"}
	_, err = client.ExplainSQL(context.Background(), query)
	var helixErr *HelixError
	if !errors.As(err, &helixErr) || helixErr.Kind != ErrorEmbedded || helixErr.Code != QueryErrorCode("unsupported_feature:compile:procedures_and_subqueries") {
		t.Fatalf("expected embedded HelixError, got %T %v", err, err)
	}
}

func TestEmbeddedSQLReportsMissingBindingMethods(t *testing.T) {
	client := &Client{embedded: fakeCypherDB{&fakeNativeDB{}}}
	_, noSQL := client.SQL(context.Background(), SQLRequest{Query: "SELECT 1"})
	_, noExplain := client.ExplainSQL(context.Background(), SQLRequest{Query: "SELECT 1"})
	for _, err := range []error{noSQL, noExplain} {
		var helixErr *HelixError
		if !errors.As(err, &helixErr) || helixErr.Kind != ErrorEmbeddedUnavailable || helixErr.Details != "rebuild native bindings with SQL support" {
			t.Fatalf("expected embedded unavailable, got %T %v", err, err)
		}
	}
}
