package helix

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestCypherContract(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v2/cypher" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer local-test" {
			t.Errorf("wrong Cypher routing or headers")
		}
		var body CypherRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Query != "RETURN $x AS x" || body.QueryName != "parameter" {
			t.Errorf("lost request fields")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["x"],"rows":[[{"$type":"integer","value":"9223372036854775807"}]]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithAPIKey("local-test"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Cypher(context.Background(), CypherRequest{Query: "RETURN $x AS x", QueryName: "parameter", Parameters: map[string]any{"x": int64(9223372036854775807)}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(result.Columns) != 1 || result.Rows[0][0].(map[string]any)["value"] != "9223372036854775807" {
		t.Fatalf("invalid lossless result: %#v", result)
	}
}

func TestCypherRoutesSendClientAndOptionHeaders(t *testing.T) {
	optionHeaders := []string{"x-helix-require-writer", "x-helix-warm", "x-helix-await-durable"}
	for _, testCase := range []struct {
		name string
		opts []ExecOption
		want map[string]string
	}{
		{name: "no options", want: map[string]string{}},
		{name: "writer only", opts: []ExecOption{WriterOnly()}, want: map[string]string{"x-helix-require-writer": "true"}},
		{name: "warm only", opts: []ExecOption{WarmOnly()}, want: map[string]string{"x-helix-warm": "true"}},
		{name: "await durable", opts: []ExecOption{AwaitDurability(true)}, want: map[string]string{"x-helix-await-durable": "true"}},
		{name: "skip durable wait", opts: []ExecOption{AwaitDurability(false)}, want: map[string]string{"x-helix-await-durable": "false"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var paths []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.URL.Path)
				if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("unexpected method %s or content type %q", r.Method, r.Header.Get("Content-Type"))
				}
				if got := r.Header.Get("Authorization"); got != "Bearer hx_secret" {
					t.Errorf("authorization = %q", got)
				}
				if got := r.Header.Get("x-helix-database-id"); got != "db-123" {
					t.Errorf("database id = %q", got)
				}
				for _, header := range optionHeaders {
					if got := r.Header.Get(header); got != testCase.want[header] {
						t.Errorf("%s = %q, want %q", header, got, testCase.want[header])
					}
				}
				if r.URL.Path == "/v2/cypher/explain" {
					_, _ = w.Write([]byte(`{"effect":"read"}`))
					return
				}
				_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
			}))
			defer server.Close()
			client, err := NewClient(server.URL, WithAPIKey("hx_secret"), WithDatabaseID("db-123"))
			if err != nil {
				t.Fatal(err)
			}

			query := CypherRequest{Query: "RETURN 1 AS x"}
			if _, err := client.Cypher(context.Background(), query, testCase.opts...); err != nil {
				t.Fatal(err)
			}
			if _, err := client.ExplainCypher(context.Background(), query, testCase.opts...); err != nil {
				t.Fatal(err)
			}
			if want := []string{"/v2/cypher", "/v2/cypher/explain"}; !reflect.DeepEqual(paths, want) {
				t.Fatalf("paths = %v, want %v", paths, want)
			}
		})
	}
}

func TestCypherOmitsUnsetCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, header := range []string{"Authorization", "x-helix-database-id"} {
			if _, ok := r.Header[http.CanonicalHeaderKey(header)]; ok {
				t.Errorf("unexpected %s header", header)
			}
		}
		_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithAPIKey("hx_secret"), WithDatabaseID("db-123"))
	if err != nil {
		t.Fatal(err)
	}

	client.ClearAPIKey().ClearDatabaseID()
	if _, err := client.Cypher(context.Background(), CypherRequest{Query: "RETURN 1 AS x"}); err != nil {
		t.Fatal(err)
	}
}

func TestExplainCypherDecodesPlanLosslessly(t *testing.T) {
	var body CypherRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"effect":"write","bindings":[],"returns":[],"operators":[{"rows":9007199254740993}],"planner":{},"notices":[]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := client.ExplainCypher(context.Background(), CypherRequest{
		Query:      "CREATE (:N {key:$key})",
		Parameters: map[string]any{"key": 7},
		QueryName:  "create_n",
	})
	if err != nil {
		t.Fatal(err)
	}
	if body.Query != "CREATE (:N {key:$key})" || body.QueryName != "create_n" || body.Parameters["key"] != float64(7) {
		t.Fatalf("explain lost request fields: %#v", body)
	}
	if plan["effect"] != "write" {
		t.Fatalf("effect = %v", plan["effect"])
	}
	rows := plan["operators"].([]any)[0].(map[string]any)["rows"]
	if rows != json.Number("9007199254740993") {
		t.Fatalf("operator estimate lost precision: %#v", rows)
	}
}

func TestCypherRoutesDecodeRemoteDiagnostics(t *testing.T) {
	const diagnostic = `{"error":"SyntaxError","msg":"unexpected end","details":{"detail":"UnexpectedEnd","phase":"compile","span":{"start":7,"end":8}}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(diagnostic))
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	query := CypherRequest{Query: "RETURN ("}
	_, cypherErr := client.Cypher(context.Background(), query)
	_, explainErr := client.ExplainCypher(context.Background(), query)
	for _, err := range []error{cypherErr, explainErr} {
		var helixErr *HelixError
		if !errors.As(err, &helixErr) {
			t.Fatalf("expected HelixError, got %T %v", err, err)
		}
		if helixErr.Kind != ErrorRemote || helixErr.StatusCode != http.StatusBadRequest {
			t.Fatalf("unexpected kind/status: %s %d", helixErr.Kind, helixErr.StatusCode)
		}
		if helixErr.Code != QueryErrorCode("SyntaxError") || helixErr.Details != "unexpected end" {
			t.Fatalf("unexpected code/details: %q %q", helixErr.Code, helixErr.Details)
		}
		if helixErr.ServerDetails != `{"detail":"UnexpectedEnd","phase":"compile","span":{"start":7,"end":8}}` {
			t.Fatalf("unexpected server details: %s", helixErr.ServerDetails)
		}
		if IsConflict(err) || IsRetryable(err) {
			t.Fatal("a diagnostic is neither a conflict nor retryable")
		}
	}
}

func TestCypherConflictAndWarmNoContent(t *testing.T) {
	status := http.StatusConflict
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	query := CypherRequest{Query: "MATCH (n) RETURN n"}
	if _, err := client.Cypher(context.Background(), query); !errors.Is(err, ErrConflict) || !IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
	status = http.StatusNoContent
	result, err := client.Cypher(context.Background(), query, WarmOnly())
	if err != nil {
		t.Fatal(err)
	}
	if result.Columns != nil || result.Rows != nil {
		t.Fatalf("warm read returned a payload: %#v", result)
	}
	plan, err := client.ExplainCypher(context.Background(), query, WarmOnly())
	if err != nil || plan != nil {
		t.Fatalf("warm explain = %v, %v", plan, err)
	}
}

func TestCypherRequestFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/cypher" {
			// Declaring more bytes than are sent ends the body early.
			w.Header().Set("Content-Length", "64")
			_, _ = w.Write([]byte(`{"columns":`))
			return
		}
		_, _ = w.Write([]byte(`not json`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	unreachable, err := NewClient(closed.URL)
	if err != nil {
		t.Fatal(err)
	}

	query := CypherRequest{Query: "RETURN 1 AS x"}
	for _, testCase := range []struct {
		name string
		run  func() error
		kind ErrorKind
	}{
		{name: "nil client", kind: ErrorInvalidRequest, run: func() error {
			_, err := (*Client)(nil).Cypher(context.Background(), query)
			return err
		}},
		{name: "unencodable parameters", kind: ErrorSerialization, run: func() error {
			_, err := client.Cypher(context.Background(), CypherRequest{Query: "RETURN $x", Parameters: map[string]any{"x": func() {}}})
			return err
		}},
		{name: "nil context", kind: ErrorInvalidRequest, run: func() error {
			var ctx context.Context
			_, err := client.Cypher(ctx, query)
			return err
		}},
		{name: "client without server", kind: ErrorInvalidURL, run: func() error {
			_, err := (&Client{}).ExplainCypher(context.Background(), query)
			return err
		}},
		{name: "malformed response", kind: ErrorSerialization, run: func() error {
			_, err := client.ExplainCypher(context.Background(), query)
			return err
		}},
		{name: "truncated response", kind: ErrorNetwork, run: func() error {
			_, err := client.Cypher(context.Background(), query)
			return err
		}},
		{name: "unreachable server", kind: ErrorNetwork, run: func() error {
			_, err := unreachable.Cypher(context.Background(), query)
			return err
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var helixErr *HelixError
			if err := testCase.run(); !errors.As(err, &helixErr) || helixErr.Kind != testCase.kind {
				t.Fatalf("expected %s HelixError, got %T %v", testCase.kind, err, err)
			}
		})
	}
}

// fakeCypherDB is a native build with Cypher execution but no explain support.
type fakeCypherDB struct {
	*fakeNativeDB
}

func (f fakeCypherDB) CypherJson(request []byte) ([]byte, error) { return f.QueryJson(request) }

// fakeCypherExplainDB is a native build with both Cypher methods.
type fakeCypherExplainDB struct {
	fakeCypherDB
	explains [][]byte
}

func (f *fakeCypherExplainDB) ExplainCypherJson(request []byte) ([]byte, error) {
	f.explains = append(f.explains, append([]byte(nil), request...))
	if f.err != nil {
		return nil, f.err
	}
	return []byte(`{"effect":"read","operators":[]}`), nil
}

func TestEmbeddedCypherUsesMatchingBindingMethods(t *testing.T) {
	native := &fakeCypherExplainDB{fakeCypherDB: fakeCypherDB{&fakeNativeDB{response: []byte(`{"columns":["x"],"rows":[[1]]}`)}}}
	client := &Client{embedded: native}
	query := CypherRequest{Query: "RETURN 1 AS x", QueryName: "one"}

	result, err := client.Cypher(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, &CypherResponse{Columns: []string{"x"}, Rows: [][]any{{json.Number("1")}}}) {
		t.Fatalf("unexpected result: %#v", result)
	}
	plan, err := client.ExplainCypher(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan["effect"] != "read" {
		t.Fatalf("unexpected plan: %#v", plan)
	}
	want := `{"query":"RETURN 1 AS x","query_name":"one"}`
	if len(native.requests) != 1 || string(native.requests[0]) != want || len(native.explains) != 1 || string(native.explains[0]) != want {
		t.Fatalf("unexpected native requests: execute=%q explain=%q", native.requests, native.explains)
	}
}

func TestEmbeddedCypherRejectsServerOptions(t *testing.T) {
	native := &fakeCypherExplainDB{fakeCypherDB: fakeCypherDB{&fakeNativeDB{response: []byte(`{"columns":[],"rows":[]}`)}}}
	client := &Client{embedded: native}
	query := CypherRequest{Query: "RETURN 1 AS x"}

	for _, opt := range []ExecOption{WriterOnly(), WarmOnly(), AwaitDurability(false)} {
		_, cypherErr := client.Cypher(context.Background(), query, opt)
		_, explainErr := client.ExplainCypher(context.Background(), query, opt)
		for _, err := range []error{cypherErr, explainErr} {
			var helixErr *HelixError
			if !errors.As(err, &helixErr) || helixErr.Kind != ErrorInvalidRequest {
				t.Fatalf("expected invalid request HelixError, got %T %v", err, err)
			}
			if helixErr.Code != QueryErrorCode("invalid_request") || helixErr.Details != "exec options require server mode" {
				t.Fatalf("unexpected rejection: code=%q details=%q", helixErr.Code, helixErr.Details)
			}
		}
	}
	if len(native.requests) != 0 || len(native.explains) != 0 {
		t.Fatal("rejected options reached the native binding")
	}
}

func TestEmbeddedCypherReportsMissingBindingMethods(t *testing.T) {
	query := CypherRequest{Query: "RETURN 1 AS x"}
	_, noCypher := (&Client{embedded: &fakeNativeDB{}}).Cypher(context.Background(), query)
	_, noExplain := (&Client{embedded: fakeCypherDB{&fakeNativeDB{}}}).ExplainCypher(context.Background(), query)
	for _, testCase := range []struct {
		err     error
		details string
	}{
		{err: noCypher, details: "rebuild native bindings with Cypher support"},
		{err: noExplain, details: "rebuild native bindings with Cypher explain support"},
	} {
		var helixErr *HelixError
		if !errors.As(testCase.err, &helixErr) || helixErr.Kind != ErrorEmbeddedUnavailable || helixErr.Details != testCase.details {
			t.Fatalf("expected embedded unavailable %q, got %T %v", testCase.details, testCase.err, testCase.err)
		}
	}
}

func TestEmbeddedCypherPreservesNativeErrorCodeAndMessage(t *testing.T) {
	native := &fakeCypherExplainDB{fakeCypherDB: fakeCypherDB{&fakeNativeDB{err: &fakeQueryError{
		code: QueryErrorCode("SyntaxError"),
		msg:  "unexpected end",
	}}}}
	client := &Client{embedded: native}
	query := CypherRequest{Query: "RETURN ("}

	_, cypherErr := client.Cypher(context.Background(), query)
	_, explainErr := client.ExplainCypher(context.Background(), query)
	for _, err := range []error{cypherErr, explainErr} {
		var helixErr *HelixError
		if !errors.As(err, &helixErr) || helixErr.Kind != ErrorEmbedded {
			t.Fatalf("expected embedded HelixError, got %T %v", err, err)
		}
		if helixErr.Code != QueryErrorCode("SyntaxError") || helixErr.Details != "unexpected end" {
			t.Fatalf("unexpected embedded error: code=%q details=%q", helixErr.Code, helixErr.Details)
		}
	}
}

func TestPostCypherRejectsUnknownRoute(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected an unknown route to panic")
		}
	}()
	client := &Client{embedded: &fakeCypherExplainDB{fakeCypherDB: fakeCypherDB{&fakeNativeDB{}}}}
	_ = client.postCypher(context.Background(), cypherRoute("/v2/unknown"), CypherRequest{}, nil, nil)
}
