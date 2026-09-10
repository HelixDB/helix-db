package helix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
