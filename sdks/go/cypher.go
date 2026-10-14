package helix

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

// CypherRequest uses the same JSON contract as the local server and embedded API.
type CypherRequest struct {
	Query      string         `json:"query"`
	Parameters map[string]any `json:"parameters,omitempty"`
	QueryName  string         `json:"query_name,omitempty"`
}

// CypherResponse retains tagged graph values and large integers losslessly.
type CypherResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// cypherRoute is a Cypher HTTP path; postCypher pairs each with the embedded
// binding method that serves the same JSON contract.
type cypherRoute string

const (
	cypherExecuteRoute cypherRoute = "/v2/cypher"
	cypherExplainRoute cypherRoute = "/v2/cypher/explain"
)

// Cypher executes once. It never retries an uncertain modifying statement.
// Exec options and the client's database ID apply as they do for Exec.
func (c *Client) Cypher(ctx context.Context, query CypherRequest, opts ...ExecOption) (*CypherResponse, error) {
	var result CypherResponse
	if err := c.postCypher(ctx, cypherExecuteRoute, query, opts, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ExplainCypher plans a statement without executing it and returns the
// planning report (effect, bindings, returns, operators, planner, notices).
// Explaining a modifying statement writes nothing.
func (c *Client) ExplainCypher(ctx context.Context, query CypherRequest, opts ...ExecOption) (map[string]any, error) {
	var result map[string]any
	if err := c.postCypher(ctx, cypherExplainRoute, query, opts, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// postCypher sends one request to route, or to its embedded binding method,
// and decodes the JSON response into out with numbers kept exact.
func (c *Client) postCypher(ctx context.Context, route cypherRoute, query CypherRequest, opts []ExecOption, out any) error {
	if c == nil {
		return &HelixError{Kind: ErrorInvalidRequest, Details: "nil client"}
	}
	body, err := json.Marshal(query)
	if err != nil {
		return &HelixError{Kind: ErrorSerialization, Err: err}
	}
	options := execOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	var data []byte
	if c.embedded != nil {
		if err := options.rejectEmbedded(); err != nil {
			return err
		}
		// Older native builds predate a method, so detect each one.
		var native func([]byte) ([]byte, error)
		switch route {
		case cypherExecuteRoute:
			db, ok := c.embedded.(interface{ CypherJson([]byte) ([]byte, error) })
			if !ok {
				return &HelixError{Kind: ErrorEmbeddedUnavailable, Details: "rebuild native bindings with Cypher support"}
			}
			native = db.CypherJson
		case cypherExplainRoute:
			db, ok := c.embedded.(interface{ ExplainCypherJson([]byte) ([]byte, error) })
			if !ok {
				return &HelixError{Kind: ErrorEmbeddedUnavailable, Details: "rebuild native bindings with Cypher explain support"}
			}
			native = db.ExplainCypherJson
		default:
			panic("helix: unknown Cypher route " + string(route))
		}
		if data, err = native(body); err != nil {
			return embeddedError(err)
		}
	} else {
		if c.baseURL == nil {
			return &HelixError{Kind: ErrorInvalidURL, Details: "nil server client"}
		}
		endpoint := c.baseURL.ResolveReference(&url.URL{Path: string(route)})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return &HelixError{Kind: ErrorInvalidRequest, Err: err}
		}
		c.setHeaders(request, options)
		response, err := c.httpClient.Do(request)
		if err != nil {
			return &HelixError{Kind: ErrorNetwork, Err: err}
		}
		defer response.Body.Close()
		data, err = io.ReadAll(response.Body)
		if err != nil {
			return &HelixError{Kind: ErrorNetwork, Err: err}
		}
		// A Helix Cloud warm read succeeds without a payload, as it does for Exec.
		if response.StatusCode == http.StatusNoContent {
			return nil
		}
		if response.StatusCode != http.StatusOK {
			remoteErr := decodeRemoteError(data, response.Status, response.StatusCode)
			if response.StatusCode == http.StatusConflict {
				remoteErr.Err = ErrConflict
			}
			return remoteErr
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return &HelixError{Kind: ErrorSerialization, Err: err}
	}
	return nil
}
