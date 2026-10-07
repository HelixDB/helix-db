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

// Cypher executes once. It never retries an uncertain modifying statement.
func (c *Client) Cypher(ctx context.Context, query CypherRequest) (*CypherResponse, error) {
	if c == nil {
		return nil, &HelixError{Kind: ErrorInvalidRequest, Details: "nil client"}
	}
	body, err := json.Marshal(query)
	if err != nil {
		return nil, &HelixError{Kind: ErrorSerialization, Err: err}
	}
	var data []byte
	if c.embedded != nil {
		native, ok := c.embedded.(interface{ CypherJson([]byte) ([]byte, error) })
		if !ok {
			return nil, &HelixError{Kind: ErrorEmbeddedUnavailable, Details: "rebuild native bindings with Cypher support"}
		}
		data, err = native.CypherJson(body)
		if err != nil {
			return nil, embeddedError(err)
		}
	} else {
		if c.baseURL == nil {
			return nil, &HelixError{Kind: ErrorInvalidURL, Details: "nil server client"}
		}
		endpoint := c.baseURL.ResolveReference(&url.URL{Path: "/v2/cypher"})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return nil, &HelixError{Kind: ErrorInvalidRequest, Err: err}
		}
		request.Header.Set("content-type", "application/json")
		if key := c.getAPIKey(); key != "" {
			request.Header.Set("authorization", "Bearer "+key)
		}
		response, err := c.httpClient.Do(request)
		if err != nil {
			return nil, &HelixError{Kind: ErrorNetwork, Err: err}
		}
		defer response.Body.Close()
		data, err = io.ReadAll(response.Body)
		if err != nil {
			return nil, &HelixError{Kind: ErrorNetwork, Err: err}
		}
		if response.StatusCode != http.StatusOK {
			return nil, decodeRemoteError(data, response.Status, response.StatusCode)
		}
	}
	var result CypherResponse
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, &HelixError{Kind: ErrorSerialization, Err: err}
	}
	return &result, nil
}
