package helix

import (
	"context"
	"encoding/json"
	"errors"
)

// SQLRequest is one SQL statement for the local server and embedded API. The
// statement is translated to Cypher and runs exactly like a [CypherRequest].
//
// Args bind each ? placeholder in order ($1, $2, …). Parameters bind :name,
// $name, @name and ?N placeholders by name. Set at most one of the two; values
// use the lossless encoding of Cypher parameters.
type SQLRequest struct {
	Query      string
	Args       []any
	Parameters map[string]any
	QueryName  string
	Mappings   SQLMappings
}

// SQLMappings maps SQL names onto the graph for one request. Tables maps a
// table to a node label, or a join's middle table to a relationship type.
// JoinColumns maps a join's ON column, written table.column, to a
// relationship type. Names without a mapping are used as written.
type SQLMappings struct {
	Tables      map[string]string `json:"tables,omitempty"`
	JoinColumns map[string]string `json:"join_columns,omitempty"`
}

// MarshalJSON encodes the POST /v2/sql request body.
func (r SQLRequest) MarshalJSON() ([]byte, error) {
	var parameters any
	switch {
	case r.Args != nil && r.Parameters != nil:
		return nil, errors.New("helix: set at most one of SQLRequest.Args and SQLRequest.Parameters")
	case r.Args != nil:
		parameters = r.Args
	case r.Parameters != nil:
		parameters = r.Parameters
	}
	return json.Marshal(struct {
		Query      string      `json:"query"`
		Parameters any         `json:"parameters,omitempty"`
		QueryName  string      `json:"query_name,omitempty"`
		Mappings   SQLMappings `json:"mappings"`
	}{r.Query, parameters, r.QueryName, r.Mappings})
}

// SQL translates one statement to Cypher and executes it once, like
// [Client.Cypher]. Exec options and the client's database ID apply as they do
// for Exec.
func (c *Client) SQL(ctx context.Context, query SQLRequest, opts ...ExecOption) (*CypherResponse, error) {
	var result CypherResponse
	if err := c.postStatement(ctx, sqlExecuteRoute, query, opts, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ExplainSQL plans a statement's translation without executing it and returns
// the same planning report as [Client.ExplainCypher].
func (c *Client) ExplainSQL(ctx context.Context, query SQLRequest, opts ...ExecOption) (map[string]any, error) {
	var result map[string]any
	if err := c.postStatement(ctx, sqlExplainRoute, query, opts, &result); err != nil {
		return nil, err
	}
	return result, nil
}
