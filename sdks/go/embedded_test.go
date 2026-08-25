package helix

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fakeNativeDB struct {
	requests [][]byte
	response []byte
	closed   bool
	err      error
}

type fakeQueryError struct {
	code QueryErrorCode
	msg  string
}

func (e *fakeQueryError) Error() string                        { return e.msg }
func (e *fakeQueryError) QueryError() (QueryErrorCode, string) { return e.code, e.msg }

func (f *fakeNativeDB) QueryJson(request []byte) ([]byte, error) {
	f.requests = append(f.requests, append([]byte(nil), request...))
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

func (f *fakeNativeDB) Close() error {
	f.closed = true
	return f.err
}

func TestEmbeddedExecCallsQueryJson(t *testing.T) {
	native := &fakeNativeDB{response: []byte(`{"users":[{"$id":1,"name":"Ada"}]}`)}
	client := &Client{embedded: native}
	var out findUsersResponse

	if err := client.Exec(context.Background(), findUsers("acme", 1), &out); err != nil {
		t.Fatal(err)
	}

	if len(native.requests) != 1 {
		t.Fatalf("expected one native request, got %d", len(native.requests))
	}
	var request map[string]any
	if err := json.Unmarshal(native.requests[0], &request); err != nil {
		t.Fatal(err)
	}
	if request["request_type"] != "read" {
		t.Fatalf("unexpected request type: %v", request["request_type"])
	}
	if got := out.Users[0].Name; got != "Ada" {
		t.Fatalf("unexpected decoded response: %q", got)
	}
}

func TestEmbeddedExecRejectsServerOptions(t *testing.T) {
	client := &Client{embedded: &fakeNativeDB{response: []byte(`{}`)}}

	err := client.Exec(context.Background(), findUsers("acme", 1), nil, WriterOnly())
	if err == nil {
		t.Fatal("expected embedded client to reject server options")
	}
	var helixErr *HelixError
	if !errors.As(err, &helixErr) || helixErr.Kind != ErrorInvalidRequest {
		t.Fatalf("expected invalid request HelixError, got %T %v", err, err)
	}
	if helixErr.Details != "exec options require server mode" {
		t.Fatalf("unexpected error details: %q", helixErr.Details)
	}
}

func TestEmbeddedCloseCallsNativeClose(t *testing.T) {
	native := &fakeNativeDB{}
	client := &Client{embedded: native}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !native.closed {
		t.Fatal("expected native close to be called")
	}
}

func TestEmbeddedExecPreservesNativeErrorCodeAndMessage(t *testing.T) {
	native := &fakeNativeDB{err: &fakeQueryError{
		code: QueryErrorCode("index_not_found"),
		msg:  "missing text index",
	}}
	client := &Client{embedded: native}

	err := client.Exec(context.Background(), findUsers("acme", 1), nil)
	var helixErr *HelixError
	if !errors.As(err, &helixErr) {
		t.Fatalf("expected HelixError, got %T", err)
	}
	if helixErr.Code != QueryErrorCode("index_not_found") || helixErr.Details != "missing text index" {
		t.Fatalf("unexpected embedded error: code=%q details=%q", helixErr.Code, helixErr.Details)
	}
}
