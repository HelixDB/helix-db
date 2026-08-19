package helix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

type ErrorKind string
type QueryErrorCode string

const (
	DatabaseIDHeader                   = "x-helix-tenant-id"
	ErrorNetwork             ErrorKind = "Network"
	ErrorRemote              ErrorKind = "Remote"
	ErrorSerialization       ErrorKind = "Serialization"
	ErrorInvalidURL          ErrorKind = "InvalidUrl"
	ErrorInvalidRequest      ErrorKind = "InvalidRequest"
	ErrorEmbedded            ErrorKind = "Embedded"
	ErrorEmbeddedUnavailable ErrorKind = "EmbeddedUnavailable"
)

var ErrConflict = errors.New("helix: conflict")
var ErrNativeBindingsUnavailable = errors.New("helix embedded native bindings are not linked")

type HelixError struct {
	Kind       ErrorKind
	Code       QueryErrorCode
	Details    string
	StatusCode int
	Err        error
}

func (e *HelixError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("helix %s error: %s", e.Kind, e.Details)
	}
	if e.Err != nil {
		return fmt.Sprintf("helix %s error: %v", e.Kind, e.Err)
	}
	return "helix " + string(e.Kind) + " error"
}

func (e *HelixError) Unwrap() error { return e.Err }

func IsConflict(err error) bool {
	var helixErr *HelixError
	return errors.Is(err, ErrConflict) || errors.As(err, &helixErr) && helixErr.Kind == ErrorRemote && helixErr.StatusCode == http.StatusConflict
}

type Client struct {
	baseURL           *url.URL
	httpClient        *http.Client
	embedded          nativeDB
	apiKeyMu          sync.RWMutex
	apiKey            string
	databaseIDMu      sync.RWMutex
	databaseID        *string
	requireDatabaseID bool
}

type nativeDB interface {
	QueryJson([]byte) ([]byte, error)
	Close() error
}

type nativeQueryError interface {
	error
	QueryError() (QueryErrorCode, string)
}

func embeddedError(err error) *HelixError {
	result := &HelixError{Kind: ErrorEmbedded, Err: err, Details: err.Error()}
	var queryErr nativeQueryError
	if errors.As(err, &queryErr) {
		result.Code, result.Details = queryErr.QueryError()
	}
	return result
}

type HelixDbSource interface {
	helixDbSource()
}

type InMemorySource struct {
	Database string
}

func (InMemorySource) helixDbSource() {}

type DiskSource struct {
	Root     string
	Database string
}

func (DiskSource) helixDbSource() {}

type ObjectStorageSource struct {
	Database  string
	Bucket    string
	Region    string
	Endpoint  string
	AllowHTTP bool
}

func (ObjectStorageSource) helixDbSource() {}

type EmbeddedCacheMode interface {
	embeddedCacheMode()
}

type VectorMemoryOnlyCache struct{}

func (VectorMemoryOnlyCache) embeddedCacheMode() {}

type MemoryCache struct{}

func (MemoryCache) embeddedCacheMode() {}

type HybridCache struct {
	SlateMemoryBytes     uint64
	SlateDiskPath        string
	SlateDiskBytes       uint64
	ObjectStoreDiskPath  string
	ObjectStoreDiskBytes uint64
}

func (HybridCache) embeddedCacheMode() {}

type EmbeddedCacheConfig struct {
	VectorMemoryBytes uint64
	Mode              EmbeddedCacheMode
}

type ClientOption func(*Client)

func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		if httpClient != nil {
			c.httpClient = httpClient
		}
	}
}

func WithAPIKey(apiKey string) ClientOption {
	return func(c *Client) { c.setAPIKey(apiKey) }
}

// WithDatabaseID configures the managed database sent in the
// x-helix-tenant-id header for server requests.
func WithDatabaseID(databaseID string) ClientOption {
	return func(c *Client) { c.setDatabaseID(databaseID) }
}

func NewClient(baseURL string, opts ...ClientOption) (*Client, error) {
	if baseURL == "" {
		baseURL = "http://localhost:6969"
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		if err == nil {
			err = fmt.Errorf("missing scheme or host")
		}
		return nil, &HelixError{Kind: ErrorInvalidURL, Err: err, Details: err.Error()}
	}
	client := &Client{baseURL: parsed, httpClient: http.DefaultClient}
	for _, opt := range opts {
		opt(client)
	}
	if databaseID, required := client.getDatabaseConfig(); databaseID != nil {
		if err := validateDatabaseID(databaseID, required); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// NewManagedClient creates a server client that requires a database ID and
// sends it using the canonical x-helix-tenant-id header.
func NewManagedClient(baseURL, databaseID string, opts ...ClientOption) (*Client, error) {
	client, err := NewClient(baseURL, opts...)
	if err != nil {
		return nil, err
	}
	if err := client.setManagedDatabaseID(databaseID); err != nil {
		return nil, err
	}
	return client, nil
}

func NewEmbeddedClient(source HelixDbSource, opts ...ClientOption) (*Client, error) {
	return newEmbeddedClient(source, nil, false, opts...)
}

func NewEmbeddedClientWithConfig(source HelixDbSource, cache EmbeddedCacheConfig, opts ...ClientOption) (*Client, error) {
	return newEmbeddedClient(source, &cache, false, opts...)
}

func newEmbeddedClient(source HelixDbSource, cache *EmbeddedCacheConfig, reader bool, opts ...ClientOption) (*Client, error) {
	db, err := openEmbedded(source, reader, cache)
	if err != nil {
		kind := ErrorEmbedded
		if errors.Is(err, ErrNativeBindingsUnavailable) {
			kind = ErrorEmbeddedUnavailable
		}
		if kind == ErrorEmbedded {
			return nil, embeddedError(err)
		}
		return nil, &HelixError{Kind: kind, Err: err, Details: err.Error()}
	}
	client := &Client{embedded: db, httpClient: http.DefaultClient}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

func NewEmbeddedReaderClient(source HelixDbSource, opts ...ClientOption) (*Client, error) {
	return newEmbeddedClient(source, nil, true, opts...)
}

func NewEmbeddedReaderClientWithConfig(source HelixDbSource, cache EmbeddedCacheConfig, opts ...ClientOption) (*Client, error) {
	return newEmbeddedClient(source, &cache, true, opts...)
}

func (c *Client) WithAPIKey(apiKey string) *Client { c.setAPIKey(apiKey); return c }
func (c *Client) ClearAPIKey() *Client             { c.setAPIKey(""); return c }
func (c *Client) WithDatabaseID(databaseID string) *Client {
	c.setDatabaseID(databaseID)
	return c
}
func (c *Client) ClearDatabaseID() *Client {
	c.clearDatabaseID()
	return c
}
func (c *Client) BaseURL() string {
	if c == nil || c.baseURL == nil {
		return ""
	}
	return c.baseURL.String()
}

func (c *Client) setAPIKey(apiKey string) {
	if c == nil {
		return
	}
	c.apiKeyMu.Lock()
	c.apiKey = apiKey
	c.apiKeyMu.Unlock()
}

func (c *Client) getAPIKey() string {
	c.apiKeyMu.RLock()
	apiKey := c.apiKey
	c.apiKeyMu.RUnlock()
	return apiKey
}

func (c *Client) setDatabaseID(databaseID string) {
	if c == nil {
		return
	}
	c.databaseIDMu.Lock()
	c.databaseID = &databaseID
	c.databaseIDMu.Unlock()
}

func (c *Client) clearDatabaseID() {
	if c == nil {
		return
	}
	c.databaseIDMu.Lock()
	c.databaseID = nil
	c.databaseIDMu.Unlock()
}

func (c *Client) setManagedDatabaseID(databaseID string) error {
	if err := validateDatabaseID(&databaseID, true); err != nil {
		return err
	}
	c.databaseIDMu.Lock()
	c.databaseID = &databaseID
	c.requireDatabaseID = true
	c.databaseIDMu.Unlock()
	return nil
}

func (c *Client) getDatabaseConfig() (*string, bool) {
	c.databaseIDMu.RLock()
	defer c.databaseIDMu.RUnlock()
	if c.databaseID == nil {
		return nil, c.requireDatabaseID
	}
	databaseID := *c.databaseID
	return &databaseID, c.requireDatabaseID
}

func validateDatabaseID(databaseID *string, required bool) error {
	if required && databaseID == nil {
		return &HelixError{
			Kind:    ErrorInvalidRequest,
			Code:    QueryErrorCode("invalid_request"),
			Details: "managed clients require a database ID",
		}
	}
	if databaseID != nil && strings.TrimSpace(*databaseID) == "" {
		return &HelixError{
			Kind:    ErrorInvalidRequest,
			Code:    QueryErrorCode("invalid_request"),
			Details: "database ID must not be empty",
		}
	}
	return nil
}

type execOptions struct {
	writerOnly      bool
	warmOnly        bool
	awaitDurability *bool
}

type ExecOption func(*execOptions)

func WriterOnly() ExecOption { return func(o *execOptions) { o.writerOnly = true } }
func WarmOnly() ExecOption   { return func(o *execOptions) { o.warmOnly = true } }
func AwaitDurability(should bool) ExecOption {
	return func(o *execOptions) { o.awaitDurability = &should }
}

func (c *Client) Exec(ctx context.Context, req Request, out any, opts ...ExecOption) error {
	if c == nil {
		return &HelixError{Kind: ErrorInvalidURL, Details: "nil client"}
	}
	body, err := MarshalRequest(req)
	if err != nil {
		return &HelixError{Kind: ErrorSerialization, Err: err, Details: err.Error()}
	}
	options := execOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if c.embedded != nil {
		if options.writerOnly || options.warmOnly || options.awaitDurability != nil {
			return &HelixError{
				Kind:    ErrorInvalidRequest,
				Code:    QueryErrorCode("invalid_request"),
				Details: "exec options require server mode",
			}
		}
		response, err := c.embedded.QueryJson(body)
		if err != nil {
			return embeddedError(err)
		}
		if out == nil || len(response) == 0 {
			return nil
		}
		decoder := json.NewDecoder(bytes.NewReader(response))
		decoder.UseNumber()
		if err := decoder.Decode(out); err != nil {
			return &HelixError{Kind: ErrorSerialization, Err: err, Details: err.Error()}
		}
		return nil
	}
	if c.baseURL == nil {
		return &HelixError{Kind: ErrorInvalidURL, Details: "nil server client"}
	}
	databaseID, requireDatabaseID := c.getDatabaseConfig()
	if err := validateDatabaseID(databaseID, requireDatabaseID); err != nil {
		return err
	}
	endpoint := c.baseURL.ResolveReference(&url.URL{Path: "/v2/query"})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return &HelixError{Kind: ErrorInvalidURL, Err: err, Details: err.Error()}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if apiKey := c.getAPIKey(); apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if databaseID != nil {
		httpReq.Header.Set(DatabaseIDHeader, *databaseID)
	}
	if options.writerOnly {
		httpReq.Header.Set("x-helix-require-writer", "true")
	}
	if options.warmOnly {
		httpReq.Header.Set("x-helix-warm", "true")
	}
	if options.awaitDurability != nil {
		if *options.awaitDurability {
			httpReq.Header.Set("x-helix-await-durable", "true")
		} else {
			httpReq.Header.Set("x-helix-await-durable", "false")
		}
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return &HelixError{Kind: ErrorNetwork, Err: err, Details: err.Error()}
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &HelixError{Kind: ErrorNetwork, Err: err, Details: err.Error()}
	}
	if resp.StatusCode != http.StatusOK {
		remoteErr := decodeRemoteError(respBody, resp.Status, resp.StatusCode)
		if resp.StatusCode == http.StatusConflict {
			remoteErr.Err = ErrConflict
		}
		return remoteErr
	}
	if out == nil || len(respBody) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return &HelixError{Kind: ErrorSerialization, Err: err, Details: err.Error()}
	}
	return nil
}

func (c *Client) Close() error {
	if c == nil || c.embedded == nil {
		return nil
	}
	if err := c.embedded.Close(); err != nil {
		return embeddedError(err)
	}
	return nil
}

func decodeRemoteError(body []byte, fallback string, statusCode int) *HelixError {
	var envelope struct {
		Error string  `json:"error"`
		Msg   *string `json:"msg"`
		Code  *string `json:"code"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Error != "" {
		if envelope.Msg != nil {
			return &HelixError{
				Kind:       ErrorRemote,
				Code:       QueryErrorCode(envelope.Error),
				Details:    *envelope.Msg,
				StatusCode: statusCode,
			}
		}
		code := QueryErrorCode("")
		if envelope.Code != nil {
			code = QueryErrorCode(*envelope.Code)
		}
		return &HelixError{
			Kind:       ErrorRemote,
			Code:       code,
			Details:    envelope.Error,
			StatusCode: statusCode,
		}
	}
	details := string(body)
	if details == "" {
		details = fallback
	}
	return &HelixError{Kind: ErrorRemote, Details: details, StatusCode: statusCode}
}
