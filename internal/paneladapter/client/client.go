// Package client implements the strict REST v1 client for the panel adapter.
//
// It provides typed methods for the four panel API endpoints, bearer header
// authentication, ETag/conditional request support, Cache-Control no-store
// verification, and retryable error classification.
//
// The client deliberately does NOT support V2bX/V2Board/XrayR/SSPanel
// compatibility paths or token-in-URL configurations.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

// ErrNotModified is returned when the panel responds with HTTP 304,
// indicating the resource has not changed since the supplied ETag.
var ErrNotModified = E.New("resource not modified")

// ErrNoStore is returned when a 200 response for a configuration or user
// endpoint is missing the required Cache-Control: no-store header.
var ErrNoStore = E.New("response missing Cache-Control: no-store")

// ---------------------------------------------------------------------------
// ResponseError — structured API error
// ---------------------------------------------------------------------------

// ResponseError represents an error returned by the panel API, parsed from
// the RFC 7807-like ProblemDetails JSON body.
type ResponseError struct {
	StatusCode int    // HTTP status code
	Code       string // Machine-readable error code from ProblemDetails
	Title      string // Human-readable title from ProblemDetails
	Detail     string // Optional detail from ProblemDetails
	RequestID  string // Request ID from ProblemDetails (or X-Request-ID header)
}

func (e *ResponseError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("panel API error %d %s: %s (%s)", e.StatusCode, e.Code, e.Title, e.Detail)
	}
	return fmt.Sprintf("panel API error %d %s: %s", e.StatusCode, e.Code, e.Title)
}

// ---------------------------------------------------------------------------
// Error classification helpers
// ---------------------------------------------------------------------------

// IsRetryable returns true if the error represents a transient failure that
// may succeed on retry (HTTP 429, 503, or transport-level errors).
func IsRetryable(err error) bool {
	retryable, _, _ := ClassifyError(err)
	return retryable
}

// ClassifyError inspects an error and returns three boolean flags:
//   - retryable:  true for 429, 503, or transport-level errors
//   - isConflict: true for 409 REVISION_CONFLICT or IDEMPOTENCY_CONFLICT
//   - isMismatch: true for 403 NODE_MISMATCH
//
// Known sentinel errors (ErrNotModified, ErrNoStore) are not retryable.
// Errors that are not *ResponseError and not a known sentinel are treated
// as transport-level errors and classified as retryable.
func ClassifyError(err error) (retryable bool, isConflict bool, isMismatch bool) {
	if err == nil {
		return false, false, false
	}
	// Known non-retryable sentinels from this package.
	if errors.Is(err, ErrNotModified) || errors.Is(err, ErrNoStore) {
		return false, false, false
	}
	var respErr *ResponseError
	if !errors.As(err, &respErr) {
		// Non-ResponseError, non-sentinel errors are transport-level → retryable.
		return true, false, false
	}
	switch respErr.StatusCode {
	case 429, 503:
		return true, false, false
	case 409:
		return false, true, false
	case 403:
		if respErr.Code == "NODE_MISMATCH" {
			return false, false, true
		}
	}
	return false, false, false
}

// ---------------------------------------------------------------------------
// Option — functional options for Client
// ---------------------------------------------------------------------------

// Option configures a Client during construction.
type Option func(*Client)

// WithHTTPClient sets a custom *http.Client on the panel client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		c.httpClient = hc
	}
}

// WithLogger sets a custom logger on the panel client.
func WithLogger(l log.ContextLogger) Option {
	return func(c *Client) {
		c.logger = l
	}
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client is the strict REST v1 client for the panel adapter API.
type Client struct {
	baseURL    *url.URL
	nodeID     string
	tokenMu    sync.RWMutex
	token      string
	httpClient *http.Client
	logger     log.ContextLogger
}

var secureRand = rand.Reader

// New creates a new panel API client.
//
// baseURL must be a valid URL without token or secret query parameters.
// nodeID identifies this node to the panel.
// token is the bearer token sent in the Authorization header.
func New(baseURL, nodeID, token string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, E.New("baseURL is required")
	}
	if nodeID == "" {
		return nil, E.New("nodeID is required")
	}
	if token == "" {
		return nil, E.New("token is required")
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, E.Cause(err, "invalid baseURL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, E.New("baseURL must use http or https scheme")
	}
	if parsed.Host == "" {
		return nil, E.New("baseURL must have a host")
	}

	// Reject token/secret in URL query — bearer header only.
	for key := range parsed.Query() {
		lower := strings.ToLower(key)
		if lower == "token" || lower == "secret" || lower == "key" || lower == "api_key" || lower == "apikey" {
			return nil, E.New("baseURL must not contain token/secret in query parameters — use bearer header authentication")
		}
	}

	c := &Client{
		baseURL: parsed,
		nodeID:  nodeID,
		token:   token,
	}

	// Apply defaults before options so options can override.
	c.httpClient = &http.Client{}
	c.logger = log.NewNOPFactory().Logger()

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// ---------------------------------------------------------------------------
// API paths
// ---------------------------------------------------------------------------

const (
	pathConfiguration = "/api/v1/nodes/{nodeID}/configuration"
	pathUsers         = "/api/v1/nodes/{nodeID}/inbounds/{inboundID}/users"
	pathTraffic       = "/api/v1/nodes/{nodeID}/traffic-reports"
	pathHeartbeat     = "/api/v1/nodes/{nodeID}/heartbeats"
	pathTokenRotation = "/api/v1/nodes/{nodeID}/adapter-token/rotate"
)

func (c *Client) configurationURL() string {
	u := *c.baseURL
	u.Path = strings.Replace(pathConfiguration, "{nodeID}", url.PathEscape(c.nodeID), 1)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (c *Client) usersURL(inboundID string) string {
	u := *c.baseURL
	p := strings.Replace(pathUsers, "{nodeID}", url.PathEscape(c.nodeID), 1)
	p = strings.Replace(p, "{inboundID}", url.PathEscape(inboundID), 1)
	u.Path = p
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (c *Client) trafficURL() string {
	u := *c.baseURL
	u.Path = strings.Replace(pathTraffic, "{nodeID}", url.PathEscape(c.nodeID), 1)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (c *Client) heartbeatURL() string {
	u := *c.baseURL
	u.Path = strings.Replace(pathHeartbeat, "{nodeID}", url.PathEscape(c.nodeID), 1)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (c *Client) tokenRotationURL() string {
	u := *c.baseURL
	u.Path = strings.Replace(pathTokenRotation, "{nodeID}", url.PathEscape(c.nodeID), 1)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

func generateRequestID() string {
	var buf [16]byte
	_, _ = io.ReadFull(secureRand, buf[:])
	return fmt.Sprintf("%x", buf[:])
}

func (c *Client) newRequest(ctx context.Context, method, urlStr string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, E.Cause(err, "create request")
	}
	req.Header.Set("Authorization", "Bearer "+c.currentToken())
	req.Header.Set("Accept", "application/json")
	if req.Header.Get("X-Request-ID") == "" {
		req.Header.Set("X-Request-ID", generateRequestID())
	}
	return req, nil
}

func (c *Client) currentToken() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.token
}

// SetToken replaces the bearer token used by subsequent requests.
func (c *Client) SetToken(token string) error {
	if token == "" {
		return E.New("token is required")
	}
	c.tokenMu.Lock()
	c.token = token
	c.tokenMu.Unlock()
	return nil
}

func (c *Client) doRequest(req *http.Request) (*http.Response, error) {
	c.logger.DebugContext(req.Context(), "panel client: ", req.Method, " ", redactURL(req.URL))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, E.Cause(err, "panel request failed")
	}
	return resp, nil
}

// readAndClose reads the response body and closes it, returning the bytes.
func readAndClose(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// handleErrorResponse parses a non-2xx response into a ResponseError.
func (c *Client) handleErrorResponse(resp *http.Response) error {
	body, err := readAndClose(resp)
	if err != nil {
		return E.Cause(err, "read error response body")
	}

	var pd contract.ProblemDetails
	if jsonErr := json.Unmarshal(body, &pd); jsonErr == nil && pd.Title != "" {
		reqID := pd.RequestID
		if reqID == "" {
			reqID = resp.Header.Get("X-Request-ID")
		}
		return &ResponseError{
			StatusCode: resp.StatusCode,
			Code:       pd.Code,
			Title:      pd.Title,
			Detail:     pd.Detail,
			RequestID:  reqID,
		}
	}

	// Fallback if body is not valid ProblemDetails.
	return &ResponseError{
		StatusCode: resp.StatusCode,
		Code:       fmt.Sprintf("HTTP_%d", resp.StatusCode),
		Title:      http.StatusText(resp.StatusCode),
		RequestID:  resp.Header.Get("X-Request-ID"),
	}
}

// checkNoStore verifies the Cache-Control: no-store header is present on
// 200 responses for configuration and user endpoints.
func checkNoStore(resp *http.Response) error {
	cc := resp.Header.Get("Cache-Control")
	if !strings.Contains(strings.ToLower(cc), "no-store") {
		return ErrNoStore
	}
	return nil
}

// ---------------------------------------------------------------------------
// FetchConfiguration
// ---------------------------------------------------------------------------

// FetchConfiguration retrieves the node configuration from the panel.
//
// If etag is non-empty, an If-None-Match header is sent. When the panel
// responds 304, ErrNotModified is returned. A 200 response must include
// Cache-Control: no-store or ErrNoStore is returned.
//
// Returns (configuration, new ETag, error).
func (c *Client) FetchConfiguration(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, c.configurationURL(), nil)
	if err != nil {
		return nil, "", err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, "", err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if err := checkNoStore(resp); err != nil {
			_, _ = readAndClose(resp)
			return nil, "", err
		}
		body, err := readAndClose(resp)
		if err != nil {
			return nil, "", E.Cause(err, "read configuration response")
		}
		var cfg contract.ConfigurationResponse
		if err := json.Unmarshal(body, &cfg); err != nil {
			return nil, "", E.Cause(err, "decode configuration response")
		}
		newETag := resp.Header.Get("ETag")
		return &cfg, newETag, nil

	case http.StatusNotModified:
		_, _ = readAndClose(resp)
		return nil, "", ErrNotModified

	default:
		return nil, "", c.handleErrorResponse(resp)
	}
}

// ---------------------------------------------------------------------------
// FetchUsers
// ---------------------------------------------------------------------------

// FetchUsers retrieves the user list for an inbound from the panel.
//
// If etag is non-empty, an If-None-Match header is sent.
// appliedConfigRev sets the X-Applied-Configuration-Revision header.
// When the panel responds 304, ErrNotModified is returned.
// A 200 response must include Cache-Control: no-store or ErrNoStore is returned.
//
// Returns (snapshot, new ETag, error).
func (c *Client) FetchUsers(ctx context.Context, inboundID, etag, appliedConfigRev string) (*contract.UserSnapshot, string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, c.usersURL(inboundID), nil)
	if err != nil {
		return nil, "", err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if appliedConfigRev != "" {
		req.Header.Set("X-Applied-Configuration-Revision", appliedConfigRev)
	}

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, "", err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if err := checkNoStore(resp); err != nil {
			_, _ = readAndClose(resp)
			return nil, "", err
		}
		body, err := readAndClose(resp)
		if err != nil {
			return nil, "", E.Cause(err, "read users response")
		}
		var snap contract.UserSnapshot
		if err := json.Unmarshal(body, &snap); err != nil {
			return nil, "", E.Cause(err, "decode users response")
		}
		newETag := resp.Header.Get("ETag")
		return &snap, newETag, nil

	case http.StatusNotModified:
		_, _ = readAndClose(resp)
		return nil, "", ErrNotModified

	default:
		return nil, "", c.handleErrorResponse(resp)
	}
}

// ---------------------------------------------------------------------------
// ReportTraffic
// ---------------------------------------------------------------------------

// ReportTraffic posts a traffic report to the panel.
//
// idempotencyKey sets the Idempotency-Key header for duplicate detection.
// A 409 with code IDEMPOTENCY_CONFLICT is surfaced as a ResponseError.
func (c *Client) ReportTraffic(ctx context.Context, report *contract.TrafficReport, idempotencyKey string) error {
	body, err := json.Marshal(report)
	if err != nil {
		return E.Cause(err, "marshal traffic report")
	}

	req, err := c.newRequest(ctx, http.MethodPost, c.trafficURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if report.ConfigurationRevision != "" {
		req.Header.Set("X-Applied-Configuration-Revision", report.ConfigurationRevision)
	}

	resp, err := c.doRequest(req)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		_, _ = readAndClose(resp)
		return nil

	default:
		return c.handleErrorResponse(resp)
	}
}

// ---------------------------------------------------------------------------
// SendHeartbeat
// ---------------------------------------------------------------------------

// SendHeartbeat posts a heartbeat to the panel.
//
// The X-Applied-Configuration-Revision header is set from
// heartbeat.AppliedConfigurationRevision.
func (c *Client) SendHeartbeat(ctx context.Context, heartbeat *contract.Heartbeat) error {
	body, err := json.Marshal(heartbeat)
	if err != nil {
		return E.Cause(err, "marshal heartbeat")
	}

	req, err := c.newRequest(ctx, http.MethodPost, c.heartbeatURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if heartbeat.AppliedConfigurationRevision != "" {
		req.Header.Set("X-Applied-Configuration-Revision", heartbeat.AppliedConfigurationRevision)
	}

	resp, err := c.doRequest(req)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		_, _ = readAndClose(resp)
		return nil

	default:
		return c.handleErrorResponse(resp)
	}
}

// TokenRotationResponse is returned after the panel issues a replacement token.
type TokenRotationResponse struct {
	Token              string    `json:"token"`
	ExpiresAt          time.Time `json:"expires_at"`
	RotateAfterSeconds int       `json:"rotate_after_seconds"`
}

// RotateToken requests a replacement for the currently authenticated token.
// The caller is responsible for persisting and activating the returned secret.
func (c *Client) RotateToken(ctx context.Context) (*TokenRotationResponse, error) {
	req, err := c.newRequest(ctx, http.MethodPost, c.tokenRotationURL(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, c.handleErrorResponse(resp)
	}
	body, err := readAndClose(resp)
	if err != nil {
		return nil, E.Cause(err, "read token rotation response")
	}
	var rotated TokenRotationResponse
	if err := json.Unmarshal(body, &rotated); err != nil {
		return nil, E.Cause(err, "decode token rotation response")
	}
	if rotated.Token == "" || rotated.ExpiresAt.IsZero() {
		return nil, E.New("invalid token rotation response")
	}
	return &rotated, nil
}

// ---------------------------------------------------------------------------
// Redaction helpers
// ---------------------------------------------------------------------------

// redactURL removes query parameters from a URL for safe logging.
func redactURL(u *url.URL) string {
	safe := *u
	safe.User = nil // redact any user:pass
	safe.RawQuery = ""
	safe.Fragment = ""
	return safe.String()
}
