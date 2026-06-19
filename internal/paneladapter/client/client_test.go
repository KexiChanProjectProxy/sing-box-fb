package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const (
	testToken  = "test-bearer-token-abc123"
	testNodeID = "node-42"
)

// captureHandler records request details for assertions.
type captureHandler struct {
	method       string
	path         string
	headers      http.Header
	body         []byte
	statusCode   int
	responseBody interface{}
	extraHeaders map[string]string
	noStore      bool
}

func (h *captureHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.method = r.Method
	h.path = r.URL.Path
	h.headers = r.Header.Clone()

	body, _ := io.ReadAll(r.Body)
	h.body = body

	// Set response headers
	if h.extraHeaders != nil {
		for k, v := range h.extraHeaders {
			w.Header().Set(k, v)
		}
	}
	if h.noStore {
		w.Header().Set("Cache-Control", "no-store")
	}

	w.WriteHeader(h.statusCode)
	if h.responseBody != nil {
		respBytes, _ := json.Marshal(h.responseBody)
		_, _ = w.Write(respBytes)
	}
}

func newTestClient(serverURL string) (*Client, error) {
	return New(serverURL, testNodeID, testToken)
}

func validConfigurationResponse() *contract.ConfigurationResponse {
	return &contract.ConfigurationResponse{
		Revision:   "rev-1",
		APIVersion: "v1",
		NodeID:     testNodeID,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRestartProcess,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         30,
			TrafficSeconds:       60,
			HeartbeatSeconds:     120,
		},
		ManagedInbounds: []contract.ManagedInbound{
			{InboundID: "inb-1", Tag: "in-hysteria2", Protocol: "hysteria2"},
		},
		SingBoxConfigTemplate: json.RawMessage(`{"inbounds":[]}`),
	}
}

func validUserSnapshot() *contract.UserSnapshot {
	return &contract.UserSnapshot{
		Revision:              "user-rev-1",
		ConfigurationRevision: "rev-1",
		NodeID:                testNodeID,
		InboundID:             "inb-1",
		Protocol:              "hysteria2",
		Users: []contract.User{
			{UserID: "u-1", Name: "alice", Credential: contract.Credential{Type: contract.CredentialTypePassword, Password: "s3cret"}},
		},
	}
}

func validTrafficReport() *contract.TrafficReport {
	return &contract.TrafficReport{
		StartedAt:             time.Now().Add(-time.Minute),
		EndedAt:               time.Now(),
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inb-1", UserID: "u-1", UploadBytes: 1024, DownloadBytes: 2048},
		},
	}
}

func validHeartbeat() *contract.Heartbeat {
	return &contract.Heartbeat{
		ObservedAt:                   time.Now(),
		SingBoxVersion:               "1.10.0",
		AdapterVersion:               "0.1.0",
		AppliedConfigurationRevision: "rev-1",
		Inbounds: []contract.HeartbeatInbound{
			{InboundID: "inb-1", Protocol: "hysteria2", AppliedUserRevision: "user-rev-1", UserCount: 1, UserLoadStatus: contract.UserLoadStatusOK},
		},
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNew_Valid(t *testing.T) {
	c, err := New("https://panel.example.com", testNodeID, testToken)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.nodeID != testNodeID {
		t.Errorf("nodeID = %q, want %q", c.nodeID, testNodeID)
	}
	if c.token != testToken {
		t.Errorf("token = %q, want %q", c.token, testToken)
	}
}

func TestNew_MissingBaseURL(t *testing.T) {
	_, err := New("", testNodeID, testToken)
	if err == nil {
		t.Fatal("expected error for empty baseURL")
	}
}

func TestNew_MissingNodeID(t *testing.T) {
	_, err := New("https://panel.example.com", "", testToken)
	if err == nil {
		t.Fatal("expected error for empty nodeID")
	}
}

func TestNew_MissingToken(t *testing.T) {
	_, err := New("https://panel.example.com", testNodeID, "")
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestNew_TokenInURLRejected(t *testing.T) {
	cases := []string{
		"https://panel.example.com?token=secret",
		"https://panel.example.com?TOKEN=secret",
		"https://panel.example.com?api_key=secret",
		"https://panel.example.com?apikey=secret",
		"https://panel.example.com?secret=value",
		"https://panel.example.com?key=value",
	}
	for _, rawURL := range cases {
		_, err := New(rawURL, testNodeID, testToken)
		if err == nil {
			t.Errorf("expected error for URL %q", rawURL)
		}
		if err != nil && !strings.Contains(err.Error(), "token") && !strings.Contains(err.Error(), "secret") && !strings.Contains(err.Error(), "query") {
			t.Errorf("error %q should mention token/secret/query for URL %q", err, rawURL)
		}
	}
}

func TestNew_InvalidScheme(t *testing.T) {
	_, err := New("ftp://panel.example.com", testNodeID, testToken)
	if err == nil {
		t.Fatal("expected error for non-http(s) scheme")
	}
}

func TestNew_OptionWithHTTPClient(t *testing.T) {
	custom := &http.Client{Timeout: 5 * time.Second}
	c, err := New("https://panel.example.com", testNodeID, testToken, WithHTTPClient(custom))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.httpClient != custom {
		t.Error("custom HTTP client not set")
	}
}

// ---------------------------------------------------------------------------
// FetchConfiguration tests
// ---------------------------------------------------------------------------

func TestFetchConfiguration_Success(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validConfigurationResponse(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg, etag, err := c.FetchConfiguration(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Revision != "rev-1" {
		t.Errorf("revision = %q, want %q", cfg.Revision, "rev-1")
	}
	if etag != `"rev-1"` {
		t.Errorf("etag = %q, want %q", etag, `"rev-1"`)
	}
}

func TestFetchConfiguration_ExactPathAndMethod(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validConfigurationResponse(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedPath := "/api/v1/nodes/node-42/configuration"
	if h.path != expectedPath {
		t.Errorf("path = %q, want %q", h.path, expectedPath)
	}
	if h.method != http.MethodGet {
		t.Errorf("method = %q, want %q", h.method, http.MethodGet)
	}
}

func TestFetchConfiguration_Headers(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validConfigurationResponse(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), `"old-rev"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Authorization header
	auth := h.headers.Get("Authorization")
	if auth != "Bearer "+testToken {
		t.Errorf("Authorization = %q, want %q", auth, "Bearer "+testToken)
	}

	// Accept header
	accept := h.headers.Get("Accept")
	if accept != "application/json" {
		t.Errorf("Accept = %q, want %q", accept, "application/json")
	}

	// If-None-Match
	inm := h.headers.Get("If-None-Match")
	if inm != `"old-rev"` {
		t.Errorf("If-None-Match = %q, want %q", inm, `"old-rev"`)
	}

	// X-Request-ID should be set
	reqID := h.headers.Get("X-Request-ID")
	if reqID == "" {
		t.Error("X-Request-ID should be set")
	}
}

func TestFetchConfiguration_304NotModified(t *testing.T) {
	h := &captureHandler{
		statusCode: 304,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), `"rev-1"`)
	if err != ErrNotModified {
		t.Errorf("error = %v, want ErrNotModified", err)
	}
}

func TestFetchConfiguration_MissingNoStore(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validConfigurationResponse(),
		noStore:      false, // Missing Cache-Control: no-store
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err != ErrNoStore {
		t.Errorf("error = %v, want ErrNoStore", err)
	}
}

func TestFetchConfiguration_403NodeMismatch(t *testing.T) {
	h := &captureHandler{
		statusCode: 403,
		responseBody: &contract.ProblemDetails{
			Status:    403,
			Code:      "NODE_MISMATCH",
			Title:     "Node mismatch",
			RequestID: "req-123",
		},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}

	var respErr *ResponseError
	if !isResponseError(err, &respErr) {
		t.Fatalf("expected *ResponseError, got %T: %v", err, err)
	}
	if respErr.StatusCode != 403 {
		t.Errorf("StatusCode = %d, want 403", respErr.StatusCode)
	}
	if respErr.Code != "NODE_MISMATCH" {
		t.Errorf("Code = %q, want %q", respErr.Code, "NODE_MISMATCH")
	}
}

func TestFetchConfiguration_409RevisionConflict(t *testing.T) {
	h := &captureHandler{
		statusCode: 409,
		responseBody: &contract.ProblemDetails{
			Status:    409,
			Code:      "REVISION_CONFLICT",
			Title:     "Revision conflict",
			RequestID: "req-456",
		},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}

	var respErr *ResponseError
	if !isResponseError(err, &respErr) {
		t.Fatalf("expected *ResponseError, got %T: %v", err, err)
	}
	if respErr.Code != "REVISION_CONFLICT" {
		t.Errorf("Code = %q, want %q", respErr.Code, "REVISION_CONFLICT")
	}
}

func TestFetchConfiguration_429Retryable(t *testing.T) {
	h := &captureHandler{
		statusCode: 429,
		responseBody: &contract.ProblemDetails{
			Status: 429,
			Code:   "RATE_LIMITED",
			Title:  "Too many requests",
		},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if !IsRetryable(err) {
		t.Errorf("429 should be retryable, got err=%v, IsRetryable=%v", err, IsRetryable(err))
	}
}

func TestFetchConfiguration_503Retryable(t *testing.T) {
	h := &captureHandler{
		statusCode: 503,
		responseBody: &contract.ProblemDetails{
			Status: 503,
			Code:   "SERVICE_UNAVAILABLE",
			Title:  "Service unavailable",
		},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if !IsRetryable(err) {
		t.Errorf("503 should be retryable, got err=%v, IsRetryable=%v", err, IsRetryable(err))
	}
}

// ---------------------------------------------------------------------------
// FetchUsers tests
// ---------------------------------------------------------------------------

func TestFetchUsers_Success(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validUserSnapshot(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"user-rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	snap, etag, err := c.FetchUsers(context.Background(), "inb-1", "", "rev-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.Revision != "user-rev-1" {
		t.Errorf("revision = %q, want %q", snap.Revision, "user-rev-1")
	}
	if etag != `"user-rev-1"` {
		t.Errorf("etag = %q, want %q", etag, `"user-rev-1"`)
	}
}

func TestFetchUsers_ExactPathAndMethod(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validUserSnapshot(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"user-rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchUsers(context.Background(), "inb-1", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedPath := "/api/v1/nodes/node-42/inbounds/inb-1/users"
	if h.path != expectedPath {
		t.Errorf("path = %q, want %q", h.path, expectedPath)
	}
	if h.method != http.MethodGet {
		t.Errorf("method = %q, want %q", h.method, http.MethodGet)
	}
}

func TestFetchUsers_Headers(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validUserSnapshot(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"user-rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchUsers(context.Background(), "inb-1", `"old-rev"`, "applied-rev-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// If-None-Match
	inm := h.headers.Get("If-None-Match")
	if inm != `"old-rev"` {
		t.Errorf("If-None-Match = %q, want %q", inm, `"old-rev"`)
	}

	// X-Applied-Configuration-Revision
	appliedRev := h.headers.Get("X-Applied-Configuration-Revision")
	if appliedRev != "applied-rev-1" {
		t.Errorf("X-Applied-Configuration-Revision = %q, want %q", appliedRev, "applied-rev-1")
	}

	// Authorization
	auth := h.headers.Get("Authorization")
	if auth != "Bearer "+testToken {
		t.Errorf("Authorization = %q, want %q", auth, "Bearer "+testToken)
	}
}

func TestFetchUsers_304NotModified(t *testing.T) {
	h := &captureHandler{statusCode: 304}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchUsers(context.Background(), "inb-1", `"old"`, "")
	if err != ErrNotModified {
		t.Errorf("error = %v, want ErrNotModified", err)
	}
}

func TestFetchUsers_MissingNoStore(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validUserSnapshot(),
		noStore:      false,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchUsers(context.Background(), "inb-1", "", "")
	if err != ErrNoStore {
		t.Errorf("error = %v, want ErrNoStore", err)
	}
}

// ---------------------------------------------------------------------------
// ReportTraffic tests
// ---------------------------------------------------------------------------

func TestReportTraffic_Success(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusAccepted,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.ReportTraffic(context.Background(), validTrafficReport(), "idem-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReportTraffic_ExactPathAndMethod(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusAccepted,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.ReportTraffic(context.Background(), validTrafficReport(), "idem-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedPath := "/api/v1/nodes/node-42/traffic-reports"
	if h.path != expectedPath {
		t.Errorf("path = %q, want %q", h.path, expectedPath)
	}
	if h.method != http.MethodPost {
		t.Errorf("method = %q, want %q", h.method, http.MethodPost)
	}
}

func TestReportTraffic_Headers(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusAccepted,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.ReportTraffic(context.Background(), validTrafficReport(), "idem-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Content-Type
	ct := h.headers.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}

	// Idempotency-Key
	idemKey := h.headers.Get("Idempotency-Key")
	if idemKey != "idem-key-1" {
		t.Errorf("Idempotency-Key = %q, want %q", idemKey, "idem-key-1")
	}

	// X-Applied-Configuration-Revision from report.ConfigurationRevision
	appliedRev := h.headers.Get("X-Applied-Configuration-Revision")
	if appliedRev != "rev-1" {
		t.Errorf("X-Applied-Configuration-Revision = %q, want %q", appliedRev, "rev-1")
	}

	// Authorization
	auth := h.headers.Get("Authorization")
	if auth != "Bearer "+testToken {
		t.Errorf("Authorization = %q, want %q", auth, "Bearer "+testToken)
	}
}

func TestReportTraffic_409IdempotencyConflict(t *testing.T) {
	h := &captureHandler{
		statusCode: 409,
		responseBody: &contract.ProblemDetails{
			Status: 409,
			Code:   "IDEMPOTENCY_CONFLICT",
			Title:  "Idempotency conflict",
		},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.ReportTraffic(context.Background(), validTrafficReport(), "dup-key")
	if err == nil {
		t.Fatal("expected error")
	}

	var respErr *ResponseError
	if !isResponseError(err, &respErr) {
		t.Fatalf("expected *ResponseError, got %T: %v", err, err)
	}
	if respErr.Code != "IDEMPOTENCY_CONFLICT" {
		t.Errorf("Code = %q, want %q", respErr.Code, "IDEMPOTENCY_CONFLICT")
	}

	// 409 should be classified as conflict
	_, isConflict, _ := ClassifyError(err)
	if !isConflict {
		t.Error("409 IDEMPOTENCY_CONFLICT should be classified as conflict")
	}
}

func TestReportTraffic_BodyIsJSON(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusAccepted,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	report := validTrafficReport()
	err = c.ReportTraffic(context.Background(), report, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded contract.TrafficReport
	if err := json.Unmarshal(h.body, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
}

// ---------------------------------------------------------------------------
// SendHeartbeat tests
// ---------------------------------------------------------------------------

func TestSendHeartbeat_Success(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusOK,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.SendHeartbeat(context.Background(), validHeartbeat())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendHeartbeat_ExactPathAndMethod(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusOK,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.SendHeartbeat(context.Background(), validHeartbeat())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedPath := "/api/v1/nodes/node-42/heartbeats"
	if h.path != expectedPath {
		t.Errorf("path = %q, want %q", h.path, expectedPath)
	}
	if h.method != http.MethodPost {
		t.Errorf("method = %q, want %q", h.method, http.MethodPost)
	}
}

func TestSendHeartbeat_Headers(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusOK,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.SendHeartbeat(context.Background(), validHeartbeat())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ct := h.headers.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}

	auth := h.headers.Get("Authorization")
	if auth != "Bearer "+testToken {
		t.Errorf("Authorization = %q, want %q", auth, "Bearer "+testToken)
	}

	// X-Applied-Configuration-Revision should be set from heartbeat body
	appliedRev := h.headers.Get("X-Applied-Configuration-Revision")
	if appliedRev != "rev-1" {
		t.Errorf("X-Applied-Configuration-Revision = %q, want %q", appliedRev, "rev-1")
	}
}

// ---------------------------------------------------------------------------
// No compatibility paths test
// ---------------------------------------------------------------------------

func TestNoCompatibilityPaths(t *testing.T) {
	// Verify that all endpoints use only /api/v1/ paths — no V2Board/V2Ray style paths.
	c, err := New("https://panel.example.com", testNodeID, testToken)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	paths := []string{
		c.configurationURL(),
		c.usersURL("inb-1"),
		c.trafficURL(),
		c.heartbeatURL(),
	}

	for _, p := range paths {
		if !strings.Contains(p, "/api/v1/") {
			t.Errorf("path %q should contain /api/v1/", p)
		}
		// Should NOT contain any compatibility path segments
		bad := []string{"/v2board/", "/v2ray/", "/xrayr/", "/sspanel/", "/mod_mu/"}
		for _, b := range bad {
			if strings.Contains(strings.ToLower(p), b) {
				t.Errorf("path %q should not contain compatibility segment %q", p, b)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Error classification tests
// ---------------------------------------------------------------------------

func TestClassifyError_Nil(t *testing.T) {
	retryable, isConflict, isMismatch := ClassifyError(nil)
	if retryable || isConflict || isMismatch {
		t.Errorf("nil error should not be classified, got retryable=%v conflict=%v mismatch=%v", retryable, isConflict, isMismatch)
	}
}

func TestClassifyError_429(t *testing.T) {
	err := &ResponseError{StatusCode: 429, Code: "RATE_LIMITED", Title: "Too many requests"}
	retryable, isConflict, isMismatch := ClassifyError(err)
	if !retryable {
		t.Error("429 should be retryable")
	}
	if isConflict || isMismatch {
		t.Error("429 should not be conflict or mismatch")
	}
}

func TestClassifyError_503(t *testing.T) {
	err := &ResponseError{StatusCode: 503, Code: "SERVICE_UNAVAILABLE", Title: "Service unavailable"}
	retryable, _, _ := ClassifyError(err)
	if !retryable {
		t.Error("503 should be retryable")
	}
}

func TestClassifyError_409RevisionConflict(t *testing.T) {
	err := &ResponseError{StatusCode: 409, Code: "REVISION_CONFLICT", Title: "Revision conflict"}
	retryable, isConflict, isMismatch := ClassifyError(err)
	if retryable {
		t.Error("409 should not be retryable")
	}
	if !isConflict {
		t.Error("409 should be conflict")
	}
	if isMismatch {
		t.Error("409 should not be mismatch")
	}
}

func TestClassifyError_409IdempotencyConflict(t *testing.T) {
	err := &ResponseError{StatusCode: 409, Code: "IDEMPOTENCY_CONFLICT", Title: "Idempotency conflict"}
	_, isConflict, _ := ClassifyError(err)
	if !isConflict {
		t.Error("409 IDEMPOTENCY_CONFLICT should be conflict")
	}
}

func TestClassifyError_403NodeMismatch(t *testing.T) {
	err := &ResponseError{StatusCode: 403, Code: "NODE_MISMATCH", Title: "Node mismatch"}
	retryable, isConflict, isMismatch := ClassifyError(err)
	if retryable || isConflict {
		t.Error("403 NODE_MISMATCH should not be retryable or conflict")
	}
	if !isMismatch {
		t.Error("403 NODE_MISMATCH should be mismatch")
	}
}

func TestClassifyError_403OtherCode(t *testing.T) {
	err := &ResponseError{StatusCode: 403, Code: "FORBIDDEN", Title: "Forbidden"}
	_, _, isMismatch := ClassifyError(err)
	if isMismatch {
		t.Error("403 with non-NODE_MISMATCH code should not be mismatch")
	}
}

func TestClassifyError_500NotRetryable(t *testing.T) {
	err := &ResponseError{StatusCode: 500, Code: "INTERNAL_ERROR", Title: "Internal error"}
	retryable, _, _ := ClassifyError(err)
	if retryable {
		t.Error("500 should not be classified as retryable (only 429/503 are)")
	}
}

func TestClassifyError_ErrNotModifiedNotRetryable(t *testing.T) {
	retryable, isConflict, isMismatch := ClassifyError(ErrNotModified)
	if retryable || isConflict || isMismatch {
		t.Errorf("ErrNotModified should not be retryable/conflict/mismatch, got retryable=%v conflict=%v mismatch=%v", retryable, isConflict, isMismatch)
	}
}

func TestClassifyError_ErrNoStoreNotRetryable(t *testing.T) {
	retryable, isConflict, isMismatch := ClassifyError(ErrNoStore)
	if retryable || isConflict || isMismatch {
		t.Errorf("ErrNoStore should not be retryable/conflict/mismatch, got retryable=%v conflict=%v mismatch=%v", retryable, isConflict, isMismatch)
	}
}

// ---------------------------------------------------------------------------
// Transport error is retryable
// ---------------------------------------------------------------------------

func TestIsRetryable_TransportError(t *testing.T) {
	// Create a server that immediately closes connections
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Force connection close
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			conn.Close()
			return
		}
		w.WriteHeader(500)
	}))
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err == nil {
		t.Fatal("expected error from broken server")
	}

	// Transport errors should be retryable
	if !IsRetryable(err) {
		t.Errorf("transport error should be retryable, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Log redaction test
// ---------------------------------------------------------------------------

type logEntry struct {
	level   string
	message string
}

type captureLogger struct {
	entries []logEntry
}

func (l *captureLogger) Trace(args ...any) {
	l.entries = append(l.entries, logEntry{"trace", fmt.Sprint(args...)})
}
func (l *captureLogger) Debug(args ...any) {
	l.entries = append(l.entries, logEntry{"debug", fmt.Sprint(args...)})
}
func (l *captureLogger) Info(args ...any) {
	l.entries = append(l.entries, logEntry{"info", fmt.Sprint(args...)})
}
func (l *captureLogger) Warn(args ...any) {
	l.entries = append(l.entries, logEntry{"warn", fmt.Sprint(args...)})
}
func (l *captureLogger) Error(args ...any) {
	l.entries = append(l.entries, logEntry{"error", fmt.Sprint(args...)})
}
func (l *captureLogger) Fatal(args ...any) {
	l.entries = append(l.entries, logEntry{"fatal", fmt.Sprint(args...)})
}
func (l *captureLogger) Panic(args ...any) {
	l.entries = append(l.entries, logEntry{"panic", fmt.Sprint(args...)})
}
func (l *captureLogger) TraceContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"trace", fmt.Sprint(args...)})
}
func (l *captureLogger) DebugContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"debug", fmt.Sprint(args...)})
}
func (l *captureLogger) InfoContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"info", fmt.Sprint(args...)})
}
func (l *captureLogger) WarnContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"warn", fmt.Sprint(args...)})
}
func (l *captureLogger) ErrorContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"error", fmt.Sprint(args...)})
}
func (l *captureLogger) FatalContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"fatal", fmt.Sprint(args...)})
}
func (l *captureLogger) PanicContext(_ context.Context, args ...any) {
	l.entries = append(l.entries, logEntry{"panic", fmt.Sprint(args...)})
}

func TestLogsDoNotContainSecrets(t *testing.T) {
	logger := &captureLogger{}

	h := &captureHandler{
		statusCode:   200,
		responseBody: validConfigurationResponse(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := New(server.URL, testNodeID, testToken, WithLogger(logger))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check all log messages for secrets
	for _, entry := range logger.entries {
		if strings.Contains(entry.message, testToken) {
			t.Errorf("log message contains token: %q", entry.message)
		}
		if strings.Contains(entry.message, "Bearer "+testToken) {
			t.Errorf("log message contains Authorization header value: %q", entry.message)
		}
	}
}

func TestLogsDoNotContainUserPasswords(t *testing.T) {
	logger := &captureLogger{}

	password := "super-secret-password"
	snap := &contract.UserSnapshot{
		Revision:              "rev-1",
		ConfigurationRevision: "cfg-rev-1",
		NodeID:                testNodeID,
		InboundID:             "inb-1",
		Protocol:              "hysteria2",
		Users: []contract.User{
			{UserID: "u-1", Name: "alice", Credential: contract.Credential{Type: contract.CredentialTypePassword, Password: password}},
		},
	}

	h := &captureHandler{
		statusCode:   200,
		responseBody: snap,
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := New(server.URL, testNodeID, testToken, WithLogger(logger))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchUsers(context.Background(), "inb-1", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, entry := range logger.entries {
		if strings.Contains(entry.message, password) {
			t.Errorf("log message contains user password: %q", entry.message)
		}
	}
}

// ---------------------------------------------------------------------------
// Request body not logged at info level
// ---------------------------------------------------------------------------

func TestRequestBodyNotLoggedAtInfoLevel(t *testing.T) {
	logger := &captureLogger{}

	h := &captureHandler{
		statusCode: http.StatusAccepted,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := New(server.URL, testNodeID, testToken, WithLogger(logger))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.ReportTraffic(context.Background(), validTrafficReport(), "idem-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No info-level log should contain the request body
	for _, entry := range logger.entries {
		if entry.level == "info" {
			// The request body is a JSON payload — it should not appear in info logs
			if strings.Contains(entry.message, "started_at") || strings.Contains(entry.message, "upload_bytes") {
				t.Errorf("info log contains request body data: %q", entry.message)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// ResponseError details
// ---------------------------------------------------------------------------

func TestResponseError_Details(t *testing.T) {
	h := &captureHandler{
		statusCode: 422,
		responseBody: &contract.ProblemDetails{
			Status:    422,
			Code:      "VALIDATION_ERROR",
			Title:     "Validation failed",
			Detail:    "Field X is required",
			RequestID: "req-789",
		},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}

	var respErr *ResponseError
	if !isResponseError(err, &respErr) {
		t.Fatalf("expected *ResponseError, got %T: %v", err, err)
	}
	if respErr.StatusCode != 422 {
		t.Errorf("StatusCode = %d, want 422", respErr.StatusCode)
	}
	if respErr.Code != "VALIDATION_ERROR" {
		t.Errorf("Code = %q, want %q", respErr.Code, "VALIDATION_ERROR")
	}
	if respErr.Title != "Validation failed" {
		t.Errorf("Title = %q, want %q", respErr.Title, "Validation failed")
	}
	if respErr.Detail != "Field X is required" {
		t.Errorf("Detail = %q, want %q", respErr.Detail, "Field X is required")
	}
	if respErr.RequestID != "req-789" {
		t.Errorf("RequestID = %q, want %q", respErr.RequestID, "req-789")
	}
}

func TestResponseError_FallbackForNonJSON(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("internal server error"))
	})
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}

	var respErr *ResponseError
	if !isResponseError(err, &respErr) {
		t.Fatalf("expected *ResponseError, got %T: %v", err, err)
	}
	if respErr.StatusCode != 500 {
		t.Errorf("StatusCode = %d, want 500", respErr.StatusCode)
	}
	if respErr.Code != "HTTP_500" {
		t.Errorf("Code = %q, want %q", respErr.Code, "HTTP_500")
	}
}

// ---------------------------------------------------------------------------
// No query params in constructed URLs
// ---------------------------------------------------------------------------

func TestNoQueryParamsInURLs(t *testing.T) {
	h := &captureHandler{
		statusCode:   200,
		responseBody: validConfigurationResponse(),
		noStore:      true,
		extraHeaders: map[string]string{"ETag": `"rev-1"`},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = c.FetchConfiguration(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The request should have no query parameters (token must be in header only)
	if h.path != strings.Split(h.path, "?")[0] {
		t.Errorf("request path has query params: %q", h.path)
	}
}

// ---------------------------------------------------------------------------
// ETag round-trip
// ---------------------------------------------------------------------------

func TestFetchConfiguration_ETagRoundTrip(t *testing.T) {
	callCount := 0
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		ifNoneMatch := r.Header.Get("If-None-Match")
		if ifNoneMatch == `"rev-1"` {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("ETag", `"rev-1"`)
		w.WriteHeader(200)
		resp, _ := json.Marshal(validConfigurationResponse())
		_, _ = w.Write(resp)
	})
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First call — no ETag
	cfg, etag, err := c.FetchConfiguration(context.Background(), "")
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("first call: cfg is nil")
	}
	if etag != `"rev-1"` {
		t.Errorf("first call etag = %q, want %q", etag, `"rev-1"`)
	}

	// Second call — with ETag, should get 304
	_, _, err = c.FetchConfiguration(context.Background(), etag)
	if err != ErrNotModified {
		t.Errorf("second call: error = %v, want ErrNotModified", err)
	}

	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// No token in request body or query
// ---------------------------------------------------------------------------

func TestNoTokenInRequestBodyOrQuery(t *testing.T) {
	h := &captureHandler{
		statusCode: http.StatusAccepted,
	}
	server := httptest.NewServer(h)
	defer server.Close()

	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.ReportTraffic(context.Background(), validTrafficReport(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Request body should not contain the token
	if bytes.Contains(h.body, []byte(testToken)) {
		t.Error("request body contains token")
	}

	// URL should not contain the token in query
	if strings.Contains(h.path, testToken) {
		t.Error("request path contains token")
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

// isResponseError checks if err wraps a *ResponseError and populates target.
func isResponseError(err error, target **ResponseError) bool {
	return errors.As(err, target)
}
