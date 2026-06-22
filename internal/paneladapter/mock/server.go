// Package mock provides a reusable mock HTTP server for the sing-box panel
// adapter API v1. It implements all four REST endpoints with full spec
// compliance including ETag/If-None-Match, Cache-Control: no-store, bearer
// token validation, request transcription, and configurable responses.
//
// The mock does NOT import any sing-box runtime packages — only the shared
// contract types from internal/paneladapter/contract.
package mock

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

// ---------------------------------------------------------------------------
// RequestRecord — transcript entry
// ---------------------------------------------------------------------------

// RequestRecord stores a captured HTTP request for later inspection.
type RequestRecord struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

// ---------------------------------------------------------------------------
// ConfigHandler — custom handler for configuration endpoint
// ---------------------------------------------------------------------------

// ConfigHandler is a callback that receives the incoming request and the
// current stored configuration, and returns the configuration to serve along
// with an optional override ETag. If etagOverride is empty, auto-ETag is used.
// Return an HTTP status code of 0 to let the server use its default logic.
type ConfigHandler func(r *http.Request, cfg *contract.ConfigurationResponse) (resp *contract.ConfigurationResponse, etagOverride string, statusCode int)

// ---------------------------------------------------------------------------
// Server — mock panel HTTP server
// ---------------------------------------------------------------------------

// Server is a thread-safe mock panel HTTP server for testing the sing-box
// panel adapter client and runtime components.
type Server struct {
	mu sync.Mutex

	t      testing.TB
	server *httptest.Server

	// Identity
	nodeID string
	token  string

	// Configured responses
	configuration *contract.ConfigurationResponse
	userSnaps     map[string]*contract.UserSnapshot // keyed by inboundID

	// Status code overrides (0 = use default 200)
	configStatus    int
	usersStatus     int
	trafficStatus   int
	heartbeatStatus int

	// ETag auto-generation
	etagAuto bool

	// Custom config handler
	configHandler ConfigHandler

	// Transcript
	transcript []RequestRecord

	// Decoded payloads
	trafficReports []contract.TrafficReport
	heartbeats     []contract.Heartbeat
}

// ---------------------------------------------------------------------------
// Option — functional options for Server
// ---------------------------------------------------------------------------

// ServerOption configures a Server during construction.
type ServerOption func(*Server)

// WithNodeID sets the expected node ID (default: "default-node").
func WithNodeID(id string) ServerOption {
	return func(s *Server) {
		s.nodeID = id
	}
}

// WithToken sets the expected bearer token (default: "test-token").
func WithToken(token string) ServerOption {
	return func(s *Server) {
		s.token = token
	}
}

// WithConfigHandler installs a custom handler for the configuration endpoint.
// The handler receives the incoming request and the current stored configuration,
// and can return a modified response or status code.
func WithConfigHandler(fn ConfigHandler) ServerOption {
	return func(s *Server) {
		s.configHandler = fn
	}
}

// WithETagAuto enables or disables automatic ETag generation from response
// bodies. When enabled (default), the server computes a SHA-256 based ETag
// for GET 200 responses. When disabled, only explicitly set ETags are used.
func WithETagAuto(enabled bool) ServerOption {
	return func(s *Server) {
		s.etagAuto = enabled
	}
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

// New creates and starts a new mock panel server. The server registers a
// cleanup function on t to close the underlying httptest.Server.
func New(t testing.TB, opts ...ServerOption) *Server {
	s := &Server{
		t:          t,
		nodeID:     "default-node",
		token:      "test-token",
		userSnaps:  make(map[string]*contract.UserSnapshot),
		etagAuto:   true,
		transcript: make([]RequestRecord, 0),
	}

	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/nodes/", s.handleAPI)

	s.server = httptest.NewServer(mux)
	t.Cleanup(s.server.Close)

	return s
}

// URL returns the base URL of the mock server (e.g. "http://127.0.0.1:PORT").
func (s *Server) URL() string {
	return s.server.URL
}

// Close shuts down the mock server immediately.
func (s *Server) Close() {
	s.server.Close()
}

// ---------------------------------------------------------------------------
// Response setters
// ---------------------------------------------------------------------------

// SetConfiguration sets the configuration response to return on GET
// /api/v1/nodes/{nodeID}/configuration.
func (s *Server) SetConfiguration(cfg *contract.ConfigurationResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configuration = cfg
}

// SetUserSnapshot sets the user snapshot response for a specific inboundID
// on GET /api/v1/nodes/{nodeID}/inbounds/{inboundID}/users.
func (s *Server) SetUserSnapshot(inboundID string, snap *contract.UserSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userSnaps[inboundID] = snap
}

// SetConfigStatus sets the HTTP status code for the configuration endpoint.
// Pass 0 to restore the default (200).
func (s *Server) SetConfigStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configStatus = code
}

// SetUsersStatus sets the HTTP status code for the users endpoint.
// Pass 0 to restore the default (200).
func (s *Server) SetUsersStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usersStatus = code
}

// SetTrafficStatus sets the HTTP status code for the traffic-reports endpoint.
// Pass 0 to restore the default (200).
func (s *Server) SetTrafficStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trafficStatus = code
}

// SetHeartbeatStatus sets the HTTP status code for the heartbeats endpoint.
// Pass 0 to restore the default (200).
func (s *Server) SetHeartbeatStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatStatus = code
}

// ---------------------------------------------------------------------------
// Transcript & payload inspection
// ---------------------------------------------------------------------------

// Transcript returns a copy of all recorded requests in order.
func (s *Server) Transcript() []RequestRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RequestRecord, len(s.transcript))
	copy(out, s.transcript)
	return out
}

// TrafficReports returns a copy of all decoded traffic reports received by
// POST /api/v1/nodes/{nodeID}/traffic-reports.
func (s *Server) TrafficReports() []contract.TrafficReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contract.TrafficReport, len(s.trafficReports))
	copy(out, s.trafficReports)
	return out
}

// Heartbeats returns a copy of all decoded heartbeats received by
// POST /api/v1/nodes/{nodeID}/heartbeats.
func (s *Server) Heartbeats() []contract.Heartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contract.Heartbeat, len(s.heartbeats))
	copy(out, s.heartbeats)
	return out
}

// Reset clears the transcript, decoded traffic reports, and decoded
// heartbeats. It does not change configured responses or status codes.
func (s *Server) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transcript = s.transcript[:0]
	s.trafficReports = s.trafficReports[:0]
	s.heartbeats = s.heartbeats[:0]
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// recordRequest captures an incoming request into the transcript.
func (s *Server) recordRequest(r *http.Request, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transcript = append(s.transcript, RequestRecord{
		Method:  r.Method,
		Path:    r.URL.Path,
		Headers: r.Header.Clone(),
		Body:    body,
	})
}

// computeETag returns a deterministic ETag from the JSON encoding of v.
func computeETag(v interface{}) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return fmt.Sprintf(`"%x"`, h[:16])
}

// writeJSON serializes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"title":"Internal Server Error","status":500,"code":"INTERNAL"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

// writeProblemDetails writes an RFC 7807-like error response.
func writeProblemDetails(w http.ResponseWriter, code int, codeStr, title, detail, requestID string) {
	pd := contract.ProblemDetails{
		Type:      fmt.Sprintf("https://panel.adapter/errors/%s", codeStr),
		Title:     title,
		Status:    code,
		Code:      codeStr,
		Detail:    detail,
		RequestID: requestID,
	}
	writeJSON(w, code, pd)
}

// extractNodeID parses the nodeID from a URL path that starts with
// /api/v1/nodes/. Returns ("", false) if the path doesn't match.
//
// Expected patterns:
//   - /api/v1/nodes/{nodeID}/configuration
//   - /api/v1/nodes/{nodeID}/inbounds/{inboundID}/users
//   - /api/v1/nodes/{nodeID}/traffic-reports
//   - /api/v1/nodes/{nodeID}/heartbeats
func extractNodeID(path string) (string, bool) {
	const prefix = "/api/v1/nodes/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	rest := path[len(prefix):]
	idx := strings.Index(rest, "/")
	if idx < 0 {
		return rest, true
	}
	return rest[:idx], true
}

// ---------------------------------------------------------------------------
// Main API router
// ---------------------------------------------------------------------------

// handleAPI is the top-level handler for all /api/v1/nodes/ routes.
func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	// Read and capture the body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "BAD_REQUEST",
			"Failed to read request body", err.Error(), r.Header.Get("X-Request-ID"))
		return
	}
	r.Body.Close()

	// Record the request (ALL requests, including ones that will be rejected).
	s.recordRequest(r, body)

	// Validate bearer token.
	if !s.checkAuth(w, r) {
		return
	}

	// Route to the correct endpoint handler.
	path := r.URL.Path
	nodeID, ok := extractNodeID(path)
	if !ok {
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", "The requested resource does not exist",
			r.Header.Get("X-Request-ID"))
		return
	}

	// Strip the /api/v1/nodes/{nodeID} prefix to get the suffix.
	const nodePrefix = "/api/v1/nodes/"
	afterNode := path[len(nodePrefix)+len(nodeID):]

	switch {
	case afterNode == "/configuration" && r.Method == http.MethodGet:
		s.handleConfiguration(w, r, nodeID)

	case afterNode == "/traffic-reports" && r.Method == http.MethodPost:
		s.handleTraffic(w, r, nodeID, body)

	case afterNode == "/heartbeats" && r.Method == http.MethodPost:
		s.handleHeartbeat(w, r, nodeID, body)

	case strings.HasPrefix(afterNode, "/inbounds/") && strings.HasSuffix(afterNode, "/users") && r.Method == http.MethodGet:
		// Extract inboundID from /inbounds/{inboundID}/users
		inboundID := afterNode[len("/inbounds/") : len(afterNode)-len("/users")]
		s.handleUsers(w, r, nodeID, inboundID)

	default:
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", "The requested resource does not exist",
			r.Header.Get("X-Request-ID"))
	}
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

// checkAuth validates the Authorization: Bearer <token> header.
// Returns true if auth passes. On failure it writes the response and returns false.
func (s *Server) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	expectedToken := s.token
	expectedNodeID := s.nodeID
	s.mu.Unlock()

	auth := r.Header.Get("Authorization")
	if auth == "" {
		writeProblemDetails(w, http.StatusUnauthorized, "UNAUTHORIZED",
			"Unauthorized", "Missing Authorization header",
			r.Header.Get("X-Request-ID"))
		return false
	}

	if !strings.HasPrefix(auth, "Bearer ") {
		writeProblemDetails(w, http.StatusUnauthorized, "UNAUTHORIZED",
			"Unauthorized", "Invalid Authorization header format",
			r.Header.Get("X-Request-ID"))
		return false
	}

	provided := strings.TrimPrefix(auth, "Bearer ")
	if provided != expectedToken {
		writeProblemDetails(w, http.StatusForbidden, "NODE_MISMATCH",
			"Node Mismatch", "Token does not match the expected node",
			r.Header.Get("X-Request-ID"))
		return false
	}

	// Also verify the nodeID in the path matches.
	pathNodeID, ok := extractNodeID(r.URL.Path)
	if ok && pathNodeID != expectedNodeID {
		writeProblemDetails(w, http.StatusForbidden, "NODE_MISMATCH",
			"Node Mismatch", "Node ID in path does not match the expected node",
			r.Header.Get("X-Request-ID"))
		return false
	}

	return true
}

// ---------------------------------------------------------------------------
// GET /api/v1/nodes/{nodeID}/configuration
// ---------------------------------------------------------------------------

func (s *Server) handleConfiguration(w http.ResponseWriter, r *http.Request, nodeID string) {
	s.mu.Lock()
	cfg := s.configuration
	statusOverride := s.configStatus
	handler := s.configHandler
	s.mu.Unlock()

	// If a custom handler is installed, delegate to it.
	if handler != nil {
		resp, etagOverride, codeOverride := handler(r, cfg)
		if codeOverride > 0 {
			statusOverride = codeOverride
		}
		if resp != nil {
			cfg = resp
		}
		if etagOverride != "" {
			w.Header().Set("ETag", etagOverride)
		}
	}

	// Non-200 status override.
	if statusOverride > 0 && statusOverride != http.StatusOK {
		writeProblemDetails(w, statusOverride, statusString(statusOverride),
			http.StatusText(statusOverride), "", r.Header.Get("X-Request-ID"))
		return
	}

	if cfg == nil {
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", "No configuration has been set",
			r.Header.Get("X-Request-ID"))
		return
	}

	// Compute ETag.
	etag := ""
	if w.Header().Get("ETag") == "" && s.etagAuto {
		etag = computeETag(cfg)
		w.Header().Set("ETag", etag)
	} else if w.Header().Get("ETag") != "" {
		etag = w.Header().Get("ETag")
	}

	// Check If-None-Match.
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" && etag != "" && ifNoneMatch == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// 200 response.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, cfg)
}

// ---------------------------------------------------------------------------
// GET /api/v1/nodes/{nodeID}/inbounds/{inboundID}/users
// ---------------------------------------------------------------------------

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request, nodeID, inboundID string) {
	s.mu.Lock()
	snap := s.userSnaps[inboundID]
	statusOverride := s.usersStatus
	s.mu.Unlock()

	// Non-200 status override.
	if statusOverride > 0 && statusOverride != http.StatusOK {
		writeProblemDetails(w, statusOverride, statusString(statusOverride),
			http.StatusText(statusOverride), "", r.Header.Get("X-Request-ID"))
		return
	}

	if snap == nil {
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", fmt.Sprintf("No user snapshot for inbound %q", inboundID),
			r.Header.Get("X-Request-ID"))
		return
	}

	// Compute ETag.
	etag := ""
	if s.etagAuto {
		etag = computeETag(snap)
		w.Header().Set("ETag", etag)
	}

	// Check If-None-Match.
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" && etag != "" && ifNoneMatch == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// 200 response.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, snap)
}

// ---------------------------------------------------------------------------
// POST /api/v1/nodes/{nodeID}/traffic-reports
// ---------------------------------------------------------------------------

func (s *Server) handleTraffic(w http.ResponseWriter, r *http.Request, nodeID string, body []byte) {
	s.mu.Lock()
	statusOverride := s.trafficStatus
	s.mu.Unlock()

	// Non-200 status override.
	if statusOverride > 0 && statusOverride != http.StatusOK && statusOverride != http.StatusAccepted && statusOverride != http.StatusNoContent {
		writeProblemDetails(w, statusOverride, statusString(statusOverride),
			http.StatusText(statusOverride), "", r.Header.Get("X-Request-ID"))
		return
	}

	// Decode and validate.
	var report contract.TrafficReport
	if err := json.Unmarshal(body, &report); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "BAD_REQUEST",
			"Bad Request", fmt.Sprintf("Invalid JSON: %v", err),
			r.Header.Get("X-Request-ID"))
		return
	}
	if err := report.Validate(); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "VALIDATION_ERROR",
			"Validation Error", err.Error(),
			r.Header.Get("X-Request-ID"))
		return
	}

	// Store the decoded report.
	s.mu.Lock()
	s.trafficReports = append(s.trafficReports, report)
	s.mu.Unlock()

	// Success response.
	code := http.StatusOK
	if statusOverride > 0 {
		code = statusOverride
	}
	if code == http.StatusNoContent {
		w.WriteHeader(code)
		return
	}
	if code == http.StatusAccepted {
		w.WriteHeader(code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(`{"status":"ok"}`))
}

// ---------------------------------------------------------------------------
// POST /api/v1/nodes/{nodeID}/heartbeats
// ---------------------------------------------------------------------------

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request, nodeID string, body []byte) {
	s.mu.Lock()
	statusOverride := s.heartbeatStatus
	s.mu.Unlock()

	// Non-2xx status override.
	if statusOverride > 0 && statusOverride != http.StatusOK && statusOverride != http.StatusAccepted && statusOverride != http.StatusNoContent {
		writeProblemDetails(w, statusOverride, statusString(statusOverride),
			http.StatusText(statusOverride), "", r.Header.Get("X-Request-ID"))
		return
	}

	// Decode and validate.
	var hb contract.Heartbeat
	if err := json.Unmarshal(body, &hb); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "BAD_REQUEST",
			"Bad Request", fmt.Sprintf("Invalid JSON: %v", err),
			r.Header.Get("X-Request-ID"))
		return
	}
	if err := hb.Validate(); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "VALIDATION_ERROR",
			"Validation Error", err.Error(),
			r.Header.Get("X-Request-ID"))
		return
	}

	// Store the decoded heartbeat.
	s.mu.Lock()
	s.heartbeats = append(s.heartbeats, hb)
	s.mu.Unlock()

	// Success response.
	code := http.StatusOK
	if statusOverride > 0 {
		code = statusOverride
	}
	if code == http.StatusNoContent {
		w.WriteHeader(code)
		return
	}
	if code == http.StatusAccepted {
		w.WriteHeader(code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(`{"status":"ok"}`))
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// statusString returns a short uppercase string for common HTTP status codes,
// used as the ProblemDetails.Code field.
func statusString(code int) string {
	switch code {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "UNAUTHORIZED"
	case http.StatusForbidden:
		return "FORBIDDEN"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "CONFLICT"
	case http.StatusTooManyRequests:
		return "RATE_LIMITED"
	case http.StatusInternalServerError:
		return "INTERNAL_ERROR"
	case http.StatusServiceUnavailable:
		return "SERVICE_UNAVAILABLE"
	default:
		return fmt.Sprintf("HTTP_%d", code)
	}
}
