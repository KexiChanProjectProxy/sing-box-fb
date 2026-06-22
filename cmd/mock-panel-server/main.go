// Package main provides a standalone mock panel server binary that implements
// the sing-box panel adapter API v1. It serves all four REST endpoints with
// full spec compliance including ETag/If-None-Match, Cache-Control: no-store,
// Bearer token validation, RFC 7807 ProblemDetails error responses, and
// graceful shutdown.
//
// This server is intended for integration testing and development — it reads
// initial state from a JSON config file (or uses sensible defaults) and serves
// it via the panel adapter API.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

var (
	flagPort       = flag.Int("port", 8080, "HTTP listen port")
	flagNodeID     = flag.String("node-id", "default-node", "Expected node ID in API paths")
	flagToken      = flag.String("token", "test-token", "Expected Bearer token for auth")
	flagConfigFile = flag.String("config-file", "", "Path to JSON config file with initial state")
	flagPretty     = flag.Bool("pretty", false, "Pretty-print log output")
)

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

// serverState holds the in-memory state for the mock panel server.
type serverState struct {
	mu sync.RWMutex

	nodeID string
	token  string

	configuration *contract.ConfigurationResponse
	userSnaps     map[string]*contract.UserSnapshot // keyed by inboundID
}

// ---------------------------------------------------------------------------
// Config file format
// ---------------------------------------------------------------------------

// configFile is the JSON structure read from --config-file.
type configFile struct {
	Configuration *contract.ConfigurationResponse   `json:"configuration,omitempty"`
	UserSnapshots map[string]*contract.UserSnapshot `json:"user_snapshots,omitempty"`
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

func defaultConfiguration(nodeID string) *contract.ConfigurationResponse {
	return &contract.ConfigurationResponse{
		Revision:   "rev-default-001",
		APIVersion: "v1",
		NodeID:     nodeID,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRestartProcess,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         30,
			TrafficSeconds:       60,
			HeartbeatSeconds:     30,
		},
		ManagedInbounds: []contract.ManagedInbound{
			{
				InboundID:       "inbound-1",
				Tag:             "hysteria2-in",
				Protocol:        "hysteria2",
				UserResource:    "default",
				UserApplyPolicy: "replace",
			},
		},
		SingBoxConfigTemplate: json.RawMessage(`{"log":{"level":"info"},"inbounds":[{"type":"hysteria2","tag":"hysteria2-in","listen":"::","listen_port":443}]}`),
	}
}

func defaultUserSnapshot(nodeID, inboundID string) *contract.UserSnapshot {
	return &contract.UserSnapshot{
		Revision:              "users-rev-001",
		ConfigurationRevision: "rev-default-001",
		NodeID:                nodeID,
		InboundID:             inboundID,
		Protocol:              "hysteria2",
		Users: []contract.User{
			{
				UserID: "user-1",
				Name:   "test-user",
				Credential: contract.Credential{
					Type:     contract.CredentialTypePassword,
					Password: "test-password",
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// writePrettyJSON serializes v as indented JSON and writes it with the given status code.
func writePrettyJSON(w http.ResponseWriter, code int, v interface{}) {
	b, err := json.MarshalIndent(v, "", "  ")
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

// statusString returns a short uppercase string for common HTTP status codes.
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

// extractNodeID parses the nodeID from a URL path that starts with
// /api/v1/nodes/. Returns ("", false) if the path doesn't match.
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

// requestID extracts a request ID from headers, or generates one.
func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	return fmt.Sprintf("mock-%d", time.Now().UnixNano())
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

func logRequest(method, path, requestID string) {
	log.Printf("[%s] %s %s", requestID, method, path)
}

func logPayload(requestID string, label string, v interface{}) {
	if *flagPretty {
		b, _ := json.MarshalIndent(v, "", "  ")
		log.Printf("[%s] %s:\n%s", requestID, label, string(b))
	} else {
		b, _ := json.Marshal(v)
		log.Printf("[%s] %s: %s", requestID, label, string(b))
	}
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

// checkAuth validates the Authorization: Bearer <token> header and node ID.
// Returns true if auth passes. On failure it writes the response and returns false.
func (st *serverState) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	st.mu.RLock()
	expectedToken := st.token
	expectedNodeID := st.nodeID
	st.mu.RUnlock()

	rid := requestID(r)

	auth := r.Header.Get("Authorization")
	if auth == "" {
		writeProblemDetails(w, http.StatusUnauthorized, "UNAUTHORIZED",
			"Unauthorized", "Missing Authorization header", rid)
		return false
	}

	if !strings.HasPrefix(auth, "Bearer ") {
		writeProblemDetails(w, http.StatusUnauthorized, "UNAUTHORIZED",
			"Unauthorized", "Invalid Authorization header format", rid)
		return false
	}

	provided := strings.TrimPrefix(auth, "Bearer ")
	if provided != expectedToken {
		writeProblemDetails(w, http.StatusForbidden, "NODE_MISMATCH",
			"Node Mismatch", "Token does not match the expected node", rid)
		return false
	}

	// Verify the nodeID in the path matches.
	pathNodeID, ok := extractNodeID(r.URL.Path)
	if ok && pathNodeID != expectedNodeID {
		writeProblemDetails(w, http.StatusForbidden, "NODE_MISMATCH",
			"Node Mismatch", "Node ID in path does not match the expected node", rid)
		return false
	}

	return true
}

// ---------------------------------------------------------------------------
// API handlers
// ---------------------------------------------------------------------------

// handleAPI is the top-level handler for all /api/v1/nodes/ routes.
func (st *serverState) handleAPI(w http.ResponseWriter, r *http.Request) {
	rid := requestID(r)

	// Read the body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "BAD_REQUEST",
			"Failed to read request body", err.Error(), rid)
		return
	}
	r.Body.Close()

	// Log the request.
	logRequest(r.Method, r.URL.Path, rid)

	// Validate bearer token.
	if !st.checkAuth(w, r) {
		return
	}

	// Route to the correct endpoint handler.
	path := r.URL.Path
	nodeID, ok := extractNodeID(path)
	if !ok {
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", "The requested resource does not exist", rid)
		return
	}

	// Strip the /api/v1/nodes/{nodeID} prefix to get the suffix.
	const nodePrefix = "/api/v1/nodes/"
	afterNode := path[len(nodePrefix)+len(nodeID):]

	switch {
	case afterNode == "/configuration" && r.Method == http.MethodGet:
		st.handleConfiguration(w, r, nodeID, rid)

	case afterNode == "/traffic-reports" && r.Method == http.MethodPost:
		st.handleTraffic(w, r, nodeID, body, rid)

	case afterNode == "/heartbeats" && r.Method == http.MethodPost:
		st.handleHeartbeat(w, r, nodeID, body, rid)

	case strings.HasPrefix(afterNode, "/inbounds/") && strings.HasSuffix(afterNode, "/users") && r.Method == http.MethodGet:
		inboundID := afterNode[len("/inbounds/") : len(afterNode)-len("/users")]
		st.handleUsers(w, r, nodeID, inboundID, rid)

	default:
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", "The requested resource does not exist", rid)
	}
}

// handleConfiguration handles GET /api/v1/nodes/{nodeID}/configuration.
func (st *serverState) handleConfiguration(w http.ResponseWriter, r *http.Request, nodeID, rid string) {
	st.mu.RLock()
	cfg := st.configuration
	st.mu.RUnlock()

	if cfg == nil {
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", "No configuration has been set", rid)
		return
	}

	// Compute ETag.
	etag := computeETag(cfg)
	w.Header().Set("ETag", etag)

	// Check If-None-Match.
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" && ifNoneMatch == etag {
		log.Printf("[%s] 304 Not Modified (ETag match)", rid)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// 200 response.
	w.Header().Set("Cache-Control", "no-store")
	logPayload(rid, "ConfigurationResponse", cfg)
	writeJSON(w, http.StatusOK, cfg)
}

// handleUsers handles GET /api/v1/nodes/{nodeID}/inbounds/{inboundID}/users.
func (st *serverState) handleUsers(w http.ResponseWriter, r *http.Request, nodeID, inboundID, rid string) {
	st.mu.RLock()
	snap := st.userSnaps[inboundID]
	st.mu.RUnlock()

	if snap == nil {
		writeProblemDetails(w, http.StatusNotFound, "NOT_FOUND",
			"Not Found", fmt.Sprintf("No user snapshot for inbound %q", inboundID), rid)
		return
	}

	// Compute ETag.
	etag := computeETag(snap)
	w.Header().Set("ETag", etag)

	// Check If-None-Match.
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" && ifNoneMatch == etag {
		log.Printf("[%s] 304 Not Modified (ETag match)", rid)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// 200 response.
	w.Header().Set("Cache-Control", "no-store")
	logPayload(rid, fmt.Sprintf("UserSnapshot[inbound=%s]", inboundID), snap)
	writeJSON(w, http.StatusOK, snap)
}

// handleTraffic handles POST /api/v1/nodes/{nodeID}/traffic-reports.
func (st *serverState) handleTraffic(w http.ResponseWriter, r *http.Request, nodeID string, body []byte, rid string) {
	var report contract.TrafficReport
	if err := json.Unmarshal(body, &report); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "BAD_REQUEST",
			"Bad Request", fmt.Sprintf("Invalid JSON: %v", err), rid)
		return
	}
	if err := report.Validate(); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "VALIDATION_ERROR",
			"Validation Error", err.Error(), rid)
		return
	}

	logPayload(rid, "TrafficReport", report)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// handleHeartbeat handles POST /api/v1/nodes/{nodeID}/heartbeats.
func (st *serverState) handleHeartbeat(w http.ResponseWriter, r *http.Request, nodeID string, body []byte, rid string) {
	var hb contract.Heartbeat
	if err := json.Unmarshal(body, &hb); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "BAD_REQUEST",
			"Bad Request", fmt.Sprintf("Invalid JSON: %v", err), rid)
		return
	}
	if err := hb.Validate(); err != nil {
		writeProblemDetails(w, http.StatusBadRequest, "VALIDATION_ERROR",
			"Validation Error", err.Error(), rid)
		return
	}

	logPayload(rid, "Heartbeat", hb)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	st := &serverState{
		nodeID:    *flagNodeID,
		token:     *flagToken,
		userSnaps: make(map[string]*contract.UserSnapshot),
	}

	// Load config file if provided.
	if *flagConfigFile != "" {
		data, err := os.ReadFile(*flagConfigFile)
		if err != nil {
			log.Fatalf("Failed to read config file %q: %v", *flagConfigFile, err)
		}
		var cf configFile
		if err := json.Unmarshal(data, &cf); err != nil {
			log.Fatalf("Failed to parse config file %q: %v", *flagConfigFile, err)
		}
		if cf.Configuration != nil {
			st.configuration = cf.Configuration
		}
		for id, snap := range cf.UserSnapshots {
			st.userSnaps[id] = snap
		}
		log.Printf("Loaded state from %q", *flagConfigFile)
	}

	// Apply defaults for anything not loaded from config file.
	if st.configuration == nil {
		st.configuration = defaultConfiguration(st.nodeID)
	}
	// Ensure user snapshots exist for all managed inbounds.
	for _, ib := range st.configuration.ManagedInbounds {
		if _, exists := st.userSnaps[ib.InboundID]; !exists {
			st.userSnaps[ib.InboundID] = defaultUserSnapshot(st.nodeID, ib.InboundID)
		}
	}

	// Set up HTTP mux.
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/nodes/", st.handleAPI)

	addr := fmt.Sprintf(":%d", *flagPort)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-stop
		log.Printf("Received signal %v, shutting down gracefully...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
	}()

	log.Printf("Mock panel server listening on %s with nodeID=%s", addr, st.nodeID)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
	log.Println("Server stopped.")
}
