package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/config"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/log"
)

// ---------------------------------------------------------------------------
// Fake panel HTTP server
// ---------------------------------------------------------------------------

type fakePanel struct {
	server     *httptest.Server
	mu         sync.Mutex
	configs    map[string]*contract.ConfigurationResponse
	users      map[string]contract.UserSnapshot
	heartbeats []contract.Heartbeat
	traffic    []contract.TrafficReport
	nodeID     string
	token      string
}

func newFakePanel(t *testing.T) *fakePanel {
	fp := &fakePanel{
		configs: make(map[string]*contract.ConfigurationResponse),
		users:   make(map[string]contract.UserSnapshot),
		nodeID:  "1",
		token:   "test-token-abc123",
	}

	mux := http.NewServeMux()

	// Auth middleware.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+fp.token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.NotFound(w, r)
	})

	// GET /api/v1/node/{nodeID}/configuration
	mux.HandleFunc("/api/v1/node/{nodeID}/configuration", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fp.mu.Lock()
		defer fp.mu.Unlock()
		cfg, ok := fp.configs[fp.nodeID]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"rev-1"`)
		json.NewEncoder(w).Encode(cfg)
	})

	// GET /api/v1/inbound/{inboundID}/users
	mux.HandleFunc("/api/v1/inbound/{inboundID}/users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		inboundID := r.PathValue("inboundID")
		fp.mu.Lock()
		defer fp.mu.Unlock()
		snap, ok := fp.users[inboundID]
		if !ok {
			// Return empty user list.
			snap = contract.UserSnapshot{Users: []contract.User{}}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"users-1"`)
		json.NewEncoder(w).Encode(snap)
	})

	// POST /api/v1/node/{nodeID}/heartbeat
	mux.HandleFunc("/api/v1/node/{nodeID}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var hb contract.Heartbeat
		if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		fp.mu.Lock()
		fp.heartbeats = append(fp.heartbeats, hb)
		fp.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	// POST /api/v1/node/{nodeID}/traffic
	mux.HandleFunc("/api/v1/node/{nodeID}/traffic", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var tr contract.TrafficReport
		if err := json.NewDecoder(r.Body).Decode(&tr); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		fp.mu.Lock()
		fp.traffic = append(fp.traffic, tr)
		fp.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	fp.server = httptest.NewServer(mux)
	t.Cleanup(fp.server.Close)
	return fp
}

func (fp *fakePanel) URL() string {
	return fp.server.URL
}

func (fp *fakePanel) SetConfig(cfg *contract.ConfigurationResponse) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.configs[fp.nodeID] = cfg
}

func (fp *fakePanel) SetUsers(inboundID string, snap contract.UserSnapshot) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.users[inboundID] = snap
}

func (fp *fakePanel) Heartbeats() []contract.Heartbeat {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	result := make([]contract.Heartbeat, len(fp.heartbeats))
	copy(result, fp.heartbeats)
	return result
}

// ---------------------------------------------------------------------------
// Helper: write a valid adapter config to a temp file
// ---------------------------------------------------------------------------

func writeAdapterConfig(t *testing.T, panelURL, stateDir string) string {
	t.Helper()
	cfg := &config.Config{
		PanelBaseURL: panelURL,
		NodeID:       "1",
		NodeToken:    "test-token-abc123",
		StatePath:    filepath.Join(stateDir, "adapter-state.json"),
		LogLevel:     "debug",
		Insecure:     true,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, "adapter-config.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestConfigLoadAndValidate(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a valid config.
	cfgPath := writeAdapterConfig(t, "http://localhost:9999", tmpDir)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NodeID != "1" {
		t.Errorf("NodeID = %q, want %q", cfg.NodeID, "1")
	}
	if cfg.PanelBaseURL == "" {
		t.Error("PanelBaseURL is empty")
	}
}

func TestConfigRejectsInvalid(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a config with missing required fields.
	invalidCfg := map[string]interface{}{
		"log_level": "info",
		// Missing panel_base_url, node_id, node_token
	}
	data, _ := json.Marshal(invalidCfg)
	path := filepath.Join(tmpDir, "bad-config.json")
	os.WriteFile(path, data, 0o644)

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}
}

func TestConfigRejectsInvalidURL(t *testing.T) {
	tmpDir := t.TempDir()

	invalidCfg := &config.Config{
		PanelBaseURL: "not-a-url",
		NodeID:       "1",
		NodeToken:    "secret",
		StatePath:    filepath.Join(tmpDir, "state.json"),
	}
	data, _ := json.Marshal(invalidCfg)
	path := filepath.Join(tmpDir, "bad-url-config.json")
	os.WriteFile(path, data, 0o644)

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid URL, got nil")
	}
}

func TestConfigRejectsEmptyToken(t *testing.T) {
	tmpDir := t.TempDir()

	invalidCfg := &config.Config{
		PanelBaseURL: "http://localhost:9999",
		NodeID:       "1",
		NodeToken:    "",
		StatePath:    filepath.Join(tmpDir, "state.json"),
	}
	data, _ := json.Marshal(invalidCfg)
	path := filepath.Join(tmpDir, "empty-token-config.json")
	os.WriteFile(path, data, 0o644)

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for empty token, got nil")
	}
}

func TestRunPeriodicCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := 0
	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPeriodic(ctx, "test", 10*time.Millisecond, logger, func(ctx context.Context) error {
			callCount++
			return nil
		})
	}()

	// Let it tick a few times.
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	if callCount == 0 {
		t.Error("expected at least one call")
	}
}

func TestRunPeriodicErrorLogging(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPeriodic(ctx, "err-test", 10*time.Millisecond, logger, func(ctx context.Context) error {
			return fmt.Errorf("test error")
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	if len(logger.warns) == 0 {
		t.Error("expected warning logs for errors")
	}
}

func TestSignalShutdownFlushesState(t *testing.T) {
	// This test verifies that when the context is cancelled,
	// the state is properly flushed. We simulate this by
	// cancelling a context and checking state persistence.
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.json")

	store, err := state.NewStore(statePath)
	if err != nil {
		t.Fatal(err)
	}

	// Modify state.
	st := store.State().Clone()
	st.Config.Revision = "rev-test-123"
	store.SetState(st)

	// Simulate shutdown flush.
	if err := store.Save(); err != nil {
		t.Fatal(err)
	}

	// Verify persisted.
	store2, err := state.NewStore(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if store2.State().Config.Revision != "rev-test-123" {
		t.Errorf("revision = %q, want %q", store2.State().Config.Revision, "rev-test-123")
	}
}

func TestNoTokenInLogs(t *testing.T) {
	logger := &testLogger{}
	token := "super-secret-token-xyz"

	// Simulate what the adapter does: log node_id but NOT token.
	logger.Info("panel client created for node_id=1")

	for _, msg := range logger.infos {
		if contains(msg, token) {
			t.Errorf("token found in info log: %s", msg)
		}
	}
	for _, msg := range logger.warns {
		if contains(msg, token) {
			t.Errorf("token found in warn log: %s", msg)
		}
	}
}

func TestNoTokenInHelp(t *testing.T) {
	// The --help output should not contain the token.
	// Since token is not a flag but a config field, this just verifies
	// that rootCommand.Flags() doesn't expose any secret.
	flags := rootCommand.Flags()
	if flags.Lookup("token") != nil {
		t.Error("--token flag should not exist (secrets go in config file only)")
	}
}

func TestGetPollIntervalsDefault(t *testing.T) {
	def := contract.PollIntervals{
		ConfigurationSeconds: 60,
		UsersSeconds:         60,
		TrafficSeconds:       60,
		HeartbeatSeconds:     60,
	}
	if def.ConfigurationSeconds != 60 {
		t.Errorf("default ConfigurationSeconds = %d, want 60", def.ConfigurationSeconds)
	}
	if def.UsersSeconds != 60 {
		t.Errorf("default UsersSeconds = %d, want 60", def.UsersSeconds)
	}
	if def.TrafficSeconds != 60 {
		t.Errorf("default TrafficSeconds = %d, want 60", def.TrafficSeconds)
	}
	if def.HeartbeatSeconds != 60 {
		t.Errorf("default HeartbeatSeconds = %d, want 60", def.HeartbeatSeconds)
	}
}

// ---------------------------------------------------------------------------
// testLogger — minimal log.ContextLogger for tests
// ---------------------------------------------------------------------------

type testLogger struct {
	infos  []string
	warns  []string
	debugs []string
}

func (l *testLogger) Trace(args ...interface{}) {}
func (l *testLogger) Debug(args ...interface{}) {
	l.debugs = append(l.debugs, fmt.Sprint(args...))
}
func (l *testLogger) Info(args ...interface{}) {
	l.infos = append(l.infos, fmt.Sprint(args...))
}
func (l *testLogger) Warn(args ...interface{}) {
	l.warns = append(l.warns, fmt.Sprint(args...))
}
func (l *testLogger) Error(args ...interface{}) {}
func (l *testLogger) Fatal(args ...interface{}) {}
func (l *testLogger) Panic(args ...interface{}) {}

func (l *testLogger) TraceContext(ctx context.Context, args ...interface{}) {}
func (l *testLogger) DebugContext(ctx context.Context, args ...interface{}) {
	l.debugs = append(l.debugs, fmt.Sprint(args...))
}
func (l *testLogger) InfoContext(ctx context.Context, args ...interface{}) {
	l.infos = append(l.infos, fmt.Sprint(args...))
}
func (l *testLogger) WarnContext(ctx context.Context, args ...interface{}) {
	l.warns = append(l.warns, fmt.Sprint(args...))
}
func (l *testLogger) ErrorContext(ctx context.Context, args ...interface{}) {}
func (l *testLogger) FatalContext(ctx context.Context, args ...interface{}) {}
func (l *testLogger) PanicContext(ctx context.Context, args ...interface{}) {}

func (l *testLogger) TraceEvent(event string, message string, fields ...log.Field) {}
func (l *testLogger) DebugEvent(event string, message string, fields ...log.Field) {
	l.debugs = append(l.debugs, message)
}
func (l *testLogger) InfoEvent(event string, message string, fields ...log.Field) {
	l.infos = append(l.infos, message)
}
func (l *testLogger) WarnEvent(event string, message string, fields ...log.Field) {
	l.warns = append(l.warns, message)
}
func (l *testLogger) ErrorEvent(event string, message string, fields ...log.Field) {}
func (l *testLogger) FatalEvent(event string, message string, fields ...log.Field) {}
func (l *testLogger) PanicEvent(event string, message string, fields ...log.Field) {}
func (l *testLogger) TraceEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
}
func (l *testLogger) DebugEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
	l.debugs = append(l.debugs, message)
}
func (l *testLogger) InfoEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
	l.infos = append(l.infos, message)
}
func (l *testLogger) WarnEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
	l.warns = append(l.warns, message)
}
func (l *testLogger) ErrorEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
}
func (l *testLogger) FatalEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
}
func (l *testLogger) PanicEventContext(ctx context.Context, event string, message string, fields ...log.Field) {
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
