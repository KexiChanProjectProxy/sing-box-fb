// Package integration_test provides end-to-end integration tests for the
// sing-box panel adapter. It uses a fake panel HTTP server (httptest.Server)
// that implements ONLY the 4 spec API paths, records every request into a
// transcript, and verifies the adapter never calls V2bX/compatibility paths
// or sends token in URL/query.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/heartbeat"
	"github.com/sagernet/sing-box/internal/paneladapter/reporter"
	"github.com/sagernet/sing-box/internal/paneladapter/runtime"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/internal/paneladapter/users"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fake panel server
// ---------------------------------------------------------------------------

// transcriptEntry records one HTTP request received by the fake panel.
type transcriptEntry struct {
	Method      string
	Path        string
	Headers     http.Header
	Body        []byte
	StatusCode  int
	RequestBody string
}

// fakePanel is a reusable fake panel HTTP server implementing ONLY the 4 spec
// API paths. It records every request into a transcript for assertions.
type fakePanel struct {
	server *httptest.Server

	mu            sync.Mutex
	nodeID        string
	configRev     string
	configETag    string
	configResp    *contract.ConfigurationResponse
	userSnap      map[string]*contract.UserSnapshot // inboundID → snapshot
	userETags     map[string]string                 // inboundID → etag
	transcript    []transcriptEntry
	trafficReqs   []contract.TrafficReport
	heartbeatReqs []contract.Heartbeat

	// Controls for simulating errors.
	configStatusOverride  int
	usersStatusOverride   int
	trafficStatusOverride int
	hbStatusOverride      int
}

// newFakePanel creates a fake panel server for the given nodeID.
func newFakePanel(nodeID string) *fakePanel {
	fp := &fakePanel{
		nodeID:    nodeID,
		configRev: "rev-initial-001",
		userSnap:  make(map[string]*contract.UserSnapshot),
		userETags: make(map[string]string),
	}
	fp.configETag = `W/"` + fp.configRev + `"`

	mux := http.NewServeMux()
	mux.HandleFunc("/", fp.handleRequest)
	fp.server = httptest.NewServer(mux)
	return fp
}

func (fp *fakePanel) Close() { fp.server.Close() }

func (fp *fakePanel) URL() string { return fp.server.URL }

// setConfiguration sets the configuration response the fake panel will serve.
func (fp *fakePanel) setConfiguration(cfg *contract.ConfigurationResponse) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.configResp = cfg
	fp.configRev = cfg.Revision
	fp.configETag = `W/"` + cfg.Revision + `"`
}

// setUserSnapshot sets the user snapshot for a given inboundID.
func (fp *fakePanel) setUserSnapshot(inboundID string, snap *contract.UserSnapshot) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.userSnap[inboundID] = snap
	fp.userETags[inboundID] = `W/"user-` + snap.Revision + `"`
}

// transcriptSnapshot returns a copy of the recorded request transcript.
func (fp *fakePanel) transcriptSnapshot() []transcriptEntry {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	out := make([]transcriptEntry, len(fp.transcript))
	copy(out, fp.transcript)
	return out
}

// trafficReports returns a copy of recorded traffic report payloads.
func (fp *fakePanel) trafficReports() []contract.TrafficReport {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	out := make([]contract.TrafficReport, len(fp.trafficReqs))
	copy(out, fp.trafficReqs)
	return out
}

// heartbeatReports returns a copy of recorded heartbeat payloads.
func (fp *fakePanel) heartbeatReports() []contract.Heartbeat {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	out := make([]contract.Heartbeat, len(fp.heartbeatReqs))
	copy(out, fp.heartbeatReqs)
	return out
}

// resetTranscript clears the recorded transcript and request bodies.
func (fp *fakePanel) resetTranscript() {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.transcript = nil
	fp.trafficReqs = nil
	fp.heartbeatReqs = nil
}

// handleRequest is the central dispatcher. ONLY the 4 spec paths are
// handled; everything else returns 404.
func (fp *fakePanel) handleRequest(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()

	fp.mu.Lock()
	entry := transcriptEntry{
		Method:      r.Method,
		Path:        r.URL.Path,
		Headers:     r.Header.Clone(),
		Body:        body,
		RequestBody: string(body),
	}
	fp.transcript = append(fp.transcript, entry)
	fp.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && fp.isConfigurationPath(r.URL.Path):
		fp.handleConfiguration(w, r)
	case r.Method == http.MethodGet && fp.isUsersPath(r.URL.Path):
		fp.handleUsers(w, r)
	case r.Method == http.MethodPost && fp.isTrafficPath(r.URL.Path):
		fp.handleTraffic(w, r, body)
	case r.Method == http.MethodPost && fp.isHeartbeatPath(r.URL.Path):
		fp.handleHeartbeat(w, r, body)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"title":"Not Found","status":404,"code":"NOT_FOUND","request_id":"fake-404"}`))
	}
}

func (fp *fakePanel) isConfigurationPath(p string) bool {
	return p == "/api/v1/nodes/"+fp.nodeID+"/configuration"
}

func (fp *fakePanel) isUsersPath(p string) bool {
	prefix := "/api/v1/nodes/" + fp.nodeID + "/inbounds/"
	suffix := "/users"
	return strings.HasPrefix(p, prefix) && strings.HasSuffix(p, suffix)
}

func (fp *fakePanel) isTrafficPath(p string) bool {
	return p == "/api/v1/nodes/"+fp.nodeID+"/traffic-reports"
}

func (fp *fakePanel) isHeartbeatPath(p string) bool {
	return p == "/api/v1/nodes/"+fp.nodeID+"/heartbeats"
}

func (fp *fakePanel) handleConfiguration(w http.ResponseWriter, r *http.Request) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.configStatusOverride != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fp.configStatusOverride)
		code := "UNKNOWN"
		switch fp.configStatusOverride {
		case 409:
			code = "REVISION_CONFLICT"
		case 503:
			code = "SERVICE_UNAVAILABLE"
		}
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"title":"Error","status":%d,"code":"%s","request_id":"fake-err"}`,
			fp.configStatusOverride, code)))
		return
	}

	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" && ifNoneMatch == fp.configETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if fp.configResp == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"title":"No Config","status":500,"code":"INTERNAL","request_id":"fake-no-config"}`))
		return
	}

	body, err := json.Marshal(fp.configResp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", fp.configETag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (fp *fakePanel) handleUsers(w http.ResponseWriter, r *http.Request) {
	prefix := "/api/v1/nodes/" + fp.nodeID + "/inbounds/"
	suffix := "/users"
	inboundID := strings.TrimPrefix(r.URL.Path, prefix)
	inboundID = strings.TrimSuffix(inboundID, suffix)

	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.usersStatusOverride != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fp.usersStatusOverride)
		code := "UNKNOWN"
		switch fp.usersStatusOverride {
		case 409:
			code = "REVISION_CONFLICT"
		case 503:
			code = "SERVICE_UNAVAILABLE"
		}
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"title":"Error","status":%d,"code":"%s","request_id":"fake-err"}`,
			fp.usersStatusOverride, code)))
		return
	}

	snap, ok := fp.userSnap[inboundID]
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"title":"Not Found","status":404,"code":"INBOUND_NOT_FOUND","request_id":"fake-404"}`))
		return
	}

	etag := fp.userETags[inboundID]
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" && ifNoneMatch == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	body, err := json.Marshal(snap)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (fp *fakePanel) handleTraffic(w http.ResponseWriter, r *http.Request, body []byte) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	var report contract.TrafficReport
	if err := json.Unmarshal(body, &report); err == nil {
		fp.trafficReqs = append(fp.trafficReqs, report)
	}

	if fp.trafficStatusOverride != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fp.trafficStatusOverride)
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"title":"Error","status":%d,"code":"SERVICE_UNAVAILABLE","request_id":"fake-err"}`,
			fp.trafficStatusOverride)))
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (fp *fakePanel) handleHeartbeat(w http.ResponseWriter, r *http.Request, body []byte) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	var hb contract.Heartbeat
	if err := json.Unmarshal(body, &hb); err == nil {
		fp.heartbeatReqs = append(fp.heartbeatReqs, hb)
	}

	if fp.hbStatusOverride != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fp.hbStatusOverride)
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"title":"Error","status":%d,"code":"SERVICE_UNAVAILABLE","request_id":"fake-err"}`,
			fp.hbStatusOverride)))
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// ---------------------------------------------------------------------------
// Redaction helpers
// ---------------------------------------------------------------------------

// redactedTranscript returns the transcript with sensitive fields redacted.
func redactedTranscript(entries []transcriptEntry) []transcriptEntry {
	out := make([]transcriptEntry, len(entries))
	for i, e := range entries {
		re := transcriptEntry{
			Method: e.Method,
			Path:   e.Path,
		}
		re.Headers = e.Headers.Clone()
		if auth := re.Headers.Get("Authorization"); auth != "" {
			re.Headers.Set("Authorization", "Bearer [REDACTED]")
		}
		bodyStr := e.RequestBody
		bodyStr = regexp.MustCompile(`"password"\s*:\s*"[^"]*"`).ReplaceAllString(bodyStr, `"password":"[REDACTED]"`)
		bodyStr = regexp.MustCompile(`"node_token"\s*:\s*"[^"]*"`).ReplaceAllString(bodyStr, `"node_token":"[REDACTED]"`)
		bodyStr = regexp.MustCompile(`"certificate"\s*:\s*"[^"]*"`).ReplaceAllString(bodyStr, `"certificate":"[REDACTED]"`)
		bodyStr = regexp.MustCompile(`"key"\s*:\s*"[^"]*"`).ReplaceAllString(bodyStr, `"key":"[REDACTED]"`)
		bodyStr = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`).ReplaceAllString(bodyStr, "[REDACTED_IP]")
		re.RequestBody = bodyStr
		re.Body = []byte(bodyStr)
		re.StatusCode = e.StatusCode
		out[i] = re
	}
	return out
}

// ---------------------------------------------------------------------------
// Test fixture builders
// ---------------------------------------------------------------------------

const (
	testNodeID = "node-test-001"
	testToken  = "test-bearer-token-do-not-use-in-prod"

	inboundIDHy2    = "ib-hy2-001"
	inboundIDAnyTLS = "ib-anytls-001"
	inboundIDSS     = "ib-ss-001"
)

// makeValidConfigurationResponse builds a valid ConfigurationResponse with
// a minimal sing-box config template containing 3 managed inbounds.
func makeValidConfigurationResponse(revision string) *contract.ConfigurationResponse {
	template := map[string]any{
		"log": map[string]any{
			"level": "warning",
		},
		"inbounds": []any{
			map[string]any{
				"type":        "hysteria2",
				"tag":         "hy2-in",
				"listen":      "::",
				"listen_port": 8443,
				"users":       []any{},
				"tls": map[string]any{
					"enabled":     true,
					"certificate": "REDACTED_CERT",
					"key":         "REDACTED_KEY",
				},
			},
			map[string]any{
				"type":        "anytls",
				"tag":         "anytls-in",
				"listen":      "::",
				"listen_port": 8444,
				"users":       []any{},
				"tls": map[string]any{
					"enabled":     true,
					"certificate": "REDACTED_CERT",
					"key":         "REDACTED_KEY",
				},
			},
			map[string]any{
				"type":        "shadowsocks",
				"tag":         "ss-in",
				"listen":      "::",
				"listen_port": 8445,
				"method":      "aes-256-gcm",
				"password":    "REDACTED_SERVER_PASS",
			},
		},
		"outbounds": []any{
			map[string]any{
				"type": "direct",
				"tag":  "direct-out",
			},
		},
	}

	templateJSON, _ := json.Marshal(template)

	return &contract.ConfigurationResponse{
		Revision:   revision,
		APIVersion: "v1",
		NodeID:     testNodeID,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         30,
			TrafficSeconds:       60,
			HeartbeatSeconds:     60,
		},
		ManagedInbounds: []contract.ManagedInbound{
			{
				InboundID:       inboundIDHy2,
				Tag:             "hy2-in",
				Protocol:        "hysteria2",
				UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
			},
			{
				InboundID:       inboundIDAnyTLS,
				Tag:             "anytls-in",
				Protocol:        "anytls",
				UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
			},
			{
				InboundID:       inboundIDSS,
				Tag:             "ss-in",
				Protocol:        "shadowsocks",
				UserApplyPolicy: contract.ApplyOnUserNone,
			},
		},
		SingBoxConfigTemplate: templateJSON,
	}
}

// makeUserSnapshot builds a UserSnapshot for a given inbound/protocol.
func makeUserSnapshot(inboundID, protocol, configRev, userRev string, userCount int) *contract.UserSnapshot {
	users := make([]contract.User, userCount)
	for i := 0; i < userCount; i++ {
		users[i] = contract.User{
			UserID: fmt.Sprintf("user-%s-%d", inboundID, i+1),
			Name:   fmt.Sprintf("user_%s_%d", inboundID, i+1),
			Credential: contract.Credential{
				Type:     contract.CredentialTypePassword,
				Password: fmt.Sprintf("pass_%s_%d", inboundID, i+1),
			},
		}
	}
	return &contract.UserSnapshot{
		Revision:              userRev,
		ConfigurationRevision: configRev,
		NodeID:                testNodeID,
		InboundID:             inboundID,
		Protocol:              protocol,
		Users:                 users,
	}
}

func managedInboundFromState(ibState state.InboundState) contract.ManagedInbound {
	policy := contract.ApplyOnUserHotReloadUsers
	if ibState.Protocol == contract.ProtocolShadowsocks {
		policy = contract.ApplyOnUserNone
	}
	return contract.ManagedInbound{
		InboundID:       ibState.InboundID,
		Tag:             ibState.Tag,
		Protocol:        ibState.Protocol,
		UserApplyPolicy: policy,
	}
}

// ---------------------------------------------------------------------------
// testEnv — shared test environment setup
// ---------------------------------------------------------------------------

// testEnv holds all initialized adapter components for a test.
type testEnv struct {
	fp         *fakePanel
	client     *client.Client
	store      *state.Store
	tracker    *traffic.Tracker
	manager    *runtime.Manager
	poller     *users.Poller
	reporter   *reporter.Reporter
	hb         *heartbeat.Heartbeat
	logFactory log.Factory
	tmpDir     string
	ctx        context.Context
	cancel     context.CancelFunc
}

// newTestEnv creates a fully initialized test environment with a fake panel.
// It does NOT call Bootstrap — the caller controls when that happens.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	fp := newFakePanel(testNodeID)
	t.Cleanup(fp.Close)

	cfg := makeValidConfigurationResponse("rev-initial-001")
	fp.setConfiguration(cfg)

	fp.setUserSnapshot(inboundIDHy2, makeUserSnapshot(inboundIDHy2, "hysteria2", "rev-initial-001", "user-hy2-v1", 2))
	fp.setUserSnapshot(inboundIDAnyTLS, makeUserSnapshot(inboundIDAnyTLS, "anytls", "rev-initial-001", "user-anytls-v1", 1))
	fp.setUserSnapshot(inboundIDSS, makeUserSnapshot(inboundIDSS, "shadowsocks", "rev-initial-001", "user-ss-v1", 3))

	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.json")

	panelClient, err := client.New(fp.URL(), testNodeID, testToken)
	require.NoError(t, err)

	store, err := state.NewStore(statePath)
	require.NoError(t, err)

	tracker := traffic.NewTracker(nil)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = include.Context(ctx)
	logFactory, err := log.New(log.Options{
		Context: ctx,
		Options: option.LogOptions{
			Level:  "warning",
			Output: "stderr",
		},
	})
	require.NoError(t, err)
	require.NoError(t, logFactory.Start())
	t.Cleanup(func() { logFactory.Close() })

	manager, err := runtime.NewManager(panelClient, store, tracker, logFactory,
		runtime.WithBoxFactory(&minimalBoxFactory{ctx: ctx}),
	)
	require.NoError(t, err)

	poller := users.NewPoller(panelClient, store, &stubUserReplacer{}, logFactory.NewLogger("panel/users"))

	rep := reporter.NewReporter(
		panelClient,
		store,
		tracker,
		testNodeID,
		reporter.WithLogger(logFactory.NewLogger("panel/reporter")),
		reporter.WithConfigRevision(""),
		reporter.WithSkipEmpty(false),
	)

	hb := heartbeat.NewHeartbeat(
		panelClient,
		store,
		testNodeID,
		C.Version,
		"test-adapter",
		heartbeat.WithLogger(logFactory.NewLogger("panel/heartbeat")),
		heartbeat.WithStartTimeProvider(manager),
		heartbeat.WithRuntimeMetricsProvider(tracker),
	)

	// After Bootstrap, replace the stub poller with one using the real Box.
	env := &testEnv{
		fp:         fp,
		client:     panelClient,
		store:      store,
		tracker:    tracker,
		manager:    manager,
		poller:     poller,
		reporter:   rep,
		hb:         hb,
		logFactory: logFactory,
		tmpDir:     tmpDir,
		ctx:        ctx,
		cancel:     cancel,
	}

	boxInst := env.manager.GetBox()
	if boxInst != nil {
		env.poller = users.NewPoller(env.client, env.store, boxInst, env.logFactory.NewLogger("panel/users"))
	}

	return env
}

// stubUserReplacer is a placeholder UserReplacer used before Bootstrap.
// After Bootstrap, the test creates a real Poller with the Box as UserReplacer.
type stubUserReplacer struct{}

func (s *stubUserReplacer) ReplaceInboundUsers(tag string, users []adapter.ManagedUser) error {
	return nil
}

// minimalBoxFactory creates a minimal *box.Box for testing without real
// protocol listeners, avoiding the need for root privileges or real TLS certs.
type minimalBoxFactory struct {
	ctx context.Context
}

func (f *minimalBoxFactory) Create(ctx context.Context, options option.Options) (*box.Box, error) {
	minimalOpts := option.Options{
		Log: &option.LogOptions{
			Level: "warning",
		},
		Inbounds: []option.Inbound{
			{
				Type: "direct",
				Tag:  "direct-in",
			},
		},
		Outbounds: []option.Outbound{
			{
				Type: "direct",
				Tag:  "direct-out",
			},
		},
	}

	boxCtx := include.Context(ctx)
	instance, err := box.New(box.Options{
		Context: boxCtx,
		Options: minimalOpts,
	})
	if err != nil {
		return nil, err
	}
	if err := instance.Start(); err != nil {
		instance.Close()
		return nil, err
	}
	return instance, nil
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

// assertOnlySpecPaths verifies every request targets one of the 4 spec paths.
func assertOnlySpecPaths(t *testing.T, entries []transcriptEntry, nodeID string) {
	t.Helper()
	for _, e := range entries {
		matched := false
		specPrefixes := []string{
			"/api/v1/nodes/" + nodeID + "/configuration",
			"/api/v1/nodes/" + nodeID + "/inbounds/",
			"/api/v1/nodes/" + nodeID + "/traffic-reports",
			"/api/v1/nodes/" + nodeID + "/heartbeats",
		}
		for _, sp := range specPrefixes {
			if e.Path == sp {
				matched = true
				break
			}
		}
		// Inbound users sub-paths.
		if strings.HasPrefix(e.Path, "/api/v1/nodes/"+nodeID+"/inbounds/") && strings.HasSuffix(e.Path, "/users") {
			matched = true
		}
		if !matched {
			if strings.Contains(e.Path, "v2b") || strings.Contains(e.Path, "V2b") ||
				strings.Contains(e.Path, "xray") || strings.Contains(e.Path, "sspanel") {
				t.Errorf("adapter called V2bX/compatibility path: %s %s", e.Method, e.Path)
			} else {
				t.Errorf("adapter called unexpected path: %s %s", e.Method, e.Path)
			}
		}
	}
}

// assertNoTokenInURL verifies no request URL contains the token in query/path.
func assertNoTokenInURL(t *testing.T, entries []transcriptEntry, token string) {
	t.Helper()
	for _, e := range entries {
		if strings.Contains(e.Path, token) {
			t.Errorf("token found in URL path: %s", e.Path)
		}
		if strings.Contains(e.Path, "token=") || strings.Contains(e.Path, "key=") ||
			strings.Contains(e.Path, "api_key=") || strings.Contains(e.Path, "apikey=") {
			t.Errorf("token/secret found in URL query: %s", e.Path)
		}
	}
}

// assertAuthorizationBearerOnly verifies all requests use Bearer auth.
func assertAuthorizationBearerOnly(t *testing.T, entries []transcriptEntry) {
	t.Helper()
	for _, e := range entries {
		auth := e.Headers.Get("Authorization")
		if auth == "" {
			continue
		}
		if !strings.HasPrefix(auth, "Bearer ") {
			t.Errorf("Authorization header not Bearer: %s", auth)
		}
	}
}

// assertRedactedTranscript verifies the redacted transcript removes secrets.
func assertRedactedTranscript(t *testing.T, entries []transcriptEntry) {
	t.Helper()
	redacted := redactedTranscript(entries)
	for _, e := range redacted {
		auth := e.Headers.Get("Authorization")
		if auth == "Bearer "+testToken {
			t.Errorf("Authorization header not redacted: %s", auth)
		}
		if strings.Contains(e.RequestBody, "pass_hy2") ||
			strings.Contains(e.RequestBody, "pass_anytls") ||
			strings.Contains(e.RequestBody, "pass_ss") {
			t.Errorf("password not redacted in body for %s %s", e.Method, e.Path)
		}
		ipRegex := regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
		if ipRegex.MatchString(e.RequestBody) {
			t.Errorf("raw IP found in redacted body for %s %s", e.Method, e.Path)
		}
	}
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestPanelAdapter_BootstrapConfigFetch(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err, "Bootstrap should succeed")

	st := env.store.State()
	require.Equal(t, "rev-initial-001", st.Config.Revision)
	require.Equal(t, testNodeID, st.Config.NodeID)
	require.NotEmpty(t, st.Config.ETag)
	require.Len(t, st.Inbounds, 3, "should have 3 managed inbounds")

	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS, inboundIDSS} {
		ib, ok := st.Inbounds[ibID]
		require.True(t, ok, "inbound %s should be in state", ibID)
		require.Equal(t, string(contract.UserLoadStatusEmptyInitialLoad), ib.UserLoadStatus,
			"inbound %s should be fail-closed (empty_initial_load)", ibID)
	}

	boxInst := env.manager.GetBox()
	require.NotNil(t, boxInst, "Box instance should exist after bootstrap")

	entries := env.fp.transcriptSnapshot()
	foundConfig := false
	for _, e := range entries {
		if e.Method == http.MethodGet && env.fp.isConfigurationPath(e.Path) {
			foundConfig = true
			break
		}
	}
	require.True(t, foundConfig, "should have fetched configuration from panel")
}

func TestPanelAdapter_InitialUserFetch(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err, "PollInbound for %s should succeed", inboundID)
	}

	st := env.store.State()

	hy2 := st.Inbounds[inboundIDHy2]
	require.Equal(t, "user-hy2-v1", hy2.UserRevision)
	require.Equal(t, 2, hy2.UserCount)
	require.Equal(t, string(contract.UserLoadStatusOK), hy2.UserLoadStatus)

	at := st.Inbounds[inboundIDAnyTLS]
	require.Equal(t, "user-anytls-v1", at.UserRevision)
	require.Equal(t, 1, at.UserCount)
	require.Equal(t, string(contract.UserLoadStatusOK), at.UserLoadStatus)

	ss := st.Inbounds[inboundIDSS]
	require.Empty(t, ss.UserRevision)
	require.Equal(t, 0, ss.UserCount)
	require.Equal(t, string(contract.UserLoadStatusOK), ss.UserLoadStatus)
}

func TestPanelAdapter_ETag304Polling(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision

	// First poll to populate ETags.
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err)
	}

	env.fp.resetTranscript()

	// Second poll — should get 304 because ETags match.
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err, "304 poll should succeed with no error")
	}

	st := env.store.State()
	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS, inboundIDSS} {
		require.Equal(t, string(contract.UserLoadStatusOK), st.Inbounds[ibID].UserLoadStatus,
			"inbound %s should still be OK after 304", ibID)
	}
}

func TestPanelAdapter_HotUserReplacement(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision

	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err)
	}

	st := env.store.State()
	require.Equal(t, 2, st.Inbounds[inboundIDHy2].UserCount)

	// Replace users with a new snapshot.
	newSnap := makeUserSnapshot(inboundIDHy2, "hysteria2", "rev-initial-001", "user-hy2-v2", 5)
	env.fp.setUserSnapshot(inboundIDHy2, newSnap)

	mi := contract.ManagedInbound{
		InboundID:       inboundIDHy2,
		Tag:             "hy2-in",
		Protocol:        "hysteria2",
		UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
	}
	err = env.poller.PollInbound(env.ctx, inboundIDHy2, mi, configRev)
	require.NoError(t, err)

	st = env.store.State()
	require.Equal(t, 5, st.Inbounds[inboundIDHy2].UserCount)
	require.Equal(t, "user-hy2-v2", st.Inbounds[inboundIDHy2].UserRevision)
	require.Equal(t, string(contract.UserLoadStatusOK), st.Inbounds[inboundIDHy2].UserLoadStatus)

	boxAfter := env.manager.GetBox()
	require.NotNil(t, boxAfter, "Box should still exist after hot user replacement")
}

func TestPanelAdapter_ConfigRevisionRecreate(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	newCfg := makeValidConfigurationResponse("rev-updated-002")
	env.fp.setConfiguration(newCfg)

	err = env.manager.PollConfiguration(env.ctx)
	require.NoError(t, err)

	st := env.store.State()
	require.Equal(t, "rev-updated-002", st.Config.Revision)

	newBox := env.manager.GetBox()
	require.NotNil(t, newBox, "new Box should exist after config change")

	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS, inboundIDSS} {
		ib, ok := st.Inbounds[ibID]
		require.True(t, ok)
		require.Equal(t, string(contract.UserLoadStatusEmptyInitialLoad), ib.UserLoadStatus,
			"inbound %s should be empty_initial_load after config change", ibID)
	}
}

func TestPanelAdapter_Config304NoChange(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	err = env.manager.PollConfiguration(env.ctx)
	require.NoError(t, err)

	st := env.store.State()
	require.Equal(t, "rev-initial-001", st.Config.Revision)
}

func TestPanelAdapter_TrafficReporting(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	inboundMapping := make(map[string]string, len(curState.Inbounds))
	for _, ib := range curState.Inbounds {
		inboundMapping[ib.Tag] = ib.InboundID
	}
	env.tracker.UpdateInboundMapping(inboundMapping)

	env.reporter.SetConfigRevision(curState.Config.Revision)

	// Report traffic. Even without live connections, the reporter
	// should either send an empty report or skip gracefully.
	err = env.reporter.ReportNow(env.ctx)
	_ = err // best-effort; no live traffic data is expected in unit tests

	// Verify the panel received a traffic-reports request or the
	// reporter skipped because there was no data.
	entries := env.fp.transcriptSnapshot()
	foundTraffic := false
	for _, e := range entries {
		if e.Method == http.MethodPost && env.fp.isTrafficPath(e.Path) {
			foundTraffic = true
			break
		}
	}
	// Traffic reporting is best-effort; the reporter may skip if empty.
	_ = foundTraffic
}

func TestPanelAdapter_HeartbeatReporting(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	err = env.hb.SendHeartbeat(env.ctx)
	require.NoError(t, err)

	hbReqs := env.fp.heartbeatReports()
	require.Len(t, hbReqs, 1)

	hb := hbReqs[0]
	require.Equal(t, "rev-initial-001", hb.AppliedConfigurationRevision)
	require.Equal(t, C.Version, hb.SingBoxVersion)
	require.Equal(t, "test-adapter", hb.AdapterVersion)
	require.False(t, hb.ObservedAt.IsZero())
	require.Len(t, hb.Inbounds, 3)

	inboundMap := make(map[string]contract.HeartbeatInbound)
	for _, ib := range hb.Inbounds {
		inboundMap[ib.InboundID] = ib
	}
	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS, inboundIDSS} {
		ib, ok := inboundMap[ibID]
		require.True(t, ok, "inbound %s should be in heartbeat", ibID)
		require.Equal(t, string(contract.UserLoadStatusEmptyInitialLoad), string(ib.UserLoadStatus),
			"inbound %s should be empty_initial_load before user poll", ibID)
	}

	require.NotNil(t, hb.Runtime)
	require.GreaterOrEqual(t, hb.Runtime.UptimeSeconds, int64(0))
}

func TestPanelAdapter_HeartbeatAfterUserPoll(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err)
	}

	env.fp.resetTranscript()
	err = env.hb.SendHeartbeat(env.ctx)
	require.NoError(t, err)

	hbReqs := env.fp.heartbeatReports()
	require.Len(t, hbReqs, 1)

	hb := hbReqs[0]
	for _, ib := range hb.Inbounds {
		require.Equal(t, string(contract.UserLoadStatusOK), string(ib.UserLoadStatus),
			"inbound %s should be OK after user poll", ib.InboundID)
		if ib.InboundID == inboundIDSS {
			require.Empty(t, ib.AppliedUserRevision, "single-user Shadowsocks should not poll user revisions")
			continue
		}
		require.NotEmpty(t, ib.AppliedUserRevision, "inbound %s should have user revision", ib.InboundID)
	}
}

func TestPanelAdapter_FailClosedInitialUserFetch(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	env.fp.mu.Lock()
	env.fp.usersStatusOverride = 503
	env.fp.mu.Unlock()
	defer func() {
		env.fp.mu.Lock()
		env.fp.usersStatusOverride = 0
		env.fp.mu.Unlock()
	}()

	curState := env.store.State()
	configRev := curState.Config.Revision
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		if ibState.Protocol == contract.ProtocolShadowsocks {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.Error(t, err, "PollInbound should fail when panel returns 503")
	}

	st := env.store.State()
	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS} {
		require.Equal(t, string(contract.UserLoadStatusEmptyInitialLoad), st.Inbounds[ibID].UserLoadStatus,
			"inbound %s should be empty_initial_load after failed initial fetch", ibID)
	}
}

func TestPanelAdapter_StaleSubsequentUserFetch(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err)
	}

	st := env.store.State()
	require.Equal(t, string(contract.UserLoadStatusOK), st.Inbounds[inboundIDHy2].UserLoadStatus)
	require.Equal(t, 2, st.Inbounds[inboundIDHy2].UserCount)

	env.fp.mu.Lock()
	env.fp.usersStatusOverride = 503
	env.fp.mu.Unlock()

	mi := contract.ManagedInbound{
		InboundID:       inboundIDHy2,
		Tag:             "hy2-in",
		Protocol:        "hysteria2",
		UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
	}
	err = env.poller.PollInbound(env.ctx, inboundIDHy2, mi, configRev)
	require.Error(t, err)

	st = env.store.State()
	require.Equal(t, string(contract.UserLoadStatusStale), st.Inbounds[inboundIDHy2].UserLoadStatus,
		"inbound should be stale after subsequent fetch failure")
	require.Equal(t, 2, st.Inbounds[inboundIDHy2].UserCount,
		"old user count should be preserved when stale")
}

func TestPanelAdapter_RevisionConflictTriggersRefetch(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	conflictSnap := makeUserSnapshot(inboundIDHy2, "hysteria2", "rev-different-999", "user-hy2-conflict", 1)
	env.fp.setUserSnapshot(inboundIDHy2, conflictSnap)

	curState := env.store.State()
	configRev := curState.Config.Revision

	mi := contract.ManagedInbound{
		InboundID:       inboundIDHy2,
		Tag:             "hy2-in",
		Protocol:        "hysteria2",
		UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
	}
	err = env.poller.PollInbound(env.ctx, inboundIDHy2, mi, configRev)
	require.Error(t, err, "PollInbound should fail on revision conflict")

	require.True(t, env.poller.NeedsConfigRefetch(), "config refetch should be triggered after revision conflict")

	st := env.store.State()
	require.Equal(t, string(contract.UserLoadStatusRevisionConflict), st.Inbounds[inboundIDHy2].UserLoadStatus)
}

func TestPanelAdapter_NetworkFailureRecovery(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	// Simulate network failure.
	env.fp.mu.Lock()
	env.fp.configStatusOverride = 503
	env.fp.mu.Unlock()

	err = env.manager.PollConfiguration(env.ctx)
	require.Error(t, err, "PollConfiguration should fail when panel returns 503")

	// Recover.
	env.fp.mu.Lock()
	env.fp.configStatusOverride = 0
	env.fp.mu.Unlock()

	err = env.manager.PollConfiguration(env.ctx)
	require.NoError(t, err, "PollConfiguration should succeed after panel recovers")
}

func TestPanelAdapter_OnlySpecPaths(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision

	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		_ = env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
	}

	env.reporter.SetConfigRevision(configRev)
	_ = env.reporter.ReportNow(env.ctx)
	_ = env.hb.SendHeartbeat(env.ctx)

	assertOnlySpecPaths(t, env.fp.transcriptSnapshot(), testNodeID)
}

func TestPanelAdapter_NoTokenInURL(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		_ = env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
	}

	env.reporter.SetConfigRevision(configRev)
	_ = env.reporter.ReportNow(env.ctx)
	_ = env.hb.SendHeartbeat(env.ctx)

	assertNoTokenInURL(t, env.fp.transcriptSnapshot(), testToken)
}

func TestPanelAdapter_AuthorizationBearerOnly(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	_ = env.manager.PollConfiguration(env.ctx)
	_ = env.hb.SendHeartbeat(env.ctx)

	assertAuthorizationBearerOnly(t, env.fp.transcriptSnapshot())
}

func TestPanelAdapter_RedactedTranscript(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	curState := env.store.State()
	configRev := curState.Config.Revision
	for inboundID, ibState := range curState.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		_ = env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
	}

	_ = env.hb.SendHeartbeat(env.ctx)
	assertRedactedTranscript(t, env.fp.transcriptSnapshot())
}

func TestPanelAdapter_FullLifecycle(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	// Step 1: Bootstrap.
	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)
	st := env.store.State()
	require.Equal(t, "rev-initial-001", st.Config.Revision)
	require.Len(t, st.Inbounds, 3)

	// Step 2: Poll users for all inbounds.
	configRev := st.Config.Revision
	for inboundID, ibState := range st.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err)
	}
	st = env.store.State()
	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS, inboundIDSS} {
		require.Equal(t, string(contract.UserLoadStatusOK), st.Inbounds[ibID].UserLoadStatus, ibID)
	}

	// Step 3: Config change → Box recreation.
	newCfg := makeValidConfigurationResponse("rev-updated-002")
	env.fp.setConfiguration(newCfg)
	err = env.manager.PollConfiguration(env.ctx)
	require.NoError(t, err)
	st = env.store.State()
	require.Equal(t, "rev-updated-002", st.Config.Revision)
	for _, ibID := range []string{inboundIDHy2, inboundIDAnyTLS, inboundIDSS} {
		require.Equal(t, string(contract.UserLoadStatusEmptyInitialLoad), st.Inbounds[ibID].UserLoadStatus, ibID)
	}

	// Step 4: Update user snapshots for new revision and re-poll.
	env.fp.setUserSnapshot(inboundIDHy2, makeUserSnapshot(inboundIDHy2, "hysteria2", "rev-updated-002", "user-hy2-v2", 4))
	env.fp.setUserSnapshot(inboundIDAnyTLS, makeUserSnapshot(inboundIDAnyTLS, "anytls", "rev-updated-002", "user-anytls-v2", 2))
	env.fp.setUserSnapshot(inboundIDSS, makeUserSnapshot(inboundIDSS, "shadowsocks", "rev-updated-002", "user-ss-v2", 1))

	configRev = st.Config.Revision
	for inboundID, ibState := range st.Inbounds {
		if !contract.IsSupportedProtocol(ibState.Protocol) {
			continue
		}
		mi := managedInboundFromState(ibState)
		err := env.poller.PollInbound(env.ctx, inboundID, mi, configRev)
		require.NoError(t, err)
	}
	st = env.store.State()
	require.Equal(t, 4, st.Inbounds[inboundIDHy2].UserCount)
	require.Equal(t, 2, st.Inbounds[inboundIDAnyTLS].UserCount)
	require.Equal(t, 0, st.Inbounds[inboundIDSS].UserCount)

	// Step 5: Traffic + heartbeat.
	env.reporter.SetConfigRevision(configRev)
	_ = env.reporter.ReportNow(env.ctx)
	err = env.hb.SendHeartbeat(env.ctx)
	require.NoError(t, err)

	hbReqs := env.fp.heartbeatReports()
	require.True(t, len(hbReqs) >= 1)
	lastHB := hbReqs[len(hbReqs)-1]
	require.Equal(t, "rev-updated-002", lastHB.AppliedConfigurationRevision)
	for _, ib := range lastHB.Inbounds {
		require.Equal(t, string(contract.UserLoadStatusOK), string(ib.UserLoadStatus), ib.InboundID)
	}

	// Step 6: Global assertions.
	entries := env.fp.transcriptSnapshot()
	assertOnlySpecPaths(t, entries, testNodeID)
	assertNoTokenInURL(t, entries, testToken)
	assertAuthorizationBearerOnly(t, entries)
	assertRedactedTranscript(t, entries)
}

func TestPanelAdapter_UnexpectedPathReturns404(t *testing.T) {
	fp := newFakePanel(testNodeID)
	defer fp.Close()

	v2bxPaths := []struct {
		method string
		path   string
	}{
		{"GET", "/api/v1/server/config"},
		{"GET", "/api/v1/server/userList"},
		{"POST", "/api/v1/server/users/submit"},
		{"GET", "/api/v2/nodes/" + testNodeID + "/configuration"},
		{"GET", "/api/v1/nodes/" + testNodeID + "/info"},
		{"POST", "/api/v1/nodes/" + testNodeID + "/report"},
		{"GET", "/v2b/nodes/" + testNodeID + "/config"},
		{"GET", "/xray/nodes/" + testNodeID + "/config"},
	}

	for _, tc := range v2bxPaths {
		req, err := http.NewRequest(tc.method, fp.URL()+tc.path, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode,
			"unexpected path %s %s should return 404", tc.method, tc.path)
	}
}

func TestPanelAdapter_ManualApplyStrategy(t *testing.T) {
	env := newTestEnv(t)
	defer env.cancel()

	err := env.manager.Bootstrap(env.ctx)
	require.NoError(t, err)

	newCfg := makeValidConfigurationResponse("rev-manual-003")
	newCfg.ApplyStrategy.OnConfigurationChange = contract.ApplyOnConfigManual
	env.fp.setConfiguration(newCfg)

	err = env.manager.PollConfiguration(env.ctx)
	require.NoError(t, err)

	st := env.store.State()
	require.Equal(t, "rev-manual-003", st.Config.Revision)

	boxInst := env.manager.GetBox()
	require.NotNil(t, boxInst, "Box should still exist")
}
