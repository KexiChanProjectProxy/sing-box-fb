package heartbeat

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"

	"github.com/sagernet/sing/common/logger"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// mockStartTimeProvider returns a fixed start time.
type mockStartTimeProvider struct {
	startTime time.Time
}

func (m *mockStartTimeProvider) StartTime() time.Time {
	return m.startTime
}

// mockMetricsProvider returns fixed runtime metrics.
type mockMetricsProvider struct {
	metrics *contract.HeartbeatRuntime
}

func (m *mockMetricsProvider) GetRuntimeMetrics() *contract.HeartbeatRuntime {
	return m.metrics
}

// captureHeartbeatHandler captures the heartbeat POST body for assertions.
type captureHeartbeatHandler struct {
	body       []byte
	headers    http.Header
	statusCode int
	noStore    bool
}

func (h *captureHeartbeatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.headers = r.Header.Clone()
	if r.Body != nil {
		defer r.Body.Close()
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		h.body = buf[:n]
	}
	if h.noStore {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(h.statusCode)
}

// newTestStore creates a Store with the given state for testing.
// Uses a temp file path but never writes to disk.
func newTestStore(st *state.State) *state.Store {
	s := &state.Store{}
	s.SetState(st)
	return s
}

// newTestClient creates a Client pointed at the given test server.
func newTestClient(serverURL string) (*client.Client, error) {
	return client.New(serverURL, "node-1", "test-token")
}

// ---------------------------------------------------------------------------
// Shared test data
// ---------------------------------------------------------------------------

var (
	testNodeID    = "node-1"
	testConfigRev = "cfg-rev-1"
)

func defaultState() *state.State {
	return &state.State{
		Version:  state.CurrentVersion,
		Inbounds: make(map[string]state.InboundState),
	}
}

// ---------------------------------------------------------------------------
// Tests: status mapping
// ---------------------------------------------------------------------------

func TestBuildInbounds_StatusOK(t *testing.T) {
	st := defaultState()
	st.Config.Revision = testConfigRev
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Tag:            "in-hy2",
		Protocol:       "hysteria2",
		UserRevision:   "u-rev-1",
		UserCount:      5,
		UserLoadStatus: string(contract.UserLoadStatusOK),
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if len(inbounds) != 1 {
		t.Fatalf("expected 1 inbound, got %d", len(inbounds))
	}
	ib := inbounds[0]
	if ib.UserLoadStatus != contract.UserLoadStatusOK {
		t.Errorf("expected status ok, got %q", ib.UserLoadStatus)
	}
	if ib.UserCount != 5 {
		t.Errorf("expected user count 5, got %d", ib.UserCount)
	}
	if ib.AppliedUserRevision != "u-rev-1" {
		t.Errorf("expected applied_user_revision u-rev-1, got %q", ib.AppliedUserRevision)
	}
}

func TestBuildInbounds_StatusStale(t *testing.T) {
	st := defaultState()
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Tag:            "in-hy2",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusStale),
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if inbounds[0].UserLoadStatus != contract.UserLoadStatusStale {
		t.Errorf("expected status stale, got %q", inbounds[0].UserLoadStatus)
	}
}

func TestBuildInbounds_StatusEmptyInitialLoad(t *testing.T) {
	st := defaultState()
	// Inbound with empty UserLoadStatus (never polled) → empty_initial_load
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID: "ib-1",
		Tag:       "in-hy2",
		Protocol:  "hysteria2",
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if inbounds[0].UserLoadStatus != contract.UserLoadStatusEmptyInitialLoad {
		t.Errorf("expected status empty_initial_load, got %q", inbounds[0].UserLoadStatus)
	}
}

func TestBuildInbounds_StatusRevisionConflict(t *testing.T) {
	st := defaultState()
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Tag:            "in-hy2",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusRevisionConflict),
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if inbounds[0].UserLoadStatus != contract.UserLoadStatusRevisionConflict {
		t.Errorf("expected status revision_conflict, got %q", inbounds[0].UserLoadStatus)
	}
}

func TestBuildInbounds_StatusApplyFailed(t *testing.T) {
	st := defaultState()
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Tag:            "in-hy2",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusApplyFailed),
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if inbounds[0].UserLoadStatus != contract.UserLoadStatusApplyFailed {
		t.Errorf("expected status apply_failed, got %q", inbounds[0].UserLoadStatus)
	}
}

// ---------------------------------------------------------------------------
// Tests: unknown protocol → unsupported_protocol
// ---------------------------------------------------------------------------

func TestBuildInbounds_UnknownProtocol_UnsupportedStatus(t *testing.T) {
	st := defaultState()
	// Even if the inbound has "ok" status, unknown protocol overrides to unsupported_protocol
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Tag:            "in-vmess",
		Protocol:       "vmess", // not in SupportedProtocols
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      3,
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if inbounds[0].UserLoadStatus != contract.UserLoadStatusUnsupportedProto {
		t.Errorf("expected unsupported_protocol for unknown protocol, got %q", inbounds[0].UserLoadStatus)
	}
	if inbounds[0].Protocol != "vmess" {
		t.Errorf("expected protocol vmess preserved, got %q", inbounds[0].Protocol)
	}
}

func TestBuildInbounds_UnknownProtocol_EmptyStatus(t *testing.T) {
	st := defaultState()
	// Unknown protocol with empty status → unsupported_protocol (not empty_initial_load)
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID: "ib-1",
		Tag:       "in-trojan",
		Protocol:  "trojan", // not in SupportedProtocols
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if inbounds[0].UserLoadStatus != contract.UserLoadStatusUnsupportedProto {
		t.Errorf("expected unsupported_protocol for unknown protocol with empty status, got %q", inbounds[0].UserLoadStatus)
	}
}

func TestBuildInbounds_SupportedProtocols(t *testing.T) {
	st := defaultState()
	st.Inbounds["ib-hy2"] = state.InboundState{
		InboundID:      "ib-hy2",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusOK),
	}
	st.Inbounds["ib-ss"] = state.InboundState{
		InboundID:      "ib-ss",
		Protocol:       "shadowsocks",
		UserLoadStatus: string(contract.UserLoadStatusOK),
	}
	st.Inbounds["ib-at"] = state.InboundState{
		InboundID:      "ib-at",
		Protocol:       "anytls",
		UserLoadStatus: string(contract.UserLoadStatusOK),
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if len(inbounds) != 3 {
		t.Fatalf("expected 3 inbounds, got %d", len(inbounds))
	}
	for _, ib := range inbounds {
		if ib.UserLoadStatus != contract.UserLoadStatusOK {
			t.Errorf("protocol %q: expected ok, got %q", ib.Protocol, ib.UserLoadStatus)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests: pending configuration revision
// ---------------------------------------------------------------------------

func TestPendingRevision_ApplyFailed(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-pending"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusApplyFailed),
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	pending := h.pendingRevision(st)

	if pending == nil {
		t.Fatal("expected pending revision for apply_failed, got nil")
	}
	if *pending != "rev-pending" {
		t.Errorf("expected pending revision rev-pending, got %q", *pending)
	}
}

func TestPendingRevision_UnsupportedProtocol_NoUserRevision(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-pending"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "vmess",
		UserLoadStatus: string(contract.UserLoadStatusUnsupportedProto),
		UserRevision:   "", // no user revision applied
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	pending := h.pendingRevision(st)

	if pending == nil {
		t.Fatal("expected pending revision for unsupported_protocol with no user revision, got nil")
	}
	if *pending != "rev-pending" {
		t.Errorf("expected pending revision rev-pending, got %q", *pending)
	}
}

func TestPendingRevision_OK_NoPending(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-applied"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserRevision:   "u-rev-1",
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	pending := h.pendingRevision(st)

	if pending != nil {
		t.Errorf("expected nil pending revision for ok status, got %q", *pending)
	}
}

func TestPendingRevision_EmptyInbounds(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	pending := h.pendingRevision(st)

	if pending != nil {
		t.Errorf("expected nil pending revision with no inbounds, got %q", *pending)
	}
}

func TestPendingRevision_UnsupportedProtocol_WithUserRevision(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-applied"
	// Unsupported protocol but has a user revision from a previous successful load
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "vmess",
		UserLoadStatus: string(contract.UserLoadStatusUnsupportedProto),
		UserRevision:   "u-rev-old",
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	pending := h.pendingRevision(st)

	// Has user revision, so not considered pending
	if pending != nil {
		t.Errorf("expected nil pending revision for unsupported_protocol with user revision, got %q", *pending)
	}
}

// ---------------------------------------------------------------------------
// Tests: runtime metrics are aggregate-only, no raw IPs
// ---------------------------------------------------------------------------

func TestBuildRuntimeMetrics_AggregateOnly(t *testing.T) {
	st := defaultState()
	store := newTestStore(st)

	metrics := &mockMetricsProvider{
		metrics: &contract.HeartbeatRuntime{
			Connections: 42,
		},
	}

	startTime := time.Now().Add(-5 * time.Minute)
	startProvider := &mockStartTimeProvider{startTime: startTime}

	h := NewHeartbeat(nil, store, testNodeID, constant.Version, "",
		WithStartTimeProvider(startProvider),
		WithLogger(logger.NOP()),
	)
	h.metrics = metrics

	rt := h.buildRuntimeMetrics()

	if rt.Connections != 42 {
		t.Errorf("expected 42 connections, got %d", rt.Connections)
	}
	if rt.UptimeSeconds < 290 || rt.UptimeSeconds > 310 {
		t.Errorf("expected uptime ~300s, got %d", rt.UptimeSeconds)
	}
	// MemoryBytes should be non-zero (from runtime.ReadMemStats)
	if rt.MemoryBytes <= 0 {
		t.Error("expected non-zero memory bytes")
	}
}

func TestBuildRuntimeMetrics_NoRawIPs(t *testing.T) {
	// Verify that HeartbeatRuntime struct has no IP-related fields.
	// This is a structural test: the contract.HeartbeatRuntime type
	// must not contain any IP list fields.
	rt := contract.HeartbeatRuntime{}

	data, err := json.Marshal(rt)
	if err != nil {
		t.Fatalf("marshal HeartbeatRuntime: %v", err)
	}
	s := string(data)
	if strings.Contains(s, "ip") || strings.Contains(s, "IP") {
		t.Errorf("HeartbeatRuntime JSON contains IP-related field: %s", s)
	}
}

func TestBuildRuntimeMetrics_NoProvider(t *testing.T) {
	st := defaultState()
	store := newTestStore(st)

	startTime := time.Now().Add(-10 * time.Second)
	startProvider := &mockStartTimeProvider{startTime: startTime}

	h := NewHeartbeat(nil, store, testNodeID, constant.Version, "",
		WithStartTimeProvider(startProvider),
		WithLogger(logger.NOP()),
	)
	// No metrics provider set — connections should be 0

	rt := h.buildRuntimeMetrics()

	if rt.Connections != 0 {
		t.Errorf("expected 0 connections without provider, got %d", rt.Connections)
	}
	if rt.UptimeSeconds < 9 {
		t.Errorf("expected uptime >= 9s, got %d", rt.UptimeSeconds)
	}
}

// ---------------------------------------------------------------------------
// Tests: adapter version is non-empty
// ---------------------------------------------------------------------------

func TestAdapterVersion_NonEmpty(t *testing.T) {
	st := defaultState()
	store := newTestStore(st)

	// Default version
	h1 := NewHeartbeat(nil, store, testNodeID, constant.Version, "")
	if h1.AdapterVersion() == "" {
		t.Error("expected non-empty default adapter version")
	}
	if h1.AdapterVersion() != defaultAdapterVersion {
		t.Errorf("expected default version %q, got %q", defaultAdapterVersion, h1.AdapterVersion())
	}

	// Explicit version
	h2 := NewHeartbeat(nil, store, testNodeID, constant.Version, "1.2.3")
	if h2.AdapterVersion() != "1.2.3" {
		t.Errorf("expected version 1.2.3, got %q", h2.AdapterVersion())
	}

	// Empty string falls back to default
	h3 := NewHeartbeat(nil, store, testNodeID, constant.Version, "")
	if h3.AdapterVersion() != defaultAdapterVersion {
		t.Errorf("expected default version for empty string, got %q", h3.AdapterVersion())
	}
}

func TestWithAdapterVersion_Option(t *testing.T) {
	st := defaultState()
	store := newTestStore(st)

	h := NewHeartbeat(nil, store, testNodeID, constant.Version, "",
		WithAdapterVersion("2.0.0"),
	)
	if h.AdapterVersion() != "2.0.0" {
		t.Errorf("expected version 2.0.0 via option, got %q", h.AdapterVersion())
	}
}

func TestWithAdapterVersion_EmptyIgnored(t *testing.T) {
	st := defaultState()
	store := newTestStore(st)

	h := NewHeartbeat(nil, store, testNodeID, constant.Version, "1.0.0",
		WithAdapterVersion(""), // empty should not override
	)
	if h.AdapterVersion() != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %q", h.AdapterVersion())
	}
}

// ---------------------------------------------------------------------------
// Tests: heartbeat does not expose secrets
// ---------------------------------------------------------------------------

func TestSendHeartbeat_DoesNotExposeSecrets(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      3,
	}

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	// Verify the heartbeat payload does not contain secrets
	body := string(handler.body)
	forbidden := []string{"token", "password", "secret", "bearer", "tls_key", "private_key"}
	for _, word := range forbidden {
		if strings.Contains(strings.ToLower(body), word) {
			t.Errorf("heartbeat body contains forbidden word %q: %s", word, body)
		}
	}

	// Verify the Authorization header is on the HTTP request, not in the body
	if handler.headers.Get("Authorization") == "" {
		t.Error("expected Authorization header on HTTP request")
	}
}

// ---------------------------------------------------------------------------
// Tests: full SendHeartbeat integration
// ---------------------------------------------------------------------------

func TestSendHeartbeat_Success(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Tag:            "in-hy2",
		Protocol:       "hysteria2",
		UserRevision:   "u-rev-1",
		UserCount:      5,
		UserLoadStatus: string(contract.UserLoadStatusOK),
	}

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	// Decode the captured heartbeat
	var hb contract.Heartbeat
	if err := json.Unmarshal(handler.body, &hb); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}

	// Verify fields
	if hb.SingBoxVersion != constant.Version {
		t.Errorf("expected sing_box_version %q, got %q", constant.Version, hb.SingBoxVersion)
	}
	if hb.AdapterVersion != "1.0.0" {
		t.Errorf("expected adapter_version 1.0.0, got %q", hb.AdapterVersion)
	}
	if hb.AppliedConfigurationRevision != "rev-1" {
		t.Errorf("expected applied_configuration_revision rev-1, got %q", hb.AppliedConfigurationRevision)
	}
	if hb.PendingConfigurationRevision != nil {
		t.Errorf("expected nil pending_configuration_revision, got %q", *hb.PendingConfigurationRevision)
	}
	if len(hb.Inbounds) != 1 {
		t.Fatalf("expected 1 inbound, got %d", len(hb.Inbounds))
	}
	if hb.Inbounds[0].UserLoadStatus != contract.UserLoadStatusOK {
		t.Errorf("expected ok status, got %q", hb.Inbounds[0].UserLoadStatus)
	}
	if hb.ObservedAt.IsZero() {
		t.Error("expected non-zero observed_at")
	}
	if hb.Runtime == nil {
		t.Error("expected runtime metrics")
	}
}

func TestSendHeartbeat_WithPendingRevision(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-pending"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusApplyFailed),
	}

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	var hb contract.Heartbeat
	if err := json.Unmarshal(handler.body, &hb); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}

	if hb.PendingConfigurationRevision == nil {
		t.Fatal("expected pending_configuration_revision for apply_failed, got nil")
	}
	if *hb.PendingConfigurationRevision != "rev-pending" {
		t.Errorf("expected pending revision rev-pending, got %q", *hb.PendingConfigurationRevision)
	}
}

func TestSendHeartbeat_BestEffort_ErrorLogged(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"

	// Server returns 500
	handler := &captureHeartbeatHandler{statusCode: http.StatusInternalServerError}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	err = h.SendHeartbeat(ctx)
	// Error should be returned (caller decides to retry or not)
	if err == nil {
		t.Error("expected error from SendHeartbeat when server returns 500")
	}
}

func TestSendHeartbeat_MultipleInbounds(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"
	st.Inbounds["ib-hy2"] = state.InboundState{
		InboundID:      "ib-hy2",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      10,
		UserRevision:   "u-rev-1",
	}
	st.Inbounds["ib-ss"] = state.InboundState{
		InboundID:      "ib-ss",
		Protocol:       "shadowsocks",
		UserLoadStatus: string(contract.UserLoadStatusStale),
		UserCount:      5,
		UserRevision:   "u-rev-old",
	}
	st.Inbounds["ib-vmess"] = state.InboundState{
		InboundID:      "ib-vmess",
		Protocol:       "vmess",                           // unsupported
		UserLoadStatus: string(contract.UserLoadStatusOK), // overridden to unsupported_protocol
		UserCount:      0,
	}

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	var hb contract.Heartbeat
	if err := json.Unmarshal(handler.body, &hb); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}

	if len(hb.Inbounds) != 3 {
		t.Fatalf("expected 3 inbounds, got %d", len(hb.Inbounds))
	}

	// Find each inbound and verify status
	statusMap := make(map[string]contract.UserLoadStatus)
	for _, ib := range hb.Inbounds {
		statusMap[ib.InboundID] = ib.UserLoadStatus
	}
	if statusMap["ib-hy2"] != contract.UserLoadStatusOK {
		t.Errorf("ib-hy2: expected ok, got %q", statusMap["ib-hy2"])
	}
	if statusMap["ib-ss"] != contract.UserLoadStatusStale {
		t.Errorf("ib-ss: expected stale, got %q", statusMap["ib-ss"])
	}
	if statusMap["ib-vmess"] != contract.UserLoadStatusUnsupportedProto {
		t.Errorf("ib-vmess: expected unsupported_protocol, got %q", statusMap["ib-vmess"])
	}
}

// ---------------------------------------------------------------------------
// Tests: X-Applied-Configuration-Revision header
// ---------------------------------------------------------------------------

func TestSendHeartbeat_AppliedConfigRevisionHeader(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-42"

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	// The client sets X-Applied-Configuration-Revision header
	gotHeader := handler.headers.Get("X-Applied-Configuration-Revision")
	if gotHeader != "rev-42" {
		t.Errorf("expected X-Applied-Configuration-Revision header rev-42, got %q", gotHeader)
	}
}

// ---------------------------------------------------------------------------
// Tests: all managed inbounds included (not just successfully polled)
// ---------------------------------------------------------------------------

func TestBuildInbounds_AllInboundsIncluded(t *testing.T) {
	st := defaultState()
	// Mix of statuses: ok, empty (never polled), unsupported
	st.Inbounds["ib-ok"] = state.InboundState{
		InboundID:      "ib-ok",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      5,
	}
	st.Inbounds["ib-empty"] = state.InboundState{
		InboundID: "ib-empty",
		Protocol:  "shadowsocks",
		// No UserLoadStatus — never polled
	}
	st.Inbounds["ib-unsupported"] = state.InboundState{
		InboundID:      "ib-unsupported",
		Protocol:       "vless",                           // not supported
		UserLoadStatus: string(contract.UserLoadStatusOK), // overridden
	}

	h := NewHeartbeat(nil, newTestStore(st), testNodeID, constant.Version, "", WithLogger(logger.NOP()))
	inbounds := h.buildInbounds(st)

	if len(inbounds) != 3 {
		t.Fatalf("expected 3 inbounds, got %d", len(inbounds))
	}

	statusMap := make(map[string]contract.UserLoadStatus)
	for _, ib := range inbounds {
		statusMap[ib.InboundID] = ib.UserLoadStatus
	}
	if statusMap["ib-ok"] != contract.UserLoadStatusOK {
		t.Errorf("ib-ok: expected ok, got %q", statusMap["ib-ok"])
	}
	if statusMap["ib-empty"] != contract.UserLoadStatusEmptyInitialLoad {
		t.Errorf("ib-empty: expected empty_initial_load, got %q", statusMap["ib-empty"])
	}
	if statusMap["ib-unsupported"] != contract.UserLoadStatusUnsupportedProto {
		t.Errorf("ib-unsupported: expected unsupported_protocol, got %q", statusMap["ib-unsupported"])
	}
}

// ---------------------------------------------------------------------------
// Tests: PendingConfigurationRevision set when different from applied
// ---------------------------------------------------------------------------

func TestPendingConfigurationRevision_SetWhenApplyFailed(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-failed"
	st.Inbounds["ib-1"] = state.InboundState{
		InboundID:      "ib-1",
		Protocol:       "hysteria2",
		UserLoadStatus: string(contract.UserLoadStatusApplyFailed),
	}

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	var hb contract.Heartbeat
	if err := json.Unmarshal(handler.body, &hb); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}

	// PendingConfigurationRevision should be set
	if hb.PendingConfigurationRevision == nil {
		t.Fatal("expected PendingConfigurationRevision to be set for apply_failed")
	}
	if *hb.PendingConfigurationRevision != "rev-failed" {
		t.Errorf("expected pending revision rev-failed, got %q", *hb.PendingConfigurationRevision)
	}
	// AppliedConfigurationRevision should also be the failed revision
	if hb.AppliedConfigurationRevision != "rev-failed" {
		t.Errorf("expected applied revision rev-failed, got %q", hb.AppliedConfigurationRevision)
	}
}

// ---------------------------------------------------------------------------
// Tests: runtime metrics with tracker
// ---------------------------------------------------------------------------

func TestBuildRuntimeMetrics_WithTracker(t *testing.T) {
	st := defaultState()
	store := newTestStore(st)

	metrics := &mockMetricsProvider{
		metrics: &contract.HeartbeatRuntime{
			Connections: 100,
		},
	}

	startTime := time.Now().Add(-1 * time.Hour)
	startProvider := &mockStartTimeProvider{startTime: startTime}

	h := NewHeartbeat(nil, store, testNodeID, constant.Version, "",
		WithStartTimeProvider(startProvider),
		WithLogger(logger.NOP()),
	)
	h.metrics = metrics

	rt := h.buildRuntimeMetrics()

	if rt.Connections != 100 {
		t.Errorf("expected 100 connections, got %d", rt.Connections)
	}
	// Uptime should be approximately 3600 seconds
	if rt.UptimeSeconds < 3590 || rt.UptimeSeconds > 3610 {
		t.Errorf("expected uptime ~3600s, got %d", rt.UptimeSeconds)
	}
	if rt.MemoryBytes <= 0 {
		t.Error("expected non-zero memory bytes from runtime.ReadMemStats")
	}
}

// ---------------------------------------------------------------------------
// Tests: context cancellation
// ---------------------------------------------------------------------------

func TestSendHeartbeat_CancelledContext(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = h.SendHeartbeat(ctx)
	if err == nil {
		t.Error("expected error with cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests: observed_at is UTC
// ---------------------------------------------------------------------------

func TestSendHeartbeat_ObservedAtUTC(t *testing.T) {
	st := defaultState()
	st.Config.Revision = "rev-1"

	handler := &captureHeartbeatHandler{statusCode: http.StatusAccepted, noStore: true}
	server := httptest.NewServer(handler)
	defer server.Close()

	cl, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}

	store := newTestStore(st)
	h := NewHeartbeat(cl, store, testNodeID, constant.Version, "1.0.0", WithLogger(logger.NOP()))

	ctx := context.Background()
	if err := h.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	var hb contract.Heartbeat
	if err := json.Unmarshal(handler.body, &hb); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}

	// observed_at should be in UTC
	if hb.ObservedAt.Location() != time.UTC {
		t.Errorf("expected observed_at in UTC, got %v", hb.ObservedAt.Location())
	}
}
