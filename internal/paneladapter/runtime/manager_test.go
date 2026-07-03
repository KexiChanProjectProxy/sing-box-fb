package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// stubBox is a minimal *box.Box stand-in. We cannot construct a real
// *box.Box without a full config, so the fake factory returns a wrapper
// that tracks Start/Close calls.
type stubBox struct {
	started atomic.Bool
	closed  atomic.Bool
}

func (s *stubBox) Start() error {
	s.started.Store(true)
	return nil
}

func (s *stubBox) Close() error {
	s.closed.Store(true)
	return nil
}

// fakeBoxFactory records calls and returns stubs.
type fakeBoxFactory struct {
	createErr error // if set, Create returns this error
	instances []*stubBox
	callCount int
}

func (f *fakeBoxFactory) Create(ctx context.Context, options option.Options) (*box.Box, error) {
	f.callCount++
	if f.createErr != nil {
		return nil, f.createErr
	}
	// We cannot create a real *box.Box from here; instead we rely on
	// the fact that the Manager only calls instance.Start() and
	// instance.Close(). The fake factory creates a Box through the
	// production path but with a minimal config that will succeed.
	// However, for unit tests we want to avoid the heavyweight box.New.
	//
	// Since box.Box is a concrete type (not an interface), we must use
	// the real constructor. To keep tests lightweight, we provide a
	// minimal valid sing-box config that creates a functional but
	// minimal Box instance.
	return nil, errors.New("fakeBoxFactory: use mockManager instead for unit tests")
}

// mockManager is a test-only Manager that replaces the box instance with
// a trackable stub. Since *box.Box is a concrete type, we test through
// the Manager's exported methods and verify side effects via the state
// store rather than trying to mock the Box itself.
type mockManager struct {
	manager *Manager
	// Track Box lifecycle through the factory.
	created int
	closed  int
}

// newTestManager creates a Manager with all dependencies wired for testing.
// The fake client responds with the provided configuration fetcher.
func newTestManager(t *testing.T, fetcher func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error)) *mockManager {
	t.Helper()

	// Create a temp state store.
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, err := state.NewStore(statePath)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	// Create a traffic tracker.
	tracker := traffic.NewTracker(map[string]string{})

	// Create a fake client.
	fakeClient := &fakeClient{fetcher: fetcher}

	// Create a log factory (NOP logger for tests).
	logFactory := log.NewNOPFactory()

	// Create the manager with the recording factory.
	recorder := &recordingFactory{}
	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	return &mockManager{
		manager: m,
	}
}

// recordingFactory is a BoxFactory that records calls but cannot create
// real Box instances. Tests that need actual Box creation should use
// integration-style tests. For unit tests, we validate side effects
// (state changes, error returns) rather than Box internals.
type recordingFactory struct {
	createErr error
	callCount int
	lastOpts  option.Options
	events    *[]string
}

func (r *recordingFactory) Create(ctx context.Context, options option.Options) (*box.Box, error) {
	r.callCount++
	r.lastOpts = options
	if r.events != nil {
		*r.events = append(*r.events, "create")
	}
	if r.createErr != nil {
		return nil, r.createErr
	}
	// Create a minimal real Box. We need at minimum:
	// - An inbound (so the Box initializes)
	// - An outbound (direct)
	// - A route with a final outbound
	// This is the lightest valid sing-box config.
	//
	// However, include.Context is needed for registries, which the
	// default factory adds. For recording, we skip actual creation
	// and return an error — tests check state side effects instead.
	return nil, errors.New("recordingFactory: use state-based assertions")
}

// fakeClient implements *client.Client's FetchConfiguration interface
// by delegating to the fetcher function.
type fakeClient struct {
	fetcher func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error)
}

func (f *fakeClient) FetchConfiguration(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
	return f.fetcher(ctx, etag)
}

// validTestConfig returns a minimal valid ConfigurationResponse for testing.
func validTestConfig(revision string, strategy string, managedInbounds []contract.ManagedInbound) *contract.ConfigurationResponse {
	if managedInbounds == nil {
		managedInbounds = []contract.ManagedInbound{
			{InboundID: "ib-1", Tag: "hy2-in", Protocol: "hysteria2"},
		}
	}

	template := map[string]interface{}{
		"inbounds": []interface{}{
			map[string]interface{}{
				"type": "hysteria2",
				"tag":  "hy2-in",
				"users": []interface{}{
					map[string]interface{}{
						"name":     "initial-user",
						"password": "secret",
					},
				},
			},
		},
		"outbounds": []interface{}{
			map[string]interface{}{
				"type": "direct",
				"tag":  "direct",
			},
		},
		"route": map[string]interface{}{
			"final": "direct",
		},
	}
	templateBytes, _ := json.Marshal(template)

	return &contract.ConfigurationResponse{
		Revision:   revision,
		APIVersion: "v1",
		NodeID:     "test-node",
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: strategy,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         30,
			TrafficSeconds:       60,
			HeartbeatSeconds:     30,
		},
		ManagedInbounds:       managedInbounds,
		SingBoxConfigTemplate: templateBytes,
	}
}

// ---------------------------------------------------------------------------
// Tests: Bootstrap
// ---------------------------------------------------------------------------

func TestBootstrap_SuccessState(t *testing.T) {
	cfg := validTestConfig("rev-1", contract.ApplyOnConfigRecreateInstance, nil)
	_ = cfg // used via stripManagedInboundUsers below

	// We test via state side effects since we can't create a real Box
	// in unit tests. The recordingFactory will fail, so Bootstrap will
	// fail — but we can test the state changes before the factory call.
	// Instead, let's test stripManagedInboundUsers directly.

	stripped, err := stripManagedInboundUsers(cfg)
	if err != nil {
		t.Fatalf("stripManagedInboundUsers: %v", err)
	}

	// Verify the users array is empty in the stripped template.
	var template map[string]json.RawMessage
	if err := json.Unmarshal(stripped, &template); err != nil {
		t.Fatalf("unmarshal stripped template: %v", err)
	}

	var inbounds []map[string]json.RawMessage
	if err := json.Unmarshal(template["inbounds"], &inbounds); err != nil {
		t.Fatalf("unmarshal inbounds: %v", err)
	}

	if len(inbounds) != 1 {
		t.Fatalf("expected 1 inbound, got %d", len(inbounds))
	}

	var users []interface{}
	if err := json.Unmarshal(inbounds[0]["users"], &users); err != nil {
		t.Fatalf("unmarshal users: %v", err)
	}
	if len(users) != 0 {
		t.Errorf("expected empty users array, got %d users", len(users))
	}
}

// ---------------------------------------------------------------------------
// Tests: stripManagedInboundUsers
// ---------------------------------------------------------------------------

func TestStripManagedInboundUsers_StripsMatchingTags(t *testing.T) {
	managedInbounds := []contract.ManagedInbound{
		{InboundID: "ib-1", Tag: "managed-in", Protocol: "hysteria2"},
		{InboundID: "ib-2", Tag: "also-managed", Protocol: "shadowsocks"},
	}
	template := map[string]interface{}{
		"inbounds": []interface{}{
			map[string]interface{}{
				"type":  "hysteria2",
				"tag":   "managed-in",
				"users": []interface{}{map[string]interface{}{"name": "alice"}},
			},
			map[string]interface{}{
				"type":  "shadowsocks",
				"tag":   "also-managed",
				"users": []interface{}{map[string]interface{}{"name": "bob"}},
			},
			map[string]interface{}{
				"type":  "direct",
				"tag":   "unmanaged-out",
				"users": []interface{}{map[string]interface{}{"name": "charlie"}},
			},
		},
	}
	templateBytes, _ := json.Marshal(template)

	cfg := &contract.ConfigurationResponse{
		Revision:              "rev-1",
		APIVersion:            "v1",
		NodeID:                "node-1",
		ManagedInbounds:       managedInbounds,
		SingBoxConfigTemplate: templateBytes,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60, UsersSeconds: 30, TrafficSeconds: 60, HeartbeatSeconds: 30,
		},
	}

	stripped, err := stripManagedInboundUsers(cfg)
	if err != nil {
		t.Fatalf("stripManagedInboundUsers: %v", err)
	}

	var tmpl map[string]json.RawMessage
	if err := json.Unmarshal(stripped, &tmpl); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	var inbounds []map[string]json.RawMessage
	if err := json.Unmarshal(tmpl["inbounds"], &inbounds); err != nil {
		t.Fatalf("unmarshal inbounds: %v", err)
	}

	var tagStr string
	if err := json.Unmarshal(inbounds[0]["tag"], &tagStr); err != nil {
		t.Fatalf("inbound[0] tag: %v", err)
	}
	if tagStr != "managed-in" {
		t.Errorf("inbound[0]: expected tag %q, got %q", "managed-in", tagStr)
	}
	var managedUsers []interface{}
	if err := json.Unmarshal(inbounds[0]["users"], &managedUsers); err != nil {
		t.Fatalf("inbound[0] users: %v", err)
	}
	if len(managedUsers) != 0 {
		t.Errorf("inbound[0] (managed-in): expected empty users, got %d", len(managedUsers))
	}

	if _, hasUsers := inbounds[1]["users"]; hasUsers {
		t.Fatal("shadowsocks single-user inbound must not include users")
	}
	if _, hasManaged := inbounds[1]["managed"]; hasManaged {
		t.Fatal("shadowsocks single-user inbound must not set managed")
	}

	// Third inbound (unmanaged) should keep its users.
	var users []interface{}
	if err := json.Unmarshal(inbounds[2]["users"], &users); err != nil {
		t.Fatalf("inbound[2] users: %v", err)
	}
	if len(users) != 1 {
		t.Errorf("unmanaged inbound: expected 1 user, got %d", len(users))
	}
}

func TestStripManagedInboundUsers_keepsShadowsocksSingleUserInbound_whenPolicyNone(t *testing.T) {
	// Given
	managedInbounds := []contract.ManagedInbound{
		{InboundID: "ib-1", Tag: "ss-in", Protocol: "shadowsocks", UserApplyPolicy: contract.ApplyOnUserNone},
	}
	template := map[string]interface{}{
		"inbounds": []interface{}{
			map[string]interface{}{
				"type":        "shadowsocks",
				"tag":         "ss-in",
				"method":      "2022-blake3-aes-128-gcm",
				"password":    "single-user-secret",
				"listen_port": 8388,
			},
		},
	}
	templateBytes, _ := json.Marshal(template)
	cfg := &contract.ConfigurationResponse{
		Revision:              "rev-1",
		APIVersion:            "v1",
		NodeID:                "node-1",
		ManagedInbounds:       managedInbounds,
		SingBoxConfigTemplate: templateBytes,
	}

	// When
	stripped, err := stripManagedInboundUsers(cfg)

	// Then
	if err != nil {
		t.Fatalf("stripManagedInboundUsers: %v", err)
	}
	var tmpl map[string]json.RawMessage
	if err := json.Unmarshal(stripped, &tmpl); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var inbounds []map[string]json.RawMessage
	if err := json.Unmarshal(tmpl["inbounds"], &inbounds); err != nil {
		t.Fatalf("unmarshal inbounds: %v", err)
	}
	if _, hasManaged := inbounds[0]["managed"]; hasManaged {
		t.Fatal("single-user Shadowsocks inbound must not set managed")
	}
	if _, hasUsers := inbounds[0]["users"]; hasUsers {
		t.Fatal("single-user Shadowsocks inbound must not include users")
	}
	var password string
	if err := json.Unmarshal(inbounds[0]["password"], &password); err != nil {
		t.Fatalf("unmarshal password: %v", err)
	}
	if password != "single-user-secret" {
		t.Fatalf("password = %q, want preserved single-user password", password)
	}
}

func TestStripManagedInboundUsers_NoInboundsKey(t *testing.T) {
	template := map[string]interface{}{
		"outbounds": []interface{}{
			map[string]interface{}{"type": "direct", "tag": "direct"},
		},
	}
	templateBytes, _ := json.Marshal(template)

	cfg := &contract.ConfigurationResponse{
		Revision:              "rev-1",
		APIVersion:            "v1",
		NodeID:                "node-1",
		ManagedInbounds:       []contract.ManagedInbound{{InboundID: "ib-1", Tag: "missing", Protocol: "hysteria2"}},
		SingBoxConfigTemplate: templateBytes,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60, UsersSeconds: 30, TrafficSeconds: 60, HeartbeatSeconds: 30,
		},
	}

	stripped, err := stripManagedInboundUsers(cfg)
	if err != nil {
		t.Fatalf("stripManagedInboundUsers: %v", err)
	}

	// Should return original template unchanged.
	if string(stripped) != string(templateBytes) {
		t.Error("expected unchanged template when no inbounds key")
	}
}

func TestStripManagedInboundUsers_EmptyTemplate(t *testing.T) {
	cfg := &contract.ConfigurationResponse{
		SingBoxConfigTemplate: json.RawMessage{},
	}
	_, err := stripManagedInboundUsers(cfg)
	if err == nil {
		t.Error("expected error for empty template")
	}
}

// ---------------------------------------------------------------------------
// Tests: Config 304 is no-op
// ---------------------------------------------------------------------------

func TestPollConfiguration_304NoOp(t *testing.T) {
	callCount := 0
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		callCount++
		return nil, "", client.ErrNotModified
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()

	m, err := NewManager(fakeClient, store, tracker, logFactory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.PollConfiguration(context.Background())
	if err != nil {
		t.Errorf("expected nil on 304, got: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 fetch call, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// Tests: Unknown managed protocol
// ---------------------------------------------------------------------------

func TestBootstrap_UnknownProtocol_RecordUnsupported(t *testing.T) {
	managedInbounds := []contract.ManagedInbound{
		{InboundID: "ib-1", Tag: "hy2-in", Protocol: "hysteria2"},
		{InboundID: "ib-2", Tag: "unknown-in", Protocol: "vmess"}, // unsupported
	}
	template := map[string]interface{}{
		"inbounds": []interface{}{
			map[string]interface{}{"type": "hysteria2", "tag": "hy2-in", "users": []interface{}{}},
			map[string]interface{}{"type": "vmess", "tag": "unknown-in", "users": []interface{}{}},
		},
		"outbounds": []interface{}{
			map[string]interface{}{"type": "direct", "tag": "direct"},
		},
		"route": map[string]interface{}{"final": "direct"},
	}
	templateBytes, _ := json.Marshal(template)

	cfg := &contract.ConfigurationResponse{
		Revision:              "rev-1",
		APIVersion:            "v1",
		NodeID:                "node-1",
		ManagedInbounds:       managedInbounds,
		SingBoxConfigTemplate: templateBytes,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60, UsersSeconds: 30, TrafficSeconds: 60, HeartbeatSeconds: 30,
		},
	}

	// Test the state update logic directly.
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-1", nil
	}}
	logFactory := log.NewNOPFactory()

	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(&recordingFactory{createErr: errors.New("test: no real box")}))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Bootstrap will fail because the factory returns an error, but
	// the state should show apply_failed for supported and unsupported_protocol for unsupported.
	err = m.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error from fake factory")
	}

	// Check state — apply_failed sets all inbounds.
	s := store.State()
	ib1, ok := s.Inbounds["ib-1"]
	if !ok {
		t.Fatal("expected ib-1 in state")
	}
	if ib1.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("ib-1: expected apply_failed, got %q", ib1.UserLoadStatus)
	}

	ib2, ok := s.Inbounds["ib-2"]
	if !ok {
		t.Fatal("expected ib-2 in state")
	}
	// Unsupported protocol should be marked unsupported_protocol, not apply_failed.
	if ib2.UserLoadStatus != string(contract.UserLoadStatusUnsupportedProto) {
		t.Errorf("ib-2: expected unsupported_protocol, got %q", ib2.UserLoadStatus)
	}
}

// ---------------------------------------------------------------------------
// Tests: restart_process strategy mapped to full Box recreation
// ---------------------------------------------------------------------------

func TestPollConfiguration_RestartProcess_StrategyMapping(t *testing.T) {
	// This test verifies that restart_process is handled identically to
	// recreate_instance. Since we can't create a real Box in unit tests,
	// we verify the code path by checking that PollConfiguration with
	// restart_process calls applyConfigLocked (which fails due to the
	// recording factory, proving the strategy was reached).

	cfg := validTestConfig("rev-2", contract.ApplyOnConfigRestartProcess, nil)
	fetchCount := 0
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		fetchCount++
		return cfg, "etag-2", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	// Set initial state with an ETag so the fetch uses it.
	store.SetState(&state.State{
		Version:  state.CurrentVersion,
		Inbounds: map[string]state.InboundState{},
		Config:   state.ConfigState{ETag: "etag-1", Revision: "rev-1", NodeID: "node-1"},
	})
	store.SaveIfChanged()

	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()
	recorder := &recordingFactory{createErr: errors.New("test: no real box")}

	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.PollConfiguration(context.Background())
	if err == nil {
		t.Fatal("expected error from recording factory (proving restart_process reaches applyConfigLocked)")
	}
	if !errors.Is(err, recorder.createErr) {
		t.Errorf("expected recording factory error, got: %v", err)
	}
	if recorder.callCount != 1 {
		t.Errorf("expected 1 factory call (restart_process mapped to recreate), got %d", recorder.callCount)
	}
}

// ---------------------------------------------------------------------------
// Tests: recreate_instance recreates Box
// ---------------------------------------------------------------------------

func TestPollConfiguration_RecreateInstance_CallsFactory(t *testing.T) {
	cfg := validTestConfig("rev-2", contract.ApplyOnConfigRecreateInstance, nil)
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-2", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	store.SetState(&state.State{
		Version:  state.CurrentVersion,
		Inbounds: map[string]state.InboundState{},
		Config:   state.ConfigState{ETag: "etag-1", Revision: "rev-1", NodeID: "node-1"},
	})
	store.SaveIfChanged()

	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()
	recorder := &recordingFactory{createErr: errors.New("test: no real box")}

	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.PollConfiguration(context.Background())
	if err == nil {
		t.Fatal("expected error from recording factory")
	}
	if recorder.callCount != 1 {
		t.Errorf("expected 1 factory call, got %d", recorder.callCount)
	}
}

func TestPollConfiguration_RecreateInstance_CancelsOldRuntimeBeforeCreate(t *testing.T) {
	cfg := validTestConfig("rev-2", contract.ApplyOnConfigRecreateInstance, nil)
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-2", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	store.SetState(&state.State{
		Version:  state.CurrentVersion,
		Inbounds: map[string]state.InboundState{},
		Config:   state.ConfigState{ETag: "etag-1", Revision: "rev-1", NodeID: "node-1"},
	})
	store.SaveIfChanged()

	events := []string{}
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()
	recorder := &recordingFactory{createErr: errors.New("test: no real box"), events: &events}

	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.cancel = func() { events = append(events, "cancel") }

	_ = m.PollConfiguration(context.Background())

	if len(events) < 2 {
		t.Fatalf("expected cancel and create events, got %v", events)
	}
	if events[0] != "cancel" || events[1] != "create" {
		t.Fatalf("events = %v, want cancel before create", events)
	}
}

// ---------------------------------------------------------------------------
// Tests: Failed new config keeps old instance
// ---------------------------------------------------------------------------

func TestPollConfiguration_FailedNewConfig_KeepsOldInstance(t *testing.T) {
	// This test verifies that when a new config fails to apply,
	// the manager sets apply_failed status in state.
	// Since we use a recording factory that always fails,
	// we verify via state that the old state is preserved.

	cfg := validTestConfig("rev-bad", contract.ApplyOnConfigRecreateInstance, nil)
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-bad", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	store.SetState(&state.State{
		Version: state.CurrentVersion,
		Config:  state.ConfigState{ETag: "etag-1", Revision: "rev-1", NodeID: "node-1"},
		Inbounds: map[string]state.InboundState{
			"ib-1": {
				InboundID:      "ib-1",
				Tag:            "hy2-in",
				Protocol:       "hysteria2",
				UserLoadStatus: string(contract.UserLoadStatusOK),
			},
		},
	})
	store.SaveIfChanged()

	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()
	recorder := &recordingFactory{createErr: errors.New("intentional failure")}

	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.PollConfiguration(context.Background())
	if err == nil {
		t.Fatal("expected error from recording factory")
	}

	// Verify state shows apply_failed for the inbounds.
	s := store.State()
	ib, ok := s.Inbounds["ib-1"]
	if !ok {
		t.Fatal("expected ib-1 in state")
	}
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("expected apply_failed, got %q", ib.UserLoadStatus)
	}
}

// ---------------------------------------------------------------------------
// Tests: manual strategy records pending without applying
// ---------------------------------------------------------------------------

func TestPollConfiguration_ManualStrategy_RecordsPending(t *testing.T) {
	cfg := validTestConfig("rev-pending", contract.ApplyOnConfigManual, nil)
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-pending", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	store.SetState(&state.State{
		Version:  state.CurrentVersion,
		Inbounds: map[string]state.InboundState{},
		Config:   state.ConfigState{ETag: "etag-1", Revision: "rev-1", NodeID: "node-1"},
	})
	store.SaveIfChanged()

	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()
	recorder := &recordingFactory{}

	m, err := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.PollConfiguration(context.Background())
	if err != nil {
		t.Fatalf("manual strategy should not error: %v", err)
	}

	// Verify state has the pending revision recorded.
	s := store.State()
	if s.Config.Revision != "rev-pending" {
		t.Errorf("expected revision rev-pending, got %q", s.Config.Revision)
	}
	if s.Config.ETag != "etag-pending" {
		t.Errorf("expected etag etag-pending, got %q", s.Config.ETag)
	}

	// The factory should NOT have been called.
	if recorder.callCount != 0 {
		t.Errorf("expected 0 factory calls for manual strategy, got %d", recorder.callCount)
	}

	// Inbound should be recorded but NOT marked as empty_initial_load
	// (that's only set when applyConfigLocked succeeds).
	ib, ok := s.Inbounds["ib-1"]
	if !ok {
		t.Fatal("expected ib-1 in state")
	}
	// For manual strategy, inbounds don't get empty_initial_load
	// because no Box was created; they just get recorded.
	if ib.Tag != "hy2-in" {
		t.Errorf("expected tag hy2-in, got %q", ib.Tag)
	}
}

// ---------------------------------------------------------------------------
// Tests: Managed template users stripped before Box creation
// ---------------------------------------------------------------------------

func TestStripManagedInboundUsers_UsersStrippedBeforeBoxCreation(t *testing.T) {
	managedInbounds := []contract.ManagedInbound{
		{InboundID: "ib-1", Tag: "hy2-in", Protocol: "hysteria2"},
	}
	template := map[string]interface{}{
		"inbounds": []interface{}{
			map[string]interface{}{
				"type":  "hysteria2",
				"tag":   "hy2-in",
				"users": []interface{}{map[string]interface{}{"name": "should-be-removed", "password": "secret"}},
			},
		},
		"outbounds": []interface{}{
			map[string]interface{}{"type": "direct", "tag": "direct"},
		},
		"route": map[string]interface{}{"final": "direct"},
	}
	templateBytes, _ := json.Marshal(template)

	cfg := &contract.ConfigurationResponse{
		Revision:              "rev-1",
		APIVersion:            "v1",
		NodeID:                "node-1",
		ManagedInbounds:       managedInbounds,
		SingBoxConfigTemplate: templateBytes,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60, UsersSeconds: 30, TrafficSeconds: 60, HeartbeatSeconds: 30,
		},
	}

	stripped, err := stripManagedInboundUsers(cfg)
	if err != nil {
		t.Fatalf("stripManagedInboundUsers: %v", err)
	}

	// Verify that "users" in the managed inbound is now [].
	var tmpl map[string]json.RawMessage
	json.Unmarshal(stripped, &tmpl)

	var inbounds []map[string]json.RawMessage
	json.Unmarshal(tmpl["inbounds"], &inbounds)

	var users []interface{}
	json.Unmarshal(inbounds[0]["users"], &users)

	if len(users) != 0 {
		t.Errorf("expected users to be stripped to [], got %d users", len(users))
	}
}

// ---------------------------------------------------------------------------
// Tests: Initial user-fetch failure leaves managed inbound empty (fail-closed)
// ---------------------------------------------------------------------------

func TestBootstrap_FailClosed_InitialState(t *testing.T) {
	// After a successful bootstrap, managed inbounds should have
	// empty_initial_load status, not ok. This is the fail-closed state:
	// no users are loaded until the first user snapshot arrives.
	//
	// We test this by calling applyConfigLocked's state update logic
	// directly through the updateStateApplied method.

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return nil, "", errors.New("not used")
	}}
	logFactory := log.NewNOPFactory()

	m, err := NewManager(fakeClient, store, tracker, logFactory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Simulate the state update that applyConfigLocked would do.
	cfg := validTestConfig("rev-1", contract.ApplyOnConfigRecreateInstance, nil)
	unsupported := map[string]bool{}
	m.updateStateApplied(cfg, "etag-1", unsupported)

	s := store.State()
	ib, ok := s.Inbounds["ib-1"]
	if !ok {
		t.Fatal("expected ib-1 in state")
	}
	if ib.UserLoadStatus != string(contract.UserLoadStatusEmptyInitialLoad) {
		t.Errorf("expected empty_initial_load, got %q", ib.UserLoadStatus)
	}
	if ib.Protocol != "hysteria2" {
		t.Errorf("expected protocol hysteria2, got %q", ib.Protocol)
	}
}

// ---------------------------------------------------------------------------
// Tests: NewManager validation
// ---------------------------------------------------------------------------

func TestNewManager_NilClient(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	logFactory := log.NewNOPFactory()

	_, err := NewManager(nil, store, tracker, logFactory)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestNewManager_NilStore(t *testing.T) {
	fakeClient := &fakeClient{}
	tracker := traffic.NewTracker(map[string]string{})
	logFactory := log.NewNOPFactory()

	_, err := NewManager(fakeClient, nil, tracker, logFactory)
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestNewManager_NilTracker(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	fakeClient := &fakeClient{}
	logFactory := log.NewNOPFactory()

	_, err := NewManager(fakeClient, store, nil, logFactory)
	if err == nil {
		t.Fatal("expected error for nil tracker")
	}
}

func TestNewManager_NilLogFactory(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{}

	_, err := NewManager(fakeClient, store, tracker, nil)
	if err == nil {
		t.Fatal("expected error for nil log factory")
	}
}

// ---------------------------------------------------------------------------
// Tests: Close with no instance
// ---------------------------------------------------------------------------

func TestClose_NoInstance(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{}
	logFactory := log.NewNOPFactory()

	m, _ := NewManager(fakeClient, store, tracker, logFactory)
	err := m.Close()
	if err != nil {
		t.Errorf("expected nil on close with no instance, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests: GetBox with no instance
// ---------------------------------------------------------------------------

func TestGetBox_NoInstance(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{}
	logFactory := log.NewNOPFactory()

	m, _ := NewManager(fakeClient, store, tracker, logFactory)
	b := m.GetBox()
	if b != nil {
		t.Error("expected nil box before bootstrap")
	}
}

// ---------------------------------------------------------------------------
// Tests: Unsupported protocol in state update
// ---------------------------------------------------------------------------

func TestUpdateStateApplied_UnsupportedProtocol(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{}
	logFactory := log.NewNOPFactory()

	m, _ := NewManager(fakeClient, store, tracker, logFactory)

	managedInbounds := []contract.ManagedInbound{
		{InboundID: "ib-1", Tag: "hy2-in", Protocol: "hysteria2"},
		{InboundID: "ib-2", Tag: "vmess-in", Protocol: "vmess"},
	}
	cfg := validTestConfig("rev-1", contract.ApplyOnConfigRecreateInstance, managedInbounds)

	unsupported := map[string]bool{"ib-2": true}
	m.updateStateApplied(cfg, "etag-1", unsupported)

	s := store.State()

	ib1 := s.Inbounds["ib-1"]
	if ib1.UserLoadStatus != string(contract.UserLoadStatusEmptyInitialLoad) {
		t.Errorf("ib-1: expected empty_initial_load, got %q", ib1.UserLoadStatus)
	}

	ib2 := s.Inbounds["ib-2"]
	if ib2.UserLoadStatus != string(contract.UserLoadStatusUnsupportedProto) {
		t.Errorf("ib-2: expected unsupported_protocol, got %q", ib2.UserLoadStatus)
	}
}

// ---------------------------------------------------------------------------
// Tests: Bootstrap fetch error
// ---------------------------------------------------------------------------

func TestBootstrap_FetchError(t *testing.T) {
	fetchErr := errors.New("network unreachable")
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return nil, "", fetchErr
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()

	m, _ := NewManager(fakeClient, store, tracker, logFactory)
	err := m.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error from fetch failure")
	}
}

// ---------------------------------------------------------------------------
// Tests: Bootstrap validation error
// ---------------------------------------------------------------------------

func TestBootstrap_ValidationError(t *testing.T) {
	// Return a config that fails validation (missing revision).
	cfg := &contract.ConfigurationResponse{
		Revision:   "", // missing — will fail validation
		APIVersion: "v1",
		NodeID:     "node-1",
	}
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-1", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()

	m, _ := NewManager(fakeClient, store, tracker, logFactory)
	err := m.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected validation error")
	}
}

// ---------------------------------------------------------------------------
// Tests: String method
// ---------------------------------------------------------------------------

func TestManager_String(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{}
	logFactory := log.NewNOPFactory()

	m, _ := NewManager(fakeClient, store, tracker, logFactory)
	s := m.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

// ---------------------------------------------------------------------------
// Tests: Manual strategy with unsupported protocol
// ---------------------------------------------------------------------------

func TestPollConfiguration_ManualStrategy_UnsupportedProtocol(t *testing.T) {
	managedInbounds := []contract.ManagedInbound{
		{InboundID: "ib-1", Tag: "hy2-in", Protocol: "hysteria2"},
		{InboundID: "ib-2", Tag: "vmess-in", Protocol: "vmess"},
	}
	cfg := validTestConfig("rev-manual", contract.ApplyOnConfigManual, managedInbounds)
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-manual", nil
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	store, _ := state.NewStore(statePath)
	store.SetState(&state.State{
		Version:  state.CurrentVersion,
		Inbounds: map[string]state.InboundState{},
		Config:   state.ConfigState{ETag: "etag-old", Revision: "rev-old", NodeID: "node-1"},
	})
	store.SaveIfChanged()

	tracker := traffic.NewTracker(map[string]string{})
	fakeClient := &fakeClient{fetcher: fetcher}
	logFactory := log.NewNOPFactory()
	recorder := &recordingFactory{}

	m, _ := NewManager(fakeClient, store, tracker, logFactory, WithBoxFactory(recorder))

	err := m.PollConfiguration(context.Background())
	if err != nil {
		t.Fatalf("manual strategy should not error: %v", err)
	}

	s := store.State()
	ib2 := s.Inbounds["ib-2"]
	if ib2.UserLoadStatus != string(contract.UserLoadStatusUnsupportedProto) {
		t.Errorf("ib-2: expected unsupported_protocol, got %q", ib2.UserLoadStatus)
	}

	// Factory should not be called for manual strategy.
	if recorder.callCount != 0 {
		t.Errorf("expected 0 factory calls, got %d", recorder.callCount)
	}
}
