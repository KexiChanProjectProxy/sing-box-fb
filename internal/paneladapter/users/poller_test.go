package users

import (
	"context"
	"errors"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"

	"github.com/sagernet/sing/common/logger"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// mockFetcher captures FetchUsers calls.
type mockFetcher struct {
	snapshot *contract.UserSnapshot
	etag     string
	err      error
	called   bool
}

func (m *mockFetcher) FetchUsers(_ context.Context, inboundID, etag, appliedConfigRev string) (*contract.UserSnapshot, string, error) {
	m.called = true
	return m.snapshot, m.etag, m.err
}

// mockReplacer captures ReplaceInboundUsers calls.
type mockReplacer struct {
	users   []adapter.ManagedUser
	err     error
	called  bool
	lastTag string
}

func (m *mockReplacer) ReplaceInboundUsers(tag string, users []adapter.ManagedUser) error {
	m.called = true
	m.lastTag = tag
	m.users = users
	return m.err
}

// newTestPoller creates a Poller with mock deps for testing.
func newTestPoller() (*Poller, *mockFetcher, *mockReplacer, *state.Store) {
	st := &state.State{
		Version:  state.CurrentVersion,
		Inbounds: make(map[string]state.InboundState),
	}
	st.Config.NodeID = "node-1"

	s := &state.Store{}
	s.SetState(st)

	fetcher := &mockFetcher{}
	replacer := &mockReplacer{}

	p := NewPoller(fetcher, s, replacer, logger.NOP())
	return p, fetcher, replacer, s
}

// helper to get inbound state from the store.
func inboundState(s *state.Store, id string) state.InboundState {
	return s.State().Inbounds[id]
}

// ---------------------------------------------------------------------------
// Shared test data
// ---------------------------------------------------------------------------

var (
	testNodeID    = "node-1"
	testConfigRev = "cfg-rev-1"
	testInboundID = "inbound-1"
	testTag       = "in-hy2"
)

func makeManagedInbound(protocol, policy string) contract.ManagedInbound {
	return contract.ManagedInbound{
		InboundID:       testInboundID,
		Tag:             testTag,
		Protocol:        protocol,
		UserApplyPolicy: policy,
	}
}

func makeSnapshot(rev, configRev, nodeID, inboundID, protocol string, users []contract.User) *contract.UserSnapshot {
	return &contract.UserSnapshot{
		Revision:              rev,
		ConfigurationRevision: configRev,
		NodeID:                nodeID,
		InboundID:             inboundID,
		Protocol:              protocol,
		Users:                 users,
	}
}

func passwordUser(id, name, password string) contract.User {
	return contract.User{
		UserID: id,
		Name:   name,
		Credential: contract.Credential{
			Type:     contract.CredentialTypePassword,
			Password: password,
		},
	}
}

// ---------------------------------------------------------------------------
// Tests: 200 snapshot full replacement (all 3 protocols)
// ---------------------------------------------------------------------------

func TestPollInbound_200_Hysteria2(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
		passwordUser("u2", "bob", "pass2"),
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !replacer.called {
		t.Fatal("ReplaceInboundUsers not called")
	}
	if len(replacer.users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(replacer.users))
	}
	if replacer.lastTag != testTag {
		t.Fatalf("expected tag %q, got %q", testTag, replacer.lastTag)
	}

	ib := inboundState(store, testInboundID)
	if ib.UserETag != "etag-v1" {
		t.Errorf("etag = %q, want %q", ib.UserETag, "etag-v1")
	}
	if ib.UserRevision != "u-rev-1" {
		t.Errorf("revision = %q, want %q", ib.UserRevision, "u-rev-1")
	}
	if ib.UserCount != 2 {
		t.Errorf("user_count = %d, want 2", ib.UserCount)
	}
	if ib.UserLoadStatus != string(contract.UserLoadStatusOK) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusOK)
	}
}

func TestPollInbound_200_AnyTLS(t *testing.T) {
	p, fetcher, replacer, _ := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "anytls", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-anytls"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("anytls", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(replacer.users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(replacer.users))
	}
}

func TestPollInbound_200_Shadowsocks(t *testing.T) {
	p, fetcher, replacer, _ := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "shadowsocks", []contract.User{
		passwordUser("u1", "alice", "pass1"),
		passwordUser("u2", "bob", "pass2"),
		passwordUser("u3", "carol", "pass3"),
	})
	fetcher.etag = "etag-ss"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("shadowsocks", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(replacer.users) != 3 {
		t.Fatalf("expected 3 users, got %d", len(replacer.users))
	}
}

// ---------------------------------------------------------------------------
// Tests: 304 keeps users
// ---------------------------------------------------------------------------

func TestPollInbound_304_KeepsUsers(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()

	// Set up pre-existing state with users already applied.
	st := store.State().Clone()
	st.Inbounds[testInboundID] = state.InboundState{
		InboundID:      testInboundID,
		Tag:            testTag,
		Protocol:       "hysteria2",
		UserETag:       "etag-v1",
		UserRevision:   "u-rev-1",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      2,
	}
	store.SetState(st)

	fetcher.err = client.ErrNotModified

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called on 304")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusOK) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusOK)
	}
	if ib.UserCount != 2 {
		t.Errorf("user_count = %d, want 2 (preserved)", ib.UserCount)
	}
}

// ---------------------------------------------------------------------------
// Tests: Omitted user removed (authoritative snapshot)
// ---------------------------------------------------------------------------

func TestPollInbound_AuthoritativeSnapshot_RemovesOmittedUser(t *testing.T) {
	p, fetcher, replacer, _ := newTestPoller()

	// First poll: 2 users.
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
		passwordUser("u2", "bob", "pass2"),
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("first poll: unexpected error: %v", err)
	}
	if len(replacer.users) != 2 {
		t.Fatalf("first poll: expected 2 users, got %d", len(replacer.users))
	}

	// Second poll: 1 user (bob removed).
	fetcher.snapshot = makeSnapshot("u-rev-2", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-v2"

	err = p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("second poll: unexpected error: %v", err)
	}
	if len(replacer.users) != 1 {
		t.Fatalf("second poll: expected 1 user, got %d", len(replacer.users))
	}
	if replacer.users[0].UserID != "u1" {
		t.Errorf("remaining user = %q, want %q", replacer.users[0].UserID, "u1")
	}
}

// ---------------------------------------------------------------------------
// Tests: Empty list removes all
// ---------------------------------------------------------------------------

func TestPollInbound_EmptyList_RemovesAll(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()

	// First poll: 2 users.
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
		passwordUser("u2", "bob", "pass2"),
	})
	fetcher.etag = "etag-v1"

	_ = p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)

	// Second poll: empty list.
	fetcher.snapshot = makeSnapshot("u-rev-2", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{})
	fetcher.etag = "etag-v2"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !replacer.called {
		t.Fatal("ReplaceInboundUsers should be called with empty list")
	}
	if len(replacer.users) != 0 {
		t.Fatalf("expected 0 users, got %d", len(replacer.users))
	}

	ib := inboundState(store, testInboundID)
	if ib.UserCount != 0 {
		t.Errorf("user_count = %d, want 0", ib.UserCount)
	}
	if ib.UserLoadStatus != string(contract.UserLoadStatusOK) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusOK)
	}
}

// ---------------------------------------------------------------------------
// Tests: Initial 503 fail-closed (empty_initial_load)
// ---------------------------------------------------------------------------

func TestPollInbound_Initial503_FailClosed(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.err = &client.ResponseError{
		StatusCode: 503,
		Code:       "SERVICE_UNAVAILABLE",
		Title:      "Service Unavailable",
	}

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error on 503")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusEmptyInitialLoad) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusEmptyInitialLoad)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called on initial 503")
	}
}

// ---------------------------------------------------------------------------
// Tests: Subsequent 503 stale (keeps old users)
// ---------------------------------------------------------------------------

func TestPollInbound_Subsequent503_Stale(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()

	// Pre-populate state with previous successful load.
	st := store.State().Clone()
	st.Inbounds[testInboundID] = state.InboundState{
		InboundID:      testInboundID,
		Tag:            testTag,
		Protocol:       "hysteria2",
		UserETag:       "etag-v1",
		UserRevision:   "u-rev-1",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      2,
	}
	store.SetState(st)

	fetcher.err = &client.ResponseError{
		StatusCode: 503,
		Code:       "SERVICE_UNAVAILABLE",
		Title:      "Service Unavailable",
	}

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error on 503")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusStale) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusStale)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called on 503 stale")
	}
	// Old user count should be preserved.
	if ib.UserCount != 2 {
		t.Errorf("user_count = %d, want 2 (preserved)", ib.UserCount)
	}
}

// ---------------------------------------------------------------------------
// Tests: 409 triggers config refetch
// ---------------------------------------------------------------------------

func TestPollInbound_409_TriggersConfigRefetch(t *testing.T) {
	p, fetcher, _, store := newTestPoller()
	fetcher.err = &client.ResponseError{
		StatusCode: 409,
		Code:       "REVISION_CONFLICT",
		Title:      "Revision Conflict",
	}

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error on 409")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusRevisionConflict) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusRevisionConflict)
	}

	if !p.NeedsConfigRefetch() {
		t.Error("expected NeedsConfigRefetch to be true after 409")
	}
	// Second call should return false (flag cleared).
	if p.NeedsConfigRefetch() {
		t.Error("NeedsConfigRefetch should return false after being cleared")
	}
}

// ---------------------------------------------------------------------------
// Tests: Unsupported credential type reports apply_failed
// ---------------------------------------------------------------------------

func TestPollInbound_UnsupportedCredentialType_ApplyFailed(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{
		{
			UserID: "u1",
			Name:   "alice",
			Credential: contract.Credential{
				Type: "certificate", // unsupported
			},
		},
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error for unsupported credential type")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for unsupported credential type")
	}
}

// ---------------------------------------------------------------------------
// Tests: Protocol mismatch rejected before core call
// ---------------------------------------------------------------------------

func TestPollInbound_ProtocolMismatch_Rejected(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	// Snapshot says shadowsocks, but managed inbound says hysteria2.
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "shadowsocks", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error for protocol mismatch")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for protocol mismatch")
	}
}

// ---------------------------------------------------------------------------
// Tests: Unknown managed protocol never calls core replacement
// ---------------------------------------------------------------------------

func TestPollInbound_UnknownProtocol_NeverCallsCore(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	mi := makeManagedInbound("trojan", contract.ApplyOnUserHotReloadUsers)

	err := p.PollInbound(context.Background(), testInboundID, mi, testConfigRev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fetcher.called {
		t.Error("FetchUsers should not be called for unsupported protocol")
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for unsupported protocol")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusUnsupportedProto) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusUnsupportedProto)
	}
}

// ---------------------------------------------------------------------------
// Tests: Non-hot_reload_users strategy rejected
// ---------------------------------------------------------------------------

func TestPollInbound_NonHotReloadUsers_Rejected(t *testing.T) {
	p, _, replacer, store := newTestPoller()
	mi := makeManagedInbound("hysteria2", contract.ApplyOnUserRestartProcess)

	err := p.PollInbound(context.Background(), testInboundID, mi, testConfigRev)
	if err == nil {
		t.Fatal("expected error for non-hot_reload_users policy")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for non-hot_reload_users policy")
	}
}

func TestPollInbound_NonHotReloadUsers_RecreateInstance(t *testing.T) {
	p, _, replacer, store := newTestPoller()
	mi := makeManagedInbound("hysteria2", contract.ApplyOnUserRecreateInstance)

	err := p.PollInbound(context.Background(), testInboundID, mi, testConfigRev)
	if err == nil {
		t.Fatal("expected error for recreate_instance policy")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for recreate_instance policy")
	}
}

// ---------------------------------------------------------------------------
// Tests: Configuration revision mismatch triggers refetch
// ---------------------------------------------------------------------------

func TestPollInbound_ConfigRevisionMismatch_TriggersRefetch(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	// Snapshot has a different config revision than what's applied.
	fetcher.snapshot = makeSnapshot("u-rev-1", "cfg-rev-old", testNodeID, testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error for config revision mismatch")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusRevisionConflict) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusRevisionConflict)
	}
	if !p.NeedsConfigRefetch() {
		t.Error("expected NeedsConfigRefetch to be true after revision mismatch")
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called on revision conflict")
	}
}

// ---------------------------------------------------------------------------
// Tests: Node ID mismatch rejected
// ---------------------------------------------------------------------------

func TestPollInbound_NodeIDMismatch_Rejected(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, "wrong-node", testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error for node_id mismatch")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for node_id mismatch")
	}
}

// ---------------------------------------------------------------------------
// Tests: Inbound ID mismatch rejected
// ---------------------------------------------------------------------------

func TestPollInbound_InboundIDMismatch_Rejected(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, "wrong-inbound", "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-v1"

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error for inbound_id mismatch")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called for inbound_id mismatch")
	}
}

// ---------------------------------------------------------------------------
// Tests: ReplaceInboundUsers failure → apply_failed
// ---------------------------------------------------------------------------

func TestPollInbound_ReplaceFails_ApplyFailed(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, testNodeID, testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-v1"
	replacer.err = errors.New("inbound not managed")

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error when ReplaceInboundUsers fails")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusApplyFailed) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusApplyFailed)
	}
}

// ---------------------------------------------------------------------------
// Tests: Transport error on initial load → empty_initial_load
// ---------------------------------------------------------------------------

func TestPollInbound_TransportError_InitialLoad(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	fetcher.err = errors.New("connection refused")

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error on transport failure")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusEmptyInitialLoad) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusEmptyInitialLoad)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called on initial transport error")
	}
}

// ---------------------------------------------------------------------------
// Tests: Transport error on subsequent load → stale
// ---------------------------------------------------------------------------

func TestPollInbound_TransportError_SubsequentLoad(t *testing.T) {
	p, fetcher, replacer, store := newTestPoller()
	st := store.State().Clone()
	st.Inbounds[testInboundID] = state.InboundState{
		InboundID:      testInboundID,
		Tag:            testTag,
		Protocol:       "hysteria2",
		UserETag:       "etag-v1",
		UserRevision:   "u-rev-1",
		UserLoadStatus: string(contract.UserLoadStatusOK),
		UserCount:      3,
	}
	store.SetState(st)

	fetcher.err = errors.New("connection refused")

	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)
	if err == nil {
		t.Fatal("expected error on transport failure")
	}

	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusStale) {
		t.Errorf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusStale)
	}
	if replacer.called {
		t.Error("ReplaceInboundUsers should not be called on subsequent transport error")
	}
}

// ---------------------------------------------------------------------------
// Tests: User conversion correctness
// ---------------------------------------------------------------------------

func TestConvertUsers(t *testing.T) {
	snap := &contract.UserSnapshot{
		Users: []contract.User{
			passwordUser("u1", "alice", "pass1"),
			passwordUser("u2", "bob", "pass2"),
		},
	}

	users := convertUsers(snap)
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	if users[0].UserID != "u1" || users[0].Name != "alice" {
		t.Errorf("user[0] = {%q, %q}, want {u1, alice}", users[0].UserID, users[0].Name)
	}
	if users[0].Credential.UserID != "u1" || users[0].Credential.Name != "alice" || users[0].Credential.Password != "pass1" {
		t.Errorf("user[0].Credential = {%q, %q, %q}, want {u1, alice, pass1}",
			users[0].Credential.UserID, users[0].Credential.Name, users[0].Credential.Password)
	}

	if users[1].UserID != "u2" || users[1].Name != "bob" {
		t.Errorf("user[1] = {%q, %q}, want {u2, bob}", users[1].UserID, users[1].Name)
	}
	if users[1].Credential.Password != "pass2" {
		t.Errorf("user[1].Credential.Password = %q, want pass2", users[1].Credential.Password)
	}
}

func TestConvertUsers_Empty(t *testing.T) {
	snap := &contract.UserSnapshot{Users: []contract.User{}}
	users := convertUsers(snap)
	if len(users) != 0 {
		t.Errorf("expected 0 users, got %d", len(users))
	}
}

// ---------------------------------------------------------------------------
// Tests: MarkConfigRefetch / NeedsConfigRefetch
// ---------------------------------------------------------------------------

func TestNeedsConfigRefetch_Flag(t *testing.T) {
	p, _, _, _ := newTestPoller()

	if p.NeedsConfigRefetch() {
		t.Error("initial flag should be false")
	}

	p.MarkConfigRefetch()
	if !p.NeedsConfigRefetch() {
		t.Error("flag should be true after MarkConfigRefetch")
	}
	if p.NeedsConfigRefetch() {
		t.Error("flag should be cleared after first NeedsConfigRefetch")
	}
}
