package users

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

func TestPollInbound_acceptsSnapshotWithoutNodeID_whenInboundMatchesAppliedConfig(t *testing.T) {
	// Given
	p, fetcher, replacer, store := newTestPoller()
	fetcher.snapshot = makeSnapshot("u-rev-1", testConfigRev, "", testInboundID, "hysteria2", []contract.User{
		passwordUser("u1", "alice", "pass1"),
	})
	fetcher.etag = "etag-ss"

	// When
	err := p.PollInbound(context.Background(), testInboundID, makeManagedInbound("hysteria2", contract.ApplyOnUserHotReloadUsers), testConfigRev)

	// Then
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !replacer.called {
		t.Fatal("ReplaceInboundUsers not called")
	}
	if len(replacer.users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(replacer.users))
	}
	ib := inboundState(store, testInboundID)
	if ib.UserLoadStatus != string(contract.UserLoadStatusOK) {
		t.Fatalf("status = %q, want %q", ib.UserLoadStatus, contract.UserLoadStatusOK)
	}
}
