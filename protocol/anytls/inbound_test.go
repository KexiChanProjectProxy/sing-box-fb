package anytls

import (
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

func TestReplaceUsersBasic(t *testing.T) {
	h := &Inbound{}

	users := []adapter.ManagedUser{
		{UserID: "u1", Name: "alice", Credential: adapter.ManagedUserCredential{Password: "pass1"}},
		{UserID: "u2", Name: "bob", Credential: adapter.ManagedUserCredential{Password: "pass2"}},
	}

	err := h.ReplaceUsers(users)
	require.NoError(t, err)

	h.userLock.RLock()
	names := h.managedNames
	h.userLock.RUnlock()

	require.Len(t, names, 2)
	require.Equal(t, "alice", names["u1"])
	require.Equal(t, "bob", names["u2"])
}

func TestReplaceUsersEmptyRemovesAll(t *testing.T) {
	h := &Inbound{
		managedNames: map[string]string{"old": "old-user"},
	}

	err := h.ReplaceUsers([]adapter.ManagedUser{})
	require.NoError(t, err)

	h.userLock.RLock()
	names := h.managedNames
	h.userLock.RUnlock()

	require.Len(t, names, 0)
}

func TestReplaceUsersUserIDAsNameFallback(t *testing.T) {
	h := &Inbound{}

	users := []adapter.ManagedUser{
		{UserID: "user-123", Name: "", Credential: adapter.ManagedUserCredential{Password: "pass"}},
	}

	err := h.ReplaceUsers(users)
	require.NoError(t, err)

	h.userLock.RLock()
	names := h.managedNames
	h.userLock.RUnlock()

	require.Len(t, names, 1)
	require.Equal(t, "user-123", names["user-123"]) // UserID used when Name is empty
}

func TestReplaceUsersAuthoritativeSnapshot(t *testing.T) {
	h := &Inbound{}

	// First replacement
	err := h.ReplaceUsers([]adapter.ManagedUser{
		{UserID: "u1", Name: "alice", Credential: adapter.ManagedUserCredential{Password: "p1"}},
		{UserID: "u2", Name: "bob", Credential: adapter.ManagedUserCredential{Password: "p2"}},
		{UserID: "u3", Name: "charlie", Credential: adapter.ManagedUserCredential{Password: "p3"}},
	})
	require.NoError(t, err)

	// Second replacement — authoritative snapshot, u2 and u3 are absent
	err = h.ReplaceUsers([]adapter.ManagedUser{
		{UserID: "u1", Name: "alice-updated", Credential: adapter.ManagedUserCredential{Password: "p1-new"}},
	})
	require.NoError(t, err)

	h.userLock.RLock()
	names := h.managedNames
	h.userLock.RUnlock()

	require.Len(t, names, 1)
	require.Equal(t, "alice-updated", names["u1"])
	// u2 and u3 are absent — removed
}

func TestReplaceUsersConcurrent(t *testing.T) {
	h := &Inbound{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			users := []adapter.ManagedUser{
				{UserID: "u1", Name: "alice", Credential: adapter.ManagedUserCredential{Password: "pass"}},
			}
			_ = h.ReplaceUsers(users)
		}(i)
	}
	wg.Wait()

	// Just verify no data race occurred — the -race detector will catch it
	h.userLock.RLock()
	_ = h.managedNames
	h.userLock.RUnlock()
}

func TestInboundImplementsManagedUserInbound(t *testing.T) {
	var _ adapter.ManagedUserInbound = (*Inbound)(nil)
}
