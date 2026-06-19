package shadowsocks

import (
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestReplaceUsersBasic(t *testing.T) {
	h := &MultiInbound{
		users: []option.ShadowsocksUser{{Name: "static-user"}},
	}

	users := []adapter.ManagedUser{
		{UserID: "u1", Name: "alice", Credential: adapter.ManagedUserCredential{Password: "pass1"}},
		{UserID: "u2", Name: "bob", Credential: adapter.ManagedUserCredential{Password: "pass2"}},
	}

	err := h.ReplaceUsers(users)
	require.NoError(t, err)

	h.userLock.RLock()
	u := h.users
	h.userLock.RUnlock()

	require.Len(t, u, 2)
	require.Equal(t, "alice", u[0].Name)
	require.Equal(t, "bob", u[1].Name)
}

func TestReplaceUsersEmptyRemovesAll(t *testing.T) {
	h := &MultiInbound{
		users: []option.ShadowsocksUser{{Name: "static-user"}},
	}

	err := h.ReplaceUsers([]adapter.ManagedUser{})
	require.NoError(t, err)

	h.userLock.RLock()
	u := h.users
	h.userLock.RUnlock()

	require.Len(t, u, 0)
}

func TestReplaceUsersUserIDAsNameFallback(t *testing.T) {
	h := &MultiInbound{}

	users := []adapter.ManagedUser{
		{UserID: "user-123", Name: "", Credential: adapter.ManagedUserCredential{Password: "pass"}},
	}

	err := h.ReplaceUsers(users)
	require.NoError(t, err)

	h.userLock.RLock()
	u := h.users
	h.userLock.RUnlock()

	require.Len(t, u, 1)
	require.Equal(t, "user-123", u[0].Name) // UserID used when Name is empty
}

func TestReplaceUsersAuthoritativeSnapshot(t *testing.T) {
	h := &MultiInbound{}

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
	u := h.users
	h.userLock.RUnlock()

	require.Len(t, u, 1)
	require.Equal(t, "alice-updated", u[0].Name)
}

func TestReplaceUsersConcurrent(t *testing.T) {
	h := &MultiInbound{}

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
	_ = h.users
	h.userLock.RUnlock()
}

func TestMultiInboundImplementsManagedUserInbound(t *testing.T) {
	var _ adapter.ManagedUserInbound = (*MultiInbound)(nil)
}
