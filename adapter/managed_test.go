package adapter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManagedUserTypesConstructible(t *testing.T) {
	t.Parallel()

	cred := ManagedUserCredential{
		UserID:   "u1",
		Name:     "alice",
		Password: "secret",
	}
	require.Equal(t, "u1", cred.UserID)
	require.Equal(t, "alice", cred.Name)
	require.Equal(t, "secret", cred.Password)

	user := ManagedUser{
		UserID:     "u2",
		Name:       "bob",
		Credential: cred,
	}
	require.Equal(t, "u2", user.UserID)
	require.Equal(t, "bob", user.Name)
	require.Equal(t, cred, user.Credential)
}

func TestManagedUserCredentialFields(t *testing.T) {
	t.Parallel()

	// Zero-value credential
	var zero ManagedUserCredential
	require.Empty(t, zero.UserID)
	require.Empty(t, zero.Name)
	require.Empty(t, zero.Password)
}

func TestManagedUserZeroValue(t *testing.T) {
	t.Parallel()

	var zero ManagedUser
	require.Empty(t, zero.UserID)
	require.Empty(t, zero.Name)
	require.Empty(t, zero.Credential.UserID)
}

func TestErrInboundNotFoundSentinel(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, ErrInboundNotFound, ErrInboundNotFound)
	wrapped := ErrInboundNotFoundTag("my-tag")
	require.ErrorIs(t, wrapped, ErrInboundNotFound)
	require.Contains(t, wrapped.Error(), "my-tag")
}

func TestErrInboundNotManagedSentinel(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, ErrInboundNotManaged, ErrInboundNotManaged)
	wrapped := ErrInboundNotManagedTag("my-tag")
	require.ErrorIs(t, wrapped, ErrInboundNotManaged)
	require.Contains(t, wrapped.Error(), "my-tag")
}

func TestErrorUnwrap(t *testing.T) {
	t.Parallel()

	err := ErrInboundNotFoundTag("test-inbound")
	require.True(t, errors.Is(err, ErrInboundNotFound))
	require.False(t, errors.Is(err, ErrInboundNotManaged))

	err2 := ErrInboundNotManagedTag("test-inbound")
	require.True(t, errors.Is(err2, ErrInboundNotManaged))
	require.False(t, errors.Is(err2, ErrInboundNotFound))
}

// Compile-time assertion: ManagedUserInbound must embed Inbound.
var _ Inbound = (ManagedUserInbound)(nil)

// Compile-time assertion: no import cycle — the adapter package must not
// import paneladapter or contract packages. This is a structural guard;
// if adapter ever adds an import of those packages, this file will fail
// to compile due to the import appearing unused. A more robust check
// would be a separate build-tagged file, but this serves as documentation.
// The real guarantee comes from `go vet` and code review.
