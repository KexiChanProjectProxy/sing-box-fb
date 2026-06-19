package adapter

import (
	"errors"
	"fmt"
)

// ErrInboundNotFound is returned when the requested inbound tag does not exist.
var ErrInboundNotFound = errors.New("inbound not found")

// ErrInboundNotManaged is returned when the inbound does not support managed users.
var ErrInboundNotManaged = errors.New("inbound does not support managed users")

// ManagedUserCredential represents a user's authentication credential
// in the adapter-internal format. It is protocol-agnostic.
type ManagedUserCredential struct {
	UserID   string
	Name     string
	Password string
}

// ManagedUser represents one managed user for an inbound.
type ManagedUser struct {
	UserID     string
	Name       string
	Credential ManagedUserCredential
}

// ManagedUserInbound is an inbound that supports managed user replacement.
type ManagedUserInbound interface {
	Inbound
	// ReplaceUsers replaces the entire managed user set.
	// Absent users are removed; an empty slice means no users allowed.
	ReplaceUsers(users []ManagedUser) error
}

// ErrInboundNotFoundTag wraps ErrInboundNotFound with the tag that was not found.
func ErrInboundNotFoundTag(tag string) error {
	return fmt.Errorf("%w: %s", ErrInboundNotFound, tag)
}

// ErrInboundNotManagedTag wraps ErrInboundNotManaged with the tag that is not managed.
func ErrInboundNotManagedTag(tag string) error {
	return fmt.Errorf("%w: %s", ErrInboundNotManaged, tag)
}
