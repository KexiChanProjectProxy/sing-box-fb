// Package users implements the per-inbound user polling/apply state machine
// for the sing-box panel adapter. It fetches user snapshots from the panel
// API, validates them, converts credentials, and applies them to the
// running inbound via Box.ReplaceInboundUsers.
//
// The Poller is synchronous — it does not start goroutines or timers.
// The runtime Manager (T8) calls PollInbound on each poll tick.
package users

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/log"

	E "github.com/sagernet/sing/common/exceptions"
)

// ---------------------------------------------------------------------------
// UserFetcher — abstraction for testability
// ---------------------------------------------------------------------------

// UserFetcher abstracts the panel client operations needed by the Poller.
// The production implementation is *client.Client.
type UserFetcher interface {
	// FetchUsers retrieves the user list for an inbound from the panel.
	// Returns (snapshot, newETag, error). On 304, returns client.ErrNotModified.
	FetchUsers(ctx context.Context, inboundID, etag, appliedConfigRev string) (*contract.UserSnapshot, string, error)
}

// Compile-time check that *client.Client satisfies UserFetcher.
var _ UserFetcher = (*client.Client)(nil)

// ---------------------------------------------------------------------------
// UserReplacer — abstraction for testability
// ---------------------------------------------------------------------------

// UserReplacer abstracts the Box.ReplaceInboundUsers operation.
type UserReplacer interface {
	// ReplaceInboundUsers replaces the managed user set for an inbound by tag.
	ReplaceInboundUsers(tag string, users []adapter.ManagedUser) error
}

// ---------------------------------------------------------------------------
// Option — functional options for Poller
// ---------------------------------------------------------------------------

// Option configures a Poller during construction.
type Option func(*Poller)

// ---------------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------------

// Poller implements the per-inbound user polling/apply state machine.
// It is called synchronously by the runtime Manager on each user-poll tick.
type Poller struct {
	fetcher       UserFetcher
	store         *state.Store
	box           UserReplacer
	logger        log.ContextLogger
	configRefetch atomic.Bool
}

// NewPoller creates a user polling/apply state machine.
func NewPoller(fetcher UserFetcher, s *state.Store, box UserReplacer, logger log.ContextLogger, opts ...Option) *Poller {
	p := &Poller{
		fetcher: fetcher,
		store:   s,
		box:     box,
		logger:  logger,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// MarkConfigRefetch sets a flag to trigger config refetch.
func (p *Poller) MarkConfigRefetch() {
	p.configRefetch.Store(true)
}

// NeedsConfigRefetch checks and clears the config refetch flag.
func (p *Poller) NeedsConfigRefetch() bool {
	return p.configRefetch.Swap(false)
}

// PollInbound fetches and applies users for one managed inbound.
//
// The configRevision parameter is the currently applied configuration
// revision, used to detect revision conflicts.
func (p *Poller) PollInbound(ctx context.Context, inboundID string, managedInbound contract.ManagedInbound, configRevision string) error {
	// Step 1: Get current inbound state from store.
	curState := p.store.State()
	ibState, hasPrev := curState.Inbounds[inboundID]
	etag := ibState.UserETag
	prevRevision := ibState.UserRevision

	// Step 2: Check if protocol is supported.
	if !contract.IsSupportedProtocol(managedInbound.Protocol) {
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusUnsupportedProto))
		p.logger.DebugContext(ctx, "unsupported protocol for inbound ", inboundID, ": ", managedInbound.Protocol)
		return nil
	}

	if managedInbound.UserApplyPolicy == contract.ApplyOnUserNone {
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusOK))
		p.logger.DebugContext(ctx, "single-user inbound skips managed user replacement for ", inboundID)
		return nil
	}

	// Step 3: Check apply policy — only hot_reload_users is supported in v1.
	if managedInbound.UserApplyPolicy != contract.ApplyOnUserHotReloadUsers {
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusApplyFailed))
		p.logger.DebugContext(ctx, "unsupported user_apply_policy for inbound ", inboundID, ": ", managedInbound.UserApplyPolicy)
		return E.New("unsupported user_apply_policy: ", managedInbound.UserApplyPolicy)
	}

	// Step 4: Call fetcher.FetchUsers.
	snapshot, newETag, err := p.fetcher.FetchUsers(ctx, inboundID, etag, configRevision)
	if err != nil {
		// Step 7/8: Handle errors.
		return p.handleFetchError(ctx, inboundID, err, hasPrev, prevRevision)
	}

	// Step 5: On 304 (ErrNotModified) — handled by fetcher returning ErrNotModified,
	// which is caught in handleFetchError. If we reach here, we have a 200 response.

	// Step 6: Validate the snapshot.
	if err := p.validateSnapshot(snapshot, inboundID, managedInbound, configRevision); err != nil {
		// Validation failure — check if it's a revision conflict.
		if errors.Is(err, errRevisionConflict) {
			p.setInboundStatus(inboundID, string(contract.UserLoadStatusRevisionConflict))
			p.MarkConfigRefetch()
			p.logger.WarnContext(ctx, "revision conflict for inbound ", inboundID, ": snapshot config_rev=", snapshot.ConfigurationRevision, " applied=", configRevision)
			return err
		}
		// Other validation errors (node_id/inbound_id/protocol mismatch).
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusApplyFailed))
		return err
	}

	// Convert users and apply.
	users := convertUsers(snapshot)
	tag := managedInbound.Tag

	if err := p.box.ReplaceInboundUsers(tag, users); err != nil {
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusApplyFailed))
		p.logger.WarnContext(ctx, "replace inbound users failed for ", tag, ": ", err)
		return E.Cause(err, "replace inbound users")
	}

	// Success — update state.
	p.updateInboundState(inboundID, newETag, snapshot.Revision, len(snapshot.Users), string(contract.UserLoadStatusOK))
	p.logger.DebugContext(ctx, "applied ", len(snapshot.Users), " users to inbound ", tag)
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

var errRevisionConflict = E.New("configuration revision conflict")

// handleFetchError classifies the fetch error and updates state accordingly.
func (p *Poller) handleFetchError(ctx context.Context, inboundID string, err error, hasPrev bool, prevRevision string) error {
	// Check for 304 NotModified first.
	if errors.Is(err, client.ErrNotModified) {
		// No change — status is ok.
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusOK))
		return nil
	}

	// Check for 409 conflict.
	var respErr *client.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == 409 {
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusRevisionConflict))
		p.MarkConfigRefetch()
		p.logger.WarnContext(ctx, "409 conflict for inbound ", inboundID)
		return err
	}

	// Transport / 5xx / other retryable errors.
	if hasPrev && prevRevision != "" {
		// Subsequent load — keep old users, mark stale.
		p.setInboundStatus(inboundID, string(contract.UserLoadStatusStale))
		p.logger.WarnContext(ctx, "fetch users failed for inbound ", inboundID, " (stale): ", err)
		return err
	}

	// Initial load failure — fail-closed.
	p.setInboundStatus(inboundID, string(contract.UserLoadStatusEmptyInitialLoad))
	p.logger.WarnContext(ctx, "initial fetch users failed for inbound ", inboundID, " (empty_initial_load): ", err)
	return err
}

// validateSnapshot checks that the snapshot matches expectations.
func (p *Poller) validateSnapshot(snap *contract.UserSnapshot, inboundID string, managedInbound contract.ManagedInbound, configRevision string) error {
	nodeID := p.store.State().Config.NodeID

	if snap.NodeID != "" && snap.NodeID != nodeID {
		return E.New("node_id mismatch: got ", snap.NodeID, ", want ", nodeID)
	}
	if snap.InboundID != inboundID {
		return E.New("inbound_id mismatch: got ", snap.InboundID, ", want ", inboundID)
	}
	if snap.Protocol != managedInbound.Protocol {
		return E.New("protocol mismatch: got ", snap.Protocol, ", want ", managedInbound.Protocol)
	}
	if snap.ConfigurationRevision != configRevision {
		return errRevisionConflict
	}

	// Validate credential types — only password is supported.
	for i, u := range snap.Users {
		if u.Credential.Type != contract.CredentialTypePassword {
			return E.New("users[", i, "]: unsupported credential type: ", u.Credential.Type)
		}
	}

	return nil
}

// convertUsers converts a contract.UserSnapshot to []adapter.ManagedUser.
func convertUsers(snapshot *contract.UserSnapshot) []adapter.ManagedUser {
	users := make([]adapter.ManagedUser, len(snapshot.Users))
	for i, u := range snapshot.Users {
		users[i] = adapter.ManagedUser{
			UserID: u.UserID,
			Name:   u.Name,
			Credential: adapter.ManagedUserCredential{
				UserID:   u.UserID,
				Name:     u.Name,
				Password: u.Credential.Password,
			},
		}
	}
	return users
}

// setInboundStatus updates the UserLoadStatus for an inbound in the store.
func (p *Poller) setInboundStatus(inboundID string, status string) {
	st := p.store.State().Clone()
	ib, ok := st.Inbounds[inboundID]
	if !ok {
		ib = state.InboundState{InboundID: inboundID}
	}
	ib.UserLoadStatus = status
	st.Inbounds[inboundID] = ib
	p.store.SetState(st)
}

// updateInboundState updates the full inbound state after a successful apply.
func (p *Poller) updateInboundState(inboundID, etag, revision string, userCount int, status string) {
	st := p.store.State().Clone()
	ib, ok := st.Inbounds[inboundID]
	if !ok {
		ib = state.InboundState{InboundID: inboundID}
	}
	ib.UserETag = etag
	ib.UserRevision = revision
	ib.UserCount = userCount
	ib.UserLoadStatus = status
	st.Inbounds[inboundID] = ib
	p.store.SetState(st)
}
