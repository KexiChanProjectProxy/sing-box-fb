// Package runtime manages the sing-box Box lifecycle driven by panel
// configuration. It handles bootstrap, config polling, and apply strategies
// (recreate_instance, restart_process, manual).
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	E "github.com/sagernet/sing/common/exceptions"
	commonjson "github.com/sagernet/sing/common/json"
)

// ---------------------------------------------------------------------------
// BoxFactory — abstraction for testability
// ---------------------------------------------------------------------------

// ConfigurationFetcher abstracts the panel client operations needed by the
// runtime Manager. The production implementation is *client.Client.
type ConfigurationFetcher interface {
	// FetchConfiguration fetches the current configuration from the panel.
	// Returns (configuration, newETag, error). On 304, returns ErrNotModified.
	FetchConfiguration(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error)
}

// BoxFactory creates and starts a box.Box from the given options.
// Production code uses the real box.New + Start; tests inject a fake.
type BoxFactory interface {
	// Create builds a new Box, starts it, and returns the running instance.
	Create(ctx context.Context, options option.Options) (*box.Box, error)
}

// defaultBoxFactory is the production BoxFactory using box.New.
type defaultBoxFactory struct {
	logFactory log.Factory
}

func (f *defaultBoxFactory) Create(ctx context.Context, options option.Options) (*box.Box, error) {
	ctx = include.Context(ctx)
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: options,
	})
	if err != nil {
		return nil, E.Cause(err, "create box instance")
	}
	if err := instance.Start(); err != nil {
		instance.Close()
		return nil, E.Cause(err, "start box instance")
	}
	return instance, nil
}

// ---------------------------------------------------------------------------
// Option — functional options for Manager
// ---------------------------------------------------------------------------

// Option configures a Manager during construction.
type Option func(*Manager)

// WithBoxFactory injects a custom BoxFactory (for testing).
func WithBoxFactory(f BoxFactory) Option {
	return func(m *Manager) {
		m.factory = f
	}
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

// Manager manages the sing-box Box lifecycle driven by panel configuration.
// It handles bootstrap (initial fetch + Box creation), config polling
// with apply strategies, and fail-closed semantics for managed inbounds.
type Manager struct {
	fetcher        ConfigurationFetcher
	store          *state.Store
	trafficTracker *traffic.Tracker
	logFactory     log.Factory
	logger         log.ContextLogger
	factory        BoxFactory

	mu        sync.Mutex
	instance  *box.Box
	cancel    context.CancelFunc
	startedAt time.Time
}

// NewManager creates a runtime manager.
//
// The fetcher parameter is typically *client.Client but can be any
// ConfigurationFetcher implementation for testing.
//
// The caller must invoke Bootstrap to perform the initial configuration
// fetch and Box creation before the manager is usable.
func NewManager(fetcher ConfigurationFetcher, s *state.Store, t *traffic.Tracker, logFactory log.Factory, opts ...Option) (*Manager, error) {
	if fetcher == nil {
		return nil, E.New("fetcher is required")
	}
	if s == nil {
		return nil, E.New("store is required")
	}
	if t == nil {
		return nil, E.New("traffic tracker is required")
	}
	if logFactory == nil {
		return nil, E.New("log factory is required")
	}

	m := &Manager{
		fetcher:        fetcher,
		store:          s,
		trafficTracker: t,
		logFactory:     logFactory,
		logger:         logFactory.NewLogger("panel/runtime"),
		factory:        &defaultBoxFactory{logFactory: logFactory},
	}
	for _, opt := range opts {
		opt(m)
	}
	m.startedAt = time.Now()
	return m, nil
}

func (m *Manager) StartTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startedAt
}

// Bootstrap performs the initial configuration fetch and Box creation.
//
// Flow:
//  1. Fetch configuration from panel via client.FetchConfiguration
//  2. Validate the response with cfg.Validate()
//  3. For each managed inbound, check protocol support
//  4. Strip managed inbound user arrays to empty in the template
//  5. Unmarshal the stripped template into option.Options
//  6. Create and start box.Box
//  7. Record applied revision and inbound metadata in state
//
// Managed inbounds start fail-closed (empty users) until the first valid
// user snapshot is applied by the user polling loop (T9).
func (m *Manager) Bootstrap(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cfg, etag, err := m.fetcher.FetchConfiguration(ctx, "")
	if err != nil {
		return E.Cause(err, "fetch initial configuration")
	}
	if err := cfg.Validate(); err != nil {
		return E.Cause(err, "validate initial configuration")
	}

	return m.applyConfigLocked(ctx, cfg, etag)
}

// PollConfiguration polls the panel for configuration changes.
//
// On 304 (ErrNotModified): no-op, returns nil.
// On new configuration: validates and applies according to the
// apply_strategy.on_configuration_change:
//   - recreate_instance: closes old Box, creates new Box from new template
//   - restart_process: mapped to same full Box recreation (not OS-level
//     self-exec). This in-process wrapper does not have the ability to
//     re-exec the process, so restart_process is equivalent to
//     recreate_instance here.
//   - manual: records the pending revision in state without applying
//
// If the new configuration fails to apply (validation or Box creation
// failure), the old Box keeps running and the inbound states are marked
// apply_failed.
func (m *Manager) PollConfiguration(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	curState := m.store.State()
	curETag := curState.Config.ETag

	cfg, newETag, err := m.fetcher.FetchConfiguration(ctx, curETag)
	if err != nil {
		if errors.Is(err, client.ErrNotModified) {
			return nil
		}
		return E.Cause(err, "fetch configuration")
	}
	if err := cfg.Validate(); err != nil {
		return E.Cause(err, "validate polled configuration")
	}

	strategy := cfg.ApplyStrategy.OnConfigurationChange
	switch strategy {
	case contract.ApplyOnConfigRecreateInstance, contract.ApplyOnConfigRestartProcess:
		// restart_process is mapped to full Box recreation (not OS-level
		// self-exec) because this in-process wrapper cannot re-exec the
		// process. The effect is identical to recreate_instance.
		return m.applyConfigLocked(ctx, cfg, newETag)

	case contract.ApplyOnConfigManual:
		return m.recordPendingRevision(cfg, newETag)

	default:
		return E.New("unknown apply strategy on_configuration_change: ", strategy)
	}
}

// GetBox returns the current Box instance. Returns nil if Bootstrap has
// not been called or if the Box has been closed.
func (m *Manager) GetBox() *box.Box {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.instance
}

// Close shuts down the current Box and releases resources.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeLocked()
}

// ---------------------------------------------------------------------------
// Internal helpers (must be called with m.mu held)
// ---------------------------------------------------------------------------

// applyConfigLocked validates, strips users, creates Box, updates state.
// Caller must hold m.mu.
func (m *Manager) applyConfigLocked(ctx context.Context, cfg *contract.ConfigurationResponse, etag string) error {
	// Check protocol support for each managed inbound.
	unsupportedInbounds := make(map[string]bool)
	for _, ib := range cfg.ManagedInbounds {
		if !contract.IsSupportedProtocol(ib.Protocol) {
			unsupportedInbounds[ib.InboundID] = true
			m.logger.WarnContext(ctx, "unsupported managed inbound protocol: ", ib.Protocol, " (inbound_id=", ib.InboundID, ")")
		}
	}

	// Strip user arrays from managed inbound tags in the template.
	stripped, err := stripManagedInboundUsers(cfg)
	if err != nil {
		return E.Cause(err, "strip managed inbound users")
	}

	// Unmarshal into option.Options.
	var options option.Options
	if err := commonjson.UnmarshalContext(include.Context(ctx), stripped, &options); err != nil {
		return E.Cause(err, "unmarshal sing-box config template")
	}

	// Preserve the old instance in case the new one fails.
	oldInstance := m.instance
	oldCancel := m.cancel

	// Create a new context for the new Box.
	boxCtx, boxCancel := context.WithCancel(ctx)

	// Create and start the new Box.
	newInstance, err := m.factory.Create(boxCtx, options)
	if err != nil {
		boxCancel()
		// Mark all managed inbounds as apply_failed.
		m.setInboundStatesApplyFailed(cfg)
		m.logger.ErrorContext(ctx, "failed to create new box instance: ", err)
		return E.Cause(err, "create new box instance")
	}

	// Success: swap in the new instance.
	m.instance = newInstance
	m.cancel = boxCancel

	// Close the old instance after the new one is running.
	if oldInstance != nil {
		if oldCancel != nil {
			oldCancel()
		}
		if closeErr := oldInstance.Close(); closeErr != nil {
			m.logger.WarnContext(ctx, "close old box instance: ", closeErr)
		}
	}

	// Update state with applied config and inbound metadata.
	m.updateStateApplied(cfg, etag, unsupportedInbounds)

	m.logger.InfoContext(ctx, "applied configuration revision=", cfg.Revision, " etag=", etag)
	return nil
}

// recordPendingRevision records a new configuration revision as pending
// without applying it. The pending revision will be reported in heartbeats.
func (m *Manager) recordPendingRevision(cfg *contract.ConfigurationResponse, etag string) error {
	s := m.store.State().Clone()
	s.Config = state.ConfigState{
		Revision: cfg.Revision,
		ETag:     etag,
		NodeID:   cfg.NodeID,
	}

	// Record inbound metadata but mark as pending (not yet applied).
	for _, ib := range cfg.ManagedInbounds {
		is := state.InboundState{
			InboundID: ib.InboundID,
			Tag:       ib.Tag,
			Protocol:  ib.Protocol,
		}
		if !contract.IsSupportedProtocol(ib.Protocol) {
			is.UserLoadStatus = string(contract.UserLoadStatusUnsupportedProto)
		}
		s.Inbounds[ib.InboundID] = is
	}

	m.store.SetState(s)
	if err := m.store.SaveIfChanged(); err != nil {
		return E.Cause(err, "save pending revision state")
	}

	m.logger.Info("recorded pending configuration revision=", cfg.Revision, " (manual strategy)")
	return nil
}

// updateStateApplied records the applied configuration revision, ETag,
// and managed inbound metadata in the state store.
func (m *Manager) updateStateApplied(cfg *contract.ConfigurationResponse, etag string, unsupported map[string]bool) {
	s := m.store.State().Clone()
	s.Config = state.ConfigState{
		Revision: cfg.Revision,
		ETag:     etag,
		NodeID:   cfg.NodeID,
	}

	// Update per-inbound state.
	for _, ib := range cfg.ManagedInbounds {
		is := state.InboundState{
			InboundID: ib.InboundID,
			Tag:       ib.Tag,
			Protocol:  ib.Protocol,
		}
		if unsupported[ib.InboundID] {
			is.UserLoadStatus = string(contract.UserLoadStatusUnsupportedProto)
		} else {
			// Fail-closed: empty initial load until first user snapshot.
			is.UserLoadStatus = string(contract.UserLoadStatusEmptyInitialLoad)
		}
		s.Inbounds[ib.InboundID] = is
	}

	m.store.SetState(s)
	if err := m.store.SaveIfChanged(); err != nil {
		m.logger.Error("save state after apply: ", err)
	}
}

// setInboundStatesApplyFailed marks all managed inbounds as apply_failed.
func (m *Manager) setInboundStatesApplyFailed(cfg *contract.ConfigurationResponse) {
	s := m.store.State().Clone()
	s.Config = state.ConfigState{
		Revision: cfg.Revision,
		ETag:     "", // No ETag since apply failed
		NodeID:   cfg.NodeID,
	}

	for _, ib := range cfg.ManagedInbounds {
		is := state.InboundState{
			InboundID: ib.InboundID,
			Tag:       ib.Tag,
			Protocol:  ib.Protocol,
		}
		if !contract.IsSupportedProtocol(ib.Protocol) {
			is.UserLoadStatus = string(contract.UserLoadStatusUnsupportedProto)
		} else {
			is.UserLoadStatus = string(contract.UserLoadStatusApplyFailed)
		}
		s.Inbounds[ib.InboundID] = is
	}

	m.store.SetState(s)
	if err := m.store.SaveIfChanged(); err != nil {
		m.logger.Error("save apply_failed state: ", err)
	}
}

// closeLocked shuts down the current Box. Caller must hold m.mu.
func (m *Manager) closeLocked() error {
	if m.instance == nil {
		return nil
	}
	if m.cancel != nil {
		m.cancel()
	}
	err := m.instance.Close()
	m.instance = nil
	m.cancel = nil
	return err
}

// ---------------------------------------------------------------------------
// Template manipulation
// ---------------------------------------------------------------------------

// stripManagedInboundUsers returns a modified copy of the
// SingBoxConfigTemplate where the "users" field of each managed inbound
// is set to an empty array []. This ensures fail-closed semantics:
// the inbound will reject all connections until the user polling loop
// (T9) pushes the first valid user snapshot.
func stripManagedInboundUsers(cfg *contract.ConfigurationResponse) (json.RawMessage, error) {
	if len(cfg.SingBoxConfigTemplate) == 0 {
		return nil, E.New("empty sing_box_config_template")
	}

	// Parse the template into a generic map for manipulation.
	var template map[string]json.RawMessage
	if err := json.Unmarshal(cfg.SingBoxConfigTemplate, &template); err != nil {
		return nil, E.Cause(err, "parse config template as object")
	}

	// Collect managed inbound tags for lookup.
	managedTags := make(map[string]string, len(cfg.ManagedInbounds))
	for _, ib := range cfg.ManagedInbounds {
		managedTags[ib.Tag] = ib.Protocol
	}

	// Get the inbounds array.
	inboundsRaw, ok := template["inbounds"]
	if !ok {
		// No inbounds key — nothing to strip.
		return cfg.SingBoxConfigTemplate, nil
	}

	// Parse the inbounds array.
	var inbounds []json.RawMessage
	if err := json.Unmarshal(inboundsRaw, &inbounds); err != nil {
		return nil, E.Cause(err, "parse inbounds array")
	}

	emptyUsers := json.RawMessage(`[]`)
	modified := false

	for i, ibRaw := range inbounds {
		var ib map[string]json.RawMessage
		if err := json.Unmarshal(ibRaw, &ib); err != nil {
			continue
		}

		// Extract the tag to check if this is a managed inbound.
		tagRaw, hasTag := ib["tag"]
		if !hasTag {
			continue
		}
		var tag string
		if err := json.Unmarshal(tagRaw, &tag); err != nil {
			continue
		}

		protocol, managed := managedTags[tag]
		if managed {
			if protocol == "shadowsocks" {
				ib["managed"] = json.RawMessage(`true`)
			}
			if _, hasUsers := ib["users"]; hasUsers {
				ib["users"] = emptyUsers
			}
			modifiedRaw, err := json.Marshal(ib)
			if err != nil {
				return nil, E.Cause(err, "re-marshal inbound with stripped users")
			}
			inbounds[i] = modifiedRaw
			modified = true
		}
	}

	// If no changes, return the original template.
	if !modified {
		return cfg.SingBoxConfigTemplate, nil
	}

	// Rebuild the template with modified inbounds.
	newInbounds, err := json.Marshal(inbounds)
	if err != nil {
		return nil, E.Cause(err, "re-marshal inbounds array")
	}
	template["inbounds"] = newInbounds

	result, err := json.Marshal(template)
	if err != nil {
		return nil, E.Cause(err, "re-marshal config template")
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// fmt.Stringer for logging
// ---------------------------------------------------------------------------

// String returns a human-readable summary of the manager state.
func (m *Manager) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.store.State()
	hasBox := m.instance != nil
	return fmt.Sprintf("Manager(applied_rev=%s has_box=%v)", s.Config.Revision, hasBox)
}
