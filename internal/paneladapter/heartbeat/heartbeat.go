// Package heartbeat provides the heartbeat reporter for the panel adapter.
// It builds and sends periodic heartbeat payloads to the panel, reporting
// the current state of the adapter, managed inbounds, and runtime metrics.
//
// Heartbeats are best-effort: errors are logged but do not propagate.
// Runtime metrics are aggregate-only (connection counts) and MUST NOT
// include raw IP lists, bearer tokens, passwords, or TLS secrets.
package heartbeat

import (
	"context"
	"runtime"
	"time"

	"github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

// defaultAdapterVersion is used when no adapter version is provided via options.
const defaultAdapterVersion = "0.1.0"

// RuntimeMetricsProvider returns aggregate runtime metrics for heartbeat.
// Connection counts are reported as aggregate numbers only — raw IP lists
// are never exposed.
type RuntimeMetricsProvider interface {
	GetRuntimeMetrics() *contract.HeartbeatRuntime
}

// StartTimeProvider returns the process start time for uptime calculation.
type StartTimeProvider interface {
	StartTime() time.Time
}

// Heartbeat builds and sends periodic heartbeat payloads to the panel.
type Heartbeat struct {
	client         *client.Client
	store          *state.Store
	metrics        RuntimeMetricsProvider
	startTime      StartTimeProvider
	nodeID         string
	singBoxVersion string
	adapterVersion string
	logger         log.ContextLogger
	startedAt      time.Time
}

// Option configures a Heartbeat instance.
type Option func(*Heartbeat)

// WithAdapterVersion sets the adapter version string.
// If not set, defaultAdapterVersion ("0.1.0") is used.
func WithAdapterVersion(v string) Option {
	return func(h *Heartbeat) {
		if v != "" {
			h.adapterVersion = v
		}
	}
}

// WithStartTimeProvider sets a custom StartTimeProvider for uptime calculation.
// If not set, the Heartbeat's own start time is used.
func WithStartTimeProvider(p StartTimeProvider) Option {
	return func(h *Heartbeat) {
		h.startTime = p
	}
}

// WithRuntimeMetricsProvider sets a custom RuntimeMetricsProvider for
// aggregate runtime metrics (connection counts, memory, etc.).
// If not set, runtime metrics in the heartbeat payload will be nil.
func WithRuntimeMetricsProvider(p RuntimeMetricsProvider) Option {
	return func(h *Heartbeat) {
		h.metrics = p
	}
}

// WithLogger sets a custom logger.
func WithLogger(logger log.ContextLogger) Option {
	return func(h *Heartbeat) {
		h.logger = logger
	}
}

// NewHeartbeat creates a heartbeat reporter.
//
// Parameters:
//   - client: panel REST client (T2)
//   - store: durable state store (T3)
//   - nodeID: the node identifier
//   - singBoxVersion: sing-box version string (use constant.Version)
//   - adapterVersion: adapter version string (empty falls back to "0.1.0")
//   - opts: optional configuration
func NewHeartbeat(
	cl *client.Client,
	store *state.Store,
	nodeID string,
	singBoxVersion string,
	adapterVersion string,
	opts ...Option,
) *Heartbeat {
	h := &Heartbeat{
		client:         cl,
		store:          store,
		nodeID:         nodeID,
		singBoxVersion: singBoxVersion,
		adapterVersion: adapterVersion,
		startedAt:      time.Now(),
	}
	if h.adapterVersion == "" {
		h.adapterVersion = defaultAdapterVersion
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// SendHeartbeat builds and sends one heartbeat payload.
//
// Build flow:
//  1. Load current state from store (config revision, etag, per-inbound statuses)
//  2. Build contract.Heartbeat payload with:
//     - ObservedAt: current UTC time
//     - SingBoxVersion: from constant.Version
//     - AdapterVersion: from option or "0.1.0" default
//     - AppliedConfigurationRevision: from state
//     - PendingConfigurationRevision: from state if config has unreconciled changes
//     - Inbounds: iterate over state inbounds, map each to HeartbeatInbound
//     - Runtime: aggregate metrics from tracker (connections, uptime, memory)
//  3. Call client.SendHeartbeat
//  4. Log errors but don't fail (heartbeat is best-effort)
func (h *Heartbeat) SendHeartbeat(ctx context.Context) error {
	st := h.store.State()

	// Build heartbeat inbounds from state.
	inbounds := h.buildInbounds(st)

	// Build runtime metrics.
	rtMetrics := h.buildRuntimeMetrics()

	// Determine pending configuration revision.
	pendingRev := h.pendingRevision(st)

	hb := &contract.Heartbeat{
		ObservedAt:                   time.Now().UTC(),
		SingBoxVersion:               h.singBoxVersion,
		AdapterVersion:               h.adapterVersion,
		AppliedConfigurationRevision: st.Config.Revision,
		PendingConfigurationRevision: pendingRev,
		Inbounds:                     inbounds,
		Runtime:                      rtMetrics,
	}

	if err := h.client.SendHeartbeat(ctx, hb); err != nil {
		// Best-effort: log but do not propagate.
		if h.logger != nil {
			h.logger.ErrorContext(ctx, "send heartbeat: ", err)
		}
		return E.Cause(err, "send heartbeat")
	}

	if h.logger != nil {
		h.logger.DebugContext(ctx, "heartbeat sent, revision=", st.Config.Revision)
	}
	return nil
}

// buildInbounds creates HeartbeatInbound entries from state.
// ALL managed inbounds from state are included, not just successfully polled ones.
// Unknown protocols get unsupported_protocol status.
func (h *Heartbeat) buildInbounds(st *state.State) []contract.HeartbeatInbound {
	inbounds := make([]contract.HeartbeatInbound, 0, len(st.Inbounds))
	for _, ib := range st.Inbounds {
		status := contract.UserLoadStatus(ib.UserLoadStatus)

		// If UserLoadStatus is empty (never polled), mark as empty_initial_load.
		if status == "" {
			status = contract.UserLoadStatusEmptyInitialLoad
		}

		// If protocol is not supported, override status to unsupported_protocol.
		if !contract.IsSupportedProtocol(ib.Protocol) {
			status = contract.UserLoadStatusUnsupportedProto
		}

		inbounds = append(inbounds, contract.HeartbeatInbound{
			InboundID:           ib.InboundID,
			Protocol:            ib.Protocol,
			AppliedUserRevision: ib.UserRevision,
			UserCount:           ib.UserCount,
			UserLoadStatus:      status,
		})
	}
	return inbounds
}

// pendingRevision returns a pointer to the pending configuration revision
// string if there are unreconciled changes (manual strategy or apply_failed),
// or nil if the applied revision is current.
//
// A pending revision is present when:
//   - An inbound has apply_failed status (config was fetched but apply failed)
//   - An inbound has unsupported_protocol status (config was recorded but not applied)
//
// In v1, the pending revision is simply the config revision from state
// when any inbound indicates the config was not fully applied.
func (h *Heartbeat) pendingRevision(st *state.State) *string {
	if len(st.Inbounds) == 0 {
		return nil
	}

	// Check if any inbound indicates the current config revision was not
	// fully applied (apply_failed or unsupported_protocol with no user revision).
	for _, ib := range st.Inbounds {
		status := contract.UserLoadStatus(ib.UserLoadStatus)
		switch {
		case status == contract.UserLoadStatusApplyFailed:
			// Config was received but apply failed — pending revision is the
			// revision from state (the failed revision).
			if st.Config.Revision != "" {
				rev := st.Config.Revision
				return &rev
			}
		case status == contract.UserLoadStatusUnsupportedProto && ib.UserRevision == "":
			// Unsupported protocol with no applied user revision means the
			// config was recorded but not applied.
			if st.Config.Revision != "" {
				rev := st.Config.Revision
				return &rev
			}
		}
	}

	return nil
}

// buildRuntimeMetrics returns aggregate runtime metrics for the heartbeat.
// Only aggregate counts are included — raw IP lists are NEVER exposed.
func (h *Heartbeat) buildRuntimeMetrics() *contract.HeartbeatRuntime {
	rt := &contract.HeartbeatRuntime{}

	// If a RuntimeMetricsProvider is available, use it for connection counts.
	if h.metrics != nil {
		if m := h.metrics.GetRuntimeMetrics(); m != nil {
			rt.Connections = m.Connections
		}
	}

	// Uptime: from StartTimeProvider or Heartbeat's own start time.
	startTime := h.startedAt
	if h.startTime != nil {
		startTime = h.startTime.StartTime()
	}
	if !startTime.IsZero() {
		rt.UptimeSeconds = int64(time.Since(startTime).Seconds())
	}

	// Memory: from runtime.ReadMemStats (optional, aggregate only).
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	rt.MemoryBytes = int64(memStats.Sys)

	return rt
}

// AdapterVersion returns the configured adapter version.
// This is guaranteed to be non-empty.
func (h *Heartbeat) AdapterVersion() string {
	return h.adapterVersion
}

// SingBoxVersion returns the sing-box version.
func (h *Heartbeat) SingBoxVersion() string {
	return h.singBoxVersion
}

// compile-time check that constant.Version is accessible.
var _ = constant.Version
