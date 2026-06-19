package contract

import (
	E "github.com/sagernet/sing/common/exceptions"
)

// ---------------------------------------------------------------------------
// ConfigurationResponse validation
// ---------------------------------------------------------------------------

// Validate checks that a ConfigurationResponse has all required fields and
// that enum values are recognised. Unknown managed_inbounds[].protocol values
// are NOT treated as errors — they become unsupported metadata that heartbeat
// will report as unsupported_protocol.
func (c *ConfigurationResponse) Validate() error {
	if c.Revision == "" {
		return E.New("missing configuration revision")
	}
	if c.APIVersion == "" {
		return E.New("missing api_version")
	}
	if c.NodeID == "" {
		return E.New("missing node_id")
	}
	if err := c.ApplyStrategy.Validate(); err != nil {
		return E.Cause(err, "invalid apply_strategy")
	}
	if c.PollIntervals.ConfigurationSeconds <= 0 {
		return E.New("poll_intervals.configuration_seconds must be positive")
	}
	if c.PollIntervals.UsersSeconds <= 0 {
		return E.New("poll_intervals.users_seconds must be positive")
	}
	if c.PollIntervals.TrafficSeconds <= 0 {
		return E.New("poll_intervals.traffic_seconds must be positive")
	}
	if c.PollIntervals.HeartbeatSeconds <= 0 {
		return E.New("poll_intervals.heartbeat_seconds must be positive")
	}
	if len(c.ManagedInbounds) == 0 {
		return E.New("managed_inbounds must not be empty")
	}
	seenIDs := make(map[string]bool, len(c.ManagedInbounds))
	for i, inbound := range c.ManagedInbounds {
		if err := inbound.Validate(); err != nil {
			return E.Cause(err, "managed_inbounds[", i, "]")
		}
		if seenIDs[inbound.InboundID] {
			return E.New("duplicate managed_inbound inbound_id: ", inbound.InboundID)
		}
		seenIDs[inbound.InboundID] = true
	}
	if len(c.SingBoxConfigTemplate) == 0 {
		return E.New("missing sing_box_config_template")
	}
	return nil
}

// ---------------------------------------------------------------------------
// ApplyStrategy validation
// ---------------------------------------------------------------------------

// Validate checks that both apply-strategy enum values are recognised.
func (s *ApplyStrategy) Validate() error {
	if s.OnConfigurationChange == "" {
		return E.New("missing on_configuration_change")
	}
	if !validOnConfigurationChange[s.OnConfigurationChange] {
		return E.New("unknown on_configuration_change: ", s.OnConfigurationChange)
	}
	if s.OnUserChange == "" {
		return E.New("missing on_user_change")
	}
	if !validOnUserChange[s.OnUserChange] {
		return E.New("unknown on_user_change: ", s.OnUserChange)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ManagedInbound validation
// ---------------------------------------------------------------------------

// Validate checks required fields. Unknown protocols are intentionally NOT
// rejected — the adapter retains them as unsupported metadata.
func (m *ManagedInbound) Validate() error {
	if m.InboundID == "" {
		return E.New("missing inbound_id")
	}
	if m.Tag == "" {
		return E.New("missing tag")
	}
	if m.Protocol == "" {
		return E.New("missing protocol")
	}
	// Unknown protocol is NOT an error; heartbeat reports unsupported_protocol.
	return nil
}

// ---------------------------------------------------------------------------
// UserSnapshot validation
// ---------------------------------------------------------------------------

// Validate checks that a UserSnapshot has required fields and that each user
// credential uses a supported type. It also verifies that configuration_revision
// is present so the caller can match it against the applied config revision.
func (u *UserSnapshot) Validate() error {
	if u.Revision == "" {
		return E.New("missing user snapshot revision")
	}
	if u.ConfigurationRevision == "" {
		return E.New("missing configuration_revision")
	}
	if u.NodeID == "" {
		return E.New("missing node_id")
	}
	if u.InboundID == "" {
		return E.New("missing inbound_id")
	}
	if u.Protocol == "" {
		return E.New("missing protocol")
	}
	for i, user := range u.Users {
		if err := user.Validate(); err != nil {
			return E.Cause(err, "users[", i, "]")
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// User validation
// ---------------------------------------------------------------------------

// Validate checks required user fields and the credential type.
func (u *User) Validate() error {
	if u.UserID == "" {
		return E.New("missing user_id")
	}
	if u.Name == "" {
		return E.New("missing name")
	}
	return u.Credential.Validate()
}

// ---------------------------------------------------------------------------
// Credential validation
// ---------------------------------------------------------------------------

// Validate ensures the credential type is known. In v1, only "password" is
// supported; unknown types are rejected.
func (c *Credential) Validate() error {
	if c.Type == "" {
		return E.New("missing credential type")
	}
	switch c.Type {
	case CredentialTypePassword:
		if c.Password == "" {
			return E.New("missing credential password")
		}
		return nil
	default:
		return E.New("unsupported credential type: ", c.Type)
	}
}

// ---------------------------------------------------------------------------
// TrafficReport validation
// ---------------------------------------------------------------------------

// Validate checks that time windows are increasing and all byte counts are
// non-negative. It also requires configuration_revision.
func (t *TrafficReport) Validate() error {
	if t.StartedAt.IsZero() {
		return E.New("missing started_at")
	}
	if t.EndedAt.IsZero() {
		return E.New("missing ended_at")
	}
	if !t.EndedAt.After(t.StartedAt) {
		return E.New("ended_at must be after started_at")
	}
	if t.ConfigurationRevision == "" {
		return E.New("missing configuration_revision")
	}
	for i, rec := range t.Records {
		if err := rec.Validate(); err != nil {
			return E.Cause(err, "records[", i, "]")
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// TrafficRecord validation
// ---------------------------------------------------------------------------

// Validate checks required fields and non-negative byte counts.
func (r *TrafficRecord) Validate() error {
	if r.InboundID == "" {
		return E.New("missing inbound_id")
	}
	if r.UserID == "" {
		return E.New("missing user_id")
	}
	if r.UploadBytes < 0 {
		return E.New("upload_bytes must be non-negative, got: ", r.UploadBytes)
	}
	if r.DownloadBytes < 0 {
		return E.New("download_bytes must be non-negative, got: ", r.DownloadBytes)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Heartbeat validation
// ---------------------------------------------------------------------------

// Validate checks required heartbeat fields and validates each inbound entry.
func (h *Heartbeat) Validate() error {
	if h.ObservedAt.IsZero() {
		return E.New("missing observed_at")
	}
	if h.SingBoxVersion == "" {
		return E.New("missing sing_box_version")
	}
	if h.AdapterVersion == "" {
		return E.New("missing adapter_version")
	}
	if h.AppliedConfigurationRevision == "" {
		return E.New("missing applied_configuration_revision")
	}
	for i, inbound := range h.Inbounds {
		if err := inbound.Validate(); err != nil {
			return E.Cause(err, "inbounds[", i, "]")
		}
	}
	if h.Runtime != nil {
		if err := h.Runtime.Validate(); err != nil {
			return E.Cause(err, "runtime")
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// HeartbeatInbound validation
// ---------------------------------------------------------------------------

// Validate checks required fields and that user_load_status is a known enum.
func (hi *HeartbeatInbound) Validate() error {
	if hi.InboundID == "" {
		return E.New("missing inbound_id")
	}
	if hi.Protocol == "" {
		return E.New("missing protocol")
	}
	if hi.UserLoadStatus == "" {
		return E.New("missing user_load_status")
	}
	if !ValidUserLoadStatuses[hi.UserLoadStatus] {
		return E.New("unknown user_load_status: ", string(hi.UserLoadStatus))
	}
	return nil
}

// ---------------------------------------------------------------------------
// HeartbeatRuntime validation
// ---------------------------------------------------------------------------

// Validate checks runtime metrics for sanity.
func (r *HeartbeatRuntime) Validate() error {
	if r.UptimeSeconds < 0 {
		return E.New("uptime_seconds must be non-negative, got: ", r.UptimeSeconds)
	}
	if r.Connections < 0 {
		return E.New("connections must be non-negative, got: ", r.Connections)
	}
	if r.MemoryBytes < 0 {
		return E.New("memory_bytes must be non-negative, got: ", r.MemoryBytes)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ProblemDetails validation
// ---------------------------------------------------------------------------

// Validate checks that all required ProblemDetails fields are present.
func (p *ProblemDetails) Validate() error {
	if p.Title == "" {
		return E.New("missing title")
	}
	if p.Status <= 0 {
		return E.New("missing or invalid status")
	}
	if p.Code == "" {
		return E.New("missing code")
	}
	if p.RequestID == "" {
		return E.New("missing request_id")
	}
	return nil
}
