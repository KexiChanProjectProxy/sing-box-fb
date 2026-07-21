package contract

import (
	"encoding/json"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }

func mustRawMessage(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// validConfigurationResponse returns a fully-populated, valid ConfigurationResponse.
func validConfigurationResponse() *ConfigurationResponse {
	return &ConfigurationResponse{
		Revision:   "cfg-0005",
		APIVersion: "1",
		NodeID:     "101",
		ApplyStrategy: ApplyStrategy{
			OnConfigurationChange: ApplyOnConfigRestartProcess,
			OnUserChange:          ApplyOnUserHotReloadUsers,
		},
		PollIntervals: PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         60,
			TrafficSeconds:       60,
			HeartbeatSeconds:     30,
		},
		ManagedInbounds: []ManagedInbound{
			{
				InboundID:       "hy2-main",
				Tag:             "hy2-in",
				Protocol:        "hysteria2",
				UserResource:    "/api/v1/nodes/101/inbounds/hy2-main/users",
				UserApplyPolicy: "hot_reload",
			},
			{
				InboundID:       "ss-main",
				Tag:             "ss-in",
				Protocol:        "shadowsocks",
				UserResource:    "/api/v1/nodes/101/inbounds/ss-main/users",
				UserApplyPolicy: "hot_reload",
			},
		},
		SingBoxConfigTemplate: mustRawMessage(map[string]any{"log": map[string]string{"level": "info"}}),
	}
}

// validUserSnapshot returns a fully-populated, valid UserSnapshot.
func validUserSnapshot() *UserSnapshot {
	return &UserSnapshot{
		Revision:              "usr-hy2-main-0011",
		ConfigurationRevision: "cfg-0005",
		NodeID:                "101",
		InboundID:             "hy2-main",
		Protocol:              "hysteria2",
		Users: []User{
			{UserID: "10001", Name: "10001", Credential: Credential{Type: CredentialTypePassword, Password: "pw1"}},
			{UserID: "10002", Name: "10002", Credential: Credential{Type: CredentialTypePassword, Password: "pw2"}},
		},
	}
}

// validTrafficReport returns a fully-populated, valid TrafficReport.
func validTrafficReport() *TrafficReport {
	return &TrafficReport{
		StartedAt:             time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC),
		EndedAt:               time.Date(2026, 6, 19, 10, 1, 0, 0, time.UTC),
		ConfigurationRevision: "cfg-0005",
		Records: []TrafficRecord{
			{InboundID: "hy2-main", UserID: "10001", UploadBytes: 12345, DownloadBytes: 67890},
			{InboundID: "ss-main", UserID: "10001", UploadBytes: 5000, DownloadBytes: 9000},
		},
	}
}

// validHeartbeat returns a fully-populated, valid Heartbeat.
func validHeartbeat() *Heartbeat {
	return &Heartbeat{
		ObservedAt:                   time.Date(2026, 6, 19, 10, 1, 0, 0, time.UTC),
		SingBoxVersion:               "1.12.0",
		AdapterVersion:               "0.1.0",
		AppliedConfigurationRevision: "cfg-0005",
		PendingConfigurationRevision: nil,
		InboundStatuses: []HeartbeatInbound{
			{
				Tag:              "hy2-in",
				Protocol:         "hysteria2",
				CurrentUserCount: 1024,
				Status:           UserLoadStatusOK,
			},
			{
				Tag:              "ss-in",
				Protocol:         "shadowsocks",
				CurrentUserCount: 512,
				Status:           UserLoadStatusStale,
			},
		},
		Runtime: &HeartbeatRuntime{
			UptimeSeconds: 3600,
			Connections:   128,
			MemoryBytes:   104857600,
		},
	}
}

// ---------------------------------------------------------------------------
// ConfigurationResponse tests
// ---------------------------------------------------------------------------

func TestConfigurationResponse_Validate_Valid(t *testing.T) {
	if err := validConfigurationResponse().Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestConfigurationResponse_Validate_SupportedProtocols(t *testing.T) {
	for _, proto := range []string{"hysteria2", "anytls", "shadowsocks"} {
		cfg := validConfigurationResponse()
		cfg.ManagedInbounds = []ManagedInbound{
			{InboundID: "in-1", Tag: "t1", Protocol: proto},
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("protocol %s: expected valid, got: %v", proto, err)
		}
	}
}

func TestConfigurationResponse_Validate_UnknownProtocolNotError(t *testing.T) {
	cfg := validConfigurationResponse()
	cfg.ManagedInbounds = []ManagedInbound{
		{InboundID: "hy2-main", Tag: "hy2-in", Protocol: "hysteria2"},
		{InboundID: "vmess-main", Tag: "vmess-in", Protocol: "vmess"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unknown managed_inbounds protocol should NOT be an error, got: %v", err)
	}
}

func TestConfigurationResponse_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ConfigurationResponse)
		wantErr string
	}{
		{"missing revision", func(c *ConfigurationResponse) { c.Revision = "" }, "missing configuration revision"},
		{"missing api_version", func(c *ConfigurationResponse) { c.APIVersion = "" }, "missing api_version"},
		{"missing node_id", func(c *ConfigurationResponse) { c.NodeID = "" }, "missing node_id"},
		{"empty managed_inbounds", func(c *ConfigurationResponse) { c.ManagedInbounds = nil }, "managed_inbounds must not be empty"},
		{"missing config template", func(c *ConfigurationResponse) { c.SingBoxConfigTemplate = nil }, "missing sing_box_config_template"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfigurationResponse()
			tt.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfigurationResponse_Validate_DuplicateInboundID(t *testing.T) {
	cfg := validConfigurationResponse()
	cfg.ManagedInbounds = []ManagedInbound{
		{InboundID: "dup", Tag: "t1", Protocol: "hysteria2"},
		{InboundID: "dup", Tag: "t2", Protocol: "shadowsocks"},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected duplicate inbound_id error")
	}
	if !contains(err.Error(), "duplicate") {
		t.Fatalf("error %q should mention duplicate", err.Error())
	}
}

func TestConfigurationResponse_Validate_PollIntervals(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*PollIntervals)
		wantErr string
	}{
		{"zero configuration_seconds", func(p *PollIntervals) { p.ConfigurationSeconds = 0 }, "configuration_seconds"},
		{"negative users_seconds", func(p *PollIntervals) { p.UsersSeconds = -1 }, "users_seconds"},
		{"zero traffic_seconds", func(p *PollIntervals) { p.TrafficSeconds = 0 }, "traffic_seconds"},
		{"zero heartbeat_seconds", func(p *PollIntervals) { p.HeartbeatSeconds = 0 }, "heartbeat_seconds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfigurationResponse()
			tt.mutate(&cfg.PollIntervals)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ApplyStrategy tests
// ---------------------------------------------------------------------------

func TestApplyStrategy_Validate_Valid(t *testing.T) {
	strategies := []ApplyStrategy{
		{OnConfigurationChange: ApplyOnConfigRestartProcess, OnUserChange: ApplyOnUserHotReloadUsers},
		{OnConfigurationChange: ApplyOnConfigRecreateInstance, OnUserChange: ApplyOnUserRestartProcess},
		{OnConfigurationChange: ApplyOnConfigManual, OnUserChange: ApplyOnUserRecreateInstance},
	}
	for i, s := range strategies {
		if err := s.Validate(); err != nil {
			t.Errorf("strategy %d: expected valid, got: %v", i, err)
		}
	}
}

func TestApplyStrategy_Validate_Unknown(t *testing.T) {
	tests := []struct {
		name    string
		strat   ApplyStrategy
		wantErr string
	}{
		{"unknown on_config", ApplyStrategy{OnConfigurationChange: "hot_reload", OnUserChange: ApplyOnUserHotReloadUsers}, "unknown on_configuration_change"},
		{"unknown on_user", ApplyStrategy{OnConfigurationChange: ApplyOnConfigRestartProcess, OnUserChange: "manual"}, "unknown on_user_change"},
		{"empty on_config", ApplyStrategy{OnConfigurationChange: "", OnUserChange: ApplyOnUserHotReloadUsers}, "missing on_configuration_change"},
		{"empty on_user", ApplyStrategy{OnConfigurationChange: ApplyOnConfigRestartProcess, OnUserChange: ""}, "missing on_user_change"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.strat.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// UserSnapshot tests
// ---------------------------------------------------------------------------

func TestUserSnapshot_Validate_Valid(t *testing.T) {
	for _, proto := range []string{"hysteria2", "anytls", "shadowsocks"} {
		snap := validUserSnapshot()
		snap.Protocol = proto
		if err := snap.Validate(); err != nil {
			t.Errorf("protocol %s: expected valid, got: %v", proto, err)
		}
	}
}

func TestUserSnapshot_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*UserSnapshot)
		wantErr string
	}{
		{"missing revision", func(u *UserSnapshot) { u.Revision = "" }, "missing user snapshot revision"},
		{"missing configuration_revision", func(u *UserSnapshot) { u.ConfigurationRevision = "" }, "missing configuration_revision"},
		{"missing node_id", func(u *UserSnapshot) { u.NodeID = "" }, "missing node_id"},
		{"missing inbound_id", func(u *UserSnapshot) { u.InboundID = "" }, "missing inbound_id"},
		{"missing protocol", func(u *UserSnapshot) { u.Protocol = "" }, "missing protocol"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := validUserSnapshot()
			tt.mutate(snap)
			err := snap.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestUserSnapshot_Validate_EmptyUsersAllowed(t *testing.T) {
	snap := validUserSnapshot()
	snap.Users = nil
	if err := snap.Validate(); err != nil {
		t.Fatalf("empty users list should be valid, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Credential / User tests
// ---------------------------------------------------------------------------

func TestCredential_Validate_Password(t *testing.T) {
	c := Credential{Type: CredentialTypePassword, Password: "secret"}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestCredential_Validate_UnknownType(t *testing.T) {
	c := Credential{Type: "certificate", Password: ""}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for unknown credential type")
	}
	if !contains(err.Error(), "unsupported credential type") {
		t.Fatalf("error %q should contain 'unsupported credential type'", err.Error())
	}
}

func TestCredential_Validate_MissingType(t *testing.T) {
	c := Credential{Type: "", Password: "pw"}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing credential type")
	}
	if !contains(err.Error(), "missing credential type") {
		t.Fatalf("error %q should contain 'missing credential type'", err.Error())
	}
}

func TestCredential_Validate_MissingPassword(t *testing.T) {
	c := Credential{Type: CredentialTypePassword, Password: ""}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for missing password")
	}
	if !contains(err.Error(), "missing credential password") {
		t.Fatalf("error %q should contain 'missing credential password'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// TrafficReport tests
// ---------------------------------------------------------------------------

func TestTrafficReport_Validate_Valid(t *testing.T) {
	if err := validTrafficReport().Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestTrafficReport_Validate_EmptyRecordsAllowed(t *testing.T) {
	tr := validTrafficReport()
	tr.Records = nil
	if err := tr.Validate(); err != nil {
		t.Fatalf("empty records should be valid, got: %v", err)
	}
}

func TestTrafficReport_Validate_BadTimeRange(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*TrafficReport)
		wantErr string
	}{
		{
			"ended_at equals started_at",
			func(t *TrafficReport) { t.EndedAt = t.StartedAt },
			"ended_at must be after started_at",
		},
		{
			"ended_at before started_at",
			func(t *TrafficReport) { t.EndedAt = t.StartedAt.Add(-time.Minute) },
			"ended_at must be after started_at",
		},
		{
			"missing started_at",
			func(t *TrafficReport) { t.StartedAt = time.Time{} },
			"missing started_at",
		},
		{
			"missing ended_at",
			func(t *TrafficReport) { t.EndedAt = time.Time{} },
			"missing ended_at",
		},
		{
			"missing configuration_revision",
			func(t *TrafficReport) { t.ConfigurationRevision = "" },
			"missing configuration_revision",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := validTrafficReport()
			tt.mutate(tr)
			err := tr.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestTrafficRecord_Validate_NegativeBytes(t *testing.T) {
	tests := []struct {
		name    string
		rec     TrafficRecord
		wantErr string
	}{
		{"negative upload", TrafficRecord{InboundID: "in", UserID: "u", UploadBytes: -1, DownloadBytes: 0}, "non-negative"},
		{"negative download", TrafficRecord{InboundID: "in", UserID: "u", UploadBytes: 0, DownloadBytes: -1}, "non-negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rec.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestTrafficRecord_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		rec     TrafficRecord
		wantErr string
	}{
		{"missing inbound_id", TrafficRecord{InboundID: "", UserID: "u", UploadBytes: 0, DownloadBytes: 0}, "missing inbound_id"},
		{"missing user_id", TrafficRecord{InboundID: "in", UserID: "", UploadBytes: 0, DownloadBytes: 0}, "missing user_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rec.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestTrafficRecord_Validate_ZeroBytesAllowed(t *testing.T) {
	rec := TrafficRecord{InboundID: "in", UserID: "u", UploadBytes: 0, DownloadBytes: 0}
	if err := rec.Validate(); err != nil {
		t.Fatalf("zero bytes should be valid, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Heartbeat tests
// ---------------------------------------------------------------------------

func TestHeartbeat_Validate_Valid(t *testing.T) {
	if err := validHeartbeat().Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestHeartbeat_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Heartbeat)
		wantErr string
	}{
		{"missing observed_at", func(h *Heartbeat) { h.ObservedAt = time.Time{} }, "missing observed_at"},
		{"missing sing_box_version", func(h *Heartbeat) { h.SingBoxVersion = "" }, "missing sing_box_version"},
		{"missing adapter_version", func(h *Heartbeat) { h.AdapterVersion = "" }, "missing adapter_version"},
		{"missing applied_config_revision", func(h *Heartbeat) { h.AppliedConfigurationRevision = "" }, "missing applied_configuration_revision"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hb := validHeartbeat()
			tt.mutate(hb)
			err := hb.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestHeartbeat_Validate_NoRuntimeAllowed(t *testing.T) {
	hb := validHeartbeat()
	hb.Runtime = nil
	if err := hb.Validate(); err != nil {
		t.Fatalf("runtime is optional, got: %v", err)
	}
}

func TestHeartbeat_Validate_PendingConfigRevisionNilAllowed(t *testing.T) {
	hb := validHeartbeat()
	hb.PendingConfigurationRevision = nil
	if err := hb.Validate(); err != nil {
		t.Fatalf("nil pending config revision should be valid, got: %v", err)
	}
}

func TestHeartbeat_Validate_PendingConfigRevisionSet(t *testing.T) {
	hb := validHeartbeat()
	hb.PendingConfigurationRevision = strPtr("cfg-0006")
	if err := hb.Validate(); err != nil {
		t.Fatalf("non-nil pending config revision should be valid, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// UserLoadStatus tests
// ---------------------------------------------------------------------------

func TestUserLoadStatus_AllValues(t *testing.T) {
	statuses := []UserLoadStatus{
		UserLoadStatusOK,
		UserLoadStatusStale,
		UserLoadStatusEmptyInitialLoad,
		UserLoadStatusRevisionConflict,
		UserLoadStatusUnsupportedProto,
		UserLoadStatusApplyFailed,
	}
	for _, s := range statuses {
		if !ValidUserLoadStatuses[s] {
			t.Errorf("expected %q to be valid", s)
		}
	}
}

func TestHeartbeatInbound_Validate_UnknownStatus(t *testing.T) {
	hi := HeartbeatInbound{
		Tag:              "in",
		Protocol:         "hysteria2",
		CurrentUserCount: 0,
		Status:           UserLoadStatus("unknown_status"),
	}
	err := hi.Validate()
	if err == nil {
		t.Fatal("expected error for unknown status")
	}
	if !contains(err.Error(), "unknown status") {
		t.Fatalf("error %q should contain 'unknown status'", err.Error())
	}
}

func TestHeartbeatInbound_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		hi      HeartbeatInbound
		wantErr string
	}{
		{"missing tag", HeartbeatInbound{Tag: "", Protocol: "hysteria2", Status: UserLoadStatusOK}, "missing tag"},
		{"missing protocol", HeartbeatInbound{Tag: "in", Protocol: "", Status: UserLoadStatusOK}, "missing protocol"},
		{"missing status", HeartbeatInbound{Tag: "in", Protocol: "hysteria2", Status: ""}, "missing status"},
		{"negative user count", HeartbeatInbound{Tag: "in", Protocol: "hysteria2", Status: UserLoadStatusOK, CurrentUserCount: -1}, "current_user_count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.hi.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProblemDetails tests
// ---------------------------------------------------------------------------

func TestProblemDetails_Validate_Valid(t *testing.T) {
	pd := ProblemDetails{
		Type:      "https://panel.example.com/problems/revision-conflict",
		Title:     "Revision conflict",
		Status:    409,
		Code:      "REVISION_CONFLICT",
		Detail:    "some detail",
		RequestID: "req_01jzabcdef",
	}
	if err := pd.Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestProblemDetails_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		pd      ProblemDetails
		wantErr string
	}{
		{"missing title", ProblemDetails{Status: 400, Code: "BAD_REQUEST", RequestID: "r1"}, "missing title"},
		{"missing status", ProblemDetails{Title: "t", Code: "BAD_REQUEST", RequestID: "r1"}, "missing or invalid status"},
		{"zero status", ProblemDetails{Title: "t", Status: 0, Code: "BAD_REQUEST", RequestID: "r1"}, "missing or invalid status"},
		{"missing code", ProblemDetails{Title: "t", Status: 400, RequestID: "r1"}, "missing code"},
		{"missing request_id", ProblemDetails{Title: "t", Status: 400, Code: "BAD_REQUEST"}, "missing request_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pd.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ManagedInbound tests
// ---------------------------------------------------------------------------

func TestManagedInbound_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mi      ManagedInbound
		wantErr string
	}{
		{"missing inbound_id", ManagedInbound{InboundID: "", Tag: "t", Protocol: "hysteria2"}, "missing inbound_id"},
		{"missing tag", ManagedInbound{InboundID: "id", Tag: "", Protocol: "hysteria2"}, "missing tag"},
		{"missing protocol", ManagedInbound{InboundID: "id", Tag: "t", Protocol: ""}, "missing protocol"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mi.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestManagedInbound_Validate_UnknownProtocolOK(t *testing.T) {
	mi := ManagedInbound{InboundID: "vmess-main", Tag: "vmess-in", Protocol: "vmess"}
	if err := mi.Validate(); err != nil {
		t.Fatalf("unknown protocol should NOT be an error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// IsSupportedProtocol tests
// ---------------------------------------------------------------------------

func TestIsSupportedProtocol(t *testing.T) {
	for _, p := range []string{"hysteria2", "anytls", "shadowsocks"} {
		if !IsSupportedProtocol(p) {
			t.Errorf("expected %q to be supported", p)
		}
	}
	if IsSupportedProtocol("vmess") {
		t.Error("vmess should not be supported")
	}
	if IsSupportedProtocol("") {
		t.Error("empty string should not be supported")
	}
}

// ---------------------------------------------------------------------------
// JSON round-trip smoke test
// ---------------------------------------------------------------------------

func TestConfigurationResponse_JSONRoundTrip(t *testing.T) {
	orig := validConfigurationResponse()
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded ConfigurationResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Revision != orig.Revision {
		t.Errorf("revision mismatch: got %q, want %q", decoded.Revision, orig.Revision)
	}
	if decoded.NodeID != orig.NodeID {
		t.Errorf("node_id mismatch: got %q, want %q", decoded.NodeID, orig.NodeID)
	}
	if decoded.ApplyStrategy.OnConfigurationChange != orig.ApplyStrategy.OnConfigurationChange {
		t.Errorf("apply_strategy.on_configuration_change mismatch")
	}
}

func TestUserSnapshot_JSONRoundTrip(t *testing.T) {
	orig := validUserSnapshot()
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded UserSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Revision != orig.Revision {
		t.Errorf("revision mismatch: got %q, want %q", decoded.Revision, orig.Revision)
	}
	if decoded.ConfigurationRevision != orig.ConfigurationRevision {
		t.Errorf("configuration_revision mismatch: got %q, want %q", decoded.ConfigurationRevision, orig.ConfigurationRevision)
	}
	if len(decoded.Users) != len(orig.Users) {
		t.Fatalf("users count mismatch: got %d, want %d", len(decoded.Users), len(orig.Users))
	}
	if decoded.Users[0].Credential.Type != CredentialTypePassword {
		t.Errorf("credential type mismatch: got %q, want %q", decoded.Users[0].Credential.Type, CredentialTypePassword)
	}
}

// ---------------------------------------------------------------------------
// contains helper
// ---------------------------------------------------------------------------

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || containsSubstr(s, sub))
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
