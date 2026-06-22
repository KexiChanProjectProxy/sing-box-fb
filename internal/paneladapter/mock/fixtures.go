// Package mock provides a comprehensive mock panel server and test helpers
// for testing the sing-box panel adapter. The fixture builders create valid
// contract types for tests, and the assertion helpers verify recorded requests.
package mock

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

// Default constants matching the mock server defaults.
const (
	DefaultNodeID = "default-node"
	DefaultToken  = "test-token"
)

// ValidConfigurationResponse returns a fully valid ConfigurationResponse
// with sensible defaults matching the mock server's expected values.
// Each call returns a fresh copy — never a cached/shared struct.
func ValidConfigurationResponse() *contract.ConfigurationResponse {
	return &contract.ConfigurationResponse{
		Revision:   "rev-001",
		APIVersion: "v1",
		NodeID:     DefaultNodeID,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         30,
			TrafficSeconds:       60,
			HeartbeatSeconds:     120,
		},
		ManagedInbounds: []contract.ManagedInbound{
			{
				InboundID: "inb-hy2",
				Tag:       "hy2-in",
				Protocol:  "hysteria2",
			},
		},
		SingBoxConfigTemplate: json.RawMessage(`{"inbounds":[],"outbounds":[{"type":"direct","tag":"direct"}]}`),
	}
}

// ValidUserSnapshot returns a valid UserSnapshot with 2 password-credential users.
// Each call returns a fresh copy.
func ValidUserSnapshot(inboundID, protocol, configRev string) *contract.UserSnapshot {
	return &contract.UserSnapshot{
		Revision:              "user-rev-001",
		ConfigurationRevision: configRev,
		NodeID:                DefaultNodeID,
		InboundID:             inboundID,
		Protocol:              protocol,
		Users: []contract.User{
			{
				UserID: "user-001",
				Name:   "user-001",
				Credential: contract.Credential{
					Type:     contract.CredentialTypePassword,
					Password: "pass-001",
				},
			},
			{
				UserID: "user-002",
				Name:   "user-002",
				Credential: contract.Credential{
					Type:     contract.CredentialTypePassword,
					Password: "pass-002",
				},
			},
		},
	}
}

// ValidTrafficReport returns a valid TrafficReport with a non-overlapping time
// window and 2 records. Each call returns a fresh copy.
func ValidTrafficReport(configRev string) *contract.TrafficReport {
	now := time.Now().UTC()
	return &contract.TrafficReport{
		StartedAt:             now.Add(-2 * time.Minute),
		EndedAt:               now.Add(-time.Minute),
		ConfigurationRevision: configRev,
		Records: []contract.TrafficRecord{
			{
				InboundID:     "inb-hy2",
				UserID:        "user-001",
				UploadBytes:   1024,
				DownloadBytes: 2048,
			},
			{
				InboundID:     "inb-hy2",
				UserID:        "user-002",
				UploadBytes:   512,
				DownloadBytes: 1024,
			},
		},
	}
}

// ValidHeartbeat returns a valid Heartbeat with 1 inbound status.
// Each call returns a fresh copy.
func ValidHeartbeat(configRev string) *contract.Heartbeat {
	return &contract.Heartbeat{
		ObservedAt:                   time.Now().UTC(),
		SingBoxVersion:               "1.12.0",
		AdapterVersion:               "0.1.0",
		AppliedConfigurationRevision: configRev,
		Inbounds: []contract.HeartbeatInbound{
			{
				InboundID:           "inb-hy2",
				Protocol:            "hysteria2",
				AppliedUserRevision: "user-rev-001",
				UserCount:           2,
				UserLoadStatus:      contract.UserLoadStatusOK,
			},
		},
	}
}

// ProblemDetails returns a ProblemDetails with the given values and a generated
// request ID.
func ProblemDetails(status int, code, title, detail string) *contract.ProblemDetails {
	return &contract.ProblemDetails{
		Type:      fmt.Sprintf("https://panel.example.com/errors/%s", code),
		Title:     title,
		Status:    status,
		Code:      code,
		Detail:    detail,
		RequestID: fmt.Sprintf("req-%d-%d", status, time.Now().UnixNano()),
	}
}

// ErrorResponseBody returns a JSON-encoded ProblemDetails byte slice ready for
// writing to an http.ResponseWriter.
func ErrorResponseBody(status int, code, title string) []byte {
	pd := ProblemDetails(status, code, title, "")
	data, err := json.Marshal(pd)
	if err != nil {
		// Should never happen with ProblemDetails — all fields are simple strings/ints.
		return []byte(`{"title":"internal mock error"}`)
	}
	return data
}

// Option is a functional option for customizing a ConfigurationResponse.
type Option func(*contract.ConfigurationResponse)

// NewConfigurationResponse builds a ConfigurationResponse starting from
// ValidConfigurationResponse defaults and applying the given Option overrides.
func NewConfigurationResponse(opts ...Option) *contract.ConfigurationResponse {
	cfg := ValidConfigurationResponse()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithManagedInbound returns an Option that appends a managed inbound to the
// configuration's ManagedInbounds list.
func WithManagedInbound(inboundID, tag, protocol string) Option {
	return func(cfg *contract.ConfigurationResponse) {
		cfg.ManagedInbounds = append(cfg.ManagedInbounds, contract.ManagedInbound{
			InboundID: inboundID,
			Tag:       tag,
			Protocol:  protocol,
		})
	}
}

// WithUserCount returns a functional option for UserSnapshot that sets the
// Users slice to contain exactly count password-credential users.
func WithUserCount(inboundID string, count int) func(*contract.UserSnapshot) {
	return func(snap *contract.UserSnapshot) {
		users := make([]contract.User, count)
		for i := 0; i < count; i++ {
			users[i] = contract.User{
				UserID: fmt.Sprintf("user-%03d", i+1),
				Name:   fmt.Sprintf("user-%03d", i+1),
				Credential: contract.Credential{
					Type:     contract.CredentialTypePassword,
					Password: fmt.Sprintf("pass-%03d", i+1),
				},
			}
		}
		snap.Users = users
	}
}
