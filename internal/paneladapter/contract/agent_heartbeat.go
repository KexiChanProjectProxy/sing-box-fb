package contract

import "time"

type AgentHeartbeat struct {
	ObservedAt              time.Time             `json:"observed_at"`
	AdapterVersion          string                `json:"adapter_version"`
	AdapterFingerprint      string                `json:"adapter_fingerprint"`
	SingBoxVersion          string                `json:"sing_box_version,omitempty"`
	AppliedManifestRevision uint64                `json:"applied_manifest_revision,omitempty"`
	ManifestAppliedAt       *time.Time            `json:"manifest_applied_at,omitempty"`
	ManifestApplyError      string                `json:"manifest_apply_error,omitempty"`
	Preflight               *AgentPreflightReport `json:"preflight,omitempty"`
	Capabilities            *AdapterCapabilities  `json:"capabilities,omitempty"`
	Nodes                   []AgentHeartbeatNode  `json:"nodes"`
}

type AgentHeartbeatNode struct {
	NodeID          string             `json:"node_id"`
	InboundStatuses []HeartbeatInbound `json:"inbound_statuses"`
}
