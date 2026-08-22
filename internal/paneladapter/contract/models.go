// Package contract defines the API v1 request/response models and validators
// for the sing-box panel adapter. These types are the shared contract used by
// the REST client (T2), runtime (T8), user polling (T9), traffic reporter (T10),
// and heartbeat (T11) components.
//
// Validation rules follow the API specification at
// .omo/plans/sing-box-panel-api-spec-v1.md.
package contract

import (
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// Supported protocol allow-list (v1)
// ---------------------------------------------------------------------------

const (
	ProtocolHysteria2   = "hysteria2"
	ProtocolAnyTLS      = "anytls"
	ProtocolShadowsocks = "shadowsocks"
)

// SupportedProtocols lists the inbound protocols the adapter can apply users to.
// Unknown managed_inbounds[].protocol values are retained as unsupported metadata
// (heartbeat reports unsupported_protocol) but never receive user-apply calls.
var SupportedProtocols = map[string]bool{
	ProtocolHysteria2:   true,
	ProtocolAnyTLS:      true,
	ProtocolShadowsocks: true,
}

// IsSupportedProtocol returns true if the protocol is in the v1 allow-list.
func IsSupportedProtocol(protocol string) bool {
	return SupportedProtocols[protocol]
}

// ---------------------------------------------------------------------------
// Apply strategy
// ---------------------------------------------------------------------------

// ApplyStrategy tells the adapter how to react when the configuration or user
// set changes.
type ApplyStrategy struct {
	OnConfigurationChange string `json:"on_configuration_change"`
	OnUserChange          string `json:"on_user_change"`
}

// Valid OnConfigurationChange values.
const (
	ApplyOnConfigRestartProcess   = "restart_process"
	ApplyOnConfigRecreateInstance = "recreate_instance"
	ApplyOnConfigManual           = "manual"
)

var validOnConfigurationChange = map[string]bool{
	ApplyOnConfigRestartProcess:   true,
	ApplyOnConfigRecreateInstance: true,
	ApplyOnConfigManual:           true,
}

// Valid OnUserChange values.
const (
	ApplyOnUserHotReloadUsers   = "hot_reload_users"
	ApplyOnUserNone             = "none"
	ApplyOnUserRestartProcess   = "restart_process"
	ApplyOnUserRecreateInstance = "recreate_instance"
)

var validOnUserChange = map[string]bool{
	ApplyOnUserHotReloadUsers:   true,
	ApplyOnUserRestartProcess:   true,
	ApplyOnUserRecreateInstance: true,
}

// ---------------------------------------------------------------------------
// Poll intervals
// ---------------------------------------------------------------------------

// PollIntervals configures the polling cadence for each resource.
type PollIntervals struct {
	ConfigurationSeconds int `json:"configuration_seconds"`
	UsersSeconds         int `json:"users_seconds"`
	TrafficSeconds       int `json:"traffic_seconds"`
	HeartbeatSeconds     int `json:"heartbeat_seconds"`
}

// ---------------------------------------------------------------------------
// Configuration response
// ---------------------------------------------------------------------------

// ConfigurationResponse is the payload returned by
// GET /api/v1/nodes/{node_id}/configuration.
type ConfigurationResponse struct {
	Revision              string            `json:"revision"`
	APIVersion            string            `json:"api_version"`
	NodeID                string            `json:"node_id"`
	ApplyStrategy         ApplyStrategy     `json:"apply_strategy"`
	PollIntervals         PollIntervals     `json:"poll_intervals"`
	ManagedInbounds       []ManagedInbound  `json:"managed_inbounds"`
	ClickHouse            *ClickHouseConfig `json:"clickhouse,omitempty"`
	Binary                *BinaryUpdate     `json:"binary,omitempty"`
	SingBoxConfigTemplate json.RawMessage   `json:"sing_box_config_template"`
}

// BinaryUpdate is optional panel-pushed adapter/node binary version info.
// When version differs from the running binary, the adapter downloads,
// atomically replaces, and execs the new file. A failed start rolls back
// and blacklists that version.
type BinaryUpdate struct {
	Version   string                    `json:"version"`
	URL       string                    `json:"url,omitempty"`
	SHA256    string                    `json:"sha256,omitempty"`
	Downloads map[string]BinaryDownload `json:"downloads,omitempty"`
}

// BinaryDownload is one architecture-specific artifact.
// downloads keys are GOOS/GOARCH, e.g. "linux/amd64".
type BinaryDownload struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

// ClickHouseConfig is optional panel-pushed access-log sink settings.
// The adapter injects a clickhouse service and sets tag to the local hostname.
type ClickHouseConfig struct {
	Server     string         `json:"server"`
	ServerPort uint16         `json:"server_port,omitempty"`
	Database   string         `json:"database,omitempty"`
	Table      string         `json:"table,omitempty"`
	Username   string         `json:"username,omitempty"`
	Password   string         `json:"password,omitempty"`
	Protocol   string         `json:"protocol,omitempty"`
	TLS        *ClickHouseTLS `json:"tls,omitempty"`
}

// ClickHouseTLS is the subset of outbound TLS the panel may push for ClickHouse.
type ClickHouseTLS struct {
	Enabled    bool   `json:"enabled,omitempty"`
	ServerName string `json:"server_name,omitempty"`
	Insecure   bool   `json:"insecure,omitempty"`
}

// ManagedInbound describes one panel-managed inbound endpoint.
type ManagedInbound struct {
	InboundID       string `json:"inbound_id"`
	Tag             string `json:"tag"`
	Protocol        string `json:"protocol"`
	UserResource    string `json:"user_resource,omitempty"`
	UserApplyPolicy string `json:"user_apply_policy,omitempty"`
}

const UserIdentitySourceUserID = "user_id"

// UserRuntimeIdentity is the panel-declared routing identity for a managed user.
type UserRuntimeIdentity struct {
	Source string `json:"source"`
	Value  string `json:"value"`
}

// ---------------------------------------------------------------------------
// User snapshot
// ---------------------------------------------------------------------------

// UserSnapshot is the payload returned by
// GET /api/v1/nodes/{node_id}/inbounds/{inbound_id}/users.
type UserSnapshot struct {
	Revision              string `json:"revision"`
	ConfigurationRevision string `json:"configuration_revision"`
	NodeID                string `json:"node_id"`
	InboundID             string `json:"inbound_id"`
	Protocol              string `json:"protocol"`
	Users                 []User `json:"users"`
}

// User represents one managed user within a UserSnapshot.
type User struct {
	UserID          string               `json:"user_id"`
	Name            string               `json:"name"`
	RuntimeIdentity *UserRuntimeIdentity `json:"runtime_identity,omitempty"`
	Credential      Credential           `json:"credential"`
}

// Credential holds the authentication material for a user.
// In v1, only type "password" is supported; unknown types must be rejected.
type Credential struct {
	Type     string `json:"type"`
	Password string `json:"password,omitempty"`
}

// CredentialTypePassword is the only supported credential type in v1.
const CredentialTypePassword = "password"

// ---------------------------------------------------------------------------
// Traffic report
// ---------------------------------------------------------------------------

// TrafficReport is the payload sent to
// POST /api/v1/nodes/{node_id}/traffic-reports.
type TrafficReport struct {
	StartedAt             time.Time       `json:"started_at"`
	EndedAt               time.Time       `json:"ended_at"`
	ConfigurationRevision string          `json:"configuration_revision"`
	Records               []TrafficRecord `json:"records"`
}

// TrafficRecord is one per-user traffic delta within a report.
type TrafficRecord struct {
	InboundID     string `json:"inbound_id"`
	UserID        string `json:"user_id"`
	UploadBytes   int64  `json:"upload_bytes"`
	DownloadBytes int64  `json:"download_bytes"`
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

// Heartbeat is the payload sent to
// POST /api/v1/nodes/{node_id}/heartbeats.
type Heartbeat struct {
	ObservedAt                   time.Time          `json:"observed_at"`
	SingBoxVersion               string             `json:"sing_box_version"`
	AdapterVersion               string             `json:"adapter_version"`
	AppliedConfigurationRevision string             `json:"applied_configuration_revision"`
	PendingConfigurationRevision *string            `json:"pending_configuration_revision,omitempty"`
	InboundStatuses              []HeartbeatInbound `json:"inbound_statuses"`
	Runtime                      *HeartbeatRuntime  `json:"runtime,omitempty"`
	BlacklistedBinaryVersions    []string           `json:"blacklisted_binary_versions,omitempty"`
}

// HeartbeatInbound reports per-inbound status within a heartbeat.
type HeartbeatInbound struct {
	Tag              string         `json:"tag"`
	Protocol         string         `json:"protocol"`
	Status           UserLoadStatus `json:"status"`
	CurrentUserCount int            `json:"current_user_count"`
}

// HeartbeatRuntime contains optional runtime metrics.
type HeartbeatRuntime struct {
	UptimeSeconds int64 `json:"uptime_seconds,omitempty"`
	Connections   int   `json:"connections,omitempty"`
	MemoryBytes   int64 `json:"memory_bytes,omitempty"`
}

// ---------------------------------------------------------------------------
// UserLoadStatus
// ---------------------------------------------------------------------------

// UserLoadStatus represents the load status of a managed inbound's user set.
type UserLoadStatus string

const (
	UserLoadStatusOK               UserLoadStatus = "ok"
	UserLoadStatusStale            UserLoadStatus = "stale"
	UserLoadStatusEmptyInitialLoad UserLoadStatus = "empty_initial_load"
	UserLoadStatusRevisionConflict UserLoadStatus = "revision_conflict"
	UserLoadStatusUnsupportedProto UserLoadStatus = "unsupported_protocol"
	UserLoadStatusApplyFailed      UserLoadStatus = "apply_failed"
)

// ValidUserLoadStatuses is the complete set of allowed UserLoadStatus values.
var ValidUserLoadStatuses = map[UserLoadStatus]bool{
	UserLoadStatusOK:               true,
	UserLoadStatusStale:            true,
	UserLoadStatusEmptyInitialLoad: true,
	UserLoadStatusRevisionConflict: true,
	UserLoadStatusUnsupportedProto: true,
	UserLoadStatusApplyFailed:      true,
}

// ---------------------------------------------------------------------------
// ProblemDetails (error response)
// ---------------------------------------------------------------------------

// ProblemDetails is the RFC 7807-like error body returned by the panel API.
type ProblemDetails struct {
	Type      string `json:"type,omitempty"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id"`
}

// ---------------------------------------------------------------------------
// ManagedUserCredential is the adapter-internal representation of a user
// credential extracted from a UserSnapshot, ready for protocol-specific
// user replacement.
// ---------------------------------------------------------------------------

// ManagedUserCredential is the normalized credential the adapter passes to
// protocol-specific user replacement functions.
type ManagedUserCredential struct {
	UserID   string
	Name     string
	Password string
}
