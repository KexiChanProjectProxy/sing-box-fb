package contract

const (
	UserIdentitySourceUserID     = "user_id"
	UserRoutingCapabilityVersion = "user-routing.v1"
	UserRoutingAdapterVersion    = "1.0.0-user-routing.1"
)

type AdapterCapabilities struct {
	UserRouting *UserRoutingCapability `json:"user_routing,omitempty"`
}

type UserRoutingCapability struct {
	Supported          bool     `json:"supported"`
	CapabilityVersion  string   `json:"capability_version"`
	IdentitySource     string   `json:"identity_source,omitempty"`
	AdapterVersion     string   `json:"adapter_version,omitempty"`
	SupportedProtocols []string `json:"supported_protocols"`
}

func EnabledUserRoutingCapabilities() *AdapterCapabilities {
	return &AdapterCapabilities{UserRouting: &UserRoutingCapability{
		Supported:          true,
		CapabilityVersion:  UserRoutingCapabilityVersion,
		IdentitySource:     UserIdentitySourceUserID,
		AdapterVersion:     UserRoutingAdapterVersion,
		SupportedProtocols: []string{ProtocolAnyTLS, ProtocolHysteria2},
	}}
}
