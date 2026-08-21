package contract

// AgentPreflightReport is host-side evidence collected before the control
// plane allows an agent to manage virtual nodes.
type AgentPreflightReport struct {
	Ports           bool     `json:"ports"`
	System          bool     `json:"system"`
	TimeSync        bool     `json:"time_sync"`
	Certificate     bool     `json:"certificate"`
	OutboundNetwork bool     `json:"outbound_network"`
	Permissions     bool     `json:"permissions"`
	Capabilities    bool     `json:"capabilities"`
	Errors          []string `json:"errors,omitempty"`
}

func (report AgentPreflightReport) Passed() bool {
	return report.Ports &&
		report.System &&
		report.TimeSync &&
		report.Certificate &&
		report.OutboundNetwork &&
		report.Permissions &&
		report.Capabilities &&
		len(report.Errors) == 0
}
