package main

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
)

func TestBuildAgentHeartbeatAdvertisesUserRoutingCapability(t *testing.T) {
	nodeID := "019f8f04-dd6c-717d-a223-f30325eba6da"
	current := &state.State{
		Manifest: state.ManifestState{Snapshot: contract.AgentManifest{ManifestRevision: 7}},
		Nodes: map[string]state.NodeState{
			nodeID: {Inbounds: map[string]state.InboundState{
				nodeID: {Protocol: contract.ProtocolHysteria2, UserLoadStatus: string(contract.UserLoadStatusOK), UserCount: 3},
			}},
		},
	}
	now := time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)

	heartbeat := buildAgentHeartbeat(current, "019f8f04-dbe0-7a38-97b1-82724b70d056", now)

	if heartbeat.AdapterVersion != contract.UserRoutingAdapterVersion {
		t.Fatalf("adapter_version = %q", heartbeat.AdapterVersion)
	}
	if heartbeat.Capabilities == nil || heartbeat.Capabilities.UserRouting == nil || !heartbeat.Capabilities.UserRouting.Supported {
		t.Fatalf("capabilities = %#v", heartbeat.Capabilities)
	}
	if heartbeat.Capabilities.UserRouting.IdentitySource != contract.UserIdentitySourceUserID {
		t.Fatalf("identity_source = %q", heartbeat.Capabilities.UserRouting.IdentitySource)
	}
	if heartbeat.AppliedManifestRevision != 7 || len(heartbeat.Nodes) != 1 {
		t.Fatalf("heartbeat = %#v", heartbeat)
	}
	if got := heartbeat.Nodes[0].InboundStatuses[0].Tag; got != nodeID {
		t.Fatalf("heartbeat inbound tag = %q, want node ID", got)
	}
	if len(heartbeat.AdapterFingerprint) != 64 {
		t.Fatalf("adapter fingerprint length = %d", len(heartbeat.AdapterFingerprint))
	}
}
