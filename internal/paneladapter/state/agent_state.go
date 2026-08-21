package state

import (
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

type ManifestState struct {
	ETag       string                 `json:"etag,omitempty"`
	Snapshot   contract.AgentManifest `json:"snapshot"`
	AppliedAt  time.Time              `json:"applied_at,omitempty"`
	ApplyError string                 `json:"apply_error,omitempty"`
}

type NodeState struct {
	Manifest       contract.AgentManifestNode `json:"manifest"`
	Config         ConfigState                `json:"config"`
	Inbounds       map[string]InboundState    `json:"inbounds"`
	PendingReports []PendingReport            `json:"pending_reports,omitempty"`
}

func (state *State) Clone() *State {
	if state == nil {
		return newState()
	}
	clone := &State{
		Version:        state.Version,
		Manifest:       cloneManifestState(state.Manifest),
		Config:         state.Config,
		Nodes:          make(map[string]NodeState, len(state.Nodes)),
		Inbounds:       make(map[string]InboundState, len(state.Inbounds)),
		PendingReports: append([]PendingReport(nil), state.PendingReports...),
	}
	for nodeID, node := range state.Nodes {
		clone.Nodes[nodeID] = cloneNodeState(node)
	}
	for inboundID, inbound := range state.Inbounds {
		clone.Inbounds[inboundID] = inbound
	}
	return clone
}

func cloneManifestState(manifest ManifestState) ManifestState {
	clone := manifest
	clone.Snapshot.Nodes = append([]contract.AgentManifestNode(nil), manifest.Snapshot.Nodes...)
	return clone
}

func cloneNodeState(node NodeState) NodeState {
	clone := node
	clone.Inbounds = make(map[string]InboundState, len(node.Inbounds))
	for inboundID, inbound := range node.Inbounds {
		clone.Inbounds[inboundID] = inbound
	}
	clone.PendingReports = append([]PendingReport(nil), node.PendingReports...)
	return clone
}
