package contract

import (
	"fmt"
)

const (
	AgentManifestAPIVersion = "v1"
	MaxAgentManifestNodes   = 256
)

type AgentManifestNodeStatus string

const (
	AgentManifestNodeStatusPending  AgentManifestNodeStatus = "pending"
	AgentManifestNodeStatusActive   AgentManifestNodeStatus = "active"
	AgentManifestNodeStatusDisabled AgentManifestNodeStatus = "disabled"
	AgentManifestNodeStatusDraining AgentManifestNodeStatus = "draining"
)

type AgentManifest struct {
	APIVersion       string                      `json:"api_version"`
	AgentID          string                      `json:"agent_id"`
	ManifestRevision uint64                      `json:"manifest_revision"`
	Reconciliation   AgentManifestReconciliation `json:"reconciliation"`
	Nodes            []AgentManifestNode         `json:"nodes"`
}

type AgentManifestReconciliation struct {
	FullSnapshot     bool `json:"full_snapshot"`
	PollAfterSeconds int  `json:"poll_after_seconds"`
}

type AgentManifestNode struct {
	NodeID                string                  `json:"node_id"`
	Status                AgentManifestNodeStatus `json:"status"`
	AssignmentRevision    uint64                  `json:"assignment_revision"`
	ConfigurationRevision string                  `json:"configuration_revision,omitempty"`
	ConfigurationResource string                  `json:"configuration_resource"`
	UserResource          string                  `json:"user_resource"`
}

func (manifest *AgentManifest) Validate() error {
	if manifest == nil {
		return fmt.Errorf("agent manifest is required")
	}
	if manifest.APIVersion != AgentManifestAPIVersion {
		return fmt.Errorf("unsupported agent manifest api_version %q", manifest.APIVersion)
	}
	if manifest.AgentID == "" {
		return fmt.Errorf("agent manifest agent_id is required")
	}
	if !manifest.Reconciliation.FullSnapshot {
		return fmt.Errorf("agent manifest must be a full snapshot")
	}
	if manifest.Reconciliation.PollAfterSeconds <= 0 {
		return fmt.Errorf("agent manifest poll_after_seconds must be positive")
	}
	if len(manifest.Nodes) > MaxAgentManifestNodes {
		return fmt.Errorf("agent manifest exceeds %d nodes", MaxAgentManifestNodes)
	}
	seen := make(map[string]struct{}, len(manifest.Nodes))
	for index := range manifest.Nodes {
		node := manifest.Nodes[index]
		if err := node.Validate(); err != nil {
			return fmt.Errorf("agent manifest node %d: %w", index, err)
		}
		if _, exists := seen[node.NodeID]; exists {
			return fmt.Errorf("agent manifest contains duplicate node_id %q", node.NodeID)
		}
		seen[node.NodeID] = struct{}{}
	}
	return nil
}

func (node AgentManifestNode) Validate() error {
	if node.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	switch node.Status {
	case AgentManifestNodeStatusPending,
		AgentManifestNodeStatusActive,
		AgentManifestNodeStatusDisabled,
		AgentManifestNodeStatusDraining:
	default:
		return fmt.Errorf("unsupported status %q", node.Status)
	}
	if node.AssignmentRevision == 0 {
		return fmt.Errorf("assignment_revision must be positive")
	}
	configurationSuffix := "/api/v1/nodes/" + node.NodeID + "/configuration"
	if node.ConfigurationResource != configurationSuffix {
		return fmt.Errorf("configuration_resource must equal %q", configurationSuffix)
	}
	userResource := "/api/v1/nodes/" + node.NodeID + "/inbounds/" + node.NodeID + "/users"
	if node.UserResource != userResource {
		return fmt.Errorf("user_resource must equal %q", userResource)
	}
	return nil
}
