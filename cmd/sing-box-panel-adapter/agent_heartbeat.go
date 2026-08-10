package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/log"
)

const agentHeartbeatInterval = 30 * time.Second

func runAgentHeartbeat(ctx context.Context, panelClient *client.Client, store *state.Store, agentID string, logger log.ContextLogger) {
	send := func() {
		if err := panelClient.SendAgentHeartbeat(ctx, buildAgentHeartbeat(store.State(), agentID, time.Now().UTC())); err != nil && ctx.Err() == nil {
			logger.Warn("agent heartbeat: ", err)
		}
	}
	send()
	ticker := time.NewTicker(agentHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func buildAgentHeartbeat(current *state.State, agentID string, observedAt time.Time) *contract.AgentHeartbeat {
	nodes := make([]contract.AgentHeartbeatNode, 0, len(current.Nodes))
	for nodeID, node := range current.Nodes {
		statuses := make([]contract.HeartbeatInbound, 0, len(node.Inbounds))
		for _, inbound := range node.Inbounds {
			status := contract.UserLoadStatus(inbound.UserLoadStatus)
			if status == "" {
				status = contract.UserLoadStatusEmptyInitialLoad
			}
			if !contract.IsSupportedProtocol(inbound.Protocol) {
				status = contract.UserLoadStatusUnsupportedProto
			}
			statuses = append(statuses, contract.HeartbeatInbound{
				Tag: nodeID, Protocol: inbound.Protocol, Status: status, CurrentUserCount: inbound.UserCount,
			})
		}
		nodes = append(nodes, contract.AgentHeartbeatNode{NodeID: nodeID, InboundStatuses: statuses})
	}
	fingerprint := sha256.Sum256([]byte("nextsub-agent:" + agentID))
	return &contract.AgentHeartbeat{
		ObservedAt:              observedAt,
		AdapterVersion:          contract.UserRoutingAdapterVersion,
		AdapterFingerprint:      fmt.Sprintf("%x", fingerprint),
		SingBoxVersion:          semanticSingBoxVersion(C.Version),
		AppliedManifestRevision: current.Manifest.Snapshot.ManifestRevision,
		Capabilities:            contract.EnabledUserRoutingCapabilities(),
		Nodes:                   nodes,
	}
}

func semanticSingBoxVersion(version string) string {
	if version == "" || version == "unknown" {
		return ""
	}
	return version
}
