package runtime

import (
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	E "github.com/sagernet/sing/common/exceptions"
)

func (manager *Manager) recordPendingRevision(configuration *contract.ConfigurationResponse, etag string) error {
	if err := manager.store.UpdateAndSave(func(current *state.State) {
		current.Config = state.ConfigState{Revision: configuration.Revision, ETag: etag, NodeID: configuration.NodeID}
		for _, inbound := range configuration.ManagedInbounds {
			inboundState := state.InboundState{InboundID: inbound.InboundID, Tag: inbound.Tag, Protocol: inbound.Protocol}
			if !contract.IsSupportedProtocol(inbound.Protocol) {
				inboundState.UserLoadStatus = string(contract.UserLoadStatusUnsupportedProto)
			}
			current.Inbounds[inbound.InboundID] = inboundState
		}
	}); err != nil {
		return E.Cause(err, "save pending revision state")
	}
	manager.logger.Info("recorded pending configuration revision=", configuration.Revision, " (manual strategy)")
	return nil
}

func (manager *Manager) updateStateApplied(configuration *contract.ConfigurationResponse, etag string, unsupported map[string]bool) {
	if err := manager.store.UpdateAndSave(func(current *state.State) {
		current.Config = state.ConfigState{Revision: configuration.Revision, ETag: etag, NodeID: configuration.NodeID}
		for _, inbound := range configuration.ManagedInbounds {
			inboundState := state.InboundState{InboundID: inbound.InboundID, Tag: inbound.Tag, Protocol: inbound.Protocol}
			if unsupported[inbound.InboundID] {
				inboundState.UserLoadStatus = string(contract.UserLoadStatusUnsupportedProto)
			} else {
				inboundState.UserLoadStatus = string(contract.UserLoadStatusEmptyInitialLoad)
			}
			current.Inbounds[inbound.InboundID] = inboundState
		}
	}); err != nil {
		manager.logger.Error("save state after apply: ", err)
	}
}

func (manager *Manager) setInboundStatesApplyFailed(configuration *contract.ConfigurationResponse) {
	if err := manager.store.UpdateAndSave(func(current *state.State) {
		current.Config = state.ConfigState{Revision: configuration.Revision, NodeID: configuration.NodeID}
		for _, inbound := range configuration.ManagedInbounds {
			inboundState := state.InboundState{InboundID: inbound.InboundID, Tag: inbound.Tag, Protocol: inbound.Protocol}
			if !contract.IsSupportedProtocol(inbound.Protocol) {
				inboundState.UserLoadStatus = string(contract.UserLoadStatusUnsupportedProto)
			} else {
				inboundState.UserLoadStatus = string(contract.UserLoadStatusApplyFailed)
			}
			current.Inbounds[inbound.InboundID] = inboundState
		}
	}); err != nil {
		manager.logger.Error("save apply_failed state: ", err)
	}
}
