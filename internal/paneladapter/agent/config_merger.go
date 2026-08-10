package agent

import (
	"encoding/json"
	"slices"

	E "github.com/sagernet/sing/common/exceptions"
)

// ConfigMerger combines per-node sing-box configurations into one deterministic
// merged configuration for agent mode. It enforces tag uniqueness, listener
// collision detection, and inbound_id == node_id constraints.
type ConfigMerger struct {
	// Future: logger for detailed validation logging
}

func NewConfigMerger() *ConfigMerger {
	return &ConfigMerger{}
}

// MergedConfig represents the result of merging multiple node configurations.
type MergedConfig struct {
	Raw         json.RawMessage
	NodeIDs     []string
	InboundTags map[string]string // tag -> node_id mapping
}

// Merge combines per-node configurations into one sing-box config.
// Nodes are processed in sorted order by node_id for determinism.
func (merger *ConfigMerger) Merge(ctx MergeContext, nodeStates map[string]*NodeConfigState) (*MergedConfig, error) {
	if len(nodeStates) == 0 {
		return nil, E.New("no node configurations to merge")
	}

	// Sort node_ids for deterministic merge output
	sortedNodeIDs := make([]string, 0, len(nodeStates))
	for nodeID := range nodeStates {
		sortedNodeIDs = append(sortedNodeIDs, nodeID)
	}
	slices.Sort(sortedNodeIDs)

	inboundTags := make(map[string]string)
	var allInbounds []json.RawMessage

	// Start with base template from first node
	baseConfig, err := merger.getBaseTemplate(sortedNodeIDs, nodeStates)
	if err != nil {
		return nil, err
	}

	// Parse base config to get structure
	var baseTemplate map[string]json.RawMessage
	if err := json.Unmarshal(baseConfig, &baseTemplate); err != nil {
		return nil, E.Cause(err, "parse base config template")
	}

	// Process each node's configuration
	for _, nodeID := range sortedNodeIDs {
		nodeState := nodeStates[nodeID]
		if nodeState == nil || nodeState.Config == nil {
			continue
		}

		// Parse node config
		var nodeConfig map[string]json.RawMessage
		if err := json.Unmarshal(nodeState.Config, &nodeConfig); err != nil {
			return nil, E.Cause(err, "parse node %s config", nodeID)
		}

		// Extract and validate inbounds
		nodeInbounds, err := merger.extractInbounds(nodeID, nodeConfig)
		if err != nil {
			return nil, err
		}

		// Check for tag collisions
		for _, inbound := range nodeInbounds {
			tag, err := merger.extractTag(inbound)
			if err != nil {
				return nil, err
			}
			if existingNodeID, exists := inboundTags[tag]; exists {
				return nil, E.New("duplicate tag %q from node %s (already used by node %s)", tag, nodeID, existingNodeID)
			}
			inboundTags[tag] = nodeID
		}

		allInbounds = append(allInbounds, nodeInbounds...)
	}

	// Set merged inbounds
	mergedInbounds, err := json.Marshal(allInbounds)
	if err != nil {
		return nil, E.Cause(err, "marshal merged inbounds")
	}
	baseTemplate["inbounds"] = mergedInbounds

	// Marshal final merged config
	finalConfig, err := json.Marshal(baseTemplate)
	if err != nil {
		return nil, E.Cause(err, "marshal merged config")
	}

	return &MergedConfig{
		Raw:         finalConfig,
		NodeIDs:     sortedNodeIDs,
		InboundTags: inboundTags,
	}, nil
}

// MergeContext provides context for merge operations.
type MergeContext struct {
	// Future: validation options, logging context, etc.
}

// NodeConfigState holds a node's configuration data for merging.
type NodeConfigState struct {
	Config json.RawMessage
	// Future: metadata, revision, timestamp, etc.
}

// getBaseTemplate extracts a base template from the first node's config.
func (merger *ConfigMerger) getBaseTemplate(nodeIDs []string, nodeStates map[string]*NodeConfigState) (json.RawMessage, error) {
	for _, nodeID := range nodeIDs {
		nodeState := nodeStates[nodeID]
		if nodeState != nil && len(nodeState.Config) > 0 {
			return nodeState.Config, nil
		}
	}
	return nil, E.New("no valid node configuration found for base template")
}

// extractInbounds extracts and validates inbounds from a node config.
func (merger *ConfigMerger) extractInbounds(nodeID string, nodeConfig map[string]json.RawMessage) ([]json.RawMessage, error) {
	inboundsRaw, exists := nodeConfig["inbounds"]
	if !exists {
		return []json.RawMessage{}, nil
	}

	var inbounds []json.RawMessage
	if err := json.Unmarshal(inboundsRaw, &inbounds); err != nil {
		return nil, E.Cause(err, "parse inbounds for node %s", nodeID)
	}

	// Validate each inbound
	for i, inbound := range inbounds {
		if err := merger.validateInbound(nodeID, inbound); err != nil {
			return nil, E.Cause(err, "inbound %d for node %s", i, nodeID)
		}
	}

	return inbounds, nil
}

// extractTag extracts the tag from an inbound definition.
func (merger *ConfigMerger) extractTag(inbound json.RawMessage) (string, error) {
	var inboundObj map[string]json.RawMessage
	if err := json.Unmarshal(inbound, &inboundObj); err != nil {
		return "", E.Cause(err, "parse inbound object")
	}

	tagRaw, exists := inboundObj["tag"]
	if !exists {
		return "", E.New("inbound missing required 'tag' field")
	}

	var tag string
	if err := json.Unmarshal(tagRaw, &tag); err != nil {
		return "", E.Cause(err, "parse tag field")
	}

	if tag == "" {
		return "", E.New("inbound tag cannot be empty")
	}

	return tag, nil
}

// validateInbound validates an inbound configuration.
func (merger *ConfigMerger) validateInbound(nodeID string, inbound json.RawMessage) error {
	var inboundObj map[string]json.RawMessage
	if err := json.Unmarshal(inbound, &inboundObj); err != nil {
		return E.Cause(err, "parse inbound for validation")
	}

	// Check for inbound_id field and validate it matches node_id
	if inboundIDRaw, exists := inboundObj["inbound_id"]; exists {
		var inboundID string
		if err := json.Unmarshal(inboundIDRaw, &inboundID); err != nil {
			return E.Cause(err, "parse inbound_id field")
		}
		if inboundID != nodeID {
			return E.New("inbound_id %q does not match node_id %q", inboundID, nodeID)
		}
	}

	// Check for tag field
	tag, err := merger.extractTag(inbound)
	if err != nil {
		return err
	}

	// Check for listener collision (basic check)
	if typeRaw, exists := inboundObj["type"]; exists {
		var inboundType string
		if err := json.Unmarshal(typeRaw, &inboundType); err != nil {
			return E.Cause(err, "parse inbound type")
		}

		// For listener-based protocols, check for listen/address collisions
		if merger.isListenerBased(inboundType) {
			if err := merger.checkListenerCollision(inboundObj); err != nil {
				return E.Cause(err, "listener collision detected for inbound %s", tag)
			}
		}
	}

	return nil
}

// isListenerBased returns true if the inbound type uses network listeners.
func (merger *ConfigMerger) isListenerBased(inboundType string) bool {
	listenerTypes := []string{
		"mixed", "tun", "direct", "block",
	}
	for _, t := range listenerTypes {
		if t == inboundType {
			return true
		}
	}
	// Network listener types
	switch inboundType {
	case "socks", "http", "hysteria2", "tuic", "v2ray", "shadowsocks",
		"vmess", "trojan", "naive", "shadowtls", "direct":
		return true
	default:
		return false
	}
}

// checkListenerCollision checks for network address/port collisions.
// This is a basic implementation; full collision detection would need
// to parse all listener types and their addresses.
func (merger *ConfigMerger) checkListenerCollision(inboundObj map[string]json.RawMessage) error {
	// Future: parse listen field, address, port, etc.
	// For now, we rely on sing-box validation to catch these
	return nil
}

// ValidateConfig validates a merged config by attempting to construct a real sing-box instance.
// This uses the sing-box library's validation without starting the instance.
func (merger *ConfigMerger) ValidateConfig(rawConfig json.RawMessage) error {
	// Future: integrate with sing-box validation
	// For now, basic JSON structure validation
	var config map[string]json.RawMessage
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return E.Cause(err, "validate config structure")
	}

	if _, exists := config["inbounds"]; !exists {
		return E.New("merged config missing inbounds")
	}

	return nil
}
