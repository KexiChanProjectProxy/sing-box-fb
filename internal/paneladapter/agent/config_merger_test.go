package agent

import (
	"encoding/json"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	E "github.com/sagernet/sing/common/exceptions"
)

// TestConfigMerger_DuplicateTag tests that duplicate tags across nodes are rejected.
func TestConfigMerger_DuplicateTag(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)
	nodeStates := map[string]*NodeConfigState{
		"node-1": {Config: nodeConfigWithInbound(t, "node-1", "trojan", "shared-tag")},
		"node-2": {Config: nodeConfigWithInbound(t, "node-2", "vmess", "shared-tag")},
	}

	// When
	result, err := merger.Merge(MergeContext{}, nodeStates)

	// Then
	if err == nil {
		t.Fatal("Merge() error = nil, want duplicate tag error")
	}
	if result != nil {
		t.Fatal("Merge() returned non-nil result on error")
	}

	var expectedErr *string
	if err != nil {
		errStr := err.Error()
		if !contains(errStr, "duplicate tag") {
			t.Fatalf("error = %q, want duplicate tag error", errStr)
		}
		expectedErr = &errStr
	}
	if expectedErr == nil {
		t.Fatal("expected duplicate tag error, got nil")
	}
}

// TestConfigMerger_InboundIDMismatch tests that inbound_id must equal node_id.
func TestConfigMerger_InboundIDMismatch(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)
	badConfig := nodeConfigWithInbound(t, "different-node", "trojan", "unique-tag")
	nodeStates := map[string]*NodeConfigState{
		"node-1": {Config: badConfig},
	}

	// When
	result, err := merger.Merge(MergeContext{}, nodeStates)

	// Then
	if err == nil {
		t.Fatal("Merge() error = nil, want inbound_id mismatch error")
	}
	if result != nil {
		t.Fatal("Merge() returned non-nil result on error")
	}

	var expectedErr *string
	if err != nil {
		errStr := err.Error()
		if !contains(errStr, "inbound_id") || !contains(errStr, "does not match") {
			t.Fatalf("error = %q, want inbound_id mismatch error", errStr)
		}
		expectedErr = &errStr
	}
	if expectedErr == nil {
		t.Fatal("expected inbound_id mismatch error, got nil")
	}
}

// TestConfigMerger_EmptyTag tests that empty tags are rejected.
func TestConfigMerger_EmptyTag(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)
	emptyTagConfig := json.RawMessage(`{
		"inbounds": [{
			"type": "trojan",
			"tag": "",
			"inbound_id": "node-1"
		}]
	}`)
	nodeStates := map[string]*NodeConfigState{
		"node-1": {Config: emptyTagConfig},
	}

	// When
	result, err := merger.Merge(MergeContext{}, nodeStates)

	// Then
	if err == nil {
		t.Fatal("Merge() error = nil, want empty tag error")
	}
	if result != nil {
		t.Fatal("Merge() returned non-nil result on error")
	}

	var expectedErr *string
	if err != nil {
		errStr := err.Error()
		if !contains(errStr, "tag") && !contains(errStr, "empty") {
			t.Fatalf("error = %q, want empty tag error", errStr)
		}
		expectedErr = &errStr
	}
	if expectedErr == nil {
		t.Fatal("expected empty tag error, got nil")
	}
}

// TestConfigMerger_MalformedConfig tests that malformed JSON is rejected.
func TestConfigMalformedConfig(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)
	malformedConfig := json.RawMessage(`{invalid json}`)
	nodeStates := map[string]*NodeConfigState{
		"node-1": {Config: malformedConfig},
	}

	// When
	result, err := merger.Merge(MergeContext{}, nodeStates)

	// Then
	if err == nil {
		t.Fatal("Merge() error = nil, want parse error")
	}
	if result != nil {
		t.Fatal("Merge() returned non-nil result on error")
	}

	var expectedErr *string
	if err != nil {
		errStr := err.Error()
		if !contains(errStr, "parse") && !contains(errStr, "invalid") && !contains(errStr, "json") {
			t.Fatalf("error = %q, want parse error", errStr)
		}
		expectedErr = &errStr
	}
	if expectedErr == nil {
		t.Fatal("expected parse error, got nil")
	}
}

// TestConfigMerger_NoNodes tests that empty node map is rejected.
func TestConfigMerger_NoNodes(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)

	// When
	result, err := merger.Merge(MergeContext{}, map[string]*NodeConfigState{})

	// Then
	if err == nil {
		t.Fatal("Merge() error = nil, want no nodes error")
	}
	if result != nil {
		t.Fatal("Merge() returned non-nil result on error")
	}

	var expectedErr *string
	if err != nil {
		errStr := err.Error()
		if !contains(errStr, "no node") {
			t.Fatalf("error = %q, want no nodes error", errStr)
		}
		expectedErr = &errStr
	}
	if expectedErr == nil {
		t.Fatal("expected no nodes error, got nil")
	}
}

// TestConfigMerger_MissingInboundTag tests that inbounds without tags are rejected.
func TestConfigMerger_MissingInboundTag(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)
	noTagConfig := json.RawMessage(`{
		"inbounds": [{
			"type": "trojan",
			"inbound_id": "node-1"
		}]
	}`)
	nodeStates := map[string]*NodeConfigState{
		"node-1": {Config: noTagConfig},
	}

	// When
	result, err := merger.Merge(MergeContext{}, nodeStates)

	// Then
	if err == nil {
		t.Fatal("Merge() error = nil, want missing tag error")
	}
	if result != nil {
		t.Fatal("Merge() returned non-nil result on error")
	}

	var expectedErr *string
	if err != nil {
		errStr := err.Error()
		if !contains(errStr, "tag") {
			t.Fatalf("error = %q, want missing tag error", errStr)
		}
		expectedErr = &errStr
	}
	if expectedErr == nil {
		t.Fatal("expected missing tag error, got nil")
	}
}

// TestConfigMerger_HappyPath tests successful merge of valid configs.
func TestConfigMerger_HappyPath(t *testing.T) {
	// Given
	merger := NewConfigMerger(nil)
	nodeStates := map[string]*NodeConfigState{
		"node-1": {Config: nodeConfigWithInbound(t, "node-1", "trojan", "tag-1")},
		"node-2": {Config: nodeConfigWithInbound(t, "node-2", "vmess", "tag-2")},
		"node-3": {Config: nodeConfigWithInbound(t, "node-3", "shadowsocks", "tag-3")},
	}

	// When
	result, err := merger.Merge(MergeContext{}, nodeStates)

	// Then
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if result == nil {
		t.Fatal("Merge() result = nil")
	}

	// Verify node IDs are sorted
	expectedNodeIDs := []string{"node-1", "node-2", "node-3"}
	if len(result.NodeIDs) != len(expectedNodeIDs) {
		t.Fatalf("NodeIDs length = %d, want %d", len(result.NodeIDs), len(expectedNodeIDs))
	}
	for i, nodeID := range result.NodeIDs {
		if nodeID != expectedNodeIDs[i] {
			t.Fatalf("NodeIDs[%d] = %s, want %s", i, nodeID, expectedNodeIDs[i])
		}
	}

	// Verify tag mapping
	expectedTags := map[string]string{"tag-1": "node-1", "tag-2": "node-2", "tag-3": "node-3"}
	if len(result.InboundTags) != len(expectedTags) {
		t.Fatalf("InboundTags length = %d, want %d", len(result.InboundTags), len(expectedTags))
	}
	for tag, nodeID := range result.InboundTags {
		if expectedTags[tag] != nodeID {
			t.Fatalf("InboundTags[%s] = %s, want %s", tag, nodeID, expectedTags[tag])
		}
	}

	// Verify merged config has inbounds array
	var mergedConfig map[string]json.RawMessage
	if err := json.Unmarshal(result.Raw, &mergedConfig); err != nil {
		t.Fatalf("parse merged config: %v", err)
	}
	inboundsRaw, exists := mergedConfig["inbounds"]
	if !exists {
		t.Fatal("merged config missing inbounds")
	}
	var inbounds []json.RawMessage
	if err := json.Unmarshal(inboundsRaw, &inbounds); err != nil {
		t.Fatalf("parse inbounds: %v", err)
	}
	if len(inbounds) != 3 {
		t.Fatalf("inbounds count = %d, want 3", len(inbounds))
	}
}

// Helper functions

func nodeConfigWithInbound(t *testing.T, nodeID, inboundType, tag string) json.RawMessage {
	config := map[string]interface{}{
		"inbounds": []map[string]interface{}{
			{
				"type":       inboundType,
				"tag":        tag,
				"inbound_id": nodeID,
			},
		},
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal node config: %v", err)
	}
	return raw
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
