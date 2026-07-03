package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestApplyConfigLocked_preservesShadowsocks2022Method_whenTemplateDecoded(t *testing.T) {
	// Given
	template := map[string]interface{}{
		"log": map[string]interface{}{"level": "info"},
		"inbounds": []interface{}{
			map[string]interface{}{
				"type":        "shadowsocks",
				"tag":         "ss-2022-in",
				"listen":      "0.0.0.0",
				"listen_port": 8388,
				"method":      "2022-blake3-aes-128-gcm",
				"password":    "test-password",
			},
		},
		"outbounds": []interface{}{
			map[string]interface{}{"type": "direct", "tag": "direct"},
		},
		"route": map[string]interface{}{"final": "direct"},
	}
	templateBytes, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("marshal template: %v", err)
	}
	cfg := &contract.ConfigurationResponse{
		Revision:   "rev-ss-2022",
		APIVersion: "v1",
		NodeID:     "node-1",
		ManagedInbounds: []contract.ManagedInbound{
			{InboundID: "ib-1", Tag: "ss-2022-in", Protocol: "shadowsocks"},
		},
		SingBoxConfigTemplate: templateBytes,
		ApplyStrategy: contract.ApplyStrategy{
			OnConfigurationChange: contract.ApplyOnConfigRecreateInstance,
			OnUserChange:          contract.ApplyOnUserHotReloadUsers,
		},
		PollIntervals: contract.PollIntervals{
			ConfigurationSeconds: 60,
			UsersSeconds:         30,
			TrafficSeconds:       60,
			HeartbeatSeconds:     30,
		},
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, err := state.NewStore(statePath)
	if err != nil {
		t.Fatalf("create state store: %v", err)
	}
	recorder := &recordingFactory{createErr: errors.New("stop before creating real box")}
	m, err := NewManager(&fakeClient{}, store, traffic.NewTracker(map[string]string{}), log.NewNOPFactory(), WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// When
	_ = m.applyConfigLocked(context.Background(), cfg, "etag-ss-2022")

	// Then
	if recorder.callCount != 1 {
		t.Fatalf("expected one factory call, got %d", recorder.callCount)
	}
	if len(recorder.lastOpts.Inbounds) != 1 {
		t.Fatalf("expected one inbound, got %d", len(recorder.lastOpts.Inbounds))
	}
	shadowsocksOptions, ok := recorder.lastOpts.Inbounds[0].Options.(*option.ShadowsocksInboundOptions)
	if !ok {
		t.Fatalf("expected shadowsocks options, got %T", recorder.lastOpts.Inbounds[0].Options)
	}
	if shadowsocksOptions.Method != "2022-blake3-aes-128-gcm" {
		t.Fatalf("expected method to be preserved, got %q", shadowsocksOptions.Method)
	}
	if shadowsocksOptions.Managed {
		t.Fatal("single-user shadowsocks inbound must not be managed")
	}
	if shadowsocksOptions.Password != "test-password" {
		t.Fatal("expected single-user password to be preserved")
	}
}
