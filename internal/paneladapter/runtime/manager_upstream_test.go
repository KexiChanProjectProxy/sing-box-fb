package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/sagernet/sing/common/json/badoption"
)

func TestApplyConfigLocked_preservesUpstreamTemplateFields(t *testing.T) {
	template := map[string]any{
		"inbounds": []any{
			map[string]any{
				"type":        "hysteria2",
				"tag":         "hy2-in",
				"listen":      "::",
				"listen_port": 8443,
				"users": []any{
					map[string]any{"name": "stale", "password": "drop-me"},
				},
				"realm": map[string]any{
					"server_url":        "https://realm.example",
					"realm_id":          "r1",
					"stun_servers":      []any{"stun.example:3478"},
					"prefer_ip_version": "v6",
					"fallback_timeout":  "2s",
					"listen_ports":      []any{"20000:20002"},
				},
			},
		},
		"outbounds": []any{
			map[string]any{
				"type":           "direct",
				"tag":            "direct",
				"non_local_bind": true,
				"source_bind": map[string]any{
					"inet4_addresses": []any{"203.0.113.10"},
					"ttl":             "30m",
				},
			},
			map[string]any{
				"type":              "loadbalance",
				"tag":               "lb",
				"primary_outbounds": []any{"direct"},
				"sorter": map[string]any{
					"latency":              1,
					"client_loss_rate_30s": 0.25,
				},
			},
		},
		"route": map[string]any{"final": "lb"},
	}
	templateBytes, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("marshal template: %v", err)
	}
	cfg := &contract.ConfigurationResponse{
		Revision:   "rev-upstream",
		APIVersion: "v1",
		NodeID:     "node-1",
		ManagedInbounds: []contract.ManagedInbound{
			{InboundID: "ib-1", Tag: "hy2-in", Protocol: "hysteria2"},
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
	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("create state store: %v", err)
	}
	recorder := &recordingFactory{createErr: errors.New("stop before creating real box")}
	m, err := NewManager(&fakeClient{}, store, traffic.NewTracker(map[string]string{}), log.NewNOPFactory(), WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := m.applyConfigLocked(context.Background(), cfg, "etag-upstream"); err == nil {
		t.Fatal("expected factory error after a successful decode")
	}
	if recorder.callCount != 1 {
		t.Fatalf("expected one factory call, got %d", recorder.callCount)
	}

	if len(recorder.lastOpts.Inbounds) != 1 {
		t.Fatalf("expected one inbound, got %d", len(recorder.lastOpts.Inbounds))
	}
	hy2, ok := recorder.lastOpts.Inbounds[0].Options.(*option.Hysteria2InboundOptions)
	if !ok {
		t.Fatalf("expected hysteria2 options, got %T", recorder.lastOpts.Inbounds[0].Options)
	}
	if len(hy2.Users) != 0 {
		t.Fatalf("managed users should be stripped, got %d", len(hy2.Users))
	}
	if hy2.Realm == nil {
		t.Fatal("expected hysteria2 realm to be preserved")
	}
	if hy2.Realm.PreferIPVersion != "v6" {
		t.Fatalf("prefer_ip_version = %q, want v6", hy2.Realm.PreferIPVersion)
	}
	if hy2.Realm.FallbackTimeout.Build() != 2*time.Second {
		t.Fatalf("fallback_timeout = %s, want 2s", hy2.Realm.FallbackTimeout.Build())
	}
	if len(hy2.Realm.ListenPorts) != 1 || hy2.Realm.ListenPorts[0] != "20000:20002" {
		t.Fatalf("listen_ports = %#v", hy2.Realm.ListenPorts)
	}

	if len(recorder.lastOpts.Outbounds) != 2 {
		t.Fatalf("expected two outbounds, got %d", len(recorder.lastOpts.Outbounds))
	}
	direct, ok := recorder.lastOpts.Outbounds[0].Options.(*option.DirectOutboundOptions)
	if !ok {
		t.Fatalf("expected direct options, got %T", recorder.lastOpts.Outbounds[0].Options)
	}
	if !direct.NonLocalBind {
		t.Fatal("expected non_local_bind to be preserved")
	}
	if direct.SourceBind == nil || len(direct.SourceBind.Inet4Addresses) != 1 {
		t.Fatalf("source_bind = %#v", direct.SourceBind)
	}
	if got := netip.Prefix(*direct.SourceBind.Inet4Addresses[0]); got != netip.MustParsePrefix("203.0.113.10/32") {
		t.Fatalf("source_bind address = %s", got)
	}
	if direct.SourceBind.TTL != badoption.Duration(30*time.Minute) {
		t.Fatalf("source_bind ttl = %s", time.Duration(direct.SourceBind.TTL))
	}

	lb, ok := recorder.lastOpts.Outbounds[1].Options.(*option.LoadBalanceOutboundOptions)
	if !ok {
		t.Fatalf("expected loadbalance options, got %T", recorder.lastOpts.Outbounds[1].Options)
	}
	if lb.Sorter["latency"] != 1 || lb.Sorter["client_loss_rate_30s"] != 0.25 {
		t.Fatalf("sorter = %#v", lb.Sorter)
	}
}
