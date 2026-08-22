package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestAccessLogNodeID_UsesHostname(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		t.Skip("hostname unavailable")
	}
	if got := accessLogNodeID("fallback-node"); got != host {
		t.Fatalf("got %q, want hostname %q", got, host)
	}
}

func TestInjectClickHouseService_NilIsNoop(t *testing.T) {
	template := json.RawMessage(`{"outbounds":[{"type":"direct","tag":"direct"}]}`)
	got, err := injectClickHouseService(template, nil, "gw-01")
	if err != nil {
		t.Fatalf("inject: %v", err)
	}
	if string(got) != string(template) {
		t.Fatalf("expected original template, got %s", got)
	}
}

func TestInjectClickHouseService_AddsServiceWithNodeTag(t *testing.T) {
	template := json.RawMessage(`{"outbounds":[{"type":"direct","tag":"direct"}]}`)
	ch := &contract.ClickHouseConfig{
		Server:     "ch.example.com",
		ServerPort: 9000,
		Database:   "logs",
		Username:   "writer",
		Password:   "secret",
		Protocol:   "native",
		TLS:        &contract.ClickHouseTLS{Enabled: true, Insecure: true},
	}
	got, err := injectClickHouseService(template, ch, "gw-01")
	if err != nil {
		t.Fatalf("inject: %v", err)
	}

	var tmpl map[string]json.RawMessage
	if err := json.Unmarshal(got, &tmpl); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	var services []map[string]any
	if err := json.Unmarshal(tmpl["services"], &services); err != nil {
		t.Fatalf("unmarshal services: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(services))
	}
	svc := services[0]
	if svc["type"] != "clickhouse" {
		t.Errorf("type: got %v", svc["type"])
	}
	if svc["tag"] != "gw-01" {
		t.Errorf("tag: got %v, want hostname node id", svc["tag"])
	}
	if svc["server"] != "ch.example.com" {
		t.Errorf("server: got %v", svc["server"])
	}
	if svc["table"] != defaultClickHouseTable {
		t.Errorf("table default: got %v, want %s", svc["table"], defaultClickHouseTable)
	}
	if svc["username"] != "writer" || svc["password"] != "secret" {
		t.Errorf("credentials: username=%v password set=%v", svc["username"], svc["password"] != nil)
	}
	tls, _ := svc["tls"].(map[string]any)
	if tls["enabled"] != true || tls["insecure"] != true {
		t.Errorf("tls: %+v", tls)
	}
}

func TestInjectClickHouseService_ReplacesExistingClickHouse(t *testing.T) {
	template := json.RawMessage(`{
		"services": [
			{"type":"ssm-api","tag":"ssm"},
			{"type":"clickhouse","tag":"old","server":"old.example","table":"old"}
		]
	}`)
	ch := &contract.ClickHouseConfig{Server: "new.example", Table: "sessions"}
	got, err := injectClickHouseService(template, ch, "gw-01")
	if err != nil {
		t.Fatalf("inject: %v", err)
	}
	var tmpl map[string]json.RawMessage
	if err := json.Unmarshal(got, &tmpl); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var services []map[string]any
	if err := json.Unmarshal(tmpl["services"], &services); err != nil {
		t.Fatalf("unmarshal services: %v", err)
	}
	if len(services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(services))
	}
	if services[0]["type"] != "ssm-api" {
		t.Errorf("kept non-clickhouse service: %+v", services[0])
	}
	if services[1]["type"] != "clickhouse" || services[1]["tag"] != "gw-01" || services[1]["server"] != "new.example" {
		t.Errorf("replaced clickhouse: %+v", services[1])
	}
}

func TestPollConfiguration_InjectsClickHouseIntoOptions(t *testing.T) {
	cfg := validTestConfig("rev-ch", contract.ApplyOnConfigRecreateInstance, nil)
	cfg.ClickHouse = &contract.ClickHouseConfig{
		Server:   "ch.example.com",
		Username: "writer",
		Password: "secret",
		Table:    "sessions",
	}
	fetcher := func(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
		return cfg, "etag-ch", nil
	}

	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	store.SetState(&state.State{
		Version:  state.CurrentVersion,
		Inbounds: map[string]state.InboundState{},
		Config:   state.ConfigState{ETag: "etag-1", Revision: "rev-1", NodeID: "node-1"},
	})
	if err := store.SaveIfChanged(); err != nil {
		t.Fatalf("save: %v", err)
	}

	recorder := &recordingFactory{createErr: errors.New("test: no real box")}
	m, err := NewManager(&fakeClient{fetcher: fetcher}, store, traffic.NewTracker(map[string]string{}), log.NewNOPFactory(), WithBoxFactory(recorder))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := m.PollConfiguration(context.Background()); err == nil {
		t.Fatal("expected factory error")
	}
	if recorder.callCount != 1 {
		t.Fatalf("expected 1 factory call, got %d", recorder.callCount)
	}
	if len(recorder.lastOpts.Services) != 1 {
		t.Fatalf("expected 1 injected service, got %d", len(recorder.lastOpts.Services))
	}
	svc := recorder.lastOpts.Services[0]
	if svc.Type != "clickhouse" {
		t.Errorf("service type: got %q", svc.Type)
	}
	host := accessLogNodeID(cfg.NodeID)
	if svc.Tag != host {
		t.Errorf("service tag: got %q, want hostname %q", svc.Tag, host)
	}
	opts, ok := svc.Options.(*option.ClickHouseServiceOptions)
	if !ok {
		t.Fatalf("options type %T", svc.Options)
	}
	if opts.Server != "ch.example.com" || opts.Username != "writer" || opts.Password != "secret" || opts.Table != "sessions" {
		t.Errorf("clickhouse options: %+v", opts)
	}
}
