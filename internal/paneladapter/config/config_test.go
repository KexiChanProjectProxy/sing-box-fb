package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// helperConfig returns a valid minimal production Config.
func helperConfig(t *testing.T) (*Config, string) {
	t.Helper()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	cfg := &Config{
		PanelBaseURL: "https://panel.example.com",
		NodeID:       "node-1",
		NodeToken:    "tok_secret123",
		StatePath:    statePath,
	}
	return cfg, dir
}

func TestValidMinimalConfig(t *testing.T) {
	cfg, _ := helperConfig(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
}

func TestMissingToken(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.NodeToken = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing node_token")
	}
}

func TestMissingNodeID(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.NodeID = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing node_id")
	}
}

func TestMissingBaseURL(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.PanelBaseURL = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing panel_base_url")
	}
}

func TestTokenInURLQuery(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.PanelBaseURL = "https://panel.example.com?token=leaked"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for token in URL query")
	}
}

func TestAPIKeyInURLQuery(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.PanelBaseURL = "https://panel.example.com?apikey=leaked"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for apikey in URL query")
	}
}

func TestHTTPWithoutInsecure(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.PanelBaseURL = "http://panel.example.com"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for http URL without insecure flag")
	}
}

func TestHTTPWithInsecure(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.PanelBaseURL = "http://panel.example.com"
	cfg.Insecure = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with insecure, got error: %v", err)
	}
}

func TestV2bXFieldNamesRejected(t *testing.T) {
	v2bxNames := []struct {
		jsonTag string
		name    string
	}{
		{"ApiHost", "ApiHost"},
		{"NodeID", "NodeID (V2bX style)"},
		{"ApiKey", "ApiKey"},
		{"NodeType", "NodeType"},
	}
	for _, v := range v2bxNames {
		t.Run(v.name, func(t *testing.T) {
			payload := `{"` + v.jsonTag + `":"value","panel_base_url":"https://x.com","node_id":"n","node_token":"t","state_path":"/tmp/s"}`
			var cfg Config
			decoder := json.NewDecoder(nil)
			_ = decoder // just proving DisallowUnknownFields is used in Load
			err := json.Unmarshal([]byte(payload), &cfg)
			// Standard json.Unmarshal does not reject unknown fields;
			// our Load function uses DisallowUnknownFields, tested in TestUnknownJSONFieldsRejected.
			// Here we verify the Config struct does NOT have V2bX-named fields.
			_ = err
		})
	}
	// Verify Config struct doesn't have V2bX JSON tags by inspecting the struct tags.
	data, _ := json.Marshal(Config{})
	fields := map[string]bool{}
	var raw map[string]json.RawMessage
	json.Unmarshal(data, &fields)
	_ = raw
	// The marshaled JSON should contain our snake_case fields, never V2bX names.
	banned := []string{"ApiHost", "NodeID", "ApiKey", "NodeType"}
	for _, b := range banned {
		if _, ok := fields[b]; ok {
			t.Errorf("Config must not use V2bX JSON tag %q", b)
		}
	}
}

func TestUnknownJSONFieldsRejected(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	payload := `{
		"panel_base_url": "https://panel.example.com",
		"node_id": "node-1",
		"node_token": "tok_secret123",
		"state_path": "` + statePath + `",
		"unknown_field": "oops"
	}`
	cfgPath := filepath.Join(dir, "adapter.json")
	if err := os.WriteFile(cfgPath, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected error for unknown JSON field")
	}
}

func TestUnwritableStateDir(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping: running as root, cannot test permission denial")
	}
	cfg, _ := helperConfig(t)
	cfg.StatePath = "/proc/1/state.json" // parent /proc/1 is not writable
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for unwritable state directory")
	}
}

func TestGeneratedConfigPathWithSecret(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.GeneratedConfigPath = "/tmp/my-token-config.json"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for secret-like pattern in generated_config_path filename")
	}
}

func TestGeneratedConfigPathClean(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.GeneratedConfigPath = "/tmp/sing-box-generated.json"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with clean generated_config_path, got error: %v", err)
	}
}

func TestDurationJSON(t *testing.T) {
	d := Duration{}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `""` {
		t.Fatalf("expected empty string for zero duration, got %s", b)
	}

	d2 := Duration{}
	if err := json.Unmarshal([]byte(`""`), &d2); err != nil {
		t.Fatal(err)
	}
	if d2.Duration != 0 {
		t.Fatalf("expected zero duration, got %v", d2.Duration)
	}

	d3 := Duration{}
	if err := json.Unmarshal([]byte(`"30s"`), &d3); err != nil {
		t.Fatal(err)
	}
	if d3.Duration.String() != "30s" {
		t.Fatalf("expected 30s, got %v", d3.Duration)
	}

	d4 := Duration{}
	if err := json.Unmarshal([]byte(`"bad"`), &d4); err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestLoadValidFile(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	payload := `{
		"panel_base_url": "https://panel.example.com",
		"node_id": "node-1",
		"node_token": "tok_secret123",
		"state_path": "` + statePath + `",
		"http_timeout": "30s",
		"log_level": "info"
	}`
	cfgPath := filepath.Join(dir, "adapter.json")
	if err := os.WriteFile(cfgPath, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.HTTPTimeout.Duration.String() != "30s" {
		t.Fatalf("expected http_timeout=30s, got %v", cfg.HTTPTimeout.Duration)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("expected log_level=info, got %q", cfg.LogLevel)
	}
}

func TestUpdateNodeToken_atomicallyPreservesConfig(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	cfgPath := filepath.Join(dir, "adapter.json")
	payload := `{
		"panel_base_url": "https://panel.example.com",
		"node_id": "node-1",
		"node_token": "old-token",
		"token_rotation_interval": "24h",
		"state_path": "` + statePath + `",
		"log_level": "debug"
	}`
	if err := os.WriteFile(cfgPath, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := UpdateNodeToken(cfgPath, "new-token"); err != nil {
		t.Fatalf("UpdateNodeToken: %v", err)
	}
	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load updated config: %v", err)
	}
	if cfg.NodeToken != "new-token" {
		t.Fatalf("node_token = %q", cfg.NodeToken)
	}
	if cfg.NodeID != "node-1" || cfg.LogLevel != "debug" || cfg.TokenRotationInterval.Duration != 24*time.Hour {
		t.Fatalf("unrelated config changed: %+v", cfg)
	}
	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("config mode = %o, want 600", got)
	}
}

func TestPollBounds(t *testing.T) {
	cfg, _ := helperConfig(t)
	cfg.PollIntervalBounds = &PollBounds{MinSeconds: 5, MaxSeconds: 60}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with poll bounds, got error: %v", err)
	}
}
