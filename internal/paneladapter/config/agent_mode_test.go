package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAgentMode_ConfigRequiresAgentCredentials(t *testing.T) {
	// Given
	dir := t.TempDir()
	cfg := &Config{
		PanelBaseURL: "https://panel.example.com",
		AgentID:      "0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b1",
		AgentToken:   "at_test_secret",
		StatePath:    filepath.Join(dir, "state.json"),
	}

	// When
	err := cfg.Validate()

	// Then
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !cfg.IsAgentMode() {
		t.Fatal("IsAgentMode() = false, want true")
	}
}

func TestAgentMode_ConfigRejectsMixedCredentials(t *testing.T) {
	// Given
	cfg, _ := helperConfig(t)
	cfg.AgentID = "0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b1"
	cfg.AgentToken = "at_test_secret"

	// When
	err := cfg.Validate()

	// Then
	if err == nil {
		t.Fatal("Validate() error = nil, want mixed credential rejection")
	}
}

func TestAgentMode_TokenRotation(t *testing.T) {
	// Given
	dir := t.TempDir()
	path := filepath.Join(dir, "adapter.json")
	payload := `{
		"panel_base_url":"https://panel.example.com",
		"agent_id":"0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b1",
		"agent_token":"at_old_secret",
		"state_path":"` + filepath.Join(dir, "state.json") + `"
	}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}

	// When
	err := UpdateAgentToken(path, "at_replacement_secret")

	// Then
	if err != nil {
		t.Fatalf("UpdateAgentToken() error = %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.AgentToken != "at_replacement_secret" {
		t.Fatalf("agent_token = %q", loaded.AgentToken)
	}
	if loaded.NodeToken != "" {
		t.Fatalf("node_token = %q, want empty", loaded.NodeToken)
	}
}
