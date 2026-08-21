package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"
)

// Duration wraps time.Duration for JSON marshaling as a Go duration string
// (e.g. "30s", "5m"). Zero value encodes as "" with omitempty.
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	if d.Duration == 0 {
		return []byte(`""`), nil
	}
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == "" {
		d.Duration = 0
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// PollBounds defines the adaptive polling interval range.
type PollBounds struct {
	MinSeconds int `json:"min_seconds,omitempty"`
	MaxSeconds int `json:"max_seconds,omitempty"`
}

// Config is the local configuration for the panel adapter daemon.
// All fields use snake_case JSON tags — never V2bX-style names.
type Config struct {
	PanelBaseURL          string      `json:"panel_base_url"`
	AgentID               string      `json:"agent_id,omitempty"`
	AgentToken            string      `json:"agent_token,omitempty"`
	NodeID                string      `json:"node_id,omitempty"`
	NodeToken             string      `json:"node_token,omitempty"`
	TokenRotationInterval Duration    `json:"token_rotation_interval,omitempty"`
	StatePath             string      `json:"state_path"`
	GeneratedConfigPath   string      `json:"generated_config_path,omitempty"`
	HTTPTimeout           Duration    `json:"http_timeout,omitempty"`
	PollIntervalBounds    *PollBounds `json:"poll_interval_bounds,omitempty"`
	LogLevel              string      `json:"log_level,omitempty"`
	Insecure              bool        `json:"insecure,omitempty"`
}

func (c *Config) IsAgentMode() bool {
	return c.AgentID != "" && c.AgentToken != ""
}

func (c *Config) BearerToken() string {
	if c.IsAgentMode() {
		return c.AgentToken
	}
	return c.NodeToken
}

// Load reads and decodes a Config from the given file path.
// It also validates the config before returning it.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks the config for required fields and security constraints.
func (c *Config) Validate() error {
	// Required: PanelBaseURL
	if c.PanelBaseURL == "" {
		return fmt.Errorf("panel_base_url is required")
	}
	parsedURL, err := url.Parse(c.PanelBaseURL)
	if err != nil {
		return fmt.Errorf("panel_base_url is not a valid URL: %w", err)
	}
	if parsedURL.Scheme != "https" && !c.Insecure {
		return fmt.Errorf("panel_base_url must use https; set insecure=true only for development")
	}
	// Reject token leaked in query string
	if token := parsedURL.Query().Get("token"); token != "" {
		return fmt.Errorf("panel_base_url must not contain a token in query parameters")
	}
	for key := range parsedURL.Query() {
		if strings.EqualFold(key, "key") || strings.EqualFold(key, "apikey") || strings.EqualFold(key, "api_key") || strings.EqualFold(key, "secret") {
			return fmt.Errorf("panel_base_url must not contain a secret (%q) in query parameters", key)
		}
	}

	hasAgentID := c.AgentID != ""
	hasAgentToken := c.AgentToken != ""
	hasNodeID := c.NodeID != ""
	hasNodeToken := c.NodeToken != ""
	if hasAgentID != hasAgentToken {
		return fmt.Errorf("agent_id and agent_token must be configured together")
	}
	if hasNodeID != hasNodeToken {
		return fmt.Errorf("node_id and node_token must be configured together")
	}
	if hasAgentID && hasNodeID {
		return fmt.Errorf("agent and node credentials are mutually exclusive")
	}
	if !hasAgentID && !hasNodeID {
		return fmt.Errorf("agent_id/agent_token or node_id/node_token is required")
	}
	if hasAgentID {
		agentID, err := uuid.FromString(c.AgentID)
		if err != nil || agentID.Version() != uuid.V7 {
			return fmt.Errorf("agent_id must be a UUIDv7")
		}
	}
	if c.TokenRotationInterval.Duration < 0 {
		return fmt.Errorf("token_rotation_interval must not be negative")
	}

	// Required: StatePath — parent directory must be writable
	if c.StatePath == "" {
		return fmt.Errorf("state_path is required")
	}
	stateDir := filepath.Dir(c.StatePath)
	if info, err := os.Stat(stateDir); err != nil {
		return fmt.Errorf("state_path parent directory %q does not exist: %w", stateDir, err)
	} else if !info.IsDir() {
		return fmt.Errorf("state_path parent %q is not a directory", stateDir)
	}
	if err := os.WriteFile(filepath.Join(stateDir, ".paneladapter_write_check"), []byte{}, 0o600); err != nil {
		return fmt.Errorf("state_path parent directory %q is not writable: %w", stateDir, err)
	}
	_ = os.Remove(filepath.Join(stateDir, ".paneladapter_write_check"))

	// Optional: GeneratedConfigPath — must not embed secrets in filename
	if c.GeneratedConfigPath != "" {
		base := filepath.Base(c.GeneratedConfigPath)
		lower := strings.ToLower(base)
		secretPatterns := []string{"token", "secret", "apikey", "api_key", "key=", "password"}
		for _, pattern := range secretPatterns {
			if strings.Contains(lower, pattern) {
				return fmt.Errorf("generated_config_path filename must not contain secret-like pattern %q", pattern)
			}
		}
	}

	return nil
}

// UpdateNodeToken atomically replaces node_token while preserving the rest of
// the validated adapter configuration. The secret file is always mode 0600.
func UpdateNodeToken(path, token string) error {
	return updateToken(path, token, false)
}

// UpdateAgentToken atomically replaces agent_token while preserving the rest
// of the validated adapter configuration. The secret file is always mode 0600.
func UpdateAgentToken(path, token string) error {
	return updateToken(path, token, true)
}

func UpdateToken(path, token string) error {
	cfg, err := Load(path)
	if err != nil {
		return err
	}
	return updateToken(path, token, cfg.IsAgentMode())
}

func updateToken(path, token string, agentMode bool) error {
	if token == "" {
		return fmt.Errorf("adapter token is required")
	}
	cfg, err := Load(path)
	if err != nil {
		return err
	}
	if cfg.IsAgentMode() != agentMode {
		return fmt.Errorf("adapter credential mode does not match token update")
	}
	if agentMode {
		cfg.AgentToken = token
	} else {
		cfg.NodeToken = token
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".adapter-config-*")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("chmod temporary config: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write temporary config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temporary config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temporary config: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace adapter config: %w", err)
	}
	return nil
}
