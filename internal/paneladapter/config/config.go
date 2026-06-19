package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	PanelBaseURL        string      `json:"panel_base_url"`
	NodeID              string      `json:"node_id"`
	NodeToken           string      `json:"node_token"`
	StatePath           string      `json:"state_path"`
	GeneratedConfigPath string      `json:"generated_config_path,omitempty"`
	HTTPTimeout         Duration    `json:"http_timeout,omitempty"`
	PollIntervalBounds  *PollBounds `json:"poll_interval_bounds,omitempty"`
	LogLevel            string      `json:"log_level,omitempty"`
	Insecure            bool        `json:"insecure,omitempty"`
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

	// Required: NodeID
	if c.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}

	// Required: NodeToken
	if c.NodeToken == "" {
		return fmt.Errorf("node_token is required")
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
