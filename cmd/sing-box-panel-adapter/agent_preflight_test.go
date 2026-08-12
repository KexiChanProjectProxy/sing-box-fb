package main

import (
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/config"
)

func TestAgentPreflightBasicHostChecks(t *testing.T) {
	if !probeSocketCapability() {
		t.Fatal("socket capability probe failed")
	}
	if !probeWritableDirectory(t.TempDir()) {
		t.Fatal("writable directory probe failed")
	}
	cfg := &config.Config{PanelBaseURL: "https://panel.example.test"}
	if !probePanelTLS(cfg) {
		t.Fatal("TLS panel probe failed")
	}
	cfg.Insecure = true
	if probePanelTLS(cfg) {
		t.Fatal("insecure panel must not pass TLS probe")
	}
	if !probeAdapterCapabilities() {
		t.Fatal("compiled adapter capabilities probe failed")
	}
	if probeWritableDirectory(filepath.Join(t.TempDir(), "missing")) {
		t.Fatal("missing directory unexpectedly passed permissions probe")
	}
}
