package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/config"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

func collectAgentPreflight(ctx context.Context, cfg *config.Config) *contract.AgentPreflightReport {
	report := &contract.AgentPreflightReport{}
	report.Ports = probeSocketCapability()
	report.System = runtime.GOOS == "linux" && (runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64")
	report.TimeSync = probeTimeSynchronization(ctx)
	report.Certificate = probePanelTLS(cfg)
	// A report is only accepted when this heartbeat reaches the panel. Its
	// presence in the control plane is therefore outbound-network evidence.
	report.OutboundNetwork = true
	report.Permissions = probeWritableDirectory(filepath.Dir(cfg.StatePath)) &&
		(cfg.GeneratedConfigPath == "" || probeWritableDirectory(filepath.Dir(cfg.GeneratedConfigPath)))
	report.Capabilities = probeAdapterCapabilities()

	checks := []struct {
		name string
		ok   bool
	}{
		{"ports", report.Ports},
		{"system", report.System},
		{"time_sync", report.TimeSync},
		{"certificate", report.Certificate},
		{"outbound_network", report.OutboundNetwork},
		{"permissions", report.Permissions},
		{"capabilities", report.Capabilities},
	}
	for _, check := range checks {
		if !check.ok {
			report.Errors = append(report.Errors, fmt.Sprintf("%s check failed", check.name))
		}
	}
	return report
}

func probeSocketCapability() bool {
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return false
	}
	_ = tcpListener.Close()
	udpListener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return false
	}
	_ = udpListener.Close()
	return true
}

func probeTimeSynchronization(ctx context.Context) bool {
	checkContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(
		checkContext, "timedatectl", "show", "--property=NTPSynchronized", "--value",
	).Output()
	return err == nil && strings.EqualFold(strings.TrimSpace(string(output)), "yes")
}

func probePanelTLS(cfg *config.Config) bool {
	panelURL, err := url.Parse(cfg.PanelBaseURL)
	return err == nil && panelURL.Scheme == "https" && !cfg.Insecure
}

func probeWritableDirectory(directory string) bool {
	file, err := os.CreateTemp(directory, ".panel-adapter-preflight-*")
	if err != nil {
		return false
	}
	name := file.Name()
	if closeErr := file.Close(); closeErr != nil {
		_ = os.Remove(name)
		return false
	}
	return os.Remove(name) == nil
}

func probeAdapterCapabilities() bool {
	capabilities := contract.EnabledUserRoutingCapabilities()
	return capabilities != nil &&
		capabilities.UserRouting != nil &&
		capabilities.UserRouting.Supported &&
		len(capabilities.UserRouting.SupportedProtocols) > 0
}
