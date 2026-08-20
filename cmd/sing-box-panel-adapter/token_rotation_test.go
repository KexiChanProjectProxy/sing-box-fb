package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/config"
	"github.com/sagernet/sing-box/log"
)

func TestRunTokenRotation_retriesPendingTokenWithoutRequestingAnother(t *testing.T) {
	var requests atomic.Int32
	rotationRequested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		rotationRequested <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(client.TokenRotationResponse{
			Token:              "replacement-token",
			ExpiresAt:          time.Now().UTC().Add(30 * 24 * time.Hour),
			RotateAfterSeconds: 86400,
		})
	}))
	defer server.Close()

	panelClient, err := client.New(server.URL, "node-1", "old-token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	dir := t.TempDir()
	configPath := filepath.Join(dir, "adapter.json")
	if err := os.Mkdir(configPath, 0o700); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runTokenRotationWithRetry(ctx, panelClient, configPath, 10*time.Millisecond, 200*time.Millisecond, log.NewNOPFactory().Logger())
	}()

	select {
	case <-rotationRequested:
	case <-time.After(time.Second):
		t.Fatal("rotation request was not sent")
	}

	time.Sleep(50 * time.Millisecond)
	if err := os.Remove(configPath); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "state.json")
	payload := `{
		"panel_base_url": "` + server.URL + `",
		"node_id": "node-1",
		"node_token": "old-token",
		"state_path": "` + statePath + `",
		"insecure": true
	}`
	if err := os.WriteFile(configPath, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		cfg, loadErr := config.Load(configPath)
		if loadErr == nil && cfg.NodeToken == "replacement-token" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement token was not persisted: %v", loadErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rotation goroutine did not stop")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("rotation requests = %d, want 1", got)
	}
}
