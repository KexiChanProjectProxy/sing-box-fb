package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

const testAgentID = "0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b1"

func TestAgentMode_Manifest304(t *testing.T) {
	// Given
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/"+testAgentID+"/manifest" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Fatalf("Authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Agent-ID") != testAgentID {
			t.Fatalf("X-Agent-ID = %q", r.Header.Get("X-Agent-ID"))
		}
		if r.Header.Get("If-None-Match") != `"manifest-7"` {
			t.Fatalf("If-None-Match = %q", r.Header.Get("If-None-Match"))
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	panelClient, err := NewAgent(server.URL, testAgentID, testToken)
	if err != nil {
		t.Fatal(err)
	}

	// When
	manifest, etag, err := panelClient.FetchManifest(context.Background(), `"manifest-7"`)

	// Then
	if !errors.Is(err, ErrNotModified) {
		t.Fatalf("FetchManifest() error = %v", err)
	}
	if manifest != nil || etag != "" {
		t.Fatalf("manifest/etag = %#v/%q", manifest, etag)
	}
}

func TestAgentMode_PerNodePolling(t *testing.T) {
	// Given
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("X-Agent-ID") != testAgentID {
			t.Fatalf("X-Agent-ID = %q", r.Header.Get("X-Agent-ID"))
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("ETag", `"resource-1"`)
		switch {
		case strings.HasSuffix(r.URL.Path, "/configuration"):
			_ = json.NewEncoder(w).Encode(validConfigurationResponse())
		case strings.HasSuffix(r.URL.Path, "/users"):
			_ = json.NewEncoder(w).Encode(validUserSnapshot())
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	agentClient, err := NewAgent(server.URL, testAgentID, testToken)
	if err != nil {
		t.Fatal(err)
	}
	nodeClient, err := agentClient.ForNode(testNodeID)
	if err != nil {
		t.Fatal(err)
	}

	// When
	_, _, configErr := nodeClient.FetchConfiguration(context.Background(), "")
	_, _, usersErr := nodeClient.FetchUsers(context.Background(), "inb-1", "", "rev-1")

	// Then
	if configErr != nil || usersErr != nil {
		t.Fatalf("poll errors = %v, %v", configErr, usersErr)
	}
	want := []string{
		"/api/v1/nodes/" + testNodeID + "/configuration",
		"/api/v1/nodes/" + testNodeID + "/inbounds/inb-1/users",
	}
	if len(paths) != len(want) || paths[0] != want[0] || paths[1] != want[1] {
		t.Fatalf("paths = %#v, want %#v", paths, want)
	}
}

func TestAgentMode_RevokedToken(t *testing.T) {
	// Given
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(contract.ProblemDetails{
			Title: "Unauthorized", Status: 401, Code: "UNAUTHORIZED", Detail: "revoked at_revoked_secret",
		})
	}))
	defer server.Close()
	panelClient, err := NewAgent(server.URL, testAgentID, "at_revoked_secret")
	if err != nil {
		t.Fatal(err)
	}

	// When
	_, _, err = panelClient.FetchManifest(context.Background(), "")

	// Then
	var responseErr *ResponseError
	if !errors.As(err, &responseErr) || responseErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("FetchManifest() error = %#v", err)
	}
	if IsRetryable(err) {
		t.Fatal("revoked token must not be retried")
	}
	if strings.Contains(err.Error(), "at_revoked_secret") {
		t.Fatalf("error leaked token: %v", err)
	}
}

func TestAgentMode_TokenRotation(t *testing.T) {
	// Given
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			if r.URL.Path != "/api/v1/agents/"+testAgentID+"/adapter-token/rotate" {
				t.Fatalf("rotation path = %q", r.URL.Path)
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(TokenRotationResponse{
				Token: "at_replacement_secret", ExpiresAt: time.Now().Add(time.Hour), RotateAfterSeconds: 60,
			})
			return
		}
		if r.Header.Get("Authorization") != "Bearer at_replacement_secret" {
			t.Fatalf("node Authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(validConfigurationResponse())
	}))
	defer server.Close()
	agentClient, err := NewAgent(server.URL, testAgentID, "at_old_secret")
	if err != nil {
		t.Fatal(err)
	}
	nodeClient, err := agentClient.ForNode(testNodeID)
	if err != nil {
		t.Fatal(err)
	}

	// When
	rotated, err := agentClient.RotateToken(context.Background())
	if err == nil {
		err = agentClient.SetToken(rotated.Token)
	}
	if err == nil {
		_, _, err = nodeClient.FetchConfiguration(context.Background(), "")
	}

	// Then
	if err != nil {
		t.Fatalf("rotation flow error = %v", err)
	}
}

func TestAgentMode_NoSecretLogging(t *testing.T) {
	// Given
	captured := &captureLogger{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(contract.AgentManifest{
			APIVersion:       contract.AgentManifestAPIVersion,
			AgentID:          testAgentID,
			ManifestRevision: 1,
			Reconciliation:   contract.AgentManifestReconciliation{FullSnapshot: true, PollAfterSeconds: 30},
			Nodes:            []contract.AgentManifestNode{},
		})
	}))
	defer server.Close()
	secret := "at_secret_never_log"
	panelClient, err := NewAgent(server.URL, testAgentID, secret, WithLogger(captured))
	if err != nil {
		t.Fatal(err)
	}

	// When
	_, _, err = panelClient.FetchManifest(context.Background(), "")

	// Then
	if err != nil {
		t.Fatalf("FetchManifest() error = %v", err)
	}
	for _, entry := range captured.entries {
		if strings.Contains(entry.message, secret) || strings.Contains(entry.message, "Bearer") {
			t.Fatalf("log entry leaked credential: %q", entry.message)
		}
	}
}
