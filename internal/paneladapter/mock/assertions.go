package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

// specPaths are the four API v1 paths the panel adapter should target.
var specPaths = []string{
	"/api/v1/nodes/",
}

// isSpecPath returns true if the request path targets one of the 4 spec endpoints.
func isSpecPath(path, nodeID string) bool {
	prefix := "/api/v1/nodes/" + nodeID
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(path, prefix)
	return suffix == "/configuration" ||
		strings.HasPrefix(suffix, "/inbounds/") && strings.HasSuffix(suffix, "/users") ||
		suffix == "/traffic-reports" ||
		suffix == "/heartbeats"
}

// AssertCalled fails the test if no request was recorded with the matching
// method and path. Path matching is substring-based.
func AssertCalled(t testing.TB, s *Server, method, path string) bool {
	t.Helper()
	for _, r := range s.Transcript() {
		if r.Method == method && strings.Contains(r.Path, path) {
			return true
		}
	}
	t.Errorf("expected request %s %s to be called, but it was not", method, path)
	return false
}

// AssertNotCalled fails the test if any request was recorded with the matching
// method and path. Path matching is substring-based.
func AssertNotCalled(t testing.TB, s *Server, method, path string) bool {
	t.Helper()
	for _, r := range s.Transcript() {
		if r.Method == method && strings.Contains(r.Path, path) {
			t.Errorf("expected request %s %s NOT to be called, but it was", method, path)
			return false
		}
	}
	return true
}

// AssertCalledN fails the test if the count of requests with the matching
// method and path is not exactly n. Path matching is substring-based.
func AssertCalledN(t testing.TB, s *Server, method, path string, n int) bool {
	t.Helper()
	count := 0
	for _, r := range s.Transcript() {
		if r.Method == method && strings.Contains(r.Path, path) {
			count++
		}
	}
	if count != n {
		t.Errorf("expected %s %s to be called exactly %d time(s), got %d", method, path, n, count)
		return false
	}
	return true
}

// AssertAuthorizationBearer verifies that every recorded request has an
// Authorization: Bearer <expectedToken> header.
func AssertAuthorizationBearer(t testing.TB, s *Server, expectedToken string) bool {
	t.Helper()
	transcript := s.Transcript()
	if len(transcript) == 0 {
		t.Errorf("expected at least one request to check Authorization header, got none")
		return false
	}
	expected := "Bearer " + expectedToken
	ok := true
	for i, r := range transcript {
		got := r.Headers.Get("Authorization")
		if got != expected {
			t.Errorf("request %d: Authorization = %q, want %q", i, got, expected)
			ok = false
		}
	}
	return ok
}

// AssertNoTokenInURL verifies that no recorded request URL path contains
// the token string (ensuring credentials aren't leaked in URLs).
func AssertNoTokenInURL(t testing.TB, s *Server) bool {
	t.Helper()
	token := DefaultToken
	ok := true
	for i, r := range s.Transcript() {
		if strings.Contains(r.Path, token) {
			t.Errorf("request %d: path %q contains token string", i, r.Path)
			ok = false
		}
	}
	return ok
}

// AssertOnlySpecPaths verifies that every recorded request targets one of the
// 4 spec paths: configuration, users, traffic-reports, or heartbeats.
func AssertOnlySpecPaths(t testing.TB, s *Server, nodeID string) bool {
	t.Helper()
	ok := true
	for i, r := range s.Transcript() {
		if !isSpecPath(r.Path, nodeID) {
			t.Errorf("request %d: path %q is not a spec path for node %s", i, r.Path, nodeID)
			ok = false
		}
	}
	return ok
}

// AssertHeader verifies that a request matching the given method and path
// (substring match) has a header with the expected value.
func AssertHeader(t testing.TB, s *Server, method, path, key, expectedValue string) bool {
	t.Helper()
	for _, r := range s.Transcript() {
		if r.Method == method && strings.Contains(r.Path, path) {
			got := r.Headers.Get(key)
			if got != expectedValue {
				t.Errorf("request %s %s: header %q = %q, want %q", method, path, key, got, expectedValue)
				return false
			}
			return true
		}
	}
	t.Errorf("no request found matching %s %s to check header %q", method, path, key)
	return false
}

// AssertCacheControlNoStore verifies that GET config and GET users 200
// responses included Cache-Control: no-store.
func AssertCacheControlNoStore(t testing.TB, s *Server) bool {
	t.Helper()
	transcript := s.Transcript()
	ok := true
	found := false
	for _, r := range transcript {
		if r.Method != http.MethodGet {
			continue
		}
		if strings.HasSuffix(r.Path, "/configuration") || (strings.Contains(r.Path, "/inbounds/") && strings.HasSuffix(r.Path, "/users")) {
			found = true
			got := r.Headers.Get("Cache-Control")
			if got != "no-store" {
				t.Errorf("GET %s: Cache-Control = %q, want %q", r.Path, got, "no-store")
				ok = false
			}
		}
	}
	if !found && len(transcript) > 0 {
		t.Errorf("no GET config or users requests found to check Cache-Control")
		return false
	}
	return ok
}

// MustDecodeBody is a generic helper that JSON-decodes a request body byte
// slice into type T, failing the test on any error.
func MustDecodeBody[T any](t testing.TB, body []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("failed to decode body into %T: %v; body=%q", v, err, string(body))
	}
	return v
}

// MustDecodeResponse is a generic helper that decodes a recorded request body
// into type T, failing the test on any error.
func MustDecodeResponse[T any](t testing.TB, rec RequestRecord) T {
	t.Helper()
	return MustDecodeBody[T](t, rec.Body)
}

// formatSpecPaths returns a human-readable list of the 4 spec paths for a node.
func formatSpecPaths(nodeID string) string {
	return fmt.Sprintf(
		"/api/v1/nodes/%s/configuration, "+
			"/api/v1/nodes/%s/inbounds/{inbound_id}/users, "+
			"/api/v1/nodes/%s/traffic-reports, "+
			"/api/v1/nodes/%s/heartbeats",
		nodeID, nodeID, nodeID, nodeID,
	)
}

// Compile-time check: contract types are available.
var _ *contract.ConfigurationResponse = nil
var _ *contract.UserSnapshot = nil
var _ *contract.TrafficReport = nil
var _ *contract.Heartbeat = nil
var _ *contract.ProblemDetails = nil
