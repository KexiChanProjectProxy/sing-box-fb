package mock

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

// ============================================================================
// Helpers
// ============================================================================

// newAuthedRequest creates an HTTP request with the default bearer token.
func newAuthedRequest(method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+DefaultToken)
	return req
}

// doGET performs an authenticated GET against the mock server.
func doGET(s *Server, path string) *http.Response {
	req := newAuthedRequest(http.MethodGet, s.URL()+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

// doPOST performs an authenticated POST against the mock server with a JSON body.
func doPOST(s *Server, path string, v interface{}) *http.Response {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	req := newAuthedRequest(http.MethodPost, s.URL()+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

// doPOSTRaw performs an authenticated POST with raw body bytes.
func doPOSTRaw(s *Server, path string, body []byte) *http.Response {
	req := newAuthedRequest(http.MethodPost, s.URL()+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

// readBody reads and closes the response body, returning the bytes.
func readBody(resp *http.Response) []byte {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return b
}

// configPath returns the configuration endpoint path for the default node.
func configPath() string {
	return "/api/v1/nodes/" + DefaultNodeID + "/configuration"
}

// usersPath returns the users endpoint path for the given inbound.
func usersPath(inboundID string) string {
	return "/api/v1/nodes/" + DefaultNodeID + "/inbounds/" + inboundID + "/users"
}

// trafficPath returns the traffic-reports endpoint path for the default node.
func trafficPath() string {
	return "/api/v1/nodes/" + DefaultNodeID + "/traffic-reports"
}

// heartbeatPath returns the heartbeats endpoint path for the default node.
func heartbeatPath() string {
	return "/api/v1/nodes/" + DefaultNodeID + "/heartbeats"
}

// ============================================================================
// 1. Initialization / Bootstrap
// ============================================================================

func TestServer_NewDefault(t *testing.T) {
	s := New(t)
	if s.URL() == "" {
		t.Fatal("expected non-empty server URL")
	}
	if !strings.HasPrefix(s.URL(), "http://") {
		t.Fatalf("expected URL to start with http://, got %s", s.URL())
	}
}

func TestServer_NewWithCustomNodeIDAndToken(t *testing.T) {
	s := New(t, WithNodeID("custom-node"), WithToken("custom-token"))
	if s.URL() == "" {
		t.Fatal("expected non-empty server URL")
	}

	// Request with the custom token should succeed.
	req, _ := http.NewRequest(http.MethodGet, s.URL()+"/api/v1/nodes/custom-node/configuration", nil)
	req.Header.Set("Authorization", "Bearer custom-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	// No configuration set, so we expect 404.
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestServer_URLIsValid(t *testing.T) {
	s := New(t)
	url := s.URL()
	resp, err := http.Get(url + "/nonexistent")
	if err != nil {
		t.Fatalf("unexpected error connecting to server: %v", err)
	}
	defer resp.Body.Close()
	// Server is reachable.
	if resp.StatusCode == 0 {
		t.Fatal("expected a valid HTTP status code")
	}
}

// ============================================================================
// 2. Configuration update
// ============================================================================

func TestServer_Configuration_GetReturnsConfiguredResponse(t *testing.T) {
	s := New(t)
	cfg := ValidConfigurationResponse()
	s.SetConfiguration(cfg)

	resp := doGET(s, configPath())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var got contract.ConfigurationResponse
	body := readBody(resp)
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if got.Revision != cfg.Revision {
		t.Errorf("expected revision %q, got %q", cfg.Revision, got.Revision)
	}
	if got.NodeID != cfg.NodeID {
		t.Errorf("expected node_id %q, got %q", cfg.NodeID, got.NodeID)
	}
}

func TestServer_Configuration_EndpointPathAndMethod(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	resp := doGET(s, configPath())
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	AssertCalled(t, s, http.MethodGet, "/configuration")
}

func TestServer_Configuration_CacheControlNoStore(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	resp := doGET(s, configPath())
	defer resp.Body.Close()

	if v := resp.Header.Get("Cache-Control"); v != "no-store" {
		t.Errorf("expected Cache-Control: no-store, got %q", v)
	}
}

func TestServer_Configuration_ETagReturned(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	resp := doGET(s, configPath())
	defer resp.Body.Close()

	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header to be set")
	}
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("ETag should be quoted, got %q", etag)
	}
}

func TestServer_Configuration_IfNoneMatchMatchingETagReturns304(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	// First request to get the ETag.
	resp1 := doGET(s, configPath())
	etag := resp1.Header.Get("ETag")
	resp1.Body.Close()
	if etag == "" {
		t.Fatal("expected ETag from first request")
	}

	// Second request with If-None-Match.
	req := newAuthedRequest(http.MethodGet, s.URL()+configPath(), nil)
	req.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", resp2.StatusCode)
	}
}

func TestServer_Configuration_IfNoneMatchNonMatchingETagReturns200(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	req := newAuthedRequest(http.MethodGet, s.URL()+configPath(), nil)
	req.Header.Set("If-None-Match", `"stale-etag"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestServer_Configuration_RevisionChangesBetweenPolls(t *testing.T) {
	s := New(t)

	cfg1 := ValidConfigurationResponse()
	cfg1.Revision = "rev-001"
	s.SetConfiguration(cfg1)

	etag1 := getETag(t, s, configPath())

	cfg2 := ValidConfigurationResponse()
	cfg2.Revision = "rev-002"
	s.SetConfiguration(cfg2)

	etag2 := getETag(t, s, configPath())

	if etag1 == etag2 {
		t.Errorf("expected different ETags when configuration revision changes, got %q both times", etag1)
	}
}

func TestServer_Configuration_StatusOverride503(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())
	s.SetConfigStatus(http.StatusServiceUnavailable)

	resp := doGET(s, configPath())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Status != http.StatusServiceUnavailable {
		t.Errorf("expected problem status 503, got %d", pd.Status)
	}
}

func TestServer_Configuration_403NodeMismatchOnWrongToken(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	req, _ := http.NewRequest(http.MethodGet, s.URL()+configPath(), nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "NODE_MISMATCH" {
		t.Errorf("expected code NODE_MISMATCH, got %q", pd.Code)
	}
}

func TestServer_Configuration_401OnMissingAuth(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	req, _ := http.NewRequest(http.MethodGet, s.URL()+configPath(), nil)
	// No Authorization header.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "UNAUTHORIZED" {
		t.Errorf("expected code UNAUTHORIZED, got %q", pd.Code)
	}
}

// ============================================================================
// 3. User add
// ============================================================================

func TestServer_Users_GetReturnsUserSnapshot(t *testing.T) {
	s := New(t)
	snap := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	s.SetUserSnapshot("inb-hy2", snap)

	resp := doGET(s, usersPath("inb-hy2"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var got contract.UserSnapshot
	body := readBody(resp)
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if got.InboundID != "inb-hy2" {
		t.Errorf("expected inbound_id inb-hy2, got %q", got.InboundID)
	}
	if len(got.Users) != 2 {
		t.Errorf("expected 2 users, got %d", len(got.Users))
	}
}

func TestServer_Users_AddNewUsers(t *testing.T) {
	s := New(t)

	// Start with 2 users.
	snap := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	s.SetUserSnapshot("inb-hy2", snap)

	resp1 := doGET(s, usersPath("inb-hy2"))
	var got1 contract.UserSnapshot
	if err := json.Unmarshal(readBody(resp1), &got1); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(got1.Users) != 2 {
		t.Fatalf("expected 2 users initially, got %d", len(got1.Users))
	}

	// Update with 5 users.
	snap2 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	WithUserCount("inb-hy2", 5)(snap2)
	s.SetUserSnapshot("inb-hy2", snap2)

	resp2 := doGET(s, usersPath("inb-hy2"))
	var got2 contract.UserSnapshot
	if err := json.Unmarshal(readBody(resp2), &got2); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(got2.Users) != 5 {
		t.Errorf("expected 5 users after update, got %d", len(got2.Users))
	}
}

func TestServer_Users_UserCountIncreases(t *testing.T) {
	s := New(t)

	snap := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	s.SetUserSnapshot("inb-hy2", snap)

	count1 := getUserCount(t, s, "inb-hy2")

	snap2 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	WithUserCount("inb-hy2", 10)(snap2)
	s.SetUserSnapshot("inb-hy2", snap2)

	count2 := getUserCount(t, s, "inb-hy2")

	if count2 <= count1 {
		t.Errorf("expected user count to increase, got %d -> %d", count1, count2)
	}
	if count2 != 10 {
		t.Errorf("expected 10 users, got %d", count2)
	}
}

func TestServer_Users_RevisionChanges(t *testing.T) {
	s := New(t)

	snap1 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	snap1.Revision = "user-rev-001"
	s.SetUserSnapshot("inb-hy2", snap1)
	etag1 := getETag(t, s, usersPath("inb-hy2"))

	snap2 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	snap2.Revision = "user-rev-002"
	s.SetUserSnapshot("inb-hy2", snap2)
	etag2 := getETag(t, s, usersPath("inb-hy2"))

	if etag1 == etag2 {
		t.Errorf("expected ETag to change when user revision changes")
	}
}

func TestServer_Users_MultipleInboundsHaveSeparateSnapshots(t *testing.T) {
	s := New(t)

	snap1 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	snap2 := ValidUserSnapshot("inb-ss", "shadowsocks", "rev-001")
	s.SetUserSnapshot("inb-hy2", snap1)
	s.SetUserSnapshot("inb-ss", snap2)

	got1 := getUserCount(t, s, "inb-hy2")
	got2 := getUserCount(t, s, "inb-ss")

	if got1 != 2 {
		t.Errorf("expected 2 users for inb-hy2, got %d", got1)
	}
	if got2 != 2 {
		t.Errorf("expected 2 users for inb-ss, got %d", got2)
	}

	// Verify they are independent.
	resp := doGET(s, usersPath("inb-hy2"))
	var hy2 contract.UserSnapshot
	if err := json.Unmarshal(readBody(resp), &hy2); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if hy2.Protocol != "hysteria2" {
		t.Errorf("expected protocol hysteria2, got %q", hy2.Protocol)
	}
}

// ============================================================================
// 4. User delete
// ============================================================================

func TestServer_Users_RemoveUsers(t *testing.T) {
	s := New(t)

	// Start with 5 users.
	snap := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	WithUserCount("inb-hy2", 5)(snap)
	s.SetUserSnapshot("inb-hy2", snap)

	count1 := getUserCount(t, s, "inb-hy2")
	if count1 != 5 {
		t.Fatalf("expected 5 users initially, got %d", count1)
	}

	// Reduce to 2 users.
	snap2 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	WithUserCount("inb-hy2", 2)(snap2)
	s.SetUserSnapshot("inb-hy2", snap2)

	count2 := getUserCount(t, s, "inb-hy2")
	if count2 != 2 {
		t.Errorf("expected 2 users after removal, got %d", count2)
	}
}

func TestServer_Users_EmptyUsersSlice(t *testing.T) {
	s := New(t)

	snap := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	snap.Users = []contract.User{} // Empty but not nil.
	s.SetUserSnapshot("inb-hy2", snap)

	resp := doGET(s, usersPath("inb-hy2"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var got contract.UserSnapshot
	if err := json.Unmarshal(readBody(resp), &got); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(got.Users) != 0 {
		t.Errorf("expected 0 users, got %d", len(got.Users))
	}
}

func TestServer_Users_UserCountDecreases(t *testing.T) {
	s := New(t)

	snap1 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	WithUserCount("inb-hy2", 10)(snap1)
	s.SetUserSnapshot("inb-hy2", snap1)

	count1 := getUserCount(t, s, "inb-hy2")

	snap2 := ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
	WithUserCount("inb-hy2", 3)(snap2)
	s.SetUserSnapshot("inb-hy2", snap2)

	count2 := getUserCount(t, s, "inb-hy2")

	if count2 >= count1 {
		t.Errorf("expected user count to decrease, got %d -> %d", count1, count2)
	}
	if count2 != 3 {
		t.Errorf("expected 3 users, got %d", count2)
	}
}

func TestServer_Users_InboundNotFoundReturns404(t *testing.T) {
	s := New(t)

	resp := doGET(s, usersPath("nonexistent-inb"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "NOT_FOUND" {
		t.Errorf("expected code NOT_FOUND, got %q", pd.Code)
	}
}

// ============================================================================
// 5. Traffic statistics
// ============================================================================

func TestServer_Traffic_PostAcceptsPayload(t *testing.T) {
	s := New(t)

	report := ValidTrafficReport("rev-001")
	resp := doPOST(s, trafficPath(), report)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestServer_Traffic_BodyDecodedAndStored(t *testing.T) {
	s := New(t)

	report := ValidTrafficReport("rev-001")
	resp := doPOST(s, trafficPath(), report)
	resp.Body.Close()

	reports := s.TrafficReports()
	if len(reports) != 1 {
		t.Fatalf("expected 1 traffic report, got %d", len(reports))
	}
	if reports[0].ConfigurationRevision != "rev-001" {
		t.Errorf("expected configuration_revision rev-001, got %q", reports[0].ConfigurationRevision)
	}
	if len(reports[0].Records) != 2 {
		t.Errorf("expected 2 records, got %d", len(reports[0].Records))
	}
}

func TestServer_Traffic_MultipleReportsAccumulated(t *testing.T) {
	s := New(t)

	r1 := ValidTrafficReport("rev-001")
	r2 := ValidTrafficReport("rev-002")
	doPOST(s, trafficPath(), r1).Body.Close()
	doPOST(s, trafficPath(), r2).Body.Close()

	reports := s.TrafficReports()
	if len(reports) != 2 {
		t.Fatalf("expected 2 traffic reports, got %d", len(reports))
	}
	if reports[0].ConfigurationRevision != "rev-001" {
		t.Errorf("report[0]: expected rev rev-001, got %q", reports[0].ConfigurationRevision)
	}
	if reports[1].ConfigurationRevision != "rev-002" {
		t.Errorf("report[1]: expected rev rev-002, got %q", reports[1].ConfigurationRevision)
	}
}

func TestServer_Traffic_InvalidBodyReturns400(t *testing.T) {
	s := New(t)

	resp := doPOSTRaw(s, trafficPath(), []byte(`{invalid json`))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "BAD_REQUEST" {
		t.Errorf("expected code BAD_REQUEST, got %q", pd.Code)
	}
}

func TestServer_Traffic_StatusOverride503(t *testing.T) {
	s := New(t)
	s.SetTrafficStatus(http.StatusServiceUnavailable)

	report := ValidTrafficReport("rev-001")
	resp := doPOST(s, trafficPath(), report)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	// Traffic report should NOT be stored when status is overridden to error.
	reports := s.TrafficReports()
	if len(reports) != 0 {
		t.Errorf("expected 0 traffic reports when status override is 503, got %d", len(reports))
	}
}

// ============================================================================
// 6. Heartbeat reporting
// ============================================================================

func TestServer_Heartbeat_PostAcceptsPayload(t *testing.T) {
	s := New(t)

	hb := ValidHeartbeat("rev-001")
	resp := doPOST(s, heartbeatPath(), hb)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestServer_Heartbeat_BodyDecodedAndStored(t *testing.T) {
	s := New(t)

	hb := ValidHeartbeat("rev-001")
	doPOST(s, heartbeatPath(), hb).Body.Close()

	heartbeats := s.Heartbeats()
	if len(heartbeats) != 1 {
		t.Fatalf("expected 1 heartbeat, got %d", len(heartbeats))
	}
	if heartbeats[0].AppliedConfigurationRevision != "rev-001" {
		t.Errorf("expected applied_configuration_revision rev-001, got %q", heartbeats[0].AppliedConfigurationRevision)
	}
	if heartbeats[0].SingBoxVersion != "1.12.0" {
		t.Errorf("expected sing_box_version 1.12.0, got %q", heartbeats[0].SingBoxVersion)
	}
	if len(heartbeats[0].InboundStatuses) != 1 {
		t.Errorf("expected 1 inbound status, got %d", len(heartbeats[0].InboundStatuses))
	}
}

func TestServer_Heartbeat_MultipleAccumulated(t *testing.T) {
	s := New(t)

	hb1 := ValidHeartbeat("rev-001")
	hb2 := ValidHeartbeat("rev-002")
	doPOST(s, heartbeatPath(), hb1).Body.Close()
	doPOST(s, heartbeatPath(), hb2).Body.Close()

	heartbeats := s.Heartbeats()
	if len(heartbeats) != 2 {
		t.Fatalf("expected 2 heartbeats, got %d", len(heartbeats))
	}
	if heartbeats[0].AppliedConfigurationRevision != "rev-001" {
		t.Errorf("heartbeat[0]: expected rev-001, got %q", heartbeats[0].AppliedConfigurationRevision)
	}
	if heartbeats[1].AppliedConfigurationRevision != "rev-002" {
		t.Errorf("heartbeat[1]: expected rev-002, got %q", heartbeats[1].AppliedConfigurationRevision)
	}
}

func TestServer_Heartbeat_InvalidBodyReturns400(t *testing.T) {
	s := New(t)

	resp := doPOSTRaw(s, heartbeatPath(), []byte(`{not json`))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "BAD_REQUEST" {
		t.Errorf("expected code BAD_REQUEST, got %q", pd.Code)
	}
}

func TestServer_Heartbeat_StatusOverride(t *testing.T) {
	s := New(t)
	s.SetHeartbeatStatus(http.StatusServiceUnavailable)

	hb := ValidHeartbeat("rev-001")
	resp := doPOST(s, heartbeatPath(), hb)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	// Heartbeat should NOT be stored on error status override.
	hbs := s.Heartbeats()
	if len(hbs) != 0 {
		t.Errorf("expected 0 heartbeats when status override is 503, got %d", len(hbs))
	}
}

// ============================================================================
// 7. Edge cases
// ============================================================================

func TestServer_UnknownPathReturns404(t *testing.T) {
	s := New(t)

	resp := doGET(s, "/api/v1/nodes/"+DefaultNodeID+"/unknown")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestServer_WrongHTTPMethodReturns404(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	// POST to a GET-only endpoint.
	resp := doPOST(s, configPath(), nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for wrong method, got %d", resp.StatusCode)
	}
}

func TestServer_NodeIDMismatchInPathReturns403(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	// Correct token but wrong nodeID in path.
	req := newAuthedRequest(http.MethodGet, s.URL()+"/api/v1/nodes/wrong-node/configuration", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "NODE_MISMATCH" {
		t.Errorf("expected code NODE_MISMATCH, got %q", pd.Code)
	}
}

func TestServer_TranscriptCapturesAllRequestsIncludingFailures(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	// Successful request.
	resp1 := doGET(s, configPath())
	resp1.Body.Close()

	// Failed request (no auth).
	req, _ := http.NewRequest(http.MethodGet, s.URL()+configPath(), nil)
	resp2, _ := http.DefaultClient.Do(req)
	resp2.Body.Close()

	// Failed request (wrong path).
	resp3 := doGET(s, "/api/v1/nodes/"+DefaultNodeID+"/nonexistent")
	resp3.Body.Close()

	transcript := s.Transcript()
	if len(transcript) < 3 {
		t.Fatalf("expected at least 3 recorded requests, got %d", len(transcript))
	}

	// Verify successful request recorded.
	AssertCalled(t, s, http.MethodGet, "/configuration")
	// Verify failed auth request recorded.
	AssertCalledN(t, s, http.MethodGet, "/configuration", 2)
}

func TestServer_ResetClearsTranscriptAndPayloads(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())
	s.SetUserSnapshot("inb-hy2", ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))

	// Make some requests.
	doGET(s, configPath()).Body.Close()
	doGET(s, usersPath("inb-hy2")).Body.Close()
	doPOST(s, trafficPath(), ValidTrafficReport("rev-001")).Body.Close()
	doPOST(s, heartbeatPath(), ValidHeartbeat("rev-001")).Body.Close()

	// Verify data was recorded.
	if len(s.Transcript()) == 0 {
		t.Fatal("expected transcript entries before reset")
	}
	if len(s.TrafficReports()) == 0 {
		t.Fatal("expected traffic reports before reset")
	}
	if len(s.Heartbeats()) == 0 {
		t.Fatal("expected heartbeats before reset")
	}

	// Reset.
	s.Reset()

	// Verify all cleared.
	if len(s.Transcript()) != 0 {
		t.Errorf("expected empty transcript after reset, got %d", len(s.Transcript()))
	}
	if len(s.TrafficReports()) != 0 {
		t.Errorf("expected empty traffic reports after reset, got %d", len(s.TrafficReports()))
	}
	if len(s.Heartbeats()) != 0 {
		t.Errorf("expected empty heartbeats after reset, got %d", len(s.Heartbeats()))
	}

	// Verify configured responses are still usable after reset.
	resp := doGET(s, configPath())
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected config to still work after reset, got %d", resp.StatusCode)
	}
}

func TestServer_ConcurrentAccessIsSafe(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())
	s.SetUserSnapshot("inb-hy2", ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Launch concurrent readers and writers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Read configuration.
			resp := doGET(s, configPath())
			if resp.StatusCode != http.StatusOK {
				errors <- fmt.Errorf("goroutine %d: config GET returned %d", i, resp.StatusCode)
			}
			resp.Body.Close()
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Post traffic report.
			report := ValidTrafficReport("rev-001")
			resp := doPOST(s, trafficPath(), report)
			if resp.StatusCode != http.StatusOK {
				errors <- fmt.Errorf("goroutine %d: traffic POST returned %d", i, resp.StatusCode)
			}
			resp.Body.Close()
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Update configuration.
			cfg := NewConfigurationResponse(WithManagedInbound(
				fmt.Sprintf("inb-%d", i), fmt.Sprintf("tag-%d", i), "hysteria2",
			))
			s.SetConfiguration(cfg)
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent access error: %v", err)
	}
}

// ============================================================================
// Additional coverage: Assertion helpers integration
// ============================================================================

func TestServer_AssertAuthorizationBearer(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	doGET(s, configPath()).Body.Close()

	if !AssertAuthorizationBearer(t, s, DefaultToken) {
		t.Fatal("AssertAuthorizationBearer should pass with correct token")
	}
}

func TestServer_AssertNoTokenInURL(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())

	doGET(s, configPath()).Body.Close()

	if !AssertNoTokenInURL(t, s) {
		t.Fatal("AssertNoTokenInURL should pass when token is not in URL")
	}
}

func TestServer_AssertOnlySpecPaths(t *testing.T) {
	s := New(t)
	s.SetConfiguration(ValidConfigurationResponse())
	s.SetUserSnapshot("inb-hy2", ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))

	doGET(s, configPath()).Body.Close()
	doGET(s, usersPath("inb-hy2")).Body.Close()
	doPOST(s, trafficPath(), ValidTrafficReport("rev-001")).Body.Close()
	doPOST(s, heartbeatPath(), ValidHeartbeat("rev-001")).Body.Close()

	if !AssertOnlySpecPaths(t, s, DefaultNodeID) {
		t.Fatal("AssertOnlySpecPaths should pass when only spec paths are called")
	}
}

func TestServer_Users_CacheControlNoStore(t *testing.T) {
	s := New(t)
	s.SetUserSnapshot("inb-hy2", ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))

	resp := doGET(s, usersPath("inb-hy2"))
	defer resp.Body.Close()

	if v := resp.Header.Get("Cache-Control"); v != "no-store" {
		t.Errorf("expected Cache-Control: no-store on users, got %q", v)
	}
}

func TestServer_Users_ETagAndIfNoneMatch(t *testing.T) {
	s := New(t)
	s.SetUserSnapshot("inb-hy2", ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))

	// Get ETag.
	resp1 := doGET(s, usersPath("inb-hy2"))
	etag := resp1.Header.Get("ETag")
	resp1.Body.Close()
	if etag == "" {
		t.Fatal("expected ETag on users response")
	}

	// If-None-Match with matching ETag.
	req := newAuthedRequest(http.MethodGet, s.URL()+usersPath("inb-hy2"), nil)
	req.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", resp2.StatusCode)
	}
}

func TestServer_Users_StatusOverride(t *testing.T) {
	s := New(t)
	s.SetUserSnapshot("inb-hy2", ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))
	s.SetUsersStatus(http.StatusServiceUnavailable)

	resp := doGET(s, usersPath("inb-hy2"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

func TestServer_Configuration_NoConfigReturns404(t *testing.T) {
	s := New(t)
	// No configuration set.

	resp := doGET(s, configPath())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 when no configuration set, got %d", resp.StatusCode)
	}
}

func TestServer_Traffic_ValidationFailureReturns400(t *testing.T) {
	s := New(t)

	// Send a TrafficReport that will fail validation (empty records, missing config rev).
	report := &contract.TrafficReport{
		StartedAt:             ValidTrafficReport("rev-001").StartedAt,
		EndedAt:               ValidTrafficReport("rev-001").EndedAt,
		ConfigurationRevision: "", // Missing.
		Records:               []contract.TrafficRecord{},
	}
	resp := doPOST(s, trafficPath(), report)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for validation failure, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "VALIDATION_ERROR" {
		t.Errorf("expected code VALIDATION_ERROR, got %q", pd.Code)
	}
}

func TestServer_Heartbeat_ValidationFailureReturns400(t *testing.T) {
	s := New(t)

	// Send a Heartbeat missing required fields.
	hb := &contract.Heartbeat{
		ObservedAt:                   time.Now(),
		SingBoxVersion:               "", // Missing.
		AdapterVersion:               "0.1.0",
		AppliedConfigurationRevision: "rev-001",
		InboundStatuses:              []contract.HeartbeatInbound{},
	}
	resp := doPOST(s, heartbeatPath(), hb)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for validation failure, got %d", resp.StatusCode)
	}

	var pd contract.ProblemDetails
	body := readBody(resp)
	if err := json.Unmarshal(body, &pd); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}
	if pd.Code != "VALIDATION_ERROR" {
		t.Errorf("expected code VALIDATION_ERROR, got %q", pd.Code)
	}
}

// ============================================================================
// Helpers (internal to test file)
// ============================================================================

// getETag performs a GET and returns the ETag header value.
func getETag(t testing.TB, s *Server, path string) string {
	t.Helper()
	resp := doGET(s, path)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 to get ETag, got %d", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}
	return etag
}

// getUserCount fetches the users endpoint and returns the user count.
func getUserCount(t testing.TB, s *Server, inboundID string) int {
	t.Helper()
	resp := doGET(s, usersPath(inboundID))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var snap contract.UserSnapshot
	if err := json.Unmarshal(readBody(resp), &snap); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	return len(snap.Users)
}

// All test fixtures use ValidHeartbeat or ValidTrafficReport which already
// set time-related fields.
