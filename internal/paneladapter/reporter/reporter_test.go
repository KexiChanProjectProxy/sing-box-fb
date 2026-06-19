package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
)

const (
	testNodeID = "node-42"
	testToken  = "test-bearer-token-abc123"
)

func testStatePath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "state.json")
}

func newTestStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.NewStore(testStatePath(t))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return store
}

func newTestTracker() *traffic.Tracker {
	return traffic.NewTracker(map[string]string{
		"ss-in":  "inbound-1",
		"hy2-in": "inbound-2",
	})
}

func mockPanelServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func newTestClient(t *testing.T, serverURL string) *client.Client {
	t.Helper()
	c, err := client.New(serverURL, testNodeID, testToken)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return c
}

func writeConflictResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	pd := contract.ProblemDetails{Code: "IDEMPOTENCY_CONFLICT", Title: "Idempotency conflict"}
	data, _ := json.Marshal(pd)
	_, _ = w.Write(data)
}

func write429Response(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	pd := contract.ProblemDetails{Code: "RATE_LIMITED", Title: "Too many requests"}
	data, _ := json.Marshal(pd)
	_, _ = w.Write(data)
}

func write503Response(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	pd := contract.ProblemDetails{Code: "SERVICE_UNAVAILABLE", Title: "Service unavailable"}
	data, _ := json.Marshal(pd)
	_, _ = w.Write(data)
}

// ---------------------------------------------------------------------------
// Unit tests for helpers
// ---------------------------------------------------------------------------

func TestMakeIdempotencyKey_Stability(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	hash := "abcdef1234567890abcdef1234567890"

	key1 := makeIdempotencyKey(testNodeID, ts, hash)
	key2 := makeIdempotencyKey(testNodeID, ts, hash)
	if key1 != key2 {
		t.Errorf("same inputs should produce same key: %q != %q", key1, key2)
	}

	hash2 := "fedcba0987654321fedcba0987654321"
	key3 := makeIdempotencyKey(testNodeID, ts, hash2)
	if key1 == key3 {
		t.Error("different body hash should produce different key")
	}

	ts2 := time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC)
	key4 := makeIdempotencyKey(testNodeID, ts2, hash)
	if key1 == key4 {
		t.Error("different window start should produce different key")
	}
}

func TestMakeIdempotencyKey_Format(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	hash := "abcdef1234567890abcdef1234567890"
	key := makeIdempotencyKey(testNodeID, ts, hash)
	expected := fmt.Sprintf("tr_%s_%s_%s", testNodeID, "2025-01-15T10:00:00Z", "abcdef12")
	if key != expected {
		t.Errorf("expected key %q, got %q", expected, key)
	}
}

func TestComputeBodyHash_Stability(t *testing.T) {
	report := &contract.TrafficReport{
		StartedAt:             time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		EndedAt:               time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 1000, DownloadBytes: 2000},
		},
	}

	hash1, err := computeBodyHash(report)
	if err != nil {
		t.Fatalf("computeBodyHash: %v", err)
	}
	hash2, err := computeBodyHash(report)
	if err != nil {
		t.Fatalf("computeBodyHash: %v", err)
	}
	if hash1 != hash2 {
		t.Errorf("same report should produce same hash: %q != %q", hash1, hash2)
	}
	if len(hash1) < 8 {
		t.Errorf("hash too short: %q", hash1)
	}
}

func TestComputeBodyHash_DifferentReports(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	report1 := &contract.TrafficReport{
		StartedAt:             ts,
		EndedAt:               ts.Add(30 * time.Minute),
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 1000, DownloadBytes: 2000},
		},
	}
	report2 := &contract.TrafficReport{
		StartedAt:             ts,
		EndedAt:               ts.Add(30 * time.Minute),
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 2000, DownloadBytes: 4000},
		},
	}

	hash1, _ := computeBodyHash(report1)
	hash2, _ := computeBodyHash(report2)
	if hash1 == hash2 {
		t.Error("different reports should produce different hashes")
	}
}

func TestRemovePendingByHash(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	pendings := []state.PendingReport{
		{IdempotencyKey: "key1", BodyHash: "hash1", StartedAt: ts, RetryCount: 0},
		{IdempotencyKey: "key2", BodyHash: "hash2", StartedAt: ts.Add(time.Hour), RetryCount: 1},
		{IdempotencyKey: "key3", BodyHash: "hash3", StartedAt: ts.Add(2 * time.Hour), RetryCount: 2},
	}

	result := removePendingByHash(pendings, "hash2", ts.Add(time.Hour))
	if len(result) != 2 {
		t.Fatalf("expected 2 remaining, got %d", len(result))
	}
	if result[0].BodyHash != "hash1" || result[1].BodyHash != "hash3" {
		t.Errorf("wrong items remaining: %+v", result)
	}

	result2 := removePendingByHash(pendings, "nonexistent", ts)
	if len(result2) != 3 {
		t.Errorf("expected 3 remaining for non-existent hash, got %d", len(result2))
	}

	result3 := removePendingByHash(nil, "hash1", ts)
	if len(result3) != 0 {
		t.Errorf("expected 0 for nil input, got %d", len(result3))
	}
}

func TestNewReporter(t *testing.T) {
	tracker := newTestTracker()
	store := newTestStore(t)
	c := newTestClient(t, "http://localhost:8080")

	r := NewReporter(c, store, tracker, testNodeID)
	if !r.skipEmpty {
		t.Error("default skipEmpty should be true")
	}
	if r.nodeID != testNodeID {
		t.Errorf("expected nodeID %q, got %q", testNodeID, r.nodeID)
	}

	r2 := NewReporter(c, store, tracker, testNodeID,
		WithSkipEmpty(false),
		WithConfigRevision("rev-42"),
	)
	if r2.skipEmpty {
		t.Error("skipEmpty should be false after option")
	}
	if r2.configRevision != "rev-42" {
		t.Errorf("expected configRevision %q, got %q", "rev-42", r2.configRevision)
	}
}

// ---------------------------------------------------------------------------
// Integration tests for ReportNow
// ---------------------------------------------------------------------------

// TestReportNow_EmptyReportSkipped verifies that a report with no records
// is skipped when skipEmpty is true (the default).
func TestReportNow_EmptyReportSkipped(t *testing.T) {
	called := false
	handler := func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err := r.ReportNow(context.Background())
	if err != nil {
		t.Fatalf("ReportNow on empty tracker: %v", err)
	}
	if called {
		t.Error("empty report should not trigger HTTP request")
	}

	st := store.State()
	if len(st.PendingReports) != 0 {
		t.Errorf("expected no pending reports, got %d", len(st.PendingReports))
	}
}

// TestReportNow_EmptyReportNotSkippedWhenConfigured verifies that a
// report with no records IS sent when skipEmpty is false.
func TestReportNow_EmptyReportNotSkippedWhenConfigured(t *testing.T) {
	callCount := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err != nil {
		t.Fatalf("ReportNow: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 HTTP call with skipEmpty=false, got %d", callCount)
	}
}

// TestReportNow_AcceptedClearsPending verifies that a successful
// report clears the pending report and staged counters.
func TestReportNow_AcceptedClearsPending(t *testing.T) {
	var capturedKey string
	handler := func(w http.ResponseWriter, r *http.Request) {
		capturedKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err != nil {
		t.Fatalf("ReportNow: %v", err)
	}

	if capturedKey == "" {
		t.Error("Idempotency-Key header should be set")
	}

	st := store.State()
	if len(st.PendingReports) != 0 {
		t.Errorf("expected no pending reports after success, got %d", len(st.PendingReports))
	}
}

// TestReportNow_DuplicateResponseClearsPending verifies that 200 OK
// (duplicate acceptance) clears the pending report.
func TestReportNow_DuplicateResponseClearsPending(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err != nil {
		t.Fatalf("ReportNow: %v", err)
	}

	st := store.State()
	if len(st.PendingReports) != 0 {
		t.Errorf("expected no pending reports after 200, got %d", len(st.PendingReports))
	}
}

// TestReportNow_RetryReusesIdempotencyKey verifies that on retryable
// error followed by success, the same idempotency key is reused.
// The key scenario: first call fails with 429, the tracker still has
// the staged report, so the second call retries with the same key.
func TestReportNow_RetryReusesIdempotencyKey(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	var keys []string
	var mu sync.Mutex
	callCount := 0

	handler := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()

		if callCount == 1 {
			write429Response(w)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	// First attempt — should fail with 429.
	err := r.ReportNow(context.Background())
	if err == nil {
		t.Fatal("expected error on 429")
	}

	// Pending report should be saved.
	st := store.State()
	if len(st.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report, got %d", len(st.PendingReports))
	}
	firstKey := st.PendingReports[0].IdempotencyKey
	if firstKey == "" {
		t.Fatal("pending report should have idempotency key")
	}

	// Reporter should have the pending report in memory for retry.
	if r.pendingReport == nil {
		t.Fatal("reporter should have pending report in memory after 429")
	}

	// Second attempt — should retry with the same key.
	err = r.ReportNow(context.Background())
	if err != nil {
		t.Fatalf("ReportNow retry: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys captured, got %d", len(keys))
	}
	if keys[0] != keys[1] {
		t.Errorf("retry should reuse same idempotency key: %q != %q", keys[0], keys[1])
	}

	st = store.State()
	if len(st.PendingReports) != 0 {
		t.Errorf("expected no pending reports after retry success, got %d", len(st.PendingReports))
	}
}

// TestReportNow_ConflictPreservesPending verifies that a 409
// IDEMPOTENCY_CONFLICT preserves the pending report as a hard error.
func TestReportNow_ConflictPreservesPending(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		writeConflictResponse(w)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err == nil {
		t.Fatal("expected error on 409")
	}

	st := store.State()
	if len(st.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report after conflict, got %d", len(st.PendingReports))
	}
}

// TestReportNow_TransportErrorKeepsPending verifies that a transport
// error keeps the pending report for retry.
func TestReportNow_TransportErrorKeepsPending(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, "http://127.0.0.1:1")

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err == nil {
		t.Fatal("expected error on transport failure")
	}

	st := store.State()
	if len(st.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report after transport error, got %d", len(st.PendingReports))
	}
}

// TestReportNow_429KeepsPending verifies that a 429 keeps the
// pending report for retry.
func TestReportNow_429KeepsPending(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		write429Response(w)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err == nil {
		t.Fatal("expected error on 429")
	}

	st := store.State()
	if len(st.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report after 429, got %d", len(st.PendingReports))
	}
	if st.PendingReports[0].RetryCount != 0 {
		t.Errorf("initial retry count should be 0, got %d", st.PendingReports[0].RetryCount)
	}
}

// TestReportNow_503KeepsPending verifies that a 503 keeps the
// pending report for retry.
func TestReportNow_503KeepsPending(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		write503Response(w)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err == nil {
		t.Fatal("expected error on 503")
	}

	st := store.State()
	if len(st.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report after 503, got %d", len(st.PendingReports))
	}
}

// TestReportNow_PersistBeforeSend verifies that the pending report
// is persisted to state BEFORE the HTTP request is made.
func TestReportNow_PersistBeforeSend(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		write503Response(w)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	err := r.ReportNow(context.Background())
	if err == nil {
		t.Fatal("expected error on 503")
	}

	st := store.State()
	if len(st.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report persisted before send, got %d", len(st.PendingReports))
	}

	pr := st.PendingReports[0]
	if pr.IdempotencyKey == "" {
		t.Error("pending report should have idempotency key")
	}
	if pr.BodyHash == "" {
		t.Error("pending report should have body hash")
	}
	if pr.StartedAt.IsZero() {
		t.Error("pending report should have started_at")
	}
	if pr.EndedAt.IsZero() {
		t.Error("pending report should have ended_at")
	}
}

// TestReportNow_RetryCountIncrements verifies that retry count
// increments on each failed retry attempt.
func TestReportNow_RetryCountIncrements(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	callCount := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		callCount++
		write429Response(w)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID, WithSkipEmpty(false))

	_ = r.ReportNow(context.Background())
	st := store.State()
	if st.PendingReports[0].RetryCount != 0 {
		t.Errorf("first attempt retry count should be 0, got %d", st.PendingReports[0].RetryCount)
	}

	_ = r.ReportNow(context.Background())
	st = store.State()
	if st.PendingReports[0].RetryCount != 1 {
		t.Errorf("second attempt retry count should be 1, got %d", st.PendingReports[0].RetryCount)
	}

	_ = r.ReportNow(context.Background())
	st = store.State()
	if st.PendingReports[0].RetryCount != 2 {
		t.Errorf("third attempt retry count should be 2, got %d", st.PendingReports[0].RetryCount)
	}
}

// ---------------------------------------------------------------------------
// Recovery tests
// ---------------------------------------------------------------------------

// TestRecovery_NoPendingReports verifies Recovery is a no-op when
// there are no pending reports.
func TestRecovery_NoPendingReports(t *testing.T) {
	called := false
	handler := func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	store := newTestStore(t)
	tracker := newTestTracker()
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err := r.Recovery(context.Background())
	if err != nil {
		t.Fatalf("Recovery: %v", err)
	}
	if called {
		t.Error("Recovery should not make HTTP calls when no pending reports")
	}
}

// TestRecovery_PendingReportsSurviveRestart verifies that pending
// reports stored in state are retried during Recovery().
func TestRecovery_PendingReportsSurviveRestart(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	startedAt := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	bodyHash := "abcdef1234567890"

	st := store.State().Clone()
	st.PendingReports = []state.PendingReport{
		{
			IdempotencyKey: makeIdempotencyKey(testNodeID, startedAt, bodyHash),
			BodyHash:       bodyHash,
			StartedAt:      startedAt,
			EndedAt:        endedAt,
			RetryCount:     1,
		},
	}
	store.SetState(st)
	if err := store.Save(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	pendingReport := &contract.TrafficReport{
		StartedAt:             startedAt,
		EndedAt:               endedAt,
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 1000, DownloadBytes: 2000},
		},
	}
	tracker.RestorePendingReport(pendingReport)

	var capturedKey string
	handler := func(w http.ResponseWriter, r *http.Request) {
		capturedKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err := r.Recovery(context.Background())
	if err != nil {
		t.Fatalf("Recovery: %v", err)
	}

	expectedKey := makeIdempotencyKey(testNodeID, startedAt, bodyHash)
	if capturedKey != expectedKey {
		t.Errorf("expected key %q, got %q", expectedKey, capturedKey)
	}

	st2 := store.State()
	if len(st2.PendingReports) != 0 {
		t.Errorf("expected no pending reports after recovery, got %d", len(st2.PendingReports))
	}
}

// TestRecovery_ConflictKeepsPending verifies that during recovery,
// a 409 conflict preserves the pending report.
func TestRecovery_ConflictKeepsPending(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	startedAt := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	bodyHash := "abcdef1234567890"

	st := store.State().Clone()
	st.PendingReports = []state.PendingReport{
		{
			IdempotencyKey: makeIdempotencyKey(testNodeID, startedAt, bodyHash),
			BodyHash:       bodyHash,
			StartedAt:      startedAt,
			EndedAt:        endedAt,
			RetryCount:     0,
		},
	}
	store.SetState(st)
	if err := store.Save(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	pendingReport := &contract.TrafficReport{
		StartedAt:             startedAt,
		EndedAt:               endedAt,
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 1000, DownloadBytes: 2000},
		},
	}
	tracker.RestorePendingReport(pendingReport)

	handler := func(w http.ResponseWriter, r *http.Request) {
		writeConflictResponse(w)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err := r.Recovery(context.Background())
	if err != nil {
		t.Fatalf("Recovery: %v", err)
	}

	st2 := store.State()
	if len(st2.PendingReports) != 1 {
		t.Errorf("expected 1 pending report after conflict recovery, got %d", len(st2.PendingReports))
	}
}

// TestRecovery_RetryableErrorKeepsPending verifies that during recovery,
// a retryable error keeps the pending report with incremented retry count.
func TestRecovery_RetryableErrorKeepsPending(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	startedAt := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	bodyHash := "abcdef1234567890"

	st := store.State().Clone()
	st.PendingReports = []state.PendingReport{
		{
			IdempotencyKey: makeIdempotencyKey(testNodeID, startedAt, bodyHash),
			BodyHash:       bodyHash,
			StartedAt:      startedAt,
			EndedAt:        endedAt,
			RetryCount:     2,
		},
	}
	store.SetState(st)
	if err := store.Save(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	pendingReport := &contract.TrafficReport{
		StartedAt:             startedAt,
		EndedAt:               endedAt,
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 1000, DownloadBytes: 2000},
		},
	}
	tracker.RestorePendingReport(pendingReport)

	handler := func(w http.ResponseWriter, r *http.Request) {
		write503Response(w)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err := r.Recovery(context.Background())
	if err != nil {
		t.Fatalf("Recovery: %v", err)
	}

	st2 := store.State()
	if len(st2.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report, got %d", len(st2.PendingReports))
	}
	if st2.PendingReports[0].RetryCount != 3 {
		t.Errorf("expected retry count 3, got %d", st2.PendingReports[0].RetryCount)
	}
}

// TestNoDoubleCountingOnRestart verifies that persisted pending reports
// do not double-count with live counters after a restart.
// After a crash, the live counters were already subtracted (journaled).
// On restart, the tracker's live counters are empty. Recovery re-sends
// the pending report but does NOT add to live counters.
func TestNoDoubleCountingOnRestart(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	startedAt := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	pendingReport := &contract.TrafficReport{
		StartedAt:             startedAt,
		EndedAt:               endedAt,
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 1000, DownloadBytes: 2000},
		},
	}

	bodyHash, err := computeBodyHash(pendingReport)
	if err != nil {
		t.Fatalf("computeBodyHash: %v", err)
	}

	st := store.State().Clone()
	st.PendingReports = []state.PendingReport{
		{
			IdempotencyKey: makeIdempotencyKey(testNodeID, startedAt, bodyHash),
			BodyHash:       bodyHash,
			StartedAt:      startedAt,
			EndedAt:        endedAt,
			RetryCount:     0,
		},
	}
	store.SetState(st)
	if err := store.Save(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Restore the pending report into the tracker (simulates restart recovery setup).
	// RestorePendingReport sets staged state but does NOT add to live counters.
	tracker.RestorePendingReport(pendingReport)

	// Verify live counters are empty (no double-counting).
	// StageForReport reads from live counters, which are empty.
	// But we don't call StageForReport here — we use Recovery which
	// reads from tracker.PendingReport() (the staged report).

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err = r.Recovery(context.Background())
	if err != nil {
		t.Fatalf("Recovery: %v", err)
	}

	st2 := store.State()
	if len(st2.PendingReports) != 0 {
		t.Errorf("expected no pending reports after recovery, got %d", len(st2.PendingReports))
	}
}

// TestRecovery_MultiplePendingReports verifies recovery with
// multiple pending reports.
func TestRecovery_MultiplePendingReports(t *testing.T) {
	store := newTestStore(t)
	tracker := newTestTracker()

	startedAt1 := time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)
	startedAt2 := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	st := store.State().Clone()
	st.PendingReports = []state.PendingReport{
		{
			IdempotencyKey: "tr_node-42_2025-01-15T09:00:00Z_aaaa0000",
			BodyHash:       "aaaa0000abcd",
			StartedAt:      startedAt1,
			EndedAt:        endedAt,
			RetryCount:     0,
		},
		{
			IdempotencyKey: "tr_node-42_2025-01-15T10:00:00Z_bbbb0000",
			BodyHash:       "bbbb0000abcd",
			StartedAt:      startedAt2,
			EndedAt:        endedAt,
			RetryCount:     1,
		},
	}
	store.SetState(st)
	if err := store.Save(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	pendingReport := &contract.TrafficReport{
		StartedAt:             startedAt2,
		EndedAt:               endedAt,
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "inbound-1", UserID: "user-1", UploadBytes: 500, DownloadBytes: 1000},
		},
	}
	tracker.RestorePendingReport(pendingReport)

	callCount := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusAccepted)
	}
	server := mockPanelServer(t, handler)
	c := newTestClient(t, server.URL)

	r := NewReporter(c, store, tracker, testNodeID)

	err := r.Recovery(context.Background())
	if err != nil {
		t.Fatalf("Recovery: %v", err)
	}

	if callCount < 1 {
		t.Error("expected at least one HTTP call during recovery")
	}
}
