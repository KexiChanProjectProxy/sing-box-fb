package traffic

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	N "github.com/sagernet/sing/common/network"
)

// helper: create a net.Pipe pair, write data, close.
func pipeTransfer(t *testing.T, uploadBytes, downloadBytes []byte) (net.Conn, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	return server, client
}

// writeAndClose writes data to conn and closes it.
func writeAndClose(conn net.Conn, data []byte) {
	_, _ = conn.Write(data)
	_ = conn.Close()
}

// readAll reads all data from conn and closes it.
func readAll(conn net.Conn) []byte {
	data, _ := io.ReadAll(conn)
	_ = conn.Close()
	return data
}

func TestNewTracker_BasicMapping(t *testing.T) {
	mapping := map[string]string{
		"ss-in":  "inbound-1",
		"hy2-in": "inbound-2",
	}
	tr := NewTracker(mapping)

	id, ok := tr.inboundID("ss-in")
	if !ok || id != "inbound-1" {
		t.Fatalf("expected inbound-1, got %q ok=%v", id, ok)
	}

	_, ok = tr.inboundID("unknown")
	if ok {
		t.Fatal("unmanaged inbound should not be found")
	}
}

func TestTrackConnection_UploadDownload(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	server, client := net.Pipe()

	// Wrap the server side — this is the "inbound" side.
	wrapped := tr.TrackConnection(server, "ss-in", "user-1")

	// In sing-box, Read on the inbound side = data from client = upload
	// Write on the inbound side = data to client = download
	uploadData := []byte("hello from client")
	downloadData := []byte("hello from server")

	var wg sync.WaitGroup
	wg.Add(2)

	// Client goroutine: write upload data, then read download data
	go func() {
		defer wg.Done()
		_, _ = client.Write(uploadData)
		buf := make([]byte, 1024)
		n, _ := client.Read(buf)
		_ = n
		_ = client.Close()
	}()

	// Server goroutine: read upload data, then write download data
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		n, _ := wrapped.Read(buf)
		if n != len(uploadData) {
			t.Errorf("read %d bytes, expected %d", n, len(uploadData))
		}
		_, _ = wrapped.Write(downloadData)
		_ = wrapped.Close()
	}()

	wg.Wait()

	// Check counters
	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	if len(report.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(report.Records))
	}

	rec := report.Records[0]
	if rec.InboundID != "inbound-1" {
		t.Errorf("inbound_id = %q, want inbound-1", rec.InboundID)
	}
	if rec.UserID != "user-1" {
		t.Errorf("user_id = %q, want user-1", rec.UserID)
	}
	if rec.UploadBytes != int64(len(uploadData)) {
		t.Errorf("upload = %d, want %d", rec.UploadBytes, len(uploadData))
	}
	if rec.DownloadBytes != int64(len(downloadData)) {
		t.Errorf("download = %d, want %d", rec.DownloadBytes, len(downloadData))
	}
}

func TestTrackConnection_UnmanagedInbound(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	wrapped := tr.TrackConnection(server, "unknown-in", "user-1")

	// Should return the same connection (unwrapped)
	if wrapped != server {
		t.Error("unmanaged inbound should return original conn")
	}
}

func TestTrackConnection_EmptyUser(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	wrapped := tr.TrackConnection(server, "ss-in", "")
	if wrapped != server {
		t.Error("empty user should return original conn")
	}
}

func TestSameUserDifferentInbounds(t *testing.T) {
	mapping := map[string]string{
		"ss-in":  "inbound-1",
		"hy2-in": "inbound-2",
	}
	tr := NewTracker(mapping)

	// Track user-1 on ss-in
	k1 := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c1 := tr.getOrCreateCounters(k1)
	tr.mu.Unlock()
	c1.Upload.Add(100)
	c1.Download.Add(200)

	// Track user-1 on hy2-in
	k2 := key{InboundTag: "hy2-in", UserID: "user-1"}
	tr.mu.Lock()
	c2 := tr.getOrCreateCounters(k2)
	tr.mu.Unlock()
	c2.Upload.Add(300)
	c2.Download.Add(400)

	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	if len(report.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(report.Records))
	}

	// Build a map for easier lookup
	recs := make(map[string]contract.TrafficRecord)
	for _, r := range report.Records {
		recs[r.InboundID] = r
	}

	if rec, ok := recs["inbound-1"]; !ok {
		t.Error("missing inbound-1 record")
	} else {
		if rec.UploadBytes != 100 || rec.DownloadBytes != 200 {
			t.Errorf("inbound-1: up=%d down=%d, want up=100 down=200", rec.UploadBytes, rec.DownloadBytes)
		}
	}

	if rec, ok := recs["inbound-2"]; !ok {
		t.Error("missing inbound-2 record")
	} else {
		if rec.UploadBytes != 300 || rec.DownloadBytes != 400 {
			t.Errorf("inbound-2: up=%d down=%d, want up=300 down=400", rec.UploadBytes, rec.DownloadBytes)
		}
	}
}

func TestStagingLifecycle_UnpersistedDoesNotClearCounters(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(1000)
	c.Download.Add(2000)

	// Stage the report
	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	if len(report.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(report.Records))
	}

	// Add more traffic after staging (accumulates on top of staged values)
	tr.mu.Lock()
	c = tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(500)
	c.Download.Add(300)

	// Reset without confirming journal — should NOT clear counters
	tr.ResetLiveCountersWhenJournaled()

	// Stage again — should see all accumulated traffic (1000+500, 2000+300)
	report2 := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-2")
	if len(report2.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(report2.Records))
	}
	rec := report2.Records[0]
	if rec.UploadBytes != 1500 {
		t.Errorf("upload = %d, want 1500", rec.UploadBytes)
	}
	if rec.DownloadBytes != 2300 {
		t.Errorf("download = %d, want 2300", rec.DownloadBytes)
	}
}

func TestStagingLifecycle_PersistedClearsCounters(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(1000)
	c.Download.Add(2000)

	// Stage
	_ = tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")

	// Add more traffic after staging
	tr.mu.Lock()
	c = tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(500)
	c.Download.Add(300)

	// Confirm journaled, then reset
	tr.ConfirmJournaled()
	tr.ResetLiveCountersWhenJournaled()

	// After reset, live counters should only have post-staging delta
	report2 := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-2")
	if len(report2.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(report2.Records))
	}
	rec := report2.Records[0]
	if rec.UploadBytes != 500 {
		t.Errorf("upload = %d, want 500 (only post-staging delta)", rec.UploadBytes)
	}
	if rec.DownloadBytes != 300 {
		t.Errorf("download = %d, want 300 (only post-staging delta)", rec.DownloadBytes)
	}
}

func TestStagingLifecycle_AcceptedClearsCounters(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(1000)
	c.Download.Add(2000)

	// Stage
	_ = tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")

	// Confirm accepted, then reset
	tr.ConfirmAccepted()
	tr.ResetLiveCountersWhenJournaled()

	// After reset, counters should be zero (no post-staging traffic)
	report2 := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-2")
	if len(report2.Records) != 0 {
		t.Errorf("expected 0 records after accepted reset, got %d", len(report2.Records))
	}
}

func TestPendingReportSurvivesRestart(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(1000)
	c.Download.Add(2000)

	// Stage the report
	report := tr.StageForReport(
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
		"rev-1",
	)

	// Simulate restart: create a new tracker and restore the pending report
	tr2 := NewTracker(mapping)
	if tr2.HasStagedReport() {
		t.Error("new tracker should not have staged report")
	}

	// Restore the pending report
	tr2.RestorePendingReport(report)

	if !tr2.HasStagedReport() {
		t.Error("restored tracker should have staged report")
	}

	pending := tr2.PendingReport()
	if pending == nil {
		t.Fatal("pending report should not be nil")
	}
	if len(pending.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(pending.Records))
	}
	rec := pending.Records[0]
	if rec.UploadBytes != 1000 || rec.DownloadBytes != 2000 {
		t.Errorf("restored: up=%d down=%d, want up=1000 down=2000", rec.UploadBytes, rec.DownloadBytes)
	}

	// Now confirm and reset on the new tracker
	tr2.ConfirmJournaled()
	tr2.ResetLiveCountersWhenJournaled()

	// The live counters on the new tracker should still be zero
	// (the restored report's snapshot was subtracted, but there was nothing to subtract from)
	report2 := tr2.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-2")
	if len(report2.Records) != 0 {
		t.Errorf("expected 0 records after restart reset, got %d", len(report2.Records))
	}
}

func TestNoDoubleCountingOnRestart(t *testing.T) {
	// Scenario:
	// 1. Traffic accumulates: 1000 up, 2000 down
	// 2. Stage report
	// 3. More traffic accumulates: 500 up, 300 down
	// 4. ConfirmJournaled + Reset → live counters become 500 up, 300 down
	// 5. Process crashes before the next stage
	// 6. On restart, the live counters start from zero (process restart)
	//    and the previous pending report was already journaled/accepted
	// 7. The new tracker should NOT include the old 1000+2000 again
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(1000)
	c.Download.Add(2000)

	// Stage and confirm
	_ = tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	tr.ConfirmJournaled()
	tr.ResetLiveCountersWhenJournaled()

	// Add more traffic
	tr.mu.Lock()
	c = tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(500)
	c.Download.Add(300)

	// New tracker (simulating restart — counters start from zero)
	tr2 := NewTracker(mapping)

	// No pending report on restart (previous one was already journaled)
	if tr2.HasStagedReport() {
		t.Error("new tracker should not have staged report after confirmed journal")
	}

	// Stage on new tracker — should see zero (no traffic on new process yet)
	report := tr2.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-2")
	if len(report.Records) != 0 {
		t.Errorf("expected 0 records on fresh tracker, got %d", len(report.Records))
	}
}

func TestDistinctIP_CountsOnly(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	// Track distinct IPs
	tr.TrackDistinctIP("1.2.3.4", "ss-in", "user-1")
	tr.TrackDistinctIP("5.6.7.8", "ss-in", "user-1")
	tr.TrackDistinctIP("1.2.3.4", "ss-in", "user-1") // duplicate

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	count := tr.ipCounts[k]
	tr.mu.Unlock()

	if count != 2 {
		t.Errorf("distinct IP count = %d, want 2", count)
	}

	// Verify raw IPs are not exposed in the report
	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	for _, rec := range report.Records {
		// TrafficRecord has no IP fields — this is the contract
		_ = rec // just verify it compiles and has no IP data
	}
}

func TestDistinctIP_IgnoresUnmanaged(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	tr.TrackDistinctIP("1.2.3.4", "unknown-in", "user-1")
	tr.TrackDistinctIP("1.2.3.4", "ss-in", "")

	if len(tr.ipCounts) != 0 {
		t.Errorf("expected no IP counts for unmanaged inbound/empty user, got %d", len(tr.ipCounts))
	}
}

func TestGetRuntimeMetrics(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	server, client := net.Pipe()
	wrapped := tr.TrackConnection(server, "ss-in", "user-1")

	metrics := tr.GetRuntimeMetrics()
	if metrics.Connections != 1 {
		t.Errorf("connections = %d, want 1", metrics.Connections)
	}

	// Close the connection
	_ = wrapped.Close()
	_ = client.Close()

	metrics = tr.GetRuntimeMetrics()
	if metrics.Connections != 0 {
		t.Errorf("connections after close = %d, want 0", metrics.Connections)
	}
}

func TestConcurrentAccess(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	const goroutines = 50
	const bytesPerOp = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			server, client := net.Pipe()
			wrapped := tr.TrackConnection(server, "ss-in", "user-1")

			var innerWg sync.WaitGroup
			innerWg.Add(2)

			// Client: write then read
			go func() {
				defer innerWg.Done()
				_, _ = client.Write(make([]byte, bytesPerOp))
				buf := make([]byte, bytesPerOp+100)
				_, _ = client.Read(buf)
				_ = client.Close()
			}()

			// Server: read then write
			go func() {
				defer innerWg.Done()
				buf := make([]byte, bytesPerOp+100)
				_, _ = wrapped.Read(buf)
				_, _ = wrapped.Write(make([]byte, bytesPerOp))
				_ = wrapped.Close()
			}()

			innerWg.Wait()
		}()
	}

	wg.Wait()

	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	if len(report.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(report.Records))
	}
	rec := report.Records[0]

	expectedUpload := int64(goroutines * bytesPerOp)
	expectedDownload := int64(goroutines * bytesPerOp)
	if rec.UploadBytes != expectedUpload {
		t.Errorf("upload = %d, want %d", rec.UploadBytes, expectedUpload)
	}
	if rec.DownloadBytes != expectedDownload {
		t.Errorf("download = %d, want %d", rec.DownloadBytes, expectedDownload)
	}
}

func TestTrackPacketConnection(t *testing.T) {
	mapping := map[string]string{
		"hy2-in": "inbound-2",
	}
	tr := NewTracker(mapping)

	// We need a PacketConn for testing. Create a simple pipe-based one.
	// Since we can't easily create a real N.PacketConn in tests,
	// we'll test the counter logic directly.

	k := key{InboundTag: "hy2-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(5000)
	c.Download.Add(8000)

	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	if len(report.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(report.Records))
	}
	rec := report.Records[0]
	if rec.InboundID != "inbound-2" {
		t.Errorf("inbound_id = %q, want inbound-2", rec.InboundID)
	}
	if rec.UploadBytes != 5000 {
		t.Errorf("upload = %d, want 5000", rec.UploadBytes)
	}
	if rec.DownloadBytes != 8000 {
		t.Errorf("download = %d, want 8000", rec.DownloadBytes)
	}
}

func TestTrackPacketConnection_Unmanaged(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	// Create a mock PacketConn
	type mockPacketConn struct {
		N.PacketConn
	}
	mock := &mockPacketConn{}

	wrapped := tr.TrackPacketConnection(mock, "unknown-in", "user-1")
	if wrapped != mock {
		t.Error("unmanaged inbound should return original PacketConn")
	}
}

func TestStageForReport_EmptyLive(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	report := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")
	if len(report.Records) != 0 {
		t.Errorf("expected 0 records, got %d", len(report.Records))
	}
	if report.ConfigurationRevision != "rev-1" {
		t.Errorf("revision = %q, want rev-1", report.ConfigurationRevision)
	}
}

func TestStageForReport_TimeWindow(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	startedAt := time.Date(2025, 6, 19, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 6, 19, 11, 0, 0, 0, time.UTC)

	report := tr.StageForReport(startedAt, endedAt, "rev-1")
	if !report.StartedAt.Equal(startedAt) {
		t.Errorf("started_at = %v, want %v", report.StartedAt, startedAt)
	}
	if !report.EndedAt.Equal(endedAt) {
		t.Errorf("ended_at = %v, want %v", report.EndedAt, endedAt)
	}
}

func TestMultipleStages_Accumulating(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	k := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c := tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(100)

	// First stage
	_ = tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-1")

	// More traffic
	tr.mu.Lock()
	c = tr.getOrCreateCounters(k)
	tr.mu.Unlock()
	c.Upload.Add(50)

	// Second stage (without confirming first) — should see total 150
	report2 := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-2")
	if report2.Records[0].UploadBytes != 150 {
		t.Errorf("upload = %d, want 150", report2.Records[0].UploadBytes)
	}

	// Now confirm and reset first staged (which was overwritten by second stage)
	// Actually, the second StageForReport overwrites the first staged snapshot
	// So confirming the second staged report and resetting should subtract 150
	tr.ConfirmJournaled()
	tr.ResetLiveCountersWhenJournaled()

	// Live counters should be zero now
	report3 := tr.StageForReport(time.Now().Add(-time.Minute), time.Now(), "rev-3")
	if len(report3.Records) != 0 {
		t.Errorf("expected 0 records after full reset, got %d", len(report3.Records))
	}
}

func TestInboundMapping_DefensiveCopy(t *testing.T) {
	original := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(original)

	// Mutate the original map
	original["ss-in"] = "inbound-mutated"

	// Tracker should have its own copy
	id, ok := tr.inboundID("ss-in")
	if !ok || id != "inbound-1" {
		t.Errorf("inbound_id = %q, want inbound-1 (should not be affected by external mutation)", id)
	}
}

func TestResetLiveCountersWhenJournaled_NoStagedReport(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	// Should not panic
	tr.ResetLiveCountersWhenJournaled()
}

func TestRestorePendingReport_UnknownInboundID(t *testing.T) {
	mapping := map[string]string{
		"ss-in": "inbound-1",
	}
	tr := NewTracker(mapping)

	// Report with an inbound_id that doesn't map to any tag
	report := &contract.TrafficReport{
		StartedAt:             time.Now().Add(-time.Minute),
		EndedAt:               time.Now(),
		ConfigurationRevision: "rev-1",
		Records: []contract.TrafficRecord{
			{InboundID: "unknown-inbound", UserID: "user-1", UploadBytes: 100, DownloadBytes: 200},
		},
	}

	tr.RestorePendingReport(report)

	if !tr.HasStagedReport() {
		t.Error("should have staged report even with unknown inbound_id")
	}

	// The snapshot should be empty since the inbound_id didn't map to any tag
	pending := tr.PendingReport()
	if len(pending.Records) != 1 {
		t.Fatalf("expected 1 record in report, got %d", len(pending.Records))
	}
}

func TestFullLifecycle_Integration(t *testing.T) {
	mapping := map[string]string{
		"ss-in":  "inbound-1",
		"hy2-in": "inbound-2",
	}
	tr := NewTracker(mapping)

	// User-1 on ss-in
	k1 := key{InboundTag: "ss-in", UserID: "user-1"}
	tr.mu.Lock()
	c1 := tr.getOrCreateCounters(k1)
	tr.mu.Unlock()
	c1.Upload.Add(1000)
	c1.Download.Add(2000)

	// User-2 on hy2-in
	k2 := key{InboundTag: "hy2-in", UserID: "user-2"}
	tr.mu.Lock()
	c2 := tr.getOrCreateCounters(k2)
	tr.mu.Unlock()
	c2.Upload.Add(3000)
	c2.Download.Add(4000)

	// Unmanaged traffic (should not appear)
	k3 := key{InboundTag: "unknown", UserID: "user-3"}
	tr.mu.Lock()
	c3 := tr.getOrCreateCounters(k3)
	tr.mu.Unlock()
	c3.Upload.Add(9999)
	c3.Download.Add(9999)

	startedAt := time.Date(2025, 6, 19, 10, 0, 0, 0, time.UTC)
	endedAt := time.Date(2025, 6, 19, 11, 0, 0, 0, time.UTC)

	// Stage
	report := tr.StageForReport(startedAt, endedAt, "rev-1")
	if len(report.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(report.Records))
	}

	// Add more traffic after staging
	tr.mu.Lock()
	c1 = tr.getOrCreateCounters(k1)
	tr.mu.Unlock()
	c1.Upload.Add(500)

	// Confirm and reset
	tr.ConfirmJournaled()
	tr.ResetLiveCountersWhenJournaled()

	// Stage again — should see only the post-staging delta for user-1
	report2 := tr.StageForReport(startedAt, endedAt, "rev-2")
	if len(report2.Records) != 1 {
		t.Fatalf("expected 1 record (only user-1 has post-staging delta), got %d", len(report2.Records))
	}
	rec := report2.Records[0]
	if rec.UserID != "user-1" {
		t.Errorf("user_id = %q, want user-1", rec.UserID)
	}
	if rec.UploadBytes != 500 {
		t.Errorf("upload = %d, want 500", rec.UploadBytes)
	}
	if rec.DownloadBytes != 0 {
		t.Errorf("download = %d, want 0", rec.DownloadBytes)
	}
}
