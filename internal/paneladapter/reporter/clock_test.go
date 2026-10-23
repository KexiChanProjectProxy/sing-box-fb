package reporter

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
)

func addClockTestBytes(t *testing.T, tracker *traffic.Tracker, count int64) {
	t.Helper()
	local, peer := net.Pipe()
	tracked := tracker.TrackConnection(local, "ss-in", "user-1")
	go func() { defer peer.Close(); _, _ = io.CopyN(io.Discard, peer, count) }()
	if _, err := tracked.Write(make([]byte, count)); err != nil {
		t.Fatal(err)
	}
	_ = tracked.Close()
}

func TestClockJumpPreservesOriginalBytesAndResumesReporting(t *testing.T) {
	for _, jump := range []time.Duration{-26 * time.Second, 0, 6 * time.Minute} {
		t.Run(jump.String(), func(t *testing.T) {
			path := testStatePath(t)
			store, err := state.NewStore(path)
			if err != nil {
				t.Fatal(err)
			}
			tracker := newTestTracker()
			var sent int64
			server := mockPanelServer(t, func(w http.ResponseWriter, req *http.Request) {
				var report contract.TrafficReport
				if err := json.NewDecoder(req.Body).Decode(&report); err != nil {
					t.Error(err)
				}
				if invalidWindowReason(&report) != "" {
					t.Error("sent invalid report")
				}
				for _, record := range report.Records {
					sent += record.DownloadBytes
				}
				w.WriteHeader(http.StatusCreated)
			})
			r := NewReporter(newTestClient(t, server.URL), store, tracker, testNodeID)
			start := time.Date(2026, 10, 18, 2, 38, 27, 0, time.UTC)
			r.reportStartTime = start
			now := start.Add(jump)
			r.now = func() time.Time { return now }
			addClockTestBytes(t, tracker, 5639)
			if err := r.ReportNow(context.Background()); err == nil {
				t.Fatal("expected quarantine warning")
			}
			if sent != 0 || len(store.State().PendingReports) != 0 {
				t.Fatal("invalid report was sent or blocked queue")
			}
			quarantined := store.State().QuarantinedReports
			if len(quarantined) != 1 || !quarantined[0].Report.StartedAt.Equal(start) || !quarantined[0].Report.EndedAt.Equal(now) || quarantined[0].Report.Records[0].DownloadBytes != 5639 {
				t.Fatal("original timestamps or bytes were lost")
			}
			now = now.Add(time.Minute)
			addClockTestBytes(t, tracker, 4096)
			if err := r.ReportNow(context.Background()); err != nil {
				t.Fatal(err)
			}
			if sent != 4096 || len(store.State().QuarantinedReports) != 1 {
				t.Fatalf("incorrect fresh metering: %d", sent)
			}
			reopened, err := state.NewStore(path)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(reopened.State().QuarantinedReports, store.State().QuarantinedReports) {
				t.Fatal("quarantine was not durable")
			}
		})
	}
}

func TestRecoveryQuarantinesInvalidWindowWithoutRewritingOrBlocking(t *testing.T) {
	store := newTestStore(t)
	start := time.Date(2026, 10, 18, 2, 38, 27, 0, time.UTC)
	bad := &contract.TrafficReport{StartedAt: start, EndedAt: start.Add(-26 * time.Second), Records: []contract.TrafficRecord{{InboundID: "inbound-1", UserID: "user-1", DownloadBytes: 5675}}}
	hash, _ := computeBodyHash(bad)
	original := state.PendingReport{IdempotencyKey: "original-key", BodyHash: hash, StartedAt: bad.StartedAt, EndedAt: bad.EndedAt, RetryCount: 7702, Report: bad}
	if err := store.UpdateAndSave(func(st *state.State) { st.PendingReports = []state.PendingReport{original} }); err != nil {
		t.Fatal(err)
	}
	var calls int
	server := mockPanelServer(t, func(w http.ResponseWriter, req *http.Request) { calls++; w.WriteHeader(http.StatusCreated) })
	r := NewReporter(newTestClient(t, server.URL), store, newTestTracker(), testNodeID)
	if err := r.Recovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || len(store.State().PendingReports) != 0 || len(store.State().QuarantinedReports) != 1 || !reflect.DeepEqual(store.State().QuarantinedReports[0].PendingReport, original) {
		t.Fatal("poison report was lost, rewritten, or retried")
	}
	if err := r.Recovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.State().QuarantinedReports) != 1 {
		t.Fatal("quarantine duplicated on recovery")
	}
	addClockTestBytes(t, r.tracker, 4096)
	if err := r.ReportNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("fresh traffic still blocked")
	}
}
