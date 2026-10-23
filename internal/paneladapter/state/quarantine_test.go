package state

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

func TestQuarantineCloneDoesNotMutateOriginalRecords(t *testing.T) {
	original := newState()
	original.QuarantinedReports = []QuarantinedReport{{PendingReport: PendingReport{Report: &contract.TrafficReport{Records: []contract.TrafficRecord{{DownloadBytes: 5639}}}}, Reason: "non_positive_window"}}
	clone := original.Clone()
	clone.QuarantinedReports[0].Reason = "changed"
	clone.QuarantinedReports[0].Report.Records[0].DownloadBytes = 0
	if original.QuarantinedReports[0].Reason != "non_positive_window" || original.QuarantinedReports[0].Report.Records[0].DownloadBytes != 5639 {
		t.Fatal("quarantine clone changed original")
	}
}

func TestUpdateAndSaveFailedQuarantineKeepsOriginalState(t *testing.T) {
	path := testStatePath(t)
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAndSave(func(st *State) { st.PendingReports = []PendingReport{{IdempotencyKey: "keep-original"}} }); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	store.path = filepath.Join(blocker, "state.json")
	move := func(st *State) {
		st.QuarantinedReports = append(st.QuarantinedReports, QuarantinedReport{PendingReport: st.PendingReports[0]})
		st.PendingReports = nil
	}
	if err := store.UpdateAndSave(move); err == nil {
		t.Fatal("expected persistence failure")
	}
	if len(store.State().PendingReports) != 1 || len(store.State().QuarantinedReports) != 0 {
		t.Fatal("failed quarantine published before persistence")
	}
	store.path = path
	if err := store.UpdateAndSave(move); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.State().PendingReports) != 0 || len(reopened.State().QuarantinedReports) != 1 {
		t.Fatal("recovered quarantine missing or duplicated")
	}
}
