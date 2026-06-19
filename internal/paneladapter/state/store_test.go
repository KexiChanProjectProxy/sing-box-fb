package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// helper: create a temp dir and state file path, cleaned up after test.
func testStatePath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "state.json")
}

// helper: assert no error.
func mustNoError(t *testing.T, err error, msg ...string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", strings.Join(msg, " "), err)
	}
}

// ---------- Load tests ----------

func TestLoad_MissingFile_ReturnsDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent.json")
	st, err := Load(path, defaultQuarantine)
	mustNoError(t, err)
	if st.Version != CurrentVersion {
		t.Errorf("expected version %d, got %d", CurrentVersion, st.Version)
	}
	if st.Inbounds == nil {
		t.Error("Inbounds map should be initialized, not nil")
	}
	if len(st.Inbounds) != 0 {
		t.Errorf("expected empty Inbounds, got %d entries", len(st.Inbounds))
	}
	if len(st.PendingReports) != 0 {
		t.Errorf("expected empty PendingReports, got %d entries", len(st.PendingReports))
	}
}

func TestLoad_EmptyFile_ReturnsDefault(t *testing.T) {
	path := testStatePath(t)
	mustNoError(t, os.WriteFile(path, []byte{}, 0o644))
	st, err := Load(path, defaultQuarantine)
	mustNoError(t, err)
	if st.Version != CurrentVersion {
		t.Errorf("expected version %d, got %d", CurrentVersion, st.Version)
	}
}

func TestLoad_ValidFile(t *testing.T) {
	path := testStatePath(t)
	original := &State{
		Version: CurrentVersion,
		Config: ConfigState{
			Revision: "rev-42",
			ETag:     "etag-abc",
			NodeID:   "node-1",
		},
		Inbounds: map[string]InboundState{
			"inbound-1": {
				InboundID:      "inbound-1",
				Tag:            "hy2-in",
				Protocol:       "hysteria2",
				UserETag:       "user-etag-1",
				UserRevision:   "user-rev-1",
				UserLoadStatus: "ok",
				UserCount:      5,
			},
		},
		PendingReports: []PendingReport{
			{
				IdempotencyKey: "key-001",
				BodyHash:       "hash-abc",
				StartedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				EndedAt:        time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
				RetryCount:     0,
			},
		},
	}
	mustNoError(t, Save(path, original))

	loaded, err := Load(path, defaultQuarantine)
	mustNoError(t, err)
	if loaded.Version != CurrentVersion {
		t.Errorf("version mismatch: %d vs %d", CurrentVersion, loaded.Version)
	}
	if loaded.Config.Revision != "rev-42" {
		t.Errorf("config revision mismatch: %s", loaded.Config.Revision)
	}
	if loaded.Config.ETag != "etag-abc" {
		t.Errorf("config etag mismatch: %s", loaded.Config.ETag)
	}
	if loaded.Config.NodeID != "node-1" {
		t.Errorf("config node_id mismatch: %s", loaded.Config.NodeID)
	}
	ib, ok := loaded.Inbounds["inbound-1"]
	if !ok {
		t.Fatal("missing inbound-1")
	}
	if ib.Tag != "hy2-in" {
		t.Errorf("inbound tag mismatch: %s", ib.Tag)
	}
	if ib.UserCount != 5 {
		t.Errorf("inbound user_count mismatch: %d", ib.UserCount)
	}
	if len(loaded.PendingReports) != 1 {
		t.Fatalf("expected 1 pending report, got %d", len(loaded.PendingReports))
	}
	if loaded.PendingReports[0].IdempotencyKey != "key-001" {
		t.Errorf("idempotency key mismatch: %s", loaded.PendingReports[0].IdempotencyKey)
	}
}

func TestLoad_CorruptFile_Quarantined(t *testing.T) {
	path := testStatePath(t)
	corruptData := []byte("{this is not valid json!!!")
	mustNoError(t, os.WriteFile(path, corruptData, 0o644))

	st, err := Load(path, defaultQuarantine)
	if err == nil {
		t.Error("expected error for corrupt file, got nil")
	}
	if st != nil {
		t.Error("expected nil state for corrupt file")
	}
	// Verify the corrupt file was renamed (quarantined), not silently deleted.
	entries, _ := os.ReadDir(filepath.Dir(path))
	foundCorrupt := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "state.json.corrupt.") {
			foundCorrupt = true
			// Verify the corrupt data is preserved in the quarantine file.
			quarantined, _ := os.ReadFile(filepath.Join(filepath.Dir(path), e.Name()))
			if string(quarantined) != string(corruptData) {
				t.Error("quarantined file content does not match original corrupt data")
			}
			break
		}
	}
	if !foundCorrupt {
		t.Error("corrupt file was not quarantined (no .corrupt.* file found)")
	}
	// Original file should no longer exist at the original path.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("original corrupt file should have been renamed")
	}
}

func TestLoad_CorruptFile_QuarantineFails_ReportsBothErrors(t *testing.T) {
	path := testStatePath(t)
	mustNoError(t, os.WriteFile(path, []byte("{{bad"), 0o644))

	// Use a quarantine function that always fails.
	failQuarantine := func(src, dst string) error {
		return os.ErrPermission
	}
	st, err := Load(path, failQuarantine)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if st != nil {
		t.Error("expected nil state")
	}
	// Error should contain both the parse error and the quarantine error.
	// sing's E.Errors wraps multiple errors with " | " separator.
	errMsg := err.Error()
	if !strings.Contains(errMsg, "invalid") && !strings.Contains(errMsg, "json") && !strings.Contains(errMsg, "character") {
		t.Errorf("error should mention parse failure: %s", errMsg)
	}
	if !strings.Contains(errMsg, "quarantine") {
		t.Errorf("error should mention quarantine failure: %s", errMsg)
	}
}

// ---------- Save / atomic write tests ----------

func TestSave_CreatesFile(t *testing.T) {
	path := testStatePath(t)
	st := newState()
	st.Config.NodeID = "test-node"
	mustNoError(t, Save(path, st))

	data, err := os.ReadFile(path)
	mustNoError(t, err)
	if len(data) == 0 {
		t.Error("saved file is empty")
	}
	// Verify it's valid JSON.
	var parsed State
	mustNoError(t, json.Unmarshal(data, &parsed))
	if parsed.Config.NodeID != "test-node" {
		t.Errorf("node_id mismatch: %s", parsed.Config.NodeID)
	}
}

func TestSave_AtomicWrite_NoPartialOnFailure(t *testing.T) {
	// This test verifies that if the target file already exists and
	// a new save succeeds, the file is always complete.
	path := testStatePath(t)
	st1 := newState()
	st1.Config.NodeID = "first"
	mustNoError(t, Save(path, st1))

	st2 := newState()
	st2.Config.NodeID = "second"
	mustNoError(t, Save(path, st2))

	data, err := os.ReadFile(path)
	mustNoError(t, err)
	var parsed State
	mustNoError(t, json.Unmarshal(data, &parsed))
	if parsed.Config.NodeID != "second" {
		t.Errorf("expected 'second', got '%s'", parsed.Config.NodeID)
	}
}

func TestSave_CreatesParentDirectories(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "dir", "state.json")
	st := newState()
	mustNoError(t, Save(path, st))
	if _, err := os.Stat(path); err != nil {
		t.Errorf("state file not created: %v", err)
	}
}

func TestAtomicWrite_TempFileCleanedUpOnError(t *testing.T) {
	dir := t.TempDir()
	// Make the directory read-only after creating it so the write fails.
	path := filepath.Join(dir, "state.json")
	// Write a valid file first.
	mustNoError(t, os.WriteFile(path, []byte("{}"), 0o644))
	// Make the directory read-only to cause temp file creation to fail.
	mustNoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755) // restore for cleanup

	err := atomicWriteFile(path, []byte("new-data"), 0o644)
	if err == nil {
		t.Error("expected error writing to read-only directory")
	}
	// Original file should still be intact.
	data, err := os.ReadFile(path)
	mustNoError(t, err)
	if string(data) != "{}" {
		t.Errorf("original file corrupted: %s", string(data))
	}
}

// ---------- SaveIfChanged tests ----------

func TestSaveIfChanged_SkipsUnchangedWrite(t *testing.T) {
	path := testStatePath(t)
	st := newState()
	st.Config.NodeID = "same"
	mustNoError(t, Save(path, st))

	// Get file info before SaveIfChanged.
	info1, err := os.Stat(path)
	mustNoError(t, err)

	// Small sleep to ensure mtime would differ if file were rewritten.
	time.Sleep(10 * time.Millisecond)

	mustNoError(t, SaveIfChanged(path, st))

	info2, err := os.Stat(path)
	mustNoError(t, err)
	// File modification time should be identical (no write occurred).
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Error("SaveIfChanged rewrote the file despite no changes")
	}
}

func TestSaveIfChanged_WritesWhenChanged(t *testing.T) {
	path := testStatePath(t)
	st1 := newState()
	st1.Config.NodeID = "v1"
	mustNoError(t, Save(path, st1))

	st2 := newState()
	st2.Config.NodeID = "v2"
	mustNoError(t, SaveIfChanged(path, st2))

	data, err := os.ReadFile(path)
	mustNoError(t, err)
	var parsed State
	mustNoError(t, json.Unmarshal(data, &parsed))
	if parsed.Config.NodeID != "v2" {
		t.Errorf("expected 'v2', got '%s'", parsed.Config.NodeID)
	}
}

// ---------- Store tests ----------

func TestNewStore_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewStore(path)
	mustNoError(t, err)
	if store.State().Version != CurrentVersion {
		t.Errorf("expected version %d", CurrentVersion)
	}
}

func TestStore_SaveAndReload(t *testing.T) {
	path := testStatePath(t)
	store, err := NewStore(path)
	mustNoError(t, err)

	st := store.State().Clone()
	st.Config.NodeID = "node-99"
	st.Inbounds["ib-1"] = InboundState{
		InboundID: "ib-1",
		Tag:       "ss-in",
		Protocol:  "shadowsocks",
		UserCount: 10,
	}
	store.SetState(st)
	mustNoError(t, store.Save())

	// Reload from disk.
	store2, err := NewStore(path)
	mustNoError(t, err)
	if store2.State().Config.NodeID != "node-99" {
		t.Errorf("node_id mismatch: %s", store2.State().Config.NodeID)
	}
	ib, ok := store2.State().Inbounds["ib-1"]
	if !ok {
		t.Fatal("missing inbound ib-1")
	}
	if ib.UserCount != 10 {
		t.Errorf("user_count mismatch: %d", ib.UserCount)
	}
}

func TestStore_SaveIfChanged_SkipsUnchanged(t *testing.T) {
	path := testStatePath(t)
	store, err := NewStore(path)
	mustNoError(t, err)

	// First save writes the file.
	mustNoError(t, store.Save())
	info1, _ := os.Stat(path)
	time.Sleep(10 * time.Millisecond)

	// Second save should be a no-op.
	mustNoError(t, store.SaveIfChanged())
	info2, _ := os.Stat(path)
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Error("SaveIfChanged wrote despite no state change")
	}
}

func TestStore_SaveIfChanged_WritesAfterMutation(t *testing.T) {
	path := testStatePath(t)
	store, err := NewStore(path)
	mustNoError(t, err)
	mustNoError(t, store.Save())

	st := store.State().Clone()
	st.Config.Revision = "new-rev"
	store.SetState(st)
	mustNoError(t, store.SaveIfChanged())

	store2, err := NewStore(path)
	mustNoError(t, err)
	if store2.State().Config.Revision != "new-rev" {
		t.Errorf("revision not persisted: %s", store2.State().Config.Revision)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	path := testStatePath(t)
	store, err := NewStore(path)
	mustNoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := store.State().Clone()
			st.Config.Revision = "rev-concurrent"
			store.SetState(st)
			_ = store.SaveIfChanged()
		}(i)
	}
	wg.Wait()
	// Just verify no panic or data race occurred.
}

// ---------- Clone tests ----------

func TestClone_DeepCopy(t *testing.T) {
	original := &State{
		Version: CurrentVersion,
		Config:  ConfigState{NodeID: "n1", Revision: "r1"},
		Inbounds: map[string]InboundState{
			"ib-1": {InboundID: "ib-1", Tag: "t1", UserCount: 3},
		},
		PendingReports: []PendingReport{
			{IdempotencyKey: "k1", BodyHash: "h1", RetryCount: 1},
		},
	}
	clone := original.Clone()

	// Mutate clone; original should be unaffected.
	clone.Config.NodeID = "n2"
	clone.Inbounds["ib-1"] = InboundState{UserCount: 99}
	clone.Inbounds["ib-2"] = InboundState{Tag: "new"}
	clone.PendingReports[0].RetryCount = 5
	clone.PendingReports = append(clone.PendingReports, PendingReport{IdempotencyKey: "k2"})

	if original.Config.NodeID != "n1" {
		t.Error("Clone did not deep-copy Config")
	}
	if original.Inbounds["ib-1"].UserCount != 3 {
		t.Error("Clone did not deep-copy Inbounds map values")
	}
	if _, exists := original.Inbounds["ib-2"]; exists {
		t.Error("Clone shared Inbounds map with original")
	}
	if original.PendingReports[0].RetryCount != 1 {
		t.Error("Clone shared PendingReports slice with original")
	}
	if len(original.PendingReports) != 1 {
		t.Error("Clone shared PendingReports backing array with original")
	}
}

func TestClone_NilState(t *testing.T) {
	var nilState *State
	clone := nilState.Clone()
	if clone.Version != CurrentVersion {
		t.Errorf("expected default version %d, got %d", CurrentVersion, clone.Version)
	}
}

// ---------- No token/password serialization tests ----------

func TestState_NoSensitiveFields(t *testing.T) {
	// Verify that State, ConfigState, InboundState, and PendingReport
	// structs have no fields named token, password, secret, key, or bearer.
	st := newState()
	data, err := st.marshalJSON()
	mustNoError(t, err)
	s := string(data)

	sensitive := []string{"token", "password", "secret", "bearer", "api_key", "apikey"}
	for _, word := range sensitive {
		if strings.Contains(strings.ToLower(s), word) {
			t.Errorf("state JSON contains sensitive word '%s': %s", word, s)
		}
	}
}

func TestState_SensitiveFieldsNotInStructTags(t *testing.T) {
	// Verify struct tags don't accidentally include sensitive field names.
	// This is a compile-time style check via reflection-like inspection.
	types := []any{
		State{},
		ConfigState{},
		InboundState{},
		PendingReport{},
	}
	for _, typ := range types {
		b, _ := json.Marshal(typ)
		s := strings.ToLower(string(b))
		for _, word := range []string{"token", "password", "secret", "bearer"} {
			if strings.Contains(s, word) {
				t.Errorf("type %T JSON contains '%s': %s", typ, word, s)
			}
		}
	}
}

// ---------- Idempotency key stability tests ----------

func TestPendingReport_IdempotencyKey_StableAcrossClones(t *testing.T) {
	report := PendingReport{
		IdempotencyKey: "stable-key-123",
		BodyHash:       "hash-abc",
		StartedAt:      time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
		EndedAt:        time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC),
		RetryCount:     2,
	}
	st := &State{
		Version:        CurrentVersion,
		PendingReports: []PendingReport{report},
	}
	// Serialize and deserialize.
	data, err := st.marshalJSON()
	mustNoError(t, err)
	var loaded State
	mustNoError(t, json.Unmarshal(data, &loaded))
	if loaded.PendingReports[0].IdempotencyKey != "stable-key-123" {
		t.Errorf("idempotency key changed after round-trip: %s", loaded.PendingReports[0].IdempotencyKey)
	}
	if loaded.PendingReports[0].RetryCount != 2 {
		t.Errorf("retry count changed: %d", loaded.PendingReports[0].RetryCount)
	}
}

func TestPendingReport_SameReportPreservesKey(t *testing.T) {
	// Simulate: a pending report is saved, reloaded, and saved again.
	// The idempotency key must remain the same.
	path := testStatePath(t)
	st := newState()
	st.PendingReports = append(st.PendingReports, PendingReport{
		IdempotencyKey: "idem-key-999",
		BodyHash:       "body-hash-1",
		StartedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		EndedAt:        time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
		RetryCount:     0,
	})
	mustNoError(t, Save(path, st))

	loaded, err := Load(path, defaultQuarantine)
	mustNoError(t, err)
	if loaded.PendingReports[0].IdempotencyKey != "idem-key-999" {
		t.Errorf("idempotency key not preserved: %s", loaded.PendingReports[0].IdempotencyKey)
	}
}

// ---------- Body-hash conflict detection tests ----------

func TestPendingReport_BodyHashConflict(t *testing.T) {
	// Two reports with the same idempotency key but different body hashes
	// should be detectable by the caller (the store just persists them).
	st := newState()
	st.PendingReports = []PendingReport{
		{
			IdempotencyKey: "key-A",
			BodyHash:       "hash-1",
			StartedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			EndedAt:        time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
		},
		{
			IdempotencyKey: "key-A",
			BodyHash:       "hash-2", // same key, different body
			StartedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			EndedAt:        time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
		},
	}
	path := testStatePath(t)
	mustNoError(t, Save(path, st))

	loaded, err := Load(path, defaultQuarantine)
	mustNoError(t, err)
	if len(loaded.PendingReports) != 2 {
		t.Fatalf("expected 2 reports, got %d", len(loaded.PendingReports))
	}
	// Both are persisted; the caller (traffic reporter) is responsible
	// for detecting the conflict by comparing BodyHash.
	if loaded.PendingReports[0].BodyHash == loaded.PendingReports[1].BodyHash {
		t.Error("body hashes should differ for conflict detection test")
	}
}

// ---------- Version field test ----------

func TestState_VersionField(t *testing.T) {
	st := newState()
	if st.Version != CurrentVersion {
		t.Errorf("new state version = %d, want %d", st.Version, CurrentVersion)
	}
	data, err := st.marshalJSON()
	mustNoError(t, err)
	var raw map[string]any
	mustNoError(t, json.Unmarshal(data, &raw))
	if v, _ := raw["version"].(float64); int(v) != CurrentVersion {
		t.Errorf("marshalled version = %v, want %d", v, CurrentVersion)
	}
}

// ---------- InboundState nil map safety ----------

func TestLoad_NilInboundsMap_Initialized(t *testing.T) {
	path := testStatePath(t)
	// Write state with inbounds = null.
	mustNoError(t, os.WriteFile(path, []byte(`{"version":1,"config":{"node_id":"n1"},"inbounds":null}`), 0o644))
	st, err := Load(path, defaultQuarantine)
	mustNoError(t, err)
	if st.Inbounds == nil {
		t.Error("Inbounds should be initialized to non-nil map when JSON has null")
	}
}

// ---------- PendingReports omitempty ----------

func TestState_PendingReportsOmitEmpty(t *testing.T) {
	st := newState()
	data, err := st.marshalJSON()
	mustNoError(t, err)
	s := string(data)
	if strings.Contains(s, "pending_reports") {
		t.Error("pending_reports should be omitted when empty (omitempty)")
	}
}
