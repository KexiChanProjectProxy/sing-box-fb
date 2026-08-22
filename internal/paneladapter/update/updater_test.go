package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func newTestUpdater(t *testing.T, version, exe string, execFn ExecFunc) *Updater {
	t.Helper()
	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	u, err := New(Options{
		Store:          store,
		CurrentVersion: version,
		Executable:     exe,
		GOOS:           "linux",
		GOARCH:         "amd64",
		Args:           []string{exe, "-c", "adapter.json"},
		Env:            []string{"PATH=/bin"},
		Exec:           execFn,
		Client:         http.DefaultClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func writeExe(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestApply_SameVersionNoop(t *testing.T) {
	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	var execs int
	u := newTestUpdater(t, "1.0.0", exe, func(string, []string, []string) error {
		execs++
		return nil
	})
	err := u.Apply(context.Background(), &contract.BinaryUpdate{
		Version: "1.0.0",
		URL:     "https://example.com/bin",
		SHA256:  sha256Hex([]byte("x")),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if execs != 0 {
		t.Fatalf("exec called %d times", execs)
	}
}

func TestApply_BlacklistedSkipped(t *testing.T) {
	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	var execs int
	u := newTestUpdater(t, "1.0.0", exe, func(string, []string, []string) error {
		execs++
		return nil
	})
	if err := u.store.UpdateAndSave(func(s *state.State) {
		s.Update.BlacklistedVersions = []string{"2.0.0"}
	}); err != nil {
		t.Fatal(err)
	}
	err := u.Apply(context.Background(), &contract.BinaryUpdate{
		Version: "2.0.0",
		URL:     "https://example.com/bin",
		SHA256:  sha256Hex([]byte("new")),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if execs != 0 {
		t.Fatal("blacklisted version must not exec")
	}
}

func TestApply_SHAMismatch(t *testing.T) {
	payload := []byte("new-binary")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	u := newTestUpdater(t, "1.0.0", exe, func(string, []string, []string) error {
		t.Fatal("must not exec on sha mismatch")
		return nil
	})
	err := u.Apply(context.Background(), &contract.BinaryUpdate{
		Version: "2.0.0",
		URL:     srv.URL + "/bin",
		SHA256:  sha256Hex([]byte("other")),
	}, nil)
	if err == nil {
		t.Fatal("expected sha256 mismatch")
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "old" {
		t.Fatalf("binary changed: %s", got)
	}
}

func TestApply_SuccessReplacesAndExecs(t *testing.T) {
	payload := []byte("new-binary")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	var prepared bool
	var execPath string
	u := newTestUpdater(t, "1.0.0", exe, func(argv0 string, argv []string, env []string) error {
		execPath = argv0
		if argv[0] != argv0 || argv[1] != "-c" {
			t.Errorf("argv=%v", argv)
		}
		return nil
	})
	err := u.Apply(context.Background(), &contract.BinaryUpdate{
		Version: "2.0.0",
		URL:     srv.URL + "/bin",
		SHA256:  sha256Hex(payload),
	}, func() error {
		prepared = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !prepared {
		t.Fatal("prepare was not called")
	}
	if execPath != exe {
		t.Fatalf("exec path %q want %q", execPath, exe)
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "new-binary" {
		t.Fatalf("executable=%s", got)
	}
	st := u.store.State()
	if st.Update.PendingVersion != "2.0.0" {
		t.Fatalf("pending=%q", st.Update.PendingVersion)
	}
	bak, _ := os.ReadFile(st.Update.BackupPath)
	if string(bak) != "old" {
		t.Fatalf("backup=%s", bak)
	}
}

func TestApply_ExecFailureRollsBackAndBlacklists(t *testing.T) {
	payload := []byte("bad-binary")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	u := newTestUpdater(t, "1.0.0", exe, func(string, []string, []string) error {
		return errors.New("exec format error")
	})
	err := u.Apply(context.Background(), &contract.BinaryUpdate{
		Version: "2.0.0",
		URL:     srv.URL + "/bin",
		SHA256:  sha256Hex(payload),
	}, nil)
	if err == nil {
		t.Fatal("expected exec error")
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "old" {
		t.Fatalf("expected rollback to old, got %s", got)
	}
	st := u.store.State()
	if st.Update.PendingVersion != "" {
		t.Fatalf("pending should be cleared, got %q", st.Update.PendingVersion)
	}
	if !st.Update.IsBlacklisted("2.0.0") {
		t.Fatalf("expected 2.0.0 blacklisted: %+v", st.Update.BlacklistedVersions)
	}
}

func TestApply_ArchDownloads(t *testing.T) {
	amd := []byte("amd64-bin")
	arm := []byte("arm64-bin")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/amd64":
			_, _ = w.Write(amd)
		case "/arm64":
			_, _ = w.Write(arm)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	u := newTestUpdater(t, "1.0.0", exe, func(string, []string, []string) error { return nil })
	err := u.Apply(context.Background(), &contract.BinaryUpdate{
		Version: "2.0.0",
		Downloads: map[string]contract.BinaryDownload{
			"linux/amd64": {URL: srv.URL + "/amd64", SHA256: sha256Hex(amd)},
			"linux/arm64": {URL: srv.URL + "/arm64", SHA256: sha256Hex(arm)},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "amd64-bin" {
		t.Fatalf("got %s", got)
	}
}

func TestReconcileStartupAndCommit(t *testing.T) {
	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "new")
	bak := writeExe(t, dir, "adapter.bak", "old")
	u := newTestUpdater(t, "2.0.0", exe, func(string, []string, []string) error { return nil })
	if err := u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = "2.0.0"
		s.Update.BackupPath = bak
	}); err != nil {
		t.Fatal(err)
	}
	trial, err := u.ReconcileStartup()
	if err != nil || !trial {
		t.Fatalf("trial=%v err=%v", trial, err)
	}
	if err := u.CommitSuccess(); err != nil {
		t.Fatal(err)
	}
	st := u.store.State()
	if st.Update.PendingVersion != "" {
		t.Fatal("pending not cleared")
	}
	if _, err := os.Stat(bak); !os.IsNotExist(err) {
		t.Fatalf("backup still present: %v", err)
	}
}

func TestReconcileStartup_AbortedPending(t *testing.T) {
	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "old")
	u := newTestUpdater(t, "1.0.0", exe, func(string, []string, []string) error { return nil })
	if err := u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = "2.0.0"
		s.Update.BackupPath = exe + ".bak"
	}); err != nil {
		t.Fatal(err)
	}
	trial, err := u.ReconcileStartup()
	if err != nil {
		t.Fatal(err)
	}
	if trial {
		t.Fatal("old binary must not be treated as trial")
	}
	if u.store.State().Update.PendingVersion != "" {
		t.Fatal("aborted pending should be cleared")
	}
}

func TestRollbackAndBlacklist_TrialStartFailed(t *testing.T) {
	dir := t.TempDir()
	exe := writeExe(t, dir, "adapter", "new")
	bak := writeExe(t, dir, "adapter.bak", "old")
	var execPath string
	u := newTestUpdater(t, "2.0.0", exe, func(argv0 string, _ []string, _ []string) error {
		execPath = argv0
		return nil
	})
	if err := u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = "2.0.0"
		s.Update.BackupPath = bak
	}); err != nil {
		t.Fatal(err)
	}
	if err := u.RollbackAndBlacklist(errors.New("bootstrap failed")); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "old" {
		t.Fatalf("restored=%s", got)
	}
	st := u.store.State()
	if !st.Update.IsBlacklisted("2.0.0") {
		t.Fatal("version not blacklisted")
	}
	if execPath != exe {
		t.Fatalf("exec path %q", execPath)
	}
}

func TestUnpackGzipAndTar(t *testing.T) {
	inner := []byte("elf-or-script")
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(inner); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := unpackExecutable(gz.Bytes(), "adapter")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, inner) {
		t.Fatalf("gzip unpack: %q", got)
	}

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for _, file := range []struct {
		name string
		body string
	}{
		{"README", "docs"},
		{"adapter", "the-bin"},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: file.name, Mode: 0755, Size: int64(len(file.body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, file.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = unpackExecutable(tarBuf.Bytes(), "adapter")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "the-bin" {
		t.Fatalf("tar unpack: %q", got)
	}
}
