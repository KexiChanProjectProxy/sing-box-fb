// Package update downloads a panel-pushed node binary, atomically replaces
// the running executable, and rolls back + blacklists the version if start fails.
package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

const (
	maxBinaryBytes    = 256 << 20
	defaultHTTPTimeout = 10 * time.Minute
	backupSuffix      = ".bak"
	stagingSuffix     = ".new"
)

// ExecFunc replaces the current process. Production uses syscall.Exec.
type ExecFunc func(argv0 string, argv []string, env []string) error

// Updater applies panel-pushed binary updates with rollback.
type Updater struct {
	mu              sync.Mutex
	store           *state.Store
	logger          log.ContextLogger
	client          *http.Client
	currentVersion  string
	executable      string
	goos            string
	goarch          string
	args            []string
	env             []string
	exec            ExecFunc
}

// Options configures a new Updater.
type Options struct {
	Store          *state.Store
	Logger         log.ContextLogger
	CurrentVersion string
	Executable     string
	Client         *http.Client
	GOOS           string
	GOARCH         string
	Args           []string
	Env            []string
	Exec           ExecFunc
}

func New(opts Options) (*Updater, error) {
	if opts.Store == nil {
		return nil, E.New("store is required")
	}
	exe := opts.Executable
	if exe == "" {
		var err error
		exe, err = os.Executable()
		if err != nil {
			return nil, E.Cause(err, "resolve executable")
		}
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	goos := opts.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	goarch := opts.GOARCH
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	execFn := opts.Exec
	if execFn == nil {
		execFn = syscall.Exec
	}
	args := opts.Args
	if len(args) == 0 {
		args = os.Args
	}
	env := opts.Env
	if env == nil {
		env = os.Environ()
	}
	return &Updater{
		store:          opts.Store,
		logger:         opts.Logger,
		client:         client,
		currentVersion: opts.CurrentVersion,
		executable:     exe,
		goos:           goos,
		goarch:         goarch,
		args:           args,
		env:            env,
		exec:           execFn,
	}, nil
}

// ReconcileStartup clears an aborted pending update, or reports that this
// process is the trial start of a newly installed version.
func (u *Updater) ReconcileStartup() (trial bool, err error) {
	st := u.store.State()
	pending := st.Update.PendingVersion
	if pending == "" {
		return false, nil
	}
	if pending == u.currentVersion {
		u.logInfo("trial start of binary version ", pending)
		return true, nil
	}
	u.logWarn("pending binary update ", pending, " aborted; still running ", u.currentVersion)
	return false, u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = ""
		s.Update.BackupPath = ""
	})
}

// CommitSuccess clears pending update state after a successful trial start.
func (u *Updater) CommitSuccess() error {
	st := u.store.State()
	backup := st.Update.BackupPath
	if err := u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = ""
		s.Update.BackupPath = ""
	}); err != nil {
		return err
	}
	if backup != "" {
		_ = os.Remove(backup)
	}
	u.logInfo("binary update committed version=", u.currentVersion)
	return nil
}

// RollbackAndBlacklist restores the backup binary, blacklists version, and execs
// the restored file. Used when the new binary fails to start.
func (u *Updater) RollbackAndBlacklist(startErr error) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	st := u.store.State()
	version := st.Update.PendingVersion
	if version == "" {
		version = u.currentVersion
	}
	u.logWarn("binary start failed version=", version, " err=", startErr)
	if err := u.restoreBackupLocked(version); err != nil {
		return err
	}
	return u.execRestoredLocked()
}

// Apply downloads and installs rel when it is newer than the running version
// and not blacklisted. prepare is invoked after the artifact is staged and
// before the executable is replaced (flush traffic, close box).
func (u *Updater) Apply(ctx context.Context, rel *contract.BinaryUpdate, prepare func() error) error {
	if rel == nil {
		return nil
	}
	if err := rel.Validate(); err != nil {
		return err
	}
	if rel.Version == u.currentVersion {
		return nil
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.store.State().Update.IsBlacklisted(rel.Version) {
		u.logInfo("skip blacklisted binary version ", rel.Version)
		return nil
	}

	dl, err := resolveDownload(rel, u.goos, u.goarch)
	if err != nil {
		return err
	}

	u.logInfo("downloading binary version=", rel.Version, " url=", redactDownloadURL(dl.URL))
	payload, err := u.download(ctx, dl.URL)
	if err != nil {
		return err
	}
	if err := verifySHA256(payload, dl.SHA256); err != nil {
		return err
	}
	unpacked, err := unpackExecutable(payload, filepath.Base(u.executable))
	if err != nil {
		return err
	}

	staging := u.executable + stagingSuffix
	backup := u.executable + backupSuffix
	if err := writeFileAtomic(staging, unpacked, 0o755); err != nil {
		return E.Cause(err, "write staged binary")
	}
	defer os.Remove(staging)

	if err := copyFile(u.executable, backup); err != nil {
		return E.Cause(err, "backup current binary")
	}
	if err := u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = rel.Version
		s.Update.BackupPath = backup
	}); err != nil {
		return E.Cause(err, "record pending binary update")
	}

	if prepare != nil {
		if err := prepare(); err != nil {
			_ = u.store.UpdateAndSave(func(s *state.State) {
				s.Update.PendingVersion = ""
				s.Update.BackupPath = ""
			})
			return E.Cause(err, "prepare for binary exec")
		}
	}

	if err := os.Rename(staging, u.executable); err != nil {
		_ = u.restoreBackupLocked("")
		return E.Cause(err, "replace executable")
	}

	argv := execArgs(u.executable, u.args)
	u.logInfo("exec new binary version=", rel.Version)
	if err := u.exec(u.executable, argv, u.env); err != nil {
		if rbErr := u.restoreBackupLocked(rel.Version); rbErr != nil {
			return E.Errors(E.Cause(err, "exec new binary"), rbErr)
		}
		return E.Cause(err, "exec new binary")
	}
	return nil
}

func (u *Updater) restoreBackupLocked(blacklist string) error {
	st := u.store.State()
	backup := st.Update.BackupPath
	if backup == "" {
		backup = u.executable + backupSuffix
	}
	if _, err := os.Stat(backup); err != nil {
		return E.Cause(err, "missing backup binary")
	}
	if err := os.Rename(backup, u.executable); err != nil {
		if copyErr := copyFile(backup, u.executable); copyErr != nil {
			return E.Errors(E.Cause(err, "restore backup"), copyErr)
		}
	}
	if err := os.Chmod(u.executable, 0o755); err != nil {
		u.logWarn("chmod restored binary: ", err)
	}
	return u.store.UpdateAndSave(func(s *state.State) {
		s.Update.PendingVersion = ""
		s.Update.BackupPath = ""
		if blacklist != "" && !s.Update.IsBlacklisted(blacklist) {
			s.Update.BlacklistedVersions = append(s.Update.BlacklistedVersions, blacklist)
		}
	})
}

func (u *Updater) execRestoredLocked() error {
	argv := execArgs(u.executable, u.args)
	u.logInfo("exec rolled-back binary version=", u.currentVersion)
	if err := u.exec(u.executable, argv, u.env); err != nil {
		return E.Cause(err, "exec rolled-back binary")
	}
	return nil
}

func (u *Updater) download(ctx context.Context, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return nil, E.Cause(err, "download binary")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, E.New("download binary: unexpected status ", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBinaryBytes+1))
	if err != nil {
		return nil, E.Cause(err, "read binary")
	}
	if len(data) > maxBinaryBytes {
		return nil, E.New("binary exceeds ", maxBinaryBytes, " bytes")
	}
	if len(data) == 0 {
		return nil, E.New("empty binary download")
	}
	return data, nil
}

func (u *Updater) logInfo(args ...any) {
	if u.logger != nil {
		u.logger.Info(args...)
	}
}

func (u *Updater) logWarn(args ...any) {
	if u.logger != nil {
		u.logger.Warn(args...)
	}
}

func resolveDownload(rel *contract.BinaryUpdate, goos, goarch string) (contract.BinaryDownload, error) {
	key := goos + "/" + goarch
	if rel.Downloads != nil {
		if dl, ok := rel.Downloads[key]; ok {
			return dl, nil
		}
	}
	if rel.URL != "" {
		return contract.BinaryDownload{URL: rel.URL, SHA256: rel.SHA256}, nil
	}
	return contract.BinaryDownload{}, E.New("no binary download for ", key)
}

func verifySHA256(payload []byte, want string) error {
	sum := sha256.Sum256(payload)
	got := hex.EncodeToString(sum[:])
	if !strings.EqualFold(got, want) {
		return E.New("binary sha256 mismatch")
	}
	return nil
}

func execArgs(executable string, args []string) []string {
	if len(args) == 0 {
		return []string{executable}
	}
	out := make([]string, len(args))
	copy(out, args)
	out[0] = executable
	return out
}

func redactDownloadURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "invalid-url"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}
