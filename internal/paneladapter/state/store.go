package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
)

// CurrentVersion is the schema version written by this code.
const CurrentVersion = 2

// State holds the durable adapter state persisted to disk.
// It MUST NOT contain bearer tokens, user passwords, or TLS secrets.
type State struct {
	Version        int                     `json:"version"`
	Manifest       ManifestState           `json:"manifest,omitempty"`
	Nodes          map[string]NodeState    `json:"nodes,omitempty"`
	Config         ConfigState             `json:"config"`
	Inbounds       map[string]InboundState `json:"inbounds"`
	PendingReports []PendingReport         `json:"pending_reports,omitempty"`
}

// ConfigState tracks the applied configuration revision and ETag.
type ConfigState struct {
	Revision string `json:"revision,omitempty"`
	ETag     string `json:"etag,omitempty"`
	NodeID   string `json:"node_id"`
}

// InboundState tracks per-inbound user loading state.
type InboundState struct {
	InboundID      string `json:"inbound_id"`
	Tag            string `json:"tag"`
	Protocol       string `json:"protocol"`
	UserETag       string `json:"user_etag,omitempty"`
	UserRevision   string `json:"user_revision,omitempty"`
	UserLoadStatus string `json:"user_load_status,omitempty"`
	UserCount      int    `json:"user_count,omitempty"`
}

// PendingReport represents a traffic report that has not yet been
// acknowledged by the panel. The IdempotencyKey is stable across
// retries for the same report window so the panel can deduplicate.
type PendingReport struct {
	IdempotencyKey string    `json:"idempotency_key"`
	BodyHash       string    `json:"body_hash"`
	StartedAt      time.Time `json:"started_at"`
	EndedAt        time.Time `json:"ended_at"`
	RetryCount     int       `json:"retry_count"`
}

// newState returns a State with sensible defaults.
func newState() *State {
	return &State{
		Version:  CurrentVersion,
		Inbounds: make(map[string]InboundState),
		Nodes:    make(map[string]NodeState),
	}
}

// marshalJSON serializes the state to indented JSON bytes.
func (s *State) marshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(s); err != nil {
		return nil, E.Cause(err, "encode state")
	}
	return buf.Bytes(), nil
}

// computeHash returns the SHA-256 hash of the JSON serialization.
func (s *State) computeHash() ([32]byte, error) {
	data, err := s.marshalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

// Store manages durable state with atomic writes and change detection.
type Store struct {
	path       string
	mu         sync.Mutex
	state      *State
	lastHash   [32]byte
	quarantine func(srcPath, dstPath string) error
}

// NewStore creates a Store backed by the file at stateFilePath.
// It loads existing state or initializes defaults. Corrupt state
// files are quarantined (renamed with a .corrupt suffix) rather
// than silently discarded.
func NewStore(stateFilePath string) (*Store, error) {
	s := &Store{
		path:       stateFilePath,
		quarantine: defaultQuarantine,
	}
	state, err := Load(stateFilePath, s.quarantine)
	if err != nil {
		return nil, err
	}
	s.state = state
	h, err := state.computeHash()
	if err != nil {
		return nil, err
	}
	s.lastHash = h
	return s, nil
}

// State returns the current state. The returned pointer MUST NOT be
// mutated directly; use Clone() to obtain a mutable copy.
func (s *Store) State() *State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// SetState replaces the in-memory state with the provided value.
func (s *Store) SetState(st *State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
}

// Save persists the current state to disk using atomic write
// (temp file → fsync → rename).
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeState(s.state)
}

// SaveIfChanged persists the current state only if it differs from
// what was last written. Uses SHA-256 comparison to detect changes.
func (s *Store) SaveIfChanged() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentHash, err := s.state.computeHash()
	if err != nil {
		return err
	}
	if currentHash == s.lastHash {
		return nil
	}
	if err := s.writeState(s.state); err != nil {
		return err
	}
	s.lastHash = currentHash
	return nil
}

// UpdateAndSave applies a mutation to a cloned state while holding the store
// lock, replaces the in-memory state, and persists it when changed.
func (s *Store) UpdateAndSave(update func(*State)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.state.Clone()
	update(next)
	s.state = next

	if s.path == "" {
		return nil
	}

	currentHash, err := next.computeHash()
	if err != nil {
		return err
	}
	if currentHash == s.lastHash {
		return nil
	}
	if err := s.writeState(next); err != nil {
		return err
	}
	s.lastHash = currentHash
	return nil
}

// writeState performs the atomic write: temp file → fsync → rename.
func (s *Store) writeState(st *State) error {
	data, err := st.marshalJSON()
	if err != nil {
		return err
	}
	return atomicWriteFile(s.path, data, 0o644)
}

// Load reads state from the given path. If the file does not exist,
// it returns a default state. If the file is corrupt, it is
// quarantined and a default state is returned; the error is non-nil
// to inform the caller that state was lost.
func Load(path string, quarantine func(srcPath, dstPath string) error) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newState(), nil
		}
		return nil, E.Cause(err, "read state file")
	}
	if len(data) == 0 {
		return newState(), nil
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		// Quarantine the corrupt file instead of silently discarding.
		corruptPath := path + ".corrupt." + fmt.Sprintf("%d", time.Now().UnixNano())
		if qErr := quarantine(path, corruptPath); qErr != nil {
			return nil, E.Errors(err, E.Cause(qErr, "quarantine corrupt state file"))
		}
		return nil, E.Cause(err, "parse state file (corrupt file quarantined to "+corruptPath+")")
	}
	// Ensure the inbounds map is never nil.
	if st.Inbounds == nil {
		st.Inbounds = make(map[string]InboundState)
	}
	if st.Nodes == nil {
		st.Nodes = make(map[string]NodeState)
	}
	if st.Version < CurrentVersion {
		st.Version = CurrentVersion
	}
	for nodeID, node := range st.Nodes {
		if node.Inbounds == nil {
			node.Inbounds = make(map[string]InboundState)
		}
		st.Nodes[nodeID] = node
	}
	return &st, nil
}

// Save writes the state to the given path using atomic write.
func Save(path string, st *State) error {
	data, err := st.marshalJSON()
	if err != nil {
		return err
	}
	return atomicWriteFile(path, data, 0o644)
}

// SaveIfChanged writes state only if it differs from the current
// file content on disk.
func SaveIfChanged(path string, st *State) error {
	data, err := st.marshalJSON()
	if err != nil {
		return err
	}
	existing, err := os.ReadFile(path)
	if err == nil && bytes.Equal(existing, data) {
		return nil
	}
	return atomicWriteFile(path, data, 0o644)
}

// atomicWriteFile writes data to path atomically:
//  1. Write to a temp file in the same directory
//  2. Fsync the temp file
//  3. Rename the temp file over the target
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return E.Cause(err, "create state directory")
	}

	// Use a predictable temp name that includes PID and timestamp to
	// reduce collision risk while keeping it in the same directory
	// (required for rename to be atomic on the same filesystem).
	tmpPath := filepath.Join(dir, fmt.Sprintf(".state.tmp.%d.%d", os.Getpid(), time.Now().UnixNano()))

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return E.Cause(err, "create temp state file")
	}

	// Clean up temp file on any error.
	cleanup := func() {
		f.Close()
		os.Remove(tmpPath)
	}

	if _, err := f.Write(data); err != nil {
		cleanup()
		return E.Cause(err, "write temp state file")
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return E.Cause(err, "fsync temp state file")
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return E.Cause(err, "close temp state file")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return E.Cause(err, "rename temp state file")
	}
	return nil
}

// defaultQuarantine renames a corrupt state file to a backup path.
func defaultQuarantine(srcPath, dstPath string) error {
	return os.Rename(srcPath, dstPath)
}
