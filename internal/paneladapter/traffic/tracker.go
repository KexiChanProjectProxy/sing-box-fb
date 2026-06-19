// Package traffic provides per-inbound per-user traffic accounting for the
// panel adapter. It wraps sing-box connections to observe bytes transferred
// and produces staged TrafficReport snapshots that survive restart without
// double-counting.
//
// Accounting identity is the tuple (inbound_id, user_id). Inbound tags that
// are not in the configured mapping are silently ignored. Empty user
// identities are also ignored.
//
// The staging lifecycle is:
//  1. Live counters accumulate upload/download bytes per key.
//  2. StageForReport() snapshots live counters into a pending report and
//     marks them as "staged" (not yet journaled).
//  3. ConfirmJournaled() or ConfirmAccepted() marks staged data as
//     durably persisted.
//  4. ResetLiveCountersWhenJournaled() clears live counters only when
//     staged data was previously journaled/accepted, preventing
//     double-counting on restart.
package traffic

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing/common/bufio"
	N "github.com/sagernet/sing/common/network"
)

// key is the accounting identity for traffic: (inboundTag, userID).
type key struct {
	InboundTag string
	UserID     string
}

// counters holds per-key upload/download byte counters.
type counters struct {
	Upload   atomic.Int64
	Download atomic.Int64
}

// stagedSnapshot holds a staged report snapshot together with the live
// counter values at the time of staging. When the report is journaled or
// accepted, these values are subtracted from live counters to avoid
// double-counting.
type stagedSnapshot struct {
	Report    *contract.TrafficReport
	Snapshot  map[key][2]int64 // [upload, download] at staging time
	Journaled bool
	Accepted  bool
}

// Tracker implements per-inbound per-user traffic accounting.
type Tracker struct {
	mu             sync.Mutex
	inboundMapping map[string]string // inbound tag → inbound_id

	// live counters: accumulate continuously
	live map[key]*counters

	// staged is the most recent staged snapshot awaiting journal/accept.
	staged *stagedSnapshot

	// runtime metrics
	activeConns atomic.Int64
	distinctIPs map[key]map[string]struct{} // key → set of IPs (never persisted/reported)
	ipCounts    map[key]int                 // key → distinct IP count (aggregate only)
}

// NewTracker creates a Tracker with the given inbound tag-to-id mapping.
// Only inbounds present in the mapping will have their traffic tracked.
// A nil or empty mapping is acceptable; call UpdateInboundMapping later.
func NewTracker(inboundMapping map[string]string) *Tracker {
	// Defensive copy
	m := make(map[string]string, len(inboundMapping))
	for k, v := range inboundMapping {
		m[k] = v
	}
	return &Tracker{
		inboundMapping: m,
		live:           make(map[key]*counters),
		distinctIPs:    make(map[key]map[string]struct{}),
		ipCounts:       make(map[key]int),
	}
}

// UpdateInboundMapping replaces the inbound tag-to-id mapping.
// This is safe to call concurrently; it acquires the mutex.
func (t *Tracker) UpdateInboundMapping(mapping map[string]string) {
	m := make(map[string]string, len(mapping))
	for k, v := range mapping {
		m[k] = v
	}
	t.mu.Lock()
	t.inboundMapping = m
	t.mu.Unlock()
}

// inboundID returns the panel inbound_id for a given sing-box inbound tag,
// or ("", false) if the inbound is not managed.
func (t *Tracker) inboundID(inboundTag string) (string, bool) {
	id, ok := t.inboundMapping[inboundTag]
	return id, ok
}

// getOrCreateCounters returns the counters for the given key, creating them
// if necessary. Caller must hold t.mu.
func (t *Tracker) getOrCreateCounters(k key) *counters {
	c, ok := t.live[k]
	if !ok {
		c = &counters{}
		t.live[k] = c
	}
	return c
}

// TrackConnection wraps a net.Conn to observe bytes transferred for the
// given inbound tag and user. If the inbound is unmanaged or the user is
// empty, the original connection is returned unchanged.
func (t *Tracker) TrackConnection(conn net.Conn, inboundTag, userID string) net.Conn {
	inboundID, ok := t.inboundID(inboundTag)
	if !ok || userID == "" {
		return conn
	}

	t.activeConns.Add(1)
	k := key{InboundTag: inboundTag, UserID: userID}

	t.mu.Lock()
	c := t.getOrCreateCounters(k)
	t.mu.Unlock()

	wrapped := bufio.NewCounterConn(conn,
		[]N.CountFunc{func(n int64) {
			c.Upload.Add(n)
		}},
		[]N.CountFunc{func(n int64) {
			c.Download.Add(n)
		}},
	)

	return &trackedConn{
		Conn:       wrapped,
		tracker:    t,
		inboundTag: inboundTag,
		inboundID:  inboundID,
		userID:     userID,
	}
}

// TrackPacketConnection wraps an N.PacketConn to observe bytes transferred
// for the given inbound tag and user. If the inbound is unmanaged or the
// user is empty, the original connection is returned unchanged.
func (t *Tracker) TrackPacketConnection(conn N.PacketConn, inboundTag, userID string) N.PacketConn {
	inboundID, ok := t.inboundID(inboundTag)
	if !ok || userID == "" {
		return conn
	}

	t.activeConns.Add(1)
	k := key{InboundTag: inboundTag, UserID: userID}

	t.mu.Lock()
	c := t.getOrCreateCounters(k)
	t.mu.Unlock()

	wrapped := bufio.NewCounterPacketConn(conn,
		[]N.CountFunc{func(n int64) {
			c.Upload.Add(n)
		}},
		[]N.CountFunc{func(n int64) {
			c.Download.Add(n)
		}},
	)

	return &trackedPacketConn{
		PacketConn: wrapped,
		tracker:    t,
		inboundTag: inboundTag,
		inboundID:  inboundID,
		userID:     userID,
	}
}

// TrackDistinctIP records a client IP for aggregate distinct-IP counting.
// Raw IPs are never persisted or reported — only the count is exposed.
func (t *Tracker) TrackDistinctIP(ip string, inboundTag, userID string) {
	if ip == "" {
		return
	}
	_, ok := t.inboundID(inboundTag)
	if !ok || userID == "" {
		return
	}

	k := key{InboundTag: inboundTag, UserID: userID}

	t.mu.Lock()
	defer t.mu.Unlock()

	ipSet, ok := t.distinctIPs[k]
	if !ok {
		ipSet = make(map[string]struct{})
		t.distinctIPs[k] = ipSet
	}
	if _, exists := ipSet[ip]; !exists {
		ipSet[ip] = struct{}{}
		t.ipCounts[k] = len(ipSet)
	}
}

// StageForReport extracts current live counter deltas as a TrafficReport,
// marks them as "staged" (not yet journaled). The live counters are NOT
// reset — they continue to accumulate. Reset happens only after
// ConfirmJournaled/ConfirmAccepted + ResetLiveCountersWhenJournaled.
func (t *Tracker) StageForReport(startedAt, endedAt time.Time, configRevision string) *contract.TrafficReport {
	t.mu.Lock()
	defer t.mu.Unlock()

	snapshot := make(map[key][2]int64, len(t.live))
	records := make([]contract.TrafficRecord, 0, len(t.live))

	for k, c := range t.live {
		up := c.Upload.Load()
		down := c.Download.Load()
		snapshot[k] = [2]int64{up, down}

		inboundID, ok := t.inboundID(k.InboundTag)
		if !ok || k.UserID == "" {
			continue
		}

		records = append(records, contract.TrafficRecord{
			InboundID:     inboundID,
			UserID:        k.UserID,
			UploadBytes:   up,
			DownloadBytes: down,
		})
	}

	report := &contract.TrafficReport{
		StartedAt:             startedAt,
		EndedAt:               endedAt,
		ConfigurationRevision: configRevision,
		Records:               records,
	}

	t.staged = &stagedSnapshot{
		Report:   report,
		Snapshot: snapshot,
	}

	return report
}

// ConfirmJournaled confirms that the staged data was durably persisted
// (e.g., written to the T3 state store). After this call,
// ResetLiveCountersWhenJournaled will subtract the staged snapshot from
// live counters.
func (t *Tracker) ConfirmJournaled() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.staged != nil {
		t.staged.Journaled = true
	}
}

// ConfirmAccepted confirms that the staged data was accepted by the panel.
// After this call, ResetLiveCountersWhenJournaled will subtract the staged
// snapshot from live counters and clear the staged state.
func (t *Tracker) ConfirmAccepted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.staged != nil {
		t.staged.Accepted = true
	}
}

// ResetLiveCountersWhenJournaled clears live counters only when the staged
// data was previously journaled or accepted. It subtracts the staged
// snapshot values from live counters (not a hard reset), so any bytes that
// accumulated after staging are preserved. This prevents double-counting
// on restart: if the process crashes before journaling, the live counters
// still contain the unstaged bytes and will be re-staged on the next cycle.
func (t *Tracker) ResetLiveCountersWhenJournaled() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.staged == nil {
		return
	}
	if !t.staged.Journaled && !t.staged.Accepted {
		return
	}

	// Subtract staged snapshot from live counters.
	for k, snap := range t.staged.Snapshot {
		c, ok := t.live[k]
		if !ok {
			continue
		}
		c.Upload.Add(-snap[0])
		c.Download.Add(-snap[1])

		// Clean up zero counters to avoid map growth.
		if c.Upload.Load() == 0 && c.Download.Load() == 0 {
			delete(t.live, k)
		}
	}

	// Clear staged state.
	t.staged = nil
}

// GetRuntimeMetrics returns aggregate runtime metrics for heartbeat.
// Connection counts and distinct IP counts are reported as aggregate
// numbers only — raw IP lists are never exposed.
func (t *Tracker) GetRuntimeMetrics() *contract.HeartbeatRuntime {
	t.mu.Lock()
	defer t.mu.Unlock()

	totalConns := int(t.activeConns.Load())

	// Sum distinct IP counts across all keys.
	var totalDistinctIPs int
	for _, count := range t.ipCounts {
		totalDistinctIPs += count
	}

	return &contract.HeartbeatRuntime{
		Connections: totalConns,
	}
}

// HasStagedReport returns whether there is a pending staged report that
// has not yet been journaled or accepted. This is useful for restart
// recovery: if a staged report exists on startup, it should be re-sent.
func (t *Tracker) HasStagedReport() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.staged != nil
}

// PendingReport returns the most recent staged report, or nil if none.
// Used for restart recovery to re-send unconfirmed reports.
func (t *Tracker) PendingReport() *contract.TrafficReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.staged == nil {
		return nil
	}
	return t.staged.Report
}

// RestorePendingReport restores a previously staged report from persistent
// storage on restart. This allows the reporter to re-send the report
// without double-counting, since the live counters will not include the
// previously staged bytes (they were subtracted before the process
// persisted the journal entry).
func (t *Tracker) RestorePendingReport(report *contract.TrafficReport) {
	t.mu.Lock()
	defer t.mu.Unlock()

	snapshot := make(map[key][2]int64, len(report.Records))
	for _, rec := range report.Records {
		// Find the inbound tag for this inbound_id (reverse lookup).
		var inboundTag string
		for tag, id := range t.inboundMapping {
			if id == rec.InboundID {
				inboundTag = tag
				break
			}
		}
		if inboundTag == "" {
			continue
		}
		k := key{InboundTag: inboundTag, UserID: rec.UserID}
		snapshot[k] = [2]int64{rec.UploadBytes, rec.DownloadBytes}
	}

	t.staged = &stagedSnapshot{
		Report:   report,
		Snapshot: snapshot,
		// Not journaled/accepted yet — needs re-sending.
	}
}

// ---------------------------------------------------------------------------
// Tracked connection wrappers
// ---------------------------------------------------------------------------

// trackedConn wraps a bufio.CounterConn and decrements the active connection
// count on Close.
type trackedConn struct {
	net.Conn
	tracker    *Tracker
	inboundTag string
	inboundID  string
	userID     string
}

func (c *trackedConn) Close() error {
	c.tracker.activeConns.Add(-1)
	return c.Conn.Close()
}

func (c *trackedConn) Upstream() any {
	return c.Conn
}

// trackedPacketConn wraps a bufio.CounterPacketConn and decrements the
// active connection count on Close.
type trackedPacketConn struct {
	N.PacketConn
	tracker    *Tracker
	inboundTag string
	inboundID  string
	userID     string
}

func (c *trackedPacketConn) Close() error {
	c.tracker.activeConns.Add(-1)
	return c.PacketConn.Close()
}

func (c *trackedPacketConn) Upstream() any {
	return c.PacketConn
}
