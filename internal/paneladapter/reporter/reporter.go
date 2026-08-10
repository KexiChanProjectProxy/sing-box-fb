// Package reporter implements traffic report batching, durable retry,
// and idempotency for the sing-box panel adapter.
//
// The Reporter stages non-zero traffic deltas from the tracker, persists
// a pending report to the state store BEFORE sending (so live counters can
// be safely reset), POSTs with a stable idempotency key, and clears the
// pending report only on accepted 201/200. On transport/429/503 errors the
// pending report is kept for retry. A 409 IDEMPOTENCY_CONFLICT is treated
// as a hard state error requiring operator attention.
//
// Retries reuse the same idempotency key and body so the panel can
// deduplicate safely. Recovery after restart loads pending reports from
// the state store and retries them with the same key.
package reporter

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
)

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Option configures a Reporter.
type Option func(*Reporter)

// WithSkipEmpty sets whether reports with zero records are skipped.
// Default is true.
func WithSkipEmpty(skip bool) Option {
	return func(r *Reporter) {
		r.skipEmpty = skip
	}
}

// WithLogger sets the logger for the reporter.
func WithLogger(l log.ContextLogger) Option {
	return func(r *Reporter) {
		r.logger = l
	}
}

// WithConfigRevision sets the configuration revision to include in reports.
func WithConfigRevision(rev string) Option {
	return func(r *Reporter) {
		r.configRevision = rev
	}
}

// ---------------------------------------------------------------------------
// Reporter
// ---------------------------------------------------------------------------

// Reporter stages traffic deltas, persists pending reports, and sends
// them to the panel with stable idempotency keys and durable retry.
type Reporter struct {
	client          *client.Client
	store           state.Repository
	tracker         *traffic.Tracker
	nodeID          string
	skipEmpty       bool
	logger          log.ContextLogger
	configRevision  string
	reportStartTime time.Time

	// pendingReport holds the last report that was persisted but not yet
	// accepted by the panel. It is kept in memory so retries can re-send
	// the exact same body with the same idempotency key. On crash, the
	// report is recovered from state via Recovery().
	pendingReport *contract.TrafficReport
	pendingKey    string
	pendingHash   string
}

// NewReporter creates a traffic reporter.
func NewReporter(c *client.Client, store state.Repository, tracker *traffic.Tracker, nodeID string, opts ...Option) *Reporter {
	r := &Reporter{
		client:    c,
		store:     store,
		tracker:   tracker,
		nodeID:    nodeID,
		skipEmpty: true,
		logger:    logger.NOP(),
	}
	r.reportStartTime = time.Now().UTC()
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// SetConfigRevision updates the configuration revision included in traffic reports.
func (r *Reporter) SetConfigRevision(rev string) {
	r.configRevision = rev
}

// ReportNow stages, persists, and sends one traffic report.
//
// If a pending (unconfirmed) report exists from a previous failed attempt,
// it is re-sent with the same idempotency key. Otherwise, new traffic is
// staged from the tracker, persisted, and sent.
func (r *Reporter) ReportNow(ctx context.Context) error {
	if r.pendingReport != nil {
		return r.retryPending(ctx)
	}
	return r.stageAndSend(ctx)
}

// stageAndSend stages a new report from live counters, persists it,
// and sends it.
func (r *Reporter) stageAndSend(ctx context.Context) error {
	now := time.Now().UTC()
	report := r.tracker.StageForReport(r.reportStartTime, now, r.configRevision)

	if r.skipEmpty && len(report.Records) == 0 {
		r.reportStartTime = now
		return nil
	}

	bodyHash, err := computeBodyHash(report)
	if err != nil {
		return E.Cause(err, "compute report body hash")
	}

	idempotencyKey := makeIdempotencyKey(r.nodeID, r.reportStartTime, bodyHash)

	if err := r.store.UpdateAndSave(func(st *state.State) {
		st.PendingReports = append(st.PendingReports, state.PendingReport{
			IdempotencyKey: idempotencyKey,
			BodyHash:       bodyHash,
			StartedAt:      r.reportStartTime,
			EndedAt:        now,
			RetryCount:     0,
		})
	}); err != nil {
		return E.Cause(err, "persist pending report")
	}

	r.tracker.ConfirmJournaled()
	r.tracker.ResetLiveCountersWhenJournaled()

	r.pendingReport = report
	r.pendingKey = idempotencyKey
	r.pendingHash = bodyHash

	return r.sendAndFinalize(ctx, report, idempotencyKey, bodyHash, r.reportStartTime)
}

// retryPending re-sends the in-memory pending report with its
// idempotency key.
func (r *Reporter) retryPending(ctx context.Context) error {
	if err := r.store.UpdateAndSave(func(st *state.State) {
		for i, pr := range st.PendingReports {
			if pr.BodyHash == r.pendingHash && pr.StartedAt.Equal(r.pendingReport.StartedAt) {
				pr.RetryCount++
				st.PendingReports[i] = pr
				break
			}
		}
	}); err != nil {
		return E.Cause(err, "persist pending report retry count")
	}

	return r.sendAndFinalize(ctx, r.pendingReport, r.pendingKey, r.pendingHash, r.pendingReport.StartedAt)
}

// sendAndFinalize sends the report and handles the response.
func (r *Reporter) sendAndFinalize(ctx context.Context, report *contract.TrafficReport, idempotencyKey, bodyHash string, startedAt time.Time) error {
	sendErr := r.client.ReportTraffic(ctx, report, idempotencyKey)
	if sendErr == nil {
		r.tracker.ConfirmAccepted()
		r.tracker.ResetLiveCountersWhenJournaled()

		if saveErr := r.store.UpdateAndSave(func(st *state.State) {
			st.PendingReports = removePendingByHash(st.PendingReports, bodyHash, startedAt)
		}); saveErr != nil {
			r.logger.ErrorContext(ctx, "reporter: failed to clear pending report after success: ", saveErr)
		}

		r.pendingReport = nil
		r.pendingKey = ""
		r.pendingHash = ""

		r.reportStartTime = time.Now().UTC()
		return nil
	}

	retryable, isConflict, _ := client.ClassifyError(sendErr)

	if isConflict {
		r.logger.ErrorContext(ctx, "reporter: IDEMPOTENCY_CONFLICT for key ", idempotencyKey,
			" — requires operator attention")
		return E.Cause(sendErr, "idempotency conflict (hard state error)")
	}

	if retryable {
		r.logger.WarnContext(ctx, "reporter: retryable error sending traffic report: ", sendErr)
		return E.Cause(sendErr, "retryable error sending traffic report")
	}

	r.logger.ErrorContext(ctx, "reporter: non-retryable error sending traffic report: ", sendErr)
	return E.Cause(sendErr, "non-retryable error sending traffic report")
}

// Recovery restores pending reports from state after a restart and
// retries them with the same idempotency keys.
func (r *Reporter) Recovery(ctx context.Context) error {
	st := r.store.State()
	if len(st.PendingReports) == 0 {
		return nil
	}

	r.logger.InfoContext(ctx, "reporter: recovering ", len(st.PendingReports), " pending report(s)")

	pendings := make([]state.PendingReport, len(st.PendingReports))
	copy(pendings, st.PendingReports)

	var remaining []state.PendingReport
	for _, pr := range pendings {
		// Try to get the staged report from the tracker.
		// On restart, RestorePendingReport should have already been called
		// before Recovery. If not, we reconstruct a minimal report.
		pendingReport := r.tracker.PendingReport()
		if pendingReport == nil {
			pendingReport = &contract.TrafficReport{
				StartedAt:             pr.StartedAt,
				EndedAt:               pr.EndedAt,
				ConfigurationRevision: r.configRevision,
				Records:               []contract.TrafficRecord{},
			}
		}

		err := r.client.ReportTraffic(ctx, pendingReport, pr.IdempotencyKey)
		if err == nil {
			r.tracker.ConfirmAccepted()
			r.tracker.ResetLiveCountersWhenJournaled()
			r.logger.InfoContext(ctx, "reporter: recovered pending report with key ", pr.IdempotencyKey)
			continue
		}

		retryable, isConflict, _ := client.ClassifyError(err)
		if isConflict {
			r.logger.ErrorContext(ctx, "reporter: IDEMPOTENCY_CONFLICT during recovery for key ", pr.IdempotencyKey,
				" — requires operator attention")
			remaining = append(remaining, pr)
			continue
		}

		if retryable {
			pr.RetryCount++
			remaining = append(remaining, pr)
			r.logger.WarnContext(ctx, "reporter: retryable error recovering report: ", err)
			continue
		}

		r.logger.ErrorContext(ctx, "reporter: non-retryable error recovering report: ", err)
		remaining = append(remaining, pr)
	}

	if err := r.store.UpdateAndSave(func(st *state.State) {
		st.PendingReports = remaining
	}); err != nil {
		return E.Cause(err, "save state after recovery")
	}

	return nil
}

// ---------------------------------------------------------------------------
// Idempotency key
// ---------------------------------------------------------------------------

// makeIdempotencyKey creates a stable key scoped by (nodeID, windowStart, bodyHash).
func makeIdempotencyKey(nodeID string, windowStart time.Time, bodyHash string) string {
	return fmt.Sprintf("tr_%s_%s_%s", nodeID, windowStart.Format(time.RFC3339), bodyHash[:8])
}

// computeBodyHash returns a hex-encoded SHA-256 hash of the report body.
func computeBodyHash(report *contract.TrafficReport) (string, error) {
	data, err := json.Marshal(report)
	if err != nil {
		return "", E.Cause(err, "marshal report for hashing")
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h), nil
}

// removePendingByHash removes a pending report matching the given body hash
// and startedAt timestamp from the slice.
func removePendingByHash(pendings []state.PendingReport, bodyHash string, startedAt time.Time) []state.PendingReport {
	result := make([]state.PendingReport, 0, len(pendings))
	for _, pr := range pendings {
		if pr.BodyHash == bodyHash && pr.StartedAt.Equal(startedAt) {
			continue
		}
		result = append(result, pr)
	}
	return result
}
