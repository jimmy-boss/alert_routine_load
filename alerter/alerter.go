// Package alerter implements the core alert logic:
// - decides whether a job should trigger an alert
// - fetches and deduplicates error URLs
// - applies backoff-based send throttling
package alerter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/model"
	glog "github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// AlertDecision represents a resolved alert ready for notification.
type AlertDecision struct {
	Event     model.AlertEvent
	Action    string // "send" or "skip"
	Reason    string // why skipped (e.g. "backoff not expired")
	StatusKey string // key for status tracking, used by caller to update after successful send
}

// Option is a functional option for Alerter.
type Option func(*Alerter)

// WithHistory injects an AlertHistory for lifecycle tracking.
func WithHistory(h *AlertHistory) Option {
	return func(a *Alerter) {
		a.history = h
	}
}

// WithLogger injects a logger implementation.
func WithLogger(logger glog.HLoggerBase) Option {
	return func(a *Alerter) {
		a.logger = logger
	}
}

// Alerter evaluates jobs against config and produces alert decisions.
type Alerter struct {
	cfg     *config.Config
	logger  glog.HLoggerBase
	client  *http.Client
	history *AlertHistory

	mu          sync.Mutex
	status      map[string]*model.AlertStatus // key = "db:jobId"
	lagCleanup  map[string]bool               // lag-sourced keys to silently clean up
}

func New(cfg *config.Config, opts ...Option) *Alerter {
	a := &Alerter{
		cfg:        cfg,
		client:     &http.Client{Timeout: cfg.Alert.ErrorURLTimeout.Duration},
		status:     make(map[string]*model.AlertStatus),
		lagCleanup: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.logger == nil {
		a.logger = glog.GlobalLoggers["default"]
	}
	return a
}

// Evaluate takes all jobs from all databases, filters paused ones,
// fetches error details, and returns decisions.
// Note: STOPPED and CANCELLED jobs are intentionally not monitored —
// they are terminal states outside the scope of this alerting system.
func (a *Alerter) Evaluate(ctx context.Context, dbJobs map[string][]model.RoutineLoadJob) []AlertDecision {
	var decisions []AlertDecision

	for dbName, jobs := range dbJobs {
		for _, job := range jobs {
			paused := isPaused(job.State)
			lagEnabled, _ := a.cfg.GetEffectiveLag(dbName, job.Name)
			runningWithLag := isRunning(job.State) && lagEnabled

			if !paused && !runningWithLag {
				continue
			}

			key := fmt.Sprintf("%s:%d", dbName, job.ID)
			decision := a.evaluateOne(ctx, key, dbName, job)
			decisions = append(decisions, decision)
		}
	}
	return decisions
}

func (a *Alerter) evaluateOne(ctx context.Context, key, dbName string, job model.RoutineLoadJob) AlertDecision {
	// Lock: read status and determine if we should send.
	a.mu.Lock()
	st, exists := a.status[key]
	isNew := !exists
	if isNew {
		source := "paused"
		if isRunning(job.State) {
			source = "lag"
		}
		st = &model.AlertStatus{FirstAlertAt: time.Now(), Source: source}
		a.status[key] = st
	}

	shouldSend := true
	var reason string
	now := time.Now()

	if !isNew && !st.LastSentAt.IsZero() {
		initial, maxInt, factor := a.cfg.GetEffective(dbName, job.Name)
		delay := computeDelay(st.SendCount, initial, maxInt, factor)
		elapsed := now.Sub(st.LastSentAt)
		if elapsed < delay {
			shouldSend = false
			reason = fmt.Sprintf("backoff: next in %s (count=%d)", (delay - elapsed).Round(time.Second), st.SendCount)
		}
	}
	a.mu.Unlock()

	if !shouldSend {
		return AlertDecision{
			Action: "skip",
			Reason: reason,
		}
	}

	// Fetch error details (outside lock, HTTP I/O).
	errorDetail := ""
	if a.cfg.Alert.FetchErrorURL && job.ErrorLogURLs != "" {
		errorDetail = a.fetchAndDedup(ctx, job.ErrorLogURLs)
		errorDetail = truncate(errorDetail, a.cfg.Alert.ErrorTruncateLen)
	}

	// Determine alert state and lag info.
	alertState := job.State
	lagEnabled, lagThreshold := a.cfg.GetEffectiveLag(dbName, job.Name)

	if isRunning(job.State) && lagEnabled {
		// RUNNING + lag check: alert with State="LAG" if any partition exceeds threshold.
		exceeded := checkLag(job, lagThreshold)
		if len(exceeded) == 0 {
			// Mark for silent cleanup in next RemoveStale cycle.
			a.mu.Lock()
			a.lagCleanup[key] = true
			a.mu.Unlock()
			return AlertDecision{Action: "skip", Reason: "lag below threshold"}
		}
		alertState = "LAG"
		errorDetail = formatLagSummary(exceeded)
	} else if isPaused(job.State) && lagEnabled {
		// PAUSED + lag: always append all non-zero lag info to existing error detail.
		lagStr := formatAllNonZeroLag(job)
		if lagStr != "" {
			if errorDetail != "" {
				errorDetail += "\n---\n"
			}
			errorDetail += "Lag: " + lagStr
		}
	}

	// Build event.
	event := model.AlertEvent{
		JobID:       job.ID,
		JobName:     job.Name,
		Database:    dbName,
		State:       alertState,
		PauseTime:   job.PauseTime,
		Reason:      job.ReasonOfStateChanged,
		ErrorDetail: errorDetail,
		Timestamp:   now,
	}

	return AlertDecision{
		Event:     event,
		Action:    "send",
		StatusKey: key,
	}
}

// GetStatus returns the alert status for the given key, or nil if not found.
func (a *Alerter) GetStatus(key string) *model.AlertStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status[key]
}

// UpdateStatus updates the alert status for a given key after a successful send.
// Must be called by the caller only after notify.Send() succeeds.
// dbName and jobName are used to create a history record on first send.
func (a *Alerter) UpdateStatus(key, dbName, jobName, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	st, exists := a.status[key]
	isNew := !exists
	if isNew {
		st = &model.AlertStatus{FirstAlertAt: time.Now()}
		a.status[key] = st
	}
	st.LastSentAt = time.Now()
	st.SendCount++

	// Create history record on first successful send (not before).
	if isNew && a.history != nil {
		a.history.AddRecord(key, jobName, dbName, reason)
	}

	// Update history send count.
	if a.history != nil {
		a.history.UpdateSendCount(key)
	}
}

// RemoveStale cleans up status entries for jobs that are no longer paused.
// Returns the list of recovered job keys for notification.
func (a *Alerter) RemoveStale(dbJobs map[string][]model.RoutineLoadJob) []string {
	pausedSet := make(map[string]bool)
	runningSet := make(map[string]bool)
	for dbName, jobs := range dbJobs {
		for _, job := range jobs {
			key := fmt.Sprintf("%s:%d", dbName, job.ID)
			if isPaused(job.State) {
				pausedSet[key] = true
			}
			if isRunning(job.State) {
				runningSet[key] = true
			}
		}
	}

	var recoveredKeys []string
	a.mu.Lock()
	for k, st := range a.status {
		switch st.Source {
		case "lag":
			if a.lagCleanup[k] {
				// Lag below threshold — silently remove, no recovery notification.
				delete(a.status, k)
				delete(a.lagCleanup, k)
			} else if !runningSet[k] {
				// Job no longer RUNNING — mark as recovered.
				recoveredKeys = append(recoveredKeys, k)
				if a.history != nil {
					a.history.MarkRecovered(k, "job no longer running")
				}
				delete(a.status, k)
			}
		default: // "paused"
			if !pausedSet[k] {
				recoveredKeys = append(recoveredKeys, k)
				if a.history != nil {
					a.history.MarkRecovered(k, "state no longer PAUSED")
				}
				delete(a.status, k)
			}
		}
	}
	a.mu.Unlock()
	return recoveredKeys
}

// fetchAndDedup requests one or more error URLs and deduplicates identical content.
// ErrorLogURLs can be a single URL or multiple separated by comma/semicolon/newline.
func (a *Alerter) fetchAndDedup(ctx context.Context, rawURLs string) string {
	urls := splitURLs(rawURLs)
	if len(urls) == 0 {
		return ""
	}

	seen := make(map[string]bool)
	var parts []string
	failed := 0

	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		body, err := a.fetchURL(ctx, u)
		if err != nil {
			a.logger.Warn("fetch error url failed",
				zap.String("url", u),
				zap.Error(err),
			)
			failed++
			continue
		}
		normalized := strings.TrimSpace(body)
		if normalized == "" {
			continue
		}
		// Dedup: identical content → skip.
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		parts = append(parts, normalized)
	}

	if len(parts) == 0 && failed > 0 {
		return "(failed to fetch error details)"
	}
	return strings.Join(parts, "\n---\n")
}

func (a *Alerter) fetchURL(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		// resp may be non-nil on redirect errors; ensure body is closed.
		if resp != nil {
			resp.Body.Close()
		}
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	limited := io.LimitReader(resp.Body, 64*1024) // 64KB max
	body, err := io.ReadAll(limited)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func splitURLs(raw string) []string {
	r := strings.NewReplacer(",", "\n", ";", "\n")
	lines := strings.Split(r.Replace(raw), "\n")
	var out []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "...(truncated)"
}

// checkLag checks if any partition's lag exceeds the threshold.
// Returns the list of exceeded partitions (sorted by partition ID).
func checkLag(job model.RoutineLoadJob, threshold int64) []model.LagInfo {
	parsed := model.ParseLag(job.Lag)
	if len(parsed) == 0 {
		return nil
	}

	var exceeded []model.LagInfo
	for pid, lag := range parsed {
		if lag > threshold {
			exceeded = append(exceeded, model.LagInfo{PartitionID: pid, LagCount: lag})
		}
	}

	// Sort by partition ID for deterministic output.
	sort.Slice(exceeded, func(i, j int) bool {
		return exceeded[i].PartitionID < exceeded[j].PartitionID
	})
	return exceeded
}

// formatAllNonZeroLag formats all non-zero lag partitions into a human-readable string.
// Example: "partition 1=80009, partition 4=80008, partition 7=80019"
func formatAllNonZeroLag(job model.RoutineLoadJob) string {
	parsed := model.ParseLag(job.Lag)
	if len(parsed) == 0 {
		return ""
	}
	var lags []model.LagInfo
	for pid, lag := range parsed {
		if lag > 0 {
			lags = append(lags, model.LagInfo{PartitionID: pid, LagCount: lag})
		}
	}
	if len(lags) == 0 {
		return ""
	}
	sort.Slice(lags, func(i, j int) bool {
		return lags[i].PartitionID < lags[j].PartitionID
	})
	return formatLagSummary(lags)
}

// formatLagSummary formats lag info into a human-readable string.
// Example: "partition 1=80009, partition 4=80008, partition 7=80019"
func formatLagSummary(lags []model.LagInfo) string {
	parts := make([]string, len(lags))
	for i, l := range lags {
		parts[i] = fmt.Sprintf("partition %s=%d", l.PartitionID, l.LagCount)
	}
	return strings.Join(parts, ", ")
}

func isPaused(state string) bool {
	s := strings.ToUpper(strings.TrimSpace(state))
	return s == "PAUSED" || s == "PAUSE"
}

func isRunning(state string) bool {
	return strings.ToUpper(strings.TrimSpace(state)) == "RUNNING"
}

// computeDelay returns the delay for the Nth alert send using exponential backoff.
// delay(n) = min(initial * factor^n, max)
func computeDelay(sendCount int, initial, max time.Duration, factor float64) time.Duration {
	d := initial
	for i := 0; i < sendCount; i++ {
		d = time.Duration(float64(d) * factor)
		if d > max {
			return max
		}
	}
	return d
}
