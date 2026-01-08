// Package alerter implements the core alert logic:
// - decides whether a job should trigger an alert
// - fetches and deduplicates error URLs
// - applies backoff-based send throttling
package alerter

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/model"
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

// Alerter evaluates jobs against config and produces alert decisions.
type Alerter struct {
	cfg     *config.Config
	logger  *slog.Logger
	client  *http.Client
	history *AlertHistory

	mu     sync.Mutex
	status map[string]*model.AlertStatus // key = "db:jobId"
}

func New(cfg *config.Config, logger *slog.Logger, opts ...Option) *Alerter {
	a := &Alerter{
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: cfg.Alert.ErrorURLTimeout.Duration},
		status: make(map[string]*model.AlertStatus),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Evaluate takes all jobs from all databases, filters paused ones,
// fetches error details, and returns decisions.
func (a *Alerter) Evaluate(ctx context.Context, dbJobs map[string][]model.RoutineLoadJob) []AlertDecision {
	var decisions []AlertDecision

	for dbName, jobs := range dbJobs {
		for _, job := range jobs {
			if !isPaused(job.State) {
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
		st = &model.AlertStatus{FirstAlertAt: time.Now()}
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

	// Create history record for first-time alert.
	if isNew && a.history != nil {
		a.history.AddRecord(key, job.Name, dbName, job.ReasonOfStateChanged)
	}

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

	// Build event.
	event := model.AlertEvent{
		JobID:       job.ID,
		JobName:     job.Name,
		Database:    dbName,
		State:       job.State,
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

// UpdateStatus updates the alert status for a given key after a successful send.
// Must be called by the caller only after notify.Send() succeeds.
func (a *Alerter) UpdateStatus(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	st, exists := a.status[key]
	if !exists {
		st = &model.AlertStatus{}
		a.status[key] = st
	}
	st.LastSentAt = time.Now()
	st.SendCount++

	// Update history record.
	if a.history != nil {
		a.history.UpdateSendCount(key)
	}
}

// RemoveStale cleans up status entries for jobs that are no longer paused.
// Returns the list of recovered job keys for notification.
func (a *Alerter) RemoveStale(dbJobs map[string][]model.RoutineLoadJob) []string {
	active := make(map[string]bool)
	for dbName, jobs := range dbJobs {
		for _, job := range jobs {
			if isPaused(job.State) {
				active[fmt.Sprintf("%s:%d", dbName, job.ID)] = true
			}
		}
	}

	var recoveredKeys []string
	a.mu.Lock()
	for k := range a.status {
		if !active[k] {
			recoveredKeys = append(recoveredKeys, k)
			// Mark recovery in history before deleting status.
			if a.history != nil {
				a.history.MarkRecovered(k, "state no longer PAUSED")
			}
			delete(a.status, k)
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

	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		body, err := a.fetchURL(ctx, u)
		if err != nil {
			a.logger.Warn("fetch error url failed", "url", u, "err", err)
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

func isPaused(state string) bool {
	s := strings.ToUpper(strings.TrimSpace(state))
	return s == "PAUSED" || s == "PAUSE"
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
