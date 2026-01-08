// Package alerter — AlertHistory manages alert lifecycle records with JSON persistence.
package alerter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/model"
	glog "github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

const (
	activeFile  = "alert_active.json"
	archiveFile = "alert_archive.json"
)

// HistoryOption is a functional option for AlertHistory.
type HistoryOption func(*AlertHistory)

// WithHistoryLogger injects a logger implementation for AlertHistory.
func WithHistoryLogger(logger glog.HLoggerBase) HistoryOption {
	return func(h *AlertHistory) {
		h.logger = logger
	}
}

// AlertHistory manages alert lifecycle records with dual-file JSON persistence.
type AlertHistory struct {
	dir    string
	maxAge time.Duration
	logger glog.HLoggerBase

	mu      sync.RWMutex
	active  []model.AlertRecord // ongoing alerts (not yet recovered)
	archive []model.AlertRecord // recovered alerts
	dirty   bool                // true if unsaved changes exist
}

// NewHistory creates an AlertHistory, loading existing data from disk.
func NewHistory(cfg *config.HistoryConfig, opts ...HistoryOption) (*AlertHistory, error) {
	h := &AlertHistory{
		dir:    cfg.Dir,
		maxAge: cfg.MaxAge.Duration,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.logger == nil {
		h.logger = glog.GlobalLoggers["default"]
	}

	// Ensure directory exists.
	if err := os.MkdirAll(h.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create history dir: %w", err)
	}

	// Load existing records.
	if err := h.load(); err != nil {
		h.logger.Warn("failed to load history, starting fresh", zap.Error(err))
	}

	// Purge expired archive records on startup.
	h.purgeExpired()

	return h, nil
}

// AddRecord creates a new alert record for a job that just entered PAUSED state.
func (h *AlertHistory) AddRecord(jobKey, jobName, db, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	record := model.AlertRecord{
		JobKey:       jobKey,
		JobName:      jobName,
		Database:     db,
		FirstAlertAt: time.Now(),
		LastReason:   reason,
	}
	h.active = append(h.active, record)
	h.dirty = true

	h.logger.Info("alert record created",
		zap.String("job_key", jobKey),
		zap.String("job_name", jobName),
		zap.String("database", db),
	)
}

// UpdateSendCount increments the send count for the given job key.
func (h *AlertHistory) UpdateSendCount(jobKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.active {
		if h.active[i].JobKey == jobKey {
			h.active[i].SendCount++
			h.dirty = true
			return
		}
	}
}

// MarkRecovered moves a record from active to archive and persists immediately.
func (h *AlertHistory) MarkRecovered(jobKey, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	for i, r := range h.active {
		if r.JobKey == jobKey {
			r.RecoveredAt = now
			r.LastReason = reason
			h.archive = append(h.archive, r)

			// Remove from active.
			h.active = append(h.active[:i], h.active[i+1:]...)
			h.dirty = true

			h.logger.Info("alert recovered",
				zap.String("job_key", jobKey),
				zap.Duration("duration", r.Duration().Round(time.Second)),
				zap.Int("send_count", r.SendCount),
			)

			// Persist immediately on recovery.
			h.saveLocked()
			return
		}
	}
}

// Save persists all records to disk (if dirty).
func (h *AlertHistory) Save() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saveLocked()
}

// saveLocked persists records. Must be called with h.mu held.
func (h *AlertHistory) saveLocked() {
	if !h.dirty {
		return
	}

	if err := writeJSON(filepath.Join(h.dir, activeFile), h.active); err != nil {
		h.logger.Error("save active records failed", zap.Error(err))
		return
	}
	if err := writeJSON(filepath.Join(h.dir, archiveFile), h.archive); err != nil {
		h.logger.Error("save archive records failed", zap.Error(err))
		return
	}
	h.dirty = false
}

// load reads records from disk.
func (h *AlertHistory) load() error {
	activePath := filepath.Join(h.dir, activeFile)
	archivePath := filepath.Join(h.dir, archiveFile)

	active, err := readRecords(activePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load active: %w", err)
	}
	archive, err := readRecords(archivePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load archive: %w", err)
	}

	h.active = active
	h.archive = archive

	h.logger.Info("history loaded",
		zap.Int("active", len(h.active)),
		zap.Int("archive", len(h.archive)),
	)
	return nil
}

// purgeExpired removes archive records older than maxAge.
func (h *AlertHistory) purgeExpired() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.maxAge <= 0 {
		return
	}

	cutoff := time.Now().Add(-h.maxAge)
	var kept []model.AlertRecord
	for _, r := range h.archive {
		if r.RecoveredAt.After(cutoff) {
			kept = append(kept, r)
		}
	}

	removed := len(h.archive) - len(kept)
	if removed > 0 {
		h.archive = kept
		h.dirty = true
		h.saveLocked()
		h.logger.Info("purged expired archive records",
			zap.Int("removed", removed),
			zap.Int("remaining", len(kept)),
		)
	}
}

// GetActiveRecords returns a snapshot of currently active alert records.
func (h *AlertHistory) GetActiveRecords() []model.AlertRecord {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]model.AlertRecord, len(h.active))
	copy(out, h.active)
	return out
}

// FindRecord returns the active record for the given job key, if any.
func (h *AlertHistory) FindRecord(jobKey string) *model.AlertRecord {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for i := range h.active {
		if h.active[i].JobKey == jobKey {
			return &h.active[i]
		}
	}
	return nil
}

// FindArchivedRecord returns the most recent archived record for the given job key.
func (h *AlertHistory) FindArchivedRecord(jobKey string) *model.AlertRecord {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Search from end (most recent).
	for i := len(h.archive) - 1; i >= 0; i-- {
		if h.archive[i].JobKey == jobKey {
			return &h.archive[i]
		}
	}
	return nil
}

func writeJSON(path string, v interface{}) error {
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readRecords(path string) ([]model.AlertRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var records []model.AlertRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}
