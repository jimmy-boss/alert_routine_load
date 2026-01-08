// Package alerter — AlertHistory manages alert lifecycle records with JSON persistence.
package alerter

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/model"
)

const (
	activeFile  = "alert_active.json"
	archiveFile = "alert_archive.json"
)

// AlertHistory manages alert lifecycle records with dual-file JSON persistence.
type AlertHistory struct {
	dir     string
	maxAge  time.Duration
	logger  *slog.Logger

	mu       sync.RWMutex
	active   []model.AlertRecord // ongoing alerts (not yet recovered)
	archive  []model.AlertRecord // recovered alerts
	dirty    bool                // true if unsaved changes exist
}

// NewHistory creates an AlertHistory, loading existing data from disk.
func NewHistory(cfg *config.HistoryConfig, logger *slog.Logger) (*AlertHistory, error) {
	h := &AlertHistory{
		dir:    cfg.Dir,
		maxAge: cfg.MaxAge.Duration,
		logger: logger,
	}

	// Ensure directory exists.
	if err := os.MkdirAll(h.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create history dir: %w", err)
	}

	// Load existing records.
	if err := h.load(); err != nil {
		logger.Warn("failed to load history, starting fresh", "err", err)
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
		"job_key", jobKey,
		"job_name", jobName,
		"database", db,
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
				"job_key", jobKey,
				"duration", r.Duration().Round(time.Second),
				"send_count", r.SendCount,
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
		h.logger.Error("save active records failed", "err", err)
		return
	}
	if err := writeJSON(filepath.Join(h.dir, archiveFile), h.archive); err != nil {
		h.logger.Error("save archive records failed", "err", err)
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
		"active", len(h.active),
		"archive", len(h.archive),
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
		h.logger.Info("purged expired archive records", "removed", removed, "remaining", len(kept))
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
