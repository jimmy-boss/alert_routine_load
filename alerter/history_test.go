package alerter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jimmy-boss/alert_routine_load/model"
	glog "github.com/jimmy-boss/go-log/glog"
)

func newTestLogger() glog.HLoggerBase {
	return glog.GetLogger("test")
}

func newTestHistory(t *testing.T) (*AlertHistory, string) {
	t.Helper()
	dir := t.TempDir()
	h := &AlertHistory{
		dir:    dir,
		maxAge: 720 * time.Hour,
		logger: newTestLogger(),
	}
	return h, dir
}

func TestAddRecord(t *testing.T) {
	h, _ := newTestHistory(t)

	h.AddRecord("db1:100", "job1", "db1", "some error")

	if len(h.active) != 1 {
		t.Fatalf("active count = %d, want 1", len(h.active))
	}
	r := h.active[0]
	if r.JobKey != "db1:100" {
		t.Errorf("JobKey = %q, want %q", r.JobKey, "db1:100")
	}
	if r.JobName != "job1" {
		t.Errorf("JobName = %q, want %q", r.JobName, "job1")
	}
	if r.Database != "db1" {
		t.Errorf("Database = %q, want %q", r.Database, "db1")
	}
	if r.FirstAlertAt.IsZero() {
		t.Error("FirstAlertAt should not be zero")
	}
	if r.SendCount != 0 {
		t.Errorf("SendCount = %d, want 0", r.SendCount)
	}
	if !r.IsActive() {
		t.Error("new record should be active")
	}
}

func TestUpdateSendCount(t *testing.T) {
	h, _ := newTestHistory(t)

	h.AddRecord("db1:100", "job1", "db1", "")
	h.UpdateSendCount("db1:100")
	h.UpdateSendCount("db1:100")

	r := h.FindRecord("db1:100")
	if r == nil {
		t.Fatal("record not found")
	}
	if r.SendCount != 2 {
		t.Errorf("SendCount = %d, want 2", r.SendCount)
	}
}

func TestUpdateSendCount_NonExistentKey(t *testing.T) {
	h, _ := newTestHistory(t)
	// Should not panic.
	h.UpdateSendCount("nonexistent:999")
}

func TestMarkRecovered(t *testing.T) {
	h, dir := newTestHistory(t)

	h.AddRecord("db1:100", "job1", "db1", "error reason")
	h.UpdateSendCount("db1:100")

	// Small delay so duration > 0.
	time.Sleep(10 * time.Millisecond)
	h.MarkRecovered("db1:100", "state changed")

	// Should be removed from active.
	if len(h.active) != 0 {
		t.Errorf("active count = %d, want 0", len(h.active))
	}
	// Should be in archive.
	if len(h.archive) != 1 {
		t.Fatalf("archive count = %d, want 1", len(h.archive))
	}
	r := h.archive[0]
	if r.RecoveredAt.IsZero() {
		t.Error("RecoveredAt should not be zero")
	}
	if r.Duration() <= 0 {
		t.Errorf("Duration should be > 0, got %v", r.Duration())
	}
	if r.SendCount != 1 {
		t.Errorf("SendCount = %d, want 1", r.SendCount)
	}
	if r.LastReason != "state changed" {
		t.Errorf("LastReason = %q, want %q", r.LastReason, "state changed")
	}

	// Verify persisted to disk.
	data, err := os.ReadFile(filepath.Join(dir, archiveFile))
	if err != nil {
		t.Fatalf("read archive file: %v", err)
	}
	var records []model.AlertRecord
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("archive file records = %d, want 1", len(records))
	}
}

func TestMarkRecovered_NonExistentKey(t *testing.T) {
	h, _ := newTestHistory(t)
	// Should not panic.
	h.MarkRecovered("nonexistent:999", "test")
}

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()

	// Create and save records.
	h1 := &AlertHistory{
		dir:    dir,
		maxAge: 720 * time.Hour,
		logger: newTestLogger(),
	}
	h1.AddRecord("db1:100", "job1", "db1", "")
	h1.AddRecord("db2:200", "job2", "db2", "")
	h1.UpdateSendCount("db1:100")
	h1.Save()

	// Load into new instance.
	h2 := &AlertHistory{
		dir:    dir,
		maxAge: 720 * time.Hour,
		logger: newTestLogger(),
	}
	if err := h2.load(); err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(h2.active) != 2 {
		t.Fatalf("loaded active count = %d, want 2", len(h2.active))
	}
	r := h2.FindRecord("db1:100")
	if r == nil {
		t.Fatal("db1:100 not found after load")
	}
	if r.SendCount != 1 {
		t.Errorf("SendCount after load = %d, want 1", r.SendCount)
	}
}

func TestPurgeExpired(t *testing.T) {
	h, dir := newTestHistory(t)

	// Add a record with old recovery time.
	now := time.Now()
	h.archive = []model.AlertRecord{
		{
			JobKey:       "db1:100",
			FirstAlertAt: now.Add(-30 * 24 * time.Hour),
			RecoveredAt:  now.Add(-31 * 24 * time.Hour), // older than max_age=30d
		},
		{
			JobKey:       "db2:200",
			FirstAlertAt: now.Add(-2 * 24 * time.Hour),
			RecoveredAt:  now.Add(-1 * 24 * time.Hour), // within max_age
		},
	}
	h.dirty = true
	h.saveLocked()

	// Load fresh and purge.
	h2 := &AlertHistory{
		dir:    dir,
		maxAge: 720 * time.Hour, // 30 days
		logger: newTestLogger(),
	}
	h2.load()
	h2.purgeExpired()

	if len(h2.archive) != 1 {
		t.Fatalf("archive count after purge = %d, want 1", len(h2.archive))
	}
	if h2.archive[0].JobKey != "db2:200" {
		t.Errorf("remaining record = %q, want %q", h2.archive[0].JobKey, "db2:200")
	}
}

func TestGetActiveRecords(t *testing.T) {
	h, _ := newTestHistory(t)

	h.AddRecord("db1:100", "job1", "db1", "")
	h.AddRecord("db2:200", "job2", "db2", "")

	records := h.GetActiveRecords()
	if len(records) != 2 {
		t.Fatalf("active records = %d, want 2", len(records))
	}

	// Modifying the returned slice should not affect internal state.
	records[0].JobKey = "modified"
	r := h.FindRecord("db1:100")
	if r == nil {
		t.Fatal("internal record was affected by external modification")
	}
}

func TestFindRecord(t *testing.T) {
	h, _ := newTestHistory(t)

	h.AddRecord("db1:100", "job1", "db1", "")

	r := h.FindRecord("db1:100")
	if r == nil {
		t.Fatal("FindRecord returned nil")
	}
	if r.JobName != "job1" {
		t.Errorf("JobName = %q, want %q", r.JobName, "job1")
	}

	r2 := h.FindRecord("nonexistent:999")
	if r2 != nil {
		t.Error("FindRecord should return nil for nonexistent key")
	}
}

func TestAlertRecord_Duration(t *testing.T) {
	now := time.Now()

	// Active alert.
	active := model.AlertRecord{
		FirstAlertAt: now.Add(-2 * time.Hour),
	}
	if d := active.Duration(); d < 1*time.Hour+59*time.Minute {
		t.Errorf("active Duration should be ~2h, got %v", d)
	}
	if !active.IsActive() {
		t.Error("should be active")
	}

	// Recovered alert.
	recovered := model.AlertRecord{
		FirstAlertAt: now.Add(-3 * time.Hour),
		RecoveredAt:  now.Add(-1 * time.Hour),
	}
	if d := recovered.Duration(); d < 1*time.Hour+59*time.Minute || d > 2*time.Hour+1*time.Minute {
		t.Errorf("recovered Duration should be ~2h, got %v", d)
	}
	if recovered.IsActive() {
		t.Error("should not be active")
	}
}
