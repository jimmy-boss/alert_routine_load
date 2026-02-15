// @Author: Jimmy
// @DateTime: 2026/02/15

package store

import (
	"testing"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
)

func TestArchive(t *testing.T) {
	dir := t.TempDir()
	s, err := NewArchiveStore(dir)
	if err != nil {
		t.Fatalf("NewArchiveStore: %v", err)
	}

	status := model.AlertStatus{
		JobKey:       "test_db:1",
		JobName:      "test_job",
		Database:     "test_db",
		Source:       "lag",
		State:        model.StateRecovered,
		FirstAlertAt: time.Now().Add(-10 * time.Minute),
		LastSentAt:   time.Now().Add(-2 * time.Minute),
		RecoveredAt:  time.Now(),
		SendCount:    3,
	}

	s.Archive(status)

	// 验证持久化
	s2, err := NewArchiveStore(dir)
	if err != nil {
		t.Fatalf("NewArchiveStore reload: %v", err)
	}

	if len(s2.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(s2.records))
	}
	r := s2.records[0]
	if r.JobKey != "test_db:1" {
		t.Fatalf("expected job_key test_db:1, got %s", r.JobKey)
	}
	if r.JobName != "test_job" {
		t.Fatalf("expected job_name test_job, got %s", r.JobName)
	}
	if r.Database != "test_db" {
		t.Fatalf("expected database test_db, got %s", r.Database)
	}
	if r.Source != "lag" {
		t.Fatalf("expected source lag, got %s", r.Source)
	}
	if r.SendCount != 3 {
		t.Fatalf("expected send_count 3, got %d", r.SendCount)
	}
	if r.RecoveredAt.IsZero() {
		t.Fatal("expected non-zero recovered_at")
	}
}

func TestArchivePurgeExpired(t *testing.T) {
	dir := t.TempDir()
	s, err := NewArchiveStore(dir, WithRetentionDays(7))
	if err != nil {
		t.Fatalf("NewArchiveStore: %v", err)
	}

	// 新记录：应保留
	s.Archive(model.AlertStatus{
		JobKey:      "recent",
		RecoveredAt: time.Now(),
	})

	// 手动插入一条过期记录
	s.mu.Lock()
	s.records = append(s.records, model.AlertRecord{
		JobKey:      "expired",
		RecoveredAt: time.Now().AddDate(0, 0, -30),
	})
	s.dirty = true
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// 重新加载验证清理结果
	s2, err := NewArchiveStore(dir, WithRetentionDays(7))
	if err != nil {
		t.Fatalf("NewArchiveStore reload: %v", err)
	}

	if len(s2.records) != 1 {
		t.Fatalf("expected 1 record after purge, got %d", len(s2.records))
	}
	if s2.records[0].JobKey != "recent" {
		t.Fatalf("expected recent record, got %s", s2.records[0].JobKey)
	}
}
