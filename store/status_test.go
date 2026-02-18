// @Author: Jimmy
// @DateTime: 2026/02/15

package store

import (
	"os"
	"testing"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
)

func newTestStatusStore(t *testing.T) (*StatusStore, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStatusStore(dir)
	if err != nil {
		t.Fatalf("NewStatusStore: %v", err)
	}
	return s, dir
}

func TestSetGetDelete(t *testing.T) {
	s, _ := newTestStatusStore(t)

	status := &model.AlertStatus{
		JobKey:       "test_db:1",
		JobName:      "test_job",
		Database:     "test_db",
		Source:       "lag",
		State:        model.StateAlerting,
		AlertActive:  true,
		FirstAlertAt: time.Now(),
		SendCount:    1,
	}

	// 初始状态为空
	if s.Len() != 0 {
		t.Fatalf("expected len 0, got %d", s.Len())
	}

	// Set + Get
	s.Set("test_db:1", status)
	got := s.Get("test_db:1")
	if got == nil {
		t.Fatal("expected non-nil after Set")
	}
	if got.JobKey != "test_db:1" {
		t.Fatalf("expected job_key test_db:1, got %s", got.JobKey)
	}
	if s.Len() != 1 {
		t.Fatalf("expected len 1, got %d", s.Len())
	}

	// Get 不存在的 key
	if s.Get("nonexistent") != nil {
		t.Fatal("expected nil for nonexistent key")
	}

	// Delete
	s.Delete("test_db:1")
	if s.Get("test_db:1") != nil {
		t.Fatal("expected nil after Delete")
	}
	if s.Len() != 0 {
		t.Fatalf("expected len 0 after delete, got %d", s.Len())
	}
}

func TestRange(t *testing.T) {
	s, _ := newTestStatusStore(t)

	for i := 0; i < 3; i++ {
		key := string(rune('a' + i))
		s.Set(key, &model.AlertStatus{
			JobKey: key,
			State:  model.StateAlerting,
		})
	}

	count := 0
	s.Range(func(key string, status *model.AlertStatus) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("expected Range 3, got %d", count)
	}
}

func TestCollectRecoveringCandidates(t *testing.T) {
	s, _ := newTestStatusStore(t)

	s.Set("alerting", &model.AlertStatus{
		JobKey: "alerting", State: model.StateAlerting,
	})
	s.Set("recovering1", &model.AlertStatus{
		JobKey: "recovering1", State: model.StateRecovering,
	})
	s.Set("recovering2", &model.AlertStatus{
		JobKey: "recovering2", State: model.StateRecovering,
	})
	s.Set("recovered", &model.AlertStatus{
		JobKey: "recovered", State: model.StateRecovered,
	})

	// CollectRecoveringCandidates 只收集不标记
	candidates := s.CollectRecoveringCandidates()
	if len(candidates) != 2 {
		t.Fatalf("expected 2 recovering candidates, got %d", len(candidates))
	}

	// 收集后状态不变，仍为 recovering
	for _, r := range candidates {
		st := s.Get(r.JobKey)
		if st == nil {
			t.Fatalf("expected %s to exist", r.JobKey)
		}
		if st.State != model.StateRecovering {
			t.Fatalf("expected %s state=recovering (unchanged), got %s", r.JobKey, st.State)
		}
	}

	// alerting 和 recovered 不受影响
	if st := s.Get("alerting"); st.State != model.StateAlerting {
		t.Fatal("alerting should remain alerting")
	}
	if st := s.Get("recovered"); st.State != model.StateRecovered {
		t.Fatal("recovered should remain recovered")
	}

	// 模拟发送成功后标记 recovered
	for _, r := range candidates {
		s.Update(r.JobKey, func(st *model.AlertStatus) {
			st.MarkRecovered()
		})
	}

	// 再次调用应返回空（已标记为 recovered）
	candidates = s.CollectRecoveringCandidates()
	if len(candidates) != 0 {
		t.Fatalf("expected 0 on second call, got %d", len(candidates))
	}
}

func TestRemoveRecovered(t *testing.T) {
	s, _ := newTestStatusStore(t)

	s.Set("alerting", &model.AlertStatus{
		JobKey: "alerting", State: model.StateAlerting,
	})
	s.Set("recovered1", &model.AlertStatus{
		JobKey: "recovered1", State: model.StateRecovered,
	})
	s.Set("recovered2", &model.AlertStatus{
		JobKey: "recovered2", State: model.StateRecovered,
	})

	s.RemoveRecovered()

	if s.Len() != 1 {
		t.Fatalf("expected len 1 after remove, got %d", s.Len())
	}
	if s.Get("alerting") == nil {
		t.Fatal("alerting should not be removed")
	}
	if s.Get("recovered1") != nil {
		t.Fatal("recovered1 should be removed")
	}
	if s.Get("recovered2") != nil {
		t.Fatal("recovered2 should be removed")
	}
}

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewStatusStore(dir)
	if err != nil {
		t.Fatalf("NewStatusStore: %v", err)
	}
	s1.Set("job1", &model.AlertStatus{
		JobKey: "job1", State: model.StateAlerting, SendCount: 5,
	})
	if err := s1.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// 重新加载验证持久化
	s2, err := NewStatusStore(dir)
	if err != nil {
		t.Fatalf("NewStatusStore reload: %v", err)
	}
	got := s2.Get("job1")
	if got == nil {
		t.Fatal("expected job1 after reload")
	}
	if got.SendCount != 5 {
		t.Fatalf("expected send_count 5, got %d", got.SendCount)
	}
}

func TestSaveNotDirty(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStatusStore(dir)
	if err != nil {
		t.Fatalf("NewStatusStore: %v", err)
	}

	// 未修改时 Save 应不写文件
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	path := dir + "/active.json"
	if _, err := os.Stat(path); err == nil {
		t.Fatal("expected no active.json when not dirty")
	}
}
