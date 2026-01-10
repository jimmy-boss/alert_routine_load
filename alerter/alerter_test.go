package alerter

import (
	"testing"
	"time"

	"github.com/jimmy-boss/alert_routine_load/model"
)

func TestComputeDelay(t *testing.T) {
	initial := 5 * time.Minute
	max := 60 * time.Minute
	factor := 2.0

	tests := []struct {
		sendCount int
		want      time.Duration
	}{
		{0, 5 * time.Minute},   // initial
		{1, 10 * time.Minute},  // 5 * 2
		{2, 20 * time.Minute},  // 10 * 2
		{3, 40 * time.Minute},  // 20 * 2
		{4, 60 * time.Minute},  // 40 * 2, capped at max
		{5, 60 * time.Minute},  // still capped
		{10, 60 * time.Minute}, // still capped
	}

	for _, tt := range tests {
		got := computeDelay(tt.sendCount, initial, max, factor)
		if got != tt.want {
			t.Errorf("computeDelay(%d) = %v, want %v", tt.sendCount, got, tt.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input  string
		max    int
		expect string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "hello...(truncated)"},
		{"", 5, ""},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc...(truncated)"},
	}
	for _, tt := range tests {
		got := truncate(tt.input, tt.max)
		if got != tt.expect {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.max, got, tt.expect)
		}
	}
}

func TestSplitURLs(t *testing.T) {
	tests := []struct {
		input string
		count int
	}{
		{"http://a.com", 1},
		{"http://a.com,http://b.com", 2},
		{"http://a.com;http://b.com", 2},
		{"http://a.com\nhttp://b.com", 2},
		{"http://a.com, http://b.com; http://c.com\nhttp://d.com", 4},
		{"", 0},
	}
	for _, tt := range tests {
		got := splitURLs(tt.input)
		if len(got) != tt.count {
			t.Errorf("splitURLs(%q) len = %d, want %d", tt.input, len(got), tt.count)
		}
	}
}

func TestIsPaused(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{"PAUSED", true},
		{"PAUSE", true},
		{"paused", true},
		{"RUNNING", false},
		{"STOPPED", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isPaused(tt.state); got != tt.want {
			t.Errorf("isPaused(%q) = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func TestUpdateStatus(t *testing.T) {
	a := &Alerter{
		status: make(map[string]*model.AlertStatus),
	}

	key := "testdb:123"

	// First update: creates new entry.
	a.UpdateStatus(key, "testdb", "job1", "test reason")
	st, ok := a.status[key]
	if !ok {
		t.Fatalf("expected status entry for key %q", key)
	}
	if st.SendCount != 1 {
		t.Errorf("SendCount = %d, want 1", st.SendCount)
	}
	if st.LastSentAt.IsZero() {
		t.Error("LastSentAt should not be zero after update")
	}

	// Second update: increments count.
	a.UpdateStatus(key, "testdb", "job1", "test reason")
	st = a.status[key]
	if st.SendCount != 2 {
		t.Errorf("SendCount = %d, want 2", st.SendCount)
	}
}

func TestRemoveStale(t *testing.T) {
	a := &Alerter{
		status: make(map[string]*model.AlertStatus),
	}

	// Seed status with two entries.
	a.status["db1:1"] = &model.AlertStatus{SendCount: 3}
	a.status["db1:2"] = &model.AlertStatus{SendCount: 1}
	a.status["db2:3"] = &model.AlertStatus{SendCount: 2}

	// Only db1:1 is still paused; others should be removed.
	dbJobs := map[string][]model.RoutineLoadJob{
		"db1": {
			{ID: 1, State: "PAUSED"},
			{ID: 2, State: "RUNNING"},
		},
		"db2": {
			{ID: 3, State: "STOPPED"},
		},
	}

	recovered := a.RemoveStale(dbJobs)

	if _, ok := a.status["db1:1"]; !ok {
		t.Error("db1:1 should still exist (paused)")
	}
	if _, ok := a.status["db1:2"]; ok {
		t.Error("db1:2 should have been removed (running)")
	}
	if _, ok := a.status["db2:3"]; ok {
		t.Error("db2:3 should have been removed (stopped)")
	}
	if len(recovered) != 2 {
		t.Errorf("recovered count = %d, want 2", len(recovered))
	}
}

func TestCheckLag_Exceeded(t *testing.T) {
	job := model.RoutineLoadJob{
		Lag: `{"0":0,"1":80009,"2":0,"4":80008,"7":80019}`,
	}
	result := checkLag(job, 10000)
	if len(result) != 3 {
		t.Fatalf("exceeded count = %d, want 3, got %v", len(result), result)
	}
	// Should be sorted by partition ID.
	if result[0].PartitionID != "1" || result[0].LagCount != 80009 {
		t.Errorf("result[0] = %+v", result[0])
	}
	if result[1].PartitionID != "4" || result[1].LagCount != 80008 {
		t.Errorf("result[1] = %+v", result[1])
	}
	if result[2].PartitionID != "7" || result[2].LagCount != 80019 {
		t.Errorf("result[2] = %+v", result[2])
	}
}

func TestCheckLag_NotExceeded(t *testing.T) {
	job := model.RoutineLoadJob{
		Lag: `{"0":0,"1":5000,"2":3000}`,
	}
	result := checkLag(job, 10000)
	if len(result) != 0 {
		t.Errorf("should not exceed, got %v", result)
	}
}

func TestCheckLag_EmptyLag(t *testing.T) {
	job := model.RoutineLoadJob{Lag: ""}
	result := checkLag(job, 10000)
	if len(result) != 0 {
		t.Errorf("empty lag should return nil, got %v", result)
	}
}

func TestCheckLag_InvalidJSON(t *testing.T) {
	job := model.RoutineLoadJob{Lag: "invalid"}
	result := checkLag(job, 10000)
	if len(result) != 0 {
		t.Errorf("invalid json should return nil, got %v", result)
	}
}

func TestFormatLagSummary(t *testing.T) {
	lags := []model.LagInfo{
		{PartitionID: "1", LagCount: 80009},
		{PartitionID: "7", LagCount: 80019},
	}
	result := formatLagSummary(lags)
	expected := "partition 1=80009, partition 7=80019"
	if result != expected {
		t.Errorf("formatLagSummary = %q, want %q", result, expected)
	}
}
