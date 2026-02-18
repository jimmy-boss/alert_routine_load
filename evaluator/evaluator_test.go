// @Author: Jimmy
// @DateTime: 2026/02/15

package evaluator

import (
	"testing"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
)

func TestCheckLag_ExceedsThreshold(t *testing.T) {
	job := model.RoutineLoadJob{Lag: `{"0":100,"1":200,"2":50}`}
	exceeded := checkLag(job, 80)
	if len(exceeded) != 2 {
		t.Fatalf("expected 2 exceeded partitions, got %d", len(exceeded))
	}
	if exceeded[0].PartitionID != "0" || exceeded[0].LagCount != 100 {
		t.Errorf("unexpected first: %+v", exceeded[0])
	}
	if exceeded[1].PartitionID != "1" || exceeded[1].LagCount != 200 {
		t.Errorf("unexpected second: %+v", exceeded[1])
	}
}

func TestCheckLag_BelowThreshold(t *testing.T) {
	job := model.RoutineLoadJob{Lag: `{"0":10,"1":20}`}
	exceeded := checkLag(job, 100)
	if len(exceeded) != 0 {
		t.Fatalf("expected 0 exceeded, got %d", len(exceeded))
	}
}

func TestCheckLag_EmptyLag(t *testing.T) {
	job := model.RoutineLoadJob{Lag: ""}
	exceeded := checkLag(job, 100)
	if len(exceeded) != 0 {
		t.Fatalf("expected 0 exceeded, got %d", len(exceeded))
	}
}

func TestComputeDelay_Initial(t *testing.T) {
	base := 5 * time.Minute
	d := computeDelay(0, base, 1.5, 1*time.Hour)
	if d != 0 {
		t.Errorf("sendCount=0: expected 0, got %v", d)
	}
}

func TestComputeDelay_ExponentialBackoff(t *testing.T) {
	base := 5 * time.Minute
	factor := 1.2
	max := 1 * time.Hour

	// sendCount=1: base = 5m
	d1 := computeDelay(1, base, factor, max)
	if d1 != base {
		t.Errorf("sendCount=1: expected %v, got %v", base, d1)
	}

	// sendCount=2: 5m * 1.2 = 6m
	d2 := computeDelay(2, base, factor, max)
	expected2 := 6 * time.Minute
	if d2 != expected2 {
		t.Errorf("sendCount=2: expected %v, got %v", expected2, d2)
	}

	// sendCount=3: 6m * 1.2 = 7.2m = 7m12s
	d3 := computeDelay(3, base, factor, max)
	expected3 := time.Duration(float64(6*time.Minute) * 1.2)
	if d3 != expected3 {
		t.Errorf("sendCount=3: expected %v, got %v", expected3, d3)
	}

	// sendCount=4: 7.2m * 1.2 = 8.64m
	d4 := computeDelay(4, base, factor, max)
	expected4 := time.Duration(float64(expected3) * 1.2)
	if d4 != expected4 {
		t.Errorf("sendCount=4: expected %v, got %v", expected4, d4)
	}
}

func TestComputeDelay_CapsAtMax(t *testing.T) {
	base := 5 * time.Minute
	factor := 2.0
	max := 15 * time.Minute

	// sendCount=1: 5m
	d1 := computeDelay(1, base, factor, max)
	if d1 != 5*time.Minute {
		t.Errorf("sendCount=1: expected 5m, got %v", d1)
	}

	// sendCount=2: 10m
	d2 := computeDelay(2, base, factor, max)
	if d2 != 10*time.Minute {
		t.Errorf("sendCount=2: expected 10m, got %v", d2)
	}

	// sendCount=3: min(20m, 15m) = 15m
	d3 := computeDelay(3, base, factor, max)
	if d3 != 15*time.Minute {
		t.Errorf("sendCount=3: expected 15m (capped), got %v", d3)
	}

	// sendCount=4: 仍然是 15m
	d4 := computeDelay(4, base, factor, max)
	if d4 != 15*time.Minute {
		t.Errorf("sendCount=4: expected 15m (capped), got %v", d4)
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
		got := isPaused(tt.state)
		if got != tt.want {
			t.Errorf("isPaused(%q) = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func TestIsRunning(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{"RUNNING", true},
		{"running", true},
		{"PAUSED", false},
		{"STOPPED", false},
		{"", false},
	}
	for _, tt := range tests {
		got := isRunning(tt.state)
		if got != tt.want {
			t.Errorf("isRunning(%q) = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func TestCheckLag_RecoveryThreshold(t *testing.T) {
	// lag=5000, recovery=3000 → exceeded（lag > recovery）
	job := model.RoutineLoadJob{Lag: `{"0":5000}`}
	exceeded := checkLag(job, 3000)
	if len(exceeded) != 1 {
		t.Errorf("expected 1 exceeded, got %d", len(exceeded))
	}

	// lag=2000, recovery=3000 → not exceeded（lag < recovery）
	exceeded = checkLag(job, 5000)
	if len(exceeded) != 0 {
		t.Errorf("expected 0 exceeded, got %d", len(exceeded))
	}
}
