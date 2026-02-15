// @Author: Jimmy
// @DateTime: 2026/02/15

package model

import (
	"testing"
	"time"
)

func TestMarkRecovering(t *testing.T) {
	s := &AlertStatus{
		JobKey:      "test_db:1",
		State:       StateAlerting,
		AlertActive: true,
	}

	s.MarkRecovering()

	if s.State != StateRecovering {
		t.Errorf("状态应为 Recovering，实际为 %v", s.State)
	}
	if !s.IsRecovery {
		t.Error("IsRecovery 应为 true")
	}
	if !s.AlertActive {
		t.Error("AlertActive 应为 true")
	}
	if s.RecoveredAt.IsZero() {
		t.Error("RecoveredAt 不应为零值")
	}
}

func TestMarkRecovered(t *testing.T) {
	now := time.Now()
	s := &AlertStatus{
		JobKey:      "test_db:1",
		State:       StateRecovering,
		AlertActive: true,
		IsRecovery:  true,
		RecoveredAt: now,
	}

	s.MarkRecovered()

	if s.State != StateRecovered {
		t.Errorf("状态应为 Recovered，实际为 %v", s.State)
	}
	if s.IsRecovery {
		t.Error("IsRecovery 应为 false")
	}
	if s.AlertActive {
		t.Error("AlertActive 应为 false")
	}
	if s.RecoveredAt != now {
		t.Error("MarkRecovered 不应修改 RecoveredAt")
	}
}

func TestIsActive(t *testing.T) {
	tests := []struct {
		name   string
		state  AlertState
		expect bool
	}{
		{"alerting 状态", StateAlerting, true},
		{"recovering 状态", StateRecovering, true},
		{"recovered 状态", StateRecovered, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &AlertStatus{State: tt.state}
			if got := s.IsActive(); got != tt.expect {
				t.Errorf("IsActive() = %v, 期望 %v", got, tt.expect)
			}
		})
	}
}

func TestParseLag(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantNil bool
		wantLen int
	}{
		{
			name:    "正常输入",
			input:   `{"0":0,"1":80009,"2":0,"3":0,"4":80008}`,
			wantNil: false,
			wantLen: 5,
		},
		{
			name:    "空字符串",
			input:   "",
			wantNil: true,
		},
		{
			name:    "无效 JSON",
			input:   "invalid",
			wantNil: true,
		},
		{
			name:    "全零延迟",
			input:   `{"0":0,"1":0}`,
			wantNil: false,
			wantLen: 2,
		},
		{
			name:    "单分区",
			input:   `{"3":99999}`,
			wantNil: false,
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseLag(tt.input)
			if tt.wantNil {
				if result != nil {
					t.Errorf("ParseLag(%q) = %v, 期望 nil", tt.input, result)
				}
				return
			}
			if result == nil {
				t.Fatalf("ParseLag(%q) = nil, 期望非 nil", tt.input)
			}
			if len(result) != tt.wantLen {
				t.Errorf("ParseLag(%q) len = %d, 期望 %d", tt.input, len(result), tt.wantLen)
			}
		})
	}
}

func TestParseLag_Values(t *testing.T) {
	input := `{"0":0,"1":80009,"2":0,"4":80008,"7":80019}`
	result := ParseLag(input)
	if result == nil {
		t.Fatal("ParseLag 返回 nil")
	}
	if result["1"] != 80009 {
		t.Errorf("partition 1 = %d, 期望 80009", result["1"])
	}
	if result["4"] != 80008 {
		t.Errorf("partition 4 = %d, 期望 80008", result["4"])
	}
	if result["7"] != 80019 {
		t.Errorf("partition 7 = %d, 期望 80019", result["7"])
	}
	if result["0"] != 0 {
		t.Errorf("partition 0 = %d, 期望 0", result["0"])
	}
}

func TestAlertRecord_Duration_Recovered(t *testing.T) {
	r := &AlertRecord{
		FirstAlertAt: time.Now().Add(-2 * time.Hour),
		RecoveredAt:  time.Now(),
	}

	d := r.Duration()
	if d < 1*time.Hour || d > 3*time.Hour {
		t.Errorf("Duration() = %v, 期望约 2 小时", d)
	}
}

func TestAlertRecord_Duration_Active(t *testing.T) {
	r := &AlertRecord{
		FirstAlertAt: time.Now().Add(-30 * time.Minute),
	}

	d := r.Duration()
	if d < 25*time.Minute || d > 35*time.Minute {
		t.Errorf("Duration() = %v, 期望约 30 分钟", d)
	}
}

func TestAlertState_String(t *testing.T) {
	tests := []struct {
		state AlertState
		want  string
	}{
		{StateAlerting, "alerting"},
		{StateRecovering, "recovering"},
		{StateRecovered, "recovered"},
		{AlertState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("AlertState(%d).String() = %q, 期望 %q", tt.state, got, tt.want)
		}
	}
}
