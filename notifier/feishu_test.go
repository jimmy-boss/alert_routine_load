// @Author: Jimmy
// @DateTime: 2026/02/15

package notifier

import (
	"testing"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
)

func TestGenSign(t *testing.T) {
	sign, err := genSign("", 1234567890)
	if err != nil {
		t.Fatalf("genSign error: %v", err)
	}
	if sign == "" {
		t.Fatal("genSign returned empty string")
	}
	sign2, _ := genSign("", 1234567890)
	if sign != sign2 {
		t.Errorf("genSign not deterministic: %q != %q", sign, sign2)
	}
}

func TestGenSign_DifferentSecret(t *testing.T) {
	sign1, _ := genSign("secret1", 1234567890)
	sign2, _ := genSign("secret2", 1234567890)
	if sign1 == sign2 {
		t.Error("different secrets should produce different signatures")
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input  time.Duration
		expect string
	}{
		{30 * time.Second, "30秒"},
		{90 * time.Second, "1分30秒"},
		{5 * time.Minute, "5分0秒"},
		{1 * time.Hour, "1小时0分钟"},
		{2*time.Hour + 15*time.Minute, "2小时15分钟"},
		{25 * time.Hour, "1天1小时"},
		{72*time.Hour + 30*time.Minute, "3天0小时"},
		{0, "0秒"},
	}
	for _, tt := range tests {
		got := formatDuration(tt.input)
		if got != tt.expect {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.expect)
		}
	}
}

func TestBuildCard_Fields(t *testing.T) {
	e := model.AlertEvent{
		JobID:       12345,
		JobName:     "test_job",
		Database:    "test_db",
		State:       "PAUSED",
		PauseTime:   "2026-01-01 10:00:00",
		Reason:      "test reason",
		ErrorDetail: "some error",
		Timestamp:   time.Date(2026, 1, 1, 10, 0, 0, 0, time.Local),
	}
	card := buildCard(e)

	header, ok := card["header"].(map[string]interface{})
	if !ok {
		t.Fatal("card missing header")
	}
	title, ok := header["title"].(map[string]interface{})
	if !ok {
		t.Fatal("header missing title")
	}
	content, ok := title["content"].(string)
	if !ok {
		t.Fatal("title missing content")
	}
	if content == "" {
		t.Error("title content should not be empty")
	}

	elements, ok := card["elements"].([]map[string]interface{})
	if !ok {
		t.Fatal("card missing elements")
	}
	if len(elements) < 2 {
		t.Errorf("elements count = %d, want >= 2", len(elements))
	}
}

func TestBuildCard_LagState(t *testing.T) {
	e := model.AlertEvent{
		JobID:     12345,
		JobName:   "test_job",
		Database:  "test_db",
		State:     "LAG",
		Timestamp: time.Now(),
	}
	card := buildCard(e)
	header := card["header"].(map[string]interface{})
	if header["template"] != "yellow" {
		t.Errorf("LAG template = %q, want %q", header["template"], "yellow")
	}
}

func TestBuildCard_PausedState(t *testing.T) {
	e := model.AlertEvent{
		JobID:     12345,
		JobName:   "test_job",
		Database:  "test_db",
		State:     "PAUSED",
		Timestamp: time.Now(),
	}
	card := buildCard(e)
	header := card["header"].(map[string]interface{})
	if header["template"] != "orange" {
		t.Errorf("PAUSED template = %q, want %q", header["template"], "orange")
	}
}

func TestBuildRecoveryCard_Fields(t *testing.T) {
	info := model.RecoveryInfo{
		Database:    "test_db",
		JobName:     "test_job",
		RecoveredAt: time.Now(),
		Duration:    2 * time.Hour,
	}
	card := buildRecoveryCard(info)

	header, ok := card["header"].(map[string]interface{})
	if !ok {
		t.Fatal("card missing header")
	}
	if header["template"] != "green" {
		t.Errorf("recovery template = %q, want %q", header["template"], "green")
	}

	elements, ok := card["elements"].([]map[string]interface{})
	if !ok {
		t.Fatal("card missing elements")
	}
	if len(elements) < 1 {
		t.Error("elements should not be empty")
	}
}

func TestValueOrDash(t *testing.T) {
	if valueOrDash("") != "-" {
		t.Errorf("valueOrDash(\"\") = %q, want %q", valueOrDash(""), "-")
	}
	if valueOrDash("  ") != "-" {
		t.Errorf("valueOrDash(\"  \") = %q, want %q", valueOrDash("  "), "-")
	}
	if valueOrDash("hello") != "hello" {
		t.Errorf("valueOrDash(\"hello\") = %q, want %q", valueOrDash("hello"), "hello")
	}
}
