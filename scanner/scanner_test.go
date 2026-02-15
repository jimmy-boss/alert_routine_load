package scanner

import (
	"testing"
)

func TestPick(t *testing.T) {
	m := map[string]string{
		"Name":  "test_job",
		"name":  "test_job_lower",
		"State": "PAUSED",
	}

	tests := []struct {
		keys []string
		want string
	}{
		{[]string{"Name"}, "test_job"},
		{[]string{"name"}, "test_job_lower"},
		{[]string{"State"}, "PAUSED"},
		{[]string{"Missing"}, ""},
		{[]string{"Missing", "Name"}, "test_job"},
		{[]string{"Missing", "Missing2"}, ""},
		{[]string{"Name", "name"}, "test_job"},
	}
	for _, tt := range tests {
		got := pick(m, tt.keys...)
		if got != tt.want {
			t.Errorf("pick(%v) = %q, want %q", tt.keys, got, tt.want)
		}
	}
}

func TestPick_TrimSpace(t *testing.T) {
	m := map[string]string{"Name": "  test_job  "}
	got := pick(m, "Name")
	if got != "test_job" {
		t.Errorf("pick with spaces = %q, want %q", got, "test_job")
	}
}

func TestParseJob_Normal(t *testing.T) {
	m := map[string]string{
		"Id":                      "12345",
		"Name":                    "test_job",
		"State":                   "PAUSED",
		"CreateTime":              "2026-01-01 10:00:00",
		"PauseTime":               "2026-01-01 11:00:00",
		"EndTime":                 "",
		"DataSourceType":          "KAFKA",
		"CurrentTaskNum":          "3",
		"JobProperties":           "{}",
		"DataSourceProperties":    "{}",
		"CustomProperties":        "{}",
		"Statistic":               "{}",
		"Progress":                "{}",
		"Lag":                     `{"0":100,"1":200}`,
		"ReasonOfStateChanged":    "some reason",
		"ErrorLogUrls":            "http://example.com/error",
	}

	job := parseJob(m)

	if job.ID != 12345 {
		t.Errorf("ID = %d, want 12345", job.ID)
	}
	if job.Name != "test_job" {
		t.Errorf("Name = %q, want %q", job.Name, "test_job")
	}
	if job.State != "PAUSED" {
		t.Errorf("State = %q, want %q", job.State, "PAUSED")
	}
	if job.CreateTime != "2026-01-01 10:00:00" {
		t.Errorf("CreateTime = %q", job.CreateTime)
	}
	if job.PauseTime != "2026-01-01 11:00:00" {
		t.Errorf("PauseTime = %q", job.PauseTime)
	}
	if job.DataSourceType != "KAFKA" {
		t.Errorf("DataSourceType = %q", job.DataSourceType)
	}
	if job.CurrentTaskNum != 3 {
		t.Errorf("CurrentTaskNum = %d, want 3", job.CurrentTaskNum)
	}
	if job.Lag != `{"0":100,"1":200}` {
		t.Errorf("Lag = %q", job.Lag)
	}
	if job.ReasonOfStateChanged != "some reason" {
		t.Errorf("ReasonOfStateChanged = %q", job.ReasonOfStateChanged)
	}
	if job.ErrorLogURLs != "http://example.com/error" {
		t.Errorf("ErrorLogURLs = %q", job.ErrorLogURLs)
	}
}

func TestParseJob_AlternativeColumnNames(t *testing.T) {
	m := map[string]string{
		"id":                       "99",
		"name":                     "alt_job",
		"state":                    "RUNNING",
		"job_id":                   "99",
		"create_time":              "2026-01-01",
		"data_source_type":         "KAFKA",
		"current_task_num":         "1",
		"reason_of_state_changed":  "ok",
		"error_log_urls":           "http://err",
	}

	job := parseJob(m)

	if job.ID != 99 {
		t.Errorf("ID = %d, want 99", job.ID)
	}
	if job.Name != "alt_job" {
		t.Errorf("Name = %q", job.Name)
	}
	if job.State != "RUNNING" {
		t.Errorf("State = %q", job.State)
	}
}

func TestParseJob_MissingFields(t *testing.T) {
	m := map[string]string{
		"Name": "minimal_job",
	}

	job := parseJob(m)

	if job.ID != 0 {
		t.Errorf("ID = %d, want 0", job.ID)
	}
	if job.Name != "minimal_job" {
		t.Errorf("Name = %q", job.Name)
	}
	if job.CurrentTaskNum != 0 {
		t.Errorf("CurrentTaskNum = %d, want 0", job.CurrentTaskNum)
	}
}

func TestParseJob_InvalidID(t *testing.T) {
	m := map[string]string{
		"Id":   "not_a_number",
		"Name": "bad_job",
	}

	job := parseJob(m)

	if job.ID != 0 {
		t.Errorf("ID = %d, want 0 (invalid input)", job.ID)
	}
}

func TestParseJob_InvalidTaskNum(t *testing.T) {
	m := map[string]string{
		"CurrentTaskNum": "abc",
		"Name":           "bad_job",
	}

	job := parseJob(m)

	if job.CurrentTaskNum != 0 {
		t.Errorf("CurrentTaskNum = %d, want 0 (invalid input)", job.CurrentTaskNum)
	}
}

func TestParseJob_JobIdColumn(t *testing.T) {
	m := map[string]string{
		"JobId": "555",
		"Name":  "jobid_test",
	}

	job := parseJob(m)
	if job.ID != 555 {
		t.Errorf("ID = %d, want 555 (from JobId column)", job.ID)
	}
}

func TestParseJob_ReasonFallback(t *testing.T) {
	m := map[string]string{
		"Name":    "msg_test",
		"Message": "fallback reason",
	}

	job := parseJob(m)
	if job.ReasonOfStateChanged != "fallback reason" {
		t.Errorf("ReasonOfStateChanged = %q, want fallback", job.ReasonOfStateChanged)
	}
}
