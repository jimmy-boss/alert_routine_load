// Package model defines the core domain types for Doris Routine Load alerting.
package model

import (
	"encoding/json"
	"time"
)

// LagInfo represents a single partition's lag that exceeds the threshold.
type LagInfo struct {
	PartitionID string
	LagCount    int64
}

// RoutineLoadJob represents a single row from SHOW ROUTINE LOAD.
type RoutineLoadJob struct {
	ID          int64
	Name        string
	CreateTime  string
	PauseTime   string
	EndTime     string
	DataSourceType string
	CurrentTaskNum int
	JobProperties string
	DataSourceProperties string
	CustomProperties string
	Statistic       string
	Progress        string
	Lag             string
	ReasonOfStateChanged string
	ErrorLogURLs    string
	State           string // RUNNING, PAUSED, STOPPED, CANCELLED
}

// AlertEvent is a single alert to be sent.
type AlertEvent struct {
	JobID          int64
	JobName        string
	Database       string
	State          string
	PauseTime      string
	Reason         string
	ErrorDetail    string        // aggregated + truncated error preview
	Timestamp      time.Time
	Duration       time.Duration // alert duration (from first alert to now/recovery)
	TotalSendCount int           // total alerts sent in this episode
}

// AlertStatus tracks send history for one job, used for interval / backoff.
type AlertStatus struct {
	FirstAlertAt time.Time // first time this job triggered an alert
	LastSentAt   time.Time
	SendCount    int
	NextDelaySec int
	Source       string // "paused" or "lag" — determines recovery behavior
}

// AlertRecord represents the full lifecycle of one alert episode.
// Persisted to JSON for history tracking.
type AlertRecord struct {
	JobKey       string    `json:"job_key"`        // "db:jobId"
	JobName      string    `json:"job_name"`
	Database     string    `json:"database"`
	FirstAlertAt time.Time `json:"first_alert_at"` // first alert time
	LastSentAt   time.Time `json:"last_sent_at"`   // last alert send time (for backoff across restarts)
	RecoveredAt  time.Time `json:"recovered_at"`   // recovery time, zero = still active
	SendCount    int       `json:"send_count"`     // total alerts sent
	LastReason   string    `json:"last_reason"`    // last state change reason
	Source       string    `json:"source"`         // "paused" or "lag"
}

// Duration returns the alert duration.
// For active alerts: now - FirstAlertAt.
// For recovered alerts: RecoveredAt - FirstAlertAt.
func (r *AlertRecord) Duration() time.Duration {
	if !r.RecoveredAt.IsZero() {
		return r.RecoveredAt.Sub(r.FirstAlertAt)
	}
	return time.Since(r.FirstAlertAt)
}

// IsActive returns true if the alert is still ongoing.
func (r *AlertRecord) IsActive() bool {
	return r.RecoveredAt.IsZero()
}

// ParseLag parses the Lag JSON string from SHOW ROUTINE LOAD into a map of
// partitionID → lagCount. Returns nil if the input is empty or malformed.
//
// Example input: '{"0":0,"1":80009,"2":0}'
func ParseLag(raw string) map[string]int64 {
	if raw == "" {
		return nil
	}
	var m map[string]int64
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	return m
}
