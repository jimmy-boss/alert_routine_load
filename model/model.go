// @Author: Jimmy
// @DateTime: 2026/02/15

// Package model 定义 Routine Load 告警的核心领域类型。
package model

import (
	"encoding/json"
	"time"
)

// AlertState 告警状态枚举。
type AlertState int

const (
	StateAlerting   AlertState = iota // 告警中
	StateRecovering                   // 恢复中
	StateRecovered                    // 已恢复
)

// String 返回 AlertState 的可读名称。
func (s AlertState) String() string {
	switch s {
	case StateAlerting:
		return "alerting"
	case StateRecovering:
		return "recovering"
	case StateRecovered:
		return "recovered"
	default:
		return "unknown"
	}
}

// AlertStatus 跟踪单个 Job 的告警状态，支持跨重启持久化。
type AlertStatus struct {
	JobKey        string     `json:"job_key"` // "db:jobId"
	JobName       string     `json:"job_name"`
	Database      string     `json:"database"`
	Source        string     `json:"source"`         // "paused" 或 "lag"
	State         AlertState `json:"state"`          // 当前告警状态
	AlertActive   bool       `json:"alert_active"`   // 是否正在告警
	IsRecovery    bool       `json:"is_recovery"`    // 是否为恢复事件
	FirstAlertAt  time.Time  `json:"first_alert_at"` // 首次告警时间
	LastSentAt    time.Time  `json:"last_sent_at"`   // 最后发送时间
	RecoveredAt   time.Time  `json:"recovered_at"`   // 恢复时间，零值表示仍在告警
	RecoverySince time.Time  `json:"recovery_since"` // 首次满足恢复条件的时间（防抖用）
	SendCount     int        `json:"send_count"`     // 累计发送次数
}

// IsActive 返回当前告警是否仍在进行中。
func (s *AlertStatus) IsActive() bool {
	return s.State == StateAlerting || s.State == StateRecovering
}

// MarkRecovering 将状态标记为恢复中。
// RecoveredAt 在此处设置，记录恢复确认时间（stability_window 满足后），
// 用于 RecoveryInfo.Duration 和 Archive 归档。MarkRecovered 不更新此字段。
func (s *AlertStatus) MarkRecovering() {
	s.State = StateRecovering
	s.IsRecovery = true
	s.AlertActive = true
	s.RecoveredAt = time.Now()
}

// MarkRecovered 将状态标记为已恢复。
func (s *AlertStatus) MarkRecovered() {
	s.State = StateRecovered
	s.IsRecovery = false
	s.AlertActive = false
}

// RoutineLoadJob 表示 SHOW ROUTINE LOAD 返回的一行数据。
type RoutineLoadJob struct {
	ID                   int64
	Name                 string
	CreateTime           string
	PauseTime            string
	EndTime              string
	DataSourceType       string
	CurrentTaskNum       int
	JobProperties        string
	DataSourceProperties string
	CustomProperties     string
	Statistic            string
	Progress             string
	Lag                  string
	ReasonOfStateChanged string
	ErrorLogURLs         string
	State                string // RUNNING, PAUSED, STOPPED, CANCELLED
}

// AlertEvent 表示一条待发送的告警事件。
type AlertEvent struct {
	JobID          int64
	JobName        string
	Database       string
	State          string
	PauseTime      string
	Reason         string
	ErrorDetail    string // 聚合并截断的错误预览
	Timestamp      time.Time
	Duration       time.Duration // 告警持续时长
	TotalSendCount int           // 本次告警累计发送次数
}

// RecoveryInfo 表示恢复相关的信息。
type RecoveryInfo struct {
	JobKey      string        `json:"job_key"`
	JobName     string        `json:"job_name"`
	Database    string        `json:"database"`
	Source      string        `json:"source"`
	RecoveredAt time.Time     `json:"recovered_at"`
	Duration    time.Duration `json:"duration"`
	SendCount   int           `json:"send_count"`
}

// AlertRecord 表示一次完整的告警生命周期。
type AlertRecord struct {
	JobKey       string    `json:"job_key"`
	JobName      string    `json:"job_name"`
	Database     string    `json:"database"`
	FirstAlertAt time.Time `json:"first_alert_at"`
	LastSentAt   time.Time `json:"last_sent_at"`
	RecoveredAt  time.Time `json:"recovered_at"`
	SendCount    int       `json:"send_count"`
	LastReason   string    `json:"last_reason"`
	Source       string    `json:"source"`
}

// Duration 返回告警持续时长。
// 对于已恢复的告警: RecoveredAt - FirstAlertAt。
// 对于仍在进行的告警: now - FirstAlertAt。
func (r *AlertRecord) Duration() time.Duration {
	if !r.RecoveredAt.IsZero() {
		return r.RecoveredAt.Sub(r.FirstAlertAt)
	}
	return time.Since(r.FirstAlertAt)
}

// LagInfo 表示单个分区的延迟信息。
type LagInfo struct {
	PartitionID string
	LagCount    int64
}

// ParseLag 解析 SHOW ROUTINE LOAD 返回的 Lag JSON 字符串。
// 返回分区 ID 到延迟数的映射，输入为空或格式错误时返回 nil。
//
// 示例输入: '{"0":0,"1":80009,"2":0}'
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
