// @Author: Jimmy
// @DateTime: 2026/02/15

package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// ArchiveStore 管理已恢复告警的历史记录。
type ArchiveStore struct {
	dir           string
	logger        glog.HLoggerBase
	mu            sync.Mutex
	records       []model.AlertRecord
	retentionDays int
	dirty         bool
}

// ArchiveOption 是 ArchiveStore 的函数选项。
type ArchiveOption func(*ArchiveStore)

// WithArchiveLogger 注入 logger 实现。
func WithArchiveLogger(logger glog.HLoggerBase) ArchiveOption {
	return func(s *ArchiveStore) {
		s.logger = logger
	}
}

// WithRetentionDays 设置保留天数。
func WithRetentionDays(days int) ArchiveOption {
	return func(s *ArchiveStore) {
		s.retentionDays = days
	}
}

// NewArchiveStore 创建 ArchiveStore，从 dir 目录加载 archive.json。
func NewArchiveStore(dir string, opts ...ArchiveOption) (*ArchiveStore, error) {
	s := &ArchiveStore{
		dir:           dir,
		retentionDays: 7,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = glog.GlobalLoggers["default"]
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Archive 将 AlertStatus 转为 AlertRecord 写入归档并持久化。
func (s *ArchiveStore) Archive(status model.AlertStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := model.AlertRecord{
		JobKey:       status.JobKey,
		JobName:      status.JobName,
		Database:     status.Database,
		FirstAlertAt: status.FirstAlertAt,
		LastSentAt:   status.LastSentAt,
		RecoveredAt:  status.RecoveredAt,
		SendCount:    status.SendCount,
		Source:       status.Source,
	}
	s.records = append(s.records, record)
	s.dirty = true

	if err := s.saveLocked(); err != nil {
		s.logger.Error("归档持久化失败", zap.Error(err))
	}
}

// Save 持久化到 archive.json，同时清理过期记录。
func (s *ArchiveStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.purgeExpired()

	if !s.dirty {
		return nil
	}

	if err := s.saveLocked(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

// saveLocked 在持锁状态下执行原子文件写入。
func (s *ArchiveStore) saveLocked() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	data, err := json.MarshalIndent(s.records, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal archive: %w", err)
	}

	path := filepath.Join(s.dir, "archive.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

// purgeExpired 清理超过保留天数的过期记录。
func (s *ArchiveStore) purgeExpired() {
	if s.retentionDays <= 0 {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -s.retentionDays)
	n := 0
	for _, r := range s.records {
		if r.RecoveredAt.After(cutoff) || r.RecoveredAt.IsZero() {
			s.records[n] = r
			n++
		}
	}
	if n < len(s.records) {
		s.records = s.records[:n]
		s.dirty = true
	}
}

// load 从 archive.json 加载历史记录。
func (s *ArchiveStore) load() error {
	path := filepath.Join(s.dir, "archive.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read archive.json: %w", err)
	}

	var records []model.AlertRecord
	if err := json.Unmarshal(data, &records); err != nil {
		s.logger.Warn("解析 archive.json 失败，将使用空记录", zap.Error(err))
		return nil
	}
	s.records = records
	return nil
}
