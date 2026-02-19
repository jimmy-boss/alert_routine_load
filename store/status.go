// @Author: Jimmy
// @DateTime: 2026/02/15

// Package store 提供告警状态和历史记录的持久化存储。
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// StatusStore 管理活跃告警状态的内存缓存与持久化。
type StatusStore struct {
	dir    string
	logger glog.HLoggerBase
	mu     sync.RWMutex
	items  map[string]*model.AlertStatus
	dirty  bool
}

// StatusOption 是 StatusStore 的函数选项。
type StatusOption func(*StatusStore)

// WithStatusLogger 注入 logger 实现。
func WithStatusLogger(logger glog.HLoggerBase) StatusOption {
	return func(s *StatusStore) {
		s.logger = logger
	}
}

// NewStatusStore 创建 StatusStore，从 dir 目录加载 active.json。
func NewStatusStore(dir string, opts ...StatusOption) (*StatusStore, error) {
	s := &StatusStore{
		dir:   dir,
		items: make(map[string]*model.AlertStatus),
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

// Get 返回指定 key 的告警状态副本，不存在返回 nil。
// 返回的是副本，修改不影响 store 内部数据。如需修改请使用 Update。
func (s *StatusStore) Get(key string) *model.AlertStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.items[key]; ok {
		copy := *v
		return &copy
	}
	return nil
}

// Set 设置指定 key 的告警状态。
func (s *StatusStore) Set(key string, status *model.AlertStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = status
	s.dirty = true
}

// Delete 删除指定 key 的告警状态。
func (s *StatusStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
	s.dirty = true
}

// Range 遍历所有告警状态的副本，回调返回 false 时中断。
// 回调操作的是副本，修改不会影响 store 中的原始数据。
func (s *StatusStore) Range(fn func(key string, status *model.AlertStatus) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.items {
		copy := *v
		if !fn(k, &copy) {
			break
		}
	}
}

// Update 获取指定 key 的状态副本，调用 fn 修改后写回 store。
// 返回 true 表示 key 存在且 fn 已执行。
func (s *StatusStore) Update(key string, fn func(st *model.AlertStatus)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.items[key]
	if !ok {
		return false
	}
	fn(st)
	s.dirty = true
	return true
}



// Len 返回当前存储的条目数。
func (s *StatusStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// CollectRecoveringCandidates 收集所有 State==StateRecovering 的条目副本。
// 不修改状态，调用方需在恢复通知发送成功后调用 Update + MarkRecovered。
func (s *StatusStore) CollectRecoveringCandidates() []model.AlertStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []model.AlertStatus
	for _, st := range s.items {
		if st.State == model.StateRecovering {
			result = append(result, *st)
		}
	}
	return result
}

// RemoveRecovered 移除所有 State==StateRecovered 的条目。
func (s *StatusStore) RemoveRecovered() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, st := range s.items {
		if st.State == model.StateRecovered {
			delete(s.items, k)
			s.dirty = true
		}
	}
}

// Save 将活跃告警状态持久化到 active.json。
// 仅在 dirty 标记为 true 时执行写入。
func (s *StatusStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

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
func (s *StatusStore) saveLocked() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	data, err := json.MarshalIndent(s.items, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal status: %w", err)
	}

	path := filepath.Join(s.dir, "active.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

// load 从 active.json 加载告警状态。
func (s *StatusStore) load() error {
	path := filepath.Join(s.dir, "active.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read active.json: %w", err)
	}

	var items map[string]*model.AlertStatus
	if err := json.Unmarshal(data, &items); err != nil {
		s.logger.Warn("解析 active.json 失败，将使用空状态", zap.Error(err))
		return nil
	}
	s.items = items
	return nil
}
