// @Author: Jimmy
// @DateTime: 2026/02/15

// Package evaluator 实现告警评估引擎，负责状态流转和告警决策。
package evaluator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/config"
	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/alert_routine_load/v2/store"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// Decision 表示一条告警决策结果。
type Decision struct {
	Event  model.AlertEvent
	Action string // "send" 或 "skip"
	Reason string // 跳过原因
	Key    string // status 存储键，发送成功后用于更新状态
}

// Option 是 Evaluator 的函数选项。
type Option func(*Evaluator)

// WithLogger 注入 logger 实现。
func WithLogger(logger glog.HLoggerBase) Option {
	return func(e *Evaluator) {
		e.logger = logger
	}
}

// Evaluator 告警评估引擎。
// 注意：非并发安全，调用方需保证 Reconcile/Evaluate/UpdateAfterSend 不被并发调用。
type Evaluator struct {
	cfg    *config.Config
	store  *store.StatusStore
	client *http.Client
	logger glog.HLoggerBase
}

// New 创建 Evaluator。
func New(cfg *config.Config, st *store.StatusStore, opts ...Option) *Evaluator {
	e := &Evaluator{
		cfg:    cfg,
		store:  st,
		client: &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.logger == nil {
		e.logger = glog.GlobalLoggers["default"]
	}
	return e
}

// Reconcile 遍历 store 中 alerting 的条目，执行状态流转：
// - lag source: lag < recovery 阈值且持续 stability_window → mark recovering
// - paused source: job 不再 paused 且持续 stability_window → mark recovering
// - job 消失 → mark recovering
func (e *Evaluator) Reconcile(dbJobs map[string][]model.RoutineLoadJob) {
	jobLookup := make(map[string]model.RoutineLoadJob)
	for dbName, jobs := range dbJobs {
		for _, job := range jobs {
			key := fmt.Sprintf("%s:%d", dbName, job.ID)
			jobLookup[key] = job
		}
	}

	stabilityWindow := e.cfg.Alert.Lag.StabilityWindow.Duration
	now := time.Now()

	// 先用 Range（RLock）收集需要操作的 key
	var toRecover []string
	var toSetRecoverySince []string
	var toClearRecoverySince []string

	e.store.Range(func(key string, st *model.AlertStatus) bool {
		if st.State != model.StateAlerting {
			return true
		}

		job, exists := jobLookup[key]
		if !exists {
			toRecover = append(toRecover, key)
			return true
		}

		dbName := ""
		if idx := strings.Index(key, ":"); idx > 0 {
			dbName = key[:idx]
		}
		lag := e.cfg.GetEffective(dbName, job.Name)

		shouldRecover := false
		switch st.Source {
		case "lag":
			exceeded := checkLag(job, lag.Recovery)
			shouldRecover = len(exceeded) == 0
		case "paused":
			shouldRecover = !isPaused(job.State)
		}

		if shouldRecover {
			if st.RecoverySince.IsZero() {
				toSetRecoverySince = append(toSetRecoverySince, key)
			} else if now.Sub(st.RecoverySince) >= stabilityWindow {
				toRecover = append(toRecover, key)
			}
		} else {
			if !st.RecoverySince.IsZero() {
				toClearRecoverySince = append(toClearRecoverySince, key)
			}
		}
		return true
	})

	// Range 结束后，用 Update（WLock）批量修改
	for _, key := range toSetRecoverySince {
		e.store.Update(key, func(st *model.AlertStatus) {
			st.RecoverySince = now
		})
	}
	for _, key := range toClearRecoverySince {
		e.store.Update(key, func(st *model.AlertStatus) {
			st.RecoverySince = time.Time{}
		})
	}
	for _, key := range toRecover {
		e.store.Update(key, func(st *model.AlertStatus) {
			st.MarkRecovering()
		})
	}
}

// Evaluate 评估每个 job 是否需要告警，返回决策列表。
func (e *Evaluator) Evaluate(ctx context.Context, dbJobs map[string][]model.RoutineLoadJob) []Decision {
	var decisions []Decision

	for dbName, jobs := range dbJobs {
		for _, job := range jobs {
			key := fmt.Sprintf("%s:%d", dbName, job.ID)
			lag := e.cfg.GetEffective(dbName, job.Name)

			needAlert := isPaused(job.State) || (isRunning(job.State) && lag.Threshold > 0)
			if !needAlert {
				continue
			}

			d := e.evaluateOne(ctx, key, dbName, job)
			decisions = append(decisions, d)
		}
	}
	return decisions
}

// evaluateOne 评估单个 job：退避检查、lag 检查、创建/更新 status。
// 关键：仅在确认需要告警时才创建 status。
func (e *Evaluator) evaluateOne(ctx context.Context, key, dbName string, job model.RoutineLoadJob) Decision {
	lag := e.cfg.GetEffective(dbName, job.Name)

	// GetOrCreate 原子获取或创建，避免 TOCTOU 竞态
	now := time.Now()
	st, isNew := e.store.GetOrCreate(key, func() *model.AlertStatus {
		return &model.AlertStatus{
			JobKey:       key,
			JobName:      job.Name,
			Database:     dbName,
			State:        model.StateAlerting,
			AlertActive:  true,
			FirstAlertAt: now,
		}
	})

	// MaxSendCount 检查（最先执行，避免后续计算浪费）
	if !isNew && st.SendCount >= e.cfg.Alert.Lag.MaxSendCount {
		return Decision{
			Action: "skip",
			Reason: fmt.Sprintf("max send count reached (%d)", e.cfg.Alert.Lag.MaxSendCount),
		}
	}

	// 指数退避检查：delay = base * factor^(sendCount-1)，上限为 maxInterval
	if !isNew && !st.LastSentAt.IsZero() {
		delay := computeDelay(
			st.SendCount,
			e.cfg.Alert.Lag.AlertInterval.Duration,
			e.cfg.Alert.Lag.BackoffFactor,
			e.cfg.Alert.Lag.MaxInterval.Duration,
		)
		elapsed := time.Since(st.LastSentAt)
		if elapsed < delay {
			return Decision{
				Action: "skip",
				Reason: fmt.Sprintf("backoff: next in %s (count=%d)", (delay - elapsed).Round(time.Second), st.SendCount),
			}
		}
	}

	source := "paused"
	var exceededLag []model.LagInfo
	if isRunning(job.State) && lag.Threshold > 0 {
		source = "lag"
		exceededLag = checkLag(job, lag.Threshold)
		if len(exceededLag) == 0 {
			return Decision{Action: "skip", Reason: "lag below threshold"}
		}
	}

	var errorDetail string
	if job.ErrorLogURLs != "" {
		errorDetail = e.fetchAndDedup(ctx, job.ErrorLogURLs)
		errorDetail = truncate(errorDetail, 300)
	}
	if source == "lag" {
		lagSummary := formatLagSummary(exceededLag)
		if errorDetail != "" {
			errorDetail = lagSummary + "\n" + errorDetail
		} else {
			errorDetail = lagSummary
		}
	}

	if isNew {
		// 新建的 status 补充 source
		e.store.Update(key, func(s *model.AlertStatus) {
			s.Source = source
		})
	}

	event := model.AlertEvent{
		JobID:          job.ID,
		JobName:        job.Name,
		Database:       dbName,
		State:          job.State,
		PauseTime:      job.PauseTime,
		Reason:         job.ReasonOfStateChanged,
		ErrorDetail:    errorDetail,
		Timestamp:      now,
		Duration:       now.Sub(st.FirstAlertAt),
		TotalSendCount: st.SendCount + 1,
	}

	return Decision{
		Event:  event,
		Action: "send",
		Key:    key,
	}
}

// UpdateAfterSend 告警发送成功后更新 status。
func (e *Evaluator) UpdateAfterSend(key string) {
	e.store.Update(key, func(st *model.AlertStatus) {
		st.LastSentAt = time.Now()
		st.SendCount++
	})
}

// isPaused 判断 job 状态是否为暂停。
func isPaused(state string) bool {
	s := strings.ToUpper(strings.TrimSpace(state))
	return s == "PAUSED" || s == "PAUSE"
}

// isRunning 判断 job 状态是否为运行中。
func isRunning(state string) bool {
	return strings.ToUpper(strings.TrimSpace(state)) == "RUNNING"
}

// checkLag 检查是否有分区的 lag 超过阈值。
func checkLag(job model.RoutineLoadJob, threshold int64) []model.LagInfo {
	parsed := model.ParseLag(job.Lag)
	if len(parsed) == 0 {
		return nil
	}

	var exceeded []model.LagInfo
	for pid, lag := range parsed {
		if lag > threshold {
			exceeded = append(exceeded, model.LagInfo{PartitionID: pid, LagCount: lag})
		}
	}

	sort.Slice(exceeded, func(i, j int) bool {
		return exceeded[i].PartitionID < exceeded[j].PartitionID
	})
	return exceeded
}

// computeDelay 计算指数退避延迟。
// delay(n) = min(base * factor^(n-1), max)
// sendCount=0 时无延迟（首次告警立即发送）。
// sendCount=1 时延迟 = base（第一次发送后的等待时间）。
// sendCount=2 时延迟 = base * factor，以此类推。
func computeDelay(sendCount int, base time.Duration, factor float64, max time.Duration) time.Duration {
	if sendCount <= 0 {
		return 0
	}
	d := base
	for i := 1; i < sendCount; i++ {
		d = time.Duration(float64(d) * factor)
		if d > max {
			return max
		}
	}
	return d
}

// fetchAndDedup 请求错误 URL 并去重。
func (e *Evaluator) fetchAndDedup(ctx context.Context, rawURLs string) string {
	urls := splitURLs(rawURLs)
	if len(urls) == 0 {
		return ""
	}

	seen := make(map[string]bool)
	var parts []string
	failed := 0

	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		body, err := e.fetchURL(ctx, u)
		if err != nil {
			e.logger.Warn("fetch error url failed",
				zap.String("url", u),
				zap.Error(err),
			)
			failed++
			continue
		}
		normalized := strings.TrimSpace(body)
		if normalized == "" {
			continue
		}
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		parts = append(parts, normalized)
	}

	if len(parts) == 0 && failed > 0 {
		return "(failed to fetch error details)"
	}
	return strings.Join(parts, "\n---\n")
}

func (e *Evaluator) fetchURL(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	limited := io.LimitReader(resp.Body, 64*1024)
	body, err := io.ReadAll(limited)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// splitURLs 按逗号、分号、换行分割 URL。
func splitURLs(raw string) []string {
	r := strings.NewReplacer(",", "\n", ";", "\n")
	lines := strings.Split(r.Replace(raw), "\n")
	var out []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// truncate 截断字符串到指定 rune 长度。
func truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "...(truncated)"
}

// formatLagSummary 格式化 lag 信息为可读字符串。
func formatLagSummary(lags []model.LagInfo) string {
	parts := make([]string, len(lags))
	for i, l := range lags {
		parts[i] = fmt.Sprintf("partition %s=%d", l.PartitionID, l.LagCount)
	}
	return strings.Join(parts, ", ")
}
