// @Author: Jimmy
// @DateTime: 2026/02/15

package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/config"
	"github.com/jimmy-boss/alert_routine_load/v2/evaluator"
	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/alert_routine_load/v2/notifier"
	"github.com/jimmy-boss/alert_routine_load/v2/scanner"
	"github.com/jimmy-boss/alert_routine_load/v2/store"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

const (
	dbRefreshInterval = 5 * time.Minute
	jobScanInterval   = 30 * time.Second
	jobChanBuffer     = 256
)

// JobTask 投递给 worker 的任务单元。
type JobTask struct {
	Database string
	Job      model.RoutineLoadJob
}

// Handler 承载所有依赖的主处理器。
type Handler struct {
	cfg          *config.Config
	sc           *scanner.Scanner
	eval         *evaluator.Evaluator
	nt           notifier.Notifier
	statusStore  *store.StatusStore
	archiveStore *store.ArchiveStore
	log          glog.HLoggerBase

	workerCount int
	workerChs   []chan JobTask // 每个 worker 独立 channel，按 database hash 投递
	roundWg     sync.WaitGroup // 跟踪每轮 dispatch 的 worker 处理进度，确保 Reconcile 前 worker 已完成
	done        chan struct{}

	dbMu      sync.RWMutex
	databases []string
	jobFilter map[string][]string
}

// NewHandler 创建 Handler 并初始化所有组件。
func NewHandler(cfgPath string, dataDir string, workerCount int, env string) (*Handler, error) {
	glog.InitLogger("default", glog.LoggerConfig{
		Level:      "info",
		OutputPath: []string{"stdout"},
		Encoder:    "console",
	})
	log := glog.GlobalLoggers["default"]

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("加载配置: %w", err)
	}

	db, err := connectDoris(cfg, log, env)
	if err != nil {
		return nil, fmt.Errorf("连接 Doris: %w", err)
	}

	statusStore, err := store.NewStatusStore(dataDir, store.WithStatusLogger(log))
	if err != nil {
		return nil, fmt.Errorf("初始化 StatusStore: %w", err)
	}

	archiveStore, err := store.NewArchiveStore(dataDir,
		store.WithArchiveLogger(log),
		store.WithRetentionDays(cfg.Alert.History.RetentionDays),
	)
	if err != nil {
		return nil, fmt.Errorf("初始化 ArchiveStore: %w", err)
	}

	sc := scanner.New(db, scanner.WithLogger(log))
	eval := evaluator.New(cfg, statusStore, evaluator.WithLogger(log))
	nt := createNotifier(cfg, log)

	if workerCount <= 0 {
		workerCount = 4
	}

	workerChs := make([]chan JobTask, workerCount)
	for i := 0; i < workerCount; i++ {
		workerChs[i] = make(chan JobTask, jobChanBuffer)
	}

	return &Handler{
		cfg:          cfg,
		sc:           sc,
		eval:         eval,
		nt:           nt,
		statusStore:  statusStore,
		archiveStore: archiveStore,
		log:          log,
		workerCount:  workerCount,
		workerChs:    workerChs,
		done:         make(chan struct{}),
		jobFilter:    buildJobFilter(cfg),
	}, nil
}

// Run 启动所有协程并阻塞直到收到关闭信号。
func (h *Handler) Run() {
	h.log.Info("启动 Doris Routine Load 告警系统",
		zap.Int("workers", h.workerCount),
		zap.String("channel", h.cfg.Notify.Channel),
		zap.Duration("db_refresh", dbRefreshInterval),
		zap.Duration("job_scan", jobScanInterval),
	)

	go h.watchSignal()
	go h.refreshDBLoop()

	// scanJobLoop 用 WaitGroup 跟踪，确保关闭 channel 前已退出
	var scanWg sync.WaitGroup
	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		h.scanJobLoop()
	}()

	var wg sync.WaitGroup
	for i := 0; i < h.workerCount; i++ {
		wg.Add(1)
		go h.worker(i, &wg)
	}

	<-h.done
	h.log.Info("收到关闭信号，等待扫描循环和 worker 退出...")

	// 等待 scanJobLoop 退出后再关闭 channel，避免 send on closed channel panic
	scanWg.Wait()

	for _, ch := range h.workerChs {
		close(ch)
	}
	wg.Wait()

	if err := h.statusStore.Save(); err != nil {
		h.log.Error("保存 StatusStore 失败", zap.Error(err))
	}
	if err := h.archiveStore.Save(); err != nil {
		h.log.Error("保存 ArchiveStore 失败", zap.Error(err))
	}

	h.log.Info("告警系统已退出")
}

// watchSignal 监听系统信号，关闭 done channel。
func (h *Handler) watchSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	if runtime.GOOS != "windows" {
		signal.Notify(sigCh, syscall.SIGTERM)
	}
	sig := <-sigCh
	h.log.Info("收到关闭信号", zap.String("signal", sig.String()))
	close(h.done)
}

// refreshDBLoop 定时刷新数据库列表。
func (h *Handler) refreshDBLoop() {
	h.refreshDB()

	ticker := time.NewTicker(dbRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			h.refreshDB()
		}
	}
}

// refreshDB 刷新数据库列表（加锁写入）。
func (h *Handler) refreshDB() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	databases, err := h.sc.ShowDatabases(ctx)
	if err != nil {
		h.log.Error("刷新数据库列表失败", zap.Error(err))
		return
	}
	filtered := h.cfg.FilterDatabases(databases)

	h.dbMu.Lock()
	h.databases = filtered
	h.dbMu.Unlock()

	h.log.Info("数据库列表已刷新", zap.Int("count", len(filtered)))
}

// scanJobLoop 定时扫描 job 并投递到 jobCh。
func (h *Handler) scanJobLoop() {
	time.Sleep(5 * time.Second)
	h.scanAndDispatch()

	ticker := time.NewTicker(jobScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			h.scanAndDispatch()
		}
	}
}

// scanAndDispatch 扫描所有 job，执行全局 reconcile + 恢复通知，再投递到 worker channel。
func (h *Handler) scanAndDispatch() {
	// 等待上一轮 worker 处理完成，避免 Reconcile 与 Evaluate 并发
	h.roundWg.Wait()

	h.dbMu.RLock()
	databases := h.databases
	h.dbMu.RUnlock()

	if len(databases) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbJobs, err := h.sc.QueryAllDatabases(ctx, databases, h.jobFilter)
	if err != nil {
		h.log.Error("扫描任务失败", zap.Error(err))
		return
	}

	// 全局 Reconcile：状态流转
	h.eval.Reconcile(dbJobs)

	// 全局收集恢复候选 + 发送恢复通知（发送成功后才标记 recovered）
	candidates := h.statusStore.CollectRecoveringCandidates()
	var recovered []model.AlertStatus
	for _, st := range candidates {
		info := model.RecoveryInfo{
			JobKey:      st.JobKey,
			JobName:     st.JobName,
			Database:    st.Database,
			Source:      st.Source,
			RecoveredAt: st.RecoveredAt,
			Duration:    st.RecoveredAt.Sub(st.FirstAlertAt),
			SendCount:   st.SendCount,
		}
		if err := h.nt.SendRecovery(info); err != nil {
			h.log.Error("发送恢复通知失败，下一轮重试", zap.String("job_key", st.JobKey), zap.Error(err))
			continue
		}
		// 发送成功，标记为 recovered
		h.statusStore.Update(st.JobKey, func(s *model.AlertStatus) {
			s.MarkRecovered()
		})
		recovered = append(recovered, st)
		h.log.Info("恢复通知已发送", zap.String("job_key", st.JobKey), zap.Duration("duration", info.Duration))
	}

	// 全局移除已恢复条目（必须在 Evaluate 之前）
	h.statusStore.RemoveRecovered()

	// 归档（仅成功发送恢复通知的条目）
	for _, st := range recovered {
		h.archiveStore.Archive(st)
	}

	// 持久化
	h.statusStore.Save()
	h.archiveStore.Save()

	// 按 database hash 投递 job 到固定 worker，保证同一 database 的 job 顺序处理
	totalJobs := 0
	for dbName, jobs := range dbJobs {
		totalJobs += len(jobs)
		idx := hashDatabase(dbName, h.workerCount)
		for _, job := range jobs {
			h.roundWg.Add(1)
			select {
			case h.workerChs[idx] <- JobTask{Database: dbName, Job: job}:
			case <-h.done:
				h.roundWg.Done() // 未投递成功，撤销 Add
				h.log.Info("收到关闭信号，停止投递任务",
					zap.Int("total_jobs", totalJobs),
				)
				return
			}
		}
	}
	h.log.Info("扫描完成，任务已投递",
		zap.Int("databases", len(dbJobs)),
		zap.Int("jobs", totalJobs),
	)
}

// worker 消费对应 channel，处理单个 job 的告警评估和通知。
func (h *Handler) worker(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	h.log.Info("worker 启动", zap.Int("worker_id", id))

	for task := range h.workerChs[id] {
		h.processJob(task)
	}

	h.log.Info("worker 退出", zap.Int("worker_id", id))
}

// processJob 处理单个 job 的告警评估和发送（不含 reconcile/恢复逻辑）。
func (h *Handler) processJob(task JobTask) {
	defer h.roundWg.Done()

	key := fmt.Sprintf("%s:%d", task.Database, task.Job.ID)
	ctx := context.Background()

	// 1. Evaluate 单条
	decisions := h.eval.Evaluate(ctx, map[string][]model.RoutineLoadJob{
		task.Database: {task.Job},
	})

	// 2. 发送告警
	for _, d := range decisions {
		if d.Action != "send" {
			continue
		}
		if err := h.nt.Send(d.Event); err != nil {
			h.log.Error("发送告警失败", zap.String("key", key), zap.Error(err))
			continue
		}
		h.eval.UpdateAfterSend(d.Key)
		h.log.Info("告警已发送",
			zap.Int64("job_id", d.Event.JobID),
			zap.String("job_name", d.Event.JobName),
			zap.String("database", d.Event.Database),
		)
	}
}

// hashDatabase 对 database 名做 hash，用于投递到固定 worker。
func hashDatabase(db string, bucketCount int) int {
	h := fnv.New32a()
	h.Write([]byte(db))
	return int(h.Sum32()) % bucketCount
}

// --- 入口 ---

func main() {
	configPath := flag.String("c", "conf/config.yaml", "配置文件路径")
	dataDir := flag.String("data", "./data", "数据持久化目录")
	workers := flag.Int("workers", 4, "worker 协程数")
	env := flag.String("env", "prod", "运行环境（dev/prod），dev 时输出所有 SQL")
	flag.Parse()

	h, err := NewHandler(*configPath, *dataDir, *workers, *env)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化失败: %v\n", err)
		os.Exit(1)
	}

	h.Run()
}

// --- 辅助函数 ---

func connectDoris(cfg *config.Config, log glog.HLogger, env string) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Doris.User, cfg.Doris.Password, cfg.Doris.Host, cfg.Doris.Port)

	logLevel := gormlogger.Warn
	if env == "dev" {
		logLevel = gormlogger.Info
	}

	gormLog := glog.NewGormLogger(log, &gormlogger.Config{
		SlowThreshold: 500 * time.Millisecond,
		LogLevel:      logLevel,
	})

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: gormLog})
	if err != nil {
		return nil, fmt.Errorf("打开数据库连接: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取底层连接: %w", err)
	}
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	return db, nil
}

func createNotifier(cfg *config.Config, log glog.HLoggerBase) notifier.Notifier {
	switch cfg.Notify.Channel {
	case "feishu":
		return notifier.NewFeishu(&cfg.Notify.Feishu, notifier.WithFeishuLogger(log))
	case "dingtalk":
		return notifier.NewDingtalk(&cfg.Notify.Dingtalk, notifier.WithDingtalkLogger(log))
	default:
		log.Error("不支持的通知渠道", zap.String("channel", cfg.Notify.Channel))
		os.Exit(1)
		return nil
	}
}

func buildJobFilter(cfg *config.Config) map[string][]string {
	filter := make(map[string][]string)
	for _, db := range cfg.Database {
		if len(db.Jobs) == 0 {
			continue
		}
		names := make([]string, 0, len(db.Jobs))
		for _, job := range db.Jobs {
			if job.Name != "" {
				names = append(names, job.Name)
			}
		}
		if len(names) > 0 {
			filter[db.Name] = names
		}
	}
	return filter
}
