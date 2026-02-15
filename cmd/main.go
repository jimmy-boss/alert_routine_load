// @Author: Jimmy
// @DateTime: 2026/02/15

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
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

var (
	configPath string
	interval   time.Duration
	dataDir    string
)

func init() {
	flag.StringVar(&configPath, "c", "conf/config.yaml", "配置文件路径")
	flag.DurationVar(&interval, "interval", 60*time.Second, "扫描间隔")
	flag.StringVar(&dataDir, "data", "./data", "数据持久化目录")
}

func main() {
	flag.Parse()

	// 初始化 logger
	glog.InitLogger("default", glog.LoggerConfig{
		Level:      "info",
		OutputPath: []string{"stdout"},
		Encoder:    "console",
	})
	log := glog.GlobalLoggers["default"]

	// 加载配置
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal("加载配置失败", zap.Error(err))
	}

	// 连接 Doris
	db, err := connectDoris(cfg, log)
	if err != nil {
		log.Fatal("连接 Doris 失败", zap.Error(err))
	}

	// 初始化 store
	statusStore, err := store.NewStatusStore(dataDir, store.WithStatusLogger(log))
	if err != nil {
		log.Fatal("初始化 StatusStore 失败", zap.Error(err))
	}

	archiveStore, err := store.NewArchiveStore(dataDir,
		store.WithArchiveLogger(log),
		store.WithRetentionDays(cfg.Alert.History.RetentionDays),
	)
	if err != nil {
		log.Fatal("初始化 ArchiveStore 失败", zap.Error(err))
	}

	// 初始化组件
	sc := scanner.New(db, scanner.WithLogger(log))
	eval := evaluator.New(cfg, statusStore, evaluator.WithLogger(log))
	nt := createNotifier(cfg, log)

	// 优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("收到关闭信号，准备退出", zap.String("signal", sig.String()))
		cancel()
	}()

	// 构建数据库列表
	databases, err := buildDatabaseList(ctx, cfg, sc)
	if err != nil {
		log.Fatal("构建数据库列表失败", zap.Error(err))
	}

	log.Info("启动 Doris Routine Load 告警系统",
		zap.Int("databases", len(databases)),
		zap.Duration("interval", interval),
		zap.String("channel", cfg.Notify.Channel),
	)

	// 主循环
	run(ctx, cfg, sc, eval, nt, statusStore, archiveStore, databases, log)

	// 持久化
	if err := statusStore.Save(); err != nil {
		log.Error("保存 StatusStore 失败", zap.Error(err))
	}
	if err := archiveStore.Save(); err != nil {
		log.Error("保存 ArchiveStore 失败", zap.Error(err))
	}

	log.Info("告警系统已退出")
}

// connectDoris 创建 Doris GORM 连接。
func connectDoris(cfg *config.Config, log glog.HLogger) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Doris.User, cfg.Doris.Password, cfg.Doris.Host, cfg.Doris.Port)

	gormLog := glog.NewGormLogger(log, &gormlogger.Config{
		SlowThreshold: 500 * time.Millisecond,
		LogLevel:      gormlogger.Warn,
	})

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLog,
	})
	if err != nil {
		return nil, fmt.Errorf("打开数据库连接失败: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取底层连接失败: %w", err)
	}
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	return db, nil
}

// createNotifier 根据配置创建通知器。
func createNotifier(cfg *config.Config, log glog.HLoggerBase) notifier.Notifier {
	switch cfg.Notify.Channel {
	case "feishu":
		return notifier.NewFeishu(&cfg.Notify.Feishu, notifier.WithFeishuLogger(log))
	default:
		log.Error("不支持的通知渠道", zap.String("channel", cfg.Notify.Channel))
		os.Exit(1)
		return nil
	}
}

// buildDatabaseList 获取并过滤数据库列表。
func buildDatabaseList(ctx context.Context, cfg *config.Config, sc *scanner.Scanner) ([]string, error) {
	databases, err := sc.ShowDatabases(ctx)
	if err != nil {
		return nil, err
	}
	return cfg.FilterDatabases(databases), nil
}

// run 主循环：扫描 → 评估 → 通知 → 归档 → 持久化。
func run(
	ctx context.Context,
	cfg *config.Config,
	sc *scanner.Scanner,
	eval *evaluator.Evaluator,
	nt notifier.Notifier,
	statusStore *store.StatusStore,
	archiveStore *store.ArchiveStore,
	databases []string,
	log glog.HLoggerBase,
) {
	jobFilter := buildJobFilter(cfg)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 首次立即执行
	cycle(ctx, cfg, sc, eval, nt, statusStore, archiveStore, databases, jobFilter, log)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cycle(ctx, cfg, sc, eval, nt, statusStore, archiveStore, databases, jobFilter, log)
		}
	}
}

// cycle 执行一次完整的告警周期。
func cycle(
	ctx context.Context,
	cfg *config.Config,
	sc *scanner.Scanner,
	eval *evaluator.Evaluator,
	nt notifier.Notifier,
	statusStore *store.StatusStore,
	archiveStore *store.ArchiveStore,
	databases []string,
	jobFilter map[string][]string,
	log glog.HLoggerBase,
) {
	// 1. 扫描所有数据库的 routine load 任务
	dbJobs, err := sc.QueryAllDatabases(ctx, databases, jobFilter)
	if err != nil {
		log.Error("扫描任务失败", zap.Error(err))
		return
	}

	totalJobs := 0
	for _, jobs := range dbJobs {
		totalJobs += len(jobs)
	}
	log.Info("扫描完成", zap.Int("databases", len(dbJobs)), zap.Int("jobs", totalJobs))

	// 2. Reconcile：状态流转
	eval.Reconcile(dbJobs)

	// 3. 收集恢复中的条目并发送恢复通知
	recovered := statusStore.CollectRecovering()
	for _, st := range recovered {
		info := model.RecoveryInfo{
			JobKey:      st.JobKey,
			JobName:     st.JobName,
			Database:    st.Database,
			Source:      st.Source,
			RecoveredAt: st.RecoveredAt,
			Duration:    st.RecoveredAt.Sub(st.FirstAlertAt),
			SendCount:   st.SendCount,
		}
		if err := nt.SendRecovery(info); err != nil {
			log.Error("发送恢复通知失败",
				zap.String("job_key", st.JobKey),
				zap.Error(err),
			)
		} else {
			log.Info("恢复通知已发送",
				zap.String("job_key", st.JobKey),
				zap.Duration("duration", info.Duration),
			)
		}
	}

	// 4. 移除已恢复条目（必须在 Evaluate 之前，避免 Evaluate 拿到旧 status 重复发送）
	statusStore.RemoveRecovered()

	// 5. Evaluate：评估告警决策
	decisions := eval.Evaluate(ctx, dbJobs)

	// 6. 发送告警
	for _, d := range decisions {
		if d.Action != "send" {
			continue
		}
		if err := nt.Send(d.Event); err != nil {
			log.Error("发送告警失败",
				zap.Int64("job_id", d.Event.JobID),
				zap.String("job_name", d.Event.JobName),
				zap.Error(err),
			)
			continue
		}
		eval.UpdateAfterSend(d.Key)
		log.Info("告警已发送",
			zap.Int64("job_id", d.Event.JobID),
			zap.String("job_name", d.Event.JobName),
			zap.String("database", d.Event.Database),
		)
	}

	// 7. 归档
	for _, st := range recovered {
		archiveStore.Archive(st)
	}

	// 8. 持久化
	if err := statusStore.Save(); err != nil {
		log.Error("保存 StatusStore 失败", zap.Error(err))
	}
	if err := archiveStore.Save(); err != nil {
		log.Error("保存 ArchiveStore 失败", zap.Error(err))
	}
}

// buildJobFilter 从配置中构建数据库→任务名过滤器。
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
