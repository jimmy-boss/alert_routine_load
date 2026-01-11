// Command doris-alert is a Routine Load alert daemon for Apache Doris.
//
// Usage:
//
//	doris-alert -c config.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jimmy-boss/alert_routine_load/alerter"
	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/notifier"
	"github.com/jimmy-boss/alert_routine_load/scanner"
	glog "github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	cfgPath := flag.String("c", "conf/alert.yaml", "path to config file")
	flag.Parse()

	// Logger.
	log, err := glog.NewZapLogger(glog.LoggerConfig{
		Level:      "info",
		OutputPath: []string{"stdout"},
		Encoder:    "console",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()

	// Load config.
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("load config failed", zap.Error(err))
		os.Exit(1)
	}
	log.Info("config loaded",
		zap.Int("databases", len(cfg.Database)),
		zap.Duration("scan_interval", cfg.Alert.ScanInterval.Duration),
		zap.Bool("history_enabled", cfg.Alert.History.Enabled),
	)

	// Standalone mode: doris connection info is required.
	if cfg.Doris.Host == "" {
		log.Error("doris.host is required in standalone mode")
		os.Exit(1)
	}

	// Connect to Doris via GORM.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Doris.User, cfg.Doris.Password, cfg.Doris.Host, cfg.Doris.Port)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Error("open db failed", zap.Error(err))
		os.Exit(1)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Error("get underlying sql.DB failed", zap.Error(err))
		os.Exit(1)
	}
	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	defer sqlDB.Close()

	log.Info("connected to doris",
		zap.String("host", cfg.Doris.Host),
		zap.Int("port", cfg.Doris.Port),
	)

	// Build alert history (optional).
	var history *alerter.AlertHistory
	if cfg.Alert.History.Enabled {
		history, err = alerter.NewHistory(&cfg.Alert.History,
			alerter.WithHistoryLogger(log),
		)
		if err != nil {
			log.Error("init alert history failed", zap.Error(err))
			os.Exit(1)
		}
	}

	// Build components.
	scan := scanner.New(db, scanner.WithLogger(log))
	alert := alerter.New(cfg, alerter.WithLogger(log), alerter.WithHistory(history))
	notify := notifier.New(&cfg.Feishu, notifier.WithLogger(log))

	// Graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down...")
		if history != nil {
			history.Save()
		}
		cancel()
	}()

	// Periodic history flush (every 5 minutes).
	if history != nil {
		go func() {
			ticker := time.NewTicker(5 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					history.Save()
				}
			}
		}()
	}

	// Build database list and job filter.
	databases, jobFilter, err := buildDatabaseList(ctx, cfg, scan, log)
	if err != nil {
		log.Error("build database list failed", zap.Error(err))
		os.Exit(1)
	}
	log.Info("monitoring databases", zap.Int("count", len(databases)))

	// Main loop.
	ticker := time.NewTicker(cfg.Alert.ScanInterval.Duration)
	defer ticker.Stop()

	// Run once immediately.
	runWithTimeout(ctx, cfg.Alert.ScanInterval.Duration, scan, alert, notify, history, databases, jobFilter, log)

	for {
		select {
		case <-ctx.Done():
			log.Info("stopped")
			return
		case <-ticker.C:
			runWithTimeout(ctx, cfg.Alert.ScanInterval.Duration, scan, alert, notify, history, databases, jobFilter, log)
		}
	}
}

// runWithTimeout wraps run() with a timeout context to prevent indefinite blocking.
func runWithTimeout(ctx context.Context, timeout time.Duration, scan *scanner.Scanner, alert *alerter.Alerter, notify *notifier.Notifier, history *alerter.AlertHistory, databases []string, jobFilter map[string][]string, log glog.HLogger) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	run(runCtx, scan, alert, notify, history, databases, jobFilter, log)
}

func run(ctx context.Context, scan *scanner.Scanner, alert *alerter.Alerter, notify *notifier.Notifier, history *alerter.AlertHistory, databases []string, jobFilter map[string][]string, log glog.HLogger) {
	log.Info("scanning routine load jobs...")

	dbJobs, err := scan.QueryAllDatabases(ctx, databases, jobFilter)
	if err != nil {
		log.Error("scan failed", zap.Error(err))
		return
	}

	totalJobs := 0
	pausedJobs := 0
	for _, jobs := range dbJobs {
		totalJobs += len(jobs)
		for _, j := range jobs {
			if strings.ToUpper(j.State) == "PAUSED" || strings.ToUpper(j.State) == "PAUSE" {
				pausedJobs++
			}
		}
	}
	log.Info("scan complete",
		zap.Int("total_jobs", totalJobs),
		zap.Int("paused_jobs", pausedJobs),
	)

	decisions := alert.Evaluate(ctx, dbJobs)

	sent := 0
	skipped := 0
	for _, d := range decisions {
		if d.Action == "skip" {
			skipped++
			log.Debug("alert skipped",
				zap.Int64("job_id", d.Event.JobID),
				zap.String("reason", d.Reason),
			)
			continue
		}
		if err := notify.Send(d); err != nil {
			log.Error("send alert failed",
				zap.Int64("job_id", d.Event.JobID),
				zap.Error(err),
			)
			continue
		}
		// Update status only after successful send.
		alert.UpdateStatus(d.StatusKey, d.Event.Database, d.Event.JobName, d.Event.Reason)
		sent++
	}

	// Clean up status for recovered jobs and send recovery notifications.
	recoveredKeys := alert.RemoveStale(dbJobs)
	recovered := 0
	notified := 0
	for _, key := range recoveredKeys {
		// Extract database name from key (format: "db:jobId").
		dbName := ""
		if idx := strings.Index(key, ":"); idx > 0 {
			dbName = key[:idx]
		}
		var duration time.Duration
		var sendCount int
		var jobName string
		if history != nil {
			if record := history.FindArchivedRecord(key); record != nil {
				duration = record.Duration()
				sendCount = record.SendCount
				jobName = record.JobName
			}
		}
		// Only send recovery notification if alerts were actually sent.
		if sendCount > 0 {
			if err := notify.SendRecovery(key, dbName, jobName, duration, sendCount); err != nil {
				log.Error("send recovery notification failed",
					zap.String("job_key", key),
					zap.Error(err),
				)
			}
			notified++
		}
		recovered++
	}

	// Flush history once at end of cycle (not per-alert).
	if history != nil {
		history.Save()
	}

	log.Info("alert cycle done",
		zap.Int("sent", sent),
		zap.Int("skipped", skipped),
		zap.Int("recovered", recovered),
		zap.Int("recovery_notified", notified),
	)
}

// buildDatabaseList builds the list of databases to monitor and the job filter
// based on the scan_databases mode.
func buildDatabaseList(ctx context.Context, cfg *config.Config, scan *scanner.Scanner, log glog.HLogger) ([]string, map[string][]string, error) {
	var databases []string

	switch cfg.ScanDatabases.Mode {
	case "all":
		allDBs, err := scan.ShowDatabases(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("show databases: %w", err)
		}
		databases = cfg.FilterDatabases(allDBs)
		log.Info("auto-discovered databases",
			zap.Int("total", len(allDBs)),
			zap.Int("after_filter", len(databases)),
		)
	default: // "configured"
		for _, dbRule := range cfg.Database {
			databases = append(databases, dbRule.Database)
		}
	}

	// Build job filter from database section (works for both modes).
	jobFilter := make(map[string][]string)
	for _, dbRule := range cfg.Database {
		for _, j := range dbRule.Jobs {
			if j.Name != "" {
				jobFilter[dbRule.Database] = append(jobFilter[dbRule.Database], j.Name)
			}
		}
	}

	return databases, jobFilter, nil
}
