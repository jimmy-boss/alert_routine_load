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
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jimmy-boss/alert_routine_load/alerter"
	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/notifier"
	"github.com/jimmy-boss/alert_routine_load/scanner"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	cfgPath := flag.String("c", "conf/alert.yaml", "path to config file")
	flag.Parse()

	// Logger.
	slogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Load config.
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slogger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	slogger.Info("config loaded",
		"databases", len(cfg.Database),
		"scan_interval", cfg.Alert.ScanInterval,
		"history_enabled", cfg.Alert.History.Enabled,
	)

	// Standalone mode: doris connection info is required.
	if cfg.Doris.Host == "" {
		slogger.Error("doris.host is required in standalone mode")
		os.Exit(1)
	}

	// Connect to Doris via GORM.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Doris.User, cfg.Doris.Password, cfg.Doris.Host, cfg.Doris.Port)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		slogger.Error("open db failed", "err", err)
		os.Exit(1)
	}

	sqlDB, err := db.DB()
	if err != nil {
		slogger.Error("get underlying sql.DB failed", "err", err)
		os.Exit(1)
	}
	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	defer sqlDB.Close()

	slogger.Info("connected to doris", "host", cfg.Doris.Host, "port", cfg.Doris.Port)

	// Build alert history (optional).
	var history *alerter.AlertHistory
	if cfg.Alert.History.Enabled {
		history, err = alerter.NewHistory(&cfg.Alert.History, slogger)
		if err != nil {
			slogger.Error("init alert history failed", "err", err)
			os.Exit(1)
		}
	}

	// Build components.
	scan := scanner.New(db, slogger)
	alert := alerter.New(cfg, slogger, alerter.WithHistory(history))
	notify := notifier.New(&cfg.Feishu, slogger)

	// Build database and job filter from config.
	var databases []string
	jobFilter := make(map[string][]string) // db → []jobName
	for _, dbRule := range cfg.Database {
		databases = append(databases, dbRule.Database)
		for _, j := range dbRule.Jobs {
			if j.Name != "" {
				jobFilter[dbRule.Database] = append(jobFilter[dbRule.Database], j.Name)
			}
		}
	}

	// Graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slogger.Info("shutting down...")
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

	// Main loop.
	ticker := time.NewTicker(cfg.Alert.ScanInterval.Duration)
	defer ticker.Stop()

	// Run once immediately.
	run(ctx, scan, alert, notify, history, databases, jobFilter, slogger)

	for {
		select {
		case <-ctx.Done():
			slogger.Info("stopped")
			return
		case <-ticker.C:
			run(ctx, scan, alert, notify, history, databases, jobFilter, slogger)
		}
	}
}

func run(ctx context.Context, scan *scanner.Scanner, alert *alerter.Alerter, notify *notifier.Notifier, history *alerter.AlertHistory, databases []string, jobFilter map[string][]string, logger *slog.Logger) {
	logger.Info("scanning routine load jobs...")

	dbJobs, err := scan.QueryAllDatabases(ctx, databases, jobFilter)
	if err != nil {
		logger.Error("scan failed", "err", err)
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
	logger.Info("scan complete", "total_jobs", totalJobs, "paused_jobs", pausedJobs)

	decisions := alert.Evaluate(ctx, dbJobs)

	sent := 0
	skipped := 0
	for _, d := range decisions {
		if d.Action == "skip" {
			skipped++
			logger.Debug("alert skipped", "job_id", d.Event.JobID, "reason", d.Reason)
			continue
		}
		// Enrich event with history data for the notification.
		if history != nil {
			if record := history.FindRecord(d.StatusKey); record != nil {
				d.Event.Duration = record.Duration()
				d.Event.TotalSendCount = record.SendCount + 1 // +1 for current send
			}
		}
		if err := notify.Send(d); err != nil {
			logger.Error("send alert failed", "job_id", d.Event.JobID, "err", err)
			continue
		}
		// Update status only after successful send.
		alert.UpdateStatus(d.StatusKey)
		sent++
	}

	// Clean up status for recovered jobs and send recovery notifications.
	recoveredKeys := alert.RemoveStale(dbJobs)
	recovered := 0
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
		if err := notify.SendRecovery(key, dbName, jobName, duration, sendCount); err != nil {
			logger.Error("send recovery notification failed", "job_key", key, "err", err)
		}
		recovered++
	}

	logger.Info("alert cycle done", "sent", sent, "skipped", skipped, "recovered", recovered)
}
