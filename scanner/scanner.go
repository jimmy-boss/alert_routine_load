// Package scanner 查询 Doris routine load 任务状态。
package scanner

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Option 是 Scanner 的函数选项。
type Option func(*Scanner)

// WithLogger 注入 logger 实现。
func WithLogger(logger glog.HLoggerBase) Option {
	return func(s *Scanner) {
		s.logger = logger
	}
}

// Scanner 查询 Doris routine load 任务。
type Scanner struct {
	db     *gorm.DB
	logger glog.HLoggerBase
}

// New 创建 Scanner，接收已有 *gorm.DB 连接。
// 第三方调用方可直接传入 *gorm.DB，无需在 YAML 中配置 doris 连接。
func New(db *gorm.DB, opts ...Option) *Scanner {
	s := &Scanner{db: db}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = glog.GlobalLoggers["default"]
	}
	return s
}

// ShowDatabases 执行 SHOW DATABASES 并返回数据库名列表。
func (s *Scanner) ShowDatabases(ctx context.Context) ([]string, error) {
	rows, err := s.db.WithContext(ctx).Raw("SHOW DATABASES").Rows()
	if err != nil {
		return nil, fmt.Errorf("show databases: %w", err)
	}
	defer rows.Close()

	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			s.logger.Warn("scan database name failed", zap.Error(err))
			continue
		}
		dbs = append(dbs, name)
	}
	return dbs, rows.Err()
}

// JobRef 是 routine load 任务的引用（数据库名 + 任务名）。
type JobRef struct {
	DBName  string
	JobName string
}

// QueryJobList 通过 information_schema 发现 routine load 任务。
// 如果 databases 非空，仅查询指定数据库。
// 如果 jobFilter 非空，仅包含指定任务名。
// 返回 JobRef 列表（数据库名 + 任务名）。
func (s *Scanner) QueryJobList(ctx context.Context, databases []string, jobFilter map[string][]string) ([]JobRef, error) {
	query := "SELECT DB_NAME, JOB_NAME FROM information_schema.routine_load_jobs"
	var args []interface{}

	if len(databases) > 0 {
		placeholders := make([]string, len(databases))
		for i, db := range databases {
			placeholders[i] = "?"
			args = append(args, db)
		}
		query += " WHERE DB_NAME IN (" + strings.Join(placeholders, ",") + ")"
	}

	rows, err := s.db.WithContext(ctx).Raw(query, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("query job list: %w", err)
	}
	defer rows.Close()

	var refs []JobRef
	for rows.Next() {
		var dbName, jobName string
		if err := rows.Scan(&dbName, &jobName); err != nil {
			s.logger.Warn("scan job list row failed", zap.Error(err))
			continue
		}
		// 应用任务名过滤。
		if names, ok := jobFilter[dbName]; ok && len(names) > 0 {
			nameSet := make(map[string]bool, len(names))
			for _, n := range names {
				nameSet[n] = true
			}
			if !nameSet[jobName] {
				continue
			}
		}
		refs = append(refs, JobRef{DBName: dbName, JobName: jobName})
	}
	return refs, rows.Err()
}

// QueryJobDetail 获取单个 routine load 任务的完整详情。
// 使用 SHOW ROUTINE LOAD FOR `db`.`job`，无需 USE database。
func (s *Scanner) QueryJobDetail(ctx context.Context, dbName, jobName string) (*model.RoutineLoadJob, error) {
	query := fmt.Sprintf("SHOW ROUTINE LOAD FOR `%s`.`%s`", dbName, jobName)
	rows, err := s.db.WithContext(ctx).Raw(query).Rows()
	if err != nil {
		return nil, fmt.Errorf("show routine load for %s.%s: %w", dbName, jobName, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	if !rows.Next() {
		return nil, fmt.Errorf("no routine load job found: %s.%s", dbName, jobName)
	}

	vals := make([]interface{}, len(cols))
	ptrs := make([]sql.NullString, len(cols))
	for i := range vals {
		vals[i] = &ptrs[i]
	}
	if err := rows.Scan(vals...); err != nil {
		return nil, fmt.Errorf("scan %s.%s: %w", dbName, jobName, err)
	}

	colMap := make(map[string]string, len(cols))
	for i, c := range cols {
		if ptrs[i].Valid {
			colMap[c] = ptrs[i].String
		}
	}

	job := parseJob(colMap)
	job.Name = jobName // 确保从查询参数设置 name
	return &job, nil
}

// QueryAllDatabases 查询配置中每个数据库的 routine load 任务。
// 返回 数据库→任务列表 的映射。
// 使用 information_schema 发现任务，再用 SHOW ROUTINE LOAD FOR 获取详情。
// 无 USE database 语句，安全支持并发使用。
func (s *Scanner) QueryAllDatabases(ctx context.Context, databases []string, jobFilter map[string][]string) (map[string][]model.RoutineLoadJob, error) {
	// 第一步：通过 information_schema 发现任务。
	refs, err := s.QueryJobList(ctx, databases, jobFilter)
	if err != nil {
		return nil, fmt.Errorf("query job list: %w", err)
	}

	// 第二步：获取每个任务的详情。
	result := make(map[string][]model.RoutineLoadJob)
	for _, ref := range refs {
		job, err := s.QueryJobDetail(ctx, ref.DBName, ref.JobName)
		if err != nil {
			s.logger.Error("query job detail failed",
				zap.String("database", ref.DBName),
				zap.String("job", ref.JobName),
				zap.Error(err),
			)
			continue
		}
		result[ref.DBName] = append(result[ref.DBName], *job)
	}
	return result, nil
}

// parseJob 将列名→值映射转换为 RoutineLoadJob 结构体。
// SHOW ROUTINE LOAD 返回的列名因 Doris 版本而异，使用通用名称匹配。
func parseJob(m map[string]string) model.RoutineLoadJob {
	j := model.RoutineLoadJob{
		Name:                 pick(m, "Name", "name"),
		State:                pick(m, "State", "state"),
		CreateTime:           pick(m, "CreateTime", "create_time"),
		PauseTime:            pick(m, "PauseTime", "pause_time"),
		EndTime:              pick(m, "EndTime", "end_time"),
		DataSourceType:       pick(m, "DataSourceType", "data_source_type"),
		JobProperties:        pick(m, "JobProperties", "job_properties"),
		DataSourceProperties: pick(m, "DataSourceProperties", "data_source_properties"),
		CustomProperties:     pick(m, "CustomProperties", "custom_properties"),
		Statistic:            pick(m, "Statistic", "statistic"),
		Progress:             pick(m, "Progress", "progress"),
		Lag:                  pick(m, "Lag", "lag"),
		ReasonOfStateChanged: pick(m, "ReasonOfStateChanged", "reason_of_state_changed", "Message"),
		ErrorLogURLs:         pick(m, "ErrorLogUrls", "error_log_urls", "ErrorLogURLs"),
	}
	if idStr := pick(m, "Id", "id", "JobId", "job_id"); idStr != "" {
		j.ID, _ = strconv.ParseInt(idStr, 10, 64)
	}
	if numStr := pick(m, "CurrentTaskNum", "current_task_num"); numStr != "" {
		j.CurrentTaskNum, _ = strconv.Atoi(numStr)
	}
	return j
}

func pick(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
