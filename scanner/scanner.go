// Package scanner queries Doris for routine load job status.
package scanner

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jimmy-boss/alert_routine_load/model"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Option is a functional option for Scanner.
type Option func(*Scanner)

// WithLogger injects a logger implementation.
func WithLogger(logger glog.HLoggerBase) Option {
	return func(s *Scanner) {
		s.logger = logger
	}
}

// Scanner queries Doris routine load jobs.
type Scanner struct {
	db     *gorm.DB
	logger glog.HLoggerBase
}

// New creates a Scanner with an existing *gorm.DB connection.
// Third-party callers can pass their own *gorm.DB directly,
// no need to configure doris connection in YAML.
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

// ShowDatabases executes SHOW DATABASES and returns the list of database names.
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

// JobRef is a reference to a routine load job (database + name).
type JobRef struct {
	DBName  string
	JobName string
}

// QueryJobList discovers routine load jobs via information_schema.
// If databases is non-empty, only those databases are queried.
// If jobFilter is non-empty, only those job names are included.
// Returns a list of JobRef (database + job name pairs).
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

	// Build job name filter set per database.
	var refs []JobRef
	for rows.Next() {
		var dbName, jobName string
		if err := rows.Scan(&dbName, &jobName); err != nil {
			s.logger.Warn("scan job list row failed", zap.Error(err))
			continue
		}
		// Apply job name filter if configured.
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

// QueryJobDetail retrieves full details for a single routine load job.
// Uses SHOW ROUTINE LOAD FOR `db`.`job` — no USE database needed.
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
	job.Name = jobName // ensure name is set from the query parameter
	return &job, nil
}

// QueryAllDatabases queries routine load for every database in the config.
// Returns a map of database → jobs.
// Uses information_schema to discover jobs, then SHOW ROUTINE LOAD FOR for details.
// No USE database statement — safe for concurrent use.
func (s *Scanner) QueryAllDatabases(ctx context.Context, databases []string, jobFilter map[string][]string) (map[string][]model.RoutineLoadJob, error) {
	// Step 1: Discover jobs via information_schema.
	refs, err := s.QueryJobList(ctx, databases, jobFilter)
	if err != nil {
		return nil, fmt.Errorf("query job list: %w", err)
	}

	// Step 2: Get details for each job.
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

// parseJob maps a column→value map to a RoutineLoadJob struct.
// Column names from SHOW ROUTINE LOAD vary across Doris versions;
// we match by common names.
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
