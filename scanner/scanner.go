// Package scanner queries Doris for routine load job status.
package scanner

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jimmy-boss/alert_routine_load/model"
	"gorm.io/gorm"
)

// Scanner queries Doris SHOW ROUTINE LOAD for configured databases.
type Scanner struct {
	db     *gorm.DB
	logger *slog.Logger
}

// New creates a Scanner with an existing *gorm.DB connection.
// Third-party callers can pass their own *gorm.DB directly,
// no need to configure doris connection in YAML.
func New(db *gorm.DB, logger *slog.Logger) *Scanner {
	return &Scanner{db: db, logger: logger}
}

// QueryJobs retrieves routine load jobs for the given database.
// If jobNames is non-empty, only those jobs are checked.
func (s *Scanner) QueryJobs(ctx context.Context, database string, jobNames []string) ([]model.RoutineLoadJob, error) {
	gormDB := s.db.WithContext(ctx)
	gormDB.Exec(fmt.Sprintf("USE `%s`", database))
	rows, err := gormDB.Raw("SHOW ROUTINE LOAD").Rows()
	if err != nil {
		return nil, fmt.Errorf("query routine load [%s]: %w", database, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	nameSet := make(map[string]bool, len(jobNames))
	for _, n := range jobNames {
		nameSet[n] = true
	}

	var jobs []model.RoutineLoadJob
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]sql.NullString, len(cols))
		for i := range vals {
			vals[i] = &ptrs[i]
		}
		if err := rows.Scan(vals...); err != nil {
			s.logger.Warn("scan row failed", "err", err)
			continue
		}

		colMap := make(map[string]string, len(cols))
		for i, c := range cols {
			if ptrs[i].Valid {
				colMap[c] = ptrs[i].String
			}
		}

		job := parseJob(colMap)

		// Filter by job name if configured.
		if len(nameSet) > 0 && !nameSet[job.Name] {
			continue
		}

		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// QueryAllDatabases queries routine load for every database in the config.
// Returns a map of database → jobs.
func (s *Scanner) QueryAllDatabases(ctx context.Context, databases []string, jobFilter map[string][]string) (map[string][]model.RoutineLoadJob, error) {
	result := make(map[string][]model.RoutineLoadJob, len(databases))
	for _, db := range databases {
		jobs, err := s.QueryJobs(ctx, db, jobFilter[db])
		if err != nil {
			s.logger.Error("query failed", "database", db, "err", err)
			continue
		}
		result[db] = jobs
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
