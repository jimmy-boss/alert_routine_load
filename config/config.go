// Package config loads and validates alert.yaml.
package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that supports human-readable YAML values
// like "5m", "30s", "1h30m", as well as raw nanosecond integers for backward compatibility.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	// Try parsing as a human-readable string first (e.g. "5m", "30s").
	var s string
	if err := node.Decode(&s); err == nil {
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = parsed
		return nil
	}

	// Fallback: try parsing as a raw integer (nanoseconds).
	var raw int64
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("cannot parse duration: expected string like \"5m\" or integer nanoseconds")
	}
	d.Duration = time.Duration(raw)
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// DefaultSystemDBs is the built-in list of Doris system databases that are
// always excluded in blacklist (scan_databases.mode=all) mode.
var DefaultSystemDBs = []string{
	"information_schema",
	"__internal_schema",
	"mysql",
}

// ScanDatabasesConfig controls how databases are discovered for monitoring.
type ScanDatabasesConfig struct {
	Mode              string   `yaml:"mode"`                     // "all" = auto-discover | "configured" = only database section (default)
	Exclude           []string `yaml:"exclude"`                  // exact-match exclusions
	ExcludePatterns   []string `yaml:"exclude_patterns"`         // regex exclusions
	OverrideSystemDBs []string `yaml:"override_system_databases"` // additional system DBs to exclude
}

// Config is the top-level configuration.
type Config struct {
	Doris          DorisConfig          `yaml:"doris"`
	Feishu         FeishuConfig         `yaml:"feishu"`
	Alert          AlertConfig          `yaml:"alert"`
	ScanDatabases  ScanDatabasesConfig  `yaml:"scan_databases"`
	Database       []DatabaseRule       `yaml:"database"`
}

type DorisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type FeishuConfig struct {
	WebhookURL string `yaml:"webhook_url"`
	SignSecret string `yaml:"sign_secret"` // optional: for signed webhook
}

type AlertConfig struct {
	// How often the scanner loop runs.
	ScanInterval Duration `yaml:"scan_interval"`
	// Global defaults for alert sending.
	DefaultInitialInterval Duration  `yaml:"default_initial_interval"`
	DefaultMaxInterval     Duration  `yaml:"default_max_interval"`
	DefaultBackoffFactor   float64   `yaml:"default_backoff_factor"`
	// Error message truncation length (runes).
	ErrorTruncateLen int `yaml:"error_truncate_len"`
	// Whether to fetch error URLs for detail.
	FetchErrorURL bool `yaml:"fetch_error_url"`
	// HTTP timeout for fetching error URLs.
	ErrorURLTimeout Duration `yaml:"error_url_timeout"`
	// Alert history persistence.
	History HistoryConfig `yaml:"history"`
}

// HistoryConfig controls alert history persistence.
type HistoryConfig struct {
	Enabled bool     `yaml:"enabled"` // default true
	Dir     string   `yaml:"dir"`     // persistence directory, default "data"
	MaxAge  Duration `yaml:"max_age"` // how long to keep archived records, default 720h (30d)
}

type DatabaseRule struct {
	Database string          `yaml:"database"`
	Jobs     []JobRule       `yaml:"jobs,omitempty"` // optional per-job overrides; empty = all jobs
	Alert    *AlertOverride  `yaml:"alert,omitempty"` // database-level overrides
}

type JobRule struct {
	Name   string         `yaml:"name"`
	Alert  *AlertOverride `yaml:"alert,omitempty"`
}

// AlertOverride allows per-database / per-job override of timing.
type AlertOverride struct {
	InitialInterval *Duration `yaml:"initial_interval,omitempty"`
	MaxInterval     *Duration `yaml:"max_interval,omitempty"`
	BackoffFactor   *float64  `yaml:"backoff_factor,omitempty"`
}

// Load reads and parses the config file (standalone mode, top-level keys).
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c := &Config{}
	if err := yaml.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(c)
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadFromYAML extracts a namespaced sub-config from a host application's YAML.
// Use this when embedding as a library to avoid top-level key conflicts.
//
// Example host config:
//
//	server:
//	  port: 8080
//	doris_alert:           ← namespace
//	  doris:
//	    host: "127.0.0.1"
//	  alert:
//	    scan_interval: "60s"
//
// Usage: cfg, err := config.LoadFromYAML(hostYAML, "doris_alert")
func LoadFromYAML(data []byte, namespace string) (*Config, error) {
	if namespace == "" {
		// No namespace: parse as top-level (same as Load).
		c := &Config{}
		if err := yaml.Unmarshal(data, c); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
		return finalize(c)
	}

	// Parse into a generic map to find the namespace node.
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	// Find the namespace key in the mapping node.
	if root.Kind != yaml.DocumentNode || root.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("yaml is not a mapping document")
	}

	mapNode := root.Content[0]
	var nsNode *yaml.Node
	for i := 0; i < len(mapNode.Content)-1; i += 2 {
		if mapNode.Content[i].Value == namespace {
			nsNode = mapNode.Content[i+1]
			break
		}
	}
	if nsNode == nil {
		return nil, fmt.Errorf("namespace %q not found in yaml", namespace)
	}

	c := &Config{}
	if err := nsNode.Decode(c); err != nil {
		return nil, fmt.Errorf("decode namespace %q: %w", namespace, err)
	}

	return finalize(c)
}

func finalize(c *Config) (*Config, error) {
	applyDefaults(c)
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func applyDefaults(c *Config) {
	if c.ScanDatabases.Mode == "" {
		c.ScanDatabases.Mode = "configured"
	}
	if c.Doris.Port == 0 {
		c.Doris.Port = 9030
	}
	if c.Alert.ScanInterval.Duration == 0 {
		c.Alert.ScanInterval.Duration = 60 * time.Second
	}
	if c.Alert.DefaultInitialInterval.Duration == 0 {
		c.Alert.DefaultInitialInterval.Duration = 5 * time.Minute
	}
	if c.Alert.DefaultMaxInterval.Duration == 0 {
		c.Alert.DefaultMaxInterval.Duration = 60 * time.Minute
	}
	if c.Alert.DefaultBackoffFactor == 0 {
		c.Alert.DefaultBackoffFactor = 2.0
	}
	if c.Alert.ErrorTruncateLen <= 0 {
		c.Alert.ErrorTruncateLen = 300
	}
	if c.Alert.ErrorURLTimeout.Duration == 0 {
		c.Alert.ErrorURLTimeout.Duration = 5 * time.Second
	}
	if c.Alert.History.Dir == "" {
		c.Alert.History.Dir = "data"
	}
	if c.Alert.History.MaxAge.Duration == 0 {
		c.Alert.History.MaxAge.Duration = 720 * time.Hour // 30 days
	}
}

func validate(c *Config) error {
	// doris.host is optional: third-party callers can pass *gorm.DB directly.
	if c.Feishu.WebhookURL == "" {
		return fmt.Errorf("feishu.webhook_url is required")
	}

	// Validate scan_databases mode.
	switch c.ScanDatabases.Mode {
	case "configured":
		if len(c.Database) == 0 {
			return fmt.Errorf("at least one database rule is required (mode=configured)")
		}
	case "all":
		// database section is optional in "all" mode
	default:
		return fmt.Errorf("scan_databases.mode must be 'all' or 'configured', got %q", c.ScanDatabases.Mode)
	}

	// Validate exclude_patterns are valid regexps.
	for _, pattern := range c.ScanDatabases.ExcludePatterns {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("scan_databases.exclude_patterns: invalid regex %q: %w", pattern, err)
		}
	}

	for i, db := range c.Database {
		if db.Database == "" {
			return fmt.Errorf("database[%d].database is required", i)
		}
	}
	return nil
}

// FilterDatabases filters a list of database names based on scan_databases config.
// It removes: built-in system DBs, override_system_databases, exclude, and exclude_patterns.
func (c *Config) FilterDatabases(allDBs []string) []string {
	// Build exclusion set: DefaultSystemDBs ∪ OverrideSystemDBs ∪ Exclude
	excludeSet := make(map[string]bool)
	for _, db := range DefaultSystemDBs {
		excludeSet[db] = true
	}
	for _, db := range c.ScanDatabases.OverrideSystemDBs {
		excludeSet[db] = true
	}
	for _, db := range c.ScanDatabases.Exclude {
		excludeSet[db] = true
	}

	// Compile exclude patterns.
	var patterns []*regexp.Regexp
	for _, p := range c.ScanDatabases.ExcludePatterns {
		re, _ := regexp.Compile(p) // already validated in validate()
		patterns = append(patterns, re)
	}

	var result []string
	for _, db := range allDBs {
		if excludeSet[db] {
			continue
		}
		excluded := false
		for _, re := range patterns {
			if re.MatchString(db) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		result = append(result, db)
	}
	return result
}

// GetEffective returns the resolved alert parameters for a given database + job name.
// Priority: job-level > database-level > global default.
func (c *Config) GetEffective(dbName, jobName string) (initialInterval, maxInterval time.Duration, backoffFactor float64) {
	initialInterval = c.Alert.DefaultInitialInterval.Duration
	maxInterval = c.Alert.DefaultMaxInterval.Duration
	backoffFactor = c.Alert.DefaultBackoffFactor

	for _, db := range c.Database {
		if db.Database != dbName {
			continue
		}
		// database-level override
		if db.Alert != nil {
			if db.Alert.InitialInterval != nil {
				initialInterval = db.Alert.InitialInterval.Duration
			}
			if db.Alert.MaxInterval != nil {
				maxInterval = db.Alert.MaxInterval.Duration
			}
			if db.Alert.BackoffFactor != nil {
				backoffFactor = *db.Alert.BackoffFactor
			}
		}
		// job-level override
		for _, job := range db.Jobs {
			if job.Name == jobName && job.Alert != nil {
				if job.Alert.InitialInterval != nil {
					initialInterval = job.Alert.InitialInterval.Duration
				}
				if job.Alert.MaxInterval != nil {
					maxInterval = job.Alert.MaxInterval.Duration
				}
				if job.Alert.BackoffFactor != nil {
					backoffFactor = *job.Alert.BackoffFactor
				}
			}
		}
	}
	return
}
