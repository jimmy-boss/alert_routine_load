package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_ValidConfig(t *testing.T) {
	content := `
doris:
  host: "127.0.0.1"
  user: "root"
  password: "test"
feishu:
  webhook_url: "https://example.com/hook"
alert:
  scan_interval: "30s"
  default_initial_interval: "5m"
  default_max_interval: "60m"
  default_backoff_factor: 2.0
  error_truncate_len: 500
  fetch_error_url: true
  error_url_timeout: "3s"
database:
  - database: "test_db"
    jobs:
      - name: "job1"
`
	path := writeTemp(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Doris.Host != "127.0.0.1" {
		t.Errorf("host = %q, want %q", cfg.Doris.Host, "127.0.0.1")
	}
	if cfg.Doris.Port != 9030 {
		t.Errorf("port = %d, want 9030", cfg.Doris.Port)
	}
	if cfg.Alert.ScanInterval.Duration != 30*time.Second {
		t.Errorf("scan_interval = %v, want 30s", cfg.Alert.ScanInterval)
	}
	if cfg.Alert.ErrorTruncateLen != 500 {
		t.Errorf("error_truncate_len = %d, want 500", cfg.Alert.ErrorTruncateLen)
	}
}

func TestLoad_Defaults(t *testing.T) {
	content := `
doris:
  host: "127.0.0.1"
feishu:
  webhook_url: "https://example.com/hook"
database:
  - database: "db1"
`
	path := writeTemp(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Doris.Port != 9030 {
		t.Errorf("default port = %d, want 9030", cfg.Doris.Port)
	}
	if cfg.Alert.ScanInterval.Duration != 60*time.Second {
		t.Errorf("default scan_interval = %v, want 60s", cfg.Alert.ScanInterval)
	}
	if cfg.Alert.DefaultInitialInterval.Duration != 5*time.Minute {
		t.Errorf("default initial_interval = %v, want 5m", cfg.Alert.DefaultInitialInterval)
	}
	if cfg.Alert.DefaultMaxInterval.Duration != 60*time.Minute {
		t.Errorf("default max_interval = %v, want 60m", cfg.Alert.DefaultMaxInterval)
	}
	if cfg.Alert.DefaultBackoffFactor != 2.0 {
		t.Errorf("default backoff_factor = %f, want 2.0", cfg.Alert.DefaultBackoffFactor)
	}
	if cfg.Alert.ErrorTruncateLen != 300 {
		t.Errorf("default error_truncate_len = %d, want 300", cfg.Alert.ErrorTruncateLen)
	}
	if cfg.Alert.ErrorURLTimeout.Duration != 5*time.Second {
		t.Errorf("default error_url_timeout = %v, want 5s", cfg.Alert.ErrorURLTimeout)
	}
}

func TestLoad_MissingHost(t *testing.T) {
	content := `
feishu:
  webhook_url: "https://example.com/hook"
database:
  - database: "db1"
`
	path := writeTemp(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	// doris.host is optional for library mode (caller passes *gorm.DB).
	if cfg.Doris.Host != "" {
		t.Errorf("host should be empty, got %q", cfg.Doris.Host)
	}
}

func TestLoad_MissingWebhookURL(t *testing.T) {
	content := `
doris:
  host: "127.0.0.1"
database:
  - database: "db1"
`
	path := writeTemp(t, content)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing webhook_url")
	}
}

func TestLoad_MissingDatabase(t *testing.T) {
	content := `
doris:
  host: "127.0.0.1"
feishu:
  webhook_url: "https://example.com/hook"
database: []
`
	path := writeTemp(t, content)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty database list")
	}
}

func TestLoad_DatabaseEmptyName(t *testing.T) {
	content := `
doris:
  host: "127.0.0.1"
feishu:
  webhook_url: "https://example.com/hook"
database:
  - database: ""
`
	path := writeTemp(t, content)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty database name")
	}
}

func TestDuration_UnmarshalYAML_HumanReadable(t *testing.T) {
	content := `
doris:
  host: "127.0.0.1"
feishu:
  webhook_url: "https://example.com/hook"
alert:
  scan_interval: "2m30s"
database:
  - database: "db1"
`
	path := writeTemp(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Alert.ScanInterval.Duration != 2*time.Minute+30*time.Second {
		t.Errorf("scan_interval = %v, want 2m30s", cfg.Alert.ScanInterval)
	}
}

func TestGetEffective_ThreeLayerOverride(t *testing.T) {
	globalInitial := 5 * time.Minute
	dbInitial := 2 * time.Minute
	jobInitial := 1 * time.Minute

	cfg := &Config{
		Alert: AlertConfig{
			DefaultInitialInterval: Duration{globalInitial},
			DefaultMaxInterval:     Duration{60 * time.Minute},
			DefaultBackoffFactor:   2.0,
		},
		Database: []DatabaseRule{
			{
				Database: "db1",
				Alert: &AlertOverride{
					InitialInterval: &Duration{dbInitial},
				},
				Jobs: []JobRule{
					{
						Name: "job1",
						Alert: &AlertOverride{
							InitialInterval: &Duration{jobInitial},
						},
					},
				},
			},
		},
	}

	// Test global default (non-existent database)
	initial, _, _ := cfg.GetEffective("other_db", "other_job")
	if initial != globalInitial {
		t.Errorf("global: initial = %v, want %v", initial, globalInitial)
	}

	// Test database-level override
	initial, _, _ = cfg.GetEffective("db1", "other_job")
	if initial != dbInitial {
		t.Errorf("db-level: initial = %v, want %v", initial, dbInitial)
	}

	// Test job-level override (highest priority)
	initial, _, _ = cfg.GetEffective("db1", "job1")
	if initial != jobInitial {
		t.Errorf("job-level: initial = %v, want %v", initial, jobInitial)
	}
}

func TestLoadFromYAML_WithNamespace(t *testing.T) {
	hostYAML := `
server:
  port: 8080
database:
  host: "mysql://localhost"
doris_alert:
  doris:
    host: "127.0.0.1"
    port: 9030
    user: "root"
    password: "test"
  feishu:
    webhook_url: "https://example.com/hook"
  alert:
    scan_interval: "30s"
  database:
    - database: "my_db"
`
	cfg, err := LoadFromYAML([]byte(hostYAML), "doris_alert")
	if err != nil {
		t.Fatalf("LoadFromYAML() error: %v", err)
	}
	if cfg.Doris.Host != "127.0.0.1" {
		t.Errorf("host = %q, want %q", cfg.Doris.Host, "127.0.0.1")
	}
	if cfg.Alert.ScanInterval.Duration != 30*time.Second {
		t.Errorf("scan_interval = %v, want 30s", cfg.Alert.ScanInterval)
	}
	if len(cfg.Database) != 1 || cfg.Database[0].Database != "my_db" {
		t.Errorf("database = %v, want [my_db]", cfg.Database)
	}
}

func TestLoadFromYAML_EmptyNamespace(t *testing.T) {
	yamlData := `
doris:
  host: "127.0.0.1"
feishu:
  webhook_url: "https://example.com/hook"
database:
  - database: "db1"
`
	cfg, err := LoadFromYAML([]byte(yamlData), "")
	if err != nil {
		t.Fatalf("LoadFromYAML() error: %v", err)
	}
	if cfg.Doris.Host != "127.0.0.1" {
		t.Errorf("host = %q, want %q", cfg.Doris.Host, "127.0.0.1")
	}
}

func TestLoadFromYAML_NamespaceNotFound(t *testing.T) {
	yamlData := `
server:
  port: 8080
`
	_, err := LoadFromYAML([]byte(yamlData), "doris_alert")
	if err == nil {
		t.Fatal("expected error for missing namespace")
	}
}

func TestLoadFromYAML_InvalidYAML(t *testing.T) {
	_, err := LoadFromYAML([]byte(":::invalid:::"), "doris_alert")
	if err == nil {
		t.Fatal("expected error for invalid yaml")
	}
}

func TestLoadFromYAML_WithDefaults(t *testing.T) {
	yamlData := `
my_alert:
  doris:
    host: "127.0.0.1"
  feishu:
    webhook_url: "https://example.com/hook"
  database:
    - database: "db1"
`
	cfg, err := LoadFromYAML([]byte(yamlData), "my_alert")
	if err != nil {
		t.Fatalf("LoadFromYAML() error: %v", err)
	}
	// Defaults should be applied.
	if cfg.Doris.Port != 9030 {
		t.Errorf("default port = %d, want 9030", cfg.Doris.Port)
	}
	if cfg.Alert.ScanInterval.Duration != 60*time.Second {
		t.Errorf("default scan_interval = %v, want 60s", cfg.Alert.ScanInterval)
	}
}

func TestScanDatabases_DefaultMode(t *testing.T) {
	content := `
feishu:
  webhook_url: "https://example.com/hook"
database:
  - database: "db1"
`
	path := writeTemp(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ScanDatabases.Mode != "configured" {
		t.Errorf("default mode = %q, want %q", cfg.ScanDatabases.Mode, "configured")
	}
}

func TestScanDatabases_ModeAll_NoDatabase(t *testing.T) {
	content := `
feishu:
  webhook_url: "https://example.com/hook"
scan_databases:
  mode: "all"
`
	path := writeTemp(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ScanDatabases.Mode != "all" {
		t.Errorf("mode = %q, want %q", cfg.ScanDatabases.Mode, "all")
	}
	if len(cfg.Database) != 0 {
		t.Errorf("database should be empty in mode=all, got %d", len(cfg.Database))
	}
}

func TestScanDatabases_InvalidMode(t *testing.T) {
	content := `
feishu:
  webhook_url: "https://example.com/hook"
scan_databases:
  mode: "invalid"
database:
  - database: "db1"
`
	path := writeTemp(t, content)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestScanDatabases_InvalidRegex(t *testing.T) {
	content := `
feishu:
  webhook_url: "https://example.com/hook"
scan_databases:
  mode: "all"
  exclude_patterns:
    - "[invalid"
`
	path := writeTemp(t, content)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

func TestFilterDatabases_SystemDBs(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{Mode: "all"},
	}
	allDBs := []string{"information_schema", "__internal_schema", "mysql", "prod_db", "test_db"}
	result := cfg.FilterDatabases(allDBs)
	if len(result) != 2 {
		t.Fatalf("result count = %d, want 2, got %v", len(result), result)
	}
	if result[0] != "prod_db" || result[1] != "test_db" {
		t.Errorf("result = %v, want [prod_db test_db]", result)
	}
}

func TestFilterDatabases_Exclude(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{
			Mode:    "all",
			Exclude: []string{"tmp_db", "staging"},
		},
	}
	allDBs := []string{"prod_db", "tmp_db", "staging", "dev_db"}
	result := cfg.FilterDatabases(allDBs)
	if len(result) != 2 {
		t.Fatalf("result count = %d, want 2, got %v", len(result), result)
	}
	if result[0] != "prod_db" || result[1] != "dev_db" {
		t.Errorf("result = %v, want [prod_db dev_db]", result)
	}
}

func TestFilterDatabases_ExcludePatterns(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{
			Mode:            "all",
			ExcludePatterns: []string{"^test_", ".*_backup$"},
		},
	}
	allDBs := []string{"prod_db", "test_dev", "test_staging", "db_backup", "real_db"}
	result := cfg.FilterDatabases(allDBs)
	if len(result) != 2 {
		t.Fatalf("result count = %d, want 2, got %v", len(result), result)
	}
	if result[0] != "prod_db" || result[1] != "real_db" {
		t.Errorf("result = %v, want [prod_db real_db]", result)
	}
}

func TestFilterDatabases_OverrideSystemDBs(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{
			Mode:              "all",
			OverrideSystemDBs: []string{"__statistics__"},
		},
	}
	allDBs := []string{"information_schema", "__statistics__", "prod_db"}
	result := cfg.FilterDatabases(allDBs)
	if len(result) != 1 {
		t.Fatalf("result count = %d, want 1, got %v", len(result), result)
	}
	if result[0] != "prod_db" {
		t.Errorf("result = %v, want [prod_db]", result)
	}
}

func TestFilterDatabases_ExcludeOverDatabase(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{
			Mode:    "all",
			Exclude: []string{"shared_db"},
		},
		Database: []DatabaseRule{
			{Database: "shared_db"},
		},
	}
	allDBs := []string{"shared_db", "prod_db"}
	result := cfg.FilterDatabases(allDBs)
	// exclude should take priority over database section
	if len(result) != 1 {
		t.Fatalf("result count = %d, want 1, got %v", len(result), result)
	}
	if result[0] != "prod_db" {
		t.Errorf("result = %v, want [prod_db]", result)
	}
}

func TestFilterDatabases_Empty(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{Mode: "all"},
	}
	result := cfg.FilterDatabases(nil)
	if len(result) != 0 {
		t.Errorf("result should be empty, got %v", result)
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	f.Close()
	return f.Name()
}
