// @Author: Jimmy
// @DateTime: 2026/02/15

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_ValidConfig(t *testing.T) {
	yaml := `
doris:
  host: "127.0.0.1"
  port: 9030
  user: "root"
  password: ""
notify:
  channel: "feishu"
  feishu:
    webhook_url: "https://open.feishu.cn/open-apis/bot/v2/hook/test"
alert:
  lag:
    threshold: 10000
    recovery: 5000
    alert_interval: "5m"
    max_send_count: 10
  history:
    retention_days: 7
scan_databases:
  exclude:
    - "information_schema"
    - "mysql"
database:
  - name: "my_db"
    jobs:
      - name: "my_job"
        alert:
          lag:
            threshold: 20000
`
	path := writeTempYAML(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Doris.Host != "127.0.0.1" {
		t.Errorf("Doris.Host = %q, 期望 %q", cfg.Doris.Host, "127.0.0.1")
	}
	if cfg.Notify.Channel != "feishu" {
		t.Errorf("Notify.Channel = %q, 期望 %q", cfg.Notify.Channel, "feishu")
	}
	if cfg.Alert.Lag.Threshold != 10000 {
		t.Errorf("Alert.Lag.Threshold = %d, 期望 10000", cfg.Alert.Lag.Threshold)
	}
	if cfg.Alert.Lag.AlertInterval.Duration.Seconds() != 300 {
		t.Errorf("Alert.Lag.AlertInterval = %v, 期望 5m", cfg.Alert.Lag.AlertInterval)
	}
	if len(cfg.Database) != 1 {
		t.Errorf("Database 长度 = %d, 期望 1", len(cfg.Database))
	}
}

func TestLoad_MissingWebhookURL(t *testing.T) {
	yaml := `
notify:
  channel: "feishu"
`
	path := writeTempYAML(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("期望校验失败，但未报错")
	}
	if expected := "webhook_url"; !containsStr(err.Error(), expected) {
		t.Errorf("错误信息 %q 应包含 %q", err.Error(), expected)
	}
}

func TestGetEffectiveLag_ThreeLayerOverride(t *testing.T) {
	globalThreshold := int64(10000)
	globalRecovery := int64(5000)
	jobThreshold := int64(30000)

	cfg := &Config{
		Alert: AlertConfig{
			Lag: LagConfig{
				Threshold: globalThreshold,
				Recovery:  globalRecovery,
			},
		},
		Database: []DatabaseRule{
			{
				Name: "my_db",
				Jobs: []JobRule{
					{
						Name: "my_job",
						Alert: AlertOverride{
							Lag: LagOverride{
								Threshold: &jobThreshold,
							},
						},
					},
				},
			},
		},
	}

	// 未匹配的 Job，使用全局默认
	lag := cfg.GetEffectiveLag("other_db", "other_job")
	if lag.Threshold != globalThreshold {
		t.Errorf("未匹配: Threshold = %d, 期望 %d", lag.Threshold, globalThreshold)
	}
	if lag.Recovery != globalRecovery {
		t.Errorf("未匹配: Recovery = %d, 期望 %d", lag.Recovery, globalRecovery)
	}

	// 匹配 database 但未匹配 job，使用全局默认
	lag = cfg.GetEffectiveLag("my_db", "unknown_job")
	if lag.Threshold != globalThreshold {
		t.Errorf("匹配 db 未匹配 job: Threshold = %d, 期望 %d", lag.Threshold, globalThreshold)
	}

	// 匹配 job，使用 job 级覆盖
	lag = cfg.GetEffectiveLag("my_db", "my_job")
	if lag.Threshold != jobThreshold {
		t.Errorf("匹配 job: Threshold = %d, 期望 %d", lag.Threshold, jobThreshold)
	}
	if lag.Recovery != globalRecovery {
		t.Errorf("匹配 job: Recovery = %d, 期望 %d", lag.Recovery, globalRecovery)
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestLoadFromBytes_TopLevel(t *testing.T) {
	data := []byte(`
notify:
  channel: "feishu"
  feishu:
    webhook_url: "https://example.com/hook"
`)
	cfg, err := LoadFromBytes(data, "")
	if err != nil {
		t.Fatalf("LoadFromBytes 空命名空间失败: %v", err)
	}
	if cfg.Notify.Channel != "feishu" {
		t.Errorf("Channel = %q, 期望 feishu", cfg.Notify.Channel)
	}
}

func TestLoadFromBytes_Namespace(t *testing.T) {
	data := []byte(`
server:
  port: 8080
doris_alert:
  doris:
    host: "192.168.1.1"
    port: 9030
  notify:
    channel: "feishu"
    feishu:
      webhook_url: "https://example.com/hook"
  alert:
    lag:
      threshold: 8000
`)
	cfg, err := LoadFromBytes(data, "doris_alert")
	if err != nil {
		t.Fatalf("LoadFromBytes 命名空间失败: %v", err)
	}
	if cfg.Doris.Host != "192.168.1.1" {
		t.Errorf("Doris.Host = %q, 期望 192.168.1.1", cfg.Doris.Host)
	}
	if cfg.Alert.Lag.Threshold != 8000 {
		t.Errorf("Lag.Threshold = %d, 期望 8000", cfg.Alert.Lag.Threshold)
	}
}

func TestLoadFromBytes_NamespaceNotFound(t *testing.T) {
	data := []byte(`
server:
  port: 8080
`)
	_, err := LoadFromBytes(data, "doris_alert")
	if err == nil {
		t.Fatal("期望报错命名空间未找到")
	}
}

func TestFilterDatabases_ExcludePatterns(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{
			Exclude:         []string{"information_schema"},
			ExcludePatterns: []string{`^tmp_.*`, `.*_bak$`},
		},
	}
	input := []string{
		"my_db",
		"information_schema",
		"tmp_test",
		"tmp_prod",
		"old_bak",
		"production",
	}
	result := cfg.FilterDatabases(input)
	expected := []string{"my_db", "production"}
	if len(result) != len(expected) {
		t.Fatalf("期望 %d 个，得到 %d 个: %v", len(expected), len(result), result)
	}
	for i, db := range result {
		if db != expected[i] {
			t.Errorf("result[%d] = %q, 期望 %q", i, db, expected[i])
		}
	}
}

func TestFilterDatabases_InvalidRegex(t *testing.T) {
	cfg := &Config{
		ScanDatabases: ScanDatabasesConfig{
			ExcludePatterns: []string{`[invalid`},
		},
	}
	input := []string{"my_db"}
	result := cfg.FilterDatabases(input)
	// 无效正则应被跳过，不影响结果
	if len(result) != 1 || result[0] != "my_db" {
		t.Errorf("无效正则应被跳过，得到 %v", result)
	}
}

func TestValidate_InvalidRegexPattern(t *testing.T) {
	cfg := &Config{
		Notify: NotifyConfig{
			Channel: "feishu",
			Feishu:  FeishuConfig{WebhookURL: "https://example.com/hook"},
		},
		ScanDatabases: ScanDatabasesConfig{
			ExcludePatterns: []string{`[invalid`},
		},
	}
	err := validate(cfg)
	if err == nil {
		t.Fatal("期望校验失败")
	}
}
