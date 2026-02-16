// @Author: Jimmy
// @DateTime: 2026/02/15

// Package config 提供 YAML 配置加载与三层覆盖逻辑。
package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration 支持 "5m" 格式的 YAML 时间解析。
type Duration struct {
	time.Duration
}

// UnmarshalYAML 实现 yaml.Unmarshaler 接口。
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("解析 Duration 失败: %w", err)
	}
	d.Duration = parsed
	return nil
}

// DefaultSystemDBs 默认的系统数据库列表，扫描时跳过。
var DefaultSystemDBs = []string{
	"information_schema",
	"mysql",
	"_statistics_",
	"doris_audit_db__",
	"__internal_schema",
}

// Config 是顶层配置结构体。
type Config struct {
	Doris         DorisConfig         `yaml:"doris"`
	Notify        NotifyConfig        `yaml:"notify"`
	Alert         AlertConfig         `yaml:"alert"`
	ScanDatabases ScanDatabasesConfig `yaml:"scan_databases"`
	Database      []DatabaseRule      `yaml:"database"`
}

// DorisConfig Doris 连接配置。
type DorisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// NotifyConfig 通知配置，支持多渠道。
type NotifyConfig struct {
	Channel  string         `yaml:"channel"`
	Feishu   FeishuConfig   `yaml:"feishu"`
	Dingtalk DingtalkConfig `yaml:"dingtalk"`
}

// FeishuConfig 飞书通知配置。
type FeishuConfig struct {
	WebhookURL string `yaml:"webhook_url"`
	SignSecret string `yaml:"sign_secret"`
}

// DingtalkConfig 钉钉通知配置。
type DingtalkConfig struct {
	WebhookURL string `yaml:"webhook_url"`
	Secret     string `yaml:"secret"`
}

// AlertConfig 告警全局配置。
type AlertConfig struct {
	Lag     LagConfig     `yaml:"lag"`
	History HistoryConfig `yaml:"history"`
}

// LagConfig 延迟告警配置。
type LagConfig struct {
	Threshold     int64    `yaml:"threshold"`
	Recovery      int64    `yaml:"recovery"`
	AlertInterval Duration `yaml:"alert_interval"`
	BackoffFactor float64  `yaml:"backoff_factor"`
	MaxInterval   Duration `yaml:"max_interval"`
	MaxSendCount  int      `yaml:"max_send_count"`
}

// HistoryConfig 历史记录配置。
type HistoryConfig struct {
	RetentionDays int `yaml:"retention_days"`
}

// ScanDatabasesConfig 扫描数据库配置。
type ScanDatabasesConfig struct {
	Exclude         []string `yaml:"exclude"`
	ExcludePatterns []string `yaml:"exclude_patterns"`
}

// DatabaseRule 数据库级规则。
type DatabaseRule struct {
	Name  string         `yaml:"name"`
	Alert *AlertOverride `yaml:"alert,omitempty"`
	Jobs  []JobRule      `yaml:"jobs"`
}

// JobRule Job 级规则。
type JobRule struct {
	Name  string        `yaml:"name"`
	Alert AlertOverride `yaml:"alert"`
}

// AlertOverride 告警覆盖配置。
type AlertOverride struct {
	Lag LagOverride `yaml:"lag"`
}

// LagOverride 延迟覆盖配置。
type LagOverride struct {
	Threshold *int64 `yaml:"threshold"`
	Recovery  *int64 `yaml:"recovery"`
}

// Load 从 YAML 文件加载配置（顶层模式）。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	return LoadFromBytes(data, "")
}

// LoadFromBytes 从 YAML 字节流加载配置。
// namespace 为空时按顶层解析；非空时从宿主 YAML 中提取指定 key 的子树解析。
//
// 宿主 YAML 示例：
//
//	server:
//	  port: 8080
//	doris_alert:            ← namespace
//	  doris:
//	    host: "127.0.0.1"
//	  notify:
//	    channel: feishu
//
// 调用方式：cfg, err := config.LoadFromBytes(hostYAML, "doris_alert")
func LoadFromBytes(data []byte, namespace string) (*Config, error) {
	if namespace == "" {
		var cfg Config
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("解析配置失败: %w", err)
		}
		return finalize(&cfg)
	}

	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("解析 YAML 失败: %w", err)
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("YAML 不是 mapping 文档")
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
		return nil, fmt.Errorf("命名空间 %q 未找到", namespace)
	}

	var cfg Config
	if err := nsNode.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("解码命名空间 %q 失败: %w", namespace, err)
	}
	return finalize(&cfg)
}

func finalize(cfg *Config) (*Config, error) {
	applyDefaults(cfg)
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}
	return cfg, nil
}

// applyDefaults 设置默认值。
func applyDefaults(cfg *Config) {
	if cfg.Notify.Channel == "" {
		cfg.Notify.Channel = "feishu"
	}
	if cfg.Alert.Lag.AlertInterval.Duration == 0 {
		cfg.Alert.Lag.AlertInterval = Duration{5 * time.Minute}
	}
	if cfg.Alert.Lag.BackoffFactor == 0 {
		cfg.Alert.Lag.BackoffFactor = 1.5
	}
	if cfg.Alert.Lag.MaxInterval.Duration == 0 {
		cfg.Alert.Lag.MaxInterval = Duration{1 * time.Hour}
	}
	if cfg.Alert.Lag.MaxSendCount == 0 {
		cfg.Alert.Lag.MaxSendCount = 10
	}
	if cfg.Alert.History.RetentionDays == 0 {
		cfg.Alert.History.RetentionDays = 7
	}
	if cfg.ScanDatabases.Exclude == nil {
		cfg.ScanDatabases.Exclude = DefaultSystemDBs
	}
}

// validate 校验配置合法性。
func validate(cfg *Config) error {
	switch cfg.Notify.Channel {
	case "feishu":
		if cfg.Notify.Feishu.WebhookURL == "" {
			return fmt.Errorf("notify.feishu.webhook_url 不能为空")
		}
	case "dingtalk":
		if cfg.Notify.Dingtalk.WebhookURL == "" {
			return fmt.Errorf("notify.dingtalk.webhook_url 不能为空")
		}
	default:
		return fmt.Errorf("不支持的通知渠道: %s，仅支持 feishu/dingtalk", cfg.Notify.Channel)
	}

	for _, p := range cfg.ScanDatabases.ExcludePatterns {
		if _, err := regexp.Compile(p); err != nil {
			return fmt.Errorf("exclude_patterns 正则无效 %q: %w", p, err)
		}
	}

	if cfg.Alert.Lag.BackoffFactor < 1.0 {
		return fmt.Errorf("alert.lag.backoff_factor 不能小于 1.0，当前值: %v", cfg.Alert.Lag.BackoffFactor)
	}
	if cfg.Alert.Lag.MaxSendCount < 0 {
		return fmt.Errorf("alert.lag.max_send_count 不能为负数，当前值: %d", cfg.Alert.Lag.MaxSendCount)
	}

	return nil
}

// FilterDatabases 过滤掉排除列表中的数据库（支持精确匹配和正则匹配）。
func (c *Config) FilterDatabases(databases []string) []string {
	exclude := make(map[string]struct{}, len(c.ScanDatabases.Exclude))
	for _, db := range c.ScanDatabases.Exclude {
		exclude[db] = struct{}{}
	}

	var patterns []*regexp.Regexp
	for _, p := range c.ScanDatabases.ExcludePatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			continue
		}
		patterns = append(patterns, re)
	}

	var result []string
	for _, db := range databases {
		if _, ok := exclude[db]; ok {
			continue
		}
		matched := false
		for _, re := range patterns {
			if re.MatchString(db) {
				matched = true
				break
			}
		}
		if !matched {
			result = append(result, db)
		}
	}
	return result
}

// EffectiveLag 生效的延迟配置。
type EffectiveLag struct {
	Threshold int64
	Recovery  int64
}

// GetEffective 获取指定数据库和 Job 的生效配置。
func (c *Config) GetEffective(database, jobName string) EffectiveLag {
	lag := c.GetEffectiveLag(database, jobName)
	return lag
}

// GetEffectiveLag 获取三层覆盖后的延迟阈值。
// 优先级：job 级 > database 级 > 全局默认。
func (c *Config) GetEffectiveLag(database, jobName string) EffectiveLag {
	result := EffectiveLag{
		Threshold: c.Alert.Lag.Threshold,
		Recovery:  c.Alert.Lag.Recovery,
	}

	for _, db := range c.Database {
		if db.Name != database {
			continue
		}
		// database 级覆盖
		if db.Alert != nil && db.Alert.Lag.Threshold != nil {
			result.Threshold = *db.Alert.Lag.Threshold
		}
		if db.Alert != nil && db.Alert.Lag.Recovery != nil {
			result.Recovery = *db.Alert.Lag.Recovery
		}
		// job 级覆盖
		for _, job := range db.Jobs {
			if job.Name != jobName {
				continue
			}
			if job.Alert.Lag.Threshold != nil {
				result.Threshold = *job.Alert.Lag.Threshold
			}
			if job.Alert.Lag.Recovery != nil {
				result.Recovery = *job.Alert.Lag.Recovery
			}
			return result
		}
		return result
	}

	return result
}
