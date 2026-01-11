# Doris Routine Load 告警系统

> 配置驱动、轻量级的 Apache Doris Routine Load 健康监控告警器，支持告警历史持久化和飞书通知。

## 功能特性

- **周期性扫描**：内置 ticker 定时拉取 Doris Routine Load 作业状态，无需外部调度
- **三层配置覆盖**：全局默认 → 数据库级 → 作业级，精确控制每个作业的告警频率
- **指数退避**：同一作业告警间隔随发送次数指数增长，防止告警风暴
- **错误去重**：多个 ErrorLogURL 返回相同内容时自动合并，减少噪音
- **告警历史持久化**：双文件 JSON 持久化（活跃 + 归档），记录告警完整生命周期
- **恢复通知**：作业恢复时自动发送飞书卡片，包含持续时间和累计告警次数
- **飞书签名**：支持 HMAC-SHA256 Webhook 签名验证
- **库模式**：可作为第三方库嵌入宿主应用，支持命名空间配置加载
- **黑名单模式**：通过 `SHOW DATABASES` 自动发现数据库，内置屏蔽系统库，支持正则排除
- **Lag 延迟告警**：检测分区消费延迟，任一分区超阈值即告警（默认关闭，三级可配）

## 目录结构

```
alert_routine_load/
├── cmd/
│   └── main.go                  # 独立运行入口：配置加载、组件组装、主循环、优雅退出
├── alerter/
│   ├── alerter.go               # 核心决策引擎：过滤、退避、错误获取与去重
│   ├── alerter_test.go          # 决策引擎单元测试
│   ├── history.go               # AlertHistory：告警生命周期管理 + JSON 持久化
│   └── history_test.go          # 告警历史单元测试
├── config/
│   ├── config.go                # YAML 配置加载、校验、三层优先级解析、命名空间加载
│   └── config_test.go           # 配置解析单元测试
├── model/
│   └── model.go                 # 领域模型：RoutineLoadJob、AlertEvent、AlertStatus、AlertRecord
├── notifier/
│   └── feishu.go                # 飞书 Webhook 卡片消息构建与发送（含签名）
├── scanner/
│   └── scanner.go               # Doris SQL 查询（SHOW ROUTINE LOAD）、行解析、字段映射
├── conf/
│   └── alert.yaml.example       # 示例配置文件
├── go.mod
├── README.md
└── Makefile_                   # 构建脚本（注意文件名带下划线）
```

### 文件关系

```
cmd/main.go
  ├── config.Load()          → config/config.go      加载并校验 YAML 配置
  ├── gorm.Open()            → scanner.New()          建立 Doris 连接，注入 Scanner
  ├── alerter.New()          → alerter/alerter.go     创建决策引擎（可选注入 AlertHistory）
  ├── notifier.New()         → notifier/feishu.go     创建飞书通知器
  └── alerter.NewHistory()   → alerter/history.go     创建告警历史管理器

alerter/
  ├── 依赖 config.Config    读取告警策略参数
  ├── 依赖 model.*          使用领域模型
  └── AlertHistory           独立的持久化管理器，可选注入

scanner/
  └── 依赖 *gorm.DB          通过 GORM 执行 Doris SQL

notifier/
  ├── 依赖 config.FeishuConfig  读取 Webhook 配置
  └── 依赖 alerter.AlertDecision 接收告警决策
```

## 快速开始

### 独立运行

```bash
# 1. 编译
make -f Makefile_ build

# 2. 复制并编辑配置文件
cp conf/alert.yaml.example conf/alert.yaml
# 编辑 conf/alert.yaml，填入 Doris 连接信息和飞书 Webhook URL

# 3. 运行
./bin/doris-alert -c conf/alert.yaml
```

### 作为第三方库使用

```go
import (
    "github.com/jimmy-boss/alert_routine_load/alerter"
    "github.com/jimmy-boss/alert_routine_load/config"
    "github.com/jimmy-boss/alert_routine_load/notifier"
    "github.com/jimmy-boss/alert_routine_load/scanner"
    glog "github.com/jimmy-boss/go-log/glog"

    "gorm.io/gorm"
)

func StartAlert(db *gorm.DB, appYAML []byte, log glog.HLogger) {
    // 从宿主应用 YAML 中提取命名空间配置（doris 字段可省略）
    cfg, err := config.LoadFromYAML(appYAML, "doris_alert")
    if err != nil {
        log.Fatal("load config failed", zap.Error(err))
    }

    scan := scanner.New(db, scanner.WithLogger(log))
    alert := alerter.New(cfg, alerter.WithLogger(log))
    notify := notifier.New(&cfg.Feishu, notifier.WithLogger(log))

    // ... 主循环逻辑见 cmd/main.go
}
```

> 所有组件均通过 `WithLogger` Option 注入日志实现，未注入时自动回退到 `glog.GlobalLoggers["default"]`。

宿主应用配置文件示例：

```yaml
# 宿主应用配置
server:
  port: 8080

# alert_routine_load 嵌套在命名空间下，doris 字段可省略
doris_alert:
  feishu:
    webhook_url: "https://open.feishu.cn/open-apis/bot/v2/hook/your-hook-id"
  alert:
    scan_interval: "60s"
  database:
    - database: "my_db"
```

## 配置说明

### 完整配置示例

```yaml
# Doris FE 连接信息（仅独立运行模式需要，库模式下由调用方传入 *gorm.DB）
doris:
  host: "127.0.0.1"
  port: 9030              # 默认 9030
  user: "root"
  password: "your-password"

# 飞书 Webhook 配置
feishu:
  webhook_url: "https://open.feishu.cn/open-apis/bot/v2/hook/your-hook-id"
  sign_secret: ""         # 可选：飞书签名密钥

# 告警策略
alert:
  scan_interval: "60s"              # 扫描间隔
  default_initial_interval: "5m"    # 首次告警后的最小间隔
  default_max_interval: "60m"       # 最大间隔上限
  default_backoff_factor: 2.0       # 每次发送后间隔倍增系数
  error_truncate_len: 300           # 错误详情截断长度（字符）
  fetch_error_url: true             # 是否获取错误 URL 详情
  error_url_timeout: "5s"           # 错误 URL 请求超时
  history:
    enabled: true                   # 是否启用告警历史
    dir: "data"                     # 持久化目录
    max_age: "720h"                 # 归档记录保留时长（30 天）
  # lag:
  #   enabled: true                 # 是否启用 Lag 延迟告警（默认关闭）
  #   threshold: 10000              # 单分区延迟阈值（消息条数）

# 监控规则
database:
  - database: "your_database"
    alert:                          # 数据库级覆盖（可选）
      initial_interval: "2m"
    jobs:
      - name: "your_routine_load_job"
        alert:                      # 作业级覆盖（可选，最高优先级）
          initial_interval: "1m"
          backoff_factor: 3.0

  - database: "another_database"   # 监控整个数据库的所有作业

# 黑名单模式（可选，不配置则使用白名单模式）
# scan_databases:
#   mode: "all"                     # "all" = 自动发现 | "configured" = 仅 database 段（默认）
#   exclude:                        # 精确排除
#     - "tmp_db"
#   exclude_patterns:               # 正则排除
#     - "^test_.*"
#   override_system_databases:      # 追加内置系统库列表（默认: information_schema, __internal_schema, mysql）
#     - "__statistics__"
```

### 三层配置优先级

```
作业级 (jobs[].alert) > 数据库级 (database[].alert) > 全局默认 (alert.default_*)
```

### 退避算法

```
delay(n) = min(initial_interval × backoff_factor^n, max_interval)
```

默认参数效果：
| 告警次数 | 等待间隔 |
|----------|----------|
| 第 1 次后 | 5 分钟 |
| 第 2 次后 | 10 分钟 |
| 第 3 次后 | 20 分钟 |
| 第 4 次后 | 40 分钟 |
| 第 5 次起 | 60 分钟（封顶） |

作业恢复后自动重置退避计数。

## 飞书通知

### 告警卡片

当作业进入 PAUSED 状态时，发送包含以下信息的飞书卡片：

- 标题：`🚨 Doris Routine Load 告警 [db_name]`
- Job ID / Job Name / Database
- 当前状态 / 暂停时间 / 变更原因
- 错误详情预览（去重 + 截断）
- 告警持续时间 / 累计告警次数

### 恢复卡片

作业恢复正常后，发送：

- 标题：`✅ Doris Routine Load 恢复 [db_name]`
- 告警持续时间 / 累计告警次数 / 恢复时间

## 构建与测试

```bash
# 构建
make -f Makefile_ build

# 运行全部测试
make -f Makefile_ test

# 运行特定包的测试
go test ./alerter/ -v
go test ./config/ -v

# 运行特定测试
go test -run TestGetEffective -v ./config/

# 清理
make -f Makefile_ clean
```

## 运行时资源

- **内存**：< 20MB（纯内存状态 + 少量 HTTP 连接）
- **CPU**：每个 scan 周期执行 N 条 SQL（N = 数据库数），极低
- **网络**：每次扫描 N 条 SHOW ROUTINE LOAD + M 条 Error URL GET

## 扩展点

| 方向 | 方式 |
|------|------|
| 新增通知渠道（钉钉/企微/Slack） | 实现 `Notifier` 接口 `Send(AlertDecision) error`，在 main.go 中注入 |
| 新增监控状态（如 RUNNING 但 lag 过大） | 在 `alerter.evaluateOne` 中扩展判断条件 |
| Prometheus 指标 | 暴露 `paused_jobs_total`、`alerts_sent_total`、`alert_skip_total` 等 |
| 告警静默期 | 在 `AlertConfig` 增加 `silence_start` / `silence_end` |

## 技术栈

- Go 1.23.6
- GORM（Doris SQL 查询）
- gopkg.in/yaml.v3（配置解析）
- go-log（结构化日志，基于 zap，支持日志轮转）

## 版本记录

| 日期         | 版本 | 变更内容 |
|------------|------|----------|
| 2026-01-05 | v1.0 | 初始版本：核心扫描、告警决策、飞书通知 |
| 2026-01-05 | v1.1 | 新增告警历史持久化（双文件 JSON）、恢复通知、代码审查修复 |
| 2026-01-06 | v1.2 | 新增命名空间配置加载（库模式）、`LoadFromYAML` API |
| 2026-01-08 | v1.3 | Scanner 改用 GORM、doris 配置改为可选（库模式无需配置连接信息） |
| 2026-01-08 | v1.4 | 日志从 log/slog 切换为 go-log/glog（统一 workspace 日志方案），所有组件支持 WithLogger Option 注入 |
| 2026-01-09 | v1.5 | 新增黑名单模式（scan_databases），支持 SHOW DATABASES 自动发现 + 系统库内置过滤 + 正则排除 |
| 2026-01-10 | v1.6 | 新增 Lag 消费延迟告警（默认关闭），支持三级阈值覆盖 |
| 2026-01-10 | v1.7 | 修复 Lag 误报恢复通知 + FindRecord 数据竞争（返回副本） |
| 2026-01-11 | v1.8 | 修复 Important 级别：新增 notifier/scanner 测试、Save 优化、fetchAndDedup 占位符、run 超时 |
