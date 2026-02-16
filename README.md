# alert_routine_load v2.0.0

Routine Load 告警守护进程 — 基于显式状态枚举的状态自描述架构。

## 快速开始

### 独立运行

```bash
# 1. 复制配置模板
cp conf/config.yaml.example conf/alert.yaml

# 2. 编辑 conf/alert.yaml，填入 Doris 连接信息和飞书 webhook

# 3. 编译运行
go build -o bin/doris-alert-v2 ./cmd/
./bin/doris-alert-v2 -c conf/alert.yaml
```

### 第三方接入（作为 Go 库）

其他应用可将本包作为依赖嵌入，通过命名空间加载配置：

```go
import (
    "github.com/jimmy-boss/alert_routine_load_v2/config"
    "github.com/jimmy-boss/alert_routine_load_v2/evaluator"
    "github.com/jimmy-boss/alert_routine_load_v2/scanner"
    "github.com/jimmy-boss/alert_routine_load_v2/store"
    "github.com/jimmy-boss/alert_routine_load_v2/notifier"
)
```

#### 方式一：命名空间加载（推荐）

宿主应用的 YAML 中嵌入 `doris_alert` 子树，互不干扰：

```yaml
# host_app.yaml
server:
  port: 8080
  mode: "release"

doris_alert:                    # ← 命名空间 key
  doris:
    host: "192.168.1.100"
    port: 9030
    user: "root"
    password: "xxx"
  notify:
    channel: feishu
    feishu:
      webhook_url: "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
  alert:
    lag:
      threshold: 10000
      recovery: 5000
```

```go
hostYAML, _ := os.ReadFile("host_app.yaml")

// 从宿主 YAML 中提取 "doris_alert" 命名空间
cfg, err := config.LoadFromBytes(hostYAML, "doris_alert")
if err != nil {
    log.Fatal(err)
}

// 后续正常初始化各组件
statusStore, _ := store.NewStatusStore(cfg.Alert.History.Dir, log)
scan := scanner.New(db, scanner.WithLogger(log))
eval := evaluator.New(cfg, statusStore, evaluator.WithLogger(log))
notify := notifier.NewFeishu(&cfg.Notify.Feishu)
```

#### 方式二：顶层加载

配置文件本身就是 alert_routine_load_v2 的完整配置：

```go
cfg, err := config.Load("conf/alert.yaml")
```

或从字节流：

```go
cfg, err := config.LoadFromBytes(yamlBytes, "")
```

---

## 配置说明

### 关闭 Lag 告警

Lag 告警默认全局生效（`threshold: 10000`）。有三种方式关闭：

**方式一：全局关闭**

设置极大阈值，使任何 lag 都不会触发：

```yaml
alert:
  lag:
    threshold: 999999999    # 极大值，等效关闭
    recovery: 0
```

**方式二：按数据库关闭**

不配置该数据库的 `database` 规则即可。全局阈值仍然生效，如果需要完全关闭，配合方式一使用。

**方式三：按 Job 关闭**

在 database 规则中为特定 job 设置极大阈值：

```yaml
database:
  - name: "my_db"
    jobs:
      - name: "no_lag_job"
        alert:
          lag:
            threshold: 999999999   # 该 job 不触发 lag 告警
```

### 三层覆盖优先级

```
全局默认 < database 级 < job 级
```

| 配置项 | 全局默认 | database 级 | job 级 |
|--------|----------|-------------|--------|
| lag.threshold | `alert.lag.threshold` | `database[].alert.lag.threshold` | `database[].jobs[].alert.lag.threshold` |
| lag.recovery | `alert.lag.recovery` | `database[].alert.lag.recovery` | `database[].jobs[].alert.lag.recovery` |

**注意**: `threshold=0` 表示不检查 lag 延迟（等效关闭该级别的 lag 告警）。

### 数据库扫描排除

支持精确排除和正则排除，两者同时生效：

```yaml
scan_databases:
  exclude:                      # 精确匹配
    - "information_schema"
    - "mysql"
  exclude_patterns:             # Go 正则语法
    - "^tmp_.*"                 # tmp_ 开头的库
    - ".*_bak$"                 # _bak 结尾的库
    - "^test_\\d{4}$"           # test_2026 这类库
```

- 无效正则在启动时校验失败（fail-fast）
- 系统库默认排除：`information_schema`、`mysql`、`_statistics_`、`doris_audit_db__`

### 指数退避

告警支持指数退避，避免频繁骚扰。计算公式：

```
delay(n) = min(alert_interval × backoff_factor^(n-1), max_interval)
```

| 参数 | 含义 | 默认值 |
|------|------|--------|
| `alert_interval` | 初始告警间隔 | 5m |
| `backoff_factor` | 退避系数（1.0 = 固定间隔） | 1.5 |
| `max_interval` | 最大间隔上限 | 1h |
| `max_send_count` | 单周期最大发送次数 | 10 |

**示例**（`alert_interval=5m`, `backoff_factor=1.2`）：

| 第几次告警 | 等待时间 | 累计 |
|-----------|---------|------|
| 1 | 立即 | 0 |
| 2 | 5m | 5m |
| 3 | 6m (5×1.2) | 11m |
| 4 | 7.2m (6×1.2) | 18.2m |
| 5 | 8.6m (7.2×1.2) | 26.8m |

**关闭退避**（固定间隔）：

```yaml
alert:
  lag:
    backoff_factor: 1.0    # 等间隔
```

### 通知渠道

```yaml
notify:
  channel: feishu     # feishu | dingtalk
  feishu:
    webhook_url: "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
    sign_secret: ""    # 可选，签名校验
  dingtalk:
    webhook_url: "https://oapi.dingtalk.com/robot/send?access_token=xxx"
    secret: ""           # 可选，加签密钥
```

### 完整配置参考

```yaml
doris:
  host: "127.0.0.1"
  port: 9030
  user: "root"
  password: ""

notify:
  channel: feishu
  feishu:
    webhook_url: ""
    sign_secret: ""
  dingtalk:
    webhook_url: ""
    secret: ""

alert:
  lag:
    threshold: 10000          # lag 告警阈值（per-partition），0 = 不检查 lag
    recovery: 5000            # lag 恢复阈值
    alert_interval: "5m"      # 告警发送间隔（退避初始值）
    backoff_factor: 1.2       # 退避系数（1.0 = 固定间隔）
    max_interval: "1h"        # 最大告警间隔（退避上限）
    max_send_count: 10        # 单周期最大发送次数
  history:
    retention_days: 7         # 归档记录保留天数

scan_databases:
  exclude:                    # 精确排除
    - "information_schema"
    - "mysql"
  exclude_patterns:           # 正则排除
    - "^tmp_.*"
    - ".*_bak$"

database:                     # 数据库级规则（可选）
  - name: "my_db"
    alert:                    # database 级覆盖（对该库所有 job 生效）
      lag:
        threshold: 8000
    jobs:
      - name: "my_job"
        alert:                # job 级覆盖（优先级最高）
          lag:
            threshold: 20000
            recovery: 10000
```

---

## 架构

```
alert_routine_load_v2/
├── cmd/main.go           # 入口，主循环
├── config/               # YAML 配置，三层覆盖，命名空间加载
├── scanner/              # Doris 查询
├── evaluator/            # 告警评估引擎
├── notifier/             # 通知器接口 + 飞书实现
├── store/                # StatusStore(active.json) + ArchiveStore(archive.json)
└── model/                # 领域类型
```

### 状态模型

```
[无状态] ──告警触发──→ [alerting] ──条件恢复──→ [recovering] ──通知发出──→ [recovered] → 归档
                         ↑                                                     │
                         └──────────────── lag 再次超阈值 ──────────────────────┘
```

| 状态 | 含义 | AlertActive |
|------|------|-------------|
| `alerting` | 正在告警中 | true |
| `recovering` | 已恢复，待发恢复通知 | true |
| `recovered` | 已恢复，恢复通知已发 | false |

---

## 测试

```bash
go test ./... -v
```

---

## 变更记录

| 日期         | 版本 | 变更内容 |
|------------|------|----------|
| 2026-02-15 | v2.0.0 | 重构版本：状态枚举、指数退避、正则排除、恢复卡片优化 |
