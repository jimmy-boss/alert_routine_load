// @Author: Jimmy
// @DateTime: 2026/02/15

package notifier

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/config"
	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// FeishuOption 飞书通知器的函数选项。
type FeishuOption func(*FeishuNotifier)

// WithFeishuLogger 注入 logger。
func WithFeishuLogger(logger glog.HLoggerBase) FeishuOption {
	return func(n *FeishuNotifier) {
		n.logger = logger
	}
}

// 编译期接口检查。
var _ Notifier = (*FeishuNotifier)(nil)

// FeishuNotifier 通过飞书 webhook 发送告警。
type FeishuNotifier struct {
	cfg    *config.FeishuConfig
	logger glog.HLoggerBase
	client *http.Client
}

// NewFeishu 创建飞书通知器。
func NewFeishu(cfg *config.FeishuConfig, opts ...FeishuOption) *FeishuNotifier {
	n := &FeishuNotifier{
		cfg:    cfg,
		client: &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(n)
	}
	if n.logger == nil {
		n.logger = glog.GlobalLoggers["default"]
	}
	return n
}

// Send 发送告警事件到飞书。
func (n *FeishuNotifier) Send(event model.AlertEvent) error {
	card := buildCard(event)

	payload := map[string]interface{}{
		"msg_type": "interactive",
		"card":     card,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	webhookURL, err := n.signURL()
	if err != nil {
		return err
	}

	resp, err := n.post(webhookURL, body)
	if err != nil {
		return err
	}

	n.logger.Info("alert sent",
		zap.Int64("job_id", event.JobID),
		zap.String("job_name", event.JobName),
		zap.String("database", event.Database),
		zap.String("response", resp),
	)
	return nil
}

// SendRecovery 发送恢复通知到飞书。
func (n *FeishuNotifier) SendRecovery(info model.RecoveryInfo) error {
	card := buildRecoveryCard(info)

	payload := map[string]interface{}{
		"msg_type": "interactive",
		"card":     card,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	webhookURL, err := n.signURL()
	if err != nil {
		return err
	}

	resp, err := n.post(webhookURL, body)
	if err != nil {
		return err
	}

	n.logger.Info("recovery notification sent",
		zap.String("job_key", info.JobKey),
		zap.String("database", info.Database),
		zap.Duration("duration", info.Duration.Round(time.Second)),
		zap.String("response", resp),
	)
	return nil
}

// signURL 生成带签名的 webhook URL。
func (n *FeishuNotifier) signURL() (string, error) {
	webhookURL := n.cfg.WebhookURL
	if n.cfg.SignSecret == "" {
		return webhookURL, nil
	}
	ts := time.Now().Unix()
	sign, err := genSign(n.cfg.SignSecret, ts)
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	sep := "?"
	if strings.Contains(webhookURL, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%stimestamp=%d&sign=%s", webhookURL, sep, ts, sign), nil
}

// post 发送 webhook 请求并校验响应。
func (n *FeishuNotifier) post(url string, body []byte) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("feishu returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(respBody, &result); err == nil && result.Code != 0 {
		return "", fmt.Errorf("feishu error %d: %s", result.Code, result.Msg)
	}

	return string(respBody), nil
}

// buildCard 构建告警卡片。
func buildCard(e model.AlertEvent) map[string]interface{} {
	color := "red"
	switch strings.ToUpper(e.State) {
	case "PAUSED":
		color = "orange"
	case "LAG":
		color = "yellow"
	}

	header := map[string]interface{}{
		"template": color,
		"title": map[string]interface{}{
			"tag":     "plain_text",
			"content": fmt.Sprintf("🚨 Doris Routine Load 告警 [%s]", e.Database),
		},
	}

	fields := []map[string]interface{}{
		field("Job ID", fmt.Sprintf("%d", e.JobID)),
		field("Job Name", e.JobName),
		field("Database", e.Database),
		field("状态", fmt.Sprintf("⚠️ %s", e.State)),
		field("暂停时间", valueOrDash(e.PauseTime)),
		field("告警时间", e.Timestamp.Format("2006-01-02 15:04:05")),
	}
	if e.Duration > 0 {
		fields = append(fields, field("告警持续时间", formatDuration(e.Duration)))
	}
	if e.TotalSendCount > 0 {
		fields = append(fields, field("累计告警次数", fmt.Sprintf("%d 次", e.TotalSendCount)))
	}

	elements := []map[string]interface{}{
		{
			"tag":    "div",
			"fields": fields,
		},
		{
			"tag": "hr",
		},
		{
			"tag": "div",
			"text": map[string]interface{}{
				"tag":     "lark_md",
				"content": fmt.Sprintf("**变更原因**\n%s", valueOrDash(e.Reason)),
			},
		},
	}

	if e.ErrorDetail != "" {
		elements = append(elements, map[string]interface{}{
			"tag": "div",
			"text": map[string]interface{}{
				"tag":     "lark_md",
				"content": fmt.Sprintf("**错误详情**\n```\n%s\n```", e.ErrorDetail),
			},
		})
	}

	elements = append(elements, map[string]interface{}{
		"tag": "note",
		"elements": []map[string]interface{}{
			{
				"tag":     "plain_text",
				"content": "Doris Routine Load Alert System",
			},
		},
	})

	return map[string]interface{}{
		"header":   header,
		"elements": elements,
	}
}

// buildRecoveryCard 构建恢复卡片。
func buildRecoveryCard(info model.RecoveryInfo) map[string]interface{} {
	header := map[string]interface{}{
		"template": "green",
		"title": map[string]interface{}{
			"tag":     "plain_text",
			"content": fmt.Sprintf("✅ Doris Routine Load 恢复 [%s]", info.Database),
		},
	}

	recoveredAt := "-"
	if !info.RecoveredAt.IsZero() {
		recoveredAt = info.RecoveredAt.Format("2006-01-02 15:04:05")
	}

	fields := []map[string]interface{}{
		field("Database", info.Database),
		field("Job Name", valueOrDash(info.JobName)),
		field("恢复时间", recoveredAt),
	}
	if info.Duration > 0 {
		fields = append(fields, field("告警持续时间", formatDuration(info.Duration)))
	}
	if info.SendCount > 0 {
		fields = append(fields, field("累计告警次数", fmt.Sprintf("%d 次", info.SendCount)))
	}

	elements := []map[string]interface{}{
		{"tag": "div", "fields": fields},
		{
			"tag": "note",
			"elements": []map[string]interface{}{
				{"tag": "plain_text", "content": "Doris Routine Load Alert System"},
			},
		},
	}

	return map[string]interface{}{
		"header":   header,
		"elements": elements,
	}
}

func field(label, value string) map[string]interface{} {
	return map[string]interface{}{
		"is_short": true,
		"text": map[string]interface{}{
			"tag":     "lark_md",
			"content": fmt.Sprintf("**%s**\n%s", label, value),
		},
	}
}

func valueOrDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

// genSign 生成飞书 webhook 签名。
// 签名算法: HMAC-SHA256(key=strToSign, msg="")，其中 strToSign = timestamp + "\n" + secret
func genSign(secret string, timestamp int64) (string, error) {
	strToSign := fmt.Sprintf("%d\n%s", timestamp, secret)
	h := hmac.New(sha256.New, []byte(strToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	days := int(d.Hours()) / 24
	h := int(d.Hours()) % 24
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if days > 0 {
		return fmt.Sprintf("%d天%d小时", days, h)
	}
	if h > 0 {
		return fmt.Sprintf("%d小时%d分钟", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%d分%d秒", m, s)
	}
	return fmt.Sprintf("%d秒", s)
}
