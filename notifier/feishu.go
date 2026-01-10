// Package notifier sends alerts to Feishu (Lark) webhook.
package notifier

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jimmy-boss/alert_routine_load/alerter"
	"github.com/jimmy-boss/alert_routine_load/config"
	"github.com/jimmy-boss/alert_routine_load/model"
	glog "github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// Option is a functional option for Notifier.
type Option func(*Notifier)

// WithLogger injects a logger implementation.
func WithLogger(logger glog.HLoggerBase) Option {
	return func(n *Notifier) {
		n.logger = logger
	}
}

// Notifier sends alert events to Feishu webhook.
type Notifier struct {
	cfg    *config.FeishuConfig
	logger glog.HLoggerBase
	client *http.Client
}

func New(cfg *config.FeishuConfig, opts ...Option) *Notifier {
	n := &Notifier{
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

// Send dispatches an alert event to the Feishu webhook as an interactive card.
func (n *Notifier) Send(decision alerter.AlertDecision) error {
	if decision.Action != "send" {
		return nil
	}
	e := decision.Event

	// Build interactive card message.
	card := buildCard(e)

	payload := map[string]interface{}{
		"msg_type": "interactive",
		"card":     card,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	// Sign if secret is configured.
	webhookURL := n.cfg.WebhookURL
	if n.cfg.SignSecret != "" {
		ts := time.Now().Unix()
		sign, err := genSign(n.cfg.SignSecret, ts)
		if err != nil {
			return fmt.Errorf("sign: %w", err)
		}
		// Append timestamp and sign to URL.
		sep := "?"
		if strings.Contains(webhookURL, "?") {
			sep = "&"
		}
		webhookURL = fmt.Sprintf("%s%stimestamp=%d&sign=%s", webhookURL, sep, ts, sign)
	}

	resp, err := n.client.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("feishu returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Check Feishu response code.
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(respBody, &result); err == nil && result.Code != 0 {
		return fmt.Errorf("feishu error %d: %s", result.Code, result.Msg)
	}

	n.logger.Info("alert sent",
		zap.Int64("job_id", e.JobID),
		zap.String("job_name", e.JobName),
		zap.String("database", e.Database),
	)
	return nil
}

// SendRecovery sends a recovery notification for a job that is no longer paused.
func (n *Notifier) SendRecovery(jobKey string, database string, jobName string, duration time.Duration, sendCount int) error {
	card := buildRecoveryCard(database, jobName, duration, sendCount)

	payload := map[string]interface{}{
		"msg_type": "interactive",
		"card":     card,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	webhookURL := n.cfg.WebhookURL
	if n.cfg.SignSecret != "" {
		ts := time.Now().Unix()
		sign, err := genSign(n.cfg.SignSecret, ts)
		if err != nil {
			return fmt.Errorf("sign: %w", err)
		}
		sep := "?"
		if strings.Contains(webhookURL, "?") {
			sep = "&"
		}
		webhookURL = fmt.Sprintf("%s%stimestamp=%d&sign=%s", webhookURL, sep, ts, sign)
	}

	resp, err := n.client.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("feishu returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(respBody, &result); err == nil && result.Code != 0 {
		return fmt.Errorf("feishu error %d: %s", result.Code, result.Msg)
	}

	n.logger.Info("recovery notification sent",
		zap.String("job_key", jobKey),
		zap.String("database", database),
		zap.Duration("duration", duration.Round(time.Second)),
		zap.Int("send_count", sendCount),
	)
	return nil
}

func buildCard(e model.AlertEvent) map[string]interface{} {
	// Status color.
	color := "red"
	switch strings.ToUpper(e.State) {
	case "PAUSED":
		color = "orange"
	case "LAG":
		color = "yellow"
	}

	// Header.
	header := map[string]interface{}{
		"template": color,
		"title": map[string]interface{}{
			"tag":     "plain_text",
			"content": fmt.Sprintf("🚨 Doris Routine Load 告警 [%s]", e.Database),
		},
	}

	// Fields.
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

	// Reason element.
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

	// Error detail (if available).
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

// genSign generates Feishu webhook signature.
// Feishu signature: HMAC-SHA256(key=empty, message=timestamp+"\n"+secret)
func genSign(secret string, timestamp int64) (string, error) {
	strToSign := fmt.Sprintf("%d\n%s", timestamp, secret)
	h := hmac.New(sha256.New, []byte(""))
	_, err := h.Write([]byte(strToSign))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func buildRecoveryCard(database, jobName string, duration time.Duration, sendCount int) map[string]interface{} {
	header := map[string]interface{}{
		"template": "green",
		"title": map[string]interface{}{
			"tag":     "plain_text",
			"content": fmt.Sprintf("✅ Doris Routine Load 恢复 [%s]", database),
		},
	}

	fields := []map[string]interface{}{
		field("Database", database),
		field("Job Name", valueOrDash(jobName)),
		field("恢复时间", time.Now().Format("2006-01-02 15:04:05")),
	}
	if duration > 0 {
		fields = append(fields, field("告警持续时间", formatDuration(duration)))
	}
	if sendCount > 0 {
		fields = append(fields, field("累计告警次数", fmt.Sprintf("%d 次", sendCount)))
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

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%d小时%d分钟", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%d分%d秒", m, s)
	}
	return fmt.Sprintf("%d秒", s)
}
