// @Author: Jimmy
// @DateTime: 2026/02/16

// Package notifier — 钉钉 Webhook 通知器实现。
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
	"net/url"
	"time"

	"github.com/jimmy-boss/alert_routine_load/v2/config"
	"github.com/jimmy-boss/alert_routine_load/v2/model"
	"github.com/jimmy-boss/go-log/glog"
	"go.uber.org/zap"
)

// DingtalkNotifier 钉钉 Webhook 通知器。
type DingtalkNotifier struct {
	cfg    *config.DingtalkConfig
	logger glog.HLoggerBase
	client *http.Client
}

// DingtalkOption 是 DingtalkNotifier 的函数选项。
type DingtalkOption func(*DingtalkNotifier)

// WithDingtalkLogger 注入 logger 实现。
func WithDingtalkLogger(logger glog.HLoggerBase) DingtalkOption {
	return func(n *DingtalkNotifier) {
		n.logger = logger
	}
}

// NewDingtalk 创建 DingtalkNotifier。
func NewDingtalk(cfg *config.DingtalkConfig, opts ...DingtalkOption) *DingtalkNotifier {
	n := &DingtalkNotifier{
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

// Send 发送告警通知到钉钉。
func (n *DingtalkNotifier) Send(event model.AlertEvent) error {
	title := fmt.Sprintf("🚨 Doris Routine Load 告警 [%s]", event.Database)
	text := fmt.Sprintf(
		"### %s\n\n- **Job ID**: %d\n- **Job Name**: %s\n- **Database**: %s\n- **状态**: ⚠️ %s\n- **暂停时间**: %s\n- **告警时间**: %s\n",
		title, event.JobID, event.JobName, event.Database,
		event.State, valueOrDash(event.PauseTime),
		event.Timestamp.Format("2006-01-02 15:04:05"),
	)
	if event.Duration > 0 {
		text += fmt.Sprintf("- **告警持续时间**: %s\n", formatDuration(event.Duration))
	}
	if event.TotalSendCount > 0 {
		text += fmt.Sprintf("- **累计告警次数**: %d 次\n", event.TotalSendCount)
	}
	text += fmt.Sprintf("\n---\n**变更原因**\n%s\n", valueOrDash(event.Reason))
	if event.ErrorDetail != "" {
		text += fmt.Sprintf("\n**错误详情**\n```\n%s\n```\n", event.ErrorDetail)
	}

	return n.sendMarkdown(title, text)
}

// SendRecovery 发送恢复通知到钉钉。
func (n *DingtalkNotifier) SendRecovery(info model.RecoveryInfo) error {
	title := fmt.Sprintf("✅ Doris Routine Load 恢复 [%s]", info.Database)

	recoveredAt := "-"
	if !info.RecoveredAt.IsZero() {
		recoveredAt = info.RecoveredAt.Format("2006-01-02 15:04:05")
	}

	text := fmt.Sprintf(
		"### %s\n\n- **Database**: %s\n- **Job Name**: %s\n- **恢复时间**: %s\n",
		title, info.Database, valueOrDash(info.JobName), recoveredAt,
	)
	if info.Duration > 0 {
		text += fmt.Sprintf("- **告警持续时间**: %s\n", formatDuration(info.Duration))
	}
	if info.SendCount > 0 {
		text += fmt.Sprintf("- **累计告警次数**: %d 次\n", info.SendCount)
	}

	return n.sendMarkdown(title, text)
}

// sendMarkdown 发送 Markdown 类型消息。
func (n *DingtalkNotifier) sendMarkdown(title, text string) error {
	webhookURL := n.cfg.WebhookURL
	if n.cfg.Secret != "" {
		ts := time.Now().UnixMilli()
		sign, err := dingtalkSign(n.cfg.Secret, ts)
		if err != nil {
			return fmt.Errorf("sign: %w", err)
		}
		webhookURL = fmt.Sprintf("%s&timestamp=%d&sign=%s", webhookURL, ts, sign)
	}

	payload := map[string]interface{}{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": title,
			"text":  text,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	resp, err := n.client.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dingtalk returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(respBody, &result); err == nil && result.ErrCode != 0 {
		return fmt.Errorf("dingtalk error %d: %s", result.ErrCode, result.ErrMsg)
	}

	n.logger.Info("notification sent",
		zap.String("channel", "dingtalk"),
		zap.String("title", title),
	)
	return nil
}

// dingtalkSign 生成钉钉加签。
// 签名算法: HMAC-SHA256(key=secret, message=timestamp)
func dingtalkSign(secret string, timestamp int64) (string, error) {
	strToSign := fmt.Sprintf("%d\n%s", timestamp, secret)
	mac := hmac.New(sha256.New, []byte(secret))
	_, err := mac.Write([]byte(strToSign))
	if err != nil {
		return "", err
	}
	return url.QueryEscape(base64.StdEncoding.EncodeToString(mac.Sum(nil))), nil
}
