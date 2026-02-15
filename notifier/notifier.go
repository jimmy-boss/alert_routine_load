// @Author: Jimmy
// @DateTime: 2026/02/15

// Package notifier 定义通知接口及飞书实现。
package notifier

import "github.com/jimmy-boss/alert_routine_load/v2/model"

// Notifier 通知接口。
type Notifier interface {
	Send(event model.AlertEvent) error
	SendRecovery(info model.RecoveryInfo) error
}
