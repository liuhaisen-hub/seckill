package queue

import (
	"fmt"
	"time"
)

// 定义相关的常量

const (
	// QueueKeyFmt 排队本体：Sorted Set，member=userID，score=入队纳秒时间戳。
	QueueKeyFmt = "queue:z:event:%d"
	// QueueActiveKey 当前有活跃队列的 eventID 集合，dispatcher/broadcaster/清理协程按它扫描。
	QueueActiveKey = "queue:active"
	// QueueAdmitFmt 放行资格标记：放行时写入，seckill 提交秒杀前校验，TTL=购买窗口。
	QueueAdmitFmt = "queue:admit:%d:%d"
	// QueueLeaderKey 放行调度器选主锁，value=实例随机 ID。
	QueueLeaderKey = "queue:leader"
	// QueueChannel 放行事件 Pub/Sub 频道：leader 放行后广播，各 ws-getway 实例推 WS 帧。
	QueueChannel = "queue:events"

	// DefaultRate 默认放行速率（人/秒）：系统的主阀门，压测后调整，或做成配置项。
	DefaultRate = 50
	// JoinTTL 入队有效期：入队后这么久还没被放行，视为放弃，由清理协程按 score 移除。
	JoinTTL = 30 * time.Minute
	// AdmitWindow 购买窗口：获得资格后这么久内必须提交秒杀，过期资格作废。
	AdmitWindow = 5 * time.Minute
	// LeaderTTL / LeaderRenewInterval 选主锁 TTL 与续期周期。
	LeaderTTL           = 10 * time.Second
	LeaderRenewInterval = 5 * time.Second
)

func QueueKey(eventID uint64) string {
	return fmt.Sprintf(QueueKeyFmt, eventID)
}

func AdmitKey(eventID, userId uint64) string {
	return fmt.Sprint(QueueAdmitFmt, eventID, userId)
}
