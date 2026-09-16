package queue

import (
	"context"
	"strconv"
	"time"
	"ws-getway/biz/contract"
	"ws-getway/biz/hub"
)

// Broadcaster 位次推送器：周期把队列里每个人的最新位次推给【本地在线】的用户。
//
// 为什么每实例自主拉取而不走 Pub/Sub（对比 §2 的放行事件）：
//
//	位次是连续状态、事实源在 Redis，谁都能算 —— 一次 ZRANGE 在内存里算出所有人的位次，
//	比给每个用户逐条 PUBLISH（20 万人 × 每 2s = 10 万 msg/s）低 5 个数量级。
//	推送前 hub.HasUser 本地过滤，不在线的不推（HTTP 轮询兜底）。
type Broadcaster struct {
	mgr      *Manager
	hub      *hub.Hub
	interval time.Duration
}

func NewBroadcaster(mgr *Manager, h *hub.Hub) *Broadcaster {
	return &Broadcaster{mgr: mgr, hub: h, interval: 2 * time.Second}
}
func (b *Broadcaster) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.broadcastOnce(ctx)
		}
	}
}
func (b *Broadcaster) broadcastOnce(ctx context.Context) {
	events, err := b.mgr.ActiveEvents(ctx)
	if err != nil {
		return
	}
	rate := int64(DefaultRate)
	for _, eventID := range events {
		zs, err := b.mgr.Snapshot(ctx, eventID)
		if err != nil || len(zs) == 0 {
			continue
		}
		for i, z := range zs {
			member, ok := z.Member.(string)
			if !ok {
				continue
			}
			uid, err := strconv.ParseUint(member, 10, 64)
			if err != nil {
				continue
			}
			if !b.hub.HasUser(uid) {
				continue // 不在线不推，省掉序列化和投递开销
			}
			b.hub.PushFrame(uid, contract.FrameTypeQueuePosition, contract.QueuePositionPayload{
				EventID:       eventID,
				Position:      int64(i + 1),
				TotalAhead:    int64(i),
				EstimatedWait: int64(i) / rate,
				Status:        "waiting",
			})
		}
	}
}
