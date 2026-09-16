package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/redis/go-redis/v9"
)

type Dispatcher struct {
	mgr        *Manager
	rdb        *redis.Client
	instanceID string // 随机 ID，作为锁 value：只有"锁里是自己"才允许续期
	isLeader   atomic.Bool
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
func NewDispatcher(mgr *Manager, rdb *redis.Client) *Dispatcher {
	return &Dispatcher{mgr: mgr, rdb: rdb, instanceID: randomID()}
}

func (d *Dispatcher) Run(ctx context.Context) {

}

func (d *Dispatcher) dispatchOnce(ctx context.Context, perTick int) {
	events, err := d.mgr.ActiveEvents(ctx)
	if err != nil {
		hlog.Error("[Queue] list active events failed", "error", err)
		return
	}
	for _, eventID := range events {
		users, err := d.mgr.ProcessBatch(ctx, eventID, perTick)
		if err != nil {
			hlog.Error("[Queue] dispatch failed", "event_id", eventID, "error", err)
			continue
		}
		if len(users) > 0 {
			hlog.Info("[Queue] admitted", "event_id", eventID, "count", len(users))
		}
	}
}

var renewScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

// campaign 选主/续期循环：每 5s 一次。抢锁（SetNX）成功 → 成为 leader；
// 抢不到但锁里是自己（续期窗口）→ 续期保持 leader；否则让位。
func (d *Dispatcher) campaign(ctx context.Context) {
	ticker := time.NewTicker(LeaderRenewInterval)
	defer ticker.Stop()
	d.tryCampaign(ctx) // 启动立即试一次，不等第一个 tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.tryCampaign(ctx)
		}
	}
}
func (d *Dispatcher) tryCampaign(ctx context.Context) {
	ok, err := d.rdb.SetNX(ctx, QueueLeaderKey, d.instanceID, LeaderTTL).Result()
	if err != nil {
		d.setLeader(false, "setnx error")
		return
	}
	if ok {
		d.setLeader(true, "acquired")
		return
	}
	// 没抢到：若锁本来是自己持有的（周期续期），续上；否则锁在别人手里
	n, err := renewScript.Run(ctx, d.rdb, []string{QueueLeaderKey},
		d.instanceID, LeaderTTL.Milliseconds()).Int()
	if err != nil || n == 0 {
		d.setLeader(false, "held by other")
		return
	}
	d.setLeader(true, "renewed")
}

func (d *Dispatcher) setLeader(v bool, why string) {
	if d.isLeader.Load() && !v {
		hlog.Info("[Queue] lost leadership", "reason", why)
	}
	if !d.isLeader.Load() && v {
		hlog.Info("[Queue] became leader", "reason", why, "instance", d.instanceID)
	}
	d.isLeader.Store(v)
}
