package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"
	"ws-getway/biz/contract"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrAlreadyInQueue 业务错误：ZADD NX 返回 0 = 已在队列。
	// 注意不是"拒绝"而是"提示"——HTTP 层捕获后会直接返回当前位次（幂等友好）。
	ErrAlreadyInQueue = errors.New("您已在排队中，请勿重复提交")
	ErrNotInQueue     = errors.New("您不在队列中")
)

// 队列管理
type Manager struct {
	rdb  *redis.Client
	rate int64
}

func NewManager(rdb *redis.Client, rate int) *Manager {
	if rate <= 0 {
		rate = DefaultRate
	}
	return &Manager{
		rdb:  rdb,
		rate: int64(rate),
	}
}

// 加入排队
func (m *Manager) JoinQueue(ctx context.Context, eventID, userID uint64) (*contract.QueuePositionPayload, error) {
	// 队列的key, 用事件作为维度
	key := QueueKey(eventID)
	// 将排队的用户加入队列，带上入队时间
	// set 集合，去重
	member := strconv.FormatUint(userID, 10)
	res, err := m.rdb.ZAddNX(ctx, key, redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: member,
	}).Result()
	if err != nil {
		// 加入队列失败
		hlog.Error(err.Error())
		return nil, err
	}
	if res == 0 {
		// 没执行加入，这个用户已经在队列里了
		return nil, ErrAlreadyInQueue
	}
	// 标记该活动有这个队列， 幂等实现，就算不是第一个人，不停的加也无妨
	if err := m.rdb.SAdd(ctx, QueueActiveKey, eventID).Err(); err != nil {
		hlog.Error(err.Error())
		return nil, err
	}
	// 返回这个用户的排名
	rank, err := m.rdb.ZRank(ctx, key, member).Result()
	if err != nil {
		hlog.Error(err.Error())
		return nil, err
	}
	return position(eventID, rank, m.rate), nil
}

// 查询排名
func (m *Manager) GetPosition(ctx context.Context, eventID, userID uint64) (*contract.QueuePositionPayload, error) {
	key := QueueKey(eventID)
	member := strconv.FormatUint(userID, 10)

	pipe := m.rdb.Pipeline()
	rankCmd := pipe.ZRank(ctx, key, member)
	scoreCmd := pipe.ZScore(ctx, key, member)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	// ZRank 对不存在的 member 返回 redis.Nil → 不在队列
	if errors.Is(rankCmd.Err(), redis.Nil) {
		return nil, ErrNotInQueue
	}
	_ = scoreCmd // score = 入队纳秒时间戳；如需返回 JoinedAt：time.Unix(0, int64(scoreCmd.Val()))
	return position(eventID, rankCmd.Val(), m.rate), nil
}

// 组装排名的方法
func position(eventId uint64, rank int64, rate int64) *contract.QueuePositionPayload {
	return &contract.QueuePositionPayload{
		EventID:       eventId,
		Position:      rank + 1,
		TotalAhead:    rank,
		EstimatedWait: rank / rate,
		Status:        "waiting",
	}
}

// LeaveQueue 退出排队。ZRem 删除不存在的 member 不报错，所以重复退出是幂等的。
func (m *Manager) LeaveQueue(ctx context.Context, eventID, userID uint64) error {
	return m.rdb.ZRem(ctx, QueueKey(eventID), strconv.FormatUint(userID, 10)).Err()
}

// 整个队伍的长度
func (m *Manager) Length(ctx context.Context, eventID uint64) (int64, error) {
	return m.rdb.ZCard(ctx, QueueKey(eventID)).Result()
}

// Snapshot 全量拉取某活动的队列（member 按 score 升序 = FIFO 顺序），broadcaster 用。
func (m *Manager) Snapshot(ctx context.Context, eventID uint64) ([]redis.Z, error) {
	return m.rdb.ZRangeWithScores(ctx, QueueKey(eventID), 0, -1).Result()
}

// ActiveEvents 活跃队列列表。
func (m *Manager) ActiveEvents(ctx context.Context) ([]uint64, error) {
	ss, err := m.rdb.SMembers(ctx, QueueActiveKey).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(ss))
	for _, s := range ss {
		if id, err := strconv.ParseUint(s, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// ProcessBatch 放行队首 n 人（dispatcher 周期调用，仅 leader 实例）：
//
//	① ZPopMin 弹出队首 n 人（score 最小的 n 个 = 最早入队的 n 个）
//	② 逐人写 admit 资格标记（TTL = 购买窗口）—— 没有这一步，放行只是数字游戏，
//	   用户提交秒杀时 seckill 无从知道"轮到他了"
//	③ PUBLISH 放行事件到 queue:events —— 所有 ws-getway 实例收到后给本地在线连接推
//	   queue_admitted 帧
func (m *Manager) ProcessBatch(ctx context.Context, eventID uint64, n int) ([]uint64, error) {
	zs, err := m.rdb.ZPopMin(ctx, QueueKey(eventID), int64(n)).Result()
	if err != nil {
		hlog.Error(err.Error())
		return nil, err
	}
	if len(zs) == 0 {
		// 没有弹出来的用户
		return nil, nil
	}
	users := make([]uint64, len(zs))
	// pipe 批量执行
	pipe := m.rdb.Pipeline()
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			// 解析不出来，跳过
			continue
		}
		uid, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			// 数据不行，还是跳过，不影响其他人
			continue
		}
		users = append(users, uid)
		// 标记，发放通行证
		pipe.Set(ctx, AdmitKey(eventID, uid), 1, AdmitWindow)
	}
	// 一次性执行
	if _, err := pipe.Exec(ctx); err != nil {
		return users, nil
	}
	// 广播放行消息 （低频：每秒最多 rate 条，Pub/Sub 完全扛得住）
	windowSec := int64(AdmitWindow.Seconds())
	for _, uid := range users {
		payload, err := json.Marshal(contract.QueueEvent{
			Type:    "admitted",
			EventID: eventID,
			UserID:  uid,
			Window:  windowSec,
		})
		if err != nil {
			// 有执行错误忽略
			continue
		}
		if err := m.rdb.Publish(ctx, QueueChannel, payload).Err(); err != nil {
			// 广播失败只影响 WS 提醒的及时性：资格标记已写成功，
			// 用户轮询 position 时会发现"已不在队列"从而去提交秒杀。记日志即可。
			_ = err
		}
	}
	return users, nil
}

// Cleanup 清理超时未放行的成员（清理协程周期调用，仅 leader 实例）。
// Sorted Set 是单 key，没法按 member 单独过期 —— 用 score（入队时间戳）做范围删除：
// 入队超过 JoinTTL 还没轮到的，视为用户已放弃（参考项目里 queue:user 标记 30 分钟 TTL 的等价物）。
func (m *Manager) Cleanup(ctx context.Context) error {
	eventIDs, err := m.ActiveEvents(ctx)
	if err != nil {
		return err
	}
	cutoff := strconv.FormatInt(time.Now().Add(-JoinTTL).UnixNano(), 10)
	for _, eventID := range eventIDs {
		if err := m.rdb.ZRemRangeByScore(ctx, QueueKey(eventID), "-inf", cutoff).Err(); err != nil {
			return err
		}
		// 队列清空了就摘掉活跃标记，避免 dispatcher/broadcaster 空扫
		if n, err := m.Length(ctx, eventID); err == nil && n == 0 {
			m.rdb.SRem(ctx, QueueActiveKey, strconv.FormatUint(eventID, 10))
		}
	}
	return nil
}

// Clear 活动结束清空整个队列（管理员操作 / 运维脚本调用）。
// Sorted Set 单 key 方案下 Del 一次就干净，不存在参考项目 List 版
// "只删了队列本体、queue:user 标记残留 30 分钟" 的问题。
func (m *Manager) Clear(ctx context.Context, eventID uint64) error {
	if err := m.rdb.Del(ctx, QueueKey(eventID)).Err(); err != nil {
		return err
	}
	return m.rdb.SRem(ctx, QueueActiveKey, strconv.FormatUint(eventID, 10)).Err()
}
