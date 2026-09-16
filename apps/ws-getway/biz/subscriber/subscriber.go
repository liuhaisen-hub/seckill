package subscriber

import (
	"context"
	"encoding/json"
	"ws-getway/biz/contract"
	"ws-getway/biz/hub"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/redis/go-redis/v9"
)

// 从redis 上拉订阅

type Subscriber struct {
	rdb *redis.Client
	hub *hub.Hub
}

func NewSubscriber(rdb *redis.Client, h *hub.Hub) *Subscriber {
	return &Subscriber{
		rdb: rdb,
		hub: h,
	}
}

// 运行函数
func (s *Subscriber) Run(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, contract.SeckillResultChannel)
	defer sub.Close()
	hlog.Info("[PubSub] subscribed", "channel", contract.SeckillResultChannel)
	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				hlog.Error("[PubSub] channel closed, subscriber exiting")
				return
			}
			switch msg.Channel {
			case contract.SeckillResultChannel:
				s.dispatchSeckillResult(msg.Payload)
			case contract.QueueChannel:
				s.dispatchQueueEvent(msg.Payload)
			}
		}
	}
}

// dispatch 单条解析失败直接丢弃，绝不让坏消息中断分发循环。
func (s *Subscriber) dispatchSeckillResult(payload string) {
	var result contract.SeckillResult
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		hlog.Error("[PubSub] bad payload", "error", err)
		return
	}
	s.hub.PushSeckillResult(&result)
}

// dispatchQueueEvent 放行事件 → 给该用户所有本地在线连接推 queue_admitted 帧。
func (s *Subscriber) dispatchQueueEvent(payload string) {
	var ev contract.QueueEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		hlog.Error("[PubSub] bad queue event", "error", err)
		return
	}
	if ev.Type == "admitted" {
		s.hub.PushFrame(ev.UserID, contract.FrameTypeQueueAdmitted, ev)
	}
}
