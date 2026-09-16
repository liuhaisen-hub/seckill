package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/cache"
	"sale/pkg/utils"
	"seckill/internal/common"

	"github.com/redis/go-redis/v9"
)

type ConsumerUseCase struct {
	repo       SeckillRepo
	rdbRepo    RdbRepo
	localCache *cache.LocalCache
	logger     *slog.Logger
}

func NewConsumerUseCase(repo SeckillRepo, rdbRepo RdbRepo, logger *slog.Logger) *ConsumerUseCase {
	return &ConsumerUseCase{
		repo:    repo,
		rdbRepo: rdbRepo,
		logger:  logger,
	}
}

func (u *ConsumerUseCase) HandleMessage(ctx context.Context, key, value []byte) error {
	var ticketMsg common.TicketMessage
	if err := json.Unmarshal(value, &ticketMsg); err != nil {
		u.logger.Error("[Kafka] Failed to unmarshal ticket message", "error", err)
		// 解析失败不走重试
		return nil
	}

	// 收到消息
	u.logger.Info("[Kafka] Processing ticket", "user_id", ticketMsg.UserID, "event_id", ticketMsg.EventID, "ticket_type_id", ticketMsg.TicketTypeID)
	orderNo := utils.OrderNo()
	idemporentKey := common.TicketIdempotentKey(&ticketMsg)
	set, err := u.rdbRepo.SetNX(ctx, idemporentKey, orderNo, common.TicketOrderTTL)
	if err != nil {
		u.logger.Error("[Kafka] Idempotent check failed", "error", err)
		return err // 重试
	}
	if !set {
		// 没有设置成功， 就是idemporentKey 已经存在了，检查是不是消息重试导致重复等其他问题
		order, err := u.rdbRepo.Get(ctx, idemporentKey)
		if err != redis.Nil {
			u.logger.Info("[Kafka] Duplicate message detected", "user_id", ticketMsg.UserID, "event_id", ticketMsg.EventID, "existing_order_no", order)
			// 消息重复了，忽略
			u.notify(ctx, common.NewTicketResult(&ticketMsg, "success", "抢购成功", order))
			return nil
		}
	}
	ticketType, err := u.repo.GetTickTypeById(ctx, int64(ticketMsg.TicketTypeID))
	if err != nil || ticketType == nil {
		// 找不到
		u.logger.Error("[Kafka] Ticket type not found", "ticket_type_id", ticketMsg.TicketTypeID)
		u.notify(ctx, common.NewTicketResult(&ticketMsg, "failed", "抢购失败，请重试", ""))
		return nil // 数据问题也不处理
	}
	ticket := &model.Ticket{
		UserID:       ticketMsg.UserID,
		EventID:      ticketMsg.EventID,
		ShowID:       ticketMsg.ShowID,
		TicketTypeID: ticketMsg.TicketTypeID,
		Quantity:     ticketMsg.Quantity,
		TotalPrice:   float64(ticketMsg.Quantity) * ticketType.Price,
		Status:       "reserved",
		OrderNo:      orderNo,
	}
	// 通过事务扣减和新增ticket
	bussinessError, err := u.repo.DeuctStockTransaction(ctx, int64(ticketMsg.TicketTypeID), ticketMsg.Quantity, ticket)
	if err != nil {
		// 回滚，补偿redis的预扣
		activityID := fmt.Sprintf("ticket:%d", ticketMsg.EventID)
		u.rdbRepo.SeckillRollback(ctx, activityID, fmt.Sprint(ticketMsg.TicketTypeID), fmt.Sprint(ticketMsg.UserID))
		// 删除key
		u.rdbRepo.Del(ctx, idemporentKey)
		if bussinessError {
			// 业务错误可以重试
			return nil
		}
		// 业务失败（库存不足等）：终态，推送失败结果，用户可以重试
		u.notify(ctx, common.NewTicketResult(&ticketMsg, "failed", "抢购失败，请重试", ""))
		return err
	}
	u.logger.Info("[Kafka] Ticket created successfully", "ticket_id", ticket.ID, "user_id", ticketMsg.UserID, "order_no", ticket.OrderNo)
	// 成功终态：推送结果
	result := common.NewTicketResult(&ticketMsg, "success", "抢购成功", ticket.OrderNo)
	result.TicketID = ticket.ID
	u.notify(ctx, result)
	return nil
}

// notify 结果出口的唯一调用封装：出口失败只记日志。
// 结果推送是 best-effort，绝不能因为出口故障让消息进入重试循环。
func (u *ConsumerUseCase) notify(ctx context.Context, result *common.TicketResult) {
	if err := u.rdbRepo.WireResult(ctx, result); err != nil {
		u.logger.Error("[Kafka] notify result failed", "user_id", result.UserID, "error", err)
	}
}
