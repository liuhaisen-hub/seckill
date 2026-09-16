package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/conf"
	"sale/pkg/notification"
	"seckill/internal/common"
)

type SeckillRepo interface {
	GetEvent(ctx context.Context, ID int64) (*model.Event, error)
	GetTickTypeById(ctx context.Context, ticketTypeID int64) (*model.TicketType, error)
	AtomicDeuctStock(ctx context.Context, id int64, quantity int) error
	CreateTicket(ctx context.Context, ticket *model.Ticket) error
	DeuctStockTransaction(ctx context.Context, ticketTypeID int64, quantity int, ticket *model.Ticket) (bool, error)
}

var (
	ErrTicketSoldOut   = fmt.Errorf("已售罄")
	ErrTicketDuplicate = fmt.Errorf("您已购买过该票种")
	ErrEventNotOnSale  = fmt.Errorf("活动未在售票")
	ErrEventNotStarted = fmt.Errorf("活动已结束")
)

type SeckillUseCase struct {
	repo     SeckillRepo
	rdbRepo  RdbRepo
	producer *notification.Producer
	logger   *slog.Logger
}

func NewSeckillUseCase(c *conf.Data, repo SeckillRepo, rdbRepo RdbRepo, logger *slog.Logger) *SeckillUseCase {
	borkers := c.GetBrokers()
	producer, err := notification.NewProducer(borkers, notification.TicketOrderTopic)
	if err != nil {
		logger.Error(err.Error())
	}
	return &SeckillUseCase{
		repo:     repo,
		logger:   logger,
		producer: producer,
		rdbRepo:  rdbRepo,
	}
}

func (u *SeckillUseCase) PurchaseTicket(ctx context.Context, eventID, showID, ticketTypeID, userId, quantity int64) (*common.TicketResult, error) {
	// 获取当前的活动
	event, err := u.repo.GetEvent(ctx, eventID)
	if err != nil {
		u.logger.Error(err.Error())
		return nil, fmt.Errorf("获取活动失败")
	}
	// 校验活动是否在卖
	if event.Status != model.EventStatusOnSale {
		return nil, ErrEventNotStarted
	}
	// 获取票的种类
	ticketType, err := u.repo.GetTickTypeById(ctx, ticketTypeID)
	if err != nil {
		u.logger.Error(err.Error())
		return nil, fmt.Errorf("找不到该票种")
	}
	if ticketType.EventID != event.ID {
		// 当前的活动没有这个票种卖
		return nil, fmt.Errorf("当前活动没有上架该票种")
	}
	if quantity > int64(ticketType.MaxPerUser) {
		// 超出预约
		return nil, fmt.Errorf("超出购买数量 %w", ticketType.MaxPerUser)
	}
	res, err := u.rdbRepo.SeckillDeduct(ctx, fmt.Sprint(eventID), fmt.Sprint(ticketTypeID), fmt.Sprint(userId))
	if err != nil {
		u.logger.Error(err.Error())
		return nil, fmt.Errorf("购买失败，稍后重试")
	}
	switch res {
	case -1:
		return nil, ErrTicketSoldOut
	case -2:
		return nil, ErrTicketDuplicate
	}
	// 没问题，可以买
	message := common.NewTicketMessage(uint(userId), event.ID, uint(ticketTypeID), int(quantity))
	data, err := json.Marshal(message)
	// 推送消息
	if err := u.producer.ProducerSync(ctx, fmt.Appendf(nil, "%d", userId), data); err != nil {
		// 下单失败了,回滚
		u.rdbRepo.SeckillRollback(ctx, fmt.Sprint(eventID), fmt.Sprint(ticketType.ID), fmt.Sprint(userId))
		return nil, fmt.Errorf("购买失败")
	}
	return &common.TicketResult{
		Status:  "queued",
		Message: "排队中",
	}, nil
}
func (u *SeckillUseCase) PurchaseTicketAsync(ctx context.Context, eventID, showID, ticketTypeID, userId, quantity int64) (*common.TicketResult, error) {
	admitKey := fmt.Sprintf("queue:admit:%d:%d", eventID, userId)
	ok, err := u.rdbRepo.Exists(ctx, admitKey)

	if err != nil {
		return nil, err
	}
	if ok == 0 {
		return nil, errors.New("请先进入排队大厅，获得资格后再提交")
	}
	// 一次资格一次提交：用掉即删，防止资格被重放/转卖。
	// 窗口期内提交失败的用户需重新排队（业务上可接受：失败通常意味着库存已尽）。
	u.rdbRepo.Del(ctx, admitKey)
	// 获取当前的活动
	event, err := u.repo.GetEvent(ctx, eventID)
	if err != nil {
		u.logger.Error(err.Error())
		return nil, fmt.Errorf("获取活动失败")
	}
	// 校验活动是否在卖
	if event.Status != model.EventStatusOnSale {
		return nil, ErrEventNotStarted
	}
	// 获取票的种类
	ticketType, err := u.repo.GetTickTypeById(ctx, ticketTypeID)
	if err != nil {
		u.logger.Error(err.Error())
		return nil, fmt.Errorf("找不到该票种")
	}
	if ticketType.EventID != event.ID {
		// 当前的活动没有这个票种卖
		return nil, fmt.Errorf("当前活动没有上架该票种")
	}
	if quantity > int64(ticketType.MaxPerUser) {
		// 超出预约
		return nil, fmt.Errorf("超出购买数量 %w", ticketType.MaxPerUser)
	}
	res, err := u.rdbRepo.SeckillDeduct(ctx, fmt.Sprint(eventID), fmt.Sprint(ticketTypeID), fmt.Sprint(userId))
	if err != nil {
		u.logger.Error(err.Error())
		return nil, fmt.Errorf("购买失败，稍后重试")
	}
	switch res {
	case -1:
		return nil, ErrTicketSoldOut
	case -2:
		return nil, ErrTicketDuplicate
	}
	// 没问题，可以买
	message := common.NewTicketMessage(uint(userId), event.ID, uint(ticketTypeID), int(quantity))
	data, err := json.Marshal(message)
	var asyncErr error
	// 推送消息
	u.producer.ProducerAsync(ctx, fmt.Appendf(nil, "%d", userId), data, func(err error) {
		if err != nil {
			// 下单失败了,回滚
			asyncErr = err
			u.rdbRepo.SeckillRollback(ctx, fmt.Sprint(eventID), fmt.Sprint(ticketType.ID), fmt.Sprint(userId))
		}

	})
	if asyncErr != nil {
		return nil, fmt.Errorf("下单失败")
	}
	token := common.TicketIdempotentKey(message)
	return &common.TicketResult{
		Status:        "queued",
		Message:       "排队中",
		PurchaseToken: token,
		Timestamp:     message.Timestamp,
	}, nil
}

func (u *SeckillUseCase) InitSeckill(ctx context.Context, activityID, productID string, stock int) error {
	return u.rdbRepo.InitSeckillStock(ctx, activityID, productID, stock)
}

func (u *SeckillUseCase) GetPurchaseResult(ctx context.Context, userID, eventID, ticketTypeID uint, timestamp int64) (*common.TicketResult, error) {
	return u.rdbRepo.GetPurchaseResult(ctx, userID, eventID, ticketTypeID, timestamp)
}
