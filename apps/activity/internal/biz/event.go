package biz

import (
	"context"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"github.com/go-kratos/kratos/v3/errors"
)

// Event 是活动领域对象（DO），不含 proto 与存储标签。

// EventRepo 是活动仓储接口，由 data 层实现，是依赖倒置的接缝。
type EventRepo interface {
	CreateEvent(ctx context.Context, event *model.Event) (*model.Event, error)
	UpdateEvent(ctx context.Context, event *model.Event) error
	DeleteEvent(ctx context.Context, id int64) error
	GetEvent(ctx context.Context, id int64) (*model.Event, error)
	ListEvent(ctx context.Context, opts ...utils.ListOption) ([]*model.Event, int64, error)
	UpdateStatus(ctx context.Context, ID int64, status string) error
}

var (
	ErrEventNotFound = errors.NotFound("EVENT_NOT_FOUND", "活动不存在")
)

// EventUseCase 是活动业务用例。
type EventUseCase struct {
	repo          EventRepo
	tiketTypeRepo TicketTypeRepo
	logger        *slog.Logger
}

func NewEventUseCase(repo EventRepo, ticketTypeRepo TicketTypeRepo, logger *slog.Logger) *EventUseCase {
	return &EventUseCase{
		repo:          repo,
		tiketTypeRepo: ticketTypeRepo,
		logger:        logger,
	}
}

func (uc *EventUseCase) CreateEvent(ctx context.Context, event *model.Event, startTime string, endTime string) (*model.Event, error) {
	if event.Title == "" {
		return nil, errors.BadRequest("EVENT_BAD_REQUEST", "活动标题不能为空")
	}
	if event.Location == "" {
		return nil, errors.BadRequest("EVENT_BAD_REQUEST", "活动地点不能为空")
	}
	if startTime == "" || endTime == "" {
		return nil, fmt.Errorf("必须填写开始时间和结束时间")
	}

	StartTime, err := utils.ParserTimeString(startTime)
	if err != nil {
		return nil, fmt.Errorf("开始时间填写错误")
	}
	EndTime, err := utils.ParserTimeString(endTime)
	if err != nil {
		return nil, fmt.Errorf("结束时间填写有误")
	}
	if EndTime.Before(StartTime) {
		return nil, fmt.Errorf("结束时间大于开始时间")
	}
	event.StartTime = StartTime
	event.EndTime = EndTime
	// status 默认只能填未发布
	event.Status = model.EventStatusDraft
	return uc.repo.CreateEvent(ctx, event)
}

func (uc *EventUseCase) UpdateEvent(ctx context.Context, event *model.Event, startTime, endTime string) error {
	if event.ID == 0 {
		return errors.BadRequest("EVENT_BAD_REQUEST", "活动 ID 不能为空")
	}
	if startTime != "" {
		StartTime, err := utils.ParserTimeString(startTime)
		if err != nil {
			return fmt.Errorf("开始时间填写错误")
		}
		event.StartTime = StartTime
	}
	if endTime != "" {
		EndTime, err := utils.ParserTimeString(endTime)
		if err != nil {
			return fmt.Errorf("结束时间填写有误")
		}
		event.EndTime = EndTime
	}
	if event.EndTime.Before(event.StartTime) {
		return fmt.Errorf("结束时间大于开始时间")
	}
	return uc.repo.UpdateEvent(ctx, event)
}

func (uc *EventUseCase) DeleteEvent(ctx context.Context, id int64) error {
	if id == 0 {
		return errors.BadRequest("EVENT_BAD_REQUEST", "活动 ID 不能为空")
	}
	return uc.repo.DeleteEvent(ctx, id)
}

func (uc *EventUseCase) GetEvent(ctx context.Context, id int64) (*model.Event, error) {
	if id == 0 {
		return nil, errors.BadRequest("EVENT_BAD_REQUEST", "活动 ID 不能为空")
	}
	return uc.repo.GetEvent(ctx, id)
}

func (uc *EventUseCase) ListEvent(ctx context.Context, page, size int64, opts ...utils.ListOption) ([]*model.Event, int64, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	opts = append(opts, utils.ListOffset(int((page-1)*size)), utils.ListLimit(int(size)))
	return uc.repo.ListEvent(ctx, opts...)
}

func (uc *EventUseCase) UpdateEventStatus(ctx context.Context, ID int64, status string) error {
	evnet, err := uc.repo.GetEvent(ctx, ID)
	if err != nil {
		return err
	}
	if !model.IsValidEventStatus(status) {
		return fmt.Errorf("状态类型错误")
	}
	if status == model.EventStatusOnSale {
		return uc.publishEvent(ctx, ID, evnet)
	}
	if status == model.EventStatusOffSale {
		return uc.unpublishEvent(ctx, ID, evnet)
	}
	if status == model.EventStatusEnded {
		if err := model.IsValidEventTransition(evnet.Status, model.EventStatusEnded); err != nil {
			return err
		}
		return uc.repo.UpdateStatus(ctx, ID, model.EventStatusEnded)
	}
	return fmt.Errorf("找不到对应状态")
}

func (uc *EventUseCase) publishEvent(ctx context.Context, ID int64, event *model.Event) error {
	// 状态机校验
	if err := model.IsValidEventTransition(event.Status, model.EventStatusOnSale); err != nil {
		return err
	}
	// 获取所有的tikceType
	list, err := uc.tiketTypeRepo.FindByEvent(ctx, ID)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return fmt.Errorf("至少有一个类型票据")
	}
	totalStock := 0
	for _, tt := range list {
		totalStock += tt.Stock
	}
	event.TotalStock = totalStock
	if err := uc.repo.UpdateEvent(ctx, event); err != nil {
		return err
	}
	if err := uc.repo.UpdateStatus(ctx, ID, model.EventStatusOnSale); err != nil {
		return err
	}
	return nil
}

func (uc *EventUseCase) unpublishEvent(ctx context.Context, ID int64, event *model.Event) error {
	if err := model.IsValidEventTransition(event.Status, model.EventStatusOffSale); err != nil {
		return err
	}
	return uc.repo.UpdateStatus(ctx, ID, model.EventStatusOffSale)
}

// GetEventStock 返回活动各票种的剩余库存。
// 注意这是数据库落库快照口径，秒杀实时库存以 Redis 为准，两者允许秒级不一致。
func (uc *EventUseCase) GetEventStock(ctx context.Context, ID int64) ([]*model.TicketType, error) {
	if ID == 0 {
		return nil, errors.BadRequest("EVENT_BAD_REQUEST", "活动 ID 不能为空")
	}
	if _, err := uc.repo.GetEvent(ctx, ID); err != nil {
		return nil, err
	}
	return uc.tiketTypeRepo.FindByEvent(ctx, ID)
}
