package biz

import (
	"context"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"github.com/go-kratos/kratos/v3/errors"
)

// ErrTicketTypeNotFound 票种不存在。
var (
	ErrTicketTypeNotFound = errors.NotFound("TICKET_TYPE_NOT_FOUND", "票种不存在")
)

type TicketTypeRepo interface {
	Create(ctx context.Context, tp *model.TicketType) error
	Updates(ctx context.Context, tp *model.TicketType) (*model.TicketType, error)
	Delete(ctx context.Context, ID int64) error
	Get(ctx context.Context, ID int64) (*model.TicketType, error)
	List(ctx context.Context, opts ...utils.ListOption) ([]*model.TicketType, int64, error)
	FindByEvent(ctx context.Context, ID int64) ([]*model.TicketType, error)
}

type TicketTypeUseCase struct {
	repo   TicketTypeRepo
	logger *slog.Logger
}

func NewTicketTypeUseCase(repo TicketTypeRepo, logger *slog.Logger) *TicketTypeUseCase {
	return &TicketTypeUseCase{
		repo:   repo,
		logger: logger,
	}
}

func (uc *TicketTypeUseCase) Create(ctx context.Context, tp *model.TicketType) (*model.TicketType, error) {
	if tp.EventID == 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "所属活动 ID 不能为空")
	}
	if tp.Name == "" {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种名称不能为空")
	}
	if tp.Price < 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种价格不能为负")
	}
	if tp.Stock < 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种库存不能为负")
	}
	if tp.MaxPerUser < 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "限购数量不能为负")
	}
	if err := uc.repo.Create(ctx, tp); err != nil {
		return nil, err
	}
	return tp, nil
}

func (uc *TicketTypeUseCase) Update(ctx context.Context, tp *model.TicketType) (*model.TicketType, error) {
	if tp.ID == 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种 ID 不能为空")
	}
	if tp.Name == "" {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种名称不能为空")
	}
	if tp.Price < 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种价格不能为负")
	}
	if tp.Stock < 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种库存不能为负")
	}
	return uc.repo.Updates(ctx, tp)
}

func (uc *TicketTypeUseCase) Delete(ctx context.Context, id int64) error {
	if id == 0 {
		return errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种 ID 不能为空")
	}
	return uc.repo.Delete(ctx, id)
}

func (uc *TicketTypeUseCase) Get(ctx context.Context, id int64) (*model.TicketType, error) {
	if id == 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "票种 ID 不能为空")
	}
	return uc.repo.Get(ctx, id)
}

func (uc *TicketTypeUseCase) List(ctx context.Context, page, size int64, opts ...utils.ListOption) ([]*model.TicketType, int64, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	opts = append(opts, utils.ListOffset(int((page-1)*size)), utils.ListLimit(int(size)))
	return uc.repo.List(ctx, opts...)
}

func (uc *TicketTypeUseCase) FindByEvent(ctx context.Context, eventID int64) ([]*model.TicketType, error) {
	if eventID == 0 {
		return nil, errors.BadRequest("TICKET_TYPE_BAD_REQUEST", "活动 ID 不能为空")
	}
	return uc.repo.FindByEvent(ctx, eventID)
}
