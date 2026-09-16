package biz

import (
	"context"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"github.com/go-kratos/kratos/v3/errors"
)

// ShowRepo 是场次仓储接口，由 data 层实现，是依赖倒置的接缝。
type ShowRepo interface {
	CreateShow(ctx context.Context, show *model.Show) (*model.Show, error)
	UpdateShow(ctx context.Context, show *model.Show) error
	DeleteShow(ctx context.Context, id int64) error
	GetShow(ctx context.Context, id int64) (*model.Show, error)
	ListShow(ctx context.Context, opts ...utils.ListOption) ([]*model.Show, int64, error)
	UpdateStatus(ctx context.Context, ID int64, status string) error
	FindByEvent(ctx context.Context, ID int64) (*model.Show, error)
}

var (
	ErrShowNotFound = errors.NotFound("SHOW_NOT_FOUND", "场次不存在")
)

// ShowUseCase 是场次业务用例。
type ShowUseCase struct {
	repo   ShowRepo
	logger *slog.Logger
}

func NewShowUseCase(repo ShowRepo, logger *slog.Logger) *ShowUseCase {
	return &ShowUseCase{
		repo:   repo,
		logger: logger,
	}
}

func (uc *ShowUseCase) CreateShow(ctx context.Context, show *model.Show, showTime, endTime string) (*model.Show, error) {
	if show.EventID == 0 {
		return nil, errors.BadRequest("SHOW_BAD_REQUEST", "所属活动 ID 不能为空")
	}
	if show.Name == "" {
		return nil, errors.BadRequest("SHOW_BAD_REQUEST", "场次名称不能为空")
	}
	if showTime == "" || endTime == "" {
		return nil, errors.BadRequest("SHOW_BAD_REQUEST", "必须填写场次开始时间和结束时间")
	}
	startAt, err := utils.ParserTimeString(showTime)
	if err != nil {
		return nil, fmt.Errorf("开始时间填写错误")
	}
	endAt, err := utils.ParserTimeString(endTime)
	if err != nil {
		return nil, fmt.Errorf("结束时间填写有误")
	}
	if endAt.Before(startAt) {
		return nil, fmt.Errorf("结束时间不能早于开始时间")
	}
	show.ShowTime = startAt
	show.EndTime = endAt
	return uc.repo.CreateShow(ctx, show)
}

func (uc *ShowUseCase) UpdateShow(ctx context.Context, show *model.Show, showTime, endTime string) error {
	if show.ID == 0 {
		return errors.BadRequest("SHOW_BAD_REQUEST", "场次 ID 不能为空")
	}
	if showTime != "" {
		startAt, err := utils.ParserTimeString(showTime)
		if err != nil {
			return fmt.Errorf("开始时间填写错误")
		}
		show.ShowTime = startAt
	}
	if endTime != "" {
		endAt, err := utils.ParserTimeString(endTime)
		if err != nil {
			return fmt.Errorf("结束时间填写有误")
		}
		show.EndTime = endAt
	}
	if show.EndTime.Before(show.ShowTime) {
		return fmt.Errorf("结束时间不能早于开始时间")
	}
	return uc.repo.UpdateShow(ctx, show)
}

func (uc *ShowUseCase) DeleteShow(ctx context.Context, id int64) error {
	if id == 0 {
		return errors.BadRequest("SHOW_BAD_REQUEST", "场次 ID 不能为空")
	}
	return uc.repo.DeleteShow(ctx, id)
}

func (uc *ShowUseCase) GetShow(ctx context.Context, id int64) (*model.Show, error) {
	if id == 0 {
		return nil, errors.BadRequest("SHOW_BAD_REQUEST", "场次 ID 不能为空")
	}
	return uc.repo.GetShow(ctx, id)
}
func (uc *ShowUseCase) GetByEvent(ctx context.Context, id int64) (*model.Show, error) {
	if id == 0 {
		return nil, errors.BadRequest("SHOW_BAD_REQUEST", "活动 ID 不能为空")
	}
	return uc.repo.FindByEvent(ctx, id)
}
func (uc *ShowUseCase) ListShow(ctx context.Context, page, size int64, opts ...utils.ListOption) ([]*model.Show, int64, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	opts = append(opts, utils.ListOffset(int((page-1)*size)), utils.ListLimit(int(size)))
	return uc.repo.ListShow(ctx, opts...)
}

func (uc *ShowUseCase) UpdateStatus(ctx context.Context, ID int64, status string) error {
	if !model.IsValidEventStatus(status) {
		return fmt.Errorf("不合法的状态值")
	}
	show, err := uc.repo.FindByEvent(ctx, ID)
	if err != nil {
		return err
	}
	if status == model.EventStatusOnSale {
		return uc.publishShow(ctx, ID, show)
	}
	if status == model.EventStatusOffSale {
		return uc.unPublishShow(ctx, ID, show)
	}
	if status == model.EventStatusEnded {
		if err := model.IsValidEventTransition(show.Status, model.EventStatusEnded); err != nil {
			return err
		}
		// 更新状态
		return uc.repo.UpdateStatus(ctx, ID, model.EventStatusEnded)
	}
	return nil
}

func (uc *ShowUseCase) publishShow(ctx context.Context, ID int64, show *model.Show) error {
	if err := model.IsValidEventTransition(show.Status, model.EventStatusOnSale); err != nil {
		return err
	}
	return uc.repo.UpdateStatus(ctx, ID, model.EventStatusOnSale)
}

func (uc *ShowUseCase) unPublishShow(ctx context.Context, ID int64, show *model.Show) error {
	if err := model.IsValidEventTransition(show.Status, model.EventStatusOffSale); err != nil {
		return err
	}
	return uc.repo.UpdateStatus(ctx, ID, model.EventStatusOffSale)
}
