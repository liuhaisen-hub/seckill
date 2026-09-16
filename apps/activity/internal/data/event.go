package data

import (
	"context"
	"errors"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"activity/internal/biz"

	"gorm.io/gorm"
)

type eventRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewEventRepo(data *Data, logger *slog.Logger) biz.EventRepo {
	return &eventRepo{
		data:   data,
		logger: logger,
	}
}

func (r *eventRepo) CreateEvent(ctx context.Context, event *model.Event) (*model.Event, error) {
	err := r.data.db.WithContext(ctx).Model(&model.Event{}).Create(&event).Error
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (r *eventRepo) UpdateEvent(ctx context.Context, event *model.Event) error {
	// 结构体更新会自动跳过零值字段，契合 proto optional 的部分更新语义。
	res := r.data.db.WithContext(ctx).
		Where("id = ?", event.ID).
		Updates(&event)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrEventNotFound
	}
	return nil
}

func (r *eventRepo) DeleteEvent(ctx context.Context, id int64) error {
	res := r.data.db.WithContext(ctx).Where("id = ?", id).Delete(&model.Event{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrEventNotFound
	}
	return nil
}

func (r *eventRepo) GetEvent(ctx context.Context, id int64) (*model.Event, error) {
	var po model.Event
	err := r.data.db.WithContext(ctx).Where("id = ?", id).First(&po).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, biz.ErrEventNotFound
	}
	if err != nil {
		return nil, err
	}
	return &po, nil
}

func (r *eventRepo) ListEvent(ctx context.Context, opts ...utils.ListOption) ([]*model.Event, int64, error) {
	o := utils.NewListOptions(opts...)

	db := r.data.db.WithContext(ctx).Model(&model.Event{})
	for field, value := range o.Filters {
		db = db.Where(field+" = ?", value)
	}
	for _, order := range o.OrderBy {
		db = db.Order(order)
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var pos []*model.Event
	if err := db.Offset(o.Offset).Limit(o.Limit).Find(&pos).Error; err != nil {
		return nil, 0, err
	}

	return pos, total, nil
}

func (r *eventRepo) UpdateStatus(ctx context.Context, ID int64, status string) error {
	return r.data.db.WithContext(ctx).Model(&model.Event{}).Where("id = ?", uint(ID)).Update("status", status).Error
}
