package data

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"activity/internal/biz"

	"gorm.io/gorm"
)

type showRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewShowRepo(data *Data, logger *slog.Logger) biz.ShowRepo {
	return &showRepo{
		data:   data,
		logger: logger,
	}
}

func (r *showRepo) CreateShow(ctx context.Context, show *model.Show) (*model.Show, error) {
	if err := r.data.db.WithContext(ctx).Model(&model.Show{}).Create(show).Error; err != nil {
		return nil, err
	}
	return show, nil
}

func (r *showRepo) UpdateShow(ctx context.Context, show *model.Show) error {
	// 结构体更新会自动跳过零值字段，契合 proto optional 的部分更新语义。
	res := r.data.db.WithContext(ctx).
		Where("id = ?", show.ID).
		Updates(show)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrShowNotFound
	}
	return nil
}

func (r *showRepo) DeleteShow(ctx context.Context, id int64) error {
	return r.data.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var show model.Show
		err := tx.Where("id = ?", id).First(&show).Error
		if err != nil {
			return err
		}
		// 检查show是否能删
		if err := model.IsValidEventTransition(show.Status, model.EventStatusOnSale); err != nil {
			return fmt.Errorf("该状态下不能删除活动")
		}
		// 能删
		if err := tx.Where("id = ?", id).Delete(&model.Show{}).Error; err != nil {
			return err
		}
		return nil
	})

}

func (r *showRepo) GetShow(ctx context.Context, id int64) (*model.Show, error) {
	var show model.Show
	err := r.data.db.WithContext(ctx).Where("id = ?", id).First(&show).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, biz.ErrShowNotFound
	}
	if err != nil {
		return nil, err
	}
	return &show, nil
}

func (r *showRepo) ListShow(ctx context.Context, opts ...utils.ListOption) ([]*model.Show, int64, error) {
	o := utils.NewListOptions(opts...)

	db := r.data.db.WithContext(ctx).Model(&model.Show{})
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

	var list []*model.Show
	if err := db.Offset(o.Offset).Limit(o.Limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *showRepo) FindByEvent(ctx context.Context, ID int64) (*model.Show, error) {
	var show model.Show
	err := r.data.db.WithContext(ctx).Where("event_id = ?", ID).First(&show).Error
	if err != nil {
		return nil, err
	}
	return &show, nil
}

func (r *showRepo) UpdateStatus(ctx context.Context, ID int64, status string) error {
	return r.data.db.WithContext(ctx).Model(&model.Show{}).Where("id = ?", ID).Update("status", status).Error
}
