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

type promoRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewPromoRepo(data *Data, logger *slog.Logger) biz.PromoRepo {
	return &promoRepo{
		data:   data,
		logger: logger,
	}
}

func (r *promoRepo) CreatePromo(ctx context.Context, promo *model.PromoCode) (*model.PromoCode, error) {
	if err := r.data.db.WithContext(ctx).Model(&model.PromoCode{}).Create(promo).Error; err != nil {
		return nil, err
	}
	return promo, nil
}

func (r *promoRepo) UpdatePromo(ctx context.Context, promo *model.PromoCode) error {
	// 结构体更新会自动跳过零值字段，契合 proto optional 的部分更新语义。
	res := r.data.db.WithContext(ctx).
		Where("id = ?", promo.ID).
		Updates(promo)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrPromoNotFound
	}
	return nil
}

func (r *promoRepo) DeletePromo(ctx context.Context, id int64) error {
	res := r.data.db.WithContext(ctx).Where("id = ?", id).Delete(&model.PromoCode{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrPromoNotFound
	}
	return nil
}

func (r *promoRepo) GetPromo(ctx context.Context, id int64) (*model.PromoCode, error) {
	var promo model.PromoCode
	err := r.data.db.WithContext(ctx).Where("id = ?", id).First(&promo).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, biz.ErrPromoNotFound
	}
	if err != nil {
		return nil, err
	}
	return &promo, nil
}

func (r *promoRepo) ListPromo(ctx context.Context, opts ...utils.ListOption) ([]*model.PromoCode, int64, error) {
	o := utils.NewListOptions(opts...)

	db := r.data.db.WithContext(ctx).Model(&model.PromoCode{})
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

	var list []*model.PromoCode
	if err := db.Offset(o.Offset).Limit(o.Limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *promoRepo) FindPromoByCode(ctx context.Context, code string) (*model.PromoCode, error) {
	var promo model.PromoCode
	err := r.data.db.WithContext(ctx).Where("code = ?", code).First(&promo).Error
	if err != nil {
		return nil, err
	}
	return &promo, nil
}
