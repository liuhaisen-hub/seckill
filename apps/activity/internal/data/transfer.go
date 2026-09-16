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

type transferRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewTransferRepo(data *Data, logger *slog.Logger) biz.TransferRepo {
	return &transferRepo{
		data:   data,
		logger: logger,
	}
}

func (r *transferRepo) CreateTransfer(ctx context.Context, transfer *model.TicketTransfer) (*model.TicketTransfer, error) {
	if err := r.data.db.WithContext(ctx).Model(&model.TicketTransfer{}).Create(transfer).Error; err != nil {
		return nil, err
	}
	return transfer, nil
}

func (r *transferRepo) UpdateTransfer(ctx context.Context, transfer *model.TicketTransfer) error {
	// 结构体更新会自动跳过零值字段，契合 proto optional 的部分更新语义。
	res := r.data.db.WithContext(ctx).
		Where("id = ?", transfer.ID).
		Updates(transfer)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrTransferNotFound
	}
	return nil
}

func (r *transferRepo) DeleteTransfer(ctx context.Context, id int64) error {
	res := r.data.db.WithContext(ctx).Where("id = ?", id).Delete(&model.TicketTransfer{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return biz.ErrTransferNotFound
	}
	return nil
}

func (r *transferRepo) GetTransfer(ctx context.Context, id int64) (*model.TicketTransfer, error) {
	var transfer model.TicketTransfer
	err := r.data.db.WithContext(ctx).Where("id = ?", id).First(&transfer).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, biz.ErrTransferNotFound
	}
	if err != nil {
		return nil, err
	}
	return &transfer, nil
}

func (r *transferRepo) ListTransfer(ctx context.Context, opts ...utils.ListOption) ([]*model.TicketTransfer, int64, error) {
	o := utils.NewListOptions(opts...)

	db := r.data.db.WithContext(ctx).Model(&model.TicketTransfer{})
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

	var list []*model.TicketTransfer
	if err := db.Offset(o.Offset).Limit(o.Limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *transferRepo) FindPending(ctx context.Context) ([]*model.TicketTransfer, error) {
	var tr []*model.TicketTransfer
	if err := r.data.db.WithContext(ctx).Where("status = ?", "pending").Order("created_at DESC").Error; err != nil {
		return nil, err
	}
	return tr, nil
}
