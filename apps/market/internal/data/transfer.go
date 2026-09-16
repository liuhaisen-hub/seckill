package data

import (
	"context"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"market/internal/biz"
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

func (r *transferRepo) ListTransferHistory(ctx context.Context, userID int64, opts ...utils.ListOption) ([]*model.TicketTransfer, int64, error) {
	o := utils.NewListOptions(opts...)

	// 用户既可能是转出方也可能是接收方，两个方向都要查
	db := r.data.db.WithContext(ctx).Model(&model.TicketTransfer{}).
		Where("from_user_id = ? OR to_user_id = ?", userID, userID)
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
