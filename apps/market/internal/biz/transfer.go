package biz

import (
	"context"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"github.com/go-kratos/kratos/v3/errors"
)

// 转让状态常量，与 model 层注释保持一致。
const (
	TransferStatusApproved = "approved"
)

// TransferRepo 是票务转让记录仓储接口，由 data 层实现。
// 市场服务只读转让记录（历史查询），转让的发起与审核在 activity 服务。
type TransferRepo interface {
	// ListTransferHistory 查某用户买入(from)或卖出(to)的转让记录。
	ListTransferHistory(ctx context.Context, userID int64, opts ...utils.ListOption) ([]*model.TicketTransfer, int64, error)
}

// TransferUseCase 是转让记录查询业务用例。
type TransferUseCase struct {
	repo   TransferRepo
	logger *slog.Logger
}

func NewTransferUseCase(repo TransferRepo, logger *slog.Logger) *TransferUseCase {
	return &TransferUseCase{
		repo:   repo,
		logger: logger,
	}
}

func (uc *TransferUseCase) ListHistory(ctx context.Context, userID, page, size int64) ([]*model.TicketTransfer, int64, error) {
	if userID == 0 {
		return nil, 0, errors.BadRequest("TRANSFER_BAD_REQUEST", "用户 ID 不能为空")
	}
	list, total, err := uc.repo.ListTransferHistory(ctx, userID, append(pageOptions(page, size),
		utils.ListOrderBy("created_at DESC"),
	)...)
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}
