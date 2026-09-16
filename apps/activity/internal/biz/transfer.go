package biz

import (
	"context"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"github.com/go-kratos/kratos/v3/errors"
)

// 转让状态与类型常量，与 model 层的默认值保持一致。
const (
	TransferStatusPending  = "pending"
	TransferStatusApproved = "approved"
	TransferStatusRejected = "rejected"

	TransferTypeGift        = "gift"
	TransferTypeMarketplace = "marketplace"
)

// TransferRepo 是票务转让仓储接口，由 data 层实现，是依赖倒置的接缝。
type TransferRepo interface {
	CreateTransfer(ctx context.Context, transfer *model.TicketTransfer) (*model.TicketTransfer, error)
	UpdateTransfer(ctx context.Context, transfer *model.TicketTransfer) error
	DeleteTransfer(ctx context.Context, id int64) error
	GetTransfer(ctx context.Context, id int64) (*model.TicketTransfer, error)
	ListTransfer(ctx context.Context, opts ...utils.ListOption) ([]*model.TicketTransfer, int64, error)
}

var (
	ErrTransferNotFound = errors.NotFound("TRANSFER_NOT_FOUND", "转让记录不存在")
)

// TransferUseCase 是票务转让业务用例。
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

func isValidTransferStatus(status string) bool {
	switch status {
	case TransferStatusPending, TransferStatusApproved, TransferStatusRejected:
		return true
	}
	return false
}

func isValidTransferType(transferType string) bool {
	switch transferType {
	case TransferTypeGift, TransferTypeMarketplace:
		return true
	}
	return false
}

func (uc *TransferUseCase) CreateTransfer(ctx context.Context, transfer *model.TicketTransfer) (*model.TicketTransfer, error) {
	if transfer.TicketID == 0 {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "票 ID 不能为空")
	}
	if transfer.FromUserID == 0 {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "转出用户 ID 不能为空")
	}
	if transfer.ToUserID == 0 {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "接收用户 ID 不能为空")
	}
	if transfer.FromUserID == transfer.ToUserID {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "不能转让给自己")
	}
	if transfer.TransferType == "" {
		transfer.TransferType = TransferTypeGift
	}
	if !isValidTransferType(transfer.TransferType) {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "转让类型错误")
	}
	if transfer.TransferType == TransferTypeMarketplace && transfer.Price < 0 {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "转让价格不能为负")
	}
	transfer.Status = TransferStatusPending
	return uc.repo.CreateTransfer(ctx, transfer)
}

func (uc *TransferUseCase) UpdateTransfer(ctx context.Context, transfer *model.TicketTransfer) error {
	if transfer.ID == 0 {
		return errors.BadRequest("TRANSFER_BAD_REQUEST", "转让记录 ID 不能为空")
	}
	if transfer.Status != "" && !isValidTransferStatus(transfer.Status) {
		return errors.BadRequest("TRANSFER_BAD_REQUEST", "转让状态错误")
	}
	return uc.repo.UpdateTransfer(ctx, transfer)
}

func (uc *TransferUseCase) DeleteTransfer(ctx context.Context, id int64) error {
	if id == 0 {
		return errors.BadRequest("TRANSFER_BAD_REQUEST", "转让记录 ID 不能为空")
	}
	return uc.repo.DeleteTransfer(ctx, id)
}

func (uc *TransferUseCase) GetTransfer(ctx context.Context, id int64) (*model.TicketTransfer, error) {
	if id == 0 {
		return nil, errors.BadRequest("TRANSFER_BAD_REQUEST", "转让记录 ID 不能为空")
	}
	return uc.repo.GetTransfer(ctx, id)
}

func (uc *TransferUseCase) ListTransfer(ctx context.Context, page, size int64, opts ...utils.ListOption) ([]*model.TicketTransfer, int64, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	opts = append(opts, utils.ListOffset(int((page-1)*size)), utils.ListLimit(int(size)))
	return uc.repo.ListTransfer(ctx, opts...)
}
