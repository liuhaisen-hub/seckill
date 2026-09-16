package biz

import (
	"context"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// 促销码折扣类型。
const (
	PromoDiscountPercent = "percent" // 按百分比折扣
	PromoDiscountFixed   = "fixed"   // 固定金额减免
)

// PromoRepo 是促销码仓储接口，由 data 层实现，是依赖倒置的接缝。
type PromoRepo interface {
	CreatePromo(ctx context.Context, promo *model.PromoCode) (*model.PromoCode, error)
	UpdatePromo(ctx context.Context, promo *model.PromoCode) error
	DeletePromo(ctx context.Context, id int64) error
	GetPromo(ctx context.Context, id int64) (*model.PromoCode, error)
	ListPromo(ctx context.Context, opts ...utils.ListOption) ([]*model.PromoCode, int64, error)
	FindPromoByCode(ctx context.Context, code string) (*model.PromoCode, error)
}

var (
	ErrPromoNotFound = errors.NotFound("PROMO_NOT_FOUND", "促销码不存在")
)

// PromoUseCase 是促销码业务用例。
type PromoUseCase struct {
	repo   PromoRepo
	logger *slog.Logger
}

func NewPromoUseCase(repo PromoRepo, logger *slog.Logger) *PromoUseCase {
	return &PromoUseCase{
		repo:   repo,
		logger: logger,
	}
}

func isValidDiscountType(discountType string) bool {
	return discountType == PromoDiscountPercent || discountType == PromoDiscountFixed
}

func validatePromo(promo *model.PromoCode) error {
	if promo.Code == "" {
		return errors.BadRequest("PROMO_BAD_REQUEST", "促销码不能为空")
	}
	if !isValidDiscountType(promo.DiscountType) {
		return errors.BadRequest("PROMO_BAD_REQUEST", "折扣类型只能是 percent 或 fixed")
	}
	if promo.DiscountValue <= 0 {
		return errors.BadRequest("PROMO_BAD_REQUEST", "折扣值必须大于 0")
	}
	if promo.DiscountType == PromoDiscountPercent && promo.DiscountValue > 100 {
		return errors.BadRequest("PROMO_BAD_REQUEST", "百分比折扣不能超过 100")
	}
	if promo.MinAmount < 0 {
		return errors.BadRequest("PROMO_BAD_REQUEST", "满减门槛不能为负")
	}
	if promo.MaxUses < 0 {
		return errors.BadRequest("PROMO_BAD_REQUEST", "使用次数上限不能为负")
	}
	return nil
}

func (uc *PromoUseCase) CreatePromo(ctx context.Context, promo *model.PromoCode, startTime, endTime string) (*model.PromoCode, error) {
	if err := validatePromo(promo); err != nil {
		return nil, err
	}
	if startTime == "" || endTime == "" {
		return nil, errors.BadRequest("PROMO_BAD_REQUEST", "必须填写生效开始时间和结束时间")
	}
	startAt, err := utils.ParserTimeString(startTime)
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
	promo.StartTime = startAt
	promo.EndTime = endAt
	return uc.repo.CreatePromo(ctx, promo)
}

func (uc *PromoUseCase) UpdatePromo(ctx context.Context, promo *model.PromoCode, startTime, endTime string) error {
	if promo.ID == 0 {
		return errors.BadRequest("PROMO_BAD_REQUEST", "促销码 ID 不能为空")
	}
	if err := validatePromo(promo); err != nil {
		return err
	}
	if startTime != "" {
		startAt, err := utils.ParserTimeString(startTime)
		if err != nil {
			return fmt.Errorf("开始时间填写错误")
		}
		promo.StartTime = startAt
	}
	if endTime != "" {
		endAt, err := utils.ParserTimeString(endTime)
		if err != nil {
			return fmt.Errorf("结束时间填写有误")
		}
		promo.EndTime = endAt
	}
	if !promo.StartTime.IsZero() && !promo.EndTime.IsZero() && promo.EndTime.Before(promo.StartTime) {
		return fmt.Errorf("结束时间不能早于开始时间")
	}
	return uc.repo.UpdatePromo(ctx, promo)
}

func (uc *PromoUseCase) DeletePromo(ctx context.Context, id int64) error {
	if id == 0 {
		return errors.BadRequest("PROMO_BAD_REQUEST", "促销码 ID 不能为空")
	}
	return uc.repo.DeletePromo(ctx, id)
}

func (uc *PromoUseCase) GetPromo(ctx context.Context, id int64) (*model.PromoCode, error) {
	if id == 0 {
		return nil, errors.BadRequest("PROMO_BAD_REQUEST", "促销码 ID 不能为空")
	}
	return uc.repo.GetPromo(ctx, id)
}

func (uc *PromoUseCase) ListPromo(ctx context.Context, page, size int64, opts ...utils.ListOption) ([]*model.PromoCode, int64, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	opts = append(opts, utils.ListOffset(int((page-1)*size)), utils.ListLimit(int(size)))
	return uc.repo.ListPromo(ctx, opts...)
}

type ValidateResp struct {
	Code          string
	DiscountType  string
	DiscountValue float64
	Discount      float64
	FinialAmount  float64
}

func (uc *PromoUseCase) Validate(ctx context.Context, code string, amount float64) (*ValidateResp, error) {
	promoCode, err := uc.repo.FindPromoByCode(ctx, code)
	if err != nil {
		return nil, err
	}
	// 校验
	if err := validatePromCode(promoCode, amount); err != nil {
		return nil, err
	}
	// 计算打折
	discount := CalculateDiscount(promoCode, amount)
	finalAmount := amount - discount
	return &ValidateResp{
		Code:          promoCode.Code,
		DiscountType:  promoCode.DiscountType,
		DiscountValue: promoCode.DiscountValue,
		Discount:      discount,
		FinialAmount:  finalAmount,
	}, nil
}

func validatePromCode(promoCode *model.PromoCode, amount float64) error {
	now := time.Now()
	// 检查是否在有效期
	if !promoCode.StartTime.IsZero() && now.Before(promoCode.StartTime) {
		return fmt.Errorf("不再有效期内")
	}
	if !promoCode.EndTime.IsZero() && now.After(promoCode.EndTime) {
		return fmt.Errorf("已经超过有效期")
	}
	// 检查使用次数
	if promoCode.MaxUses > 0 && promoCode.UsedCount >= promoCode.MaxUses {
		return fmt.Errorf("已经超出使用次数")
	}
	// 最低消费
	if amount < promoCode.MinAmount {
		return fmt.Errorf("未达到最低消费金额")
	}
	return nil
}

func CalculateDiscount(promoCode *model.PromoCode, amount float64) float64 {
	switch promoCode.DiscountType {
	case "percent":
		discount := amount * promoCode.DiscountValue / 100
		return discount
	case "fixed":
		return promoCode.DiscountValue
	default:
		return 0
	}
}
