package service

import (
	"context"
	"sale/model"
	"sale/pkg/utils"

	pb "activity/api/promo/v1"
	"activity/internal/biz"

	"gorm.io/gorm"
)

type PromoService struct {
	pb.UnimplementedPromoServer
	uc *biz.PromoUseCase
}

func NewPromoService(uc *biz.PromoUseCase) *PromoService {
	return &PromoService{uc: uc}
}

func (s *PromoService) CreatePromo(ctx context.Context, req *pb.CreatePromoRequest) (*pb.CreatePromoReply, error) {
	res, err := s.uc.CreatePromo(ctx, &model.PromoCode{
		Code:          req.Code,
		EventID:       uint(req.EventId),
		DiscountType:  req.DiscountType,
		DiscountValue: float64(req.DiscountValue),
		MinAmount:     float64(req.MinAmount),
		MaxUses:       int(req.MaxUses),
		UsedCount:     int(req.UsedCount),
		IsActive:      req.IsActive,
	}, req.StartTime, req.EndTime)
	if err != nil {
		return nil, err
	}
	return &pb.CreatePromoReply{
		Code:          res.Code,
		EventId:       int64(res.EventID),
		DiscountType:  res.DiscountType,
		DiscountValue: float32(res.DiscountValue),
		MinAmount:     float32(res.MinAmount),
		MaxUses:       int32(res.MaxUses),
		UsedCount:     int32(res.UsedCount),
		StartTime:     utils.FormatTime(res.StartTime),
		EndTime:       utils.FormatTime(res.EndTime),
		IsActive:      res.IsActive,
	}, nil
}

func (s *PromoService) UpdatePromo(ctx context.Context, req *pb.UpdatePromoRequest) (*pb.UpdatePromoReply, error) {
	promo := &model.PromoCode{
		Model:         gorm.Model{ID: uint(req.Id)},
		Code:          req.Code,
		EventID:       uint(req.EventId),
		DiscountType:  req.DiscountType,
		DiscountValue: float64(req.DiscountValue),
		MinAmount:     float64(req.MinAmount),
		MaxUses:       int(req.MaxUses),
		UsedCount:     int(req.UsedCount),
	}
	if req.IsActive != nil {
		promo.IsActive = *req.IsActive
	}
	if err := s.uc.UpdatePromo(ctx, promo, req.StartTime, req.EndTime); err != nil {
		return nil, err
	}
	return &pb.UpdatePromoReply{
		Id:            int64(promo.ID),
		Code:          promo.Code,
		EventId:       int64(promo.EventID),
		DiscountType:  promo.DiscountType,
		DiscountValue: float32(promo.DiscountValue),
		MinAmount:     float32(promo.MinAmount),
		MaxUses:       int32(promo.MaxUses),
		UsedCount:     int32(promo.UsedCount),
		StartTime:     req.StartTime,
		EndTime:       req.EndTime,
		IsActive:      req.IsActive,
	}, nil
}

func (s *PromoService) DeletePromo(ctx context.Context, req *pb.DeletePromoRequest) (*pb.DeletePromoReply, error) {
	if err := s.uc.DeletePromo(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &pb.DeletePromoReply{}, nil
}

func (s *PromoService) GetPromo(ctx context.Context, req *pb.GetPromoRequest) (*pb.GetPromoReply, error) {
	res, err := s.uc.GetPromo(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	isActive := res.IsActive
	return &pb.GetPromoReply{
		Id:            int64(res.ID),
		Code:          res.Code,
		EventId:       int64(res.EventID),
		DiscountType:  res.DiscountType,
		DiscountValue: float32(res.DiscountValue),
		MinAmount:     float32(res.MinAmount),
		MaxUses:       int32(res.MaxUses),
		UsedCount:     int32(res.UsedCount),
		StartTime:     utils.FormatTime(res.StartTime),
		EndTime:       utils.FormatTime(res.EndTime),
		IsActive:      &isActive,
	}, nil
}

func (s *PromoService) ListPromo(ctx context.Context, req *pb.ListPromoRequest) (*pb.ListPromoReply, error) {
	res, total, err := s.uc.ListPromo(ctx, req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	list := make([]*pb.PromoData, 0, len(res))
	for _, item := range res {
		isActive := item.IsActive
		list = append(list, &pb.PromoData{
			Id:            int64(item.ID),
			Code:          item.Code,
			EventId:       int64(item.EventID),
			DiscountType:  item.DiscountType,
			DiscountValue: float32(item.DiscountValue),
			MinAmount:     float32(item.MinAmount),
			MaxUses:       int32(item.MaxUses),
			UsedCount:     int32(item.UsedCount),
			StartTime:     utils.FormatTime(item.StartTime),
			EndTime:       utils.FormatTime(item.EndTime),
			IsActive:      &isActive,
		})
	}
	return &pb.ListPromoReply{
		List:  list,
		Total: total,
	}, nil
}

func (s *PromoService) VaildatePromo(ctx context.Context, req *pb.ValidRequest) (*pb.ValiReply, error) {
	res, err := s.uc.Validate(ctx, req.GetCode(), float64(req.Amount))
	if err != nil {
		return nil, err
	}
	return &pb.ValiReply{
		Code:          res.Code,
		DiscountType:  res.DiscountType,
		DiscountValue: float32(res.DiscountValue),
		Discount:      float32(res.Discount),
		FinalAmount:   float32(res.FinialAmount),
	}, nil
}
