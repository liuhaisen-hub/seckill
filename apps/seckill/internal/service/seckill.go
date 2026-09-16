package service

import (
	"context"

	pb "seckill/api/seckill/v1"
	"seckill/internal/biz"
)

type SeckillService struct {
	pb.UnimplementedSeckillServer
	uc *biz.SeckillUseCase
}

func NewSeckillService(uc *biz.SeckillUseCase) *SeckillService {
	return &SeckillService{
		uc: uc,
	}
}

func (s *SeckillService) CreateSeckill(ctx context.Context, req *pb.CreateSeckillRequest) (*pb.CreateSeckillReply, error) {
	// s.uc.PurchaseTicketAsync(ctx, req.EventId, req.ShowId, req.TicketTypeId, req.Quantity)
	return &pb.CreateSeckillReply{}, nil
}

func (s *SeckillService) InitSeckill(ctx context.Context, req *pb.InitSeckillRequest) (*pb.InitSeckillReply, error) {
	err := s.uc.InitSeckill(ctx, req.ActivityId, req.ProductId, int(req.Stock))
	if err != nil {
		return nil, err
	}
	return &pb.InitSeckillReply{}, nil
}
func (s *SeckillService) GetSeckillResult(ctx context.Context, req *pb.GetSeckillResultRequest) (*pb.GetSeckillResultReply, error) {
	res, err := s.uc.GetPurchaseResult(ctx,
		uint(req.GetUserId()), uint(req.GetEventId()),
		uint(req.GetTicketTypeId()), req.GetTimestamp())
	if err != nil {
		return nil, err
	}
	// 默认 processing：结果键不存在 = 还在排队/处理，或已超过 10 分钟 TTL
	reply := &pb.GetSeckillResultReply{
		Status:    "processing",
		Message:   "处理中",
		Timestamp: req.GetTimestamp(),
	}
	if res != nil {
		reply.Status = res.Status
		reply.Message = res.Message
		reply.OrderNo = res.OrderNo
	}
	return reply, nil
}
