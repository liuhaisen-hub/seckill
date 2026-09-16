package service

import (
	"context"

	pb "activity/api/stats/v1"
	"activity/internal/biz"
)

type StatsService struct {
	pb.UnimplementedStatsServer
	uc *biz.StatsUseCase
}

func NewStatsService(uc *biz.StatsUseCase) *StatsService {
	return &StatsService{
		uc: uc,
	}
}

func (s *StatsService) GetDashboard(ctx context.Context, req *pb.GetDashboardRequest) (*pb.GetDashboardReply, error) {
	return &pb.GetDashboardReply{}, nil
}
func (s *StatsService) ListSalesTrend(ctx context.Context, req *pb.ListSalesTrendRequest) (*pb.ListSalesTrendReply, error) {
	return &pb.ListSalesTrendReply{}, nil
}
func (s *StatsService) ListTicketTypeStats(ctx context.Context, req *pb.ListTicketTypeStatsRequest) (*pb.ListTicketTypeStatsReply, error) {
	return &pb.ListTicketTypeStatsReply{}, nil
}
func (s *StatsService) GetConversionFunnel(ctx context.Context, req *pb.GetConversionFunnelRequest) (*pb.GetConversionFunnelReply, error) {
	return &pb.GetConversionFunnelReply{}, nil
}
