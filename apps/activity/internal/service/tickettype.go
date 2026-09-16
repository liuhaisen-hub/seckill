package service

import (
	"context"
	"sale/model"

	pb "activity/api/ticket_type/v1"
	"activity/internal/biz"

	"gorm.io/gorm"
)

type TicketTypeService struct {
	pb.UnimplementedTicketTypeServer
	uc *biz.TicketTypeUseCase
}

func NewTicketTypeService(uc *biz.TicketTypeUseCase) *TicketTypeService {
	return &TicketTypeService{uc: uc}
}

func (s *TicketTypeService) CreateTicketType(ctx context.Context, req *pb.CreateTicketTypeRequest) (*pb.CreateTicketTypeReply, error) {
	res, err := s.uc.Create(ctx, &model.TicketType{
		EventID:    uint(req.EventId),
		Name:       req.Name,
		Price:      float64(req.Price),
		Stock:      int(req.Stock),
		MaxPerUser: int(req.MaxPerUser),
		SortOrder:  int(req.SortOrder),
	})
	if err != nil {
		return nil, err
	}
	return &pb.CreateTicketTypeReply{
		Id:         int64(res.ID),
		Name:       res.Name,
		Price:      float32(res.Price),
		Stock:      int32(res.Stock),
		MaxPerUser: int32(res.MaxPerUser),
		SortOrder:  int32(res.SortOrder),
		EventId:    int64(res.EventID),
	}, nil
}
func (s *TicketTypeService) UpdateTicketType(ctx context.Context, req *pb.UpdateTicketTypeRequest) (*pb.UpdateTicketTypeReply, error) {
	res, err := s.uc.Update(ctx, &model.TicketType{
		Model:      gorm.Model{ID: uint(req.Id)},
		EventID:    uint(req.EventId),
		Name:       req.Name,
		Price:      float64(req.Price),
		Stock:      int(req.Stock),
		MaxPerUser: int(req.MaxPerUser),
		SortOrder:  int(req.SortOrder),
	})
	if err != nil {
		return nil, err
	}
	return &pb.UpdateTicketTypeReply{
		Id:         int64(res.ID),
		Name:       res.Name,
		Price:      float32(res.Price),
		Stock:      int32(res.Stock),
		MaxPerUser: int32(res.MaxPerUser),
		SortOrder:  int32(res.SortOrder),
		EventId:    int64(res.EventID),
	}, nil
}
func (s *TicketTypeService) DeleteTicketType(ctx context.Context, req *pb.DeleteTicketTypeRequest) (*pb.DeleteTicketTypeReply, error) {
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &pb.DeleteTicketTypeReply{}, nil
}
func (s *TicketTypeService) GetTicketType(ctx context.Context, req *pb.GetTicketTypeRequest) (*pb.GetTicketTypeReply, error) {
	res, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.GetTicketTypeReply{
		Id:         int64(res.ID),
		Name:       res.Name,
		Price:      float32(res.Price),
		Stock:      int32(res.Stock),
		MaxPerUser: int32(res.MaxPerUser),
		SortOrder:  int32(res.SortOrder),
		EventId:    int64(res.EventID),
	}, nil
}
func (s *TicketTypeService) ListTicketType(ctx context.Context, req *pb.ListTicketTypeRequest) (*pb.ListTicketTypeReply, error) {
	res, total, err := s.uc.List(ctx, req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	list := make([]*pb.TicketTypeData, 0, len(res))
	for _, item := range res {
		list = append(list, &pb.TicketTypeData{
			Id:         int64(item.ID),
			Name:       item.Name,
			Price:      float32(item.Price),
			Stock:      int32(item.Stock),
			MaxPerUser: int32(item.MaxPerUser),
			SortOrder:  int32(item.SortOrder),
			EventId:    int64(item.EventID),
		})
	}
	return &pb.ListTicketTypeReply{
		List:  list,
		Total: total,
	}, nil
}
