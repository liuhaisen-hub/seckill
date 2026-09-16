package service

import (
	"context"
	"sale/model"
	"sale/pkg/utils"

	pb "activity/api/event/v1"
	"activity/internal/biz"
)

type EventService struct {
	pb.UnimplementedEventServer
	uc *biz.EventUseCase
}

func NewEventService(uc *biz.EventUseCase) *EventService {
	return &EventService{
		uc: uc,
	}
}

func (s *EventService) CreateEvent(ctx context.Context, req *pb.CreateEventRequest) (*pb.CreateEventReply, error) {
	res, err := s.uc.CreateEvent(ctx, &model.Event{
		Title:       req.Title,
		Description: req.Description,
		Location:    req.Location,
	}, req.StartTime, req.EndTime)
	if err != nil {
		return nil, err
	}
	return &pb.CreateEventReply{
		Id:        int64(res.ID),
		StartTime: utils.FormatTime(res.StartTime),
		EndTime:   utils.FormatTime(res.EndTime),
		Status:    res.Status,
		Title:     res.Title,
		Location:  res.Location,
	}, nil
}
func (s *EventService) UpdateEvent(ctx context.Context, req *pb.UpdateEventRequest) (*pb.UpdateEventReply, error) {
	err := s.uc.UpdateEvent(ctx, &model.Event{
		Title:       req.Title,
		Description: req.Description,
		Location:    req.Location,
	}, req.StartTime, req.EndTime)
	if err != nil {
		return nil, err
	}
	return &pb.UpdateEventReply{}, nil
}
func (s *EventService) DeleteEvent(ctx context.Context, req *pb.DeleteEventRequest) (*pb.DeleteEventReply, error) {
	return &pb.DeleteEventReply{}, nil
}
func (s *EventService) GetEvent(ctx context.Context, req *pb.GetEventRequest) (*pb.GetEventReply, error) {
	res, err := s.uc.GetEvent(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.GetEventReply{
		Title:       res.Title,
		Description: res.Description,
		Location:    res.Location,
		StartTime:   utils.FormatTime(res.StartTime),
		EndTime:     utils.FormatTime(res.EndTime),
		SoldCount:   int32(res.TotalStock),
		Status:      res.Status,
	}, nil
}
func (s *EventService) ListEvent(ctx context.Context, req *pb.ListEventRequest) (*pb.ListEventReply, error) {
	res, total, err := s.uc.ListEvent(ctx, req.Page, req.Size)
	if err != nil {
		return nil, err
	}
	list := make([]*pb.EventData, 0, len(res))
	if len(res) > 0 {
		for _, item := range res {
			list = append(list, &pb.EventData{
				Id:        int64(item.ID),
				Title:     item.Title,
				Location:  item.Location,
				StartTime: utils.FormatTime(item.StartTime),
				EndTime:   utils.FormatTime(item.EndTime),
				Status:    item.Status,
				SoldCount: int32(item.TotalStock),
			})
		}
	}
	return &pb.ListEventReply{
		List:  list,
		Total: total,
	}, nil
}

func (s *EventService) UpdateStatusEvent(ctx context.Context, req *pb.UpdateStatusRequest) (*pb.UpdateStatusReply, error) {
	err := s.uc.UpdateEventStatus(ctx, req.GetId(), req.GetStatus())
	if err != nil {
		return nil, err
	}
	return &pb.UpdateStatusReply{}, nil
}

func (s *EventService) GetEventStock(ctx context.Context, req *pb.GetEventStockRequest) (*pb.GetEventStockReply, error) {
	list, err := s.uc.GetEventStock(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	stocks := make([]*pb.TicketTypeStock, 0, len(list))
	for _, tt := range list {
		stocks = append(stocks, &pb.TicketTypeStock{
			TicketTypeId: int64(tt.ID),
			Stock:        int32(tt.Stock),
		})
	}
	return &pb.GetEventStockReply{Stocks: stocks}, nil
}
