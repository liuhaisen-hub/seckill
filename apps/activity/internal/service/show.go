package service

import (
	"context"
	"sale/model"
	"sale/pkg/utils"

	pb "activity/api/show/v1"
	"activity/internal/biz"

	"gorm.io/gorm"
)

type ShowService struct {
	pb.UnimplementedShowServer
	uc *biz.ShowUseCase
}

func NewShowService(uc *biz.ShowUseCase) *ShowService {
	return &ShowService{
		uc: uc,
	}
}

func (s *ShowService) CreateShow(ctx context.Context, req *pb.CreateShowRequest) (*pb.CreateShowReply, error) {
	res, err := s.uc.CreateShow(ctx, &model.Show{
		EventID:   uint(req.EventId),
		Name:      req.Name,
		Stock:     int(req.Stock),
		SoldCount: int(req.SoldCount),
		SortOrder: int(req.SortOrder),
	}, req.ShowTime, req.EndTime)
	if err != nil {
		return nil, err
	}
	return toCreateShowReply(res), nil
}

func (s *ShowService) UpdateShow(ctx context.Context, req *pb.UpdateShowRequest) (*pb.UpdateShowReply, error) {
	show := &model.Show{
		Model:     gorm.Model{ID: uint(req.Id)},
		EventID:   uint(req.EventId),
		Name:      req.Name,
		Stock:     int(req.Stock),
		SoldCount: int(req.SoldCount),
		SortOrder: int(req.SortOrder),
	}
	if req.Status != nil {
		show.Status = *req.Status
	}
	if err := s.uc.UpdateShow(ctx, show, req.ShowTime, req.EndTime); err != nil {
		return nil, err
	}
	return &pb.UpdateShowReply{
		Id:        int64(show.ID),
		EventId:   int64(show.EventID),
		Name:      show.Name,
		ShowTime:  req.ShowTime,
		EndTime:   req.EndTime,
		Status:    req.Status,
		Stock:     int32(show.Stock),
		SoldCount: int32(show.SoldCount),
		SortOrder: int32(show.SortOrder),
	}, nil
}

func (s *ShowService) DeleteShow(ctx context.Context, req *pb.DeleteShowRequest) (*pb.DeleteShowReply, error) {
	if err := s.uc.DeleteShow(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &pb.DeleteShowReply{}, nil
}

func (s *ShowService) GetShow(ctx context.Context, req *pb.GetShowRequest) (*pb.GetShowReply, error) {
	res, err := s.uc.GetShow(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.GetShowReply{
		Id:        int64(res.ID),
		EventId:   int64(res.EventID),
		Name:      res.Name,
		ShowTime:  utils.FormatTime(res.ShowTime),
		EndTime:   utils.FormatTime(res.EndTime),
		Status:    res.Status,
		Stock:     int32(res.Stock),
		SoldCount: int32(res.SoldCount),
		SortOrder: int32(res.SortOrder),
	}, nil
}

func (s *ShowService) ListShow(ctx context.Context, req *pb.ListShowRequest) (*pb.ListShowReply, error) {
	var opts []utils.ListOption
	if req.EventId > 0 {
		opts = append(opts, utils.ListFilter("event_id", uint(req.EventId)))
	}
	if req.Status != nil {
		opts = append(opts, utils.ListFilter("status", *req.Status))
	}
	res, total, err := s.uc.ListShow(ctx, req.GetPage(), req.GetSize(), opts...)
	if err != nil {
		return nil, err
	}
	list := make([]*pb.ShowData, 0, len(res))
	for _, item := range res {
		list = append(list, toShowData(item))
	}
	return &pb.ListShowReply{
		List:  list,
		Total: total,
	}, nil
}

func (s *ShowService) UpdateStatus(ctx context.Context, req *pb.UpdateStatusRequest) (*pb.UpdateStatusReply, error) {
	if err := s.uc.UpdateStatus(ctx, req.GetId(), req.GetStatus()); err != nil {
		return nil, err
	}
	return &pb.UpdateStatusReply{}, nil
}

func toCreateShowReply(res *model.Show) *pb.CreateShowReply {
	return &pb.CreateShowReply{
		Id:        int64(res.ID),
		EventId:   int64(res.EventID),
		Name:      res.Name,
		ShowTime:  utils.FormatTime(res.ShowTime),
		EndTime:   utils.FormatTime(res.EndTime),
		Status:    res.Status,
		Stock:     int32(res.Stock),
		SoldCount: int32(res.SoldCount),
		SortOrder: int32(res.SortOrder),
	}
}

func toShowData(item *model.Show) *pb.ShowData {
	return &pb.ShowData{
		Id:        int64(item.ID),
		EventId:   int64(item.EventID),
		Name:      item.Name,
		ShowTime:  utils.FormatTime(item.ShowTime),
		EndTime:   utils.FormatTime(item.EndTime),
		Status:    item.Status,
		Stock:     int32(item.Stock),
		SoldCount: int32(item.SoldCount),
		SortOrder: int32(item.SortOrder),
	}
}

func (s *ShowService) GetEventShow(ctx context.Context, req *pb.GetEventShowRequest) (*pb.GetEventShowReply, error) {
	res, err := s.uc.GetByEvent(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.GetEventShowReply{
		Id:        int64(res.ID),
		EventId:   int64(res.EventID),
		Name:      res.Name,
		ShowTime:  utils.FormatTime(res.ShowTime),
		EndTime:   utils.FormatTime(res.EndTime),
		Status:    res.Status,
		Stock:     int32(res.Stock),
		SoldCount: int32(res.SoldCount),
		SortOrder: int32(res.SortOrder),
	}, nil
}
