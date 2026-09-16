package service

import (
	"context"

	pb "market/api/market/v1"
	"market/internal/biz"
)

type MarketplaceService struct {
	pb.UnimplementedMarketplaceServer
	marketUC   *biz.MarketplaceUseCase
	transferUC *biz.TransferUseCase
}

func NewMarketplaceService(marketUC *biz.MarketplaceUseCase, transferUC *biz.TransferUseCase) *MarketplaceService {
	return &MarketplaceService{
		marketUC:   marketUC,
		transferUC: transferUC,
	}
}

func (s *MarketplaceService) CreateListing(ctx context.Context, req *pb.CreateListingRequest) (*pb.CreateListingReply, error) {
	res, err := s.marketUC.CreateListing(ctx, req.GetSellerId(), req.GetTicketId(), req.GetPrice(), req.GetDescription())
	if err != nil {
		return nil, err
	}
	return &pb.CreateListingReply{
		Id:          int64(res.ID),
		TicketId:    int64(res.TicketID),
		SellerId:    int64(res.SellerID),
		Price:       res.Price,
		Status:      res.Status,
		Description: res.Description,
		CreatedAt:   res.CreatedAt.Format("2006-01-02 15:04:05"),
	}, nil
}

func (s *MarketplaceService) GetListing(ctx context.Context, req *pb.GetListingRequest) (*pb.GetListingReply, error) {
	view, err := s.marketUC.GetListing(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.GetListingReply{Listing: toListingData(view)}, nil
}

func (s *MarketplaceService) ListActiveListings(ctx context.Context, req *pb.ListActiveListingsRequest) (*pb.ListActiveListingsReply, error) {
	views, total, err := s.marketUC.ListActiveListings(ctx, req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	return &pb.ListActiveListingsReply{List: toListingList(views), Total: total}, nil
}

func (s *MarketplaceService) ListEventListings(ctx context.Context, req *pb.ListEventListingsRequest) (*pb.ListEventListingsReply, error) {
	views, total, err := s.marketUC.ListEventListings(ctx, req.GetEventId(), req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	return &pb.ListEventListingsReply{List: toListingList(views), Total: total}, nil
}

func (s *MarketplaceService) ListMyListings(ctx context.Context, req *pb.ListMyListingsRequest) (*pb.ListMyListingsReply, error) {
	views, total, err := s.marketUC.ListMyListings(ctx, req.GetSellerId(), req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	return &pb.ListMyListingsReply{List: toListingList(views), Total: total}, nil
}

func (s *MarketplaceService) ListMyPurchases(ctx context.Context, req *pb.ListMyPurchasesRequest) (*pb.ListMyPurchasesReply, error) {
	views, total, err := s.marketUC.ListMyPurchases(ctx, req.GetBuyerId(), req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	return &pb.ListMyPurchasesReply{List: toListingList(views), Total: total}, nil
}

func (s *MarketplaceService) BuyListing(ctx context.Context, req *pb.BuyListingRequest) (*pb.BuyListingReply, error) {
	if err := s.marketUC.BuyListing(ctx, req.GetId(), req.GetBuyerId()); err != nil {
		return nil, err
	}
	return &pb.BuyListingReply{Message: "购买成功"}, nil
}

func (s *MarketplaceService) CancelListing(ctx context.Context, req *pb.CancelListingRequest) (*pb.CancelListingReply, error) {
	if err := s.marketUC.CancelListing(ctx, req.GetId(), req.GetSellerId()); err != nil {
		return nil, err
	}
	return &pb.CancelListingReply{Message: "下架成功"}, nil
}

func (s *MarketplaceService) ListTransferHistory(ctx context.Context, req *pb.ListTransferHistoryRequest) (*pb.ListTransferHistoryReply, error) {
	list, total, err := s.transferUC.ListHistory(ctx, req.GetUserId(), req.GetPage(), req.GetSize())
	if err != nil {
		return nil, err
	}
	data := make([]*pb.TransferHistoryData, 0, len(list))
	for _, item := range list {
		data = append(data, &pb.TransferHistoryData{
			Id:           int64(item.ID),
			TicketId:     int64(item.TicketID),
			FromUserId:   int64(item.FromUserID),
			ToUserId:     int64(item.ToUserID),
			Status:       item.Status,
			TransferType: item.TransferType,
			Price:        item.Price,
			Reason:       item.Reason,
			CreatedAt:    item.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}
	return &pb.ListTransferHistoryReply{List: data, Total: total}, nil
}

// toListingData 把挂牌读模型转成 DTO，反向转换内联在返回处。
func toListingData(view *biz.ListingView) *pb.ListingData {
	return &pb.ListingData{
		Id:          int64(view.ID),
		TicketId:    int64(view.TicketID),
		EventId:     int64(view.EventID),
		EventTitle:  view.EventTitle,
		TicketName:  view.TicketName,
		SellerId:    int64(view.SellerID),
		Price:       view.Price,
		Status:      view.Status,
		BuyerId:     int64(view.BuyerID),
		Description: view.Description,
		CreatedAt:   view.CreatedAt.Format("2006-01-02 15:04:05"),
	}
}

func toListingList(views []*biz.ListingView) []*pb.ListingData {
	list := make([]*pb.ListingData, 0, len(views))
	for _, view := range views {
		list = append(list, toListingData(view))
	}
	return list
}
