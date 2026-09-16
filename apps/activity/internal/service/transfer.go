package service

import (
	"context"

	pb "activity/api/transfer/v1"
	"activity/internal/biz"
)

type TransferService struct {
	pb.UnimplementedTransferServer
	uc *biz.TransferUseCase
}

func NewTransferService(uc *biz.TransferUseCase) *TransferService {
	return &TransferService{
		uc: uc,
	}
}

func (s *TransferService) CreateTransfer(ctx context.Context, req *pb.CreateTransferRequest) (*pb.CreateTransferReply, error) {
	return &pb.CreateTransferReply{}, nil
}
func (s *TransferService) UpdateTransfer(ctx context.Context, req *pb.UpdateTransferRequest) (*pb.UpdateTransferReply, error) {
	return &pb.UpdateTransferReply{}, nil
}
func (s *TransferService) DeleteTransfer(ctx context.Context, req *pb.DeleteTransferRequest) (*pb.DeleteTransferReply, error) {
	return &pb.DeleteTransferReply{}, nil
}
func (s *TransferService) GetTransfer(ctx context.Context, req *pb.GetTransferRequest) (*pb.GetTransferReply, error) {
	return &pb.GetTransferReply{}, nil
}
func (s *TransferService) ListTransfer(ctx context.Context, req *pb.ListTransferRequest) (*pb.ListTransferReply, error) {
	return &pb.ListTransferReply{}, nil
}
