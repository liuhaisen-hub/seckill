package service

import (
	"context"
	"fmt"

	pb "user/api/user/v1"
	"user/internal/biz"
)

type UserService struct {
	pb.UnimplementedUserServer
	uc *biz.UserUseCase
}

func NewUserService(uc *biz.UserUseCase) *UserService {
	return &UserService{
		uc: uc,
	}
}

func (s *UserService) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.CreateUserReply, error) {
	if err := s.uc.RegisterUser(ctx, req.GetUserName(), req.GetPassword(), req.GetEmali()); err != nil {
		return nil, err
	}

	return &pb.CreateUserReply{}, nil
}
func (s *UserService) UpdateUser(ctx context.Context, req *pb.UpdateUserRequest) (*pb.UpdateUserReply, error) {
	return &pb.UpdateUserReply{}, nil
}
func (s *UserService) DeleteUser(ctx context.Context, req *pb.DeleteUserRequest) (*pb.DeleteUserReply, error) {
	return &pb.DeleteUserReply{}, nil
}
func (s *UserService) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.GetUserReply, error) {
	fmt.Print(req.GetId())
	user, err := s.uc.GetUser(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.GetUserReply{
		UserName: user.Username,
		Email:    user.Email,
		Role:     user.Role,
	}, nil
}
func (s *UserService) ListUser(ctx context.Context, req *pb.ListUserRequest) (*pb.ListUserReply, error) {
	return &pb.ListUserReply{}, nil
}

func (s *UserService) GetAuthUser(ctx context.Context, req *pb.GetAuthUserRequest) (*pb.GetAuthUserReply, error) {
	user, err := s.uc.GetAuthUser(ctx, req.GetUserName(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	return &pb.GetAuthUserReply{
		Id:       int64(user.ID),
		UserName: user.Username,
		Email:    user.Email,
	}, nil
}
