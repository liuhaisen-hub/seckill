package biz

import (
	"context"
	"fmt"
	"log/slog"
	"sale/pkg/utils"
	"user/internal/dao"
)

type UserRepo interface {
	CreateUser(ctx context.Context, userName, password, sercet, email string) error
	GetUser(ctx context.Context, ID int64) (*dao.User, error)
	GetUserByName(ctx context.Context, username string) (*dao.User, error)
}

type UserUseCase struct {
	repo   UserRepo
	logger *slog.Logger
}

func NewUserUseCase(repo UserRepo, logger *slog.Logger) *UserUseCase {
	return &UserUseCase{
		repo:   repo,
		logger: logger,
	}
}

func (u *UserUseCase) RegisterUser(ctx context.Context, username, password, email string) error {
	salt, err := utils.GenerateSalt(12)
	if err != nil {
		u.logger.Error("create salt error")
		return err
	}
	// 加密密码
	pwd := utils.CreatePasswordHash(password, salt)
	return u.repo.CreateUser(ctx, username, pwd, salt, email)
}

func (u *UserUseCase) GetAuthUser(ctx context.Context, username, password string) (*dao.User, error) {
	usr, err := u.repo.GetUserByName(ctx, username)
	if err != nil {
		return nil, err
	}
	// 校验密码
	bool := utils.VerifyPassword(password, usr.Salt, usr.Password)
	if !bool {
		return nil, fmt.Errorf("密码错误")
	}
	return usr, nil

}

func (u *UserUseCase) GetUser(ctx context.Context, ID int64) (*dao.User, error) {
	return u.repo.GetUser(ctx, ID)
}
