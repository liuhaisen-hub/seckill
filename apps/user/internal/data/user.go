package data

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"user/internal/biz"
	"user/internal/dao"

	"gorm.io/gorm"
)

type userRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewUserRepo(data *Data, logger *slog.Logger) biz.UserRepo {
	return &userRepo{
		data:   data,
		logger: logger,
	}
}

func (r *userRepo) CreateUser(ctx context.Context, userName, password, sercet, email string) error {
	// 开启事务，高并发下注册email 是唯一索引
	return r.data.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var user dao.User
		err := tx.Where("email =?", email).Take(&user).Error
		if err == nil {
			// 查询成功，存在
			return fmt.Errorf("该邮箱已注册")
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			// 数据库错误
			return err
		}
		// 没找到新增
		newUser := dao.User{
			Username: userName,
			Password: password,
			Email:    email,
			Salt:     sercet,
		}
		if err := tx.Create(&newUser).Error; err != nil {
			return fmt.Errorf("创建用户失败: %w", err)
		}
		return nil
	})

}

func (r *userRepo) GetUser(ctx context.Context, ID int64) (*dao.User, error) {
	var user dao.User
	err := r.data.db.WithContext(ctx).Model(dao.User{}).Where("id = ?", ID).First(&user).Error
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (r *userRepo) GetUserByName(ctx context.Context, username string) (*dao.User, error) {
	var user dao.User
	if err := r.data.db.WithContext(ctx).Model(&dao.User{}).Where("user_name = ?", username).First(&user).Error; err != nil {
		r.logger.Error(err.Error())
		return nil, err
	}
	return &user, nil
}
