package data

import (
	"context"
	"fmt"
	"log/slog"
	"sale/model"
	"seckill/internal/biz"
	"time"

	"gorm.io/gorm"
)

const (
	// 限流配置
	PublicRateLimit  = 100 // 公共接口每分钟请求数
	SeckillRateLimit = 10  // 秒杀每秒每用户请求数
	RateLimitWindow  = time.Minute
	SeckillWindow    = time.Second
)

type seckillRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewSkillRepo(data *Data, logger *slog.Logger) biz.SeckillRepo {
	return &seckillRepo{
		data:   data,
		logger: logger,
	}
}

func (r *seckillRepo) GetEvent(ctx context.Context, ID int64) (*model.Event, error) {
	var event model.Event
	if err := r.data.db.WithContext(ctx).Where("id = ?", uint(ID)).First(&event).Error; err != nil {
		return nil, err
	}
	return &event, nil
}

func (r *seckillRepo) GetTickTypeById(ctx context.Context, ticketTypeID int64) (*model.TicketType, error) {
	var ticketType model.TicketType
	if err := r.data.db.WithContext(ctx).Where("id = ?", ticketTypeID).First(&ticketType).Error; err != nil {
		return nil, err
	}
	return &ticketType, nil
}

// 原子扣减库存
func (r *seckillRepo) AtomicDeuctStock(ctx context.Context, id int64, quantity int) error {
	result := r.data.db.WithContext(ctx).Model(&model.TicketType{}).Where("id = ? AND stock > ?", quantity).Update("stock", gorm.Expr("stock - ?", quantity))
	if result.RowsAffected == 0 {
		return fmt.Errorf("库存不足")
	}
	return result.Error
}
func (r *seckillRepo) UpdateStock(ctx context.Context, ID int64, quantity int) error {
	return r.data.db.WithContext(ctx).Model(&model.TicketType{}).Where("id = ?", ID).Update("quantity", quantity).Error
}
func (r *seckillRepo) CreateTicket(ctx context.Context, ticket *model.Ticket) error {
	return r.data.db.WithContext(ctx).Model(&model.Ticket{}).Create(&ticket).Error
}

// 通过事务扣减库存和创建ticket订单
func (r *seckillRepo) DeuctStockTransaction(ctx context.Context, ticketTypeID int64, quantity int, ticket *model.Ticket) (bool, error) {
	var bussinessError bool
	err := r.data.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.TicketType{}).Where("id = ?", ticketTypeID).Update("quantity", quantity)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			// 未执行成功,业务错误
			bussinessError = true
			return fmt.Errorf("未扣减库存成功")
		}
		// 最后再增加ticket的插入
		return r.data.db.WithContext(ctx).Model(&model.Ticket{}).Create(&ticket).Error
	})
	return bussinessError, err
}
