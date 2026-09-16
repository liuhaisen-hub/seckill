package data

import (
	"context"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"activity/internal/biz"
)

type ticketTypeRepo struct {
	data   *Data
	logger *slog.Logger
}

//	type TicketType struct {
//		gorm.Model
//		EventID uint    `gorm:"not null;index"` // 外键索引，查「某活动的所有票种」必备
//		Name    string  `gorm:"not null"`
//		Price   float64 `gorm:"not null"`
//		// Stock 是数据库中的权威库存。
//		// ⚠️ 注意：秒杀时的实时扣减发生在 Redis，这里是异步落库时才更新，
//		//    两者存在秒级不一致，这是「最终一致性」的取舍（第 11、12 章详解）。
//		Stock      int `gorm:"not null;default:0"`
//		MaxPerUser int `gorm:"not null;default:1"` // 限购，防黄牛的第一道闸
//		SortOrder  int `gorm:"default:0"`          // 前端展示排序
//	}
func NewTicketTypeRepo(data *Data, logger *slog.Logger) biz.TicketTypeRepo {
	return &ticketTypeRepo{
		data:   data,
		logger: logger,
	}
}

func (r *ticketTypeRepo) Create(ctx context.Context, tp *model.TicketType) error {
	return r.data.db.WithContext(ctx).Create(&tp).Error
}

func (r *ticketTypeRepo) Updates(ctx context.Context, tp *model.TicketType) (*model.TicketType, error) {
	err := r.data.db.WithContext(ctx).Model(&model.TicketType{}).Where("id = ?", uint(tp.ID)).Updates(&tp).Error
	if err != nil {
		return nil, err
	}
	return tp, nil
}

func (r *ticketTypeRepo) Delete(ctx context.Context, ID int64) error {
	return r.data.db.WithContext(ctx).Model(&model.TicketType{}).Delete("id = ?", uint(ID)).Error
}

func (r *ticketTypeRepo) Get(ctx context.Context, ID int64) (*model.TicketType, error) {
	var tp model.TicketType
	err := r.data.db.WithContext(ctx).Where("id = ?", uint(ID)).First(&tp).Error
	if err != nil {
		return nil, err
	}
	return &tp, nil
}
func (r *ticketTypeRepo) List(ctx context.Context, opts ...utils.ListOption) ([]*model.TicketType, int64, error) {
	o := utils.NewListOptions(opts...)
	var total int64
	tx := r.data.db.WithContext(ctx).Model(&model.TicketType{})
	for field, value := range o.Filters {
		tx = tx.Where(field+" = ?", value)
	}
	for _, order := range o.OrderBy {
		tx = tx.Order(order)
	}
	if err := tx.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []*model.TicketType
	if err := tx.Offset(o.Offset).Limit(o.Limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *ticketTypeRepo) FindByEvent(ctx context.Context, ID int64) ([]*model.TicketType, error) {
	var list []*model.TicketType
	err := r.data.db.WithContext(ctx).Model(&model.TicketType{}).Where("event_id = ?", uint(ID)).Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}
