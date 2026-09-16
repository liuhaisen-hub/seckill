package model

import (
	"time"

	"gorm.io/gorm"
)

// TicketTransfer 票务转让记录（转赠 or 二手交易）
type TicketTransfer struct {
	gorm.Model
	TicketID   uint `gorm:"not null;index"`
	FromUserID uint `gorm:"not null;index"`
	ToUserID   uint `gorm:"not null;index"`
	// 转让必须审核：这是反黄牛的关键控制点，
	// 平台可以据此拦截「同一人短时间内大量转出」的异常行为。
	Status       string `gorm:"not null;default:pending;index"` // pending/approved/rejected
	TransferType string `gorm:"not null;default:gift;index"`    // gift(转赠) / marketplace(二手)
	Price        float64
	Reason       string
	ReviewedBy   uint
	// 用指针类型 *time.Time：因为「未审核」时该字段应为 NULL 而不是零值时间。
	// 若用 time.Time，未审核记录会存成 '0001-01-01 00:00:00'，
	// 既浪费空间，也让 "WHERE reviewed_at IS NULL" 这类查询失效。
	ReviewedAt *time.Time
}

// MarketplaceListing 二手市场挂牌
type MarketplaceListing struct {
	gorm.Model
	TicketID    uint    `gorm:"not null;index"`
	SellerID    uint    `gorm:"not null;index"`
	Price       float64 `gorm:"not null"`
	Status      string  `gorm:"not null;default:active;index"` // active/sold/cancelled
	BuyerID     uint
	Description string `gorm:"type:text"`
}

// PromoCode 促销码
type PromoCode struct {
	gorm.Model
	Code          string  `gorm:"uniqueIndex;not null"` // 用户输入的码，唯一
	EventID       uint    `gorm:"index"`                // 0 表示全场通用
	DiscountType  string  `gorm:"not null"`             // percent(百分比) / fixed(固定金额)
	DiscountValue float64 `gorm:"not null"`
	MinAmount     float64 `gorm:"default:0"` // 满减门槛
	MaxUses       int     `gorm:"default:0"` // 0 = 不限次数
	UsedCount     int     `gorm:"default:0"`
	StartTime     time.Time
	EndTime       time.Time
	IsActive      bool `gorm:"default:true"`
}
