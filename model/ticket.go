package model

import "gorm.io/gorm"

const (
	TicketStatusReserved  = "reserved"  // 已锁定，待支付（秒杀成功后的初始态）
	TicketStatusPaid      = "paid"      // 已支付
	TicketStatusUsed      = "used"      // 已核销入场
	TicketStatusExpired   = "expired"   // 超时未支付，被系统回收
	TicketStatusCancelled = "cancelled" // 用户主动取消 / 退款
)

// Ticket 票/订单，系统的核心实体。
type Ticket struct {
	gorm.Model
	UserID uint `gorm:"not null;index"`
	// 三个 ID 全部冗余存储：
	// 虽然 EventID 可以通过 TicketTypeID 反查，但那需要 JOIN。
	// 「我的票」列表是高频查询，冗余能省掉一次表连接。
	// 代价：TicketType 换绑活动时要同步更新（实际业务中不允许换绑，所以安全）。
	EventID      uint `gorm:"not null;index"`
	ShowID       uint `gorm:"index"` // 可为 0：不分场次的活动
	TicketTypeID uint `gorm:"not null;index"`

	// OrderNo 唯一索引 —— 幂等性的最后防线。
	// 即使 Kafka 重复投递、消费者幂等键失效，
	// 数据库的 UNIQUE 约束也会让第二次 INSERT 直接报错，
	// 从物理上杜绝重复订单。这叫「纵深防御」。
	OrderNo string `gorm:"uniqueIndex;not null"`

	Quantity   int `gorm:"not null;default:1"`
	TotalPrice float64
	// Status 加索引：「我的待支付订单」「超时未支付回收」都靠它过滤
	Status string `gorm:"not null;default:reserved;index"`
	QRCode string `gorm:"type:text"` // 电子票二维码内容（现场核销用）

	DiscountCode string `gorm:"index"` // 使用的促销码

	// —— 实名制字段（中国大陆演出票务的强制合规要求）——
	RealName string `gorm:"index"`
	IDCard   string `gorm:"index"` // ⚠️ 生产环境必须加密存储，见下方提示
	Phone    string `gorm:"index"`

	// —— 转让相关 ——
	TransferredTo  uint   `gorm:"index"`
	TransferStatus string `gorm:"default:none"` // none/pending/approved/rejected
}

type TicketType struct {
	gorm.Model
	EventID uint    `gorm:"not null;index"` // 外键索引，查「某活动的所有票种」必备
	Name    string  `gorm:"not null"`
	Price   float64 `gorm:"not null"`
	// Stock 是数据库中的权威库存。
	// ⚠️ 注意：秒杀时的实时扣减发生在 Redis，这里是异步落库时才更新，
	//    两者存在秒级不一致，这是「最终一致性」的取舍（第 11、12 章详解）。
	Stock      int `gorm:"not null;default:0"`
	MaxPerUser int `gorm:"not null;default:1"` // 限购，防黄牛的第一道闸
	SortOrder  int `gorm:"default:0"`          // 前端展示排序
}
