package model

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Event 活动（演唱会、话剧、球赛……）
type Event struct {
	gorm.Model
	Title       string `gorm:"not null"`
	Description string `gorm:"type:text"` // 富文本，可能很长，用 text 而非 varchar
	Location    string `gorm:"not null"`
	CoverImage  string
	// StartTime/EndTime 加索引：首页「近期活动」按时间范围查询是最高频的读操作
	StartTime time.Time `gorm:"not null;index"`
	EndTime   time.Time `gorm:"not null;index"`
	// Status 加索引：几乎所有列表查询都带 WHERE status = 'on_sale'
	Status     string `gorm:"default:draft;not null;index"`
	TotalStock int    `gorm:"default:0"` // 各票种库存之和的冗余字段，便于列表页展示
}

// Show 场次。同一活动的不同演出时间，各自独立库存。
type Show struct {
	gorm.Model
	EventID   uint      `gorm:"not null;index"`
	Name      string    `gorm:"not null"` // 如「11月1日 19:30 场」
	ShowTime  time.Time `gorm:"not null;index"`
	EndTime   time.Time `gorm:"not null"`
	Status    string    `gorm:"default:draft;not null;index"` // draft/on_sale/off_sale/ended
	Stock     int       `gorm:"not null;default:0"`
	SoldCount int       `gorm:"default:0"` // 已售数，用于前端进度条
	SortOrder int       `gorm:"default:0"`
}

// Event的状态机，用代码守住数据的正确性
const (
	EventStatusDraft   = "draft"    // 草稿，仅管理员可见，可自由编辑
	EventStatusOnSale  = "on_sale"  // 售票中，用户可见可购买
	EventStatusOffSale = "off_sale" // 暂停售票（如临时调整票价）
	EventStatusEnded   = "ended"    // 已结束（终态）
)

var validEventStatus = map[string]bool{
	EventStatusDraft:   true,
	EventStatusOnSale:  true,
	EventStatusOffSale: true,
	EventStatusEnded:   true,
}

//	draft ──发布──► on_sale ⇄ off_sale
//	                   │          │
//	                   └──► ended ◄┘
//
// 注意：on_sale ⇄ off_sale 是双向的（可以暂停后恢复），
// 但没有任何状态能回到 draft——活动一旦发布，就不允许再改标题、时间等核心信息，
// 这是对已购票用户的契约保护。
var eventAllowedTransitions = map[string]map[string]bool{
	EventStatusDraft:   {EventStatusOnSale: true},
	EventStatusOnSale:  {EventStatusOffSale: true, EventStatusEnded: true},
	EventStatusOffSale: {EventStatusOnSale: true, EventStatusEnded: true},
	EventStatusEnded:   {},
}

func IsValidEventStatus(status string) bool {
	return validEventStatus[status]
}

func IsValidEventTransition(from, to string) error {
	if !validEventStatus[to] {
		return fmt.Errorf("invalid event status: %s", to)
	}
	transitions, ok := eventAllowedTransitions[from]
	if !ok {
		return fmt.Errorf("invalid current event status: %s", from)
	}
	if !transitions[to] {
		return fmt.Errorf("cannot transition event from %s to %s", from, to)
	}
	return nil
}
