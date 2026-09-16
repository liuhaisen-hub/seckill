package dao

import "gorm.io/gorm"

// User 用户表。
// gorm.Model 内嵌了 4 个字段：
//
//	ID uint (主键)、CreatedAt、UpdatedAt、DeletedAt (软删除)
//
// DeletedAt 带索引，所有查询会自动追加 WHERE deleted_at IS NULL。
type User struct {
	gorm.Model
	Username string `gorm:"unique;not null;column:user_name" json:"user_name"`
	// json:"-" 极其重要：确保密码哈希永远不会被序列化进 API 响应。
	// 这是「默认安全」——就算某个 handler 手滑直接 c.JSON(user)，密码也不会泄露。
	Password string `gorm:"not null" json:"-"`
	Email    string `gorm:"unique;not null" json:"email"`
	TenantID int64  `gorm:"not null; defaut:1"`
	Salt     string `gorm:"not null"`
	Role     string `gorm:"default:user" json:"role"` // user / admin
}
