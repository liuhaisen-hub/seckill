package biz

import (
	"context"
	"seckill/internal/common"

	"time"

	"github.com/redis/go-redis/v9"
)

// 专门处理redis扣减业务

type RdbRepo interface {
	SeckillDeduct(ctx context.Context, activityID, productID, userID string) (int64, error)
	SeckillRollback(ctx context.Context, activityID, productID, userID string) error
	InitSeckillStock(ctx context.Context, activityID, productID string, stock int) error
	GetSeckillStock(ctx context.Context, activityID string, productID string) (int, error)
	SetSeckillActivity(ctx context.Context, activityID string, info map[string]any) error
	GetSeckillActivity(ctx context.Context, activityID string) (map[string]string, error)
	XAdd(ctx context.Context, stream string, values map[string]interface{}) error
	XReadGroup(ctx context.Context, group, consumer string, streams []string, count int64, block time.Duration) ([]redis.XStream, error)
	XAck(ctx context.Context, stream, group string, IDs ...string) error
	WireResult(ctx context.Context, result *common.TicketResult) error
	GetPurchaseResult(ctx context.Context, userID, eventID, ticketTypeID uint, timestamp int64) (*common.TicketResult, error)
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, key string) (int64, error)
	Exists(ctx context.Context, key string) (int64, error)
}
