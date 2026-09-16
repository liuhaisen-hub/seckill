package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"seckill/internal/biz"
	"seckill/internal/common"
	"time"

	"github.com/redis/go-redis/v9"
)

// 专门处理redis扣减逻辑
type rdbRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewRdbRepo(data *Data, logger *slog.Logger) biz.RdbRepo {
	return &rdbRepo{
		data:   data,
		logger: logger,
	}
}

// 秒杀扣件的lua脚本. -1 失败 1 成功 2 已经购买过
var seckillDeductScript = redis.NewScript(`
 -- 库存hash key
  local stock_key = KEYS[1]
  -- 用户已购集合key
  local bought_key = KEYS[2]
  -- 商品id
  local product_id = ARGV[1]
  -- 用户id
  local user_id = ARGV[2]

  -- 检查库存
  local stock = tonumber(redis.call('HGET', stock_key, product_id))
  if stock == nil or stock <= 0 then 
    return -1
  end

  -- 检查用户是否已经购买
  if redis.call('SISMEMBER', bought_key, user_id) == 1 then 
    return 2
  end   

  -- 扣减库存
  redis.call('HINCRBY', stock_key, product_id, -1)
  -- 记录用户购买
  redis.call('SADD', bought_key, user_id)    

  return 1
`)

// 回滚
var seckillRollBackScript = redis.NewScript(`
   local stock_key = KEYS[1]
   local bought_key = KEYS[2]
   local product_id = ARGV[1]
   local user_id = ARGV[2]
   -- 先判断用户是否购买过
   if redis.call('SREM', bought_key, user_id) == 1 then 
     redis.call('HINCRBY',stock_key, product_id,1)
	 retunr 1
   end
   return 0 
`)

// 秒杀扣减
func (r *rdbRepo) SeckillDeduct(ctx context.Context, activityID, productID, userID string) (int64, error) {
	res, err := seckillDeductScript.Run(ctx, r.data.rdb,
		[]string{
			"seckill:{" + activityID + "}:stock",
			"seckill:{" + activityID + "}:bought",
		},
		productID, userID,
	).Int64()
	return res, err
}

// 回滚操作
func (r *rdbRepo) SeckillRollback(ctx context.Context, activityID, productID, userID string) error {
	_, err := seckillRollBackScript.Run(ctx, r.data.rdb,
		[]string{
			"seckill:{" + activityID + "}:stock",
			"seckill:{" + activityID + "}:bought",
		},
		productID, userID,
	).Result()
	return err
}

// 初始化库存
func (r *rdbRepo) InitSeckillStock(ctx context.Context, activityID, productID string, stock int) error {
	key := "seckill:{" + activityID + "}:stock"
	return r.data.rdb.HSet(ctx, key, productID, stock).Err()
}

// 获取秒杀库存
func (r *rdbRepo) GetSeckillStock(ctx context.Context, activityID string, productID string) (int, error) {
	key := "seckill:{" + activityID + "}:stock"
	stock, err := r.data.rdb.HGet(ctx, key, productID).Int()
	if err == redis.Nil {
		return 0, nil
	}
	return stock, err
}

// SetSeckillActivity 设置秒杀活动信息
func (r *rdbRepo) SetSeckillActivity(ctx context.Context, activityID string, info map[string]any) error {
	key := "seckill:{" + activityID + "}:info"
	return r.data.rdb.HSet(ctx, key, info).Err()
}
func (r *rdbRepo) GetSeckillActivity(ctx context.Context, activityID string) (map[string]string, error) {
	key := "seckill:{" + activityID + "}:info"
	return r.data.rdb.HGetAll(ctx, key).Result()
}

// XAdd 添加消息到 Stream
func (r *rdbRepo) XAdd(ctx context.Context, stream string, values map[string]interface{}) error {
	return r.data.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Err()
}

// XReadGroup 从 Stream 读取消息
func (r *rdbRepo) XReadGroup(ctx context.Context, group, consumer string, streams []string, count int64, block time.Duration) ([]redis.XStream, error) {
	return r.data.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    count,
		Block:    block,
	}).Result()
}

// XAck 确认消息
func (r *rdbRepo) XAck(ctx context.Context, stream, group string, IDs ...string) error {
	return r.data.rdb.XAck(ctx, stream, group, IDs...).Err()
}

func (r *rdbRepo) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	return r.data.rdb.SetNX(ctx, key, value, ttl).Result()
}

func (r *rdbRepo) Get(ctx context.Context, key string) (string, error) {
	return r.data.rdb.Get(ctx, key).Result()
}

func (r *rdbRepo) Del(ctx context.Context, key string) (int64, error) {
	return r.data.rdb.Del(ctx, key).Result()
}
func (r *rdbRepo) WireResult(ctx context.Context, result *common.TicketResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal ticket result: %w", err)
	}
	key := common.PurchaseResultKey(result.UserID, result.EventID, result.TicketTypeID, result.Timestamp)
	if err := r.data.rdb.Set(ctx, key, payload, common.TicketResultTTL).Err(); err != nil {
		return fmt.Errorf("write result key %s: %w", key, err)
	}
	if err := r.data.rdb.Publish(ctx, common.SeckillResultChannel, payload).Err(); err != nil {
		return fmt.Errorf("publish seckill result: %w", err)
	}
	return nil
}
func (r *rdbRepo) GetPurchaseResult(ctx context.Context, userID, eventID, ticketTypeID uint, timestamp int64) (*common.TicketResult, error) {
	key := common.PurchaseResultKey(userID, eventID, ticketTypeID, timestamp)
	val, err := r.data.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result common.TicketResult
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (r *rdbRepo) Exists(ctx context.Context, key string) (int64, error) {
	return r.data.rdb.Exists(ctx, key).Result()
}
