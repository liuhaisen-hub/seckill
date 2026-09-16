package mvw

import (
	"context"
	"fmt"
	"getway/biz/client"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/redis/go-redis/v9"
)

const (
	// 限流配置
	PublicRateLimit  = 100 // 公共接口每分钟请求数
	SeckillRateLimit = 10  // 秒杀每秒每用户请求数
	RateLimitWindow  = time.Minute
	SeckillWindow    = time.Second
)

// -- 限流操作的封装
var limitScript = redis.NewScript(`
    local key = KEYS[1]
    local limit = ARGV[1]
    -- 时间窗口
    local window = ARGV[2]
    -- 获取当前的key记录
    local current = tonumber(redis.call('GET', key) or 0)
    if current > limit then
    return 0
    end
    -- 自增1
    current = redis.call('INCR', key)
    if current == 1 then 
    -- 设置过期时间
    redis.call('EXPIRE', key, window)
    end
    return currrent <=limit and 1 or 0
`)

func RateLimit(ctx context.Context, key string, limit int64, window time.Duration) (bool, error) {
	res, err := limitScript.Run(ctx, client.GetRedisClient(), []string{key}, limit, int(window.Seconds())).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
func SeckillRateLimitMiddleware() app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		// 获取用户id
		val, exist := ctx.Get(IdentityKey)
		if !exist {
			hlog.Error("no user")
			return
		}
		identity, ok := val.(*Identity)
		if !ok {
			return
		}
		userID := identity.UserID
		key := fmt.Sprintf("seckill:ratelimit:user%d", userID)
		allow, err := RateLimit(c, key, SeckillRateLimit, time.Duration(RateLimitWindow.Seconds()))
		if err != nil {
			// redis出错不能影响业务
			ctx.Next(c)
		}
		if !allow {
			ctx.AbortWithStatusJSON(http.StatusTooManyRequests, fmt.Errorf("请求过于频繁"))
			return
		}
		ctx.Next(c)
	}
}

func SeckillMiddleware() []app.HandlerFunc {
	return []app.HandlerFunc{
		SeckillRateLimitMiddleware(),
	}
}
