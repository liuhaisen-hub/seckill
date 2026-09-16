package mvw

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/hertz-contrib/jwt"
)

// JwtMiddleware 全局 JWT 中间件实例。
var JwtMiddleware *jwt.HertzJWTMiddleware

// IdentityKey 与 getway 保持一致。
const IdentityKey = "identity"

// Identity 与 getway 的定义保持一致（user_id claim）。
type Identity struct {
	UserID uint64
}

// getJWTKey 密钥必须与 getway 完全一致：同一把密钥才能校验 getway 签发的 token。
func getJWTKey() []byte {
	if key := os.Getenv("JWT_SECRET_KEY"); key != "" {
		return []byte(key)
	}
	hlog.Warn("JWT_SECRET_KEY 未设置，使用内置开发密钥，请勿用于生产")
	return []byte("taie-backend-client")
}

func InitJwt() {
	var err error
	JwtMiddleware, err = jwt.New(&jwt.HertzJWTMiddleware{
		Realm: "taie",
		Key:   getJWTKey(),
		// 关键差异：WS 握手无法自定义 Header，token 优先从 query 上取：
		//   ws://host/ws?token=xxx
		TokenLookup:   "query: token, header: Authorization, cookie: jwt",
		TokenHeadName: "Bearer",
		IdentityKey:   IdentityKey,
		Timeout:       2 * time.Hour,
		MaxRefresh:    7 * 24 * time.Hour,

		// 与 getway 完全一致的 claims 结构
		PayloadFunc: func(data any) jwt.MapClaims {
			switch v := data.(type) {
			case *Identity:
				return jwt.MapClaims{"user_id": v.UserID}
			case Identity:
				return jwt.MapClaims{"user_id": v.UserID}
			}
			return jwt.MapClaims{}
		},
		IdentityHandler: func(ctx context.Context, c *app.RequestContext) interface{} {
			claims := jwt.ExtractClaims(ctx, c)
			identity := &Identity{}
			if val, ok := claims["user_id"].(float64); ok {
				identity.UserID = uint64(val)
			}
			return identity
		},
		// 本服务只校验不签发，Authenticator 不会被走到
		Authenticator: func(ctx context.Context, c *app.RequestContext) (any, error) {
			return nil, jwt.ErrFailedAuthentication
		},
		Unauthorized: func(ctx context.Context, c *app.RequestContext, code int, message string) {
			// 握手阶段的鉴权失败直接回 401，客户端看到后应引导重新登录
			c.JSON(http.StatusUnauthorized, utils.H{"code": code, "msg": message})
		},
		HTTPStatusMessageFunc: func(e error, ctx context.Context, c *app.RequestContext) string {
			hlog.CtxErrorf(ctx, "jwt err: %v", e)
			return e.Error()
		},
	})
	if err != nil {
		panic(err)
	}
}

func CheckAuthMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		val, exist := c.Get(IdentityKey)
		if !exist {
			c.JSON(200, utils.H{
				"code": 401,
				"msg":  "未授权",
			})
			c.Abort() // 终止后续执行
			return
		}
		if _, ok := val.(*Identity); !ok {
			c.JSON(200, utils.H{
				"code": 401,
				"msg":  "身份信息错误",
			})
			c.Abort()
			return
		}
		c.Next(ctx)
	}
}

func AuthMiddleware() []app.HandlerFunc {
	return []app.HandlerFunc{
		JwtMiddleware.MiddlewareFunc(),
		CheckAuthMiddleware(),
	}
}

// GetUserID 鉴权通过后从 RequestContext 取出用户 ID。
func GetUserID(c *app.RequestContext) uint64 {
	val, exist := c.Get(IdentityKey)
	if !exist {
		return 0
	}
	identity, ok := val.(*Identity)
	if !ok {
		return 0
	}
	return identity.UserID
}
