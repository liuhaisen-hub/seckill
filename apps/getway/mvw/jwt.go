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

// JwtMiddleware 全局 JWT 中间件实例，由 InitJwt() 初始化。
var JwtMiddleware *jwt.HertzJWTMiddleware

// IdentityKey 存放身份信息的 key，鉴权通过后可用 c.Get(IdentityKey) 取出 *Identity。
const IdentityKey = "identity"

// Identity 写入/读出 JWT 载荷的身份信息。
type Identity struct {
	UserID uint64
}

// getJWTKey 从环境变量读密钥；缺省回退到固定值以便本地开发，生产必须设置 JWT_SECRET_KEY。
func getJWTKey() []byte {
	if key := os.Getenv("JWT_SECRET_KEY"); key != "" {
		return []byte(key)
	}
	hlog.Warn("JWT_SECRET_KEY 未设置，使用内置开发密钥，请勿用于生产")
	return []byte("taie-backend-client")
}

// InitJwt 初始化 JWT 中间件，需在 server 启动前调用。
func InitJwt() {
	var err error
	JwtMiddleware, err = jwt.New(&jwt.HertzJWTMiddleware{
		Realm:         "taie",
		Key:           getJWTKey(),
		Timeout:       2 * time.Hour,
		MaxRefresh:    7 * 24 * time.Hour,
		TokenLookup:   "header: Authorization, query: token, cookie: jwt",
		TokenHeadName: "Bearer",
		IdentityKey:   IdentityKey,

		// PayloadFunc 把身份写进 token 载荷。
		PayloadFunc: func(data any) jwt.MapClaims {
			switch v := data.(type) {
			case *Identity:
				return jwt.MapClaims{
					"user_id": v.UserID,
				}
			case Identity:
				return jwt.MapClaims{
					"user_id": v.UserID,
				}
			}
			return jwt.MapClaims{}
		},

		// IdentityHandler 从 token 载荷还原身份，鉴权通过后写入 ctx。
		IdentityHandler: func(ctx context.Context, c *app.RequestContext) interface{} {
			claims := jwt.ExtractClaims(ctx, c)
			identity := &Identity{}
			// user_id 是数字，claims拿到是 float64
			if val, ok := claims["user_id"].(float64); ok {
				identity.UserID = uint64(val)
			}
			return identity
		},

		// Authenticator 是库自带 LoginHandler 的校验钩子
		Authenticator: func(ctx context.Context, c *app.RequestContext) (any, error) {
			return nil, jwt.ErrFailedAuthentication
		},

		// Unauthorized 统一鉴权失败响应。
		Unauthorized: func(ctx context.Context, c *app.RequestContext, code int, message string) {
			c.JSON(http.StatusOK, utils.H{
				"code": code,
				"msg":  message,
			})
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
		InjectUserID(),
	}
}
func GetIdentity(c *app.RequestContext) *Identity {
	val, exist := c.Get(IdentityKey)
	if !exist {
		return nil
	}
	identity, ok := val.(*Identity)
	if !ok {
		return nil
	}
	return identity
}

func GetUserID(c *app.RequestContext) uint64 {
	identity := GetIdentity(c)
	if identity == nil {
		return 0
	}
	return identity.UserID
}
