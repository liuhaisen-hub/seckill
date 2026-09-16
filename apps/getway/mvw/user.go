package mvw

import (
	"context"
	"sale/pkg/authctx"

	"github.com/cloudwego/hertz/pkg/app"
)

// 必须排在 JwtMiddleware.MiddlewareFunc() 之后执行。
func InjectUserID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if id := GetUserID(c); id != 0 {
			ctx = authctx.WithUserID(ctx, id)
		}
		// 关键：把改造过的 ctx 传给后续 handler，
		// handler 签名里的 ctx 参数就带上了 userID。
		c.Next(ctx)
	}
}
