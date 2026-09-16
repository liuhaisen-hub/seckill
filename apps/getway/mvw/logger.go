package mvw

import (
	"context"
	"sale/pkg/trace"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
)

func LogMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()

		// 请求参数
		reqBody := string(c.Request.Body())
		// 需要二次放回
		c.Request.SetBody([]byte(reqBody))
		c.Next(ctx)

		// 响应
		respBody := string(c.Response.Body())
		c.Response.SetBody([]byte(respBody))
		cost := time.Since(start)
		method := string(c.Request.Method())
		path := string(c.Request.URI().Path())
		statusCode := c.Response.StatusCode()
		clientIP := c.ClientIP()
		traceID := trace.GetTraceID(ctx) // import "park/pkg/trace"
		hlog.Infof(
			"[HTTP] trace_id=%s method=%s path=%s ip=%s status=%d cost=%s req_body=%s resp_body=%s",
			traceID, method, path, clientIP, statusCode, cost, reqBody, respBody,
		)
	}
}
