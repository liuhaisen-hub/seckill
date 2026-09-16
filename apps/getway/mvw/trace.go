package mvw

import (
	"context"
	pkgtrace "sale/pkg/trace"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// 全局唯一
var tracerProvider *pkgtrace.TracerProvider

// 初始化全
func InitTrace() {
	tp, err := pkgtrace.NewTracerProvier(&pkgtrace.Config{
		Enable:      true,
		ServiceName: "getway",
		Endpoint:    "",
		SampleRate:  1.0,
	})
	if err != nil {
		hlog.Error(err.Error())
		return
	}
	tracerProvider = tp
	hlog.Info("trace init")
}

// 停止
func ShutdownTrace() {
	if tracerProvider != nil {
		_ = tracerProvider.Shutdown(context.Background())
	}
}

// hertzHeaderCarrier 把 Hertz 的请求头适配成 OTel 的 TextMapCarrier。
// OTel 只认 Get/Set/Keys 三个方法，与具体框架解耦。
type hertzHeaderCarrier struct{ c *app.RequestContext }

func (h hertzHeaderCarrier) Get(key string) string {
	return string(h.c.Request.Header.Peek(key))
}

func (h hertzHeaderCarrier) Set(key, value string) {
	h.c.Request.Header.Set(key, value)
}

func (h hertzHeaderCarrier) Keys() []string {
	keys := make([]string, 0, 8)
	h.c.Request.Header.VisitAll(func(k, _ []byte) {
		keys = append(keys, string(k))
	})
	return keys
}

var _ propagation.TextMapCarrier = hertzHeaderCarrier{}

// TracingMiddleware 入站追踪中间件。
func TracingMiddleware() app.HandlerFunc {
	tracer := otel.Tracer("getway-http")
	prop := otel.GetTextMapPropagator()

	return func(ctx context.Context, c *app.RequestContext) {
		// ① 提取入站头：外部若带 traceparent 则加入其链路；否则 Extract 得到空 SpanContext，
		//    Start 会开一条全新的根 Trace。
		ctx = prop.Extract(ctx, hertzHeaderCarrier{c: c})

		// Span 名优先用路由模板（/login 而非带参数的 /users/123），避免基数爆炸。
		route := c.FullPath()
		if route == "" {
			route = string(c.Request.URI().Path())
		}
		method := string(c.Request.Method())

		// ② 开 Server Span，返回装着 Span 的新 ctx。
		var span trace.Span
		ctx, span = tracer.Start(ctx, method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPMethodKey.String(method),
				semconv.HTTPTargetKey.String(string(c.Request.URI().RequestURI())),
				semconv.HTTPRouteKey.String(route),
			),
		)
		defer span.End()

		// ③ 方便排障：把 TraceID 回写到响应头，curl/前端立即可拿。
		if sc := span.SpanContext(); sc.HasTraceID() {
			c.Response.Header.Set("X-Trace-Id", sc.TraceID().String())
		}

		// ④ 关键：必须把新 ctx 传给 Next！handler 里调 gRPC 用的就是这个 ctx。
		c.Next(ctx)

		// ⑤ 收尾：记录 HTTP 状态码，5xx 标记为错误。
		status := c.Response.StatusCode()
		span.SetAttributes(semconv.HTTPStatusCodeKey.Int(status))
		if status >= 500 {
			span.SetStatus(codes.Error, "internal server error")
		}
	}
}
