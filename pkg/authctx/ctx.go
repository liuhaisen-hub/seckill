package authctx

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/v3/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	grpcmd "google.golang.org/grpc/metadata"
)

// MetadataUserIDKey 是 gRPC metadata 中承载用户 ID 的 header 名。
// gRPC metadata key 必须全小写。这是网关与所有下游服务的唯一约定。
const MetadataUserIDKey = "x-user-id"

// ctxKey 是 userID 在进程内 context.Context 中的私有 key，避免与其他包冲突。
type ctxKey struct{}

// 将用户id注入
func WithUserID(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// 取出用户id
func UserIdFormContext(ctx context.Context) (uint64, bool) {
	id, ok := ctx.Value(ctxKey{}).(uint64)
	return id, ok && id != 0
}

// 网关层将userID 注入 到grpc到 metata
func AppendUserID(ctx context.Context, id uint64) context.Context {
	if id == 0 {
		// 不注入
		return ctx
	}
	return grpcmd.AppendToOutgoingContext(ctx, MetadataUserIDKey, strconv.FormatUint(id, 10))
}

// 网关侧实现拦截
func ClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id, ok := UserIdFormContext(ctx); ok {
			ctx = AppendUserID(ctx, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ContextMiddleware 返回 middleware.Middleware
func ContextMiddleware() middleware.Middleware {
	// Middleware = func(next middleware.Handler) middleware.Handler
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			md, ok := metadata.FromIncomingContext(ctx)
			if ok {
				raw := md.Get(MetadataUserIDKey)
				if len(raw) > 0 {
					id, err := strconv.ParseUint(raw[0], 10, 64)
					if err == nil && id != 0 {
						ctx = WithUserID(ctx, id)
					}
				}
			}
			// 调用下一层 handler
			return next(ctx, req)
		}
	}
}
