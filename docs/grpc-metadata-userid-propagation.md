# 网关 userID 透传到 Kratos 下游服务教程（gRPC metadata）

> 适用工程：`sale`（go.work monorepo）
> 网关：`apps/getway`（Hertz + hertz-contrib/jwt，module `getway`）
> 下游：`apps/user`(:9000)、`apps/activity`(:9001)、`apps/market`(:9003)、`apps/seckill`(:9003)，均为 Kratos v3 + gRPC
> 版本：`google.golang.org/grpc v1.8x`、`github.com/go-kratos/kratos/v3 v3.0.0`
> 本文只给方案和代码，不直接改业务代码；落地时按“改动清单”逐文件修改。

---

## 1. 现状与问题根因

### 1.1 userID 现在在哪里

鉴权链路在 `apps/getway/mvw/jwt.go`：

1. `JwtMiddleware.MiddlewareFunc()` 校验 JWT；
2. `IdentityHandler` 把 claims 里的 `user_id` 还原成 `*mvw.Identity`；
3. 身份被写进 **Hertz 的请求对象** `*app.RequestContext`，key 为 `mvw.IdentityKey`；
4. handler 里通过 `mvw.GetUserID(c)` 取出。

调用 gRPC 的典型写法（`apps/getway/biz/handler/market/market_services.go:28`）：

```go
func CreateListing(ctx context.Context, c *app.RequestContext) {
    ...
    res, err := client.GetMarket().CreateListing(ctx, &marketv1.CreateListingRequest{
        SellerId: int64(mvw.GetUserID(c)), // userID 只能手动塞进请求体
        ...
    })
}
```

### 1.2 为什么 userID 不会“自动”到下游

Hertz handler 有两个“上下文”，必须分清：

| 对象 | 类型 | 生命周期 |
| --- | --- | --- |
| `ctx` | 标准库 `context.Context` | 可透传给 gRPC 调用，但里面现在**没有** userID |
| `c` | `*app.RequestContext`（Hertz HTTP 对象） | 只存在于本次 HTTP 请求内，**不能跨进程** |

关键事实：**Go 的 `context.Context` 只在单个进程内有效，不会随 gRPC 请求传到另一台服务器。** gRPC 跨进程传递附加信息的标准载体是 **metadata**（底层就是 HTTP/2 headers）。

所以要做到“下游 Kratos 服务里直接拿到当前登录用户 ID”，链路必须是：

```
HTTP 请求(带 JWT)
   │  网关 mvw.JwtMiddleware 校验，得到 userID
   ▼
网关进程：userID  ──写入──▶ gRPC outgoing metadata（HTTP/2 header: x-user-id）
   │  网络传输（对下游来说是 incoming metadata）
   ▼
Kratos 进程：server 中间件从 incoming metadata 读出 userID
   │  写入 context.WithValue
   ▼
service / biz 层：authctx.UserIDFromContext(ctx)
```

### 1.3 gRPC metadata 的硬规则

- key 必须**全小写**（推荐 `x-user-id`），value 都是字符串，所以 `uint64` 要 `strconv.FormatUint`；
- 二进制值要用 `-bin` 后缀 key；
- 一次 RPC 调用对应一份 metadata；
- metadata 是“信封”，和 protobuf 请求体互不影响——**不需要改任何 `.proto` 文件**。

---

## 2. 方案一：最小改动版（先跑通原理）

不引入任何新包，只改两处。

### 2.1 网关：调用前把 userID 塞进 outgoing metadata

以 `CreateListing` 为例：

```go
import (
    "context"
    "strconv"

    grpcmd "google.golang.org/grpc/metadata"
    "getway/mvw"
)

func CreateListing(ctx context.Context, c *app.RequestContext) {
    // ...BindAndValidate...

    // 关键一行：把 JWT 解析出的 userID 放进 gRPC outgoing metadata
    ctx = grpcmd.AppendToOutgoingContext(ctx,
        "x-user-id", strconv.FormatUint(mvw.GetUserID(c)), 10)

    res, err := client.GetMarket().CreateListing(ctx, &marketv1.CreateListingRequest{
        // SellerId 后续就可以不再依赖请求体传了
        TicketId: req.GetTicketId(),
        Price:    req.GetPrice(),
    })
    // ...
}
```

每个调 RPC 的 handler 加这一行即可（`user/activity/market/seckill` 全部通用）。

### 2.2 下游：从 incoming metadata 读取

任意 Kratos service 方法里：

```go
import (
    grpcmd "google.golang.org/grpc/metadata"
)

func (s *MarketplaceService) CreateListing(ctx context.Context, req *pb.CreateListingRequest) (*pb.CreateListingReply, error) {
    var sellerID uint64
    if md, ok := grpcmd.FromIncomingContext(ctx); ok {
        if raw := md.Get("x-user-id"); raw != "" {
            sellerID, _ = strconv.ParseUint(raw, 10, 64)
        }
    }
    if sellerID == 0 {
        return nil, errors.Unauthorized("UNAUTHORIZED", "未登录或登录已失效")
    }
    // 用 sellerID 而不是 req.GetSellerId()
    ...
}
```

这已经能完整跑通。但每个方法手写解析很啰嗦，且 key 字符串散落在多个模块容易写错。**正式开发建议用方案二。**

---

## 3. 方案二：工程化版（推荐）

思路：

1. 在根模块新增一个**两端共享**的小包 `sale/pkg/authctx`（常量、注入、提取、拦截器、Kratos 中间件全在里面）；
2. 网关侧用 **Hertz 中间件**把 userID 放进标准 `ctx`，再用 **gRPC 客户端 UnaryInterceptor** 统一写入 outgoing metadata——业务 handler 零改动；
3. 下游在 Kratos gRPC server 上加一个**入站中间件**，统一解析进 `ctx`；
4. service/biz 里一行 `authctx.UserIDFromContext(ctx)` 取值。

> 为什么放 `sale/pkg`：四个下游服务的 go.mod 已经依赖 `sale`（如 `sale/pkg/conf`）；网关在 `go.work` 中与根模块同 workspace，可直接 import，无需 replace。

### 3.1 新增共享包 `sale/pkg/authctx/authctx.go`

完整代码（可直接新建文件使用）：

```go
// Package authctx 负责登录用户 ID 在 HTTP 网关与 gRPC 下游服务之间的透传。
package authctx

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/v3/middleware"
	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"
)

// MetadataUserIDKey 是 gRPC metadata 中承载用户 ID 的 header 名。
// gRPC metadata key 必须全小写。这是网关与所有下游服务的唯一约定。
const MetadataUserIDKey = "x-user-id"

// ctxKey 是 userID 在进程内 context.Context 中的私有 key，避免与其他包冲突。
type ctxKey struct{}

// ---------- 进程内 context 存取（网关、下游通用） ----------

// WithUserID 把 userID 写入 context。
func WithUserID(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// UserIDFromContext 从 context 取出 userID。
func UserIDFromContext(ctx context.Context) (uint64, bool) {
	id, ok := ctx.Value(ctxKey{}).(uint64)
	return id, ok && id != 0
}

// ---------- 网关出站（getway 使用） ----------

// AppendUserID 把 userID 追加到 gRPC outgoing metadata。
// 用 Append 而不是 New：不会清掉链路上已有的 metadata（如 trace、其他 header）。
func AppendUserID(ctx context.Context, id uint64) context.Context {
	if id == 0 {
		return ctx
	}
	return grpcmd.AppendToOutgoingContext(ctx, MetadataUserIDKey, strconv.FormatUint(id, 10))
}

// ClientUnaryInterceptor 是网关侧 gRPC 客户端拦截器：
// 从 ctx 取出 userID，自动写进每次 RPC 的 outgoing metadata。
func ClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if id, ok := UserIDFromContext(ctx); ok {
			ctx = AppendUserID(ctx, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ---------- Kratos 下游入站（user/activity/market/seckill 使用） ----------

// Server 是 Kratos gRPC 服务端中间件：
// 从 incoming metadata 解析 x-user-id 并写入 ctx，后续 service/biz 直接读取。
func Server() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			md, ok := grpcmd.FromIncomingContext(ctx)
			if ok {
				if raw := md.Get(MetadataUserIDKey); raw != "" {
					if id, err := strconv.ParseUint(raw, 10, 64); err == nil && id != 0 {
						ctx = WithUserID(ctx, id)
					}
				}
			}
			return next(ctx, req)
		}
	}
}
```

落地后在根目录执行一次：

```bash
go mod tidy   # 根模块 sale：grpc 从 indirect 变 direct，并引入 kratos middleware
```

> 说明：Kratos 的 gRPC server 中间件执行时，`ctx` 中保留着原生 incoming metadata（Kratos 的 unary interceptor 内部基于该 ctx 构建 transport），所以直接用 `grpcmd.FromIncomingContext(ctx)` 一定取得到，不依赖 Kratos 封装。

### 3.2 网关：Hertz 中间件把 userID 放进标准 ctx

新建 `apps/getway/mvw/context.go`：

```go
package mvw

import (
	"context"

	"sale/pkg/authctx"
	"github.com/cloudwego/hertz/pkg/app"
)

// InjectUserID 把 JWT 解析出的 userID 注入到标准 context.Context。
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
```

把它挂到鉴权链尾部。`apps/getway/mvw/jwt.go` 中：

```go
func AuthMiddleware() []app.HandlerFunc {
	return []app.HandlerFunc{
		JwtMiddleware.MiddlewareFunc(),
		CheckAuthMiddleware(),
		InjectUserID(), // 新增：鉴权通过后注入 userID
	}
}
```

`apps/getway/biz/router/{user,market}/middleware.go` 里现有路由组不用改，它们调用的就是 `mvw.AuthMiddleware()`。公开（免登录）接口若也想带“可选用户”，可单独在全局 `customizedRegister` 之前挂 `mvw.InjectUserID()`，该中间件对 userID=0 是安全的空操作。

### 3.3 网关：四个 gRPC client 加客户端拦截器

拦截器只从 ctx 读值，**不会也不应该去读 HTTP header**。在 client 构造处统一加一次即可，所有出站 RPC 自动带上 `x-user-id`。

`apps/getway/biz/client/user.go`：

```go
conn, err := grpc.NewClient("127.0.0.1:9000",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithUnaryInterceptor(authctx.ClientUnaryInterceptor()), // 新增
)
```

`activity.go`(:9001)、`market.go`(:9003)、`seckill.go`(:9003) 同样加这一行，import：

```go
"sale/pkg/authctx"
```

网关目录补依赖（go.work 下不加 replace 也会用本地代码；若 CI 脱离 workspace 构建则必须补）：

```bash
cd apps/getway
go mod edit -require=sale@v0.0.0
go build ./...
```

完成后，**所有 handler 不用任何改动**：Hertz 注入的 ctx 原样传给 `client.GetXxx().Method(ctx, req)`，拦截器自动加 metadata。方案一里的 `AppendToOutgoingContext` 也不需要手写了。

### 3.4 下游：四个 Kratos server 加入站中间件

以 `apps/market/internal/server/grpc.go` 为例（`user/activity/seckill` 同理）：

```go
import (
    "sale/pkg/authctx"
    // ...
)

func NewGRPCServer(c *conf.Server, logger *slog.Logger, marketSvc *service.MarketplaceService) *grpc.Server {
    var opts = []grpc.ServerOption{
        grpc.Middleware(
            recovery.Recovery(),
            logging.Server(logger),
            authctx.Server(), // 新增：解析 x-user-id 到 ctx
        ),
    }
    // ...其余不变
}
```

注意事项：

- `NewGRPCServer` 的函数签名没有变化，**不需要重新跑 wire**；
- `activity` 服务有多个 service 实现，中间件是 server 级的，加一次对全部 service 生效；
- 各下游模块执行 `go mod tidy` 即可（它们本来就依赖 `sale`）。

### 3.5 service / biz 层使用

`apps/market/internal/service/...` 或 biz 层：

```go
import (
    "github.com/go-kratos/kratos/v3/errors"

    "sale/pkg/authctx"
)

func (s *MarketplaceService) BuyListing(ctx context.Context, req *pb.BuyListingRequest) (*pb.BuyListingReply, error) {
    buyerID, ok := authctx.UserIDFromContext(ctx)
    if !ok {
        // metadata 缺失/为 0：说明不是从可信网关进来的，直接拒绝
        return nil, errors.Unauthorized("UNAUTHORIZED", "未登录或登录已失效")
    }

    if err := s.uc.BuyListing(ctx, req.GetId(), int64(buyerID)); err != nil {
        return nil, err
    }
    return &pb.BuyListingReply{}, nil
}
```

迁移建议：proto 里的 `BuyerId/SellerId` 字段可以保留一段时间，但服务端逻辑只信任 ctx 中的值；待前端/网关全部切换后，再考虑从 proto 中废弃这些身份字段。

---

## 4. 多级透传（Kratos 服务 A 再调服务 B）

当前四个服务暂未互相调用。将来出现“market 内部再调 user/activity”时，userID 要逐跳继续传。两种做法：

### 4.1 手动透传（原生 grpc client，推荐，行为可控）

服务 A 收到请求时 ctx 里已有 userID（3.4 的中间件写入），发起下一跳前：

```go
if id, ok := authctx.UserIDFromContext(ctx); ok {
    ctx = authctx.AppendUserID(ctx, id)
}
resp, err := userClient.GetUser(ctx, req)
```

服务 B 同样挂 `authctx.Server()` 即可。每一跳显式声明，审计清晰。

### 4.2 Kratos metadata 中间件自动透传

Kratos 自带 `middleware/metadata`，约定 **`x-md-global-` 前缀**的 header 在服务间自动传播：

- 服务端：`metadata.Server(metadata.WithPropagatedPrefix("x-md-global-"))`
- 客户端（Kratos gRPC client）：创建连接时 `kgrpc.WithMiddleware(metadata.Client())`

如果把本方案的 key 改成 `x-md-global-user-id`：

- 网关仍用原生 grpc，直接 `AppendToOutgoingContext(ctx, "x-md-global-user-id", ...)` 即可；
- 下游用 `kratosmetadata.FromServerContext(ctx).Get("x-md-global-user-id")` 读取；
- 之后所有挂了 `metadata.Client()` 的 Kratos 出站调用自动转发，无需手写。

建议：**跨团队/大量内部调用用 4.2；当前规模用 4.1 更直观。** 无论哪种，key 一旦定了就不要改，它是跨服务契约。

---

## 5. 安全注意事项（务必阅读）

1. **网关是唯一信任边界。** userID 的唯一来源是 JWT 校验结果（`mvw.GetUserID(c)`）。严禁把入站 HTTP header（如客户端自己发的 `X-User-Id`）原样拷进 gRPC metadata——任何人都能伪造。
2. **metadata 可被直连伪造。** 现在下游监听 `127.0.0.1:900x` 且用 `insecure` 凭据。任何能直连这些端口的进程都可以自己发 `x-user-id: 1`。生产环境必须：
   - 下游端口只对内网/安全组开放，公网不可达；或
   - 上 mTLS（`grpc/credentials` + 自定义 `Peer` 校验），服务端中间件只接受网关身份；
   - 敏感操作在服务端校验 `UserIDFromContext` 成功且非 0，不能只靠请求体里的 ID。
3. **匿名接口**取不到 userID 是正常情况（`ok == false`），由业务决定允许还是拒绝；不要把 0 当成真实用户。
4. 用 `AppendToOutgoingContext` 而非 `NewOutgoingContext`：后者会替换掉整份 outgoing metadata，可能丢掉 trace 等其他头。
5. 本项目全部是 unary RPC；若以后有 streaming RPC，需要再实现 `StreamClientInterceptor` / `StreamServerInterceptor`，unary 拦截器对流式调用不生效。

---

## 6. 验证方法

### 6.1 端到端验证

1. 正常登录拿 token，请求需要鉴权的接口（如 `POST /api/marketplace`）；
2. 在下游 `authctx.Server()` 里临时加一行日志：

   ```go
   slog.InfoContext(ctx, "grpc incoming", "user_id", id, "method", ctx.Value(...))
   ```

   或直接在 service 方法打印 `authctx.UserIDFromContext(ctx)`，应与 JWT 中的用户一致。

### 6.2 绕过网关直连下游（验证接收 & 认识伪造风险）

```bash
grpcurl -plaintext \
  -H 'x-user-id: 1001' \
  -d '{"ticket_id":1,"price":100}' \
  127.0.0.1:9003 market.v1.Marketplace/CreateListing
```

能取到 `1001` 即说明接收侧正常——同时也印证了第 5 节的隔离要求。

### 6.3 不发 header

不带 `x-user-id` 调用时，`UserIDFromContext` 返回 `false`，敏感接口应返回 Unauthorized。

---

## 7. 落地改动清单

| # | 文件 | 改动 | 必须 |
| --- | --- | --- | --- |
| 1 | `pkg/authctx/authctx.go`（新增，根模块 `sale`） | 共享常量、ctx 存取、客户端拦截器、Kratos `Server()` 中间件 | 方案二必须 |
| 2 | `apps/getway/mvw/context.go`（新增） | `InjectUserID()` Hertz 中间件 | 方案二必须 |
| 3 | `apps/getway/mvw/jwt.go` | `AuthMiddleware()` 链尾追加 `InjectUserID()` | 方案二必须 |
| 4 | `apps/getway/biz/client/user.go` | `grpc.WithUnaryInterceptor(authctx.ClientUnaryInterceptor())` | 方案二必须 |
| 5 | `apps/getway/biz/client/activity.go` | 同上 | 方案二必须 |
| 6 | `apps/getway/biz/client/market.go` | 同上 | 方案二必须 |
| 7 | `apps/getway/biz/client/seckill.go` | 同上 | 方案二必须 |
| 8 | `apps/user/internal/server/grpc.go` | 中间件链加 `authctx.Server()` | 必须 |
| 9 | `apps/activity/internal/server/grpc.go` | 同上 | 必须 |
| 10 | `apps/market/internal/server/grpc.go` | 同上 | 必须 |
| 11 | `apps/seckill/internal/server/grpc.go` | 同上 | 必须 |
| 12 | 各下游 `internal/service`（按需） | `authctx.UserIDFromContext(ctx)` 取用户，替代请求体身份字段 | 业务需要时 |
| 13 | `apps/getway/go.mod` | workspace 下构建即可；脱离 workspace 的 CI 补 `require sale v0.0.0` | 视 CI 而定 |

只想快速验证时，用“方案一”两处改动即可，不需要表中 1–7。

---

## 8. FAQ / 排错

**Q：handler 里 `ctx` 明明一路传下去了，为什么下游还是拿不到？**
A：`context.Context` 不跨进程。跨进程只能靠 gRPC metadata（或请求体）。确认网关侧真的执行了 `AppendToOutgoingContext` / 客户端拦截器。

**Q：下游 `FromIncomingContext` 取到空字符串？**
A：逐项排查：① key 是否两边完全一致且全小写（`x-user-id`）；② 四个 client 是否都加了拦截器，调的是不是没改的那个；③ 调 RPC 传的 ctx 是否是被 Hertz `c.Next(ctx)` 传下来的那个 ctx（不要在中间件里 `context.Background()` 另起）；④ userID 是否为 0（未登录时拦截器会跳过）。

**Q：`c.Get(IdentityKey)` 和 `ctx.Value(...)` 有什么区别？**
A：前者是 Hertz HTTP 请求对象里的键值，仅网关 HTTP 层可见；后者是标准 ctx，可随 gRPC 出站。方案二的 `InjectUserID` 就是在两者之间架桥。

**Q：需要改 proto / 重新生成 pb 吗？**
A：不需要。metadata 独立于请求消息。

**Q：`AppendToOutgoingContext` 和 `NewOutgoingContext` 怎么选？**
A：永远优先 `Append`（叠加，保留 trace 等已有头）；`New` 会覆盖整份 metadata。

**Q：改完 `NewGRPCServer` 要重新跑 wire 吗？**
A：不用。函数入参没变化，只是内部 opts 多一项，`wire_gen.go` 不受影响。

**Q：日志里想带 userID 怎么办？**
A：在 `authctx.Server()` 之后、业务之前记录；或封装一个 Kratos logging 的 Valuer，从 ctx 取 userID 输出。注意别把 userID 以外的敏感 metadata 全量打日志。
