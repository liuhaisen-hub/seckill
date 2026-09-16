# 秒杀结果 WebSocket 推送实现教程

> 目标：秒杀请求异步处理完成后，把「抢购成功 / 失败」实时推送到用户浏览器，
> 并保留轮询接口作为正确性兜底。
> 本文基于仓库当前代码（`apps/seckill` + `apps/getway`），从零搭建 `apps/ws-getway`（Hertz）。

---

## 0. 现状盘点：先看清楚缺口在哪

对照当前代码，整条链路已经有一半，缺的是「结果怎么出来」和「谁持有连接」：

```
✅ 已有：Client → getway(HTTP) → seckill(gRPC) → Redis 预扣 → Kafka(ticket.orders)
❌ 缺口1：seckill 的 Kafka consumer 从未启动 —— wire_gen.go 里没有 NewConsumerUseCase 的消费循环，
          biz/consumer.go 的 HandleMessage 没有任何调用方
❌ 缺口2：consumer 处理完没有「结果出口」—— RdbRepo.WireResult 定义了但没人调，
          而且它只写结果键、不广播
❌ 缺口3：ws-getway 是空目录
❌ 缺口4：提交秒杀的响应没带关联 ID（TicketMessage.Timestamp），
          客户端拿到「排队中」后无法知道哪条推送/查询结果属于哪次请求
❌ 缺口5：getway 没有秒杀路由，客户端根本无法通过 HTTP 提交秒杀/查询结果
          （biz/client/seckill.go 建好了连接，但没有路由使用它）
```

## 1. 目标架构

```
Client ──HTTP──► getway :8000 ──gRPC──► seckill :9002
   │                                   Redis 预扣(Lua) 
   │                                   Kafka ▼ ticket.orders
   │                                   consumer：幂等 → 事务扣减/建单
   │                                       │
   │                                       ▼ 结果出口（一次动作，两个去向）
   │                                  ① SET purchase:result:{uid}:{event}:{type}:{ts}
   │                                  ② PUBLISH seckill:results
   │                                       │
   ▼                                       ▼
Client ◄──WS── ws-getway :8001 ◄── Subscribe（广播，所有实例都收）
   │              │ 本地查「user_id → 连接」表，命中才推
   │              └ 未命中直接丢弃（best-effort）
   └──HTTP(兜底)──► getway ──► seckill GetSeckillResult
                    读结果键 ①，TTL 10 分钟
```

核心原则（为什么这样设计，见前两轮讨论，这里只列结论）：

- **seckill 不持有连接**：consumer 是消费组模型，处理消息的实例 ≠ 持有连接的实例；
  seckill 只管把结果事件广播出去，路由由持有连接的一方（ws-getway）本地完成。
- **推送走 Redis Pub/Sub，不走 Kafka 结果 topic**：ws-getway 需要「广播」语义（每实例都收），
  Kafka 消费组是「队列」语义（每条只给一个实例），形状不对。且 Redis 基础设施现成。
- **推送允许丢**：事实源是结果键（`WireResult` 写入），WS 只是降低感知延迟的加速器。
  客户端 N 秒没收到推送 → 轮询 → 业务仍然正确。

## 2. 消息契约（全局只有这一张表）

| 项目 | 值 | 定义位置 |
|---|---|---|
| 结果键 | `purchase:result:{user_id}:{event_id}:{ticket_type_id}:{timestamp}` | `seckill/internal/common.PurchaseResultKey` |
| 结果键 TTL | 10 分钟（`TicketResultTTL`，已有） | 同上 |
| Redis 频道 | `seckill:results` | 新增 `common.SeckillResultChannel` |
| 广播载荷 | `common.TicketResult` 的 JSON（含关联 ID 四元组） | `seckill/internal/common` |
| 关联 ID | `timestamp`（提交秒杀时服务端生成，随 `TicketMessage` 走完全程） | 客户端 ↔ 服务端 |
| WS 下行帧 | `{"type":"seckill_result","data":{...TicketResult...}}` / `{"type":"pong"}` | `ws-getway/contract` |
| WS 上行帧 | `{"type":"ping"}`（客户端每 25 秒一次） | 同上 |

> 注意：`common.TicketResult` 在 `seckill/internal/` 里，internal 包**不能跨模块 import**，
> 所以 ws-getway 侧用 `contract.SeckillResult` 按 json tag 镜像一份。
> **JSON 字段名就是跨服务契约，改一边必须同步另一边**（彻底做法是下沉到 `sale/model`，见 §10）。

---

## 3. 第一步（seckill）：定义契约常量与载荷

### 3.1 `internal/common/ticket.go`

改动三处：加频道常量、给 `TicketResult` 补关联字段、加按字段构造键的函数。

```go
const (
	// Kafka Topics
	TicketOrderTopic    = "ticket.orders"
	TicketOrderDLQTopic = "ticket.orders.dlq"
	TicketResultTopic   = "ticket.results"

	// Kafka Consumer Groups
	TicketOrderConsumerGroup = "ticket-order-processor"

	// Redis Pub/Sub 频道：秒杀结果广播。
	// seckill 的 consumer 处理完发广播，ws-getway 的所有实例订阅它。
	SeckillResultChannel = "seckill:results"

	// Legacy Redis Stream keys (保留兼容，后续移除)
	TicketStreamKey     = "ticket:orders"
	TicketConsumerGroup = "ticket-processor"
	TicketResultTTL     = 10 * time.Minute
	TicketOrderTTL      = 24 * time.Hour
)
```

```go
// TicketResult 票务处理结果（广播载荷 + 结果键内容 + 轮询响应的统一形状）
type TicketResult struct {
	TicketID     uint   `json:"ticket_id"`
	UserID       uint   `json:"user_id"`
	EventID      uint   `json:"event_id"`      // 新增：关联 ID
	TicketTypeID uint   `json:"ticket_type_id"` // 新增：关联 ID
	Status       string `json:"status"`        // queued / success / failed / processing
	Message      string `json:"message"`
	OrderNo      string `json:"order_no"`
	Timestamp    int64  `json:"timestamp"`     // 关联 ID：来自 TicketMessage.Timestamp
}
```

```go
// PurchaseResultKey 用关联 ID 四元组直接构造结果键。
// 轮询查询时只有这四个字段，构造不出 TicketMessage，所以单独提供。
func PurchaseResultKey(userID, eventID, ticketTypeID uint, timestamp int64) string {
	return fmt.Sprintf("purchase:result:%d:%d:%d:%d", userID, eventID, ticketTypeID, timestamp)
}

// TickeyResultKey 从消息构造结果键（委托给 PurchaseResultKey，保证两处永远不会不一致）
func TickeyResultKey(msg *TicketMessage) string {
	return PurchaseResultKey(msg.UserID, msg.EventID, msg.TicketTypeID, msg.Timestamp)
}
```

```go
// NewTicketResult 从票务消息构造结果。Timestamp 自动带过去 —— 它就是关联 ID。
// consumer 的每个终态路径都用它组装，保证广播出来的字段永远完整。
func NewTicketResult(msg *TicketMessage, status, message, orderNo string) *TicketResult {
	return &TicketResult{
		UserID:       msg.UserID,
		EventID:      msg.EventID,
		TicketTypeID: msg.TicketTypeID,
		Status:       status,
		Message:      message,
		OrderNo:      orderNo,
		Timestamp:    msg.Timestamp,
	}
}
```

---

## 4. 第二步（seckill）：实现「结果出口」

出口只有一个动作定义：**写结果键（事实源）+ PUBLISH（广播）**。
两个动作都允许失败且只记日志 —— 出口故障不能拖垮订单主流程，客户端有轮询兜底。

### 4.1 `internal/biz/rdb.go`（接口层）

替换掉原来的 `WireResult` 签名（它还没有任何调用方，直接改是安全的），并新增查询方法：

```go
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

	// WireResult 结果出口：把结果写入结果键（事实源）并 PUBLISH 广播（ws-getway 推送用）。
	// 幂等可重入：重复调用只是重复写同一把键、多发一次广播，不会产生副作用。
	WireResult(ctx context.Context, result *common.TicketResult) error

	// GetPurchaseResult 轮询兜底：按关联 ID 四元组查结果。
	// 返回 (nil, nil) 表示键不存在 —— 仍在处理中，或已超过 TTL 视为超时。
	GetPurchaseResult(ctx context.Context, userID, eventID, ticketTypeID uint, timestamp int64) (*common.TicketResult, error)

	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, key string) (int64, error)
}
```

### 4.2 `internal/data/rdb.go`（实现层）

替换 `WireResult`，新增 `GetPurchaseResult`：

```go
// WireResult 秒杀结果出口。
// ① SET purchase:result:... —— 事实源，轮询接口读这里
// ② PUBLISH seckill:results —— 广播，ws-getway 所有实例都收到，各自匹配本地连接
// 为什么 Pub/Sub 丢了不补发：推送本来就是 best-effort，正确性由结果键 + 轮询保证。
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

// GetPurchaseResult 轮询兜底查询。redis.Nil 视为「处理中」（返回 nil, nil），
// 由上层决定如何向客户端表述。
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
```

import 需要补 `"errors"` 和 `"fmt"`。

---

## 5. 第三步（seckill）：consumer 的每个终态都接上出口

`internal/biz/consumer.go` 全量替换。关键设计：

- **成功、重复消息、业务失败** 三个终态都推送（重复消息重推是刻意为之：上次处理成功
  但用户可能没收到推送，比如 ws-getway 刚重启过；幂等键保证不会重复下单）。
- **系统错误（要重试的）不推送**：消息会被 Kafka 再次投递，最终会有终态推送；
  过早发「失败」会在重试成功后变成前后矛盾。
- **notify 失败只记日志**：绝不让结果出口的失败导致消息重试。

```go
package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"
	"seckill/internal/common"

	"github.com/redis/go-redis/v9"
)

type ConsumerUseCase struct {
	repo    SeckillRepo
	rdbRepo RdbRepo
	logger  *slog.Logger
}

func NewConsumerUseCase(repo SeckillRepo, rdbRepo RdbRepo, logger *slog.Logger) *ConsumerUseCase {
	return &ConsumerUseCase{
		repo:    repo,
		rdbRepo: rdbRepo,
		logger:  logger,
	}
}

func (u *ConsumerUseCase) HandleMessage(ctx context.Context, key, value []byte) error {
	var ticketMsg common.TicketMessage
	if err := json.Unmarshal(value, &ticketMsg); err != nil {
		u.logger.Error("[Kafka] Failed to unmarshal ticket message", "error", err)
		// 解析失败不走重试
		return nil
	}

	// 收到消息
	u.logger.Info("[Kafka] Processing ticket", "user_id", ticketMsg.UserID, "event_id", ticketMsg.EventID, "ticket_type_id", ticketMsg.TicketTypeID)
	orderNo := utils.OrderNo()
	idemporentKey := common.TicketIdempotentKey(&ticketMsg)
	set, err := u.rdbRepo.SetNX(ctx, idemporentKey, orderNo, common.TicketOrderTTL)
	if err != nil {
		u.logger.Error("[Kafka] Idempotent check failed", "error", err)
		return err // 重试（此时还没有结果，不推送）
	}
	if !set {
		// 没有设置成功，就是 idempotentKey 已存在，说明这条请求之前处理成功过。
		// 仍然要推送一次：上次的结果广播用户可能没收到（如 ws-getway 重启）。
		// 幂等键保证这里只是重推结果，不会重复下单。
		order, err := u.rdbRepo.Get(ctx, idemporentKey)
		if err != redis.Nil {
			u.logger.Info("[Kafka] Duplicate message detected", "user_id", ticketMsg.UserID, "existing_order_no", order)
			u.notify(ctx, common.NewTicketResult(&ticketMsg, "success", "抢购成功", order))
			return nil
		}
	}
	ticketType, err := u.repo.GetTickTypeById(ctx, int64(ticketMsg.TicketTypeID))
	if err != nil || ticketType == nil {
		// 找不到：数据问题，正常流量在预扣阶段就该拦住，这里保持静默
		u.logger.Error("[Kafka] Ticket type not found", "ticket_type_id", ticketMsg.TicketTypeID)
		return nil
	}
	ticket := &model.Ticket{
		UserID:       ticketMsg.UserID,
		EventID:      ticketMsg.EventID,
		ShowID:       ticketMsg.ShowID,
		TicketTypeID: ticketMsg.TicketTypeID,
		Quantity:     ticketMsg.Quantity,
		TotalPrice:   float64(ticketMsg.Quantity) * ticketType.Price,
		Status:       "reserved",
		OrderNo:      orderNo,
	}
	// 通过事务扣减和新增 ticket
	bussinessError, err := u.repo.DeuctStockTransaction(ctx, int64(ticketMsg.TicketTypeID), ticketMsg.Quantity, ticket)
	if err != nil {
		// 回滚，补偿 redis 的预扣
		activityID := fmt.Sprintf("ticket:%d", ticketMsg.EventID)
		u.rdbRepo.SeckillRollback(ctx, activityID, fmt.Sprint(ticketMsg.TicketTypeID), fmt.Sprint(ticketMsg.UserID))
		// 删除幂等键
		u.rdbRepo.Del(ctx, idemporentKey)
		if bussinessError {
			// 业务失败（库存不足等）：终态，推送失败结果，用户可以重试
			u.notify(ctx, common.NewTicketResult(&ticketMsg, "failed", "抢购失败，请重试", ""))
			return nil
		}
		// 系统错误：会被 Kafka 重试，等重试出终态再推送，这里不发"失败"避免前后矛盾
		return err
	}
	u.logger.Info("[Kafka] Ticket created successfully", "ticket_id", ticket.ID, "user_id", ticketMsg.UserID, "order_no", ticket.OrderNo)
	// 成功终态：推送结果
	result := common.NewTicketResult(&ticketMsg, "success", "抢购成功", ticket.OrderNo)
	result.TicketID = ticket.ID
	u.notify(ctx, result)
	return nil
}

// notify 结果出口的唯一调用封装：出口失败只记日志。
// 结果推送是 best-effort，绝不能因为出口故障让消息进入重试循环。
func (u *ConsumerUseCase) notify(ctx context.Context, result *common.TicketResult) {
	if err := u.rdbRepo.WireResult(ctx, result); err != nil {
		u.logger.Error("[Kafka] notify result failed", "user_id", result.UserID, "error", err)
	}
}
```

> 注意 import：原文件里有 `sale/pkg/cache` 和 `sale/pkg/utils`。上面保留 `utils.OrderNo()`，
> 如果 `localCache` 字段你确实没用（当前代码声明了但没初始化），删掉它和 `cache` import。

---

## 6. 第四步（seckill）：提交响应带关联 ID

客户端提交成功后必须立刻拿到 `timestamp`，否则后面推送来了也对不上号。
`internal/biz/seckill.go` 的 `PurchaseTicket` / `PurchaseTicketAsync` 末尾，把返回值补上
`Timestamp`（`message` 变量就在上面几行，时间戳在 `NewTicketMessage` 里已生成）：

```go
	message := common.NewTicketMessage(uint(userId), event.ID, uint(ticketTypeID), int(quantity))
	data, err := json.Marshal(message)
	// ......（推送逻辑不变）......
	return &common.TicketResult{
		Status:    "queued",
		Message:   "排队中",
		Timestamp: message.Timestamp, // 关联 ID：客户端凭它匹配 WS 推送 / 轮询结果
	}, nil
```

两个方法都改。

---

## 7. 第五步（seckill）：把 Kafka consumer 真正跑起来

这是缺口 1 —— `HandleMessage` 写好了但没人喂消息。Kratos 的做法是把 consumer
包装成一个 `transport.Server`，交给 `kratos.App` 统一管理生命周期。

### 7.1 新建 `internal/server/consumer.go`

kratos v3 的 `transport.Server` 接口只需要 `Start/Stop` 两个方法（已核对 v3.0.0 源码）：

```go
package server

import (
	"context"
	"log/slog"

	"sale/pkg/conf"
	"sale/pkg/notification"
	"seckill/internal/biz"
	"seckill/internal/common"
)

// ConsumerServer 把 Kafka 消费循环包装成 Kratos 的 Server，
// 由 app.Run() 统一负责启动、收到退出信号后统一停止。
type ConsumerServer struct {
	consumer *notification.Consumer
	logger   *slog.Logger
}

func NewConsumerServer(c *conf.Data, uc *biz.ConsumerUseCase, logger *slog.Logger) (*ConsumerServer, error) {
	consumer, err := notification.NewConsumer(&notification.ConsumerConfig{
		Brokers: c.GetBrokers(),                    // 对应 config.yaml 的 data.brokers
		Topic:   common.TicketOrderTopic,           // ticket.orders
		Group:   common.TicketOrderConsumerGroup,   // ticket-order-processor
		Handler: uc.HandleMessage,                  // 上面改造好的处理逻辑
		DLQTopic: common.TicketOrderDLQTopic,       // 重试超限进死信
	})
	if err != nil {
		return nil, err
	}
	return &ConsumerServer{consumer: consumer, logger: logger}, nil
}

// Start 实现 transport.Server：异步启动消费循环，立即返回不阻塞 app 启动。
// notification.Consumer.Start 内部会在 ctx 取消时关闭 kafka client，所以 Stop 无事可做。
func (s *ConsumerServer) Start(ctx context.Context) error {
	go s.consumer.Start(ctx)
	s.logger.Info("[Kafka] consumer server started")
	return nil
}

// Stop 实现 transport.Server。
func (s *ConsumerServer) Stop(ctx context.Context) error {
	s.logger.Info("[Kafka] consumer server stopped")
	return nil
}
```

### 7.2 `internal/server/server.go`：注册进 ProviderSet

```go
var ProviderSet = wire.NewSet(NewGRPCServer, NewConsumerServer)
```

### 7.3 `cmd/seckill/main.go`：newApp 挂上第二个 server

```go
func newApp(logger *slog.Logger, gs *grpc.Server, cs *server.ConsumerServer) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
			cs, // Kafka 消费者，与 gRPC server 同一生命周期
		),
	)
}
```

### 7.4 重新生成 Wire

`cmd/seckill/wire.go` 的 `wireApp` 签名不用动（还是 `*conf.Server, *conf.Data, *slog.Logger`），
直接在 `apps/seckill` 下执行：

```bash
make generate   # = go generate + wire + go mod tidy
```

重新生成的 `wire_gen.go` 里应该能看到这一段（自查用）：

```go
	rdbRepo := data.NewRdbRepo(dataData, logger)
	consumerUseCase := biz.NewConsumerUseCase(seckillRepo, rdbRepo, logger)
	consumerServer, err := server.NewConsumerServer(confData, consumerUseCase, logger)
	// ...
	app := newApp(logger, grpcServer, consumerServer)
```

> 之前 `NewConsumerUseCase` 虽然在 `biz.ProviderSet` 里，但没人消费它，Wire 根本不会实例化；
> 现在 `NewConsumerServer` 依赖它，链路就通了。

### 7.5 `configs/config.yaml`：修正两个配置问题

当前配置有两个坑（对照 `pkg/conf` proto 核对过）：

1. `mq.broker: []` —— proto 里根本没有这个字段，`conf.Data.Brokers` 对应的路径是 `data.brokers`；
   brokers 为空时 `notification.NewProducer` 会失败且 `NewSeckillUseCase` 只打日志不阻断，
   真正下单时会空指针 panic。
2. `data.redis` 用的是 `addr:` 字段，proto 是 `host/port/password/db`（参考 getway 的 config.yaml）。

```yaml
server:
  http:
    addr: 0.0.0.0:8000
    timeout: 1s
  grpc:
    addr: 0.0.0.0:9002
    timeout: 1s
data:
  database:
    # ......（保持不变）......
  redis:
    host: 123.207.0.63      # 字段名以 pkg/conf proto 为准：host/port，不是 addr
    port: 6379
    db: 0
    password: <你的密码>
    read_timeout: 3s
    write_timeout: 3s
  brokers:                  # Kafka 地址，data.brokers 才能被 c.GetBrokers() 读到
    - 127.0.0.1:9092
```

---

## 8. 第六步（seckill）：新增结果查询 RPC（轮询兜底）+ 补全提交接口

### 8.1 `api/seckill/v1/seckill.proto`

```proto
syntax = "proto3";

package api.seckill.v1;

option go_package = "seckill/api/seckill/v1;v1";

service Seckill {
	rpc CreateSeckill (CreateSeckillRequest) returns (CreateSeckillReply);
	// 轮询兜底：WS 推送丢失/超时时，客户端凭关联 ID 主动查询结果
	rpc GetSeckillResult (GetSeckillResultRequest) returns (GetSeckillResultReply);
}

message CreateSeckillRequest {
	int64 event_id = 1;
	int64 show_id = 2;
	int64 ticket_type_id = 3;
	int64 quantity = 4;
	int64 user_id = 5;   // 由 getway 从 JWT 解出后填充，客户端传什么都不算数
}

message CreateSeckillReply {
	string status = 1;
	string message = 2;
	int64 timestamp = 3; // 关联 ID：客户端保存它，用于匹配 WS 推送和轮询结果
}

message GetSeckillResultRequest {
	int64 user_id = 1;        // 同样来自 JWT
	int64 event_id = 2;
	int64 ticket_type_id = 3;
	int64 timestamp = 4;      // CreateSeckillReply 返回的关联 ID
}

message GetSeckillResultReply {
	string status = 1;   // processing / success / failed
	string message = 2;
	string order_no = 3;
	int64 timestamp = 4; // 原样回显，方便客户端匹配
}
```

在 `apps/seckill` 下生成代码：

```bash
make api        # buf generate，产出 *.pb.go / *_grpc.pb.go
```

### 8.2 `internal/biz/seckill.go`：加查询用例

```go
// GetPurchaseResult 轮询兜底。res == nil 表示仍在处理中（键不存在）。
func (u *SeckillUseCase) GetPurchaseResult(ctx context.Context, userID, eventID, ticketTypeID uint, timestamp int64) (*common.TicketResult, error) {
	return u.rdbRepo.GetPurchaseResult(ctx, userID, eventID, ticketTypeID, timestamp)
}
```

### 8.3 `internal/service/seckill.go`：补全两个 handler

```go
func (s *SeckillService) CreateSeckill(ctx context.Context, req *pb.CreateSeckillRequest) (*pb.CreateSeckillReply, error) {
	res, err := s.uc.PurchaseTicketAsync(ctx,
		req.GetEventId(), req.GetShowId(), req.GetTicketTypeId(),
		req.GetUserId(), req.GetQuantity())
	if err != nil {
		return nil, err
	}
	return &pb.CreateSeckillReply{
		Status:    res.GetStatus(),
		Message:   res.GetMessage(),
		Timestamp: res.GetTimestamp(),
	}, nil
}

func (s *SeckillService) GetSeckillResult(ctx context.Context, req *pb.GetSeckillResultRequest) (*pb.GetSeckillResultReply, error) {
	res, err := s.uc.GetPurchaseResult(ctx,
		uint(req.GetUserId()), uint(req.GetEventId()),
		uint(req.GetTicketTypeId()), req.GetTimestamp())
	if err != nil {
		return nil, err
	}
	// 默认 processing：结果键不存在 = 还在排队/处理，或已超过 10 分钟 TTL
	reply := &pb.GetSeckillResultReply{
		Status:    "processing",
		Message:   "处理中",
		Timestamp: req.GetTimestamp(),
	}
	if res != nil {
		reply.Status = res.Status
		reply.Message = res.Message
		reply.OrderNo = res.OrderNo
	}
	return reply, nil
}
```

---

## 9. 第七步（ws-getway）：Hertz 长连接网关

### 9.1 目录结构与工程接入

```
apps/ws-getway/
├── go.mod
├── main.go                 # 装配：config → jwt → redis → hub → subscriber → hertz
├── router.go               # 路由注册
├── config/
│   ├── config.go           # 与 getway 相同的 kratos config 加载方式
│   └── config.yaml
├── mvw/
│   └── jwt.go              # 握手鉴权（query token 优先）
├── contract/
│   └── contract.go         # WS 帧 / Redis 广播载荷的 JSON 契约
├── handler/
│   └── ws.go               # /ws 升级入口
└── internal/
    ├── hub/
    │   ├── hub.go          # 有状态核心：user_id → 连接 注册表
    │   └── conn.go         # 单连接：读循环 + 唯一写 goroutine + 心跳
    └── subscriber/
        └── subscriber.go   # 订阅 seckill:results → 分发给 hub
```

`go.work` 加一行（与其它 app 并列）：

```
use (
	.
	./apps/market
	./apps/activity
	./apps/seckill
	./apps/getway
	./apps/user
	./apps/ws-getway
)
```

`apps/ws-getway/go.mod`（workspace 会解析 `sale/...` 的导入，indirect 依赖交给 `go mod tidy` 补齐）：

```go
module ws-getway

go 1.25.7

require (
	github.com/cloudwego/hertz v0.10.6
	github.com/go-kratos/kratos/v3 v3.0.0
	github.com/hertz-contrib/jwt v1.0.4
	github.com/hertz-contrib/websocket v0.2.0
	github.com/redis/go-redis/v9 v9.22.0
)
```

### 9.2 `config/config.go` 与 `config/config.yaml`

config.go 与 `apps/getway/config/config.go` 完全相同（加载 kratos config → 全局 `C *conf.Bootstrap`），直接复制。
config.yaml：

```yaml
server:
  http:
    addr: 0.0.0.0:8001     # ws-getway 自己的端口，不与 getway :8000 冲突
    timeout: 1s
data:
  redis:                    # 与 seckill 连同一个 Redis（广播频道在它上面）
    host: 123.207.0.63
    port: 6379
    db: 0
    password: <你的密码>
    read_timeout: 3s
    write_timeout: 3s
```

### 9.3 `mvw/jwt.go`：握手鉴权

与 getway 用**同一把密钥、同一套 claims** —— token 由 getway 签发、ws-getway 只校验，
用户登录一次，HTTP 和 WS 通用。唯一区别：**浏览器发起 WS 握手不能自定义 Header**，
所以 token 从 query 上取。

```go
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
			c.AbortWithStatusAndDirectString(http.StatusUnauthorized, []byte(`{"code":401,"msg":"未授权"}`))
			return
		}
		if _, ok := val.(*Identity); !ok {
			c.AbortWithStatusAndDirectString(http.StatusUnauthorized, []byte(`{"code":401,"msg":"身份信息错误"}`))
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
```

### 9.4 `contract/contract.go`：JSON 契约

```go
// Package contract 定义 ws-getway 对外的 JSON 契约。
//
// 为什么不直接 import seckill 的类型：载荷定义在 seckill/internal/common 里，
// Go 的 internal 规则禁止跨模块导入 internal 包。所以这里按 json tag 镜像一份。
// JSON 字段名就是两个服务之间的契约 —— 改动必须两边同步（评审时重点盯这里）。
package contract

// SeckillResult 与 seckill/internal/common.TicketResult 的 JSON 形状一一对应。
// seckill consumer 每个终态都会：marshal 这份结构 → 写结果键 → PUBLISH 频道。
type SeckillResult struct {
	TicketID     uint   `json:"ticket_id"`
	UserID       uint   `json:"user_id"`
	EventID      uint   `json:"event_id"`
	TicketTypeID uint   `json:"ticket_type_id"`
	Status       string `json:"status"`   // success / failed
	Message      string `json:"message"`
	OrderNo      string `json:"order_no"`
	Timestamp    int64  `json:"timestamp"` // 关联 ID = 提交秒杀时返回的 timestamp
}

// SeckillResultChannel 与 seckill/internal/common.SeckillResultChannel 保持一致。
const SeckillResultChannel = "seckill:results"

// 下行帧类型
const (
	FrameTypeSeckillResult = "seckill_result"
	FrameTypePong          = "pong"
)

// Frame WS 下行帧：{"type":"seckill_result","data":{...}} / {"type":"pong"}
type Frame struct {
	Type string `json:"type"`
	Data any    `json:"data,omitempty"`
}

// UpFrame WS 上行帧：目前只有心跳 {"type":"ping"}
type UpFrame struct {
	Type string `json:"type"`
}
```

### 9.5 `internal/hub/conn.go`：单连接封装

**并发模型是这里的关键**：`websocket.Conn` 不允许并发写。推送来自 subscriber goroutine、
心跳回包来自读循环 —— 所有写都先投进 `send` channel，由每连接唯一的写 goroutine 消费。

```go
package hub

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/hertz-contrib/websocket"

	"ws-getway/contract"
)

const (
	// sendBufferSize 每连接发送缓冲：扛瞬时突发。满了直接丢帧（推送允许丢，轮询兜底）。
	sendBufferSize = 16
	// readIdleTimeout 读空闲超时：这么久没收到客户端任何消息就断开，
	// 防止客户端假死后连接和内存永不释放。
	readIdleTimeout = 60 * time.Second
	// writeTimeout 单次写超时。
	writeTimeout = 10 * time.Second
)

// Conn 一条用户连接。
// 读：handler 所在 goroutine 阻塞在 readLoop；
// 写：Start 拉起的唯一 writeLoop goroutine，从 send channel 消费。
type Conn struct {
	ws     *websocket.Conn
	userID uint64
	send   chan []byte
	done   chan struct{} // Serve 关闭它 → writeLoop 退出
	logger *slog.Logger
}

func NewConn(ws *websocket.Conn, userID uint64, logger *slog.Logger) *Conn {
	return &Conn{
		ws:     ws,
		userID: userID,
		send:   make(chan []byte, sendBufferSize),
		done:   make(chan struct{}),
		logger: logger,
	}
}

// trySend 非阻塞投递。缓冲满说明客户端消费慢或假死：丢帧保服务，
// 不阻塞 subscriber 的分发循环（否则一个慢客户端会拖慢所有人的推送）。
func (c *Conn) trySend(frame []byte) {
	select {
	case c.send <- frame:
	default:
		c.logger.Warn("[WS] send buffer full, drop frame", "user_id", c.userID)
	}
}

// readLoop 阻塞读上行。目前只处理应用层心跳：
// 客户端每 25s 发 {"type":"ping"}，服务端回 pong，顺路刷新读空闲计时。
// （浏览器 JS 发不了协议层 ping 帧，所以用应用层心跳，天然跨端。）
func (c *Conn) readLoop() {
	_ = c.ws.SetReadDeadline(time.Now().Add(readIdleTimeout))
	for {
		msgType, payload, err := c.ws.ReadMessage()
		if err != nil {
			c.logger.Info("[WS] read closed", "user_id", c.userID, "error", err)
			return
		}
		// 收到任何消息都算活着
		_ = c.ws.SetReadDeadline(time.Now().Add(readIdleTimeout))
		if msgType != websocket.TextMessage {
			continue
		}
		var in contract.UpFrame
		if json.Unmarshal(payload, &in) != nil {
			continue
		}
		if in.Type == "ping" {
			pong, _ := json.Marshal(contract.Frame{Type: contract.FrameTypePong})
			c.trySend(pong)
		}
	}
}

// writeLoop 唯一的写 goroutine。写失败时关闭底层连接，
// 读循环的 ReadMessage 会随之报错退出，收尾统一由 hub.Serve 做。
func (c *Conn) writeLoop() {
	for {
		select {
		case frame := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.ws.WriteMessage(websocket.TextMessage, frame); err != nil {
				c.logger.Info("[WS] write failed, closing conn", "user_id", c.userID, "error", err)
				_ = c.ws.Close()
				return
			}
		case <-c.done:
			return
		}
	}
}
```

### 9.6 `internal/hub/hub.go`：注册表与分发

```go
package hub

import (
	"encoding/json"
	"log/slog"
	"sync"

	"ws-getway/contract"
)

// Hub 是 ws-getway 的有状态核心：user_id → 该用户的全部连接。
// 值用集合而不是单个连接：允许同一账号多标签页/多端同时在线。
// 当前实现是单机内存版 —— 连接只能被本进程推送，这正是「广播 + 本地过滤」
// 模式成立的原因：seckill 广播给所有 ws-getway 实例，谁持有连接谁推送。
type Hub struct {
	mu    sync.RWMutex
	conns map[uint64]map[*Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{conns: make(map[uint64]map[*Conn]struct{})}
}

// Serve 管理一条连接的完整生命周期：注册 → 拉起写 goroutine → 阻塞读 → 注销收尾。
// 该方法阻塞直到连接断开，调用方（handler 里 Upgrade 的回调）不必再管清理。
func (h *Hub) Serve(c *Conn) {
	h.Register(c)
	go c.writeLoop()
	c.readLoop()
	// 走到这里 = 读循环退出 = 连接已断。
	// 先从注册表摘除（之后不再有任何线程能拿到 c 并往 send 投递），
	// 再关 done 让写 goroutine 退出，顺序不能反 —— 避免向已停止的 writer 投递。
	h.Unregister(c)
	close(c.done)
}

func (h *Hub) Register(c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.conns[c.userID]
	if !ok {
		set = make(map[*Conn]struct{})
		h.conns[c.userID] = set
	}
	set[c] = struct{}{}
}

func (h *Hub) Unregister(c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if set, ok := h.conns[c.userID]; ok {
		delete(set, c)
		if len(set) == 0 {
			delete(h.conns, c.userID)
		}
	}
}

// Online 返回当前在线连接数（监控用）。
func (h *Hub) Online() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, set := range h.conns {
		n += len(set)
	}
	return n
}

// PushSeckillResult 把秒杀结果推给该用户的所有在线连接。
// 用户不在线 → 直接丢弃：推送是 best-effort，正确性由结果键 + 轮询兜底保证。
func (h *Hub) PushSeckillResult(result *contract.SeckillResult) {
	frame, err := json.Marshal(contract.Frame{Type: contract.FrameTypeSeckillResult, Data: result})
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	set, ok := h.conns[uint64(result.UserID)]
	if !ok {
		return // 不在线，丢弃
	}
	for c := range set {
		c.trySend(frame)
	}
}
```

### 9.7 `internal/subscriber/subscriber.go`：订阅广播

```go
package subscriber

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"ws-getway/contract"
	"ws-getway/internal/hub"
)

// Subscriber 订阅 seckill 广播的结果频道，解析后交给 Hub 分发。
// 每个实例都订阅同一个频道 —— 这就是「广播 + 本地过滤」：
// 是否推送由 Hub 查自己的本地连接表决定，seckill 对此毫不知情。
type Subscriber struct {
	rdb    *redis.Client
	hub    *hub.Hub
	logger *slog.Logger
}

func NewSubscriber(rdb *redis.Client, h *hub.Hub, logger *slog.Logger) *Subscriber {
	return &Subscriber{rdb: rdb, hub: h, logger: logger}
}

// Run 阻塞运行，放到独立 goroutine 里跑。
// go-redis 的 Pub/Sub 内部自带断线重连，正常情况不需要我们干预；
// 只有 ctx 取消（进程退出）或底层致命错误导致 channel 关闭时才会返回。
func (s *Subscriber) Run(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, contract.SeckillResultChannel)
	defer sub.Close()
	s.logger.Info("[PubSub] subscribed", "channel", contract.SeckillResultChannel)

	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				s.logger.Error("[PubSub] channel closed, subscriber exiting")
				return
			}
			s.dispatch(msg.Payload)
		}
	}
}

// dispatch 单条解析失败直接丢弃，绝不让坏消息中断分发循环。
func (s *Subscriber) dispatch(payload string) {
	var result contract.SeckillResult
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		s.logger.Error("[PubSub] bad payload", "error", err)
		return
	}
	s.hub.PushSeckillResult(&result)
}
```

### 9.8 `handler/ws.go`：升级入口

```go
// Package handler WS 握手入口。
package handler

import (
	"context"
	"log/slog"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/hertz-contrib/websocket"

	"ws-getway/internal/hub"
	"ws-getway/mvw"
)

var upgrader = websocket.HertzUpgrader{
	HandshakeTimeout: 10 * time.Second,
	// 生产环境应校验 Origin 白名单，开发期先全放行
	CheckOrigin: func(ctx *app.RequestContext) bool { return true },
}

var (
	wsHub    *hub.Hub
	wsLogger *slog.Logger
)

// InitWS 由 main 注入依赖（hub 是全进程单例）。
func InitWS(h *hub.Hub, logger *slog.Logger) {
	wsHub = h
	wsLogger = logger
}

// GetWS 处理 WS 握手。路由上已挂 mvw.AuthMiddleware()：
// token 从 query 读取并校验，通过后 Identity 已放进 RequestContext。
//
// upgrader.Upgrade 内部会 Hijack 连接并把回调阻塞在读写循环上，
// 直到连接断开才返回 —— 所以这个 handler 的「慢」是正常的，它就是连接本身。
func GetWS(ctx context.Context, c *app.RequestContext) {
	userID := mvw.GetUserID(c)
	if userID == 0 {
		c.AbortWithStatusAndDirectString(401, []byte(`{"code":401,"msg":"unauthorized"}`))
		return
	}
	if err := upgrader.Upgrade(c, func(conn *websocket.Conn) {
		wsHub.Serve(hub.NewConn(conn, userID, wsLogger))
	}); err != nil {
		hlog.CtxErrorf(ctx, "ws upgrade failed: %v", err)
	}
}
```

### 9.9 `router.go` 与 `main.go`

```go
// router.go
package main

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"ws-getway/handler"
	"ws-getway/mvw"
)

func register(r *server.Hertz) {
	r.GET("/ping", func(ctx context.Context, c *app.RequestContext) {
		c.String(200, "pong")
	})
	// 握手鉴权：query token 优先（浏览器 WS 无法自定义 Header）
	r.GET("/ws", append(mvw.AuthMiddleware(), handler.GetWS)...)
}
```

```go
// main.go
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/redis/go-redis/v9"

	"sale/pkg/rdb"
	"ws-getway/config"
	"ws-getway/handler"
	"ws-getway/internal/hub"
	"ws-getway/internal/subscriber"
	"ws-getway/mvw"
)

func main() {
	flag.Parse()
	config.Init()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. JWT：密钥与 getway 共用，getway 签发的 token 在这里直接有效
	mvw.InitJwt()

	// 2. Redis 客户端（复用 sale/pkg/rdb，与其它服务同一套配置结构）
	rdbClient := rdb.NewRdbClient(config.C.Data.GetRedis())

	// 3. 连接注册表（有状态核心）+ 注入 handler
	h := hub.NewHub()
	handler.InitWS(h, logger)

	// 4. 订阅 seckill 广播：必须先于/同时于对外服务启动，否则会有窗口期丢推送
	//    （丢了也没关系：客户端轮询兜底，这里从简）
	go subscriber.NewSubscriber(rdbClient, h, logger).Run(context.Background())

	// 5. HTTP 服务 + 优雅退出（h.Spin 内部等待退出信号）
	hz := server.Default()
	register(hz)
	hz.Spin()
}
```

启动并补齐依赖：

```bash
cd apps/ws-getway
go mod tidy
go run .          # 或 go build -o bin/ws-getway .
```

---

## 10. 第八步（getway）：补上秒杀的 HTTP 路由

### 10.1 新建 `biz/handler/seckill/seckill.go`

风格对齐现有 handler（`BindAndValidate` + `response` 包）：

```go
package seckill

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"getway/biz/client"
	"getway/biz/handler/response"
	"getway/mvw"
	seckillv1 "seckill/api/seckill/v1"
)

type CreateSeckillReq struct {
	EventID      int64 `json:"event_id" vd:"$>0"`
	ShowID       int64 `json:"show_id"`
	TicketTypeID int64 `json:"ticket_type_id" vd:"$>0"`
	Quantity     int64 `json:"quantity" vd:"$>0"`
}

type CreateSeckillResp struct {
	Status    string `json:"status"`   // queued
	Message   string `json:"message"`  // 排队中
	Timestamp int64  `json:"timestamp"` // 关联 ID：客户端必须保存
}

type GetSeckillResultReq struct {
	EventID      int64 `query:"event_id" vd:"$>0"`
	TicketTypeID int64 `query:"ticket_type_id" vd:"$>0"`
	Timestamp    int64 `query:"timestamp" vd:"$>0"`
}

type SeckillResultResp struct {
	Status    string `json:"status"`             // processing / success / failed
	Message   string `json:"message"`
	OrderNo   string `json:"order_no,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// CreateSeckill 提交秒杀，立即返回「排队中 + 关联 timestamp」。
// 结果通过两条路返回：WS 推送（快）/ 轮询 /api/seckill/result（兜底）。
// user_id 只信 JWT —— 客户端 body 里传什么都不算数。
func CreateSeckill(ctx context.Context, c *app.RequestContext) {
	var req CreateSeckillReq
	if err := c.BindAndValidate(&req); err != nil {
		response.HandleParamsError(c)
		return
	}
	userID := mvw.GetUserID(c)
	if userID == 0 {
		c.JSON(consts.StatusOK, response.Response{Code: response.ERRORTOKENFAIL_CODE, Msg: response.ERRORTOKENFAIL_MSG})
		return
	}
	res, err := client.GetSeckillClient().CreateSeckill(ctx, &seckillv1.CreateSeckillRequest{
		EventId:      req.EventID,
		ShowId:       req.ShowID,
		TicketTypeId: req.TicketTypeID,
		Quantity:     req.Quantity,
		UserId:       int64(userID),
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "create seckill failed: %v", err)
		response.HandleBussinessFail(c, err.Error())
		return
	}
	response.HandleSuccess(c, CreateSeckillResp{
		Status:    res.GetStatus(),
		Message:   res.GetMessage(),
		Timestamp: res.GetTimestamp(),
	})
}

// GetSeckillResult 轮询兜底：凭 (event_id, ticket_type_id, timestamp) 查结果。
// WS 推送正常时客户端永远不用调它。
func GetSeckillResult(ctx context.Context, c *app.RequestContext) {
	var req GetSeckillResultReq
	if err := c.BindAndValidate(&req); err != nil {
		response.HandleParamsError(c)
		return
	}
	userID := mvw.GetUserID(c)
	if userID == 0 {
		c.JSON(consts.StatusOK, response.Response{Code: response.ERRORTOKENFAIL_CODE, Msg: response.ERRORTOKENFAIL_MSG})
		return
	}
	res, err := client.GetSeckillClient().GetSeckillResult(ctx, &seckillv1.GetSeckillResultRequest{
		UserId:       int64(userID),
		EventId:      req.EventID,
		TicketTypeId: req.TicketTypeID,
		Timestamp:    req.Timestamp,
	})
	if err != nil {
		hlog.CtxErrorf(ctx, "get seckill result failed: %v", err)
		response.HandleBussinessFail(c, err.Error())
		return
	}
	response.HandleSuccess(c, SeckillResultResp{
		Status:    res.GetStatus(),
		Message:   res.GetMessage(),
		OrderNo:   res.GetOrderNo(),
		Timestamp: res.GetTimestamp(),
	})
}
```

### 10.2 新建 `biz/router/seckill/seckill.go`

不要塞进 `router_gen.go` / 生成的 `register.go` —— 那是 hz 工具的地盘，手改会被覆盖：

```go
package seckill

import (
	"github.com/cloudwego/hertz/pkg/app/server"

	"getway/biz/handler/seckill"
	"getway/mvw"
)

// Register 注册秒杀路由，统一挂鉴权。
func Register(r *server.Hertz) {
	g := r.Group("/api", mvw.AuthMiddleware()...)
	g.POST("/seckill", seckill.CreateSeckill)
	g.GET("/seckill/result", seckill.GetSeckillResult)
}
```

### 10.3 `router.go`：在自定义注册入口挂上

import 处加上（与 `handler "getway/biz/handler"` 并列）：

```go
import (
	// ...现有 import...
	seckillRouter "getway/biz/router/seckill"
)

func customizedRegister(r *server.Hertz) {
	r.GET("/ping", handler.Ping)

	seckillRouter.Register(r)
}
```

### 10.4 对齐 gRPC 端口

`biz/client/seckill.go` 目前拨的是 `127.0.0.1:9003`，而 seckill 的 grpc 监听 `:9002`，
改成一致（生产应进配置文件）：

```go
conn, err := grpc.NewClient("127.0.0.1:9002",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
)
```

---

## 11. 联调验证

### 11.1 环境准备

```bash
# 1) Kafka topic（franz-go 默认不自动建 topic）
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic ticket.orders --partitions 8
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic ticket.orders.dlq --partitions 1

# 2) 预置库存：活动(event) 1、票种 2、100 张
#    注意 key 里的花括号要加引号，shell 才不会当成命令替换
redis-cli HSET 'seckill:{1}:stock' '2' '100'
```

### 11.2 启动（三个终端）

```bash
cd apps/seckill  && make generate && go run ./cmd/seckill -conf ./configs
cd apps/ws-getway && go mod tidy   && go run .
cd apps/getway   && go run .
```

### 11.3 端到端手动测试

```bash
# 1) 登录拿 token（getway 现有接口）
curl -X POST http://127.0.0.1:8000/api/auth/login -H 'Content-Type: application/json' \
  -d '{"username":"...","password":"..."}'
TOKEN=<返回的token>

# 2) 建立 WS 连接（安装：brew install websocat）
websocat "ws://127.0.0.1:8001/ws?token=$TOKEN"
# 连上后输入 {"type":"ping"} 应收到 {"type":"pong"}

# 3) 提交秒杀（另开终端）
curl -X POST http://127.0.0.1:8000/api/seckill \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"event_id":1,"ticket_type_id":2,"quantity":1}'
# → {"code":0,"msg":"成功","data":{"status":"queued","message":"排队中","timestamp":1758...}}

# 4) 回看 websocat 终端，应收到：
# {"type":"seckill_result","data":{"ticket_id":9,"user_id":1,"event_id":1,
#  "ticket_type_id":2,"status":"success","message":"抢购成功","order_no":"...","timestamp":1758...}}
# data.timestamp 与第 3 步返回的 timestamp 相同 —— 这就是匹配依据

# 5) 单测 ws-getway（跳过整条业务链，手动模拟 seckill 广播）
redis-cli PUBLISH seckill:results \
  '{"user_id":1,"event_id":1,"ticket_type_id":2,"status":"success","message":"测试","order_no":"T001","timestamp":123}'

# 6) 验证轮询兜底（把 websocat 断开后再提交一次，等 2 秒直接查）
curl "http://127.0.0.1:8000/api/seckill/result?event_id=1&ticket_type_id=2&timestamp=<第3步的timestamp>" \
  -H "Authorization: Bearer $TOKEN"
```

### 11.4 浏览器端参考实现

```js
const token = localStorage.getItem('token');

function connectWS() {
  const ws = new WebSocket(`ws://127.0.0.1:8001/ws?token=${token}`);
  let heartbeat;

  ws.onopen = () => {
    // 每 25s 一次应用层心跳（服务端 60s 读空闲会踢连接）
    heartbeat = setInterval(() => ws.send('{"type":"ping"}'), 25000);
  };

  ws.onmessage = (e) => {
    const frame = JSON.parse(e.data);
    if (frame.type === 'seckill_result') {
      // frame.data = {user_id, event_id, ticket_type_id, status, message, order_no, timestamp}
      // 用 timestamp 匹配到具体那次抢购，更新对应 UI 状态
    }
  };

  ws.onclose = () => {
    clearInterval(heartbeat);
    // 降级轮询 + 指数退避重连（重连要带新 token，过期 token 握手会被 401）
    startPolling();
    setTimeout(connectWS, 3000);
  };
}
```

---

## 12. 本教程顺手修掉的坑（自查清单）

1. **seckill consumer 没启动**：`HandleMessage` 此前无调用方 → §7 的 `ConsumerServer`。
2. **`configs/config.yaml` 的 `mq.broker` 是无效字段**：proto 是 `data.brokers`；
   且 brokers 为空时 producer 创建失败只打日志，下单时 nil panic → §7.5。
3. **`data.redis` 用 `addr:` 字段**：proto 是 `host/port/password/db` → §7.5。
4. **getway seckill client 端口 9003 ≠ seckill grpc 9002** → §10.4。
5. **提交响应缺关联 ID**：客户端没法对账 → §6、proto `CreateSeckillReply.timestamp`。
6. **重复消息静默丢弃**：改为重推成功结果（幂等，见 §5）。
7. **`seckillRollBackScript` Lua 有个拼写错误 `retunr 1`**：目前 lua 报错被 `.Err()` 吞掉，
   建议顺手改成 `return 1`。

## 13. 生产化清单（按需演进）

- **频道分片**：单 channel 广播在实例多/流量大后放大明显，改 `hash(user_id) % N` 分片频道，
  每个 ws-getway 实例只订阅分片子集；Redis Cluster 下用 Sharded Pub/Sub（SSUBSCRIBE）避免全网广播。
- **契约下沉**：把 `TicketResult` 从 `seckill/internal/common` 移到根模块 `sale/model`，
  ws-getway 直接 import，消灭镜像 struct 的漂移风险。
- **wss + Origin 白名单**：握手必须走 TLS；`CheckOrigin` 校验站点白名单防 CSRF。
- **连接治理**：单实例连接数上限、单用户连接数上限（防多端滥用）、握手失败限速。
- **监控**：在线连接数（`Hub.Online()`）、推送 P99 延迟、`trySend` 丢帧计数、PubSub 断连告警。
- **优雅停机**：先摘流量 → 等 send 缓冲 flush → 主动 close（客户端指数退避重连 + 轮询兜底）。
- **token 生命周期**：JWT 2h 过期，但校验只发生在握手时；长连接期间过期不影响已建连接，
  重连需要新 token —— 前端要处理 401 → 重新登录 → 重连的链路。
- **轮询停止条件**：结果键 TTL 10 分钟，客户端轮询超过 TTL 仍未查到即视为超时失败，停止轮询。
