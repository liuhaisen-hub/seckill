# 排队大厅（Waiting Room）实现教程

> 目标：在秒杀链路之前加一道"排队削峰"——大流量先进入 Redis 排队大厅，
> 由放行调度器按固定速率放人进入秒杀，位次通过 WebSocket 实时推送给用户。
> 本文基于仓库当前代码（`apps/ws-getway`），实现方案参考
> `Full-stack-distributed-flash-sale-engine-main` 的第 15 章（`edu/15-排队大厅与等候名单.md`），
> 并针对本仓库的 WS 网关架构做了三处关键适配（见 §2）。

---

## 0. 为什么需要排队大厅

真实票务场景（演唱会开票）会出现：**20 万人在 10:00:00 同时点击"抢票"，而票只有 5000 张**。
即使 Redis Lua 每秒能处理 10 万次扣减，也会带来两个问题：

| 问题 | 表现 | 后果 |
| --- | --- | --- |
| **瞬时洪峰** | 20 万请求 1 秒内涌入网关 | 连接数打爆、CPU 飙升，正常用户也访问不了 |
| **体验极差** | 19.5 万人收到"已售罄" | 用户不知道自己排第几，疯狂刷新，形成二次冲击 |

排队大厅的思路：

```
        ┌────────────────────┐
20万人 →│  排队大厅            │ → 每秒放行 N 人 → 秒杀链路（getway → seckill Lua 扣减）
        │ (Redis Sorted Set) │
        └────────────────────┘
             ↓
      返回"你排第 8123 位，预计等待 4 分钟"（WS 实时推送 + HTTP 轮询兜底）
```

三个收益：

1. **削峰**：真正打到秒杀链路的 QPS 由我们控制（放行调度器的速率），而不是由用户决定。
2. **可预期**：用户看到明确的位次和预计时间，不会疯狂刷新。
3. **公平**：先到先得（FIFO），而不是"网络快的人赢"。

> 🎯 **核心要点**：限流是"拒绝"，排队是"延后"。前者丢请求，后者保留请求。两者组合使用：
> 排队入队接口本身也要限流，防止有人刷入队接口把真实用户挤到队尾。

**排队大厅 ≠ 等候名单**（两个不同阶段的东西，见附录 A）：

| 维度 | 排队大厅 Queue（本文主体） | 等候名单 Waitlist（附录） |
| --- | --- | --- |
| 触发时机 | 开票**前/中**，还有库存 | 已经**售罄**之后 |
| 目的 | 控制进入秒杀的速率 | 记录"想要但没抢到"的用户 |
| 生命周期 | 分钟级（入队 30 分钟 TTL） | 天级（7 天 TTL） |
| 出队意味着 | 获得抢票资格 | 有人退票了，通知你来买 |

---

## 1. 现状盘点：ws-getway 已有什么、缺什么

对照当前 `apps/ws-getway` 代码：

```
✅ 已有：
  /ws 握手入口        biz/handler/ws_services.go（gorilla upgrader + adaptor.HertzHandler）
  连接注册表          biz/hub/hub.go（userID → set[*Conn]，PushSeckillResult 按用户推送）
  单连接读写循环      biz/hub/conn.go（读空闲 60s、send 缓冲 16、满则丢帧）
  结果订阅分发        biz/subscriber/subscriber.go（订阅 seckill:results → hub 推送）
  WS 帧契约          biz/contract/seckill.go（Frame{type,data} / ping-pong）
  握手鉴权           mvw/jwt.go（query token 优先，与 getway 同密钥同 claims）
  配置加载           config/config.go（kratos config → sale/pkg/conf.Bootstrap，含 data.redis）

❌ 缺口（本文要补的）：
  缺口1：排队领域逻辑完全不存在（入队/查位次/放行/退出）
  缺口2：main.go 装配不完整 —— 没有调用 mvw.InitJwt()、没创建 hub、
         没创建 Redis client、没启动 subscriber（当前 main.go 只有 config.Init + register + Spin）
  缺口3：main.go import 的是 "getway/config"（getway 服务的包），应为 "ws-getway/config"
  缺口4：router.go 的 /ws 只挂了 mvw.CheckAuthMiddleware() —— 它只检查 RequestContext 里
         是否已有 Identity，而写入 Identity 的是 JwtMiddleware.MiddlewareFunc()。
         JWT 中间件没挂 → Identity 永远不存在 → /ws 现在必然 401
  缺口5：WS 上行帧只处理 ping，hub 只有"推秒杀结果"一种推送，没有通用推送能力
```

> ⚠️ 缺口 2/3/4 是**前置修复项**：不修好它们，任何基于 ws-getway 的新功能都跑不起来。
> 排队大厅的落地步骤里第一步就是修装配（§5）。

---

## 2. 目标架构

```
Client ──HTTP POST /api/queue/:event_id/join──► ws-getway :8001
   │                                              │
   │                                    ┌─────────▼──────────┐
   │                                    │  QueueManager       │
   │                                    │  Redis Sorted Set   │ queue:z:event:{id}
   │                                    └─────────┬──────────┘
   │                                              │ 定速放行（仅 leader 实例）
   │                                    ┌─────────▼──────────┐
   │                                    │  Dispatcher         │ ZPopMin 批量放行
   │                                    │  (分布式锁选主)      │ 写 queue:admit:{e}:{u} 资格标记
   │                                    └─────────┬──────────┘ PUBLISH queue:events {admitted}
   │                                              │
   │        ┌────────────────────────────────────┤
   │        ▼                                    ▼
   │  各 ws-getway 实例 subscriber 收到放行事件 → hub 推 WS 帧 queue_admitted
   │  各 ws-getway 实例 broadcaster 周期拉队列 → hub 推 WS 帧 queue_position
   │
   ├──WS── ws-getway :8001 ◄── {"type":"queue_position",...} / {"type":"queue_admitted",...}
   │
   └──获得资格后──HTTP POST /api/seckill──► getway :8000 ──gRPC──► seckill
                                                  （seckill 校验 queue:admit 标记，见 §13）
```

### 与参考项目的三个关键适配（为什么这样改）

参考项目是 Gin 单体（HTTP 排队 + 同进程 wsHub 推送）；本仓库里排队能力放在 **ws-getway**，
因为它已经持有用户连接和广播基础设施。据此做三个适配：

1. **数据结构直接上 Sorted Set，不用参考项目的 List 教学版。**
   参考项目 `internal/queue/queue.go` 用 List（RPush/LPop），查位次要 `LRANGE 0 -1` 全量拉取 +
   线性扫描（O(N)，20 万人一次查询 ~50ms + 2MB 传输）；其配套教材明确给出生产推荐是 Sorted Set
   （`ZADD NX` 入队 / `ZRANK` 查位次，都是 O(logN)，且天然去重、score 即入队时间，省掉每用户一个标记 key）。
   新项目没有历史包袱，直接采用 Sorted Set 版本。

2. **写操作走 HTTP，实时推送走 WS，轮询兜底** —— 与本仓库已落地的秒杀结果推送
   （`docs/seckill-ws-push.md`）完全同一模式：
   - 入队/查位次/退出：HTTP 接口（简单、幂等、可限流、可被网关统一治理）；
   - 位次变化/放行通知：WS 推帧（推优于拉，20 万人轮询是 6.7 万 QPS，广播是每实例每 2 秒一次读）；
   - WS 丢帧（send 缓冲满即丢）不影响正确性：客户端 3~5 秒轮询一次 position 接口兜底。

3. **位次推送不走 Pub/Sub，改"每实例自主拉取 + 本地过滤"。**
   秒杀结果是一次性事件、产生方（seckill consumer）不知道谁在线，必须 Pub/Sub 广播；
   位次是**连续状态且事实源就在 Redis**，每个 ws-getway 实例自己 `ZRANGE` 一遍、
   只推本地在线用户即可。若把每个用户的位次逐条 PUBLISH，20 万人 × 每 2 秒 = 10 万 msg/s，
   Redis Pub/Sub 反而被我们自己打死。
   唯一需要 Pub/Sub 的是**放行事件**（低频，只发生在 leader 实例，其他实例需要被通知去推
   `queue_admitted` 帧）。

### 核心设计原则

- **放行速率是系统的主阀门**：进入 seckill 的 QPS = 放行速率（人/秒），与用户行为解耦。
- **放行调度器全集群只跑一个**（Redis 分布式锁选主），否则实际放行速率 = 配置速率 × 实例数。
- **放行 = 发"资格"**：出队用户获得 `queue:admit:{event}:{user}` 标记（带 TTL 的购买窗口），
  seckill 侧提交秒杀前校验该标记。没有这一步，放行就只是"心理安慰"，用户提交秒杀依然会挤爆链路。
- **推送是 best-effort**：不在线/丢帧的用户靠 HTTP 轮询兜底，正确性永远在 Redis/结果键上。

---

## 3. Redis Key 与数据结构设计

### 3.1 结构选型：为什么是 Sorted Set

| 结构 | 入队 | 出队 | 查位次 | 去重 | 结论 |
| --- | --- | --- | --- | --- | --- |
| List | RPUSH O(1) | LPOP O(1) | LRANGE 全量 **O(N)** | 需额外 Key | ❌ 参考项目教学版，位次查询是瓶颈 |
| **Sorted Set** | ZADD NX O(logN) | ZPOPMIN O(logN) | ZRANK **O(logN)** | **天然去重** | ✅ 本文采用 |
| Stream | XADD O(1) | XREADGROUP | 不支持 | 无 | ❌ 适合消息，不适合排队 |

score 用 **入队时刻的纳秒时间戳**（`time.Now().UnixNano()`）：

- 先入队 → score 更小 → `ZRANK`/`ZPOPMIN` 自然给出 FIFO 顺序；
- score 本身就是入队时间，查位次时顺带能恢复 JoinedAt，**不需要参考项目里每人一个
  `queue:user:*` 标记 key**（那个方案还引入了 Get 检查 + Set 写入的非原子竞态，教材里列为缺陷 1）；
- `ZADD NX`（仅不存在时添加）原子去重，重复入队返回 0，一个命令替代"Get 检查 + RPush + Set 标记"三步。

> 💡 **float64 精度说明**：Redis score 是 float64（53 位有效数字 ≈ 9×10^15），纳秒时间戳
> ~1.7×10^18 会损失约几百纳秒精度。相邻两人入队时间差远大于该误差，排序不受影响；同纳秒
> 极端冲突时顺序不确定，但只是两人位次互换，无正确性问题。若要严格单调可改用
> `毫秒时间戳*1e6 + 序列号`，本场景无必要。

### 3.2 Key 与频道总表（全局唯一定义处）

| Key / 频道 | 类型 | 内容 | TTL | 定义位置（新代码） |
| --- | --- | --- | --- | --- |
| `queue:z:event:{event_id}` | Sorted Set | member=userID，score=入队纳秒时间戳 | 无（靠清理协程，见 §7 Cleanup） | `biz/queue/keys.go` |
| `queue:active` | Set | 当前有队列在跑的 eventID 集合 | 无 | 同上 |
| `queue:admit:{event_id}:{user_id}` | String | "1"（放行资格标记） | 5 分钟（购买窗口） | 同上 |
| `queue:leader` | String | 持锁实例的随机 ID | 10s（续期） | 同上 |
| `queue:events` | Pub/Sub 频道 | 放行事件 `{type:"admitted",...}` | - | 同上 |

> 🎯 参考项目教材的教训之一：库存检查 key 与秒杀模块实际 key 对不上（散落的 `fmt.Sprintf`
> 各写各的），校验形同虚设。本仓库秒杀库存 key 是 `seckill:{ticket:{event_id}}:stock`
> （见 `apps/seckill/internal/data/rdb.go`），排队相关的 key **统一定义在 `biz/queue/keys.go`，
> 禁止在业务代码里散落拼接**。seckill 侧校验 admit 标记时引用同一个命名（§13 给出对齐方式）。

---

## 4. 消息契约

### 4.1 WS 下行帧（`biz/contract/seckill.go` 追加）

```go
// 下行帧类型（追加到现有 const 块）
const (
	FrameTypeSeckillResult = "seckill_result"
	FrameTypePong          = "pong"
	FrameTypeQueuePosition = "queue_position" // 位次推送：{"type":"queue_position","data":{...}}
	FrameTypeQueueAdmitted = "queue_admitted" // 放行通知：{"type":"queue_admitted","data":{...}}
)

// QueuePositionPayload queue_position 帧载荷（与 HTTP 查询响应共用形状）
type QueuePositionPayload struct {
	EventID       uint64 `json:"event_id"`
	Position      int64  `json:"position"`       // 我排第几（从 1 开始，前端展示用）
	TotalAhead    int64  `json:"total_ahead"`    // 前面还有几人（从 0 开始，进度条计算用）
	EstimatedWait int64  `json:"estimated_wait"` // 预估等待秒数 = TotalAhead / 放行速率
	Status        string `json:"status"`         // waiting
}

// QueueEvent 排队 Pub/Sub 事件（queue:events 频道，leader 实例 → 所有 ws-getway 实例）
type QueueEvent struct {
	Type    string `json:"type"`             // admitted
	EventID uint64 `json:"event_id"`
	UserID  uint64 `json:"user_id,omitempty"`
	Window  int64  `json:"window,omitempty"` // 购买窗口秒数，倒计时展示用
}
```

### 4.2 WS 上行帧

**保持只有 `{"type":"ping"}` 不变。** 排队动作一律走 HTTP —— 入队必须经过限流和登录校验，
放在 HTTP 层比塞进长连接帧里好治理（限流器、网关策略、日志都现成）。

### 4.3 HTTP 接口（ws-getway 新增，全部挂 `mvw.AuthMiddleware()`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/queue/:event_id/join` | 入队，返回当前位次；重复入队返回当前位次（幂等友好） |
| GET | `/api/queue/:event_id/position` | 查位次（WS 丢帧时的轮询兜底，前端每 3~5s 一次） |
| DELETE | `/api/queue/:event_id` | 退出排队（放弃按钮 / 页面关闭 beacon） |
| GET | `/api/queue/:event_id/length` | 当前排队人数（运营大屏） |

---

## 5. 第一步：修复 main.go 装配（前置修复，不改这一步全都跑不通）

当前 `main.go` 有四个问题（对应 §1 缺口 2/3/4）：import 错包、`InitJwt` 未调用、
hub/Redis/subscriber 未装配、`/ws` 路由中间件不对。**全量替换 `apps/ws-getway/main.go`：**

```go
// Code generated by hertz generator. （保留原注释头）

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/redis/go-redis/v9"

	"sale/pkg/rdb"
	ws_services "ws-getway/biz/handler"
	"ws-getway/biz/hub"
	"ws-getway/biz/queue"
	"ws-getway/biz/subscriber"
	"ws-getway/config"
	"ws-getway/mvw"
)

func main() {
	config.Init()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	// 1. JWT：密钥与 getway 共用，getway 签发的 token 在这里直接有效
	//    【修复】原来从未调用，JwtMiddleware 是 nil
	mvw.InitJwt()

	// 2. Redis 客户端（复用 sale/pkg/rdb，与 seckill 连同一个 Redis：
	//    排队 key / 放行事件频道 / 秒杀结果频道都在它上面）
	rdbClient := rdb.NewRdbClient(config.C.Data.GetRedis())

	// 3. 连接注册表（有状态核心）+ 注入 handler
	h := hub.NewHub()
	ws_services.InitWS(h, logger)

	// 4. 排队大厅三件套
	qm := queue.NewManager(rdbClient, queue.DefaultRate)
	ws_services.InitQueue(qm)
	go queue.NewDispatcher(qm, rdbClient, logger).Run(ctx) // 放行调度器（内部选主，仅 leader 放行）
	go queue.NewBroadcaster(qm, h, logger).Run(ctx)        // 位次推送（每实例都跑，本地过滤）

	// 5. 订阅 Redis 广播：秒杀结果 + 排队放行事件（必须在对外服务前启动，避免窗口期丢推送）
	go subscriber.NewSubscriber(rdbClient, h, logger).Run(ctx)

	// 6. HTTP + WS 服务（h.Spin 内部等待退出信号）
	hz := server.Default()
	register(hz)
	hz.Spin()
}
```

要点：

- `import "ws-getway/config"` —— 替换掉原来的 `"getway/config"`（那是 getway 服务的包，
  能编译纯靠 go.work 把两个模块都挂进来了，属于复制粘贴残留）。
- **Dispatcher 每个 ws-getway 实例都启动 `Run`，但内部竞选 leader，非 leader 自动空转** ——
  这样扩缩容不需要改任何启动逻辑，leader 挂掉后 10 秒内自动换主（§9）。
- **Broadcaster 每个实例都要跑**：它只服务"本地在线用户"，不存在选主问题。

`router.go` 同步修复 `/ws` 的中间件（缺口 4），并挂上排队 HTTP 路由（最终形态，§12 逐个实现）：

```go
// Code generated by hertz generator.

package main

import (
	"context"
	ws_services "ws-getway/biz/handler"
	"ws-getway/mvw"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
)

// customizeRegister registers customize routers.
func customizedRegister(r *server.Hertz) {
	r.GET("/ping", func(c context.Context, ctx *app.RequestContext) {
		ctx.String(200, "pong")
	})

	// 【修复】原来是 mvw.CheckAuthMiddleware()：它只检查 RequestContext 里有没有 Identity，
	// 而真正解析 token 并写入 Identity 的是 JwtMiddleware.MiddlewareFunc()。
	// 原写法下 Identity 永远不存在，/ws 必然 401。
	r.GET("/ws", append(mvw.AuthMiddleware(), ws_services.GetWS())...)

	// 排队大厅 HTTP 接口：全部登录可见
	api := r.Group("/api", mvw.AuthMiddleware()...)
	api.POST("/queue/:event_id/join", ws_services.JoinQueue)
	api.GET("/queue/:event_id/position", ws_services.QueuePosition)
	api.DELETE("/queue/:event_id", ws_services.LeaveQueue)
	api.GET("/queue/:event_id/length", ws_services.QueueLength)
}
```

---

## 6. 第二步：contract 增加排队契约

`biz/contract/seckill.go` 追加 §4.1 的常量与两个 struct（代码见 §4.1，直接复制）。
`Frame` / `UpFrame` 结构不用动——`Data any` 天然承载新载荷。

---

## 7. 第三步：QueueManager —— 排队领域逻辑

新建 `apps/ws-getway/biz/queue/keys.go`（Key 唯一定义处）与 `apps/ws-getway/biz/queue/manager.go`。

### 7.1 `keys.go`

```go
// Package queue 实现排队大厅：入队（Sorted Set）、定速放行（dispatcher + 选主）、
// 位次推送（broadcaster + 本地过滤）、放行资格（admit 标记）。
//
// 所有 Redis Key / 频道命名集中在本文件，禁止在业务代码里散落 fmt.Sprintf 拼接 ——
// 跨服务引用（如 seckill 校验 admit 标记）时以这里的命名为准。
package queue

import (
	"fmt"
	"time"
)

const (
	// QueueKeyFmt 排队本体：Sorted Set，member=userID，score=入队纳秒时间戳。
	QueueKeyFmt = "queue:z:event:%d"
	// QueueActiveKey 当前有活跃队列的 eventID 集合，dispatcher/broadcaster/清理协程按它扫描。
	QueueActiveKey = "queue:active"
	// QueueAdmitFmt 放行资格标记：放行时写入，seckill 提交秒杀前校验，TTL=购买窗口。
	QueueAdmitFmt = "queue:admit:%d:%d"
	// QueueLeaderKey 放行调度器选主锁，value=实例随机 ID。
	QueueLeaderKey = "queue:leader"
	// QueueChannel 放行事件 Pub/Sub 频道：leader 放行后广播，各 ws-getway 实例推 WS 帧。
	QueueChannel = "queue:events"

	// DefaultRate 默认放行速率（人/秒）：系统的主阀门，压测后调整，或做成配置项。
	DefaultRate = 50
	// JoinTTL 入队有效期：入队后这么久还没被放行，视为放弃，由清理协程按 score 移除。
	JoinTTL = 30 * time.Minute
	// AdmitWindow 购买窗口：获得资格后这么久内必须提交秒杀，过期资格作废。
	AdmitWindow = 5 * time.Minute
	// LeaderTTL / LeaderRenewInterval 选主锁 TTL 与续期周期。
	LeaderTTL          = 10 * time.Second
	LeaderRenewInterval = 5 * time.Second
)

func QueueKey(eventID uint64) string              { return fmt.Sprintf(QueueKeyFmt, eventID) }
func AdmitKey(eventID, userID uint64) string      { return fmt.Sprintf(QueueAdmitFmt, eventID, userID) }
```

### 7.2 `manager.go`

```go
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"ws-getway/biz/contract"
)

var (
	// ErrAlreadyInQueue 业务错误：ZADD NX 返回 0 = 已在队列。
	// 注意不是"拒绝"而是"提示"——HTTP 层捕获后会直接返回当前位次（幂等友好）。
	ErrAlreadyInQueue = errors.New("您已在排队中，请勿重复提交")
	ErrNotInQueue     = errors.New("您不在队列中")
)

// Manager 排队大厅领域逻辑。无本地状态（数据全在 Redis），可随意 new，
// 不需要参考项目 QueueManager 那种 sync.Once 单例（单例反而让测试没法换 miniredis）。
type Manager struct {
	rdb  *redis.Client
	rate int64 // 放行速率（人/秒），只用于预估等待时间的展示
}

func NewManager(rdb *redis.Client, rate int) *Manager {
	if rate <= 0 {
		rate = DefaultRate
	}
	return &Manager{rdb: rdb, rate: int64(rate)}
}

// JoinQueue 入队。
//
// ZADD NX 一个命令同时完成"去重检查 + 入队"，原子的：
// 参考项目 List 版的"Get 检查 + Set 标记"两步之间存在竞态（并发点击可重复入队），
// 这里从结构上消灭了这个问题。
func (m *Manager) JoinQueue(ctx context.Context, eventID, userID uint64) (*contract.QueuePositionPayload, error) {
	key := QueueKey(eventID)
	member := strconv.FormatUint(userID, 10)

	added, err := m.rdb.ZAddNX(ctx, key, redis.Z{
		Score:  float64(time.Now().UnixNano()), // score 即入队时间：FIFO 顺序 + JoinedAt 一份数据两用
		Member: member,
	}).Result()
	if err != nil {
		return nil, err
	}
	if added == 0 {
		return nil, ErrAlreadyInQueue
	}

	// 标记该活动有活跃队列（SAdd 幂等，重复添加无副作用）
	if err := m.rdb.SAdd(ctx, QueueActiveKey, eventID).Err(); err != nil {
		return nil, err
	}

	rank, err := m.rdb.ZRank(ctx, key, member).Result()
	if err != nil {
		return nil, err
	}
	return m.position(eventID, rank), nil
}

// GetPosition 查位次，O(logN)。
// Pipeline 一次网络往返同时取 rank（位次）和 score（入队时间，需要 JoinedAt 时从这里恢复）。
func (m *Manager) GetPosition(ctx context.Context, eventID, userID uint64) (*contract.QueuePositionPayload, error) {
	key := QueueKey(eventID)
	member := strconv.FormatUint(userID, 10)

	pipe := m.rdb.Pipeline()
	rankCmd := pipe.ZRank(ctx, key, member)
	scoreCmd := pipe.ZScore(ctx, key, member)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	// ZRank 对不存在的 member 返回 redis.Nil → 不在队列
	if errors.Is(rankCmd.Err(), redis.Nil) {
		return nil, ErrNotInQueue
	}
	_ = scoreCmd // score = 入队纳秒时间戳；如需返回 JoinedAt：time.Unix(0, int64(scoreCmd.Val()))
	return m.position(eventID, rankCmd.Val()), nil
}

// position 由 rank 构造位次快照。EstimatedWait = 前面人数 / 放行速率（整除即秒）。
func (m *Manager) position(eventID uint64, rank int64) *contract.QueuePositionPayload {
	return &contract.QueuePositionPayload{
		EventID:       eventID,
		Position:      rank + 1, // rank 从 0 开始，给人看的位次 +1
		TotalAhead:    rank,
		EstimatedWait: rank / m.rate,
		Status:        "waiting",
	}
}

// LeaveQueue 退出排队。ZRem 删除不存在的 member 不报错，所以重复退出是幂等的。
func (m *Manager) LeaveQueue(ctx context.Context, eventID, userID uint64) error {
	return m.rdb.ZRem(ctx, QueueKey(eventID), strconv.FormatUint(userID, 10)).Err()
}

// Length 队列长度（O(1)），运营大屏 / 溢出判断用。
func (m *Manager) Length(ctx context.Context, eventID uint64) (int64, error) {
	return m.rdb.ZCard(ctx, QueueKey(eventID)).Result()
}

// Snapshot 全量拉取某活动的队列（member 按 score 升序 = FIFO 顺序），broadcaster 用。
func (m *Manager) Snapshot(ctx context.Context, eventID uint64) ([]redis.Z, error) {
	return m.rdb.ZRangeWithScores(ctx, QueueKey(eventID), 0, -1).Result()
}

// ActiveEvents 活跃队列列表。
func (m *Manager) ActiveEvents(ctx context.Context) ([]uint64, error) {
	ss, err := m.rdb.SMembers(ctx, QueueActiveKey).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(ss))
	for _, s := range ss {
		if id, err := strconv.ParseUint(s, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// ProcessBatch 放行队首 n 人（dispatcher 周期调用，仅 leader 实例）：
//
//	① ZPopMin 弹出队首 n 人（score 最小的 n 个 = 最早入队的 n 个）
//	② 逐人写 admit 资格标记（TTL = 购买窗口）—— 没有这一步，放行只是数字游戏，
//	   用户提交秒杀时 seckill 无从知道"轮到他了"
//	③ PUBLISH 放行事件到 queue:events —— 所有 ws-getway 实例收到后给本地在线连接推
//	   queue_admitted 帧
func (m *Manager) ProcessBatch(ctx context.Context, eventID uint64, n int) ([]uint64, error) {
	zs, err := m.rdb.ZPopMin(ctx, QueueKey(eventID), int64(n)).Result()
	if err != nil {
		return nil, err
	}
	if len(zs) == 0 {
		return nil, nil
	}

	users := make([]uint64, 0, len(zs))
	pipe := m.rdb.Pipeline()
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		uid, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			continue // 脏数据：跳过，不影响其他人放行
		}
		users = append(users, uid)
		pipe.Set(ctx, AdmitKey(eventID, uid), 1, AdmitWindow)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return users, err
	}

	// 广播放行事件（低频：每秒最多 rate 条，Pub/Sub 完全扛得住）
	windowSec := int64(AdmitWindow.Seconds())
	for _, uid := range users {
		payload, err := json.Marshal(contract.QueueEvent{
			Type: "admitted", EventID: eventID, UserID: uid, Window: windowSec,
		})
		if err != nil {
			continue
		}
		if err := m.rdb.Publish(ctx, QueueChannel, payload).Err(); err != nil {
			// 广播失败只影响 WS 提醒的及时性：资格标记已写成功，
			// 用户轮询 position 时会发现"已不在队列"从而去提交秒杀。记日志即可。
			_ = err
		}
	}
	return users, nil
}

// Cleanup 清理超时未放行的成员（清理协程周期调用，仅 leader 实例）。
// Sorted Set 是单 key，没法按 member 单独过期 —— 用 score（入队时间戳）做范围删除：
// 入队超过 JoinTTL 还没轮到的，视为用户已放弃（参考项目里 queue:user 标记 30 分钟 TTL 的等价物）。
func (m *Manager) Cleanup(ctx context.Context) error {
	eventIDs, err := m.ActiveEvents(ctx)
	if err != nil {
		return err
	}
	cutoff := strconv.FormatInt(time.Now().Add(-JoinTTL).UnixNano(), 10)
	for _, eventID := range eventIDs {
		if err := m.rdb.ZRemRangeByScore(ctx, QueueKey(eventID), "-inf", cutoff).Err(); err != nil {
			return err
		}
		// 队列清空了就摘掉活跃标记，避免 dispatcher/broadcaster 空扫
		if n, err := m.Length(ctx, eventID); err == nil && n == 0 {
			m.rdb.SRem(ctx, QueueActiveKey, strconv.FormatUint(eventID, 10))
		}
	}
	return nil
}

// Clear 活动结束清空整个队列（管理员操作 / 运维脚本调用）。
// Sorted Set 单 key 方案下 Del 一次就干净，不存在参考项目 List 版
// "只删了队列本体、queue:user 标记残留 30 分钟" 的问题。
func (m *Manager) Clear(ctx context.Context, eventID uint64) error {
	if err := m.rdb.Del(ctx, QueueKey(eventID)).Err(); err != nil {
		return err
	}
	return m.rdb.SRem(ctx, QueueActiveKey, strconv.FormatUint(eventID, 10)).Err()
}
```

> 💡 **为什么 JoinQueue 里 LLen/RPush 那种"位次可能差 1"的竞态这里没有了？**
> 参考项目 List 版是"先 LLen 再 RPush"两条命令，中间可能被人插队，位次显示差 1。
> Sorted Set 版是"ZAddNX + ZRank"，ZRank 读的是**入队之后**的真实排名，天然准确。
> （即便有微小偏差也不用修——位次是估算展示值，客户端会持续收到推送/轮询刷新。）

---

## 8. 第四步：hub 支持通用推送

当前 `hub.Hub` 只有 `PushSeckillResult` 一种推送。新增两个方法（`biz/hub/hub.go` 追加），
现有 `PushSeckillResult` 改为薄封装（行为不变）：

```go
// HasUser 该用户是否有本地在线连接（broadcaster 过滤用，O(1)）。
func (h *Hub) HasUser(userID uint64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.conns[userID]
	return ok
}

// PushFrame 把任意契约帧推给该用户的所有在线连接。
// 用户不在线 → 直接丢弃（best-effort：位次靠轮询兜底，放行靠 admit 标记兜底）。
func (h *Hub) PushFrame(userID uint64, frameType string, data any) {
	frame, err := json.Marshal(contract.Frame{Type: frameType, Data: data})
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	set, ok := h.conns[userID]
	if !ok {
		return
	}
	for c := range set {
		c.trySend(frame)
	}
}
```

`PushSeckillResult` 内部改为一行 `h.PushFrame(uint64(result.UserID), contract.FrameTypeSeckillResult, result)`，
对外签名不变，subscriber 无需改动。

> 🎯 **丢帧/离线为什么不补发？** send 缓冲（16 帧）满说明客户端消费慢或假死，丢帧保服务；
> 位次数据是连续状态，下一个广播周期自然覆盖；放行事件有 admit 标记做事实源。
> 这与 `seckill-ws-push.md` 的"推送允许丢，轮询兜底"是同一条原则。

---

## 9. 第五步：放行调度器 Dispatcher（含分布式锁选主）

新建 `apps/ws-getway/biz/queue/dispatcher.go`。

**为什么必须选主**：`ProcessBatch` 是全局有副作用的操作（弹出用户 + 发资格）。
如果 3 个 ws-getway 实例都无脑跑放行循环，实际放行速率 = 配置速率 × 3，秒杀链路的阀门失效。
方案：`SET queue:leader {instanceID} NX PX 10000` 抢锁，抢到的实例是 leader；
持有者每 5 秒 Lua 原子续期；leader 崩溃后锁 10 秒自动过期，其他实例补位。

```go
package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Dispatcher 放行调度器：按固定速率把用户从排队大厅放进秒杀链路。
// 每个 ws-getway 实例都启动它，但只有持有 queue:leader 锁的实例真正放行。
type Dispatcher struct {
	mgr        *Manager
	rdb        *redis.Client
	logger     *slog.Logger
	instanceID string    // 随机 ID，作为锁 value：只有"锁里是自己"才允许续期
	isLeader   atomic.Bool
}

func NewDispatcher(mgr *Manager, rdb *redis.Client, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{mgr: mgr, rdb: rdb, logger: logger, instanceID: randomID()}
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Run 阻塞运行：选主循环 + 放行循环 + 清理循环。main 里 go Run(ctx) 启动。
func (d *Dispatcher) Run(ctx context.Context) {
	go d.campaign(ctx)

	// 放行节拍：每 200ms 一 tick，每 tick 放 rate/5 人 → 平均速率 = rate 人/秒。
	// 比每秒一大波放行更平滑，避免 seckill 收到锯齿状流量。
	perTick := DefaultRate / 5
	if perTick < 1 {
		perTick = 1
	}
	dispatchTicker := time.NewTicker(200 * time.Millisecond)
	cleanupTicker := time.NewTicker(time.Minute)
	defer dispatchTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dispatchTicker.C:
			if !d.isLeader.Load() {
				continue // 非 leader 空转：循环本身保留，锁易主后自动接棒
			}
			d.dispatchOnce(ctx, perTick)
		case <-cleanupTicker.C:
			if !d.isLeader.Load() {
				continue
			}
			if err := d.mgr.Cleanup(ctx); err != nil {
				d.logger.Error("[Queue] cleanup failed", "error", err)
			}
		}
	}
}

func (d *Dispatcher) dispatchOnce(ctx context.Context, perTick int) {
	events, err := d.mgr.ActiveEvents(ctx)
	if err != nil {
		d.logger.Error("[Queue] list active events failed", "error", err)
		return
	}
	for _, eventID := range events {
		users, err := d.mgr.ProcessBatch(ctx, eventID, perTick)
		if err != nil {
			d.logger.Error("[Queue] dispatch failed", "event_id", eventID, "error", err)
			continue
		}
		if len(users) > 0 {
			d.logger.Info("[Queue] admitted", "event_id", eventID, "count", len(users))
		}
	}
}

// renewScript 原子续期：锁 value 等于自己才续，防止把别人的锁续到自己名下。
var renewScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

// campaign 选主/续期循环：每 5s 一次。抢锁（SetNX）成功 → 成为 leader；
// 抢不到但锁里是自己（续期窗口）→ 续期保持 leader；否则让位。
func (d *Dispatcher) campaign(ctx context.Context) {
	ticker := time.NewTicker(LeaderRenewInterval)
	defer ticker.Stop()
	d.tryCampaign(ctx) // 启动立即试一次，不等第一个 tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.tryCampaign(ctx)
		}
	}
}

func (d *Dispatcher) tryCampaign(ctx context.Context) {
	ok, err := d.rdb.SetNX(ctx, QueueLeaderKey, d.instanceID, LeaderTTL).Result()
	if err != nil {
		d.setLeader(false, "setnx error")
		return
	}
	if ok {
		d.setLeader(true, "acquired")
		return
	}
	// 没抢到：若锁本来是自己持有的（周期续期），续上；否则锁在别人手里
	n, err := renewScript.Run(ctx, d.rdb, []string{QueueLeaderKey},
		d.instanceID, LeaderTTL.Milliseconds()).Int()
	if err != nil || n == 0 {
		d.setLeader(false, "held by other")
		return
	}
	d.setLeader(true, "renewed")
}

func (d *Dispatcher) setLeader(v bool, why string) {
	if d.isLeader.Load() && !v {
		d.logger.Info("[Queue] lost leadership", "reason", why)
	}
	if !d.isLeader.Load() && v {
		d.logger.Info("[Queue] became leader", "reason", why, "instance", d.instanceID)
	}
	d.isLeader.Store(v)
}
```

> ⚠️ 记得 import `"sync/atomic"`（`isLeader atomic.Bool`）。
>
> 💡 **leader 切换瞬间的语义**：旧 leader 崩溃到新 leader 补位之间有 ≤10 秒的放行空窗——
> 这只是"排队时间暂时变长"，不产生任何正确性问题。反过来（双 leader）才是事故，
> 锁 + 原子续期保证了不会发生（极端时钟漂移下有小概率双主，放行是幂等弹栈，多放几个人
> 也只是速率瞬时偏高，可接受）。

---

## 10. 第六步：位次推送器 Broadcaster

新建 `apps/ws-getway/biz/queue/broadcaster.go`。**每个实例都跑，不需要选主。**

```go
package queue

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"ws-getway/biz/contract"
	"ws-getway/biz/hub"
)

// Broadcaster 位次推送器：周期把队列里每个人的最新位次推给【本地在线】的用户。
//
// 为什么每实例自主拉取而不走 Pub/Sub（对比 §2 的放行事件）：
//   位次是连续状态、事实源在 Redis，谁都能算 —— 一次 ZRANGE 在内存里算出所有人的位次，
//   比给每个用户逐条 PUBLISH（20 万人 × 每 2s = 10 万 msg/s）低 5 个数量级。
//   推送前 hub.HasUser 本地过滤，不在线的不推（HTTP 轮询兜底）。
type Broadcaster struct {
	mgr      *Manager
	hub      *hub.Hub
	logger   *slog.Logger
	interval time.Duration
}

func NewBroadcaster(mgr *Manager, h *hub.Hub, logger *slog.Logger) *Broadcaster {
	return &Broadcaster{mgr: mgr, hub: h, logger: logger, interval: 2 * time.Second}
}

func (b *Broadcaster) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.broadcastOnce(ctx)
		}
	}
}

func (b *Broadcaster) broadcastOnce(ctx context.Context) {
	events, err := b.mgr.ActiveEvents(ctx)
	if err != nil {
		return
	}
	rate := int64(DefaultRate)
	for _, eventID := range events {
		zs, err := b.mgr.Snapshot(ctx, eventID)
		if err != nil || len(zs) == 0 {
			continue
		}
		for i, z := range zs {
			member, ok := z.Member.(string)
			if !ok {
				continue
			}
			uid, err := strconv.ParseUint(member, 10, 64)
			if err != nil {
				continue
			}
			if !b.hub.HasUser(uid) {
				continue // 不在线不推，省掉序列化和投递开销
			}
			b.hub.PushFrame(uid, contract.FrameTypeQueuePosition, contract.QueuePositionPayload{
				EventID:       eventID,
				Position:      int64(i + 1),
				TotalAhead:    int64(i),
				EstimatedWait: int64(i) / rate,
				Status:        "waiting",
			})
		}
	}
}
```

**成本对比（为什么"推"在这里完胜"拉"）**：

| 方案 | Redis 命令/秒（20 万人） | 网络流量 |
| --- | --- | --- |
| 每人每 3s 轮询 GetPosition | ~66,667 次 ZRANK | 小（但 6.7 万 QPS 打连接层） |
| 每实例每 2s 一次 Snapshot + 本地过滤 | 实例数 ÷ 2 次 ZRANGE | 每次全量 ~2MB，10 实例 = 10MB/2s ✅ |

> 💡 **生产化方向**（先不做）：队列规模到几十万后，改为"只对本地在线用户逐个 ZRANK（pipeline 批量）"，
> 把 O(队列长度) 的传输变成 O(本地在线排队用户数)。前提是 hub 能回答"哪些本地用户在排哪个队"，
> 需要给 Conn 加一个 eventID 归属（入队 HTTP 响应时记录），见 §16。

---

## 11. 第七步：subscriber 扩展 —— 订阅放行事件

`biz/subscriber/subscriber.go` 现在只订阅 `seckill:results`。改为同时订阅 `queue:events`，
按消息来源分发（go-redis 的一个 PubSub 连接可以订阅多个频道，`msg.Channel` 区分来源）：

```go
// Run 订阅两个频道：秒杀结果（seckill 产生）+ 排队放行事件（本服务 leader 产生）。
func (s *Subscriber) Run(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, contract.SeckillResultChannel, contract.QueueChannel)
	defer sub.Close()
	hlog.Info("[PubSub] subscribed", "channels", []string{contract.SeckillResultChannel, contract.QueueChannel})
	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				hlog.Error("[PubSub] channel closed, subscriber exiting")
				return
			}
			switch msg.Channel {
			case contract.SeckillResultChannel:
				s.dispatchSeckillResult(msg.Payload)
			case contract.QueueChannel:
				s.dispatchQueueEvent(msg.Payload)
			}
		}
	}
}

// dispatchQueueEvent 放行事件 → 给该用户所有本地在线连接推 queue_admitted 帧。
func (s *Subscriber) dispatchQueueEvent(payload string) {
	var ev contract.QueueEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		hlog.Error("[PubSub] bad queue event", "error", err)
		return
	}
	if ev.Type == "admitted" {
		s.hub.PushFrame(ev.UserID, contract.FrameTypeQueueAdmitted, ev)
	}
}
```

原 `dispatch` 重命名为 `dispatchSeckillResult`，逻辑不变。

---

## 12. 第八步：HTTP 接口

新建 `apps/ws-getway/biz/handler/queue_handler.go`（与 `ws_services.go` 同包 `ws_services`）：

```go
package ws_services

import (
	"context"
	"errors"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"ws-getway/biz/queue"
	"ws-getway/mvw"
)

var queueMgr *queue.Manager

// InitQueue 由 main 注入排队管理器。
func InitQueue(m *queue.Manager) { queueMgr = m }

// JoinQueue 处理 POST /api/queue/:event_id/join
func JoinQueue(ctx context.Context, c *app.RequestContext) {
	eventID, ok := parseEventID(c)
	if !ok {
		return
	}
	userID := mvw.GetUserID(c)
	if userID == 0 {
		c.JSON(401, map[string]string{"error": "未登录"})
		return
	}

	pos, err := queueMgr.JoinQueue(ctx, eventID, userID)
	if err != nil {
		if errors.Is(err, queue.ErrAlreadyInQueue) {
			// 幂等友好：重复点击"排队"不报错，直接返回当前位次
			if p, e := queueMgr.GetPosition(ctx, eventID, userID); e == nil {
				c.JSON(200, p)
				return
			}
			c.JSON(200, map[string]string{"error": queue.ErrAlreadyInQueue.Error()})
			return
		}
		c.JSON(500, map[string]string{"error": "加入队列失败"})
		return
	}
	c.JSON(200, pos)
}

// QueuePosition 处理 GET /api/queue/:event_id/position（WS 丢帧时的轮询兜底）
func QueuePosition(ctx context.Context, c *app.RequestContext) {
	eventID, ok := parseEventID(c)
	if !ok {
		return
	}
	userID := mvw.GetUserID(c)
	if userID == 0 {
		c.JSON(401, map[string]string{"error": "未登录"})
		return
	}
	pos, err := queueMgr.GetPosition(ctx, eventID, userID)
	if err != nil {
		if errors.Is(err, queue.ErrNotInQueue) {
			c.JSON(404, map[string]string{"error": err.Error()})
			return
		}
		c.JSON(500, map[string]string{"error": "获取位次失败"})
		return
	}
	c.JSON(200, pos)
}

// LeaveQueue 处理 DELETE /api/queue/:event_id（放弃排队；页面关闭时可发 beacon 调用）
func LeaveQueue(ctx context.Context, c *app.RequestContext) {
	eventID, ok := parseEventID(c)
	if !ok {
		return
	}
	userID := mvw.GetUserID(c)
	if userID == 0 {
		c.JSON(401, map[string]string{"error": "未登录"})
		return
	}
	if err := queueMgr.LeaveQueue(ctx, eventID, userID); err != nil {
		c.JSON(500, map[string]string{"error": "退出队列失败"})
		return
	}
	c.JSON(200, map[string]string{"message": "已离开队列"})
}

// QueueLength 处理 GET /api/queue/:event_id/length（运营大屏）
func QueueLength(ctx context.Context, c *app.RequestContext) {
	eventID, ok := parseEventID(c)
	if !ok {
		return
	}
	n, err := queueMgr.Length(ctx, eventID)
	if err != nil {
		c.JSON(500, map[string]string{"error": "获取队列长度失败"})
		return
	}
	c.JSON(200, map[string]int64{"length": n})
}

// parseEventID 校验并解析路径参数：必须是正整数。
// 排队 key 用 eventID 拼接，放任意的字符串进来会污染 Redis key 空间
// （垃圾 key 难清理，参考项目教材特别强调过这一点）。
func parseEventID(c *app.RequestContext) (uint64, bool) {
	id, err := strconv.ParseUint(string(c.Param("event_id")), 10, 64)
	if err != nil || id == 0 {
		c.JSON(400, map[string]string{"error": "invalid event id"})
		return 0, false
	}
	return id, true
}
```

路由注册已在 §5 的 `router.go` 里给出。**别忘了给入队接口加限流**——排队入口本身就可被刷，
攻击者用 1 万个账号入队就能把真实用户挤到 1 万位之后。最少做两层：
- getway/网关层：IP 级限流（现有限流中间件直接复用）；
- ws-getway 层：单用户维度限流（同一 userID 对 join 接口 1 次/秒，可用
  `queue:rl:{user}` + `SET NX PX`，或接入本项目后续统一的限流组件）。

---

## 13. 第九步：与秒杀链路联动（seckill 侧小改动）

放行的产物是资格标记 `queue:admit:{event_id}:{user_id}`（TTL 5 分钟）。
**必须让 seckill 在受理秒杀前校验它**，否则排队大厅只是"显示层"，不拦任何流量。
改三处（都在 `apps/seckill`）：

1. `internal/biz/rdb.go`：`RdbRepo` 接口加方法
   ```go
   // Exists key 是否存在（排队资格校验用）
   Exists(ctx context.Context, key string) (bool, error)
   ```

2. `internal/data/rdb.go`：实现（一行 `r.data.rdb.Exists(ctx, key).Result() > 0`）。

3. `internal/biz/seckill.go`：`PurchaseTicketAsync`（以及同步版 `PurchaseTicket`）入口处：
   ```go
   // 排队大厅联动：大流量活动必须持放行资格才能提交。
   // key 命名与 ws-getway/biz/queue/keys.go 的 QueueAdmitFmt 对齐（跨服务契约）。
   admitKey := fmt.Sprintf("queue:admit:%d:%d", eventID, userID)
   ok, err := u.rdbRepo.Exists(ctx, admitKey)
   if err != nil {
       return nil, err
   }
   if !ok {
       return nil, errors.New("请先进入排队大厅，获得资格后再提交")
   }
   // 一次资格一次提交：用掉即删，防止资格被重放/转卖。
   // 窗口期内提交失败的用户需重新排队（业务上可接受：失败通常意味着库存已尽）。
   u.rdbRepo.Del(ctx, admitKey)
   ```

**设计取舍**：资格校验放在 **seckill**（业务源头）而不是 getway——getway 只是转发层，
绕过网关直调 gRPC 的请求也必须被拦住；且 ws-getway 与 seckill 共用同一个 Redis，无新依赖。

> ⚠️ **灰度提示**：如果现有活动不想强制排队（小活动直接抢），可以把校验做成
> "活动元信息里带 `queue_required` 开关"，由运营配置决定是否校验 admit 标记。

---

## 14. 联调验证

### 14.1 启动

```bash
cd apps/ws-getway && go mod tidy && go run .
# 观察日志应出现：
#   [Queue] became leader reason=acquired instance=xxxx   （第一个实例）
# 第二个实例启动时显示 reason=held by other，且不出现放行日志
```

### 14.2 redis-cli 手动理解数据形态

```bash
# 本仓库 config.yaml 的 Redis：123.207.0.63:6379
redis-cli -h 123.207.0.63 -p 6379 -a '<password>'

# 手工塞 3 个用户进队列（模拟客户端入队，score 用递增时间戳即可）
127.0.0.1:6379> ZADD queue:z:event:1 1700000001000000000 "101"
127.0.0.1:6379> ZADD queue:z:event:1 1700000002000000000 "102"
127.0.0.1:6379> ZADD queue:z:event:1 1700000003000000000 "103"
127.0.0.1:6379> SADD queue:active 1

# 1002 的位次（0 起）—— 一条命令，无需拉全量
127.0.0.1:6379> ZRANK queue:z:event:1 "102"
(integer) 1

# 模拟放行队首 1 人
127.0.0.1:6379> ZPOPMIN queue:z:event:1 1
1) "101"
2) "1700000001000000000"

# 放行后 leader 会写出的资格标记长这样：
127.0.0.1:6379> SET queue:admit:1:101 1 EX 300
127.0.0.1:6379> GET queue:admit:1:101
"1"

# 清空某活动队列（Clear）
127.0.0.1:6379> DEL queue:z:event:1
```

### 14.3 端到端

```bash
# 1) 登录拿 token（getway 签发，ws-getway 同密钥可验）
TOKEN=<...>

# 2) 建立 WS 连接（brew install websocat）
websocat "ws://127.0.0.1:8001/ws?token=$TOKEN"
# 每 25s 发 {"type":"ping"} 保活（服务端 60s 读空闲踢连接）

# 3) 入队（另开终端）
curl -X POST http://127.0.0.1:8001/api/queue/1/join \
  -H "Authorization: Bearer $TOKEN"
# → {"event_id":1,"position":1,"total_ahead":0,"estimated_wait":0,"status":"waiting"}

# 重复入队 —— 幂等返回当前位次，不报错
curl -X POST http://127.0.0.1:8001/api/queue/1/join -H "Authorization: Bearer $TOKEN"

# 4) 回看 websocat：每 2 秒收到一次位次推送
# {"type":"queue_position","data":{"event_id":1,"position":1,"total_ahead":0,...}}
# 排到你时收到：
# {"type":"queue_admitted","data":{"type":"admitted","event_id":1,"user_id":1,"window":300}}

# 5) 验证资格标记已写入、放行后已出队
redis-cli EXISTS queue:admit:1:<你的userID>     # → 1
redis-cli ZRANK queue:z:event:1 "<你的userID>"  # → (nil)

# 6) 带资格提交秒杀（§13 的校验生效后）
curl -X POST http://127.0.0.1:8000/api/seckill \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"event_id":1,"ticket_type_id":2,"quantity":1}'
# 无资格时提交 → {"error":"请先进入排队大厅，获得资格后再提交"}

# 7) 验证轮询兜底：断开 websocat 后再入队
curl http://127.0.0.1:8001/api/queue/1/position -H "Authorization: Bearer $TOKEN"

# 8) 退出排队
curl -X DELETE http://127.0.0.1:8001/api/queue/1 -H "Authorization: Bearer $TOKEN"
```

### 14.4 单元测试要点（miniredis，不依赖外部 Redis）

```go
// apps/ws-getway/biz/queue/manager_test.go
func TestJoinQueue_FIFO(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := NewManager(rdb, 1) // rate=1：估算秒数 = 前面人数

	for i, uid := range []uint64{101, 102, 103} {
		pos, err := m.JoinQueue(context.Background(), 1, uid)
		if err != nil { t.Fatalf("JoinQueue(%d): %v", uid, err) }
		if pos.Position != int64(i+1) {
			t.Errorf("uid=%d 期望位次 %d 实际 %d", uid, i+1, pos.Position)
		}
	}
	// 重复入队被拒
	if _, err := m.JoinQueue(context.Background(), 1, 101); !errors.Is(err, ErrAlreadyInQueue) {
		t.Error("重复入队应返回 ErrAlreadyInQueue")
	}
	// 放行顺序必须 FIFO
	for _, want := range []uint64{101, 102, 103} {
		users, _ := m.ProcessBatch(context.Background(), 1, 1)
		if len(users) != 1 || users[0] != want {
			t.Errorf("期望放行 %d 实际 %v", want, users)
		}
		// 放行必须伴随资格标记
		if ok, _ := rdb.Exists(context.Background(), AdmitKey(1, want)).Result(); ok == 0 {
			t.Errorf("用户 %d 放行后缺少 admit 标记", want)
		}
	}
}
```

再补两个用例：`TestGetPosition_NotInQueue`（查不在队列的人返回 `ErrNotInQueue`）、
`TestCleanup`（score 早于 cutoff 的成员被移除，未超时的保留）。

---

## 15. 常见坑（每一条都值得在评审时盯一眼）

| 坑 | 后果 | 本方案的做法 |
| --- | --- | --- |
| 用 `LRANGE 0 -1` 查位次 | 20 万人时单次查询 ~50ms + 2MB，Redis 被打爆 | `ZRANK` O(logN)（§7） |
| `Get` 检查 + `Set` 写入做去重 | 非原子，并发点击重复入队 | `ZADD NX` 原子占位（§7） |
| 位次推送逐用户走 Pub/Sub | 20 万人 × 2s = 10 万 msg/s，自打自 | 每实例拉取 + 本地过滤，Pub/Sub 只承载低频放行事件（§2/§10） |
| 多实例都跑放行调度器 | 实际放行速率 = 配置 × 实例数，冲垮秒杀链路 | Redis 锁选主 + Lua 原子续期（§9） |
| 放行后不发资格标记 / 秒杀侧不校验 | 排队纯摆设，用户照样挤爆提交接口 | admit 标记 + seckill 入口校验 + 一次性使用（§13） |
| 资格不销毁 | 窗口内反复提交/转卖资格 | 提交秒杀时 `Del`，一次资格一次提交（§13） |
| 入队接口不限流 | 攻击者海量账号入队，挤走真实用户 | 网关 IP 限流 + 单用户限流（§12） |
| ZSet 成员没有 TTL | 用户关页面后永远占着队列位 | Cleanup 按 score（入队时间）范围删除，JoinTTL=30min（§7） |
| `ClearQueue` 只删本体、标记残留 | 参考项目 List 版的残留问题 | Sorted Set 单 key，Del 一次干净（§7） |
| 广播器对离线用户也推帧 | 序列化 + trySend 空转，浪费 CPU | `hub.HasUser` 先过滤（§10） |
| 忽略 Redis 写操作返回的 error | 故障延迟暴露、日志无痕 | 关键路径（ZAddNX/Set admit）错误直接返回；广播失败至少记日志 |
| eventID 不校验直接拼 key | 垃圾 key 污染 Redis，难清理 | `parseEventID` 强制正整数（§12） |
| 前端不处理 admitted 超时 | 窗口 5 分钟过期后用户还在等 | 客户端收到 `queue_admitted` 起倒计时 window 秒，超时提示重新排队 |

---

## 16. 生产化清单（按需演进）

- **放行速率做成配置/动态**：当前 `DefaultRate` 是常量。应接入 config，并支持按
  "秒杀侧库存消耗速度"动态调节（库存快见底就降速，给候补流转留窗口）。
- **队列粒度**：当前按 eventID 排队。如果某场活动的票种热度差异大，可细化为
  `queue:z:event:{id}:type:{ticket_type_id}`（key 命名加在 `keys.go`，别散落）。
- **溢出策略**：入队前查 `Length`，超过阈值（如 50 万）直接友好拒绝（"当前排队人数过多，请稍后再试"），
  防止 Redis 内存被极端流量打穿。
- **售罄联动**：seckill 库存为 0 时（`seckill:{ticket:{id}}:stock` 各 field 均为 0），
  leader 停止放行 + `PUBLISH queue:events {type:"cleared"}` 通知所有排队用户 →
  引导进入等候名单（附录 A）。
- **监控指标**：各活动队列长度（`ZCard`）、放行速率（dispatcher 日志聚合）、
  实际等待 P50/P99（score 到放行时刻的差值）、`trySend` 丢帧计数、leader 存活性告警。
- **前端体验**：位次推送节流渲染；admitted 倒计时；断线重连 + 轮询降级（照抄
  `seckill-ws-push.md` §11.4 的模式）。
- **压测**：重点压三个点——入队接口 QPS、20 万人队列下 broadcaster 单轮耗时、
  leader 切换期间放行空窗。

---

## 附录 A：等候名单（Waitlist）扩展要点

排队大厅（售罄前，分钟级）与等候名单（售罄后，天级）是两个机制。若要补等候名单，
参考项目 `edu/15` 第四章 + `internal/queue/waitlist.go` 的要点，同样用 Sorted Set 实现：

- **Key**：`waitlist:z:event:{id}`（score=加入时间戳），TTL 7 天语义；
- **加入**：售罄判定通过后才允许加入（查 `seckill:{ticket:{id}}:stock` 各 field 之和为 0；
  参考项目在这里翻过车——库存 key 拼错导致校验形同虚设，务必引用统一定义的 key）；
- **回补触发**：退票/订单超时释放库存后 → `ZPOPMIN` 取队首 → 写**预留标记**
  `reserved:{event}:{user}`（24h）→ WS 推 `waitlist_available` 帧 + 邮件兜底；
- **预留必须存在**：不预留的话，通知了候补用户、票却被路过的普通用户抢走，机制就白做了——
  秒杀入口同样要校验预留标记；
- **状态机**：waiting → notified（24h 购买窗口）→ converted / expired，
  expired 的回收用"以过期时间戳为 score 的待检 ZSet + `ZRANGEBYSCORE 0 now` 扫描"实现，
  不要起定时器遍历名单。

**下一章（参考项目）** 👉 `edu/16-活动场次票种与票务生命周期.md`
