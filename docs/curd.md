⏺ 对比完成。老项目 router.go 中所有路由与新项目 apps/getway（Hertz 生成路由）逐一比对后，结果如下：

  ✅ 已覆盖的（无需关注）

  - 认证：/api/auth/register、/api/auth/login、/api/profile、/admin/users/role
  - 活动/场次：events 列表/详情、shows 全部（含 admin 的 CRUD 和 publish/unpublish/end）
  - 票种管理：admin 的 ticket-types CRUD 全有；新增了用户侧 GET /api/ticket-types、GET /api/ticket-types/:id
  - 秒杀购买：POST /api/purchase（对应老的 /api/tickets/purchase，路径变了）
  - 促销码：validate、admin create/delete 都有（新增了用户侧 Get/Update Promo）
  - 统计、转让审核（admin 侧）、促销码 admin 管理全部有

  ❌ 缺失的 CRUD 类型路由

  ┌────────────────────────────────┬────────────────────┬───────────────────┐
  │             老路由             │       老函数       │       说明        │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/events/:id/stock      │ GetEventStock      │ 库存查询（R）     │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/tickets               │ GetMyTickets       │ 我的票列表（R）   │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/tickets/:id           │ GetTicketDetail    │ 票详情（R）       │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/transfer/history      │ GetTransferHistory │ 转让历史查询（R） │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/marketplace           │ ListActive         │ 二手市场列表（R） │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/marketplace/my        │ ListMyListings     │ 我的挂售（R）     │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/marketplace/purchases │ ListMyPurchases    │ 我的购买（R）     │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/marketplace/event/:id │ ListByEvent        │ 按活动查（R）     │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ GET /api/marketplace/:id       │ GetListing         │ 挂售详情（R）     │
  ├────────────────────────────────┼────────────────────┼───────────────────┤
  │ POST /api/marketplace          │ CreateListing      │ 创建挂售（C）     │
  └────────────────────────────────┴────────────────────┴───────────────────┘

  ▎ 整个 marketplace（二手市场）模块 在新项目中完全没有。

  ❌ 缺失的非 CRUD 类型（业务流程/基础设施）

  票务生命周期（订单状态机）
  - POST /api/tickets/:id/pay — 支付
  - POST /api/tickets/:id/cancel — 取消
  - POST /api/tickets/:id/use — 核销

  二手市场交易操作
  - POST /api/marketplace/:id/buy — 购买
  - POST /api/marketplace/:id/cancel — 取消挂售

  票务转让（用户侧发起）
  - POST /api/transfer — 发起转让
  - POST /api/transfer/gift — 直接赠送（admin 侧 approve/reject 已有，但用户侧发起端缺失）

  排队系统（整个模块缺失）
  - POST /api/queue/:event_id/join、GET /api/queue/:event_id/position、POST /api/queue/:event_id/leave

  等候名单（整个模块缺失）
  - POST /api/waitlist/:event_id/join、GET /api/waitlist/:event_id/position、POST /api/waitlist/:event_id/leave

  基础设施
  - GET /ws — WebSocket Hub（新项目无 WS）
  - GET /health — 健康检查（新项目只有 /ping，不含 DB/Redis 依赖检查，算部分覆盖）

  另外注意：新项目有几个老项目没有的新增路由：DELETE /api/events/:id（DeleteEvent）、促销码的 GetPromo/UpdatePromo，重构时可以确认这些是否为有意扩展。