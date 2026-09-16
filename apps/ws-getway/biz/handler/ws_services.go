package ws_services

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gorilla/websocket"

	"ws-getway/biz/hub"
	"ws-getway/biz/queue"
	"ws-getway/mvw"
)

var queueMgr *queue.Manager

// InitQueue 由 main 注入排队管理器。
func InitQueue(m *queue.Manager) { queueMgr = m }

var upgrader = websocket.Upgrader{
	HandshakeTimeout: 10 * time.Second,
	// 生产环境应校验 Origin 白名单，开发期先全放行
	CheckOrigin: func(r *http.Request) bool { return true },
}

var (
	wsHub *hub.Hub
)

// InitWS 由 main 注入依赖（hub 是全进程单例）。
func InitWS(h *hub.Hub) {
	wsHub = h
}

// GetWS 处理 WS 握手。路由上已挂 mvw.AuthMiddleware()：
// token 从 query 读取并校验，通过后 Identity 已放进 RequestContext。
//
// upgrader.Upgrade 内部会 Hijack 连接并把回调阻塞在读写循环上，
// 直到连接断开才返回 —— 所以这个 handler 的「慢」是正常的，它就是连接本身。
func GetWS() app.HandlerFunc {
	return adaptor.HertzHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		value, ok := ctx.Value(mvw.IdentityKey).(mvw.Identity)
		if !ok {
			http.Error(w, "未登录", http.StatusForbidden)
			return
		}
		if value.UserID == 0 {
			http.Error(w, "未登录", http.StatusForbidden)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, "未登录", http.StatusForbidden)
			hlog.CtxErrorf(ctx, "ws upgrade failed: %v", err)
			return
		}
		wsHub.Server(hub.NewConn(conn, value.UserID))
	}))
}

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
