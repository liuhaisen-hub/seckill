package contract

type SeckillResult struct {
	TicketID     uint   `json:"ticket_id"`
	UserID       uint   `json:"user_id"`
	EventID      uint   `json:"event_id"`
	TicketTypeID uint   `json:"ticket_type_id"`
	Status       string `json:"status"` // success / failed
	Message      string `json:"message"`
	OrderNo      string `json:"order_no"`
	Timestamp    int64  `json:"timestamp"` // 关联 ID = 提交秒杀时返回的 timestamp
}

// SeckillResultChannel 与 seckill/internal/common.SeckillResultChannel 保持一致。
const SeckillResultChannel = "seckill:results"
const QueueChannel = "seckill:quene"

// 下行帧类型
const (
	FrameTypeSeckillResult = "seckill_result"
	FrameTypePong          = "pong"
	FrameTypeQueuePosition = "queue_position" // 位次推送：{"type":"queue_position","data":{...}}
	FrameTypeQueueAdmitted = "queue_admitted" // 放行通知：{"type":"queue_admitted","data":{...}}
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
	Type    string `json:"type"` // admitted
	EventID uint64 `json:"event_id"`
	UserID  uint64 `json:"user_id,omitempty"`
	Window  int64  `json:"window,omitempty"` // 购买窗口秒数，倒计时展示用
}
