package hub

import (
	"encoding/json"
	"time"
	"ws-getway/biz/contract"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gorilla/websocket"
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

type Conn struct {
	ws     *websocket.Conn
	userID uint64
	send   chan []byte
	done   chan struct{} // Serve 关闭它 → writeLoop 退出
}

func NewConn(ws *websocket.Conn, userID uint64) *Conn {
	return &Conn{
		ws:     ws,
		userID: userID,
		send:   make(chan []byte, sendBufferSize),
		done:   make(chan struct{}),
	}
}

// 不阻塞， 缓冲慢了说明客户端可能已经没了， 丢帧
func (c *Conn) trySend(frame []byte) {
	select {
	case c.send <- frame:
	default:
		hlog.Warn("[WS] send buffer full, drop frame", "user_id", c.userID)
	}
}

func (c *Conn) readLoop() {
	_ = c.ws.SetReadDeadline(time.Now().Add(readIdleTimeout))
	for {
		msgType, playload, err := c.ws.ReadMessage()
		if err != nil {
			hlog.Info("[WS] read closed", "user_id", c.userID, "error", err)
			return
		}
		// 只要能收到信号，就是活着的. 续期
		_ = c.ws.SetReadDeadline(time.Now().Add(readIdleTimeout))
		if msgType != websocket.TextMessage {
			continue
		}
		var in contract.Frame
		if json.Unmarshal(playload, &in) != nil {
			continue
		}
		if in.Type == "ping" {
			// 心跳
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
				hlog.Info("[WS] write failed, closing conn", "user_id", c.userID, "error", err)
				_ = c.ws.Close()
				return
			}
		case <-c.done:
			return
		}
	}
}
