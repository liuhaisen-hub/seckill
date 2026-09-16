package hub

import (
	"encoding/json"
	"sync"
	"ws-getway/biz/contract"
)

// 注册分发表

type Hub struct {
	// 读写锁
	mu sync.RWMutex
	// 所以的链接
	conns map[uint64]map[*Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{conns: make(map[uint64]map[*Conn]struct{})}
}

// 注册
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

// 统一管理生命周期
func (h *Hub) Server(c *Conn) {
	h.Register(c)
	// 写入
	go c.writeLoop()
	// 循环读取
	c.readLoop()
	// 走到这里 = 读循环退出 = 连接已断。
	// 先从注册表摘除（之后不再有任何线程能拿到 c 并往 send 投递），
	// 再关 done 让写 goroutine 退出，顺序不能反 —— 避免向已停止的 writer 投递。
	h.Unregister(c)
	close(c.done)

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
func (h *Hub) PushSeckillResult(result *contract.SeckillResult) {
	h.PushFrame(uint64(result.UserID), contract.FrameTypeSeckillResult, result)
}

func (h *Hub) HasUser(userID uint64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.conns[userID]
	return ok
}

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
