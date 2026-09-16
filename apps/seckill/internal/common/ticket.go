package common

import (
	"fmt"
	"time"
)

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

	TicketResultSuccessStatus = "success"
	TicketResultFailStatus    = "fail"
)

// TicketMessage 票务消息
type TicketMessage struct {
	UserID       uint  `json:"user_id"`
	EventID      uint  `json:"event_id"`
	ShowID       uint  `json:"show_id,omitempty"`
	TicketTypeID uint  `json:"ticket_type_id"`
	Quantity     int   `json:"quantity"`
	Timestamp    int64 `json:"timestamp"`
}

// TicketResult 票务处理结果
type TicketResult struct {
	TicketID      uint   `json:"ticket_id"`
	UserID        uint   `json:"user_id"`
	EventID       uint   `json:"event_id"`
	TicketTypeID  uint   `json:"ticket_type_id"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	OrderNo       string `json:"order_no"`
	Timestamp     int64  `json:"timestamp"`
	PurchaseToken string
}

// NewTicketMessage 创建票务消息
func NewTicketMessage(userID, eventID, ticketTypeID uint, quantity int) *TicketMessage {
	return &TicketMessage{
		UserID:       userID,
		EventID:      eventID,
		TicketTypeID: ticketTypeID,
		Quantity:     quantity,
		Timestamp:    time.Now().UnixNano(),
	}
}

// NewTicketMessageWithShow 创建带场次的票务消息
func NewTicketMessageWithShow(userID, eventID, showID, ticketTypeID uint, quantity int) *TicketMessage {
	return &TicketMessage{
		UserID:       userID,
		EventID:      eventID,
		ShowID:       showID,
		TicketTypeID: ticketTypeID,
		Quantity:     quantity,
		Timestamp:    time.Now().UnixNano(),
	}
}

func TicketIdempotentKey(msg *TicketMessage) string {
	return fmt.Sprintf("mq:processed:%d:%d:%d:%d",
		msg.UserID, msg.EventID, msg.TicketTypeID, msg.Timestamp)
}

func TickeyResultKey(msg *TicketMessage) string {
	return fmt.Sprintf("purchase:result:%d:%d:%d:%d",
		msg.UserID, msg.EventID, msg.TicketTypeID, msg.Timestamp)
}

// 轮询查询时只有这四个字段，构造不出 TicketMessage，所以单独提供。
func PurchaseResultKey(userID, eventID, ticketTypeID uint, timestamp int64) string {
	return fmt.Sprintf("purchase:result:%d:%d:%d:%d", userID, eventID, ticketTypeID, timestamp)
}
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
