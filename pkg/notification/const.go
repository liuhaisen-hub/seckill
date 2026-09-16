package notification

const (
	// Kafka Topics
	TicketOrderTopic    = "ticket.orders"
	TicketOrderDLQTopic = "ticket.orders.dlq"
	TicketResultTopic   = "ticket.results"

	// Kafka Consumer Groups
	TicketOrderConsumerGroup = "ticket-order-processor"

	// Legacy Redis Stream keys (保留兼容，后续移除)
	TicketStreamKey     = "ticket:orders"
	TicketConsumerGroup = "ticket-processor"
)
