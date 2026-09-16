package server

import (
	"context"
	"log/slog"
	"sale/pkg/conf"
	"sale/pkg/notification"
	"seckill/internal/biz"
	"seckill/internal/common"
)

type ConsumerServer struct {
	consumer *notification.Consumer
	logger   *slog.Logger
}

func NewConsumerServer(c *conf.Data, uc *biz.ConsumerUseCase, logger *slog.Logger) (*ConsumerServer, error) {
	consumer, err := notification.NewConsumer(&notification.ConsumerConfig{
		Brokers:  c.GetBrokers(),                  // 对应 config.yaml 的 data.brokers
		Topic:    common.TicketOrderTopic,         // ticket.orders
		Group:    common.TicketOrderConsumerGroup, // ticket-order-processor
		Handler:  uc.HandleMessage,                // 上面改造好的处理逻辑
		DLQTopic: common.TicketOrderDLQTopic,      // 重试超限进死信
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
