package notification

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 消费者封装

type MessageHandler func(ctx context.Context, key, value []byte) error

type Consumer struct {
	client      *kgo.Client
	topic       string
	group       string
	handler     MessageHandler
	maxRetries  int    // 重试次数
	dlqTopic    string //死信队列
	dlqProducer *Producer
}

type ConsumerConfig struct {
	Brokers    []string
	Topic      string
	Group      string
	Handler    MessageHandler
	MaxRetries int    // 默认 3
	DLQTopic   string // 默认 "topic.dlq"
}

func NewConsumer(c *ConsumerConfig, opts ...kgo.Opt) (*Consumer, error) {
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	defaultOpts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumeTopics(c.Topic),
		kgo.ConsumerGroup(c.Group),

		// 【关键：手动提交 offset】
		// 默认是自动提交（每 5 秒）。
		// 自动提交的风险：
		//   消费 → 处理中 → 自动提交 offset → 处理失败（如 DB 宕机）
		//   → 消费者重启 → 从已提交的 offset 开始消费 → 跳过了那条失败的消息！
		// 手动提交保证：只有「处理成功的消息」才提交 offset。
		kgo.DisableAutoCommit(),

		// 每次 Fetch 最多 10MB 数据，防止单次拉取过多占用内存
		kgo.FetchMaxBytes(10 << 20), // 10MB
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.SessionTimeout(30 * time.Second),
		kgo.RebalanceTimeout(30 * time.Second),
	}
	defaultOpts = append(defaultOpts, opts...)
	client, err := kgo.NewClient(defaultOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	consumer := &Consumer{
		client:     client,
		topic:      c.Topic,
		group:      c.Group,
		handler:    c.Handler,
		maxRetries: c.MaxRetries,
		dlqTopic:   c.DLQTopic,
	}

	// 如果配置了 死信 Topic，创建一个专用生产者
	if c.DLQTopic != "" {
		dlqProducer, err := NewProducer(c.Brokers, c.DLQTopic)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create DLQ producer: %w", err)
		}
		consumer.dlqProducer = dlqProducer
	}
	return consumer, nil
}

// 开始消费
func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.client.Close()
			if c.dlqProducer != nil {
				c.dlqProducer.Close()
			}
			return
		default:
			// 拉数据
			fetches := c.client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			fetches.EachError(func(s string, i int32, err error) {
				hlog.Info("[Kafka] Fetch error: topic=%s partition=%d err=%v",
					s, i, err)
			})
			// 收集所有 Record
			var records []*kgo.Record
			fetches.EachRecord(func(record *kgo.Record) {
				records = append(records, record)
			})
			for _, record := range records {
				c.processRecord(ctx, record)
			}
			// 手动提交offset
			if len(records) > 0 {
				if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
					log.Printf("[Kafka] Failed to commit offsets: %v", err)
				}
			}
		}
	}
}

// processRecord 处理单条消息（带重试和死信队列）
func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) {
	retryCount := getRetryCount(record)
	for i := retryCount; i > c.maxRetries; i++ {
		err := c.handler(ctx, record.Key, record.Value)
		if err != nil {
			hlog.Info("handle success: %w", record.Key)
			return // 处理成功
		}
		// 退避策略：等差数列重试间隔，重试越多次等越久
		//   retry 0 → 100ms
		//   retry 1 → 200ms
		//   retry 2 → 300ms
		// 避免重试风暴打爆下游
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	// ===== 超过最大重试次数，发到死信队列 =====
	if c.dlqProducer != nil {
		log.Printf("[Kafka] Message sent to DLQ: topic=%s offset=%d", c.topic, record.Offset)

		dlqMsg := &kgo.Record{
			Topic: c.dlqTopic,
			Key:   record.Key,
			Value: record.Value,
			// 把原始元信息写进消息头，方便运维排查
			Headers: []kgo.RecordHeader{
				{Key: "original-topic", Value: []byte(record.Topic)},
				{Key: "original-offset", Value: []byte(fmt.Sprintf("%d", record.Offset))},
				{Key: "retry-count", Value: []byte(fmt.Sprintf("%d", c.maxRetries))},
				{Key: "error-time", Value: []byte(time.Now().Format(time.RFC3339))},
			},
		}
		c.dlqProducer.ProducerAsync(ctx, dlqMsg.Key, dlqMsg.Value, func(err error) {
			if err != nil {
				hlog.Info("[Kafka] Failed to send to DLQ: %v", err)
			} else {
				hlog.Info("[Kafka] Message dropped (no DLQ): topic=%s offset=%d", c.topic, record.Offset)
			}
		})
	}
}

// getRetryCount 从消息头读取已有的重试次数（用于 DLQ 再次消费后判断是否继续重试）
func getRetryCount(record *kgo.Record) int {
	for _, header := range record.Headers {
		if header.Key == "retry-count" {
			var count int
			fmt.Sscanf(string(header.Value), "%d", &count)
			return count
		}
	}
	return 0
}
