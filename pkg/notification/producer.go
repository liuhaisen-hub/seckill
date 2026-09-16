package notification

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// 生产者
type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string, opts ...kgo.Opt) (*Producer, error) {
	defaultOpts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),

		// ===== 生产确认级别 ====
		// kgo.AllFromISR：等待所有 ISR（同步副本）确认后才视为成功。
		// 这是最强的可靠性保障（配合 acks=-1），代价是延迟增加约 1 个 RTT。
		// 对订单场景：宁可慢几毫秒，不能丢数据。
		kgo.RequiredAcks(kgo.AllISRAcks()),

		// 失败重试上限。超出后报错给调用方，由调用方决定是否降级。
		kgo.RecordRetries(3),

		// 最大消息大小 1MB：默认值较小，不设的话发大消息会静默失败
		kgo.ProduceRequestTimeout(10 * time.Second),
	}
	// 合并自定义选项（调用者可在 opts 里覆盖默认值）
	defaultOpts = append(defaultOpts, opts...)
	client, err := kgo.NewClient(defaultOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &Producer{client: client, topic: topic}, nil
}

// 同步消息，否则失败不能及时回滚
func (p *Producer) ProducerSync(ctx context.Context, key []byte, value []byte) error {
	record := &kgo.Record{
		Key:   []byte(key),
		Value: value,
	}
	ctxTimout, canncel := context.WithTimeout(ctx, 10*time.Second)
	defer canncel()
	resutl := p.client.ProduceSync(ctxTimout, record)
	if err := resutl.FirstErr(); err != nil {
		return fmt.Errorf("produce failed: %w", err)
	}
	return nil
}

// 异步消息
func (p *Producer) ProducerAsync(ctx context.Context, key []byte, value []byte, cb func(error)) {
	record := &kgo.Record{
		Key:   []byte(key),
		Value: value,
	}
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if cb != nil {
			cb(err)
		}
	})
}
func (p *Producer) Close() {
	p.client.Close()
}
