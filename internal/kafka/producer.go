package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sayr777/egts-server/internal/metrics"
	kgo "github.com/segmentio/kafka-go"
)

// Producer wraps kafka-go writers, one per topic.
type Producer struct {
	writers map[string]*kgo.Writer
}

// NewProducer creates batching writers for the given topic list.
func NewProducer(brokers []string, topics []string, batchSize int, batchTimeout time.Duration) *Producer {
	writers := make(map[string]*kgo.Writer, len(topics))
	for _, topic := range topics {
		writers[topic] = &kgo.Writer{
			Addr:         kgo.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kgo.Hash{}, // route by key → same device → same partition
			BatchSize:    batchSize,
			BatchTimeout: batchTimeout,
			Async:        true, // non-blocking: fire & forget, errors go to Completion
			Completion: func(messages []kgo.Message, err error) {
				if err != nil {
					metrics.KafkaErrorsTotal.WithLabelValues(topic).Add(float64(len(messages)))
					slog.Error("kafka write error", "topic", topic, "err", err)
					return
				}
				metrics.KafkaProducedTotal.WithLabelValues(topic).Add(float64(len(messages)))
			},
		}
	}
	return &Producer{writers: writers}
}

// Produce sends payload to topic with key (device ID as string bytes).
// Returns immediately — delivery is async.
func (p *Producer) Produce(ctx context.Context, topic string, key []byte, payload any) error {
	w, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("unknown topic %q", topic)
	}
	val, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return w.WriteMessages(ctx, kgo.Message{
		Key:   key,
		Value: val,
		Time:  time.Now(),
	})
}

// Close flushes and closes all writers. Call on shutdown.
func (p *Producer) Close() {
	for topic, w := range p.writers {
		if err := w.Close(); err != nil {
			slog.Error("kafka close error", "topic", topic, "err", err)
		}
	}
}
