// consumer reads Kafka topics and writes to ClickHouse + Redis.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	kgo "github.com/segmentio/kafka-go"

	"github.com/sayr777/egts-server/internal/clickhouse"
	"github.com/sayr777/egts-server/internal/egts"
	"github.com/sayr777/egts-server/internal/rediscache"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	brokers    := envOr("KAFKA_BROKERS",     "kafka:9092")
	chAddr     := envOr("CLICKHOUSE_ADDR",   "clickhouse:9000")
	chDB       := envOr("CLICKHOUSE_DB",     "egts")
	chUser     := envOr("CLICKHOUSE_USER",   "egts")
	chPass     := envOr("CLICKHOUSE_PASS",   "egts")
	redisAddr  := envOr("REDIS_ADDR",        "redis:6379")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ClickHouse
	chWriter, err := waitClickHouse(ctx, chAddr, chDB, chUser, chPass)
	if err != nil {
		slog.Error("clickhouse unavailable", "err", err)
		os.Exit(1)
	}
	chWriter.Start(ctx)
	slog.Info("clickhouse connected", "addr", chAddr)

	// Redis
	cache := rediscache.New(redisAddr)
	for i := 0; i < 30; i++ {
		if err := cache.Ping(ctx); err == nil {
			break
		}
		slog.Info("waiting for redis...", "attempt", i+1)
		time.Sleep(2 * time.Second)
	}
	slog.Info("redis connected", "addr", redisAddr)

	// Two consumer goroutines share topics by consumer group
	go runConsumer(ctx, brokers, "egts.positions", "consumer-pos", func(msg []byte) error {
		return handlePosition(ctx, chWriter, cache, msg)
	})

	go runConsumer(ctx, brokers, "egts.events", "consumer-events", func(msg []byte) error {
		return handleEvents(ctx, chWriter, msg)
	})

	<-ctx.Done()
	slog.Info("consumer exiting")
}

func handlePosition(ctx context.Context, w *clickhouse.Writer, cache *rediscache.Cache, data []byte) error {
	var msg posMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if msg.Pos == nil {
		return nil
	}
	p := msg.Pos

	// ClickHouse (batched, async)
	w.WritePosition(msg.ReceivedAt, uint64(msg.DeviceID), p)

	// Redis — last known position + pub/sub for WebSocket
	cache.UpdateDevice(ctx, rediscache.DeviceState{
		DeviceID:  uint64(msg.DeviceID),
		Lat:       p.Lat,
		Lon:       p.Lon,
		Speed:     float32(p.Speed),
		Direction: p.Direction,
		UpdatedAt: msg.ReceivedAt,
	})
	return nil
}

func handleEvents(ctx context.Context, w *clickhouse.Writer, data []byte) error {
	var msg egts.KafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	t := msg.ReceivedAt
	did := uint64(msg.DeviceID)

	for _, b := range msg.Ibeacons {
		w.WriteIbeacon(t, did, b)
	}
	for _, c := range msg.Cells {
		w.WriteCell(t, did, c)
	}
	for _, ap := range msg.WifiAPs {
		w.WriteWifi(t, did, ap)
	}
	return nil
}

func runConsumer(ctx context.Context, broker, topic, group string, handle func([]byte) error) {
	r := kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: time.Second,
	})
	defer r.Close()
	slog.Info("consumer started", "topic", topic, "group", group)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("fetch error", "topic", topic, "err", err)
			time.Sleep(time.Second)
			continue
		}
		if err := handle(m.Value); err != nil {
			slog.Warn("handle error", "topic", topic, "err", err)
		}
		r.CommitMessages(ctx, m)
	}
}

func waitClickHouse(ctx context.Context, addr, db, user, pass string) (*clickhouse.Writer, error) {
	var (
		w   *clickhouse.Writer
		err error
	)
	for i := 0; i < 30; i++ {
		w, err = clickhouse.New(addr, db, user, pass)
		if err == nil {
			return w, nil
		}
		slog.Info("waiting for clickhouse...", "attempt", i+1, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, err
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type posMessage struct {
	DeviceID   uint32        `json:"device_id"`
	ReceivedAt time.Time     `json:"received_at"`
	Pos        *egts.PosData `json:"position"`
}
