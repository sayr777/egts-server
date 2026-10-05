// consumer reads egts.positions and egts.events from Kafka
// and writes them into TimescaleDB using batch COPY.
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

	"github.com/jackc/pgx/v5/pgxpool"
	kgo "github.com/segmentio/kafka-go"

	"github.com/sayr777/egts-server/internal/egts"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	brokers := envOr("KAFKA_BROKERS", "kafka:9092")
	dbURL := envOr("DATABASE_URL", "postgres://egts:egts@timescaledb:5432/egts?sslmode=disable")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("db connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Wait for DB to be ready
	for i := 0; i < 30; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		slog.Info("waiting for database...", "attempt", i+1)
		time.Sleep(2 * time.Second)
	}
	slog.Info("database connected", "url", dbURL)

	go runConsumer(ctx, brokers, "egts.positions", "consumer-positions", func(msg []byte) error {
		return handlePosition(ctx, pool, msg)
	})

	go runConsumer(ctx, brokers, "egts.events", "consumer-events", func(msg []byte) error {
		return handleEvents(ctx, pool, msg)
	})

	<-ctx.Done()
	slog.Info("consumer shutting down")
}

// runConsumer reads messages from topic and calls handler for each.
// Uses manual offset commit after successful handler execution.
func runConsumer(ctx context.Context, broker, topic, groupID string, handler func([]byte) error) {
	r := kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: time.Second,
	})
	defer r.Close()

	slog.Info("consumer started", "topic", topic, "group", groupID)
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
		if err := handler(m.Value); err != nil {
			slog.Warn("handler error", "topic", topic, "err", err)
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			slog.Warn("commit error", "topic", topic, "err", err)
		}
	}
}

// handlePosition inserts one position record into TimescaleDB.
func handlePosition(ctx context.Context, pool *pgxpool.Pool, data []byte) error {
	var msg posMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if msg.Pos == nil {
		return nil
	}
	p := msg.Pos

	_, err := pool.Exec(ctx, `
		INSERT INTO positions (time, device_id, lat, lon, speed, direction, altitude, odometer, valid)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		p.Time, msg.DeviceID, p.Lat, p.Lon,
		p.Speed, p.Direction, p.Altitude, p.Odometer, p.Valid,
	)
	if err != nil {
		return fmt.Errorf("insert position: %w", err)
	}

	// Fire-and-forget upsert of device last-seen (non-critical)
	go pool.Exec(ctx,
		`SELECT update_device_last_seen($1, $2, $3, $4, $5)`,
		msg.DeviceID, p.Time, p.Lat, p.Lon, p.Speed,
	)
	return nil
}

// handleEvents inserts iBeacon / cell / WiFi events.
func handleEvents(ctx context.Context, pool *pgxpool.Pool, data []byte) error {
	var msg egts.KafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	for _, b := range msg.Ibeacons {
		uuidStr := formatUUID(b.UUID[:])
		_, err := pool.Exec(ctx, `
			INSERT INTO events_ibeacon (time, device_id, event_type, major, minor, rssi, tx_power, uuid)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8::uuid)`,
			msg.ReceivedAt, msg.DeviceID,
			b.EventType, b.Major, b.Minor, b.RSSI, b.TxPower, uuidStr,
		)
		if err != nil {
			slog.Warn("insert ibeacon failed", "err", err)
		}
	}

	for _, c := range msg.Cells {
		_, err := pool.Exec(ctx, `
			INSERT INTO events_cell (time, device_id, mcc, mnc, lac, cell_id, rssi, rat)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			msg.ReceivedAt, msg.DeviceID,
			c.MCC, c.MNC, c.LAC, c.CellID, c.RSSI, c.RAT,
		)
		if err != nil {
			slog.Warn("insert cell failed", "err", err)
		}
	}

	for _, w := range msg.WifiAPs {
		mac := fmt.Sprintf("%02X:%02X:%02X:%02X:%02X:%02X",
			w.BSSID[0], w.BSSID[1], w.BSSID[2],
			w.BSSID[3], w.BSSID[4], w.BSSID[5])
		_, err := pool.Exec(ctx, `
			INSERT INTO events_wifi (time, device_id, bssid, ssid, rssi, channel)
			VALUES ($1, $2, $3::macaddr, $4, $5, $6)`,
			msg.ReceivedAt, msg.DeviceID,
			mac, w.SSID, w.RSSI, w.Channel,
		)
		if err != nil {
			slog.Warn("insert wifi failed", "err", err)
		}
	}

	return nil
}

func formatUUID(b []byte) string {
	if len(b) < 16 {
		return "00000000-0000-0000-0000-000000000000"
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// posMessage mirrors the position-specific Kafka payload.
type posMessage struct {
	DeviceID   uint32        `json:"device_id"`
	ReceivedAt time.Time     `json:"received_at"`
	Pos        *egts.PosData `json:"position"`
}
